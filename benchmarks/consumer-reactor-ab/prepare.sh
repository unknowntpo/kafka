#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

# Jenkins workers may start with the C locale, which makes Gradle and javac
# decode Java sources as US-ASCII. Keep artifact preparation reproducible when
# a source comment contains non-ASCII punctuation.
export LANG=C.UTF-8
export LC_ALL=C.UTF-8

readonly DEFAULT_BASELINE_COMMIT=80a74f3b84525563ef060b6e0e1b70bc127ec064
readonly EXPECTED_BASELINE_COMMIT="${EXPECTED_BASELINE_COMMIT:-$DEFAULT_BASELINE_COMMIT}"
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly POC_WORKTREE="$(cd -- "$SCRIPT_DIR/../.." && pwd -P)"
readonly DEFAULT_BASELINE_WORKTREE="$(cd -- "$POC_WORKTREE/.." && pwd -P)/reactor-benchmark-baseline"
readonly BASELINE_WORKTREE="${BASELINE_WORKTREE:-$DEFAULT_BASELINE_WORKTREE}"
readonly BUILD_DIR="${REACTOR_AB_BUILD_DIR:-$SCRIPT_DIR/build}"
readonly INIT_SCRIPT="$SCRIPT_DIR/runtime-classpath.gradle"
readonly THROUGHPUT_HARNESS="$SCRIPT_DIR/ThroughputHarness.java"

die() {
    printf 'prepare.sh: %s\n' "$*" >&2
    exit 1
}

validate_worktree() {
    local label="$1"
    local path="$2"
    [[ -d "$path" ]] || die "$label worktree does not exist: $path"
    [[ -x "$path/gradlew" ]] || die "$label worktree has no executable gradlew: $path"
    git -C "$path" rev-parse --is-inside-work-tree >/dev/null 2>&1 ||
        die "$label path is not a Git worktree: $path"
}

validate_worktree baseline "$BASELINE_WORKTREE"
validate_worktree reactor "$POC_WORKTREE"

mkdir -p "$BUILD_DIR"
readonly CANONICAL_BUILD_DIR="$(cd -- "$BUILD_DIR" && pwd -P)"
case "$CANONICAL_BUILD_DIR" in
    /|"$POC_WORKTREE"|"$SCRIPT_DIR")
        die "unsafe REACTOR_AB_BUILD_DIR: $CANONICAL_BUILD_DIR"
        ;;
esac

readonly BASELINE_COMMIT="$(git -C "$BASELINE_WORKTREE" rev-parse HEAD)"
readonly REACTOR_COMMIT="$(git -C "$POC_WORKTREE" rev-parse HEAD)"
[[ "$BASELINE_COMMIT" == "$EXPECTED_BASELINE_COMMIT" ]] ||
    die "baseline must be $EXPECTED_BASELINE_COMMIT, found $BASELINE_COMMIT"

if [[ -n "$(git -C "$BASELINE_WORKTREE" status --porcelain=v1 --untracked-files=all -- clients)" ]]; then
    die "baseline clients/ contains local changes; restore it to $EXPECTED_BASELINE_COMMIT before benchmarking"
fi

build_variant() {
    local label="$1"
    local worktree="$2"
    local output_dir="$BUILD_DIR/$label"

    rm -rf "$output_dir"
    mkdir -p "$output_dir/lib" "$output_dir/classes"
    printf 'Building %s client artifacts from %s\n' "$label" "$worktree"
    (
        cd -- "$worktree"
        ./gradlew --no-daemon -Dfile.encoding=UTF-8 \
            --init-script "$INIT_SCRIPT" \
            -PreactorAbLibDir="$output_dir/lib" \
            :clients:copyReactorAbRuntimeClasspath
    )

    local client_jar_count
    client_jar_count="$(find "$output_dir/lib" -maxdepth 1 -type f -name 'kafka-clients-*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' | wc -l | tr -d ' ')"
    [[ "$client_jar_count" == 1 ]] ||
        die "$label runtime directory must contain exactly one kafka-clients jar; found $client_jar_count"

    javac \
        -encoding UTF-8 \
        --release 11 \
        -classpath "$output_dir/lib/*" \
        -d "$output_dir/classes" \
        "$SCRIPT_DIR/IdleWakeHarness.java" \
        "$THROUGHPUT_HARNESS"
}

# Finish all compilation before any measurement begins.
build_variant baseline "$BASELINE_WORKTREE"
build_variant reactor "$POC_WORKTREE"

artifact_path() {
    local variant="$1"
    find "$BUILD_DIR/$variant/lib" -maxdepth 1 -type f -name 'kafka-clients-*.jar' \
        ! -name '*-sources.jar' ! -name '*-javadoc.jar'
}

sha256_file() {
    local path="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$path" | awk '{ print $1 }'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$path" | awk '{ print $1 }'
    else
        die "sha256sum or shasum is required to record artifact identity"
    fi
}

readonly BASELINE_ARTIFACT="$(artifact_path baseline)"
readonly REACTOR_ARTIFACT="$(artifact_path reactor)"
readonly BASELINE_SHA256="$(sha256_file "$BASELINE_ARTIFACT")"
readonly REACTOR_SHA256="$(sha256_file "$REACTOR_ARTIFACT")"

cat >"$BUILD_DIR/build-info.properties" <<EOF
baseline.worktree=$BASELINE_WORKTREE
baseline.commit=$BASELINE_COMMIT
baseline.clients.dirty=false
baseline.artifact=$(basename "$BASELINE_ARTIFACT")
baseline.artifact.sha256=$BASELINE_SHA256
baseline.consumer.config=group.protocol=consumer;enable.auto.commit=false;manual.assignment=true
reactor.worktree=$POC_WORKTREE
reactor.commit=$REACTOR_COMMIT
reactor.worktree.dirty=$(if [[ -n "$(git -C "$POC_WORKTREE" status --porcelain=v1 --untracked-files=no)" ]]; then printf true; else printf false; fi)
reactor.clients.dirty=$(if [[ -n "$(git -C "$POC_WORKTREE" status --porcelain=v1 --untracked-files=all -- clients)" ]]; then printf true; else printf false; fi)
reactor.artifact=$(basename "$REACTOR_ARTIFACT")
reactor.artifact.sha256=$REACTOR_SHA256
reactor.consumer.config=group.protocol=consumer;enable.auto.commit=false;manual.assignment=true
classic-reference.commit=$BASELINE_COMMIT
classic-reference.artifact=$(basename "$BASELINE_ARTIFACT")
classic-reference.artifact.sha256=$BASELINE_SHA256
classic-reference.consumer.config=group.protocol=classic;enable.auto.commit=false
java.version=$(java -version 2>&1 | head -n 1)
EOF

printf 'Prepared benchmark artifacts in %s\n' "$BUILD_DIR"
printf 'Baseline: %s\nReactor:  %s\n' "$BASELINE_COMMIT" "$REACTOR_COMMIT"
