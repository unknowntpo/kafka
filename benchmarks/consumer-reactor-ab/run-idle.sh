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

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly BUILD_DIR="${REACTOR_AB_BUILD_DIR:-$SCRIPT_DIR/build}"
readonly BUILD_INFO="$BUILD_DIR/build-info.properties"
readonly REPETITIONS="${REPETITIONS:-5}"
readonly IDLE_DURATION_MS="${IDLE_DURATION_MS:-60000}"
readonly FIRST_RECORD_WARMUP_SAMPLES="${FIRST_RECORD_WARMUP_SAMPLES:-10}"
readonly FIRST_RECORD_SAMPLES="${FIRST_RECORD_SAMPLES:-100}"
readonly FIRST_RECORD_IDLE_MS="${FIRST_RECORD_IDLE_MS:-1000}"
readonly POLL_TIMEOUT_MS="${POLL_TIMEOUT_MS:-30000}"
readonly OUTPUT_ROOT="${REACTOR_AB_OUTPUT_ROOT:-$SCRIPT_DIR/results}"

die() {
    printf 'run-idle.sh: %s\n' "$*" >&2
    exit 1
}

positive_integer() {
    local name="$1"
    local value="$2"
    [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "$name must be a positive integer, found: $value"
}

[[ $# == 1 ]] || die "usage: run-idle.sh BOOTSTRAP_SERVERS"
readonly BOOTSTRAP_SERVERS="$1"
[[ -n "$BOOTSTRAP_SERVERS" ]] || die "BOOTSTRAP_SERVERS must not be empty"

positive_integer REPETITIONS "$REPETITIONS"
positive_integer IDLE_DURATION_MS "$IDLE_DURATION_MS"
positive_integer FIRST_RECORD_WARMUP_SAMPLES "$FIRST_RECORD_WARMUP_SAMPLES"
positive_integer FIRST_RECORD_SAMPLES "$FIRST_RECORD_SAMPLES"
positive_integer FIRST_RECORD_IDLE_MS "$FIRST_RECORD_IDLE_MS"
positive_integer POLL_TIMEOUT_MS "$POLL_TIMEOUT_MS"
[[ -f "$BUILD_INFO" ]] || die "missing $BUILD_INFO; run $SCRIPT_DIR/prepare.sh first"

property() {
    local name="$1"
    local value
    value="$(sed -n "s/^${name}=//p" "$BUILD_INFO")"
    [[ -n "$value" ]] || die "missing $name in $BUILD_INFO"
    printf '%s' "$value"
}

readonly BASELINE_COMMIT="$(property baseline.commit)"
readonly REACTOR_COMMIT="$(property reactor.commit)"
readonly RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-${RANDOM}"
readonly RESULT_DIR="$OUTPUT_ROOT/$RUN_ID"
readonly RESULTS_CSV="$RESULT_DIR/results.csv"

mkdir -p "$OUTPUT_ROOT"
mkdir "$RESULT_DIR"
mkdir "$RESULT_DIR/raw"
printf '%s\n' \
    'run_id,repetition,order,sequence,variant,commit,scenario,wall_ms,cpu_ms,cpu_percent,records,poll_calls,poll_calls_per_sec,p50_ms,p95_ms,p99_ms,network_poll_avg_ms,network_poll_max_ms,network_poll_rate_hz,raw_log' \
    >"$RESULTS_CSV"
cp "$BUILD_INFO" "$RESULT_DIR/build-info.properties"

if [[ -n "${REACTOR_AB_JAVA_OPTS:-}" ]]; then
    # Shell quoting inside REACTOR_AB_JAVA_OPTS is intentionally unsupported.
    read -r -a JAVA_OPTS <<<"$REACTOR_AB_JAVA_OPTS"
else
    JAVA_OPTS=()
fi

append_result() {
    local repetition="$1"
    local order="$2"
    local sequence="$3"
    local variant="$4"
    local commit="$5"
    local raw_log="$6"
    local result_line
    local result_count

    result_count="$(awk -F, '$1 == "RESULT" { count++ } END { print count + 0 }' "$raw_log")"
    [[ "$result_count" == 1 ]] ||
        die "expected one RESULT line in $raw_log, found $result_count"
    result_line="$(awk -F, '$1 == "RESULT" { print }' "$raw_log")"

    local marker scenario wall_ms cpu_ms cpu_percent records poll_calls poll_calls_per_sec
    local p50_ms p95_ms p99_ms network_avg network_max network_rate
    IFS=, read -r marker scenario wall_ms cpu_ms cpu_percent records poll_calls poll_calls_per_sec \
        p50_ms p95_ms p99_ms network_avg network_max network_rate <<<"$result_line"
    [[ "$marker" == RESULT ]] || die "malformed RESULT line in $raw_log"
    printf '%s,%d,%s,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"%s"\n' \
        "$RUN_ID" "$repetition" "$order" "$sequence" "$variant" "$commit" "$scenario" \
        "$wall_ms" "$cpu_ms" "$cpu_percent" "$records" "$poll_calls" "$poll_calls_per_sec" \
        "$p50_ms" "$p95_ms" "$p99_ms" "$network_avg" "$network_max" "$network_rate" \
        "$raw_log" >>"$RESULTS_CSV"
}

run_scenario() {
    local repetition="$1"
    local order="$2"
    local sequence="$3"
    local variant="$4"
    local scenario="$5"
    local commit
    local variant_dir="$BUILD_DIR/$variant"
    local raw_log="$RESULT_DIR/raw/rep-$(printf '%02d' "$repetition")-seq-${sequence}-${variant}-${scenario}.log"
    local scenario_args

    [[ -d "$variant_dir/classes" && -d "$variant_dir/lib" ]] ||
        die "missing prepared $variant artifacts; run $SCRIPT_DIR/prepare.sh"
    if [[ "$variant" == baseline ]]; then
        commit="$BASELINE_COMMIT"
    else
        commit="$REACTOR_COMMIT"
    fi
    if [[ "$scenario" == idle ]]; then
        scenario_args=(--duration-ms "$IDLE_DURATION_MS")
    else
        scenario_args=(
            --warmup-samples "$FIRST_RECORD_WARMUP_SAMPLES"
            --samples "$FIRST_RECORD_SAMPLES"
            --idle-ms "$FIRST_RECORD_IDLE_MS"
            --poll-timeout-ms "$POLL_TIMEOUT_MS"
        )
    fi

    printf 'repetition=%d order=%s sequence=%d variant=%s scenario=%s\n' \
        "$repetition" "$order" "$sequence" "$variant" "$scenario"
    java ${JAVA_OPTS[@]+"${JAVA_OPTS[@]}"} \
        -cp "$variant_dir/classes:$variant_dir/lib/*" \
        org.apache.kafka.tools.reactorbenchmark.IdleWakeHarness \
        --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --scenario "$scenario" \
        "${scenario_args[@]}" >"$raw_log" 2>&1 || {
            printf 'run-idle.sh: benchmark JVM failed; raw log: %s\n' "$raw_log" >&2
            return 1
        }
    append_result "$repetition" "$order" "$sequence" "$variant" "$commit" "$raw_log"
}

run_variant() {
    local repetition="$1"
    local order="$2"
    local sequence="$3"
    local variant="$4"
    run_scenario "$repetition" "$order" "$sequence" "$variant" idle
    run_scenario "$repetition" "$order" "$sequence" "$variant" first-record
}

for ((repetition = 1; repetition <= REPETITIONS; repetition++)); do
    if ((repetition % 2 == 1)); then
        order=AB
        variants=(baseline reactor)
    else
        order=BA
        variants=(reactor baseline)
    fi
    sequence=1
    for variant in "${variants[@]}"; do
        run_variant "$repetition" "$order" "$sequence" "$variant"
        ((sequence += 1))
    done
done

printf 'Benchmark complete: %s\n' "$RESULT_DIR"
printf 'Results CSV: %s\n' "$RESULTS_CSV"
