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
readonly WARMUP_RECORDS="${THROUGHPUT_WARMUP_RECORDS:-500000}"
readonly MEASUREMENT_RECORDS="${THROUGHPUT_MEASUREMENT_RECORDS:-5000000}"
readonly FETCH_TIMEOUT_MS="${THROUGHPUT_FETCH_TIMEOUT_MS:-60000}"
readonly OUTPUT_ROOT="${REACTOR_AB_OUTPUT_ROOT:-$SCRIPT_DIR/results}"

die() {
    printf 'run-throughput.sh: %s\n' "$*" >&2
    exit 1
}

positive_integer() {
    local name="$1"
    local value="$2"
    [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "$name must be a positive integer, found: $value"
}

[[ $# == 2 ]] || die "usage: run-throughput.sh BOOTSTRAP_SERVERS TOPIC"
readonly BOOTSTRAP_SERVERS="$1"
readonly TOPIC="$2"
[[ -n "$BOOTSTRAP_SERVERS" ]] || die "BOOTSTRAP_SERVERS must not be empty"
[[ -n "$TOPIC" ]] || die "TOPIC must not be empty"
positive_integer REPETITIONS "$REPETITIONS"
positive_integer THROUGHPUT_WARMUP_RECORDS "$WARMUP_RECORDS"
positive_integer THROUGHPUT_MEASUREMENT_RECORDS "$MEASUREMENT_RECORDS"
positive_integer THROUGHPUT_FETCH_TIMEOUT_MS "$FETCH_TIMEOUT_MS"
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
    'run_id,repetition,order,sequence,variant,commit,scenario,wall_ms,cpu_ms,cpu_percent,records,bytes,records_per_sec,mb_per_sec,cpu_ns_per_record,network_poll_avg_ms,network_poll_max_ms,max_rss_kb,raw_log' \
    >"$RESULTS_CSV"
cp "$BUILD_INFO" "$RESULT_DIR/build-info.properties"

run_throughput() {
    local variant="$1"
    local output="$2"
    local time_output="$3"
    local variant_dir="$BUILD_DIR/$variant"

    [[ -d "$variant_dir/lib" ]] || die "missing prepared $variant artifacts"
    /usr/bin/time -f 'TIME,max_rss_kb=%M' -o "$time_output" \
        java -Xms512m -Xmx512m \
        -cp "$variant_dir/classes:$variant_dir/lib/*" \
        org.apache.kafka.tools.reactorbenchmark.ThroughputHarness \
        --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --topic "$TOPIC" \
        --warmup-records "$WARMUP_RECORDS" \
        --measurement-records "$MEASUREMENT_RECORDS" \
        --timeout-ms "$FETCH_TIMEOUT_MS" \
        >"$output" 2>&1
}

append_result() {
    local repetition="$1"
    local order="$2"
    local sequence="$3"
    local variant="$4"
    local commit="$5"
    local raw_log="$6"
    local time_log="$7"
    local result_line
    local time_line

    result_line="$(awk -F, '$1 == "RESULT" { line=$0 } END { print line }' "$raw_log")"
    [[ -n "$result_line" ]] || die "no ThroughputHarness result row in $raw_log"
    time_line="$(sed -n 's/^TIME,//p' "$time_log")"
    [[ -n "$time_line" ]] || die "no time result in $time_log"

    local marker scenario wall_ms cpu_ms cpu_percent records bytes records_per_sec
    local mb_per_sec cpu_ns_per_record network_poll_avg_ms network_poll_max_ms
    IFS=, read -r marker scenario wall_ms cpu_ms cpu_percent records bytes records_per_sec \
        mb_per_sec cpu_ns_per_record network_poll_avg_ms network_poll_max_ms <<<"$result_line"
    [[ "$marker" == RESULT && "$scenario" == throughput ]] ||
        die "malformed ThroughputHarness result row in $raw_log"
    (( records >= MEASUREMENT_RECORDS )) ||
        die "expected at least $MEASUREMENT_RECORDS records in $raw_log, found $records"

    local max_rss_kb
    max_rss_kb="$(sed -n 's/.*max_rss_kb=\([^,]*\).*/\1/p' <<<"$time_line")"

    printf '%s,%d,%s,%d,%s,%s,throughput,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"%s"\n' \
        "$RUN_ID" "$repetition" "$order" "$sequence" "$variant" "$commit" \
        "$wall_ms" "$cpu_ms" "$cpu_percent" "$records" "$bytes" "$records_per_sec" \
        "$mb_per_sec" "$cpu_ns_per_record" "$network_poll_avg_ms" \
        "$network_poll_max_ms" "$max_rss_kb" "$raw_log" >>"$RESULTS_CSV"
}

run_variant() {
    local repetition="$1"
    local order="$2"
    local sequence="$3"
    local variant="$4"
    local commit
    local prefix="rep-$(printf '%02d' "$repetition")-seq-${sequence}-${variant}"
    local measurement_log="$RESULT_DIR/raw/${prefix}-throughput.log"
    local time_log="$RESULT_DIR/raw/${prefix}-time.log"

    if [[ "$variant" == baseline ]]; then
        commit="$BASELINE_COMMIT"
    else
        commit="$REACTOR_COMMIT"
    fi
    printf 'repetition=%d order=%s sequence=%d variant=%s scenario=throughput\n' \
        "$repetition" "$order" "$sequence" "$variant"
    run_throughput "$variant" "$measurement_log" "$time_log"
    append_result "$repetition" "$order" "$sequence" "$variant" "$commit" \
        "$measurement_log" "$time_log"
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
