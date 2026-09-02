<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements. See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Async Consumer Reactor A/B Benchmark

## Status

This is an experimental, repository-local benchmark for the async consumer reactor POC. It does not replace Kafka's
existing performance system tests. It closes a narrower evidence gap: compare the async consumer immediately before
the POC with the POC using the same broker, protocol, workload, JVM settings, and measurement code.

## Comparison boundary

The primary comparison is:

| Variant | Commit | Consumer selection |
| --- | --- | --- |
| baseline | `80a74f3b84` | `group.protocol=consumer` -> `AsyncKafkaConsumer` |
| reactor | POC commit under test | `group.protocol=consumer` -> `AsyncKafkaConsumer` |

`group.protocol=classic` is a secondary reference only. It creates `ClassicKafkaConsumer`, changes the group
protocol, and accepts a different configuration set. It is not a valid regression baseline for this refactor.

One binary cannot switch the POC behavior off. Build the baseline and reactor worktrees separately, then run their
client classpaths against one broker. Never compare two brokers or a classic consumer against the reactor POC and
attribute the difference to this refactor.

## Existing-tool limits

- `ConsumerPerformance` is usable for an initial saturated-throughput comparison. Read `fetch.nMsg.sec`, which
  excludes rebalance time, and retain the raw output. Its `poll(100 ms)` loop, lack of an explicit warmup phase, and
  throughput-only output make it unsuitable for the idle and lost-wakeup gates.
- Trogdor `ConsumeBench` uses `poll(50 ms)`. Its latency histogram records one batch turnaround value once per record
  in the batch, so its p99 is batch-size weighted. It is a cadence diagnostic, not the first-record latency gate.
- `EndToEndLatency` forces `fetch.max.wait.ms=0`, removing the wait interval changed by this POC.
- `ConsumerPerformanceService.settings` currently appends bare `key=value` tokens rather than
  `--command-property key=value`. Do not use that path to claim `group.protocol=consumer` coverage until it is fixed.
- Existing JMH benchmarks do not exercise the reactor/poll path. Add a reactor microbenchmark only if allocation or
  CPU profiles show a hot local reducer worth isolating.

## Required experiment protocol

1. Use one Linux host for formal results. macOS is a smoke-test environment only.
2. Pin the broker build and configuration. Only the client worktree changes.
3. Use the same topic data, partition count, record size, compression, fetch settings, heap, GC, CPU affinity, and
   security protocol.
4. Build both worktrees before measurement. Do not compile one variant while the other is running.
5. Use unique topic and group names per run. The idle harness creates and deletes its own topic.
6. Run A/A first. If the baseline compared with itself crosses a proposed gate, fix the harness or widen the gate
   before looking at A/B.
7. Run at least five independent JVMs per variant. Alternate order by repetition: `AB`, `BA`, `AB`, `BA`, `AB`.
8. Report every sample, median, median absolute deviation, exact two-sided Mann-Whitney p-value, commit, JVM, OS,
   broker configuration, and raw logs.
9. A threshold breach with insufficient statistical evidence is `INCONCLUSIVE`, not `PASS`.

## Workload matrix

| ID | Scenario | Primary metric | Purpose |
| --- | --- | --- | --- |
| W1 | Preloaded saturated consume, 1/8/64 partitions, 1 KiB records | `fetch.nMsg.sec`, CPU/record | Detect steady-state throughput regression. |
| W2 | 100 B, 1 KiB, 100 KiB records | bytes/sec, records/sec, allocation/record | Separate per-record and per-byte overhead. |
| W3 | 1/100/1,000 assigned partitions | throughput, allocation/record, reactor CPU | Expose intent creation and partition-scan cost. |
| W4 | Empty assigned partition with a long public poll | process CPU, JFR parks, network-poll metrics | Measure removal of periodic no-progress wakeups. |
| W5 | Consumer blocked, then one record arrives | p50/p95/p99 first-record latency | Detect lost wakeups or over-waiting. |
| W6 | All partitions paused | CPU, poll/wakeup count | Reproduce no-progress busy-loop conditions. |
| W7 | Slow application callback | queue high-water marks, network-poll cadence | Verify callback isolation and memory pressure. |
| W8 | Rebalance/coordinator churn | recovery p99, duplicate/failed operations | Exercise callback and membership handshakes. |
| W9 | Broker stop/restart during a groupless wait | recovery p99, CPU, request rate | Cover the POC's remaining real-socket gate. |
| W10 | Throttled producer cadence | delivery latency, CPU | Exercise event/deadline transitions between bursts. |

W1, W4, and W5 form the first implementation slice. W6-W10 are required before claiming the complete architecture
has no performance regression.

## Initial gates

| Gate | Failure threshold |
| --- | --- |
| Saturated throughput | Reactor median `fetch.nMsg.sec` is more than 3% lower and `p < 0.05`. |
| First-record latency | Reactor p99 median is worse by more than `max(10 ms, 10%)` and `p < 0.05`. |
| Idle CPU | Reactor median CPU percentage is more than 10% higher and `p < 0.05`. |
| Idle wake/park rate | Reactor rate is more than 10% higher and `p < 0.05`. |
| Allocation | Bytes/record is more than 5% higher; any accepted increase requires a profile and explanation. |
| Reconnect recovery | Reactor recovery p99 is more than 20% higher and `p < 0.05`. |
| Queue high-water mark | Reactor maximum is more than 2x baseline under identical admitted load. |

These are initial engineering gates, not universal Kafka release policy. Calibrate them only from A/A noise and
document any change before reading the A/B result.

## Quick start

The expected sibling worktrees are:

```text
kafka/
  trunk/
  reactor-benchmark-baseline/   # 80a74f3b84
  async-consumer-reactor-poc/   # POC
```

Build the two tool/client classpaths and compile the standalone public-API harness:

```shell
benchmarks/consumer-reactor-ab/prepare.sh
```

`prepare.sh` refuses a baseline other than the full commit `80a74f3b84525563ef060b6e0e1b70bc127ec064`
or a baseline with local changes under `clients/`. It copies each client's resolved runtime artifacts into an isolated
benchmark build directory and compiles the harness separately against each variant with `--release 11`. It does not
start or inspect a broker.

Run the idle and idle-to-first-record smoke matrix against an already running broker:

```shell
REPETITIONS=2 IDLE_DURATION_MS=5000 FIRST_RECORD_SAMPLES=10 \
  benchmarks/consumer-reactor-ab/run-idle.sh localhost:9092
```

For a formal run, use at least five repetitions, 60 seconds of idle measurement, 100 first-record samples, and an
idle interval which is longer than `retry.backoff.ms`:

```shell
REPETITIONS=5 IDLE_DURATION_MS=60000 FIRST_RECORD_SAMPLES=100 FIRST_RECORD_IDLE_MS=500 \
  benchmarks/consumer-reactor-ab/run-idle.sh localhost:9092
```

The runner prints the isolated result directory. Compare one metric with:

```shell
python3 benchmarks/consumer-reactor-ab/compare.py \
  --input RESULT_DIR/results.csv --scenario first-record --metric p99_ms \
  --direction lower --relative-threshold 10 --absolute-threshold 10
```

The CSV contains one row per variant/scenario/JVM. First-record raw logs additionally contain every measured sample as
`SAMPLE,first-record,INDEX,LATENCY_MS`; warmup samples are intentionally omitted. Idle rows expose process CPU,
application `poll` return rate, and the async consumer's average/max network-thread poll interval and derived rate.

`compare.py` reports each variant's median and median absolute deviation (MAD). It uses an exact two-sided
Mann-Whitney label-permutation test, including ties, while the number of label combinations is at most 200,000; larger
experiments use a tie-corrected normal approximation. Thresholds are direction-aware and the allowed regression is
`max(absolute threshold, baseline median * relative threshold)`:

| Verdict | Meaning | Exit code |
| --- | --- | --- |
| `PASS` | The reactor median does not breach the configured regression threshold. | 0 |
| `FAIL` | The threshold is breached and the two-sided p-value is below alpha. | 1 |
| `INCONCLUSIVE` | The threshold is breached without sufficient statistical evidence. | 2 |
| input error | CSV, metric, or arguments are invalid. | 3 |

Run the dependency-free statistical self-check with `python3 benchmarks/consumer-reactor-ab/compare.py --self-test`.

The runner only connects to the explicit bootstrap address. It never starts, stops, or reconfigures a broker. Each JVM
creates a UUID-suffixed one-partition topic, manually assigns it (so no group offsets are reused), and waits for topic
deletion before a successful exit. `FIRST_RECORD_IDLE_MS` is deliberate benchmark event scheduling, not a broker
readiness sleep.

Enable JFR for W4 when formal evidence is collected:

```shell
export REACTOR_AB_JAVA_OPTS='-XX:StartFlightRecording=settings=profile,dumponexit=true'
```

Keep the per-run recordings and count `jdk.ThreadPark` events for the application thread named
`consumer-reactor-ab-application`. JFR is diagnostic evidence; the public first-record and CPU gates remain the
portable acceptance checks.

## Jenkins Ducktape phase

The dedicated wrappers are:

```text
tests/kafkatest/tests/client/consumer_reactor_ab_test.py
```

`ConsumerReactorABTest` compiles and runs `IdleWakeHarness.java` against the client artifact from the current
checkout. It never checks out or measures another revision. Selecting the method without parameters is a short smoke
run:

```text
tests/kafkatest/tests/client/consumer_reactor_ab_test.py::ConsumerReactorABTest.test_current_revision
```

`ConsumerReactorPairedABTest` is the reproducible comparison path. One Ducktape service node fetches the pinned
baseline, builds both client artifacts before measurement, and runs `run-idle.sh` against one broker. Formal runs
alternate the order `AB`, `BA`, `AB`, `BA`, `AB`, so both variants share the same Jenkins allocation, host, Java
runtime, broker, and test configuration:

```text
tests/kafkatest/tests/client/consumer_reactor_ab_test.py::ConsumerReactorPairedABTest.test_same_worker --parameters '{"metadata_quorum":"COMBINED_KRAFT","reactor_ab_profile":"formal"}'
```

Single-revision formal runs inject the metadata quorum, profile, and logical variant. Paired runs inject only the
metadata quorum and profile. The profile expands to fixed workload values inside the test, keeping Ducktape's
parameter-derived result-directory names safely below filesystem limits. Jenkins `run_tests.sh` already
appends the single `--` separator consumed by `ducker-ak` before its Ducktape options. Therefore, a Jenkins
`TC_PATHS` value must put `--parameters` directly after the test selector and must not add another `--`:

```text
tests/kafkatest/tests/client/consumer_reactor_ab_test.py::ConsumerReactorABTest.test_current_revision --parameters '{"metadata_quorum":"COMBINED_KRAFT","reactor_ab_profile":"formal","reactor_ab_variant":"proposal"}'
```

The outer single quotes are part of the Jenkins `TC_PATHS` value. They preserve the JSON double quotes through
`run_tests.sh`, `ducker-ak`, and the inner `bash -c`, while Jenkins' `--max-parallel` remains a separate Ducktape
option. A direct `ducker-ak test` invocation outside `run_tests.sh` still uses `--` before Ducktape arguments, as
documented by `ducker-ak --help`.

For the baseline+harness revision, use:

```text
tests/kafkatest/tests/client/consumer_reactor_ab_test.py::ConsumerReactorABTest.test_current_revision --parameters '{"metadata_quorum":"COMBINED_KRAFT","reactor_ab_profile":"formal","reactor_ab_variant":"pre-refactor-async-baseline"}'
```

The wrapper rejects a formal profile without one of those two explicit variant names. Passing the bare file or class
is discouraged; the method selector above makes the Jenkins scope auditable. Revalidate this forwarding against the
live job before submission if its wrapper script changes.

The smoke workload itself is about 7 seconds, with roughly 2-5 minutes expected for broker startup and test overhead
after the repository build. A paired formal run has 1,150 seconds (19 minutes 10 seconds) of deliberate measurement
scheduling: five AB/BA repetitions, with 60 seconds of idle measurement and 110 first-record attempts at a 500 ms
interval for each variant. This leaves build and startup headroom under Jenkins' 30-minute Ducktape runner timeout.
The interval remains longer than the default retry backoff. The service wait budget is intentionally larger than the
outer runner timeout so an individual scenario still reports its own bounded failure when Jenkins allows it to finish.

The Jenkins job at `https://jenkins.opensource4you.tw/job/kafka-e2e/` checks out one `ACCOUNT`/`REVISION` per build.
Do not make a Ducktape test perform its own Git checkout. Put the same harness-only test change on a branch based on
`80a74f3b84`, and put the test on the POC branch. Submit two Jenkins builds with exact revisions:

| Build | `ACCOUNT` | `REVISION` | `TC_PATHS` | Variants emitted |
| --- | --- | --- | --- | --- |
| baseline | `unknowntpo` | exact baseline+harness commit | parameterized method path | pre-refactor async baseline |
| proposal | `unknowntpo` | exact POC+harness commit | parameterized method path | proposal |

Use a branch name only while iterating. Use the exact tested commit in retained evidence. Do not point `TC_PATHS` at
all of `tests/kafkatest/tests`: build 873 did that and took about 7 hours 8 minutes. Retain Jenkins `report.txt` and
`results.zip` with the raw client logs and the manifest described below.

Each test writes raw logs, `results.csv`, the broker configuration, and a machine-readable `manifest.json` into its
collected service result directory. The manifest records the one variant collected by that checkout and declares the
three-variant comparison contract. The post-Jenkins merge must produce one entry for each logical variant:

| Manifest variant | Commit | Artifact | Required distinguishing configuration |
| --- | --- | --- | --- |
| `legacy-classic-reference` | exact baseline+harness revision | client jar name and SHA-256 | `group.protocol=classic`, remaining consumer properties |
| `pre-refactor-async-baseline` | exact baseline+harness revision | client jar name and SHA-256 | `group.protocol=consumer`, remaining consumer properties |
| `proposal` | exact POC+harness revision | client jar name and SHA-256 | `group.protocol=consumer`, remaining consumer properties |

The wrapper also records the broker commit, core artifact, complete broker runtime jar set and SHA-256 values, broker
configuration and SHA-256, JVM/OS, workload parameters, execution order, Jenkins build URL/number when present, and
raw-log paths.
The runtime set intentionally includes the checkout's `kafka-clients` jar because `kafka-server-start.sh` puts it on
the broker classpath. The merge step must reject manifests whose broker runtime artifacts/configuration or workload
differs between the baseline and proposal builds. The current two-checkout Jenkins contract therefore reveals, but
does not by itself eliminate, broker-binary differences; pinning one broker build requires a later execution split.
`BUILD_URL` and `BUILD_NUMBER` remain null unless Jenkins forwards them into the existing Ducker container, so retain
the exact revision and manifest alongside the Jenkins build URL externally as the authoritative association.
The current public harness fixes `group.protocol=consumer`, so the wrapper cannot honestly emit the legacy Classic
reference yet; add an explicit group-protocol harness option before collecting that third variant. No Jenkins
submission is performed by these files.

## Throughput command

Preload one topic once, then use a unique group for every run. Run the same command from each worktree and retain the
last ten-field CSV row plus `/usr/bin/time -p` output:

```shell
bin/kafka-consumer-perf-test.sh \
  --bootstrap-server localhost:9092 \
  --topic reactor-ab-throughput \
  --group reactor-ab-UNIQUE \
  --num-records 10000000 \
  --timeout 60000 \
  --command-property group.protocol=consumer \
  --command-property enable.auto.commit=false \
  --print-metrics
```

Gate on field 10, `fetch.nMsg.sec`, not field 6, because field 10 removes group rebalance time. A duration-based
warmup/measurement throughput harness is still required before treating W1 as release-grade evidence.
