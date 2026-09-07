# Contract-Guided Coordination: local acceptance record

## Scope and identities

This is local evidence for the replacement-direction KIP, not upstream acceptance.
No remote branch, PR, Jenkins job, Jira issue or Confluence page was changed by this
completion task. The previous design and rollback checkpoint remain preserved.

- Functional implementation: `95095ac064`.
- Comment-only clarification: `37c6603a99`; after recompilation, every client class
  has the same SHA-256 as before that comment change.
- Performance baseline: `820533b870106cc0e0ac60e2076b8644d68bd85f`.
- Implementation branch: `codex/kip-1371-batched-decisions-poc`.
- Independent draft branch: `codex/kip-1371-contracts-docs`.
- Preserved rollback tag: `checkpoint/kip-1371-design-v0-2026-09-06`.
- Java: Temurin 17.0.8+7; Gradle 9.7.1; macOS Apple M1 Pro, 8 logical CPUs, 32 GiB RAM.

The complete draft is in the sibling DOC worktree at
`docs/design/kip-1371-contract-guided-coordination.md`, with matching local HTML.
The [original issue index](kip-1371-issue-coverage.md) and
[baseline provenance](kip-1371-historical-regression-provenance.md) separate inherited
repairs from new protection. The [existing-behavior ledger](kip-1371-approach2-compatibility-ledger.md)
records component/public-path boundaries rather than treating test counts as a proof
of every possible execution.

## Correctness receipts

| Selection at functional revision | Suites | Tests | Failures / errors / skips | Retry policy |
| --- | ---: | ---: | --- | --- |
| Consumer unit/component namespace | 97 | 3,383 | 0 / 0 / 0 | Disabled |
| Explicitly flaky consumer unit selection | 2 | 6 | 0 / 0 / 0 | Disabled |
| Consumer integration namespace | 21 | 419 | 0 / 0 / 0 | Disabled; fresh JVM per class |
| Selected Streams topology/protocol integration | 4 | 9 | 0 / 0 / 0 | Disabled; fresh JVM per class |

The selections are reported separately. Historical overlapping runs are not added
to these totals. XML suite attributes were reconciled with individual test cases;
there are no duplicate class/name pairs within each selection.
The source `*Test.java` inventory was also compared with reported suite names in
both consumer namespaces. No concrete annotated test class was missing. The four
unreported unit class names are abstract bases whose tests run through concrete
subclasses. This checks discovery coverage, not branch or interleaving coverage.

The integration namespace includes regular and SASL consumers, Share Consumer
delivery/acknowledgement/renewal/recovery paths, public callback-exception recovery,
and graceful close/broker-restart recovery. It is not the entire repository's test
suite or every Kafka Streams application-processing test.

The real close/restart test consumes offsets 0–49, observes position 50, closes,
requires the exact consumer background thread to terminate, checks committed offset
50 with Admin, gracefully restarts all brokers, and consumes 50–99 with a fresh
consumer. It does not simulate abrupt process death or power loss.

New public callback tests throw WakeupException and InterruptException from an
assignment callback in both KRaft configurations. They observe the error, retry the
public poll, and require successful reconciliation and real-record delivery. They
do not establish every possible OS-thread interruption schedule.

### Reproduction commands

Use the implementation checkout and JDK above. The two isolation init scripts set
`forkEvery=1` only on the named integration project. They are preserved with receipts.

```sh
gradle :clients:test --rerun --tests 'org.apache.kafka.clients.consumer.*' \
  :clients:clients-integration-tests:test --rerun \
  --tests 'org.apache.kafka.clients.consumer.*' \
  --init-script isolate-integration.gradle \
  --continue --offline --no-parallel --max-workers=2 \
  -PmaxParallelForks=1 -PmaxTestRetries=0 -PtestLoggingEvents=failed

gradle :clients:test --rerun --tests 'org.apache.kafka.clients.consumer.*' \
  -Pkafka.test.run.flaky=true --offline --no-parallel --max-workers=2 \
  -PmaxParallelForks=1 -PmaxTestRetries=0 -PtestLoggingEvents=failed

gradle :streams:integration-tests:test --rerun \
  --tests '*TopologyDescription*' --tests '*RebalanceProtocolMigrationIntegrationTest' \
  --init-script isolate-streams.gradle --no-scan --no-parallel --max-workers=2 \
  -PmaxParallelForks=1 -PmaxTestRetries=0 -PtestLoggingEvents=failed
```

The Streams run needed one missing logging dependency fetched before tests; no
build scan was published. A preceding offline dependency-resolution failure is not
a failing test or a passing receipt. Checkstyle, Spotless and production SpotBugs
checks passed during the functional acceptance sequence; the comment-only recompile
also passed main Checkstyle and Java Spotless checks.

### Retained negative evidence

- D2 has failing zero-initial-heartbeat-interval controls before its repair.
- D3 has five failing in-flight-heartbeat controls before its repair.
- All seven D2/D3 failing class/name pairs were matched to passing, non-skipped
  cases in the final full unit receipt. The final integration receipt also contains
  the real close/restart case, both callback types in two configurations each, and
  the inherited unknown-coordinator close regression.
- D1 invalidates the old expectation of callback-time offset recapture; the new
  assertion verifies retained offsets, refreshed identity and successful response,
  rather than deleting the scenario.
- An earlier plaintext suite had a topic-setup timeout and heap-exhaustion log.
  The isolated follow-up and later 419-case run passed. Its root cause is not
  established; the failed XML/log remains in the archive.
- An early callback fixture constructed InterruptException before setup and set
  the test thread's interrupt flag. Construction was moved into the actual callback;
  the failed fixture receipt remains, but is not a production defect.

## Decisions made under the user's autonomous-work authorization

| Decision | Choice and reason | Cost / limitation |
| --- | --- | --- |
| D1: pre-rebalance retry offsets | Retain the safely captured operation offsets; never recapture during concurrent application collection. | Can commit older offsets and increase replay. Does not make arbitrary asynchronous application processing at-least-once. |
| D2: blocked startup heartbeat | A zero initial interval does not force application spin while coordinator/membership blocks work. | Keep actual poll-timeout and eligible leave behavior; a positive wait alone is not a full liveness proof. |
| D3: in-flight heartbeat | Await its completion rather than emit empty immediate network polls. | Preserve a separate application poll-timer refresh bound across regular/share/Streams. |
| Callback execution | Keep inline owner responses as default; retain the batching experiment disabled. | Owner validation, scope and notification prerequisites remain mandatory. |
| Extra post-I/O pass | Retain the existing bounded conditional full pass; no speculative rewrite without a failing schedule. | Additional work/captured-admission timing; not claimed as a throughput optimization. |
| Compatibility scope | Keep raw-delay adapters and the existing synchronized ShareFetchBuffer path explicitly visible. | Narrow regular/Streams fetch capabilities are not a universal compile-time restriction. |
| Review history | Preserve original ordered migration commits and later phase-labelled amendments. | This is a reviewable POC history, not a rebased production-ready patch series. |

## Performance evidence

Methods and thresholds are in `benchmarks/contract-guided/README.md`. Measurements
use the shipped ConsumerPerformance class, `group.protocol=consumer`, exact record
counts, five alternating AB/BA pairs, fixed payload and a task-owned loopback broker.
Desktop/VM processes were not stopped; this is a non-exclusive host.
The baseline is an ancestor of the candidate. The measured revisions have no diff
in ConsumerPerformance/tool main sources, broker core main sources, `build.gradle`
or `gradle/dependencies.gradle`; classpath content hashes are recorded for both arms.

The initial 50-million-record NOP-logging run completed all timing and idle/latency
pairs, but fetch intervals were 23.697–26.438 seconds, below the unchanged 30-second
minimum. Throughput median was 2,054,062.94 -> 1,979,022.36 records/s (-3.65%);
whole-process CPU was 15.09 -> 16.51 seconds (+9.41%). Idle CPU rose by 0.146029
percentage points and median per-JVM first-record p99 by 3.256 ms. These are costs,
not improvements, and short throughput is **inconclusive**, not accepted.

Its final diagnostic parser failed on a legal partial-target final-poll overshoot.
The corrected parser permits that bounded diagnostic overshoot while preserving
exact full-dataset acceptance. Nine parser/performance-event tests pass.
Normal release logging jars are now included in both runtimes. Subsequent normal-
logging measurements must not be pooled with the earlier NOP-logging results.

### Completed normal-logging run

Receipt: `/private/tmp/kip1371-throughput-sfcdl1ks/`. The script exited successfully;
all ten timing JVMs consumed exactly 70,000,000 records, with 33.936–35.916 seconds
of fetching each. All five idle/first-record pairs and both separate profiling
invocations completed. Each profile also consumed exactly 70,000,000 records.

| Metric | Baseline median (MAD) | Candidate median (MAD) | Difference / predeclared result |
| --- | --- | --- | --- |
| Fetch records/s | 2,041,351.96 (18,639.79) | 2,001,544.05 (14,701.13) | -1.95%; passes minimum ratio 0.95 and duration/pair gates |
| Whole-process CPU seconds | 20.73 (0.37) | 22.12 (0.31) | +6.71%; descriptive cost, no CPU-time gate was declared |
| Peak process RSS bytes | 1,119,191,040 (3,670,016) | 1,116,864,512 (4,096,000) | Similar footprint; descriptive, not proof of a memory improvement |
| Idle process CPU, % of one core | 2.411986 (0.042897) | 2.492712 (0.113390) | +0.080726 percentage points; passes +0.2 allowance |
| Per-JVM first-record p99, ms | 18.860 (0.142) | 17.045 (0.422) | -1.815 ms; passes +10 ms allowance |

Throughput candidate/baseline paired ratios in execution-pair order are
0.995963, 0.987701, 0.982846, 0.984765 and 0.978469. Every pair is a little slower;
do not describe passing the 5% allowance as "no regression" or "faster overall."
First-record latency includes producer/broker/network time and uses manual assignment
with auto-commit disabled. Healthy throughput uses group subscription and auto-commit
enabled. These are different workloads, not a single combined performance claim.
The average-derived network-poll frequency remains an estimate in the raw harness
results, not an exact event count or a separate acceptance gate.

This establishes the predeclared **local workload gates**, not statistical equivalence,
dedicated-worker acceptance, all consumer variants or fault-workload performance.
No old Centralized Coordination benchmark percentage is reused here.

JFR diagnostics are separate invocations from timing. The performance-only configuration
disables environment variables and unrelated process metadata. Early default-profile
raw recordings remain private and are excluded from the evidence bundle; only explicitly
allowlisted CPU/allocation/GC/lock extracts may be included. Allocation weights and CPU
stack samples are diagnostic estimates over the whole JVM, not exact per-record counters.
The normal-logging recordings contain 261/203 Java execution samples and 9,882/10,267
allocation samples (baseline/candidate). Most sampled Java execution leaves are
CompletedFetch record traversal/parsing. This does not identify the cause of the
CPU-time increase or justify attributing it to the post-I/O pass.

Summed allocation sample weights are 58,353,679,736 / 58,602,967,832 bytes, or
833.624 / 837.185 sampled-weight bytes divided by each consumed record. These include
startup/close and are not exact record-attributed allocations. GC pause sums are
84.203 / 88.935 ms. The raw performance-only JFRs, allowlisted JSON and Java folded
stacks are retained for deeper inspection; profiling throughput is not pooled into
the acceptance sample set. Sensitive environment/system-process event counts were
verified zero in both recordings, and runner/JFC hashes match their manifest.

The owned broker PID 9647 exited. Only its validated, regenerable 17.92 GB broker-data
directory was removed after shutdown; all logs and measurements remain. No existing
user broker, topic or process was stopped or deleted.

## Limits and release boundary

The durable [evidence bundle](/Users/unknowntpo/.codex/visualizations/2026/08/28/01a045cc-5836-7f92-a800-9fc27bd770ff/kip-1371-acceptance-95095ac064.tar.gz)
contains document snapshots, passing and failing XML/HTML receipts, exact measurement
commands/configuration, runtime fingerprints, safe profiles and folded stacks, plus
a SHA-256 inventory. Generated broker payload and early default-profile raw JFRs are
excluded. The expanded bundle remains beside the archive, outside Gradle's cleanable
build directories. Files are local only; sharing requires a separate decision.

All original issue IDs remain indexed, with current protection and executable evidence.
This does not mean every historical pre-fix binary was rerun, or that controlled transport
schedules are real-broker reproductions of the exact historical fault. Inherited repairs
are not claimed as new fixes. The broader current regression selections pass, but arbitrary
new RM behavior still requires an explicit owner/admission/observation/effect contract.

The new draft deliberately weakens the original universal publication barrier to necessary
effect-specific ordering, and does not impose a physical single writer on every legacy
SubscriptionState field. A final upstream proposal must review the retained-offset policy
and these scope differences explicitly. No publication is authorized by this record.
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
