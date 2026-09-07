# Contract-Guided Coordination: completion work log

## Authority and preservation

The user authorized autonomous implementation, testing, and design decisions on 2026-09-06 while unavailable.
Complete a local KIP draft with explicit evidence and limitations. Do not publish to upstream, Jira, or Confluence.
This work also keeps new commits local: no remote pushes or new CI submissions without a subsequent request.
Preserve the original KIP and independent worktrees. Starting revision: `45e9cc7275`.
Rollback checkpoint: `checkpoint/kip-1371-design-v0-2026-09-06`.

## Acceptance method

1. Read the entire original KIP and trace every named issue to its actual failure shape and regression evidence.
2. Inventory existing consumer behaviors, including lifecycle, callback acknowledgement, regular/share/Streams
   compositions, and distinguish inherited repairs from new POC mechanisms.
3. Resolve demonstrated correctness gaps with the smallest justified mechanism. Record behavioral tradeoffs;
   neither callback centralization nor an extra manager pass is a correctness proof by itself.
4. Run deterministic regressions without automatic retries, then real isolated broker integration tests.
5. Finish the local KIP Markdown and matching HTML preview, linking pinned receipts and explicit open gates.

Passing the existing suite is regression evidence, not proof of every possible interleaving. Historical benchmark
results for another design/revision are not acceptance evidence for this implementation.

## Receipts

### Full consumer unit namespace at `45e9cc7275`

- JDK: Temurin 17. Gradle: cached 9.7.1. Offline, two workers, one test fork, retries disabled.
- Command: `:clients:test --rerun --tests 'org.apache.kafka.clients.consumer.*' :clients:spotlessJavaCheck`
  with `-PmaxParallelForks=1 -PmaxTestRetries=0 -PtestLoggingEvents=failed`.
- Result: **97 suites, 3,392 tests, zero failures/errors/skips**; build successful in 3m 12s.
- XML source: `clients/build/test-results/test/TEST-*.xml` (ephemeral; subsequent test runs can overwrite it).
- Scope: existing consumer unit/component namespace, including the POC tests. Not a real-broker suite.
- Raw XML archived at `/tmp/kip1371-overnight-evidence.TC1I8d/unit-baseline-45e9/` before later reruns.

### Broad plaintext integration invocation at `45e9cc7275`

Selection: `:clients:clients-integration-tests:test --rerun --tests 'org.apache.kafka.clients.consumer.PlaintextConsumer*'`;
same JDK, offline cache, one fork and zero retries. Production classes were compiled before the snapshot-policy edit.

Result: **236 tests across eight suites: 235 passed, one failed, zero errors/skips**, 16m 51s.
Raw XML archived at `/tmp/kip1371-overnight-evidence.TC1I8d/plaintext-baseline-45e9/`.

Failure: `PlaintextConsumerSubscriptionTest.testAsyncConsumerPatternSubscription()[2]`
timed out in `setup()` while awaiting topic metadata. Its test log contains `OutOfMemoryError: Java heap space`
in `kafka-admin-client-thread | adminclient-162`, allocating a network receive buffer. The build config uses a
3 GiB test heap. This is before that case's consumer assertions, but its deeper cause is **not established**:
do not conclude either a consumer regression or harmless infrastructure flakiness from the stack alone.
Preserve this failure even if isolated reruns pass. Further isolation should use fresh test JVMs per class and
the same heap rather than quietly raising memory or enabling retry.
Raw failure log: `/tmp/kip1371-overnight-evidence.TC1I8d/plaintext-subscription-heap-failure.log`.

## Initial work plan (historical; later receipts supersede it)

- Original 1,192-line KIP fully read, including migration targets, lifecycle issues and historical evidence caveats.
- Next: inherited lifecycle regression provenance and real integration coverage; safe rebalance snapshot retry
  decision; complete candidate KIP and evidence index.

## Decision D1: retain admitted pre-rebalance commit offsets

The candidate removes callback-time recapture and the default-off experiment toggle.
`OffsetCommitRequestState.offsets` is now a final reference. Initial safe capture remains
the application-poll checkpoint; identity, coordinator and retry deadlines retain existing rules.
This sacrifices retry freshness to avoid committing not-yet-returned records without adding
another queue or delaying retries until the next application poll. See the updated snapshot audit.
The legacy two-mode controls remain at starting revision `45e9cc7275`; current tests must prove
the selected behavior with normal construction. Verification is pending; no passing receipt is
claimed yet for this change.

D1 completed locally in commit `0829a57bfa`: 415 affected unit/component tests passed twice.
No push. See the snapshot audit for counts and raw receipt paths.

## Decision D2: do not interpret the initial heartbeat interval as useful urgency

New deterministic regular and Streams tests both failed at the unchanged heartbeat implementation:
unknown coordinator + initial heartbeat interval zero still returned application wait zero.
The earlier KAFKA-20253 test used a negotiated positive interval and did not cover startup.
Raw red receipt: `/tmp/kip1371-overnight-evidence.TC1I8d/zero-heartbeat-red/` (417 tests, exactly these two failed).

Candidate correction: mirror heartbeat admission guards in Streams application waiting and, when blocked
with interval zero, retain a positive half-remaining poll-timer refresh bound (minimum 1 ms while live).
Preserve existing negotiated-interval and actual poll-timer-expiry behavior. This adds no timer setting
or global scheduler policy. Shared regular/share tests cover the rounding boundary; discovery recovery
must still produce a real heartbeat request. Validation pending.

## Callback integration fixture correction

The first fresh-JVM follow-up ran six integration cases: two wakeup-callback cases and both previously
failed pattern-subscription configurations passed. Two new interruption cases failed in setup because
constructing `InterruptException` itself sets the calling thread's interrupt flag. This was a test
construction error before topic creation, not consumer behavior. The fixture now constructs the exception
inside the intended callback and explicitly handles/clears interruption before choosing another poll.
Raw receipt: `/tmp/kip1371-overnight-evidence.TC1I8d/callback-fixture-construction-error/`.
The temporary Gradle isolation script also needed `findProject` rather than `project` to tolerate included
builds; the initial script attempt ran no tests. Neither failed invocation is counted as a passing run.

## Local document preview

Complete narrative draft created separately at `kip-1371-contracts-docs/docs/design/kip-1371-contract-guided-coordination.md`.
Pandoc generated a self-contained UTF-8 HTML preview. Plain-text round trips of Markdown and HTML are identical;
there are no embedded old diagrams or external assets. Browser visual inspection is pending: browser-harness
requires the user's remote-debugging approval. The waiting CLI was cancelled, without repeated prompts or
changing browser settings. This does not block code/tests or static document checks.

## Checkpoints and full regression

- `0829a57bfa`: selected safe snapshot policy; 415 affected tests passed twice.
- `6588fa8c4a`: zero-initial-interval heartbeat wait repair; 362 tests passed twice.
- `074cb9dc07`: real callback acknowledgement/recovery proof; four integration cases passed twice.
- DOC branch `3498de3758`: complete replacement narrative and identical generated local HTML.

Full consumer unit and integration namespaces are now running at `074cb9dc07`, offline,
retries disabled, one test fork, with a fresh JVM per integration class. This replaces neither
the earlier failed plaintext receipt nor the need to inspect each failure independently.
Do not edit compiled inputs or run another Gradle invocation against this worktree while that
build is compiling. Preserve reports before any later run overwrites them.

Next independently scoped check: an already in-flight heartbeat with an expired interval.
Existing tests assert no duplicate request but do not necessarily assert a nonzero wait.
Inspect and reproduce that branch before declaring activation coverage complete.

## Full-run finding and decision D3

The full unit run at `074cb9dc07` ran 3,375 cases, with one failure and no automatic retry.
It stopped before integration. The failing BEFORE_REBALANCE / COORDINATOR_LOAD_IN_PROGRESS
case still asserted legacy callback recapture: assignment loss made the recaptured map empty,
so the operation completed without sending anything. D1 intentionally removes that behavior.
The assertion now proves the stronger selected outcome: retry captured offset 10 with current
epoch -1, receive success, then complete. It does not cancel an admitted commit on assignment loss
or pretend that broker rejection cannot occur. Other fatal errors still fail the operation.
Raw reports: `/tmp/kip1371-overnight-evidence.TC1I8d/full-unit-074cb9-failure/`.

Five new regular/share/Streams in-flight heartbeat cases failed before D3: request admission
correctly refused duplicates, but the expired interval still returned zero network wait.
D3 returns `AwaitInput(NETWORK_COMPLETION)` for that blocked result. Application waiting retains
a positive negotiated-interval/poll-timer refresh bound; actual poll timeout and urgent leave
remain actionable. No global scheduler, new queue, or public configuration is introduced.
Raw red reports: `/tmp/kip1371-overnight-evidence.TC1I8d/inflight-heartbeat-red/`.
Two old Streams assertions expected a finite timer while in flight; these now check the typed
completion condition. Their intervening failed receipt is retained separately.

The selected suite now passes, including three new configured-loop cases: in-flight wait and
recovery with/without the extra pass, plus manual-assignment no-spin and membership recovery.
Those use real managers and a real delegate with mocked membership/transport, not real brokers.
Raw first green: `/tmp/kip1371-overnight-evidence.TC1I8d/inflight-first-green/`.

### Full unit receipt at `95095ac064`

The full consumer unit/component namespace passed: **97 suites, 3,383 cases,
zero failures/errors/skips**, retries disabled. Raw XML is archived in
`/tmp/kip1371-overnight-evidence.TC1I8d/full-unit-95095ac/`.
This reruns all 443 selected heartbeat/loop cases, providing their second passing execution.
The count differs from the initial 3,392 because obsolete policy-toggle combinations were
removed and selected-policy cases added; counts from the different revisions must not be summed.
The default Kafka discovery filter excludes explicitly flaky tests before execution; zero XML
skips does not mean those excluded cases ran. A separate flaky selection remains to be run.
The real consumer integration namespace is now executing on the same pinned revision with
fresh test JVMs per class. Streams application-level tests are a separate module and require
an explicit follow-up; their manager-unit coverage must not be described as that integration run.

## Local performance preparation (not a result)

Created sibling baseline worktree `kip-1371-acceptance-baseline` on
`codex/kip-1371-acceptance-baseline`, pinned to `820533b870106cc0e0ac60e2076b8644d68bd85f`,
using the guarded worktree alias. It is clean and correctly anchored in `trunk/.git`.
No old worktree was moved or modified. Compilation/measurement are not yet complete.

`benchmarks/contract-guided/` contains a runtime exporter and a task-owned loopback-broker
runner for the shipped ConsumerPerformance CLI. Five parser tests passed; real fixture smoke
must pass before full measurement. The predeclared full workload and thresholds are in its README.
The runner fingerprints runtime content, rejects dirty/mismatched measured source and short reads,
uses independent AB/BA JVMs, and retains raw results. A separate JFR may be captured after timing.
It never accepts an existing broker address or submits a Jenkins job.

Next execution order after the current broad client integration run:

1. Archive both result sets and diagnose any failed case; do not overwrite or retry away a failure.
2. Run the explicitly flaky consumer unit selection and Streams topology/protocol migration integration
   tests in their own module, with no automatic retries. These are separate from the main namespace.
3. Export baseline/candidate runtime classpaths offline. Run the 10,000-record, one-pair fixture smoke
   (inconclusive for performance), then the predeclared full profile if smoke is valid.
4. Finalize local KIP evidence, archive raw receipts in a durable workspace location, and regenerate HTML.
5. Commit locally and report actual coverage and limits. No remote publication or push.

### Complete real-consumer integration receipt at `95095ac064`

The combined unit/integration command finished successfully in 56m48s. The integration
namespace reports **419 tests, 21 suites, zero failures/errors/skips**, retries disabled,
fresh test JVM per class. Raw XML and HTML are archived at
`/tmp/kip1371-overnight-evidence.TC1I8d/full-integration-95095ac/` and
`/tmp/kip1371-overnight-evidence.TC1I8d/full-integration-html-95095ac/`.
The passing set includes exact background-thread shutdown plus broker restart/resumed offsets,
the inherited unknown-coordinator close failure, four new callback recovery cases, regular/SASL
consumer flows and Share Consumer callback, delivery, DLQ, lag, rack, renewal and recovery tests.
The previous shared-JVM setup/heap failure did not recur in this invocation; that observation
does not establish its root cause or authorize erasing the failed receipt.

The baseline runtime export has completed successfully. The common idle harness was copied
unchanged and compiled against the baseline with Java 11 bytecode; source SHA-256:
`d4cd92bd6df4643befaa4a27c14adfbbada81a9e4ccf566e885926ad3801a070`.
Benchmark preparation now verifies the broker cluster ID before topic writes, excludes only
absent Gradle resource outputs (not missing jars/classes), and supports separate baseline/candidate
JFR diagnostics. These are preparation checks, not measured performance results.

The explicit `kafka.test.run.flaky=true` consumer selection passed **six tests in two
suites**, no failures/errors/skips or retries; raw receipt `flaky-unit-95095ac/` in the
same evidence directory. The subsequent runtime export failed on the Scala-only core
module's absent Java output, not on a test. The exporter now omits only outputs whose
Java/resource producer demonstrably has no source, and still rejects unexpected missing
classes/jars. Real smoke validation remains required. Streams protocol/topology integration
is now running with its own per-class JVM isolation.

### Streams supplement and runtime preparation completed

The first Streams invocation stopped before testing because offline Gradle lacked
`org.apache.logging.log4j:log4j-1.2-api:2.25.5`. Dependencies were then resolved with
`--no-scan` (no build-scan publication), not by changing test assertions or retrying failures.
The selected `*TopologyDescription*` and `*RebalanceProtocolMigrationIntegrationTest`
tests passed: **nine cases in four suites**, zero failures/errors/skips and retries,
in 3m23s including preparation. XML/HTML receipts are in `streams-integration-95095ac/`
and `streams-integration-html-95095ac/`. This is the relevant protocol/topology supplement,
not the full Streams application-processing suite. Candidate/broker runtime export also passed.

The local measurement host is an Apple M1 Pro, eight logical CPUs and 32 GiB RAM.
It is not exclusive: desktop/VM processes remain active and were not stopped. The benchmark
manifest records this limitation, platform/JDK, load average, runtime and runner hashes.
No old benchmark percentage is reused as current-candidate evidence.

### Real fixture smokes and bounded throughput measurement

The task-owned broker fixture succeeded twice, including exact seed/end-offset/consumed counts,
cluster identity, cleanup and separate JFR invocations. Receipts:
`/private/tmp/kip1371-throughput-kxyfahgw/` (10,000 records) and
`/private/tmp/kip1371-throughput-gsrzcjz_/` (1,000,000 records). Both are short/inconclusive.

A proposed 200,000,000-record / 51.2 GB run was rejected by the safety reviewer for excessive
shared-desktop resource impact; it did not start and was not bypassed. The approved alternative
uses the original 50,000,000-record / 12.8 GB dataset, with unchanged five pairs and acceptance
thresholds. Receipt: `/private/tmp/kip1371-throughput-birb15ka/`; its pinned `runner.py` copy
matches the manifest SHA-256 even though later profiling-privacy improvements are being prepared.

All ten throughput JVMs consumed exactly 50,000,000 records. Median fetch throughput is
2,054,062.94 -> 1,979,022.36 records/s (**-3.65%**), MAD 35,736.53 -> 17,782.75.
Whole-process CPU median is 15.09 -> 16.51 seconds (**+9.41%**); peak RSS median is
1,111,392,256 -> 1,126,858,752 bytes. These costs must be disclosed, not hidden by the idle gate.
Each fetch interval was only 23.697–26.438 seconds, below the predeclared 30-second minimum;
the throughput gate is therefore **inconclusive-short**, not pass. The separate idle/first-record
workloads subsequently completed all five pairs.

The JDK's built-in profile configuration also records environment variables and unrelated process
metadata. No such values were printed or published. A performance-only `profile.jfc` now disables
those events for future diagnostics. Early default-profile recordings must remain private and
must not be put into the evidence bundle; only allowlisted performance-event extracts may be shared.

### Measurement corrections and completion of the first local run

The 50-million-record run's idle CPU median was 2.339168% -> 2.485197%
(+0.146029 percentage points; MAD 0.033110 -> 0.021262). Median per-JVM
first-record p99 was 17.463 -> 20.719 ms (+3.256 ms; MAD 0.386 -> 1.185).
Both are inside their predeclared allowances, not evidence of improvement.
First-record latency is producer-send-to-consume end-to-end, not consumer-only time.

The script finally failed only in its diagnostic profile parser: ConsumerPerformance
requested 5,000,000 records but finished its last poll batch at 5,000,214. This is
legal for a partial-dataset target. The corrected parser permits at most 499 excess
records only for partial profiling; the ten complete-throughput counts remain exact.
Nine parser/export tests pass. Baseline profiling completed; candidate profiling did
not start. Preserve that incomplete diagnostic receipt rather than inventing a pair.

The initial runtime exporter omitted distribution `releaseOnly` logging jars, so
these measurements used the same NOP logger in both arms. The exporter now includes
the normal release logging implementation; both resolved exports succeed offline.
NOP-runtime results stay separate from subsequent release-logging measurements.

`37c6603a99` clarifies two comment lines without changing behavior. After recompilation,
SHA-256 manifests of every client class are byte-for-byte identical to the pre-comment
`95095ac064` classes. Checkstyle and Spotless also pass. Test receipts remain pinned to
the functional revision rather than pretending that a new full test invocation occurred.

The performance-only JFC probe contains zero initial-environment, system-property,
system-process, JVM-information or process-start events. Safe allowlisted extracts of
the earlier recordings were generated separately; raw default JFRs remain private.
The task-owned 50-million-record broker was stopped, its unique topic deleted, and its
validated 12.8 GB generated broker-data directory removed after shutdown. All reports
remain. The next bounded run uses 70,000,000 records (17.92 GB) with unchanged gates,
3 GB combined broker/client heap cap and 145 GiB initially free; safety review allowed it.
It uses standard release logging and profiles the full dataset separately from timing.
Receipt: `/private/tmp/kip1371-throughput-sfcdl1ks/`.

### No speculative scheduling rewrite

Source inspection considered whether a previous zero application-wait cache can remain
visible during a blocking network poll. The baseline also publishes this cache after
network I/O; this inspection did not establish a new regression or its real public-poll
trigger. Do not label D2/D3 a proof against every transient scheduling interleaving, or
remove the post-I/O pass based on a conjectured CPU cause. The selected contract and
tested implementation remain unchanged. A future change needs a failing production-path
schedule and its own correctness/performance receipts.

### Normal-logging acceptance complete

The bounded 70-million-record run finished successfully with all ten throughput
counts exact, five complete idle/latency pairs, and two complete full-dataset profiles.
All fetch intervals exceed 30 seconds. Median throughput is 2,041,351.96 ->
2,001,544.05 records/s (-1.95%, inside the unchanged 5% allowance). Whole-process
CPU rises 20.73 -> 22.12 seconds (+6.71%); this cost is disclosed, not called an
improvement. Idle CPU increases 0.080726 percentage points, within +0.2; median
per-JVM first-record p99 decreases 1.815 ms, within +10 ms. Full MADs, paired ratios,
profiles, scope limits and commands are in [local acceptance](kip-1371-local-acceptance.md).

Both profiles consumed exactly 70,000,000 records. JFR summaries verify zero
environment/system-property/system-process/JVM-information/process-start events.
Their allowlisted JSON and Java folded stacks were generated successfully. Most Java
execution samples concern record parsing/traversal; the profiles do not establish
the cause of the measured CPU increase. No speculative production rewrite was made.

The runner stopped its own broker; a separate PID check confirmed it no longer
existed. Only `/private/tmp/kip1371-throughput-sfcdl1ks/broker-data` was removed after
its exact configuration and ownership were checked. This frees 17.92 GB of generated
payload; reports remain. No existing cluster or unrelated process was touched.

### Local handoff

The independent KIP Markdown and embedded-style HTML now include all five semantics,
the preserved-design comparison, migration sequence, current correctness receipts and
the complete normal-logging performance result. Pandoc plain-text roundtrips are identical;
code/anchor balance, local/fragment links and encoding checks pass. Browser visual review
remains unperformed because its debugging approval is unavailable while the user sleeps;
static validation is not described as a screenshot review.

The durable evidence package lives under the thread's visualization directory as
`kip-1371-acceptance-95095ac064`, outside Gradle build output. It preserves failed controls,
successful reports, profiles, raw results and a hashed inventory, excluding generated
broker payload and the early default-profile raw JFRs. All changes are kept in local
commits on the existing implementation/DOC branches. No remote push or CI submission,
upstream/Jira/Confluence publication, worktree deletion or history rewrite occurred.

Method: pattern-language connected forces to the smallest supported contracts and
explicit consequences; isolated-demo-tests shaped fresh cluster/JVM evidence and
task-owned benchmark cleanup. The ascii-diagram skill kept the runtime explanation
as text. These methods do not substitute for the executable receipts above.
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
