# Contract-Guided Coordination: original-KIP issue acceptance inventory

## Current evidence update at `95095ac064`

The consolidated [local acceptance record](kip-1371-local-acceptance.md) provides
current counts, commands, autonomous decisions and performance limits. Older gap
tables below are retained audit history, not the current completion summary.

The inventory below is retained as the original gap checklist; its "unverified" and pending-policy
labels must be read with this newer source-audited update. Full historical source provenance is in
[regression provenance](kip-1371-historical-regression-provenance.md), including the exact baseline
ancestor for each inherited repair. The current user authorizes autonomous design decisions;
the selected decisions and any observable behavior change are recorded, not silently treated as
equivalent refactoring. No upstream, Jira, Confluence or remote publication is authorized here.

| Original evidence | Current protection and executable coverage | Limit on the claim |
| --- | --- | --- |
| KAFKA-17066 | Inherited background position initialization; OffsetsRequestManager and ApplicationEventProcessor regression suites exercise initialization and error propagation. | No claim that this POC introduced background ownership; old pre-fix binary not rerun. |
| KAFKA-17674 | Inherited captured partition scope; `testUpdatePositionsDoesNotResetPositionBeforeRetrievingOffsetsForNewlyAddedPartition` and `testUpdatePositionsDoesNotApplyOffsetsIfPartitionNotInitializingAnymore`. | An assignment does not authorize expanding an older request; not a global generation mechanism. |
| KAFKA-18641 | Safe public-poll checkpoint plus D1 retained retry offsets. Real Collector/public-poll schedules protect against recapture before return; cancellation/error/wakeup, seek, leave/rejoin and retry identity are covered. | Intentional freshness/replay tradeoff. Graceful close/restart proof is separate from a forced broker-wire retry/crash reproduction. |
| PR 21476 / KAFKA-15529 | Inherited position update before volatile consumed marker; `testPositionUpdatedBeforeDrainOnExhaustedFetch` and the full Collector suite. | Buffer notification alone would not establish this data-ordering property. |
| KAFKA-20426 | Inherited UNSUBSCRIBED wait; new real-RM/configured-loop manual-assignment no-spin and enabling-membership-input test. | Membership transition is controlled, not a public subscribe() end-to-end test. |
| KAFKA-20253 | Existing feasibility guard plus D2 zero-startup-interval and D3 in-flight completion repair across regular/share/Streams. Real loop no-spin/recovery checks both invocation strategies. | Deterministic no-zero-wait evidence, not an idle CPU percentage or exact reauthentication wire replay. |
| KAFKA-20854 | Inherited no-progress classification plus narrow producer capability. FetchRequestManager tests exercise no-fetchable/backoff no-wake, in-flight completion, empty incremental responses, disconnect/session errors, pause/resume and enabling retries with real buffer waiters. | Empty *response* may legitimately wake; empty *preparation* is not progress. Not every notification is a data-available signal. |
| KAFKA-20970 | Commit unknown-coordinator/expired-auto-commit typed wait plus independent Streams startup/in-flight repairs. Commit and all three heartbeat suites validate local wait and subsequent admission. | Regular commit proof does not stand in for Streams; current-candidate performance still needs measurement. |
| KAFKA-20397 | Pending metadata-dependent operation routing; real-buffer before/during-wait publication tests, actual public-poll latch path, deadline/error identity, wakeup and later recovery. | Controlled transport/collector seams remain; do not call this an exact old failing real-broker wire reproduction. |
| KAFKA-18160 | Inherited acknowledgement-before-rethrow; new public-poll callback WakeupException/InterruptException tests recover and deliver records through real background thread and brokers, in both KRaft configurations. | Four integration cases passed twice; not exhaustive OS interruption timing. |
| KAFKA-19357 | Exact inherited `testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose`: stopped brokers, 500 ms close budget, exactly one failed callback. It passed again in the complete 419-case integration run at this revision. | Correct result is bounded failure, not discovery until success; the earlier separate setup/heap failure remains recorded. |
| KAFKA-18569 | Inherited close signaling; eight public-close/real-membership component combinations prove stop after dependent work and no new discovery after backoff. Separate real close proof observes background-thread termination. | No new universal terminal lifecycle semantics or abrupt broker-crash proof. |

Focused receipts and failures are retained in the completion log. The broad consumer unit namespace
passes at this revision (3,383 cases, 97 suites, no failures/errors/skips; default non-flaky selection).
The full real-consumer integration invocation also passes: **419 cases in 21 suites**, no
failures/errors/skips and no retries, with a fresh JVM per class. This includes the close/restart
persistence test and all four new public callback-recovery cases. Streams application integration
is a separate module; its follow-up is not included in this count.
Existing repaired cases remain inherited evidence even when the POC adds stronger contract tests.

## Original gap checklist (historical)

## Scope and evidence rules

Approach 2 baseline: `8d2fdb7c2cd478121c6571ab6b6496cf44fe12a0`.
Original Approach 3 source: sibling `async-consumer-reactor-poc`,
`docs/design/kip-introduce-consumer-reactor-state-management-event-processing.md`,
SHA-256 `e787f484013b61bc7231d69203a9a1392934a66a306772521b828953b9e037dd`.
Its historical index contains 11 issue IDs plus PR 21476. The full text also
mentions KAFKA-14246 as related work and KAFKA-20995 as the KIP tracking issue.

User acceptance requirement: every historical issue remains in scope, including
the three lifecycle issues the original KIP deferred. No row is declared fully
closed by this inventory. The descriptions below are the original draft's failure
claims, not a new independent root-cause audit of all linked PRs. Historical titles,
exact triggers, fixes, and affected variants still require source-level verification
before assigning historical-reproduction credit.

Evidence labels:

- **Inherited**: baseline repair/regression retained; not a new Approach 2 fix.
- **POC delta**: a documented failing component case improved by Approach 2.
- **Partial**: some obligations have evidence, but an exact trigger, route, or variant is missing.
- **Unverified**: this inventory has not established issue-specific proof; absence of
  proof here does not establish absence of relevant code/tests elsewhere.

All cited test files are under
`clients/src/test/java/org/apache/kafka/clients/consumer/internals/` unless noted.
Passing mocks can prove the consuming side of a contract without proving the real
producer supplies that condition. Test counts alone never close an issue.

## Historical issue matrix

Later evidence update: the [public-close/publication follow-up](kip-1371-approach2-compatibility-ledger.md#public-close-and-actual-publication-follow-up)
adds real membership/commit/discovery routing from public close and replaces the
named public metadata test's mocked wait return with the actual FetchBuffer latch.
This supersedes the mocked-public-wait qualifier below for that test. Close-related
rows now have additional component evidence, but their exact historical trigger,
callback acknowledgement, real handler shutdown and broker proof remain unclosed.

| Evidence / original failure claim | Required correctness or progress property | Approach 2 protection and current evidence | Remaining acceptance gate |
| --- | --- | --- | --- |
| [KAFKA-17066 / PR 16885](https://github.com/apache/kafka/pull/16885): position initialization split across threads | One operation owns its initialization workflow; no unsafe split between assignment/offset work and position update | **Inherited / partial.** Existing background initialization retained; newer response staging is not its original fix | Trace the actual initialization workflow and historical repair; exercise assignment changes and delayed success/failure through the real application-event path |
| [KAFKA-17674 / PR 17342](https://github.com/apache/kafka/pull/17342): old operation touches a later-added partition | An older offset operation updates only its admitted scope | **Inherited / partial.** `OffsetsRequestManagerTest.testUpdatePositionsDoesNotResetPositionBeforeRetrievingOffsetsForNewlyAddedPartition`; captured scope retained | Bind the failing historical baseline and current real event-loop/public-consumer schedule; test cancellation/reassignment without broadening scope |
| [KAFKA-18641 / PR 18737](https://github.com/apache/kafka/pull/18737): auto-commit snapshot races with position advancement | Auto-commit captures the intended offsets at the protocol-defined point | **POC repair / partial.** Counterexample preserved at `6c7a68ef59`; scoped capture checkpoint guards collection, with a separate cancellable wait and post-wait error check. Guard removal and earlier error/wakeup bugs have failing tests; interruption and both active-wait wakeup/error orders have controlled coverage. See [snapshot audit](kip-1371-auto-commit-snapshot-audit.md) | Complete reconciliation/revocation scenarios and performance validation. Controlled wait tests are not exhaustive simultaneous-thread schedules; mock transport success is not broker-durable crash/restart evidence |
| [PR 21476](https://github.com/apache/kafka/pull/21476): consumed marker visible before matching position | Observing exhausted/consumed fetch cannot expose its older position | **Inherited / partial.** `FetchCollectorTest.testPositionUpdatedBeforeDrainOnExhaustedFetch`; existing synchronized handoff retained | Verify the repair's exact visibility boundary and observer; exercise interruption/wakeup/partial collection without claiming the rejected position-handshake experiment is valid |
| [KAFKA-20426 / PR 22018](https://github.com/apache/kafka/pull/22018): heartbeat wait forces manual-assignment poll spinning | Unsubscribed/manual-assignment state must not create an immediate no-progress wait loop | **Inherited / partial.** `AsyncKafkaConsumerTest.testPollWithManualAssignmentDoesNotBusyLoop` checks application behavior with a mocked corrected maximum wait | The cited test explicitly supplies `Long.MAX_VALUE`; add evidence that actual membership/heartbeat/aggregate wiring produces it and that enabling input resumes progress |
| [KAFKA-20253 / PR 22836](https://github.com/apache/kafka/pull/22836): heartbeat urgency while coordinator unavailable | Blocked heartbeat cannot keep forcing zero waits; discovery/recovery enables useful work | **Inherited guards / partial.** `ConsumerHeartbeatRequestManagerTest.testMaximumTimeToWaitWhenCoordinatorUnavailableDoesNotSpin`; existing result is a positive heartbeat interval, not proof of full typed migration | Exact historical failure trigger, real wait aggregation, relevant variants, and no-spin plus recovery evidence; old CPU benchmarks do not validate this revision |
| [KAFKA-20854 / PR 23014](https://github.com/apache/kafka/pull/23014): empty preparation creates wakeup ping-pong | Notify only for a useful enabling change; data/error/empty completion cases must also avoid stranded waits | **Inherited classification + POC capability / partial.** `FetchBufferProducerTest` exercises guarded data notification and store-before-signal; existing fetch preparation guards retained | Reproduce no-progress ping-pong through the real app/background path for in-flight, paused, leader/backoff cases; also prove empty/error completion re-enables legitimate preparation |
| [KAFKA-20970 / PR 23227](https://github.com/apache/kafka/pull/23227): expired commit/Streams timer with unknown coordinator | A blocked request must not create a zero-wait loop; discovery must unblock it | **POC delta / partial.** `CommitRequestManagerTest.testExpiredAutoCommitAwaitsCoordinatorInsteadOfZeroApplicationWait` has documented baseline red-to-green evidence; coordinator/commit activation and scheduling notification added | Commit public-consumer schedule plus the distinct Streams heartbeat branch; do not treat regular commit success as evidence for Streams |
| [KAFKA-20397 / PR 21991](https://github.com/apache/kafka/pull/21991): metadata error around wait entry | Error reaches the eligible operation and is observable without losing its notification; later work can recover | **POC delta / partial.** `ConsumerAsyncPollMetadataTest`; `AsyncKafkaConsumerTest.testMetadataErrorFromDelegateAfterAdmissionSurfacesThroughPoll` | Real broker/wire-error reproduction and exact historical failing baseline; selected real-buffer tests and mocked public wait tests are complementary, not one full end-to-end run |
| [KAFKA-18160 / PR 18089](https://github.com/apache/kafka/pull/18089): wakeup/interruption skips callback acknowledgement | Preserve required callback acknowledgement and terminal outcome despite interruption | **Unverified exact issue path.** `AsyncKafkaConsumerTest.testCloseRunsRevocationCallbackAndSendsLeaveGroupEventOnInterrupt` is adjacent regression evidence only | Identify the exact skipped acknowledgement and reproduce its lifecycle schedule; assert acknowledgement reaches its intended owner exactly as required, not just that close returns |
| [KAFKA-19357 / PR 19914](https://github.com/apache/kafka/pull/19914): close stops discovery too early | Required dependent close work can still use discovery within the existing close contract | **Unverified issue-specific coverage.** Existing close behavior retained; no replacement lifecycle design | Pin historical trigger, dependent operation, close deadline and outcome; test actual loop/transport shutdown ordering |
| [KAFKA-18569 / PR 18590](https://github.com/apache/kafka/pull/18590): discovery continues after dependent close work ends | Stop unnecessary discovery without abandoning still-required work | **Unverified issue-specific coverage.** Not solved merely by next-poll typing or callback batching | Verify exact stop condition and prove both no extra discovery and no premature stop using the same lifecycle model as KAFKA-19357 |

## Other identifiers: retain, but do not misclassify

- [KAFKA-14246](https://issues.apache.org/jira/browse/KAFKA-14246): related consumer-threading
  refactor context, not an extra historical failure in the original evidence index.
  Preserve its application/network-thread and callback ownership contracts.
- [KAFKA-20995](https://issues.apache.org/jira/browse/KAFKA-20995): KIP tracking issue,
  not an independent regression case.
- KAFKA-20915 and KAFKA-21010 appeared in discussion but are not identifiers in this
  pinned original draft. Keep them as supplementary candidates; do not silently use
  them to replace an original-index row or assume their root causes are identical.

## Corrections to earlier evidence interpretation

The inherited contracts-first evidence document describes an older POC revision.
Its late-metadata delivery gap was subsequently addressed at the component level;
see [late metadata delivery](kip-1371-async-poll-metadata-delivery.md). Its historical
scope and CPU/E2E limits remain. Likewise, the original Approach 3 evidence is not
automatically evidence that a test or fix exists in Approach 2.

The coordinator C7/C9 counterexample is a useful architectural protection test,
not the historical root cause of every issue in this table. The 991-test callback
comparison run does not by itself close any of these rows.

## Execution order and completion definition

### Local inventory validation receipt

On 2026-09-06, a focused rerun at the baseline above produced JUnit XML reports
for 39 tests across seven suites: zero failures, errors, or skipped tests.
The run used Java 17, offline Gradle, one test fork, and retries disabled.
The reports cover the named Offsets, FetchCollector, heartbeat, commit,
manual-assignment, delegate metadata-error, and close-interruption methods above,
plus the complete `ConsumerAsyncPollMetadataTest` and `FetchBufferProducerTest`
classes. This is current component regression evidence, not historical
reproduction, full-suite validation, or broker/system-test evidence.

### Next verification sequence

1. Verify source provenance and exact old-failure/new-result assertions per row.
   Prioritize KAFKA-18641 and the three lifecycle rows where exact evidence is weakest.
2. Complete data/position visibility and auto-commit snapshot assertions; preserving
   notification order alone cannot establish data correctness.
3. Complete no-progress and enabling-input schedules for regular, share, and Streams
   where the issue applies; include both no-spin and eventual progress.
4. Complete metadata wait-entry and lifecycle/acknowledgement schedules. Treat early
   and late shutdown as paired requirements, not two unrelated patches.
5. Run affected real-consumer/broker paths and current-revision performance checks.
   External benchmark submissions require their own existing authorization workflow.

A row closes only when its exact failure and required outcomes are established,
the relevant current path/variants are exercised, baseline-vs-new protection is
identified, and remaining limits are explicit. Historical baseline unavailability
must be recorded, not silently replaced with a similar mocked example. A mechanism
change follows a demonstrated gap; do not invent a global barrier merely to fill
the matrix. Any necessary change to public outcome/timeout policy is a user decision.
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
