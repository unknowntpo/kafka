<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Async Consumer Reactor Evidence

## Causal Standard

A bug belongs in the main KIP argument only when its failure path demonstrates at least one of these conditions:

1. the same logical state transition is split across execution contexts;
2. separate components decide urgency and feasibility from different state snapshots;
3. a cross-thread operation can miss or duplicate its terminal acknowledgement.

Generic null checks, isolated cleanup mistakes, metrics defects, and unrelated implementation bugs should not be
attributed to the reactor design.

## Issue Verification Matrix

An issue is `covered` only when the POC passes a test that reproduces the issue's original ordering and failure
conditions through the relevant production components. A manager unit test establishes a local rule, but it does not
prove the application event, reactor ordering, network completion, schedule publication, and application-visible
effect form one correct operation. Rows remain `pending` or `partial` until that proof exists.

| Issue | Root cause | Required reactor invariant | POC mechanism | Verification evidence | Status |
| --- | --- | --- | --- | --- | --- |
| KAFKA-17066 / PR 16885 | Position initialization was split between application and background execution. | One execution owner orders the complete position-update operation. | `OffsetsRequestManager` owns position initialization; `ConsumerReactor` orders the application event and network completion. | Manager regressions plus `AsyncKafkaConsumerTest.testReactorPreservesNewPartitionAcrossOlderOffsetFetchCompletion`, which exercises the real application-event, reactor, manager, and `MockClient` path. | **Component path verified; original pre-17066 negative baseline pending** |
| KAFKA-17674 / PR 17342 | An OffsetFetch operation captured `tp1`; `tp2` joined while it was in flight, and completion of the old operation reset `tp2` before its committed offset was fetched. | A completion may mutate only the operation scope it captured; a newly added partition is handled by a later ordered operation. | Manager preserves the captured initializing-partition set; Reactor orders assignment input, response completion, schedule publication, and the next position update. | `OffsetsRequestManagerTest.testUpdatePositionsDoesNotResetPositionBeforeRetrievingOffsetsForNewlyAddedPartition`; component test `AsyncKafkaConsumerTest.testReactorPreservesNewPartitionAcrossOlderOffsetFetchCompletion`; the same behavior test fails at baseline `6744a718c2` because old tp1 completion produces a `ListOffsets` request for tp2. | **Deterministic component proof complete; broker stress pending** |
| KAFKA-18641 / PR 18737 | Application position advancement raced with the background auto-commit snapshot. | Position advancement precedes any commit snapshot derived from it. | Reactor input ordering plus one application-visible transition/action boundary. | Historical unit tests around `PollEvent` and `CommitEvent`; exact POC ordering test pending. | **Pending** |
| KAFKA-15529 / PR 21476 | Consumed state became visible before the corresponding position update. | Apply and publish the completed fetch transition atomically. | Fetch owner updates position before publishing records/effects. | Historical `FetchCollectorTest.testPositionUpdatedBeforeDrainOnExhaustedFetch`; cross-component POC test pending. | **Unit only — not covered** |
| KAFKA-17439 / PR 17035 | Fetch creation was inferred independently from application polling and background fetch state. | One ordered input produces one fetch-scheduling decision. | `CreateFetchRequestsEvent` enters the Reactor; fetch manager reports work and schedule facts. | Manager/application-event unit tests exist; exact production-chain component assertion pending. | **Partial — not covered** |
| KAFKA-17182 / PR 18795 | Buffer state and fetch-session eviction were decided from different snapshots. | Fetch feasibility and session mutation stay under one owner. | Fetch manager evaluates buffered/unfetchable partitions and returns bounded work/effects. | Historical `FetchRequestManagerTest` buffered-partition cases; Reactor component reproduction pending. | **Unit only — not covered** |
| KAFKA-20426 / PR 22018 | Heartbeat urgency returned zero although manual assignment made heartbeat progress impossible. | Global schedule combines urgency with progress feasibility. | Reactor merges manager deadlines; a blocked manager cannot force an immediate application wake. | Historical `testPollWithManualAssignmentDoesNotBusyLoop` and heartbeat unit test; POC multi-manager schedule tests are related but do not reproduce the exact path. | **Partial — not covered** |
| KAFKA-20253 / PR 22836 | Heartbeat urgency caused CPU spin while coordinator progress was unavailable or backing off. | Coordinator and heartbeat constraints form one schedule; no manager can independently request a zero-delay loop. | Per-manager schedule contributions are retained and merged by Reactor. | Historical heartbeat/coordinator unit tests plus POC multi-manager schedule tests; exact Async consumer component test pending. | **Partial — not covered** |
| KAFKA-20854 / PR 23014 | An empty fetch-preparation result could cause application/background notification ping-pong. | Wake only for a named state transition, deadline, or capacity change. | Fetch blockers affect the Reactor schedule; only terminal/data transitions produce application actions. | `AsyncKafkaConsumerTest.testPausedPartitionDoesNotProduceNoProgressWakeup` exercises the real consumer, reactor, fetch manager, and observed fetch buffer. It reports zero wakes in the POC; the same behavior test reports one invalid wake at pre-PR commit `9521d77da3`. | **Deterministic component proof complete; broker stress pending** |
| KAFKA-20397 / PR 21991 | Metadata-error publication raced with the application entering its fetch-buffer wait. | Publish the terminal decision before a retained notification to the same wait set. | Reactor publishes schedule/state before executing the application wake action. | POC publish-before-wakeup tests cover the invariant; exact metadata-error reproduction pending. | **Partial — not covered** |
| KAFKA-18160 / PR 18089 | Wakeup or interruption could skip the callback-completed acknowledgement. | Every cross-thread operation has exactly one terminal acknowledgement. | Callback completion is an ordered input; Reactor owns the terminal action. | Historical consumer integration tests exist; POC callback/acknowledgement component proof pending. | **Partial — not covered** |
| KAFKA-19357 / PR 19914 | Close stopped coordinator discovery while a pending commit still required it. | Shutdown scheduling preserves dependencies of admitted operations until terminal acknowledgement. | Reactor owns shutdown ordering across managers. | Historical commit unit/integration tests; POC shutdown dependency test pending. | **Pending** |
| KAFKA-19394 / PR 20792 | Reactor-thread initialization failure could leave application close waiting indefinitely. | Startup has exactly one success/failure publication consumed by close. | Reactor lifecycle publishes initialization failure before releasing waiters. | Historical initialization/close tests; POC lifecycle proof pending. | **Pending** |

## Direct Evidence

| Issue / change | Observed invalid state | Missing contract |
| --- | --- | --- |
| [KAFKA-17066 / PR 16885](https://github.com/apache/kafka/pull/16885) | `updateFetchPositions` was split between application and background threads | one owner for a complete position update |
| [KAFKA-18641 / PR 18737](https://github.com/apache/kafka/pull/18737) | application position advancement raced with a background auto-commit snapshot, allowing record loss | ordered position and commit-snapshot transition |
| [KAFKA-15529 / PR 21476](https://github.com/apache/kafka/pull/21476) | consumed state became visible before the corresponding position, allowing a duplicate fetch | publish a completed transition atomically |
| [KAFKA-17439 / PR 17035](https://github.com/apache/kafka/pull/17035) | application and background threads inspected fetch-buffer state at different times | one fetch scheduling decision from one snapshot |
| [KAFKA-17182 / PR 18795](https://github.com/apache/kafka/pull/18795) | a buffer-state race caused unnecessary fetch-session removal and recreation | keep decision and execution under one owner |
| [KAFKA-20426 / PR 22018](https://github.com/apache/kafka/pull/22018) | a zero wait was returned although manual assignment made heartbeat progress impossible | combine urgency with progress feasibility |
| [KAFKA-20253 / PR 22836](https://github.com/apache/kafka/pull/22836) | heartbeat urgency produced CPU spin while the coordinator was unavailable or backing off | one reactor schedule across heartbeat and coordinator state |
| [KAFKA-20854 / PR 23014](https://github.com/apache/kafka/pull/23014) | an ambiguous empty fetch result caused application/network wakeup ping-pong | typed outcome and transition-specific notification |
| [KAFKA-20397 / PR 21991](https://github.com/apache/kafka/pull/21991) | metadata error publication raced with waiting on the fetch buffer | one completion protocol and wait set |
| [KAFKA-18160 / PR 18089](https://github.com/apache/kafka/pull/18089) | wakeup or interruption could skip a callback-completed event | exactly-once terminal acknowledgement |

## Supporting Lifecycle Evidence

| Issue / change | Observed invalid state | Relationship |
| --- | --- | --- |
| [KAFKA-19357 / PR 19914](https://github.com/apache/kafka/pull/19914) | close stopped coordinator discovery while pending commits still required it | contradictory lifecycle decisions across managers |
| [KAFKA-19394 / PR 20792](https://github.com/apache/kafka/pull/20792) | network-thread initialization failure could leave application close waiting forever | incomplete startup/close handshake |

These are supporting rather than primary examples because a narrower lifecycle coordinator could also address them.

## KIP Positioning

[KIP-945](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/255073708/KIP-945+Update+threading+model+for+Consumer)
already records the motivation for changing the consumer threading model, but its detailed threading, data-flow,
and network-I/O sections remain incomplete. It is related history rather than a dependency or approval gate for the
focused ownership, progress, wakeup, and resource-bound proposal. That proposal can proceed independently while
explicitly building on the event-loop architecture already established by the threading refactor.

The design claim is not that a reactor automatically prevents every bug. The narrower claim is:

> The existing model permits recurring invalid states because ownership, progress feasibility, and cross-thread
> completion are separate implicit contracts. Making those contracts explicit removes entire classes of invalid
> intermediate states and gives focused tests a stable boundary.

The main KIP motivation should use one correctness example (KAFKA-18641), one liveness example (KAFKA-20854 or
KAFKA-20426), and one handshake example (KAFKA-18160 or KAFKA-20397). The remaining cases belong in an appendix.

## POC Adversarial Review Gate

A read-only adversarial Claude review challenged the direct removal of the fetch wait rescan. Its high-severity
finding was that a typed deadline is insufficient unless publication and notification form one protocol: the
application may already be waiting with an older snapshot and cannot observe a newly shortened deadline until it is
released. A later Claude Fable review of the implemented reconciliation boundary found two additional scheduling
risks: a persistent compatibility `0 ms` result could recreate an expired deadline on every loop, and preserving
only the global limiting deadline could erase an unaffected manager's earlier deadline. The POC now uses a
per-manager schedule cache and one-shot compatibility-deadline delivery to cover both findings.
The same review also found that `PollResult.timeUntilNextPollMs` still bypassed the published schedule. The migration
adapter now converts that value to a Reactor-only `NextReconcile`: it participates in the single schedule and bounds
network polling, but cannot synthesize an application wakeup.
Before PR 23014, the groupless path did not read `maximumTimeToWait()` at all; current trunk now reads it for every
consumer, but that fixes the next wait rather than an already-blocked wait.

The rescan-removal slice uses the following properties as its deterministic gate:

| Risk | Required property | POC evidence |
| --- | --- | --- |
| stale long wait | publish the new immutable snapshot before waking the waiter | `testShorterDecisionIsPublishedBeforeApplicationWakeup` |
| caller is already waiting on an older decision | publish and deadline expiry produce retained wakeups; the real groupless consumer chain limits the network poll to one reconnect backoff | `testDeadlineWakeupReleasesReactorScheduleUsingOlderSnapshot`, `AsyncKafkaConsumerTest.testGrouplessPollRetriesFetchWhenReconnectBackoffExpires` |
| relative-deadline drift | publish the decision which bounds network I/O, preserve it across early returns, and deliver it before recomputing a legacy relative wait | `testLegacyRelativeWaitDoesNotDriftAcrossEarlyNetworkReturns`, `testLegacyRelativeWaitExpiresBeforeFreshScheduleMovesDeadlineForward` |
| stale `0 ms` wait | mark expiry delivery in the immutable snapshot before waking and preserve it for the same semantic decision | `testExpiredDeadlineDoesNotLeaveZeroReactorSchedule`, `testSameDeadlineFromDifferentSourceIsANewTransition` |
| persistent compatibility `0 ms` wait | suppress a delivered zero wait until the manager reports new progress or a positive delay | `ManagerReconcileCacheTest.testDeliveredPersistentZeroCompatibilityDeadlineIsSuppressed`, `testPersistentZeroCompatibilityWaitDoesNotBusyLoop` |
| incremental multi-manager scheduling | retain each unaffected manager's contribution when one affected manager is reconciled | `ManagerReconcileCacheTest.testIncrementalUpdateRetainsUnaffectedManagerDeadline`, `testNextFullReconciliationRecomputesCrossManagerSchedule` |
| split network/application scheduling | normalize legacy poll delays into the same immutable schedule while keeping Reactor-only deadlines from waking the application | `testLegacyPollTimeoutIsPublishedInReactorSchedule`, `ManagerReconcileCacheTest.testLegacyPollDeadlineDoesNotDriftAcrossEarlyReconciliation` |
| delivered deadline reactivation | delivering a later manager deadline must not reactivate an earlier delivered deadline | `ManagerReconcileCacheTest.testDeliveringAnotherManagerDeadlineDoesNotReactivateEarlierDeadline` |
| same-source, same-timestamp retry | include the manager-owned semantic generation in native deadline identity | `testSameSourceAndDeadlineWithNewGenerationIsANewTransition` |
| deadline starvation | do not postpone an absolute deadline when the same block is re-observed | `ReactorScheduleTest.testReactorScheduleSubtractsElapsedTime`, `testRepeatedPreparationDoesNotPostponeRetryDeadline` |
| reconnect retry before capacity | use the network client's actual connection delay, including exponential backoff | `testMaximumTimeToWaitBoundedWhenPartitionsSkippedDueToBackoff` |
| mixed partitions | retry conditions win over event-only in-flight conditions | `testRetryDeadlineWinsWhenInFlightAndReconnectConditionsAreMixed` |
| wakeup ping-pong | `NO_FETCHABLE_PARTITIONS` schedules a reactor deadline without an eager wake; terminal request completion becomes a named reactor effect | `testNoFetchablePartitionsDoesNotWakeUpBuffer`, `testNoFetchablePartitionsUsesReactorRetryDeadline` |
| manager bypasses reactor | request completion and preparation failure report bounded effects; only the reactor applies the synthetic wake | `testEmptyFetchResponseReportsStateTransition`, `testFailedFetchResponseReportsStateTransition`, `testFetchSessionErrorResponseReportsStateTransition`, `testPollWithCreateFetchRequestsError` |
| duplicate notification pressure | equal manager-owned state transitions coalesce in an enum-bounded set and multiple wake reasons collapse into one primitive wake per phase | `testDuplicateStateTransitionsAreReturnedOnceByReconcile`, `testReactorCoalescesStateTransitionAndShorterScheduleIntoOneAction` |
| network callback ordering | a callback records a manager-owned transition; the next ordered reconciliation returns it, then the reactor publishes the schedule before acting | `testReactorPublishesScheduleBeforeExecutingTransitionFromNextReconciliation` |
| data plus terminal double wake | a response that adds a completed fetch uses the mailbox signal and does not also report a terminal effect | `testMaximumTimeToWaitUnboundedWhenBufferedDataWakesApplication`, `testEmptyFetchResponseReportsStateTransition` |
| unsent request expiration | timeout failure marks the owning manager; affected post-I/O reconciliation returns its terminal transition before waking one blocked application waiter | `testUnsentFetchExpirationIsPublishedByAffectedPostPollReconciliation` |

The POC now covers the stale-wait publication protocol at two deterministic levels: a real application-side
`FetchBuffer` wait running concurrently with the reactor scheduler, and the complete
`AsyncKafkaConsumer -> ApplicationEventHandler -> ConsumerReactor -> FetchRequestManager -> FetchBuffer`
chain with only the socket replaced by a controllable `MockClient`. `testPollWaitUsesOnlyPublishedReactorDecision`
also proves that the application thread no longer derives a competing timeout from `SubscriptionState` or
`FetchBuffer`. The real KRaft `PlaintextConsumerPollTest` suite also remains green. The POC removes the rescan, but
production integration still has one open gate:

1. add a real-socket broker-restart smoke test for manual-assignment/groupless reconnect backoff;
