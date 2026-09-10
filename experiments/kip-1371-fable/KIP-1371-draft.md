> 閱讀指南（給 owner，不貼到 Confluence）：
> 1. 本文是 KIP-1371 新版草稿，結構依 `DESIGN.md` §9；只講五條契約與它們修的 issue，機制放在各契約的「Minimal change」。
> 2. Reviewer 可以讀完 §2 Summary 或 §4 Contracts 就停；量測與替代方案在 §8–§9，證據文件在 §10。
> 3. 已實作（分支 `fable/kip-1371-event-loop`，base trunk `74fbd50061`）：C1、C2、C3；C4、C5 只有文件與測試，沒改 production code。
> 4. §8 的 trunk vs branch JMH 數字是佔位表，等 `jmh-ab/results/summary.md` 產出後填入；系統測試尚未跑。
> 5. 待決事項：metric 可拿掉；C2 的 `commitSync(Duration.ZERO)` 行為變更需要 reviewer 決定；KAFKA-21031 與 PR #23357 要協調。

# KIP-1371: Wait, scope, publication, termination and thread-ownership contracts for the async consumer background loop

## 1. Status

| Field | Value |
|---|---|
| Status | Draft |
| JIRA | [KAFKA-20995](https://issues.apache.org/jira/browse/KAFKA-20995) (umbrella) |
| Discussion thread | TBD |
| Author | [placeholder] |
| Related tickets | KAFKA-20253, KAFKA-20426, KAFKA-20970, KAFKA-21010, KAFKA-21031, KAFKA-21049, KAFKA-20540, KAFKA-19804, KAFKA-20854, KAFKA-17066, KAFKA-17674, KAFKA-15529 (PR #21476), KAFKA-18641, KAFKA-20397, KAFKA-18160, KAFKA-18569, KAFKA-19357 |
| Baseline | trunk `74fbd50061` (2026-09-10) |

## 2. Summary

`AsyncKafkaConsumer` (`group.protocol=consumer`), `ShareConsumerImpl` and the Streams group protocol share one background loop (`ConsumerNetworkThread`) and one set of `RequestManager`s. The rules that keep this loop correct are not written down. The same bugs keep coming back in different managers. This KIP writes the rules down as five contracts, fixes the places where trunk breaks them with the smallest possible change, and pins each rule with a test.

| Contract | One sentence | Closes |
|---|---|---|
| C1 Wait reason | A manager that did not send a request must never ask for a zero wait, on either the network channel (`timeUntilNextPollMs`) or the application channel (`maximumTimeToWait`). | Busy loops: KAFKA-21031, KAFKA-21049; makes the 20253/20426/20970/21010 pattern a rule |
| C2 Scope and late response | An operation captures its scope when it is admitted, checks it before applying a response, and never starts after its observer's deadline has passed. | Never-expiring commits, ListOffsets retried forever, late responses after seek/unsubscribe |
| C3 Publish then notify | The background thread changes state, then publishes, then wakes the application thread; anything the application must handle wakes it; interval auto-commit only snapshots positions already delivered. | KAFKA-20397, KAFKA-18641 residual |
| C4 Termination | Close is one written sequence, and every kind of pending work has exactly one terminator and one result. | KAFKA-18569, KAFKA-19357 family (documented and pinned, no code change) |
| C5 Thread ownership | Manager state lives on the network thread, user callbacks run on the application thread, and every shared field has one named writer. | KAFKA-18160 family (documented and pinned, no code change) |

Public interface change: one new metric (`network-thread-invalid-poll-result-total`) and one behaviour change (C2: a never-attempted commit or offset fetch expires at its deadline instead of being sent later). No scheduler. No new thread model. No `Consumer` or `ShareConsumer` API change.

## 3. Motivation

### 3.1 Six failure shapes

The 14 tickets under the umbrella fall into six shapes. Status is at trunk `74fbd50061`.

| Shape | Tickets | Status at trunk | One sentence |
|---|---|---|---|
| A. Blocked work reports zero wait | KAFKA-20253, 20426, 20970, 21010 | fixed | Wait time is computed from a timer that is only reset when a request is sent, so it becomes 0 while in flight, while the coordinator is unknown, or before the first heartbeat. |
| | KAFKA-21031, KAFKA-21049, KAFKA-20540 | **open** | Same shape, not yet fixed. |
| B. Scope leaks across stages | KAFKA-17066, 17674 | fixed | A later stage of an async chain reads the current assignment instead of the set captured at admission. |
| C. Publication order | KAFKA-15529 / PR #21476 | fixed | No written order between position advance, record delivery, `isConsumed` and the auto-commit snapshot. |
| | KAFKA-18641 | **partly regressed** | KAFKA-18376 removed the application-thread wait that gave the ordering; interval auto-commit can again read positions of records not yet returned. No trunk test covers it. |
| D. Error delivery and no-progress wakeup | KAFKA-20854 | fixed | An empty fetch preparation result woke the application for no reason. |
| | KAFKA-20397 | **open** | The background thread marks the in-flight poll with a metadata error but does not wake the parked application thread. |
| E. Callback acknowledgement | KAFKA-18160 | fixed | A `WakeupException` inside a rebalance callback skipped the `CallbackCompletedEvent`. |
| F. Close and termination | KAFKA-18569, 19357 | fixed | Each manager decided its own close behaviour; two fixes conflicted. |

### 3.2 The same method was patched four times

`AbstractHeartbeatRequestManager.maximumTimeToWait()` was changed by KAFKA-20253 (guard on coordinator unavailable), KAFKA-20970 (coordinator unknown returns backoff), KAFKA-21010 (return `retry.backoff.ms` instead of the heartbeat interval, which is 0 before the first response), and is changed again by KAFKA-21031 (in-flight heartbeat still returns 0). Each fix rebuilt the condition "will `poll()` send anything?" by hand. The 20253 guard `!requestInFlight()` only protected the "heartbeat now" branch and let the in-flight case fall through to the same zero. This is what happens when the rule is not written down.

### 3.3 Measured cost

- Busy-loop family: with the coordinator unreachable, trunk burns about 1.6 CPU cores (1607–1725 process CPU ms/s) in the `unavailable` scenario of the real-broker A/B harness. The no-spin change alone removes about 93% of that (the design notes quote < 0.05 core). The scheduler prototype did not change it at all.
- Scheduler alternative (S1): consume CPU per record +31.3%, process CPU +24.0%, idle allocation on the network thread +110%, throughput within noise. See §9.
- Manager polls are not the cost: async-profiler shows 82–88% of network-thread time in `Selector.select` / socket read; the seven manager `poll()` calls together are under 1%.

## 4. Proposed changes

Each contract uses the same format: definition, where trunk violates it, minimal change, tests, tickets. C1, C2 and C3 are implemented on the branch. C4 and C5 are documentation plus tests only.

### C1. Wait-reason contract

**Definition.** `RequestManager.poll()` returns one of three results:
- `progress(requests)`: requests were staged; only this case may ask for an immediate re-poll (`timeUntilNextPollMs == 0`);
- `retryAfter(delayMs)`: waiting for time, `delayMs > 0`;
- `awaitInput()`: waiting for an in-flight response or another owner's state; `Long.MAX_VALUE`, re-evaluated on the next network poll or application event.

`maximumTimeToWait()` bounds the application thread only by deadlines of actions the application thread itself must start and that are possible now: poll-timer refresh, auto-commit when the coordinator is known, Streams topology push. Waiting for an in-flight request, the coordinator, or DNS is not such an action, so it returns `Long.MAX_VALUE` (or the poll-timer bound).

**Where trunk violates it.**
- `HeartbeatRequestState.timeToNextHeartbeatMs`: with the timer expired and a request in flight, returns `remainingBackoffMs()`, which is 0 before the first response (KAFKA-21031). `AbstractHeartbeatRequestManager.poll()` and `maximumTimeToWait()` both forward it, so both threads spin until the response or `request.timeout.ms`.
- `AbstractHeartbeatRequestManager.maximumTimeToWait`: `pollTimer.remainingMs() / 2` rounds the last millisecond to 0.
- `FetchRequestManager.maximumTimeToWait` and `AsyncKafkaConsumer.pollForFetches`: return `retry.backoff.ms`, which may be 0 (KAFKA-21049).

**Minimal change (implemented, commit "KIP-1371 C1").**
1. `NetworkClientDelegate.PollResult.progress / retryAfter / awaitInput` named factories; `retryAfter` rejects non-positive delays. No new fields.
2. `NetworkClientDelegate.addAll(PollResult)`: a zero delay with no requests is counted in `network-thread-invalid-poll-result-total` and replaced by `max(1, retry.backoff.ms)`. This is a safety net and a detector, not a scheduler.
3. `HeartbeatRequestState.timeToNextHeartbeatMs`: returns `max(1, retryBackoffMs)` while a request is in flight. `AbstractHeartbeatRequestManager` and `StreamsGroupHeartbeatRequestManager.maximumTimeToWait`: while in flight, bound only by `max(1, pollTimer.remainingMs() / 2)`.
4. `FetchRequestManager.maximumTimeToWait`: `max(1, retryBackoffMs)`. The same floor in `pollForFetches` (`recheckMs`) landed with the C3 commit.

**Tests that pin it.** `ConsumerHeartbeatRequestManagerTest.testInFlightHeartbeatWithExpiredIntervalDoesNotSpin` (interval 0 and 5000), `testMaximumTimeToWaitWithOneMsLeftOnPollTimerDoesNotRoundToZero`; `StreamsGroupHeartbeatRequestManagerTest.testInFlightFirstHeartbeatWithExpiredIntervalDoesNotSpin`; `FetchRequestManagerTest.testMaximumTimeToWaitBoundedToAtLeastOneMsWhenRetryBackoffIsZero`; `NetworkClientDelegateTest.testAddAll{ClampsZeroDelayWithoutRequestsToRetryBackoff, ClampsZeroDelayWithoutRequestsToAtLeastOneMs, ProgressResultStagesRequestAndReturnsZero, AwaitInputResultWaitsForever, RetryAfterResultReturnsDelay}`, `testRetryAfterRejectsNegativeDelay`; `AsyncConsumerMetricsTest` for the metric. Existing `...DoesNotSpin` tests from 20253/20426/20970/21010 stay unchanged.

**Tickets.** KAFKA-21031, KAFKA-21049; pattern of KAFKA-20253, 20426, 20970, 21010, 20540.

### C2. Scope and late-response contract

**Definition.** An operation that spans several polls captures its scope (partition set, assignment) at admission and checks it before applying a response. A stale result is dropped but still clears the in-flight state. The application's timeout, wakeup or cancel ends the observer, not the RPC: a request already sent completes or fails on its own. A request that was **never attempted** must not start after the observer's deadline has passed.

**Where trunk violates it.**
- `CommitRequestManager.RetriableRequestState.maybeExpire` requires `numAttempts > 0`, so a `commitSync` that waited out its timeout while the coordinator was unknown is still sent when the coordinator appears. `poll()` returns `EMPTY` in the coordinator-unknown branch without expiring anything.
- `OffsetsRequestManager.ListOffsetsRequestState` has no deadline: partitions with an unknown leader stay in `requestsToRetry` and are rebuilt on every metadata update, and their transient topics stay registered.

**Minimal change (implemented, commit "KIP-1371 C2").**
1. `CommitRequestManager`: `maybeExpire` drops the `numAttempts > 0` condition; `PendingRequests.failAndRemoveExpiredRequests` covers unsent commits (attempted or not) and never-attempted unsent offset fetches; it runs in `drain()` and in the coordinator-unknown branch of `poll()` when not closing. Closing with an unknown coordinator still fails with `CommitFailedException` (KAFKA-19357 wins). Auto-commit and `commitAsync` use `Long.MAX_VALUE` deadlines and are unaffected. An offset fetch that was attempted keeps its existing retry semantics.
2. `OffsetsRequestManager.fetchOffsets(timestamps, requireTimestamps, deadlineMs)` carries `ListOffsetsEvent.deadlineMs()` into `ListOffsetsRequestState.deadlineMs`; `poll()` and `onUpdate()` first run `failExpiredRequestsToRetry`, which removes expired states from `requestsToRetry` and completes them with `TimeoutException` (the existing completion handler releases transient topics). `currentLag` passes `Long.MAX_VALUE` (it never retried).
3. Tests only: late responses for positions (OffsetFetch), reset (ListOffsets) and validation (OffsetsForLeaderEpoch) after seek, unsubscribe and observer timeout, using a real `SubscriptionState`.

`ShareConsumeRequestManager.AcknowledgeRequestState.maybeExpire` keeps `numAttempts > 0`: it is a reusable per-node container with different semantics and is out of scope.

**Tests that pin it.** `CommitRequestManagerTest.testCommitSyncExpiredWhileCoordinatorUnknownIsNotSentWhenCoordinatorDiscovered`, `testFetchOffsetsExpiredWhileCoordinatorUnknownIsNotSentWhenCoordinatorDiscovered` (both fail on trunk), `testPollWithClosingAndExpiredPendingCommitFailsWithCommitFailedException`; `OffsetsRequestManagerTest.testListOffsetsWaitingForMetadataUpdate_Timeout` (rewritten, fails on trunk), `..._ExpiredOnMetadataUpdate` (fails on trunk), `testUpdatePositionsAppliesCommittedOffsetsReceivedAfterEventDeadline`, `testUpdatePositionsErrorReceivedAfterEventDeadlineIsThrownOnNextCall`, `testResetPositionsLateResponseNotAppliedAfter{Seek,Unsubscribe}`, `testValidatePositionsLateResponseNotAppliedAfter{Seek,Unsubscribe}`.

**Tickets.** KAFKA-17066, KAFKA-17674 (pattern); new behaviour, see §5.

### C3. Publication and notification contract

**Definition.**
- Order of background-visible effects: change state, publish (volatile or queue), then signal.
- After the background thread puts anything the application must handle into a queue or onto the `AsyncPollEvent` (records, a `BackgroundEvent`, a metadata or fatal error), it wakes a parked application thread.
- An interval auto-commit may only snapshot positions of records already returned by a completed `poll()`.

**Where trunk violates it.**
- `BackgroundEventHandler.add` enqueues but does not wake the application thread.
- `ConsumerNetworkThread.maybeFailOnMetadataError` → `AsyncPollEvent.onMetadataError` marks the error and nobody wakes `FetchBuffer.awaitWakeup` (KAFKA-20397). The open PR #21991 adds a check before blocking, which still leaves a window.
- `ApplicationEventProcessor` calls `CommitRequestManager.updateTimerAndMaybeCommit(now)` while the application thread may be advancing positions in `FetchCollector` (KAFKA-18641 residual after KAFKA-18376).

**Minimal change (implemented, commit "KIP-1371 C3").**
1. `BackgroundEventHandler` takes an `applicationWakeup` hook and runs it after enqueueing; `AsyncKafkaConsumer` and `ShareConsumerImpl` pass their fetch buffer's `wakeup`. `NetworkClientDelegate.wakeupApplication()` exposes the same hook.
2. `AsyncPollEvent` takes an `onError` hook; `completeExceptionally` writes the volatile error first, then runs the hook (`fetchBuffer::wakeup`).
3. `FetchBuffer.wakeup()` sets the sticky flag first and takes the lock only when a thread is waiting.
4. `CommitRequestManager.updateTimerAndMaybeCommit(now, committableOffsets)`: the interval auto-commit driven by an `AsyncPollEvent` uses only the snapshot carried by the event. The application thread captures `allConsumed()` in `checkInflightPoll`, before `collectFetch`, and only when the network thread asked for it through the shared `autoCommitSnapshotRequested` flag. Commit due but no snapshot: set the flag, do not commit, the next poll carries one. The blocking overload (`AssignmentChangeEvent`, close) still reads live `allConsumed()`.

**Tests that pin it.** `EventLoopContractRegressionTest.testMetadataErrorWakesParkedApplicationThread` and `testIntervalAutoCommitDoesNotIncludeUndeliveredPositions` (both fail on trunk); `BackgroundEventHandlerTest.testAddPublishesEventBeforeRunningApplicationWakeup`; `FetchBufferTest.testWakeupBeforeAwaitIsSticky`, `testWakeupRacingWithAwaitIsNeverLost`, `testWakeupReleasesParkedThreadPromptly`; `CommitRequestManagerTest.testPollDrivenAutoCommit{CommitsExactlyTheSnapshot, DoesNothingBeforeIntervalElapses, DoesNothingWhileCommitInFlight, WithoutSnapshotRequestsSnapshotAndDoesNotCommit}`; `AsyncKafkaConsumerTest.testPollEventCarriesNoCommittableOffsetsWhenSnapshotNotRequested`; `ApplicationEventProcessorTest.testAsyncPollEventPassesCommittableOffsetsSnapshotToCommitManager`; `ConsumerNetworkThreadTest.testMetadataErrorOnAsyncPollEventRunsErrorHookAndSkipsProcessing`.

**Tickets.** KAFKA-20397, KAFKA-18641, KAFKA-15529 / PR #21476 (pattern).

### C4. Termination contract

**Definition.** `close()` is one written sequence (ten steps: disable wakeups, one shared `closeTimer`, auto-commit on close, stop FindCoordinator, run rebalance callbacks on the application thread, leave group, await async commits, stop the network thread and run `cleanup()`, reap the background queue, close resources). Every kind of pending work has one terminator and one result: sent, `CommitFailedException`, `TimeoutException`, or dropped. The table lives in `termination-table.md` (18 kinds of pending work) instead of in each manager's `closing` flag.

**Where trunk violates it.** Not a code violation. The table found one manager-level future without a terminator (unsent OffsetFetch for positions at close: `clearAll()` clears it without completing it) and a few ambiguous rows (validation has no observable result; `AsyncPollEvent` is the only application event without a reaper; Streams topology push has no close hook). None can hang a caller because the application-side events are all reaped, so no production change is made in this KIP.

**Minimal change.** Documentation and tests only (commit "KIP-1371 C4/C5"). If a test shows a pending work item with no terminator that can hang a caller, the smallest fix goes in a follow-up.

**Tests that pin it.** `TerminationContractTest.testUnsentCommitSyncWithCoordinatorUnknownFailsWithCommitFailedExceptionAtClose`, `testUnsentCommitWithCoordinatorKnownIsDrainedOnFirstPollAfterSignalClose`, `testUnsentOffsetFetchIsDroppedWithoutCompletingItsFutureAtClose` (characterization), `testLateListOffsetsResponseAfterAssignmentReleasedDoesNotThrowOrWritePosition`, `testLateListOffsetsResponseWithManualAssignmentStillWritesPosition`, `testLateOffsetFetchResponseAfterAssignmentReleasedDoesNotThrowOrWritePosition`, `testRebalanceCallbackEventsLeftInBackgroundQueueAreFailedAtClose`, `testApplyAssignmentEventInApplicationQueueIsFailedByNetworkThreadCleanup`.

**Tickets.** KAFKA-18569, KAFKA-19357.

### C5. Thread-ownership contract

**Definition.** (1) `RequestManager` state is read and written only on the network thread; the application thread reaches it only through `ApplicationEvent`s and their futures. (2) User callbacks (`ConsumerRebalanceListener`, `StreamsRebalanceListener`, `OffsetCommitCallback`, interceptors, `AcknowledgementCommitCallback`) run only on the application thread. (3) The application completes a background future only through a `*CallbackCompletedEvent`. (4) Positions are advanced by the application thread (`FetchCollector`), except seek/reset which run on the network thread while the application is blocked inside the API. (5) A field with one writer thread and a different reader thread is at least `volatile`/`Atomic*`; two writer threads means one lock or one queue. The writer/reader table for `SubscriptionState`, `FetchBuffer`, `AsyncPollEvent` and the other shared objects lives in `thread-ownership.md`.

**Where trunk violates it.** Rule 5: `SubscriptionState.seekUnvalidated(tp, position)` and `TopicPartitionState.seekUnvalidated` are not synchronized while every other `SubscriptionState` entry point is. Not changed in this KIP; recorded for a follow-up.

**Minimal change.** Documentation and tests only.

**Tests that pin it.** `TerminationContractTest.testCallbackWakeupOrInterruptStillProducesCallbackCompletedEvent`, `testCallbackWakeupOrInterruptDuringProcessBackgroundEventsReportsThenPropagates`, `testCommitCallbackWakeupOrInterruptPropagatesFromPollWithoutBlockingNetworkThread`, `testApplicationThreadReleasedFromApplyAssignmentReportsErrorAndDoesNotHang`; existing `ConsumerMembershipManagerTest.testListenerCallbacksThrowsErrorOnPartitions{Revoked,Assigned}`, `testOnPartitionsLost`.

**Tickets.** KAFKA-18160.

## 5. Public interfaces

### 5.1 New metric

| Field | Value |
|---|---|
| Name | `network-thread-invalid-poll-result-total` |
| Group | `consumer-metrics` (consumer, Streams); `consumer-share-metrics` (share) |
| Type | `CumulativeSum` |
| Description | The total number of request manager poll results that asked for an immediate re-poll without staging any request. Such results are clamped to `retry.backoff.ms` to avoid a busy loop. |

The metric is a detector for C1 violations. If reviewers prefer, it can be dropped and the clamp kept with a debug log; the KIP then has no public interface change.

### 5.2 Behaviour changes (C2)

| Case | Before (trunk) | After |
|---|---|---|
| `commitSync(timeout)` times out while the coordinator is unknown | The caller gets `TimeoutException`; the commit is still sent when the coordinator appears, so the broker records an offset the caller was told did not commit. | The commit expires on the network thread with `TimeoutException` and is never sent. Same for `maybeAutoCommitSyncBeforeRebalance` and the close-path `commitSync` with a finite deadline. |
| `committed()` / `updateFetchPositions` OffsetFetch never sent while the coordinator is unknown and the deadline passes | Sent later; result cached for a later call. | Fails with `TimeoutException`; `AsyncKafkaConsumer.updateFetchPositions` swallows it (returns false), so `poll()` throws nothing new. |
| `offsetsForTimes` / `beginningOffsets` / `endOffsets` with an unknown leader | Retried on every metadata update forever; transient topics stay in metadata. | Fails at the API timeout and releases the transient topics. The caller already saw `TimeoutException` from the reaper at the same time; the difference is no leftover request. |
| `commitSync(Duration.ZERO)` | Sent once; the caller gets `TimeoutException` immediately. | The request state is created with a deadline equal to now and expires in `drain()` before it is sent. The caller still gets `TimeoutException`, but nothing is sent. This differs from the classic consumer, which sends the request. Reviewer decision needed: keep, or require at least one attempt when the coordinator is known. |
| RPC already sent when the application times out | Not cancelled. | Unchanged. |
| Late OffsetFetch response for positions | Applied while the partition is still initializing. | Unchanged (scope is the partition set, not the deadline). |

## 6. Compatibility, deprecation and migration

- No `Consumer`, `ShareConsumer`, callback or configuration change. No thread rename. Existing metrics unchanged.
- Classic consumer (`group.protocol=classic`): untouched.
- Consumer (`group.protocol=consumer`) and Streams: all five contracts. C2 applies only here (`CommitRequestManager`, `OffsetsRequestManager`).
- Share consumer: C1 through `HeartbeatRequestState` and the `addAll` clamp; C3 through the `BackgroundEventHandler` wakeup hook passed by `ShareConsumerImpl`. `ShareConsumeRequestManager` acknowledgement expiry is out of scope.
- KAFKA-21031: the C1 change to `HeartbeatRequestState.timeToNextHeartbeatMs` overlaps PR #23357. Whichever lands first, the other is reduced to its test; this must be coordinated with the PR author.
- Migration: none.

## 7. Test plan

### 7.1 Contract tests on the branch

| Contract | Coordinator | Heartbeat (consumer / share / streams) | Commit | Offsets | Fetch | Loop / delegate |
|---|---|---|---|---|---|---|
| C1 | existing `testNoBusyPollWhileFindCoordinatorRequestInFlight` | `ConsumerHeartbeatRequestManagerTest`, `StreamsGroupHeartbeatRequestManagerTest` (new, above); existing `...DoesNotSpin` | existing `testMaximumTimeToWaitWhenCoordinatorUnknownDoesNotSpin`, `...DuringRealBootstrapDnsResolution` | — | `FetchRequestManagerTest` (retry.backoff.ms=0) | `NetworkClientDelegateTest` (clamp, factories), `AsyncConsumerMetricsTest` |
| C2 | — | — | `CommitRequestManagerTest` (expiry, closing) | `OffsetsRequestManagerTest` (deadline, late responses) | — | — |
| C3 | — | — | `CommitRequestManagerTest` (poll-driven auto-commit) | — | `FetchBufferTest` | `BackgroundEventHandlerTest`, `ConsumerNetworkThreadTest`, `ApplicationEventProcessorTest`, `AsyncKafkaConsumerTest`, `EventLoopContractRegressionTest` |
| C4 | `TerminationContractTest` (close sequence, late responses after close) | | | | | |
| C5 | `TerminationContractTest` (wakeup/interrupt in rebalance and commit callbacks) | | | | | |

Gap: the design asked for one table-driven test that polls every manager twice in the same state and asserts no zero wait without a request. The branch has per-manager tests instead; the table-driven form is a follow-up.

### 7.2 Regression tests that fail on trunk

- `EventLoopContractRegressionTest.testMetadataErrorWakesParkedApplicationThread` (KAFKA-20397).
- `EventLoopContractRegressionTest.testIntervalAutoCommitDoesNotIncludeUndeliveredPositions` (KAFKA-18641 residual).
- `CommitRequestManagerTest.testCommitSyncExpiredWhileCoordinatorUnknownIsNotSentWhenCoordinatorDiscovered`, `testFetchOffsetsExpiredWhileCoordinatorUnknownIsNotSentWhenCoordinatorDiscovered` (C2).
- `OffsetsRequestManagerTest.testListOffsetsWaitingForMetadataUpdate_Timeout`, `..._ExpiredOnMetadataUpdate` (C2).
- `ConsumerHeartbeatRequestManagerTest.testInFlightHeartbeatWithExpiredIntervalDoesNotSpin` (KAFKA-21031).

### 7.3 Loop-driving tests that must stay green

- Unit, real thread or real client: `KafkaConsumerTest` (about 90 dual-protocol methods over `MockClient`), `ConsumerNetworkThreadTest`, `ApplicationEventHandlerTest`, `KafkaShareConsumerTest`, `NetworkClientDelegateTest`, `FetchRequestManagerTest`, `ShareConsumeRequestManagerTest`, `CommitRequestManagerTest` and `ConsumerHeartbeatRequestManagerTest` (real DNS resolution cases).
- Integration (embedded KRaft): `PlaintextConsumer{,Poll,Fetch,Assign,Subscription,Commit,Callback,Close}Test`, `ConsumerBounceTest`, `ConsumerIntegrationTest`, `ClientRebootstrapTest`, SASL variants; share `ShareConsumer*Test`; Scala `BaseConsumerTest`, `AuthorizerIntegrationTest`; Streams `EosIntegrationTest`, `RestoreIntegrationTest`, `KafkaStreamsCloseOptionsIntegrationTest`, `KafkaStreamsStaticMemberIntegrationTest`, `RebalanceProtocolMigrationIntegrationTest`. The timing-sensitive ones are `PlaintextConsumerPollTest.test*PollEventuallyReturnsRecordsWithZeroTimeout`, `test*MaxPollIntervalMsShorterThanPollTimeout`, `test*RecoveryOnPollAfterDelayedRebalance`, `PlaintextConsumerCloseTest.test*CloseWithDefaultTakesAtLeastFetchMaxWaitMs` and `PlaintextConsumerCommitTest.testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose`.

### 7.4 System tests

`tests/kafkatest/tests/client/consumer_test.py` (9 `group_protocol`-parameterized cases), `consumer_protocol_migration_test.py`, `share_consumer_test.py`, `streams_broker_bounce_test.py` on Jenkins. **Not yet run.**

## 8. Performance

What is claimed: the C1 fixes remove the busy loop in the blocked scenarios, and the C1–C3 changes do not change the cost of the consume and idle paths beyond noise. What is not claimed: any throughput or latency improvement.

Benchmarks added on the branch (`jmh-benchmarks/README-consumer-loop.md`):
- `ConsumerNetworkThreadPassBenchmark`: one `runOnce()` pass over `MockClient` and `MockTime` with real `RequestManagers`; scenarios `IDLE`, `BLOCKED` (FindCoordinator never answered), `BLOCKED_HEARTBEAT_INFLIGHT` (first heartbeat never answered), `CONSUME`, `RECOVERY`. Counters include `zeroWaitEmptyResults`, `zeroNetworkTimeoutPasses` and `zeroMaximumTimeToWaitPasses` per pass.
- `AsyncConsumerBrokerBenchmark`: real `KafkaConsumer` against a running broker; modes `consume`, `idle`, `unavailable`; counters `records`, `processCpuMillis`, `networkThreadCpuMillis`.

Trunk vs branch, interleaved A/B, same host. **[to be filled from jmh-ab/results/summary.md]**

| Scenario | Metric | trunk | branch | Expected |
|---|---|---|---|---|
| pass `IDLE` | ns/pass, zero-wait passes | | | within noise; zero-wait passes 0 |
| pass `BLOCKED` | ns/pass, zero-wait passes | | | zero-wait passes drop to 0 |
| pass `BLOCKED_HEARTBEAT_INFLIGHT` | ns/pass, zero-wait passes | | | zero-wait passes drop to 0 |
| pass `CONSUME` | ns/pass | | | within noise |
| pass `RECOVERY` | ns/pass | | | within noise |
| broker `consume` | records/s, CPU ms per M records | | | within noise |
| broker `idle` | process CPU ms/s | | | within noise |
| broker `unavailable` | process CPU ms/s | ~1.6 core (prior harness) | | large drop |

Only the blocked scenarios are expected to change. Run an A/A pair first to measure noise; laptop runs with a co-located broker have shown 30% drift inside one session.

## 9. Rejected alternatives

**S1. `NextPollCondition` / Signal scheduler.** Each manager returns a typed condition (`progress`, `retryAfter`, `awaitInput`) and the loop skips managers that are not ready. Measured on a real broker with an interleaved A/B harness: consume CPU per record +31.3%, process CPU +24.0%, network-thread allocation while idle +110%, throughput within noise, and no improvement in the coordinator-unavailable scenario (the improvement there came from a separate no-spin patch). Two liveness regressions were found in review: a member whose poll timer expired stayed `STALE` and never rejoined, and one partition in `AWAIT_VALIDATION` stopped fetches for all partitions. Skipping a manager moves the burden of proving liveness onto every owner. Evidence: `next-poll-condition/fable-review/REPORT.md` §1, §3.

**S2. Per-pass immutable snapshot with versioned waits.** The background thread publishes one snapshot per pass and the application waits on a version. Strong for C3, but it rewrites several wait paths, and positions advanced by the application thread are not in the snapshot. Not validated on trunk. Evidence: `codex-model-summary.md` §4.2 (deadline tree, ready set), `consumer-ng` R3 notes.

**S3. Operation / continuation framework.** Make each operation an object with scope, blocker and continuation. Makes C2 and C5 explicit, but adds a framework the reviewers have rejected before (PR #20521 review history) and its cost is the handoffs, not the data structures. Evidence: `reviewer-preferences.md` §4, `codex-model-summary.md` §4.2.

**S4. Extra post-I/O decision pass.** Poll the managers a second time after network I/O so responses are acted on in the same pass. Measured: local throughput −1.95% / CPU +6.71%; Jenkins runs 930/931 throughput −4.36% / CPU +5.67%; the ablation (removing the pass) moved the numbers the other way, so the effect is not attributable. Evidence: `codex-model-summary.md` §4.2.

**Skipping unready managers in general.** The seven manager `poll()` calls are under 1% of network-thread CPU (82–88% is `select` / socket read). There is nothing to save, and every skip needs a liveness proof (F1). Evidence: `next-poll-condition/fable-review/REPORT.md` §3.

## 10. Appendix

Evidence documents (in `experiments/kip-1371-fable/` unless noted):

| File | Content |
|---|---|
| `DESIGN.md` | Context, forces, contracts, alternatives, hypotheses |
| `core-inventory.md` | Trunk background loop, every manager's `poll` / `maximumTimeToWait`, cross-manager dependencies, zero-wait branch table, close sequence |
| `issues-traceability.md` | Every ticket: root cause from code, fix commit, trunk regression test, mismatches with the old KIP text |
| `termination-table.md` | C4: close sequence, 18 kinds of pending work × terminator × result × time bound |
| `thread-ownership.md` | C5: writer/reader table for `SubscriptionState`, `FetchBuffer`, `AsyncPollEvent` and other shared objects |
| `test-inventory.md` | Use case × consumer type matrix, loop-driving tests, gaps |
| `reviewer-preferences.md` | Reviewer evidence from 15 PRs; implications in §4 |
| `codex-model-summary.md` | What the earlier prototypes proved and disproved |
| `../next-poll-condition/fable-review/REPORT.md` | S1 review: regressions, real-broker A/B numbers, profiles |
| `jmh-benchmarks/README-consumer-loop.md` | Benchmark method |

Out of scope (each goes in its own MINOR PR with its own JMH numbers): `Selector.wakeup` only when the network thread is parked; per-pass `LinkedList` / `ArrayList` / `Optional` allocations in `runOnce`; a new `PollResult` per heartbeat pass in `STABLE`; removing the application-side 100 ms re-check in `pollForFetches` (a consequence of C3, to be done after the notification path has soaked); `ShareConsumeRequestManager` acknowledgement expiry; `SubscriptionState.seekUnvalidated` synchronization; `AsyncPollEvent` termination in `cleanup()`; Streams topology push close hook.
