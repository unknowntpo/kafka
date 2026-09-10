> 閱讀指南（給 owner，不貼到 Confluence）：
> 1. 結構依 `DESIGN.md` §9：只講五條 semantics 與它們修的 issue，實作機制放在每條的「Minimal change」。
> 2. Reviewer 讀完 §2 或 §4 就可以停；量測在 §8、替代方案在 §9、證據文件在 §10。
> 3. 已實作於分支 `fable/kip-1371-event-loop`（base trunk `74fbd50061`）：wait／operation／publication 改程式；close／callback 只寫下語意並用測試釘住。
> 3b. 內部文件（DESIGN、traceability）仍用 C1–C5 當交叉引用鍵，對應順序即上表；KIP 正文與討論串一律用名字，不用代號。
> 4. §8 的 loop-level 與 end-to-end JMH 都已實測；ducktape 系統測試尚未跑。
> 5. 待決事項：新 metric 可拿掉；KAFKA-21031 與 PR #23357 需與作者協調。

# KIP-1371: Wait, operation, publication, close and callback semantics for the async consumer background loop

## 1. Status

| Field | Value |
|---|---|
| Status | Draft |
| JIRA | [KAFKA-20995](https://issues.apache.org/jira/browse/KAFKA-20995) (umbrella) |
| Discussion thread | TBD |
| Author | [placeholder] |
| Baseline | trunk `74fbd50061` (2026-09-10) |
| Related tickets | KAFKA-20253, 20426, 20970, 21010, 21031, 21049, 20540, 19804, 20854, 17066, 17674, 15529 (PR #21476), 18641, 20397, 18160, 18569, 19357 |

## 2. Summary

`AsyncKafkaConsumer` (`group.protocol=consumer`), `ShareConsumerImpl` and the Streams group protocol share one background loop (`ConsumerNetworkThread`) and one set of `RequestManager`s. The rules that keep this loop correct are not written down anywhere, so the same bug keeps coming back in a different manager. This KIP defines five semantics for that loop, fixes the places where trunk does not hold them with the smallest change that works, and pins each one with a test. A semantics here says what a value, an operation or a callback *means* to whoever observes it; the rules an implementer follows to honour that meaning are stated under each one, and the tests are the proof.

| Semantics | What it defines | Closes |
|---|---|---|
| Wait | What a manager's two wait values mean. A zero wait means "I staged a request", never "I am blocked". | KAFKA-21031, 21049; turns the 20253/20426/20970/21010 pattern into a rule |
| Operation | What an operation's scope and deadline mean: the deadline bounds the observer, not the RPC, and a response is only applied inside the scope it was requested for. | Commits that never expire, ListOffsets retried forever, late responses after seek or unsubscribe |
| Publication and notification | What the application may conclude when it is woken, and which positions an auto-commit is allowed to treat as consumed. | KAFKA-20397, KAFKA-18641 residual |
| Close | What each kind of pending work means after `close()` returns: sent, failed with a named exception, or dropped. | KAFKA-18569, 19357 family (documented and pinned, no code change) |
| Callback and visibility | Where user code runs, what it may do there, what it may conclude about the state it observes, and what happens when it throws. | KAFKA-18160 family (documented and pinned, no code change) |

Public interface: one new metric and one behaviour change, in the operation semantics. No scheduler, no new thread model, no `Consumer` or `ShareConsumer` API change, no configuration change.

## 3. Motivation

### 3.1 Six failure shapes

The tickets under the umbrella fall into six shapes. Status is at trunk `74fbd50061`.

| Shape | Tickets | Status | One sentence |
|---|---|---|---|
| A. Blocked work reports zero wait | 20253, 20426, 20970, 21010 | fixed | Wait time comes from a timer that is only reset when a request is sent, so it reads 0 while a request is in flight, while the coordinator is unknown, or before the first heartbeat. |
| | 21031, 21049, 20540 | **open** | Same shape, not yet fixed. |
| B. Scope leaks across stages | 17066, 17674 | fixed | A later stage of an async chain reads the current assignment instead of the set captured at admission. |
| C. Publication order | 15529 / PR #21476 | fixed | No written order between position advance, record delivery, `isConsumed` and the auto-commit snapshot. |
| | 18641 | **partly regressed** | KAFKA-18376 removed the application-thread wait that provided the ordering, so an interval auto-commit can again read positions of records not yet returned. No trunk test covers it. |
| D. Error delivery and no-progress wakeup | 20854 | fixed | An empty fetch preparation woke the application for no reason. |
| | 20397 | **open** | The background thread marks the in-flight poll with a metadata error and does not wake the parked application thread. |
| E. Callback acknowledgement | 18160 | fixed | A `WakeupException` inside a rebalance callback skipped the `CallbackCompletedEvent`. |
| F. Close and termination | 18569, 19357 | fixed | Each manager decided its own close behaviour; two fixes then conflicted. |

### 3.2 The same method was patched four times

`AbstractHeartbeatRequestManager.maximumTimeToWait()` was changed by KAFKA-20253 (guard when the coordinator is unavailable), KAFKA-20970 (coordinator unknown returns a backoff), KAFKA-21010 (return `retry.backoff.ms` rather than the heartbeat interval, which is 0 before the first response), and is changed again by KAFKA-21031 (an in-flight heartbeat still returns 0). Each fix rebuilt the question "will `poll()` send anything?" by hand. The 20253 guard `!requestInFlight()` protected only the "heartbeat now" branch and let the in-flight case fall through to the same zero. That is what happens when the rule is not written down.

### 3.3 Measured cost

- Busy-loop family: with an unreachable coordinator, trunk burns about 1.6 CPU cores (1607–1725 process CPU ms/s) in the real-broker harness of the NextPollCondition review; the no-spin change alone removes about 93% of it.
- The scheduler alternative did not help there at all, and cost 31.3% more CPU per record while consuming (§9).
- Manager polls are not the cost: async-profiler shows 82–88% of network-thread time in `Selector.select` and socket read; the seven manager `poll()` calls together are under 1%.

## 4. Proposed changes

Each semantics has the same format: definition, where trunk does not hold it, minimal change, tests, tickets. The wait, operation, and publication semantics change code. The close and callback semantics are stated and pinned by tests without a code change.

### Wait semantics

**Definition.** `RequestManager.poll()` returns one of three results:

- `progress(requests)` — requests were staged; only this may ask for an immediate re-poll (`timeUntilNextPollMs == 0`);
- `retryAfter(delayMs)` — waiting for time to pass, `delayMs > 0`;
- `awaitInput()` — waiting for an in-flight response or for another owner's state; `Long.MAX_VALUE`, re-evaluated on the next network poll or application event.

`maximumTimeToWait()` bounds the application thread only by deadlines of actions the application thread itself must start and that are possible now: refreshing the poll timer, an auto-commit when the coordinator is known, a Streams topology push. Waiting for an in-flight request, for the coordinator, or for DNS is not such an action, so it returns `Long.MAX_VALUE` or the poll-timer bound.

**Where trunk does not hold it.**

- `HeartbeatRequestState.timeToNextHeartbeatMs`: with the timer expired and a request in flight it returns `remainingBackoffMs()`, which is 0 before the first response (KAFKA-21031). `AbstractHeartbeatRequestManager.poll()` and `maximumTimeToWait()` both forward it, so both threads spin until the response or `request.timeout.ms`.
- `AbstractHeartbeatRequestManager.maximumTimeToWait`: `pollTimer.remainingMs() / 2` rounds the last millisecond to 0.
- `FetchRequestManager.maximumTimeToWait` and `AsyncKafkaConsumer.pollForFetches`: return `retry.backoff.ms`, which may be configured to 0 (KAFKA-21049).

**Minimal change.**

1. `NetworkClientDelegate.PollResult.progress / retryAfter / awaitInput` named factories; `retryAfter` rejects non-positive delays. No new fields.
2. `NetworkClientDelegate.addAll(PollResult)`: a zero delay with no requests is counted in `network-thread-invalid-poll-result-total` and replaced by `max(1, retry.backoff.ms)`. A safety net and a detector, not a scheduler.
3. `HeartbeatRequestState.timeToNextHeartbeatMs` returns `max(1, retryBackoffMs)` while a request is in flight; `AbstractHeartbeatRequestManager` and `StreamsGroupHeartbeatRequestManager.maximumTimeToWait` bound the application only by `max(1, pollTimer.remainingMs() / 2)` in that case.
4. `FetchRequestManager.maximumTimeToWait` and the three re-check branches of `pollForFetches` use `max(1, retry.backoff.ms)`.

**Tests.** `ConsumerHeartbeatRequestManagerTest.testInFlightHeartbeatWithExpiredIntervalDoesNotSpin` (heartbeat interval 0 and 5000), `testMaximumTimeToWaitWithOneMsLeftOnPollTimerDoesNotRoundToZero`; `StreamsGroupHeartbeatRequestManagerTest.testInFlightFirstHeartbeatWithExpiredIntervalDoesNotSpin`; `FetchRequestManagerTest.testMaximumTimeToWaitBoundedToAtLeastOneMsWhenRetryBackoffIsZero`; `NetworkClientDelegateTest` (clamp, metric, three factories, negative delay); `AsyncConsumerMetricsTest`. The existing `...DoesNotSpin` tests from 20253/20426/20970/21010 are unchanged.

**Tickets.** KAFKA-21031, 21049; pattern of 20253, 20426, 20970, 21010, 20540.

### Operation semantics

**Definition.** An operation that spans several polls captures its scope (partition set, assignment) when admitted and checks it before applying a response. A stale result is dropped but still clears the in-flight state. The application's timeout, wakeup or cancel ends the observer, not the RPC: a request already sent completes or fails on its own. A request that could never be sent, and whose observer's deadline has passed, must not start later.

**Where trunk does not hold it.**

- `CommitRequestManager.RetriableRequestState.maybeExpire` requires `numAttempts > 0`, and the coordinator-unknown branch of `poll()` returns `EMPTY` without expiring anything. A `commitSync` that timed out while the coordinator was unknown is still sent once the coordinator appears, so the broker records an offset the caller was told did not commit.
- `OffsetsRequestManager.ListOffsetsRequestState` has no deadline: partitions with an unknown leader stay in `requestsToRetry`, are rebuilt on every metadata update forever, and keep their transient topics registered.

**Minimal change.**

1. `CommitRequestManager.PendingRequests.failAndRemoveExpiredRequests(includeNeverAttempted)` runs in two places. In `drain()` with `false`: only requests that were already attempted expire, so a request that can be sent now still gets its one attempt, exactly as trunk and the classic consumer do. In the coordinator-unknown branch of `poll()` with `true`: sending is impossible anyway, so never-attempted commits and offset fetches expire with `TimeoutException`. Closing with an unknown coordinator still fails with `CommitFailedException` first (KAFKA-19357). Auto-commit and `commitAsync` use `Long.MAX_VALUE` deadlines and never expire here.
2. `OffsetsRequestManager.fetchOffsets(...)` carries `ListOffsetsEvent.deadlineMs()` into `ListOffsetsRequestState`; `poll()` and `onUpdate()` first run `failExpiredRequestsToRetry`, which removes expired states and completes them with `TimeoutException`, releasing the transient topics. `currentLag` passes `Long.MAX_VALUE`, as it never retried.
3. Both sweeps allocate nothing when their queues are empty, which matters because the first runs on every poll while the coordinator is unknown (§8.1).

`ShareConsumeRequestManager.AcknowledgeRequestState.maybeExpire` keeps `numAttempts > 0`: it is a reusable per-node container with different semantics, and is out of scope.

**Tests.** `CommitRequestManagerTest.testCommitSyncExpiredWhileCoordinatorUnknownIsNotSentWhenCoordinatorDiscovered`, `testFetchOffsetsExpiredWhileCoordinatorUnknownIsNotSentWhenCoordinatorDiscovered` (both fail on trunk), `testExpiredCommitIsStillAttemptedOnceWhenCoordinatorIsKnown`, `testPollWithClosingAndExpiredPendingCommitFailsWithCommitFailedException`; `OffsetsRequestManagerTest.testListOffsetsWaitingForMetadataUpdate_Timeout` and `..._ExpiredOnMetadataUpdate` (both fail on trunk), plus late-response tests for positions, reset and validation after seek and unsubscribe.

**Tickets.** KAFKA-17066, 17674 (pattern); new behaviour, see §5.2.

### Publication and notification semantics

**Definition.**

- Order of background-visible effects: change state, publish (volatile or queue), then signal.
- After the background thread puts anything the application must handle into a queue or onto the `AsyncPollEvent` — records, a `BackgroundEvent`, a metadata or fatal error — it wakes a parked application thread.
- An interval auto-commit may only snapshot positions of records already returned by a completed `poll()`.

**Where trunk does not hold it.**

- `BackgroundEventHandler.add` enqueues without waking the application thread.
- `ConsumerNetworkThread.maybeFailOnMetadataError` marks the in-flight poll with the error and nobody wakes `FetchBuffer.awaitWakeup` (KAFKA-20397). The open PR #21991 adds a check before blocking, which still leaves a window.
- `ApplicationEventProcessor` calls `CommitRequestManager.updateTimerAndMaybeCommit(now)` while the application thread may be advancing positions in `FetchCollector` (KAFKA-18641 residual after KAFKA-18376).

**Minimal change.**

1. `BackgroundEventHandler` takes an `applicationWakeup` hook and runs it after enqueueing; `AsyncKafkaConsumer` and `ShareConsumerImpl` pass their fetch buffer's `wakeup`. `NetworkClientDelegate.wakeupApplication()` exposes the same hook.
2. `AsyncPollEvent` takes an `onError` hook; `completeExceptionally` writes the volatile error first, then runs the hook.
3. `FetchBuffer.wakeup()` sets the sticky flag first and takes the lock only when a thread is actually waiting.
4. `CommitRequestManager.updateTimerAndMaybeCommit(now, committableOffsets)`: an interval auto-commit driven by an `AsyncPollEvent` uses only the snapshot the event carries. The application thread captures `allConsumed()` in `checkInflightPoll`, before `collectFetch`, and only when the network thread asked for it through the shared `autoCommitSnapshotRequested` flag. If a commit is due and no snapshot was carried, the flag is set, nothing is committed, and the next poll carries one. The blocking overload used by `AssignmentChangeEvent` and by close still reads live `allConsumed()`.

**Tests.** `EventLoopContractRegressionTest.testMetadataErrorWakesParkedApplicationThread` and `testIntervalAutoCommitDoesNotIncludeUndeliveredPositions` (both fail on trunk); `BackgroundEventHandlerTest.testAddPublishesEventBeforeRunningApplicationWakeup`; `FetchBufferTest.testWakeupBeforeAwaitIsSticky`, `testWakeupRacingWithAwaitIsNeverLost`, `testWakeupReleasesParkedThreadPromptly`; four `CommitRequestManagerTest.testPollDrivenAutoCommit*` cases; `AsyncKafkaConsumerTest.testPollEventCarriesNoCommittableOffsetsWhenSnapshotNotRequested`; `ApplicationEventProcessorTest.testAsyncPollEventPassesCommittableOffsetsSnapshotToCommitManager`; `ConsumerNetworkThreadTest.testMetadataErrorOnAsyncPollEventRunsErrorHookAndSkipsProcessing`.

**Tickets.** KAFKA-20397, 18641; 15529 / PR #21476 (pattern).

### Close semantics

**Definition.** What each piece of work in flight means once `close()` has returned. Every kind of pending work has exactly one terminator and one of four outcomes: sent to the broker, failed with `CommitFailedException`, failed with `TimeoutException`, or dropped without a result. A caller can therefore say what happened to the commit it issued just before closing. The mechanism is one written sequence of ten steps — disable wakeups, take one shared close timer, auto-commit, stop FindCoordinator, run rebalance callbacks on the application thread, leave the group, await async commits, stop the network thread and run `cleanup()`, reap the background queue, close resources — and one table of 18 kinds of pending work in `termination-table.md`, rather than a `closing` flag interpreted separately by each manager.

**Where trunk does not hold it.** No caller-visible break. The table found one manager-level future with no terminator (an unsent OffsetFetch for positions at close: `clearAll()` clears it without completing it) and a few ambiguous rows: validation has no observable result, `AsyncPollEvent` is the only application event outside the reaper, and a Streams topology push has no close hook. None can hang a caller, because every application-side event is reaped, so this KIP changes no code here.

**Minimal change.** None; the semantics are stated and pinned by tests. If a test later shows a pending item with no terminator that can hang a caller, the smallest fix goes in a follow-up.

**Tests.** Eight `TerminationContractTest` cases: close with an unsent commit (coordinator known and unknown), the unsent OffsetFetch characterization, late ListOffsets and OffsetFetch responses after the assignment is released, rebalance callback events left in the background queue, and an `ApplyAssignmentEvent` failed by `cleanup()`.

**Tickets.** KAFKA-18569, 19357.

### Callback and visibility semantics

**Definition.** What a user may conclude about their own code and about the state it sees.

1. **Where it runs.** A user callback (`ConsumerRebalanceListener`, `StreamsRebalanceListener`, `OffsetCommitCallback`, interceptors, `AcknowledgementCommitCallback`) runs on the thread that called `poll()`, `commitSync()` or `close()`, inside that call, and never at the same time as another user callback. A user may therefore touch state that is not thread safe and may call consumer methods from inside a callback.
2. **Terminal outcome exactly once.** When a callback throws, including `WakeupException` and `InterruptException`, the background reconciliation still observes a terminal outcome for that callback, exactly once. The observable difference is a rebalance that continues instead of one that hangs.
3. **Visibility.** State the application observes through a consumer method includes everything the background thread published before it woke the application.
4. **Exclusive position window.** Between the moment `poll()` advances a position and the moment it returns those records, nothing else observes or acts on that position.

The mechanisms that deliver this are: request manager state is read and written only on the network thread and the application reaches it through `ApplicationEvent`s and their futures; the application completes a background future only through a `*CallbackCompletedEvent`; positions are advanced by the application thread in `FetchCollector`, while seek and reset run on the network thread while the application is blocked inside the API call; and a field with one writer thread and a different reader thread is at least `volatile` or `Atomic*`, while two writer threads mean one lock or one queue. The writer and reader table for `SubscriptionState`, `FetchBuffer`, `AsyncPollEvent` and the other shared objects lives in `thread-ownership.md`.

**Where trunk does not hold it.** Rule 3: `SubscriptionState.seekUnvalidated` and `TopicPartitionState.seekUnvalidated` are not synchronized, while every other entry point of that class is. An application thread reading a position through the synchronized path has no happens-before edge to the network thread's unsynchronized write, so it may read a stale position. Recorded for a follow-up, not changed here.

**Minimal change.** None; the semantics are stated and pinned by tests.

**Tests.** `TerminationContractTest` covers `WakeupException` and `InterruptException` thrown from each rebalance callback and from an `OffsetCommitCallback`, through both the direct path and `processBackgroundEvents`, plus the application thread being released from `ApplyAssignmentEvent` with an error rather than hanging. Existing `ConsumerMembershipManagerTest` callback-error tests are unchanged.

**Tickets.** KAFKA-18160.

## 5. Public interfaces

### 5.1 New metric

| Field | Value |
|---|---|
| Name | `network-thread-invalid-poll-result-total` |
| Group | `consumer-metrics` (consumer, Streams); `consumer-share-metrics` (share) |
| Type | `CumulativeSum` |
| Description | Number of request manager poll results that asked for an immediate re-poll without staging any request. Such results are clamped to `retry.backoff.ms` to avoid a busy loop. |

The metric detects a broken wait semantics. If reviewers prefer, it can be dropped and the clamp kept with a debug log, leaving this KIP with no public interface change.

### 5.2 Behaviour changes in the operation semantics

| Case | Before (trunk) | After |
|---|---|---|
| `commitSync(timeout)` times out while the coordinator is unknown | The caller gets `TimeoutException`, and the commit is still sent when the coordinator appears, so the broker records an offset the caller was told did not commit. | The commit expires on the network thread with `TimeoutException` and is never sent. Same for `maybeAutoCommitSyncBeforeRebalance` and the close-path `commitSync`. |
| An OffsetFetch for positions never sent while the coordinator is unknown, deadline passes | Sent later; the result is cached for a later call. | Fails with `TimeoutException`. `AsyncKafkaConsumer.updateFetchPositions` swallows it and returns false, so `poll()` throws nothing new. |
| `offsetsForTimes`, `beginningOffsets`, `endOffsets` with an unknown leader | Retried on every metadata update forever; transient topics stay registered. | Fails at the API timeout and releases the transient topics. The caller already saw `TimeoutException` from the reaper at the same moment; what changes is that no request is left behind. |
| A request that can be sent now with its deadline already passed, such as `commitSync(Duration.ZERO)` with a known coordinator | Sent once. | Unchanged: the sending path expires only requests that were attempted before. |
| An RPC already sent when the application times out | Not cancelled. | Unchanged. |
| A late OffsetFetch response for positions | Applied while the partition is still initializing. | Unchanged: the scope is the partition set, not the deadline. |

## 6. Compatibility, deprecation and migration

- No `Consumer`, `ShareConsumer`, callback or configuration change; no thread rename; existing metrics unchanged.
- Classic consumer (`group.protocol=classic`): untouched.
- Consumer and Streams: all five semantics. The operation semantics apply only here (`CommitRequestManager`, `OffsetsRequestManager`).
- Share consumer: the wait semantics through `HeartbeatRequestState` and the `addAll` clamp; the publication semantics through the wakeup hook passed by `ShareConsumerImpl`. Acknowledgement expiry is out of scope.
- KAFKA-21031: the change to `HeartbeatRequestState.timeToNextHeartbeatMs` overlaps PR #23357. Whichever lands first, the other is reduced to its test; this needs coordinating with the PR author.
- Migration: none.

## 7. Test plan

### 7.1 Semantics tests

| Semantics | Coordinator | Heartbeat (consumer / share / streams) | Commit | Offsets | Fetch | Loop and delegate |
|---|---|---|---|---|---|---|
| Wait | existing `testNoBusyPollWhileFindCoordinatorRequestInFlight` | new in-flight cases plus the existing `...DoesNotSpin` set | existing coordinator-unknown and DNS cases | — | `retry.backoff.ms=0` floor | `NetworkClientDelegateTest`, `AsyncConsumerMetricsTest` |
| Operation | — | — | expiry and closing cases | deadline and late responses | — | — |
| Publication | — | — | poll-driven auto-commit, four cases | — | `FetchBufferTest` | `BackgroundEventHandlerTest`, `ConsumerNetworkThreadTest`, `ApplicationEventProcessorTest`, `AsyncKafkaConsumerTest`, `EventLoopContractRegressionTest` |
| Close, callback | `TerminationContractTest`: close sequence, late responses after close, wakeup and interrupt inside rebalance and commit callbacks | | | | | |

Gap: the design asked for one table-driven test that polls every manager twice in the same state and asserts no zero wait without a request. The branch has per-manager tests instead; the table-driven form is a follow-up.

### 7.2 Tests that fail on trunk and pass on the branch

`EventLoopContractRegressionTest.testMetadataErrorWakesParkedApplicationThread` (KAFKA-20397); `...testIntervalAutoCommitDoesNotIncludeUndeliveredPositions` (KAFKA-18641 residual); `CommitRequestManagerTest.testCommitSyncExpiredWhileCoordinatorUnknownIsNotSentWhenCoordinatorDiscovered` and `testFetchOffsetsExpired...`; `OffsetsRequestManagerTest.testListOffsetsWaitingForMetadataUpdate_Timeout` and `..._ExpiredOnMetadataUpdate`; `ConsumerHeartbeatRequestManagerTest.testInFlightHeartbeatWithExpiredIntervalDoesNotSpin` (KAFKA-21031).

### 7.3 Loop-driving tests that must stay green

- Real thread or real client at unit level: `KafkaConsumerTest` (about 90 dual-protocol methods over `MockClient`), `ConsumerNetworkThreadTest`, `ApplicationEventHandlerTest`, `KafkaShareConsumerTest`, `NetworkClientDelegateTest`, `FetchRequestManagerTest`, `ShareConsumeRequestManagerTest`, and the real-DNS cases in `CommitRequestManagerTest` and `ConsumerHeartbeatRequestManagerTest`.
- Integration on embedded KRaft: the `PlaintextConsumer*Test` family, `ConsumerBounceTest`, `ConsumerIntegrationTest`, `ClientRebootstrapTest`, the SASL variants, the `ShareConsumer*Test` family, Scala `BaseConsumerTest` and `AuthorizerIntegrationTest`, and the Streams classes with a protocol axis (`EosIntegrationTest`, `RestoreIntegrationTest`, `KafkaStreamsCloseOptionsIntegrationTest`, `KafkaStreamsStaticMemberIntegrationTest`, `RebalanceProtocolMigrationIntegrationTest`). The timing-sensitive ones are the zero-timeout poll, `max.poll.interval.ms`, delayed-rebalance recovery, close-takes-at-least-`fetch.max.wait.ms`, and commit-fails-when-coordinator-unavailable-during-close cases.

### 7.4 Results so far

| Suite | Result |
|---|---|
| `:clients:test`, consumer packages | 3209 pass, 0 fail (trunk baseline 3174) |
| `:clients:clients-integration-tests`, consumer packages | 410 of 413 pass; the 3 failures pass on rerun and one also fails on trunk |
| checkstyle, spotless, spotbugs, RAT | clean |
| Fork CI (GitHub Actions, JDK 17 and 25) | compile and validate pass; JUnit matrix running |

### 7.5 System tests

`consumer_test.py` (9 `group_protocol`-parameterized cases), `consumer_protocol_migration_test.py`, `share_consumer_test.py`, `streams_broker_bounce_test.py` on Jenkins. **Not yet run.**

## 8. Performance

Claimed: the wait-semantics fix removes the busy loop in the blocked scenario, and none of the three code changes moves the cost of the consume and idle paths beyond noise. Not claimed: any throughput or latency improvement.

Two benchmarks were added (`jmh-benchmarks/src/main/java/org/apache/kafka/jmh/consumer/README.md`). `ConsumerNetworkThreadPassBenchmark` runs one loop pass over `MockClient` and `MockTime` with real request managers, and counts zero-wait passes. `AsyncConsumerBrokerBenchmark` drives a real `KafkaConsumer` against a running broker.

### 8.1 Loop level

Trunk `74fbd50061` vs branch, M1 Pro laptop, JDK 21, `-f 2 -wi 5 -i 5 -w 1s -r 2s -prof gc`, run order T,B,B,T per scenario. Raw data in `jmh-ab/`.

| Scenario | trunk ns/pass | branch ns/pass | trunk B/pass | branch B/pass | zero-wait passes, trunk to branch |
|---|---|---|---|---|---|
| IDLE | 316.6 ± 5.0 | 309.3 ± 10.8 | 240.3 | 240.3 | 0 to 0 |
| BLOCKED (coordinator unknown) | 316.8 ± 43.7 | 296.6 ± 20.5 | 223.7 | 223.7 | 0 to 0 |
| BLOCKED_HEARTBEAT_INFLIGHT | 332.6 ± 20.4 | 338.4 ± 9.1 | 320.2 | 320.2 | **0.968 to 0 per pass** |
| CONSUME (one fetch every second pass, 50 records) | 9192.4 ± 171.9 | 9001.4 ± 33.9 | 44563.5 | 44487.5 | 0 to 0 |
| RECOVERY (coordinator unknown, then known) | 277.0 ± 3.6 | 273.5 ± 6.0 | 256.1 | 240.1 | 0 to 0 |

Only the busy-loop scenario changes behaviour: trunk asks for a zero wait on 97% of passes while the first heartbeat is in flight, the branch never does. Everything else is within noise in time and identical in allocation.

The benchmark also caught a regression in an earlier build of this branch: BLOCKED and RECOVERY were 29% slower with twice the allocation per pass, because the operation-semantics expiry sweep copied empty queues on every poll while the coordinator was unknown. Guarding both sweeps with `isEmpty()` removed it, and the rows above come from the re-run.

### 8.2 End to end

Real broker on the same laptop, 2,000,000 records of 128 B, one partition, `-f 1 -wi 5 -w 2s -i 5 -r 5s`, interleaved T,B,B,T. Raw data in `e2e-ab/`.

| Mode | Metric | trunk | branch | Change |
|---|---|---:|---:|---:|
| consume | records/s | 2,671,455 | 2,750,425 | +3.0% |
| consume | process CPU ms/s | 465.22 | 474.92 | +2.1% |
| consume | network-thread CPU ms/s | 186.93 | 180.68 | -3.3% |
| idle | process CPU ms/s | 18.29 | 17.43 | -4.7% |
| idle | network-thread CPU ms/s | 7.38 | 6.52 | -11.6% |

Consume and idle are within noise, which is what this KIP claims. The detection floor is wide: this run can rule out a large regression, not a small one, because the page cache warmed monotonically across runs and the interleaving had only two pairs per mode. `benchmark-method.md` records the method and what it should have done instead. Inside a single variant, consume throughput moved from 1.22M to 4.21M records/s across runs as the page cache warmed, far more than the 3% between variants; the idle rows have n=2 and absolute values under 1% of a core.

The `unavailable` mode of this harness is **not** evidence in either direction and its numbers are not quoted here. It points the bootstrap at a closed port, so every connection is refused at once and the client sits in reconnect backoff, which trunk already handles correctly. The busy loop needs a peer that accepts the connection and never answers the heartbeat; that is what the loop-level `BLOCKED_HEARTBEAT_INFLIGHT` scenario builds and where the effect is measured. Teaching the end-to-end harness to do the same is a follow-up.

## 9. Rejected alternatives

**A scheduler that skips managers (`NextPollCondition` / Signal).** Each manager returns a typed condition and the loop skips managers that are not ready. Measured on a real broker with an interleaved A/B harness: consume CPU per record +31.3%, process CPU +24.0%, network-thread allocation while idle +110%, throughput within noise, and no improvement at all in the coordinator-unavailable scenario, where the gain had come from a separate no-spin patch. Review also found two liveness regressions: a member whose poll timer expired stayed `STALE` and never rejoined, and one partition in `AWAIT_VALIDATION` stopped fetches for every partition. Skipping a manager moves the burden of proving liveness onto every owner.

**A per-pass immutable snapshot with versioned waits.** The background thread publishes one snapshot per pass and the application waits on a version. Strong for the publication semantics, but it rewrites several wait paths, and positions advanced by the application thread are not in the snapshot. Never validated on trunk.

**An operation or continuation framework.** Make each operation an object carrying scope, blocker and continuation. This makes the operation and callback semantics explicit, but it adds a framework reviewers have turned down before, and the cost in this loop is the handoffs, not the data structures.

**An extra post-I/O decision pass.** Poll the managers a second time after network I/O so responses are acted on in the same pass. Measured: local throughput -1.95% with CPU +6.71%; Jenkins runs 930 and 931 throughput -4.36% with CPU +5.67%; removing the pass again moved the numbers the other way, so nothing is attributable. An A/A run of identical code on the same infrastructure showed a spread larger than every A/B delta.

**Skipping unready managers in general.** The seven manager `poll()` calls are under 1% of network-thread CPU, while 82–88% is `select` and socket read. There is nothing to save, and every skip needs its own liveness proof.

## 10. Appendix

| File | Content |
|---|---|
| `DESIGN.md` | Context, forces, semantics, alternatives, hypotheses |
| `core-inventory.md` | Trunk loop, every manager's `poll` and `maximumTimeToWait`, cross-manager dependencies, zero-wait branches, close sequence |
| `issues-traceability.md` | Every ticket: root cause read from code, fix commit, trunk regression test, mismatches with the old KIP text |
| `traceability-matrix.md` | Issue to semantics to implementation to test; use case by consumer type; semantics to files to evidence |
| `termination-table.md` | Close semantics: close sequence and 18 kinds of pending work |
| `thread-ownership.md` | Callback semantics: writer and reader for every shared field |
| `test-inventory.md` | Use case by consumer type, loop-driving tests, coverage gaps |
| `reviewer-preferences.md` | Evidence from 15 reviewed PRs and what it implies for this proposal |
| `codex-model-summary.md` | What the earlier prototypes proved and disproved |
| `benchmark-method.md` | How these numbers were produced, what the method got wrong, and the rules that follow |
| `jmh-ab/`, `e2e-ab/` | Benchmark raw data and summaries |
| `../next-poll-condition/fable-review/REPORT.md` | The scheduler review: regressions, real-broker A/B, profiles |

Out of scope, each with its own MINOR PR and its own numbers: `Selector.wakeup` only when the network thread is parked; per-pass `LinkedList`, `ArrayList` and `Optional` allocations in `runOnce`; a new `PollResult` per heartbeat pass while `STABLE`; removing the application-side 100 ms re-check in `pollForFetches`, which becomes possible once the publication semantics have soaked; `ShareConsumeRequestManager` acknowledgement expiry; `SubscriptionState.seekUnvalidated` synchronization; `AsyncPollEvent` termination in `cleanup()`; the Streams topology push close hook.
