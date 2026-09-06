# Approach 2: auto-commit snapshot audit

Scope: KAFKA-18641, Approach 2 original audit baseline `17b05c7433`.
The sections below preserve the characterization history; the final section
describes a subsequent bounded production repair.

## Original repair and intent

Primary evidence is local Apache Kafka commit
`35a372c71db1dbe793460e211e6897953cec97d4`,
[PR 18737](https://github.com/apache/kafka/pull/18737).
The commit is titled "AsyncKafkaConsumer could lose records with auto offset commit".
The local patch, rather than an unavailable live PR page, was inspected.

The repair removes periodic auto-commit creation from ordinary manager `poll()`.
The application submits a `PollEvent` and waits for `reconcileAndAutoCommit()`
before collecting more records. The event processor triggers reconciliation and
periodic auto-commit, then marks that particular checkpoint complete. Explicit
commits without supplied offsets similarly use `offsetsReady()`.

The safety requirement is not merely commit latency: a snapshot must not include
position advancement for records the application has not received. Committing
such offsets can skip those records after restart. Snapshot readiness does not
mean the broker has acknowledged the commit, or all managers have settled.

## Forces and mechanism choice

- Preserve delivered-record/committed-offset semantics across two threads.
- Allow transport and application processing to overlap after safe capture.
- Keep position and commit policy local rather than introducing a global barrier.

An operation-specific capture checkpoint satisfies a narrower requirement than
Approach 3's universal effect boundary. Retaining current code is sufficient only
if the complete capture-before-collection relationship still holds. Owner version
checks protect stale observations; they do not establish this cross-thread order.
Moving all response callbacks to one phase would not establish it either.

## Current component evidence

`CommitRequestManagerTest` adds two deterministic, broker-free tests:

1. `testAutoCommitRetainsAdmissionOffsetsWhenPositionsAdvanceBeforeSend`:
   admit at position 100, advance position to 200, then build the actual
   `OffsetCommitRequestData`. Its committed offset must remain 100.
2. `testExpiredAutoCommitIsNotAdmittedByOrdinaryManagerPoll`:
   an elapsed interval alone does not let manager `poll()` read `allConsumed()`
   or emit an auto-commit request.

These characterize inherited safeguards. They do not reproduce a crash, assert
broker-persisted offsets, or establish that current application-event admission
always occurs at a safe point. The initial second-test run had a test-fixture
error (`verify` on a non-mock); that is not a production red-to-green result.

Validation on 2026-09-06: the complete `CommitRequestManagerTest` suite passed
twice (150 tests each, zero failures/errors/skips), with Java 17, offline Gradle,
one fork, and retries disabled. Checkstyle test validation and Spotless Java
checks passed. Main production sources are unchanged.

## Unresolved capture boundary

The current code no longer has the original blocking `PollEvent` protocol:

- `ApplicationEventProcessor.process(AsyncPollEvent)` completes the reconciliation
  checkpoint before `updateTimerAndMaybeCommit()`.
- `AsyncKafkaConsumer.collectFetch()` waits for that checkpoint only when
  reconciliation is pending. Its position-validation fast path can collect
  without waiting for the current event's validation stage.
- `testPollDoesNotWaitForReconciliationCheckIfNoPendingReconciliation` explicitly
  exercises collection with an unprocessed event, using a mocked collector.

The component experiment below establishes that capture can include the advanced
position while collection has not returned. It is not a demonstrated public-poll
record-loss execution. The reconciliation/revocation path must also be covered
separately. Passing a snapshot-retention test cannot close this gap, and simply
moving the reconciliation marker is not sufficient if a no-reconciliation fast
path bypasses it.

## Controlled two-thread interleaving

`FetchCollectorTest.testAutoCommitCaptureBeforeOrDuringCollection` uses a real
collector, buffer, subscription state, event processor, and commit manager.
Offsets/fetch request work is stubbed; the coordinator is known. Auto-commit is
enabled and its interval expires on a mock clock. A separate executor runs event
processing and request construction, with a bounded join and explicit shutdown.

Two schedules share the same data, positions, and commit configuration:

- Process the async poll event before collection: request offset is 0.
- Pause at the real exhausted-fetch `drain()` boundary, after the collector has
  advanced position but before it returns; process that event on the background
  executor: request offset is 10, for the ten records still inside collection.

The executor can complete capture at this boundary; synchronization of individual
state accesses does not make the entire collection/delivery operation atomic.
The test asserts existing behavior, including the unsafe-under-early-delivery
ordering, rather than representing a production fix. No fabricated position
update replaces the collector's real update.

Limits: event delivery is explicitly scheduled, not driven by a real
`AsyncKafkaConsumer.poll()` plus `ConsumerNetworkThread.runOnce()` fixture. There
is no broker acknowledgement, injected process crash, or restart/offset readback.
Thus the result demonstrates the component hazard; it does not on its own prove
that every public-poll gate permits this schedule or that records were lost.
The initial run omitted the test group's configuration and failed before reaching
the scenario; correcting that fixture is not a production red-to-green result.

The public-poll experiment below now exercises the collection fast path. Preserve
the two schedules as a counterexample/control pair.

Two-thread validation on 2026-09-06: 439 tests across `FetchCollectorTest`,
`ShareFetchCollectorTest`, and `CommitRequestManagerTest` passed twice, with no
failures/errors/skips and retries disabled. Checkstyle and Spotless Java checks
passed. The new test characterizes the hazard; a passing run is not a safety fix.

## Public poll and runOnce reproduction

`FetchCollectorTest.testPublicPollAutoCommitCaptureBeforeOrDuringCollection`
extends the experiment to actual `AsyncKafkaConsumer.poll(Duration)`, a real
input queue, `ConsumerNetworkThread.runOnce()`, transport delegate, and request
transmission to `MockClient`. The application handler is a queue-forwarding test
adapter. Unrelated request work and coordinator discovery remain mocked.

The fixture starts with an established manual assignment, buffered records, and
a position validated through `PositionsValidator.refreshAndGetPartitionsToValidate`
on the background executor. It asserts the real validator's fast-path predicate,
rather than mocking that predicate or fabricating a validation-complete event.
Auto-commit is enabled in both the consumer and commit manager.

- Control: run the queued poll event on the background executor during admission,
  before collection starts. The transmitted request offset is 0.
- Counterexample: retain that input until the collector's exhausted-fetch boundary.
  While public `poll()` is still inside the collector, execute `runOnce()` on the
  background executor. The transmitted request offset is 10.

Both public calls eventually return the ten records. The mock transport supplies
a successful offset-commit response before the paused collector proceeds. This
establishes a public-method/real-loop request-before-return schedule for this POC,
not broker durability or a tested crash/restart loss. There is no actual broker,
subscription/group-join setup, or group-reconciliation coverage. Queue admission
and scheduler timing are controlled, while the relevant poll gates, collector
position mutation, auto-commit capture, and runOnce ordering are production code.

The remaining design obligation is no longer just proving reachability for this
manual-assignment fast path. It is preventing the request from committing records
not yet eligible for auto-commit, while preserving buffered-record delivery,
timeout/wakeup semantics, and the separate reconciliation commit contract.
The shared-state owner and immutable captured map do not establish that boundary.
Do not call the characterization test a passing safety regression: its expected
offset 10 deliberately records the current hazard.

Validation on 2026-09-06: 572 tests across `AsyncKafkaConsumerTest`,
`CommitRequestManagerTest`, `FetchCollectorTest`, and `ShareFetchCollectorTest`
passed twice, with zero failures/errors/skips and retries disabled. Final
Checkstyle and Spotless Java checks passed. Production code remains unchanged.

## Scoped capture checkpoint repair

The preserved counterexample revision is `6c7a68ef59`. The subsequent repair
uses the existing per-poll reconciliation checkpoint for both reconciliation and
periodic auto-commit capture. It does not introduce a queue, general effect type,
or new global publication phase:

1. The event processor captures periodic auto-commit offsets before completing
   the checkpoint, while preserving reconciliation-before-periodic-commit order.
2. With auto-commit enabled, `collectFetch()` observes that checkpoint even if
   no reconciliation is pending and the position-validation fast path succeeds.
3. With no auto-commit and no pending reconciliation, the fast path stays intact.

The checkpoint promises capture readiness, not successful network transmission,
broker acknowledgement, or settlement of every manager. The existing checkpoint
name is retained in this bounded POC rather than conflating a rename with the fix.

Alternative considered: a separate snapshot of delivered progress could avoid
this wait, but introduces another state lifecycle across seeks, reassignment,
partial delivery, and rebalance commits. That is not selected without those
contracts and tests. This repair instead restores an operation-specific ordering
constraint already present in the original historical fix.

Cost: when auto-commit is enabled, a newly submitted unprocessed poll event can
delay buffered collection. A zero-duration poll may return empty until capture
completes; a subsequent call must still retrieve those records. This is an
explicit safety/latency trade-off, not a claim of unchanged performance. It waits
for local capture, never the broker commit result. Current-revision performance
and full broker/rebalance validation remain outstanding.

The public test is now named
`testPublicPollWaitsForAutoCommitCaptureBeforeCollection`. Delayed processing must
leave position at 0 and buffered records intact after a zero-duration poll;
after background processing, the next poll returns ten records and the transmitted
commit remains 0. The earlier low-level counterexample remains intentionally
unchanged: bypassing the application gate still permits unsafe capture timing.
`ApplicationEventProcessorTest.testAsyncPollEvent` also asserts the checkpoint
is not complete inside periodic capture. The existing wakeup test is expanded to
auto-commit enabled, asserting interruption before collector invocation and
unchanged position.

Negative control: temporarily restoring the old `hasPendingReconciliation`-only
collection condition caused the delayed public-poll safety assertion to fail
(records returned instead of empty); the immediate-capture control passed.
The new guard was restored before final validation.

Final repair validation on 2026-09-06: 680 tests in seven suites passed twice,
zero failures/errors/skips, Java 17, offline, one fork, retries disabled.
Suites: AsyncKafkaConsumer, CommitRequestManager, regular/share FetchCollector,
ApplicationEventProcessor, ConsumerAsyncPollMetadata, ConsumerBatchedDecision.
Checkstyle main/test, Spotless Java, and SpotBugs main passed. This does not close
the complete historical issue: positive-timeout/error-at-checkpoint races,
rebalance variants, broker crash/restart, and performance remain explicit gates.

## Capture wait cancellation and error follow-up

Baseline `28614d0e3f` exposed two additional failures in deterministic tests:

- `testCaptureCheckpointErrorIsNotMistakenForSuccessfulReadiness`: completing
  the event with an error at the wait boundary released its future normally;
  `poll()` returned records instead of throwing that error.
- The strengthened `testWakeupWhileWaitingOnReconciliationCheck(true)` showed
  that `WakeupTrigger` completed the owner checkpoint exceptionally. Its
  `isDone()` readiness predicate then returned true without background capture.

The repair registers a `CompletableFuture.copy()` as the cancellable active wait,
leaving the original background-owned checkpoint untouched. After a successful
wait return, the existing inflight-result handler checks for failure before
collection and clears a failed event so its error is not reported twice. This
is waiter/operation separation, not another work queue or a global effect phase.
The extra future exists only when an application actually needs to wait.

The wakeup test verifies that a second zero-duration poll still cannot collect
until the background completes the checkpoint. The error test verifies no
collector invocation, unchanged position, and no duplicate error on the next
call. `testCaptureCheckpointTimeoutPreservesRecordsForNextPoll` models expiry
of a positive-duration wait using MockTime and a deterministic getResult seam:
position remains unchanged and the source checkpoint incomplete; after completion,
a later zero-duration poll receives the fixture's record.

Evidence limits: these error/timeout tests use a mocked collector and controlled
wait boundary, not a real broker or wall-clock interleaving. They complement the
real-collector public-poll tests above, rather than replacing crash/restart or
full rebalance evidence. Thread interruption and simultaneous wakeup/error winner
ordering still require dedicated capture-checkpoint tests.

Validation on 2026-09-06: the focused four-suite run passed 447 tests, then the
broader seven-suite run passed 682 tests, all without failures/errors/skips,
with retries disabled. The affected error/wakeup/timeout tests ran in both.
Checkstyle main/test, Spotless Java, and SpotBugs main passed. Both negative
results above were observed before the repair; neither was a fixture failure.

## Interruption and competing wakeup/error outcomes

At production baseline `d85e8308dd`, three additional controlled cases pass
without a production change:

- Wakeup first while the wait copy is active, then event error: the first poll
  throws `WakeupException`; the next poll throws the original event error.
- Event error first completes the checkpoint and its copy, then wakeup: the
  first poll throws the original error; the next poll throws `WakeupException`.
- Interruption immediately before the real `Future.get` on the incomplete wait
  copy throws `InterruptException`, without completing the owner checkpoint.
  After clearing interruption, zero-duration polling remains gated; after
  background checkpoint completion the fixture's record can be collected.

`testCaptureCheckpointWakeupAndErrorAreBothObserved` asserts both orderings,
no duplicate third outcome, no collector calls, and unchanged position.
`testCaptureCheckpointThreadInterruptionDoesNotCompleteOwnerOperation` uses
the real thread interrupt flag and real future wait; a finally block clears
interruption so it cannot contaminate cleanup or later tests.

These tests control delivery at the `getResult` seam and use a mocked collector.
They establish the two serialized competing-outcome schedules at an active
capture wait, not exhaustive simultaneous-thread interleavings, broker durability,
or rebalance behavior. They preserve existing WakeupTrigger winner/pending rules,
rather than introducing a new global error-priority policy. The next main gate
is reconciliation/revocation, where commit capture and partition eligibility
must be validated together.

Validation on 2026-09-06: the three new cases passed the focused run. Fresh XML
reports from the broader regression contain 685 tests across seven suites, with
zero failures, errors, or skips. Both runs disabled retries and used Java 17,
offline dependencies, and one test fork. Spotless Java and test Checkstyle passed
in the focused run. No production code changed for this verification slice.

## Rebalance: initial capture versus retry capture

The next bounded question is whether the scoped capture contract also covers
rebalance commits. This is not a proposal for a global dependency graph or a
universal callback queue.

Existing protections in `AbstractMembershipManager.maybeReconcile`:

- Background `maybeReconcile(false)` cannot start reconciliation involving
  revocation or auto-commit. The application-event path supplies `true`.
- Revoked partitions are marked pending revocation before the initial commit
  capture. `SubscriptionState.isFetchable` then excludes those partitions.
- This restriction does not pause retained partitions.

`testReconcilePartitionsRevokedWithSuccessfulAutoCommitNoCallbacks` already
checks the initial mark-before-commit order. The new manager-level unit test
`testRebalanceRetryRecapturesRetainedPartitionOffset` uses real subscription
state and commit manager, with a simulated transport response. It establishes:

1. Revoked partition 0 has offset 5 and is not fetchable; retained partition 1
   has offset 10 and remains fetchable.
2. The initial rebalance commit captures retained offset 10.
3. After a controlled position change to 20 and a retriable response, the next
   built commit contains revoked offset 5 and retained offset 20.

Thus `autoCommitSyncBeforeRebalanceWithRetries` is a new capture boundary:
it reads `subscriptions.allConsumed()` in the completion callback, rather than
retaining the first capture. The per-poll initial checkpoint is not by itself
proof that this later capture is safe.

Evidence limit: the unit test uses `seek` to supply the changed position. It
does not establish that public `poll` can produce this position while delivery
is unfinished, nor that a broker durably commits it. The next experiment must
check that exact retained-partition/public-poll interleaving before calling it
a regression or choosing a repair. If reachable, compare scoped retry capture
against an application-provided safe offset view while preserving existing
rebalance retry semantics. Do not restore a universal effect queue solely for
this case.

Validation on 2026-09-06: the new characterization and existing membership-order
test passed together, then the full commit and consumer-membership suites passed
244 tests (151 + 93), with zero failures, errors, or skips and retries disabled.
The new test therefore ran twice. Spotless Java and test Checkstyle passed in
the focused run. This slice changes tests and documentation only.

## Public poll with an admitted rebalance retry

`testPublicPollRebalanceRetryCaptureBeforeOrDuringCollection` extends the same
public-poll runtime fixture used for the periodic-capture checkpoint. It uses
real `AsyncKafkaConsumer.poll`, `FetchCollector`, subscription state,
`ApplicationEventProcessor`, `CommitRequestManager`, `ConsumerNetworkThread.runOnce`,
and `NetworkClientDelegate`, with `MockClient` replacing broker transport.

The controlled setup directly admits a rebalance commit on the background
executor; it does not simulate group join or a real membership assignment.
The partition remains fetchable, representing the retained-partition case.
The initial request is verified to contain offset 0. The application-event
checkpoint is processed before collection, so the previous repair is active.

Two schedules distinguish capture timing:

- Before collection: deliver a retriable response, run the background loop,
  advance mock time past backoff, then send the retry. The retry contains 0.
- During collection: after the real collector advances position to 10, pause
  it at `CompletedFetch.drain`, before `poll` returns. On the separate background
  executor deliver the retriable response through `MockClient.respond` and
  `runOnce`, then send the retry after backoff. The retry contains 10 and receives
  simulated success while the application is still inside collection.

Both schedules then return ten records. This proves that the public-poll
collection path and an admitted rebalance retry can overlap despite the initial
checkpoint. It does not prove complete membership-driven reachability or durable
broker commit followed by crash/restart loss. Those are separate evidence gates.

The architectural conclusion is narrow: the safe-capture contract must cover
recapture on retry, not just initial admission. This does not yet choose between
deferring retry capture to an application-safe boundary and supplying a safe
application offset view. Freezing the original snapshot would change the existing
latest-offset retry behavior and must not be silently presented as equivalent.

Initial fixture failures (style checks and preparing a response for a future
request instead of responding to the already in-flight request) were corrected
before the successful schedules; they are not product regressions. Mock managers
also return empty close-poll results so teardown does not generate spurious errors.

Validation on 2026-09-06: all four public-poll schedules passed, then the five
affected suites passed 674 tests with zero failures/errors/skips and retries
disabled. The new schedules ran in both runs. Spotless Java and test Checkstyle
passed. Production code remains unchanged; these passing characterization tests
record the unsafe-capture possibility, not a repair or a safety acceptance gate.

## Membership-driven admission of the same retry

The test is now named
`testPublicPollMembershipRetryCaptureBeforeOrDuringCollection`. It replaces
direct commit admission with a real `ConsumerMembershipManager` in the shared
`RequestManagers`. The fixture seeds a subscribed assignment of partitions 0
and 1, both at offset 0, then supplies a heartbeat assignment retaining only 0.
Topic-ID/name lookup is mocked; membership transitions and reconciliation are not.

The actual application-event processor invokes `maybeReconcile(true)` during
`runOnce`. Membership marks partition 1 pending revocation and starts the commit
through its real reconciliation hook. Assertions confirm reconciliation is in
progress, partition 1 is no longer fetchable, and retained partition 0 remains
fetchable. There is no direct call to `maybeAutoCommitSyncBeforeRebalance` in
this fixture anymore. Both partitions receive explicit simulated commit results.

The same before/during-collection controls still produce retry offsets 0 and 10,
respectively. Therefore membership-driven admission does not by itself close the
retry-capture window. The earlier initial-capture checkpoint remains active.

Scope: this is a component-level reachability result from a seeded subscribed
assignment and a supplied heartbeat response, not an end-to-end group join.
The background event handler is mocked; callback completion, final assignment
installation, broker durability, and crash/restart remain outside this test.
No production behavior is changed. The next repair experiment should protect
recapture specifically, without silently replacing latest-offset retry semantics
with a frozen initial snapshot or introducing a universal effect queue.

Validation on 2026-09-06: the four focused public-poll schedules passed. After
making mock responses cover both requested partitions, the affected five-suite
regression passed 674 tests with no failures/errors/skips and retries disabled.
Spotless Java and test Checkstyle passed. Initial fixture compilation/style
errors were corrected before test execution; they were not product failures.
