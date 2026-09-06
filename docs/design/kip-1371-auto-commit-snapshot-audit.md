# Approach 2: auto-commit snapshot audit

Scope: KAFKA-18641, Approach 2 production baseline `17b05c7433`.
This audit adds characterization tests, not a new production fix.

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

If the interleaving is reachable, compare a scoped capture handshake against an
explicit application-provided offset snapshot. Preserve existing timeout,
wakeup, and record-delivery outcomes before choosing a mechanism; do not restore
a universal effect queue solely for this case.
