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

This is an ordering-proof gap, not yet a demonstrated record-loss execution.
The next test must combine actual commit capture with collection paused after
position advancement but before record return, with auto-commit enabled, and
check whether the snapshot can include the undelivered position. It must also
cover the reconciliation/revocation path separately. Passing a snapshot-retention
test cannot close this gap, and simply moving the reconciliation marker is not
sufficient if a no-reconciliation fast path bypasses it.

If the interleaving is reachable, compare a scoped capture handshake against an
explicit application-provided offset snapshot. Preserve existing timeout,
wakeup, and record-delivery outcomes before choosing a mechanism; do not restore
a universal effect queue solely for this case.
