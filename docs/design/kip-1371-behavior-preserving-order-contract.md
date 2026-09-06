# Approach 2: preserve behavior and make the execution-order contract explicit

Local work, 2026-09-06. Behavioral baseline: `971a163277`.

## Intent and correction

The architecture goal is to retain local state owners and existing operation
semantics while making cross-manager ordering an explicit loop contract. It is
not to make arbitrary manager permutations equivalent by changing error retention.

Experiment `1f8486eefc` separated the fatal fact from its application notification.
It removed one read-before-clear dependency, but also made newly queued operations
fail on a previously notified error. That is an observable policy change, not a
behavior-neutral refactoring. Commit `5e09508617` reverts it. The experiment and its
notes remain recoverable in Git history; it is not part of the adopted source tree.

## Existing mechanism, explicit contract

`RequestManagers` constructs an immutable ordered list. For regular and Streams
configurations, Coordinator precedes Commit, which precedes the corresponding
Heartbeat. `ConsumerNetworkThread.pollAndStageRequests` executes the list in order
for both normal pre-I/O and eligible post-I/O passes.

The contract for this slice is:

1. The coordinator owner exposes a discovery failure.
2. Commit handles the pending commit/offset-fetch operations before Heartbeat
   consumes and clears that failure for the application-error handoff.
3. The consumed error remains cleared. A later operation waits for discovery
   instead of inheriting a retained error. Recovery can admit it normally.
4. Already completed failures retain their original outcome after recovery.

The owner still decides what the error means. The configured order defines the
cross-manager sequence; the loop executes it. No central dependency graph,
topological sorting, new phase, retained error flag, or application-effect queue
is added. Production changes in this follow-up are documentation comments only.

This is an explicit existing ordering contract, not a new universal enforcement
mechanism. An author can still bypass the loop or add another consuming reader;
that requires contract review. The tests guard the known wiring and loop behavior,
not arbitrary future code. An exception or close that aborts a pass is outside
this normal-execution guarantee.

## Validation design

The component fixture now uses the real `RequestManagers` constructor instead of
a mocked list with test-authored ordering. Coordinator, Commit, Heartbeat, loop
and network delegate are real; unrelated managers are idle mocks, membership and
heartbeat payload are controlled seams, and transport is MockClient.

`testConfiguredLoopPreservesFatalErrorReadBeforeClear` covers two entry points:

- A failure already exists before the pre-I/O manager pass.
- A real discovery response callback produces the failure inside the delegate I/O
  poll, and the eligible full post-I/O pass processes it.

At the ErrorEvent handoff, both pending operation futures must already be failed,
and the owner's error must be cleared. They must carry the same error identity as
the event. Repeated polling must not duplicate notification. A later commit remains
pending until discovery recovers, then succeeds on its own broker-response fixture;
the earlier failure remains unchanged. These are component outcomes, not evidence
that an application thread actually consumed the mocked handoff.

The supplier tests additionally assert regular/Streams reader-before-consumer
ordering and list immutability. The original reversed-order characterization is
retained to demonstrate why the normal execution order matters, not to endorse the
alternative order as a supported behavior.

All new scenarios are isolated, use MockTime, and need no broker or wall-clock
sleep. Complete public-consumer lifecycle, all variant operation outcomes and
performance remain outside this slice. This follow-up does not prove the added
Approach 2 post-I/O pass is necessary or worth its cost versus the parent next pass.

## Negative control

A temporary production-list mutation put regular Heartbeat before Commit. Both
new component cases failed at the ErrorEvent handoff because the pending commit
had not yet completed exceptionally. The stack traces reached the same
`pollAndStageRequests` helper from pre-I/O `runOnce` and post-I/O `runOnce`
respectively. The mutation was then removed. It is not part of the final change.

This checks observable ordering, not just the numeric positions in a test-authored
list. It does not prove a runtime guard rejects arbitrary invalid configurations;
the production constructor and regression suite remain the enforcement boundary.

## Final validation

The restored final source/test tree passed **573 tests in eight suites twice**,
with zero failures, errors, or skips and retries disabled. Suites:
`ConsumerBatchedDecisionTest`, `CoordinatorRequestManagerTest`, `RequestManagersTest`,
`ConsumerNetworkThreadTest`, `CommitRequestManagerTest`,
`ConsumerHeartbeatRequestManagerTest`, `ShareHeartbeatRequestManagerTest`, and
`StreamsGroupHeartbeatRequestManagerTest`.

Both runs used JDK 17, Gradle 9.7.1, `:clients:test --rerun`, offline mode,
`--max-workers=2 -PmaxParallelForks=1 -PmaxTestRetries=0`. Checkstyle main/test,
Spotless Java and SpotBugs main passed or remained up-to-date. The final test
report is `clients/build/reports/tests/test/index.html`; the negative-control
report was replaced by these normal-order runs. No benchmark, remote push or
published KIP update was performed.
