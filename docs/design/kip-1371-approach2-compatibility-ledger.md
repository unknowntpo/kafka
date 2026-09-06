# Approach 2: existing-behavior compatibility ledger

## Scope and verdict

2026-09-06. Candidate code: `0234b4f8fe` (production code unchanged since
`df756cee98`). Comparison baseline: `820533b870106cc0e0ac60e2076b8644d68bd85f`,
the parent of the first explicit-activation POC commit. This is a pinned local
baseline, not a claim about current upstream Kafka.

This first slice inventories Coordinator / Heartbeat / Commit. It supplements
the [historical issue inventory](kip-1371-issue-coverage.md); passing historical
regressions alone cannot establish compatibility for existing operations.
Fetch/application delivery and complete lifecycle/variant coverage remain separate
required slices. No production behavior is changed by this document.

Verdict: narrow owner capabilities can preserve useful existing contracts, but
the extra post-I/O manager pass is not yet demonstrated behavior-equivalent.
Passing candidate tests characterize candidate behavior; they are not a
baseline-versus-candidate equivalence proof.

## Intent and competing forces

- Preserve operation outcomes, scope, retry policy and bounded progress while
  reducing cross-manager knowledge required of an RM author.
- An earlier decision can improve responsiveness, but can also change which
  input or error wins. Earlier is not automatically compatible.
- Owner-version validation should prevent an old observation from invalidating
  a newer coordinator, without suppressing the old operation's own outcome.
- Avoid no-progress polling without losing the input that re-enables work.
- Retain local domain rules, including shutdown exceptions; a common admission
  entry does not prove all domain predicates are complete.

## Behavior ledger

Evidence below names tests in `clients/src/test/java/org/apache/kafka/clients/consumer/internals/`.
"Preserved structure" means source comparison supports the stated routing; it
does not imply every public-consumer schedule has been tested.

| Existing behavior / obligation | Approach 2 mechanism and evidence | Classification / remaining gate |
| --- | --- | --- |
| Discovery waits while a request is in flight; retries after backoff, and stops discovery when closing | Coordinator derives `NextPollCondition` from the same owner state before and after building a request. `CoordinatorRequestManagerTest` covers in-flight no-spin, network timeout, backoff and discovery responses. | Explicit activation representation; complete public-close discovery liveness remains open. |
| A commit queued with no coordinator waits, then becomes sendable after discovery | Commit returns `AwaitInput(COORDINATOR_CHANGE)` and suppresses an expired auto-commit application timer while blocked. `testAsyncCommitWhileCoordinatorUnknownIsSentOutWhenCoordinatorDiscovered` and `testExpiredAutoCommitAwaitsCoordinatorInsteadOfZeroApplicationWait`. | Intended no-spin change, not pure refactoring; public blocked-poll wake/recovery still needs integration evidence. |
| Pending commit and offset-fetch operations see a discovery fatal error before Heartbeat clears it | Immutable configured Coordinator → Commit → Heartbeat order; real-constructor pre/post-I/O tests in `testConfiguredLoopPreservesFatalErrorReadBeforeClear`. | Known normal-pass ordering preserved. Not arbitrary RM-order independence, and not a guarantee after an exception aborts a pass. |
| Once consumed, a discovery error is not permanently replayed to later operations | Commit's value handler fails its current unsent operations; Heartbeat retains the legacy consuming path. Same configured-loop test checks later discovery recovery and stable earlier failures. | Delivery scope remains dependent on the exact input/poll cutoff; see gate G1. No pub/sub migration is implemented. |
| Heartbeat skip, interval, backoff, leave and response policies remain domain-local | Regular/share use `AbstractHeartbeatRequestManager`; Streams has its own manager. All three heartbeat suites were rerun. | Preserved policy structure, but legacy `PollResult.EMPTY` / numeric constructors remain. Do not claim complete typed activation migration or all-variant loop equivalence. |
| A completed attempt reports its own result even when coordinator state changes | Captured owner version fences coordinator invalidation; Commit, regular/share heartbeat and Streams response paths pass the version to the owner. `testCapturedOffsetFetchResponsePreservesNewCoordinator` and the batched response-order tests cover named paths. | Intended stale-observation correction. Owner version is not member epoch, assignment generation, or proof of authority to retry a changed partition scope. |
| Normal commits respect in-flight reservation and backoff; close has a different drain policy | `tryAdmit` combines check/reserve/build. `testCommitAdmissionReservesOnceAndRechecksRetryState`; commit timeout/backoff/close tests. Close keeps `buildRequestWithoutAdmission`. | Preserved structure, not a universal admission framework. Do not silently impose normal backoff on close or timeout-first membership policy. |
| Sync, async and rebalance commits retain their different retry/error outcomes | Existing Commit tests cover stale epoch, retriable failure, expiry and async periodic recovery; batched tests characterize failure after poll timeout. | Component coverage, not proof of every public API completion race. G1 is specifically unresolved. |
| Rebalance retry ordinarily recaptures current positions | Default still calls `subscriptions.allConsumed()`; `testRebalanceRetrySnapshotPolicy` compares default and retained modes. | Known capture-safety gap. Retaining the first snapshot is an opt-in semantic alternative, not an adopted equivalent fix; see G2. |
| Completing an old commit does not authorize obsolete reconciliation after leave/rejoin | Existing Membership owner guards; prior lifecycle audit covers success/failure across leave/rejoin and a fixed-scope new-identity retry. | Inherited protection, not a new reactor guarantee. Those lifecycle tests were not part of this turn's nine-suite rerun. Changed ownership scope still requires evidence. |

## Priority gates before claiming compatibility

### G1 — Post-I/O pass: result winner and error audience

Baseline `ConsumerNetworkThread.runOnce` polls managers before I/O, then reaps
expired application events without another manager pass. Approach 2 can execute
one full manager pass after I/O, before that reaping, when running, with no queued
application input and at least one completed request.

Candidate `ConsumerBatchedDecisionTest` already records:

- `testPostIoFatalDeliveryPrecedesEndOfRoundApplicationTimeout`: discovery fatal
  error completes the operation before end-of-round application timeout.
- `testQueuedInputCutoffDeterminesWhichCommitsSeeDiscoveryFailure`: an operation
  arriving during I/O versus after the eligible post-I/O pass has a different
  error audience.
- `testFailureContinuationQueuesAnInputAfterTheCurrentErrorConsumption`: a
  continuation's later input does not inherit an already consumed error.

These are observed candidate outcomes. Source comparison makes a baseline
difference plausible, but the exact same schedules have not been executed against
the pinned baseline in this audit. They must not be described as a newly proven
public API regression or as automatically permitted nondeterminism.

Next validation: replay the same three schedules against both boundaries, assert
operation error identity, pending/completed state, notification count and recovery
of the next operation. Then trace any difference to the public API contract. If
the required outcome differs, either retain the original next-iteration boundary
or explicitly seek acceptance of the changed behavior; do not change it silently.

### G2 — Retry snapshot: safety versus freshness

The retained-first-snapshot experiment avoids the demonstrated mid-collection
recapture but changes offset freshness. Before adoption, validate actual broker
committed offsets and restart behavior, legitimate seek/ownership transitions,
deadline handling, and the documented application processing precondition.
See the [snapshot audit](kip-1371-auto-commit-snapshot-audit.md).

## Alternatives and resulting direction

Keep three choices distinct: retain the original next-pass boundary with narrow
capabilities; keep the bounded post-I/O pass with explicitly accepted outcomes;
or introduce stronger staged delivery after demonstrating a need. The present
evidence does not select the third option and does not justify adding a global
queue or dependency graph. Prioritize G1's differential check before expanding
the architecture. Continue the full behavior inventory even if G1 passes.

## Current validation receipt

On candidate `0234b4f8fe`, **614 tests across nine suites passed**, with zero
failures, errors or skips and retries disabled. Suites: CoordinatorRequestManager,
CommitRequestManager, ConsumerHeartbeatRequestManager, ShareHeartbeatRequestManager,
StreamsGroupHeartbeatRequestManager, ConsumerBatchedDecision, ConsumerAdmissionContract,
RequestManagers and ConsumerNetworkThread (each with the `Test` suffix).

JDK 17 / Gradle 9.7.1, `:clients:test --rerun`, offline, two workers, one test fork,
`-PmaxTestRetries=0`. XML totals were read from `clients/build/test-results/test`.
No new test was added, no baseline differential test or broker run was performed,
and historical counts are not added to this receipt.
