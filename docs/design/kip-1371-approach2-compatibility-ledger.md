# Approach 2: existing-behavior compatibility ledger

## Current contract after discussion

Keep `RequestManager.poll()`; do not introduce `preIO()` / `postIO()` methods.
The loop owns invocation timing. The working discovery-error contract is:

1. When Commit RM processes a coordinator error while the coordinator is unknown,
   it fails the currently waiting unsent operations selected by its owner logic.
   This is a processing-time boundary, not a snapshot of operations at response
   arrival and not cancellation of requests already sent to the broker.
2. Heartbeat's configured delivery path consumes the error after Commit has read
   it. Later operations do not inherit this consumed error; they await subsequent
   discovery and remain subject to their own result/timeout/close rules.
3. Repeated polls without a new discovery failure must not repeat that ErrorEvent
   handoff or rewrite already completed outcomes. A genuinely new discovery
   failure is a new observation and may be delivered again.

**Correction to the earlier recommendation below:** a different error audience
under an earlier processing boundary does not by itself justify removing the
post-I/O pass. Both invocation strategies can satisfy this contract. Retain the
comparison and its observed outcomes, but do not label the extra pass incorrect
or select its removal solely because those outcomes differ. Necessity, cost,
other ordering obligations and public API compatibility still need independent
evaluation. No production behavior or retry-snapshot policy is changed here.

The shared tests already cover queued-input cutoffs, callback-enqueued subsequent
operations, error identity, single ErrorEvent handoff, and public async recovery.
`testOperationAfterConsumedDiscoveryErrorUsesOwnTimeoutOrClose` adds four cases:
both invocation strategies, each with the later sync event either expiring or
encountering Commit RM's close signal while coordinator is unknown. The later
operation first remains pending, then receives its own TimeoutException or
CommitFailedException, not the consumed authorization failure. Additional polling
does not change either operation's selected outcome or repeat the ErrorEvent.

These are real-loop in-memory tests with controlled transport. The close cases
call `CommitRequestManager.signalClose()`; they do not prove complete consumer
close, in-flight request cancellation, or every offset-fetch/lifecycle path.
The timeout assertion concerns the application event's terminal outcome, not a
claim that every internal request was removed or could never reach the broker.
Historical receipts and recommendations below are retained as the audit trail;
this section supersedes their earlier default-boundary recommendation.

Validation for this contract follow-up: **239 tests in three suites passed twice**
(ConsumerBatchedDecisionTest, CommitRequestManagerTest, CoordinatorRequestManagerTest),
zero failures/errors/skips, retries disabled, JDK 17 / Gradle 9.7.1 offline.
Checkstyle and Spotless Java checks passed on the first run. This validates the
named processing-time cases; it is not a new benchmark or closure of the full KIP
issue inventory.

## Extra-pass benefit and admission-cost probe

`testExtraPassChangesAdmissionButNotFollowupTransportPoll` compares both
invocation strategies, with and without a controlled owner epoch update between
the completion round and the next round. It uses an internal future continuation
to register the followup, not a user callback executed on the network thread.

| Observation | Next-pass control | Extra post-I/O pass |
| --- | --- | --- |
| Followup built after first commit response in network poll 2 | No; waits in Commit RM | Yes; staged in delegate queue |
| Followup reaches MockClient | Network poll 3 | Network poll 3 |
| Full manager passes through round 3 in this fixture | 3 | 4 |
| Epoch changes from 7 to 8 between rounds 2 and 3 | Followup captures 8 at admission | Previously admitted followup retains 7 |
| Successful followup response | Completes its operation without duplicate request | Same |

Thus earlier admission does not save a network poll in this schedule. The new
pass performs another full manager traversal and extends the period during which
a built attempt may precede a subsequent owner change. The unchanged-epoch pair
is the control: both send the same offset and epoch. The changed-epoch pair
demonstrates captured-context timing, not an invalid request or data-loss proof.
It invokes `onMemberEpochUpdated` directly; it does not prove a full membership
transition or application command can produce every such interleaving. MockClient
send admission is not a measured socket write or broker acknowledgement latency.

The known positive effect remains earlier processing of manager-local results
and errors, as verified by the public commit/error tests. It is not a new guarantee
that user callbacks run immediately, nor proof of faster broker progress.
The loop's final aggregate-wait publication/latched notification also exists with
the extra pass suppressed; some state-dependent wait values may differ because
the managers have advanced, so this does not assert identical wait behavior.

Conclusion: both modes remain viable under the processing-time error contract.
Do not add `preIO` / `postIO` methods or another queue to explain this difference.
Adoption of the extra pass needs a concrete benefit beyond "one round earlier"
(for example, a required observation deadline or measured application-response
latency), weighed against extra manager work and earlier context capture. These
tests establish neither CPU cost magnitude nor a workload-level performance win.
The current experiment remains enabled as before; no default is changed here.

Validation: **103 tests in three suites passed twice**, zero failures/errors/skips,
retries disabled (ConsumerBatchedDecisionTest, ConsumerNetworkThreadTest,
NetworkClientDelegateTest). JDK 17 / Gradle 9.7.1 offline; Checkstyle and Spotless
Java passed on the first run. Four new cases provide the controlled comparison.
No production source or external benchmark was changed.

## Owner-version safety across decision boundaries

The existing `testLateHeartbeatInvalidationCannotClearRediscoveredOwner` is
expanded from two cases to eight: inline/deferred response application,
next-pass/extra-pass decision timing, and heartbeat-first/commit-first response
order. A heartbeat attempt captures the old coordinator version; the fixture
invalidates and rediscovers the owner before that heartbeat returns NOT_COORDINATOR.
A concurrent successful commit registers a followup operation.

All matrix cases assert the newer coordinator version survives, no replacement
FindCoordinator is staged from the stale observation, the original commit success
remains valid, and the followup is sent and completes successfully without a
duplicate commit. Only admission timing differs across decision boundaries.

`CoordinatorRequestManager.markCoordinatorUnknownIfCurrent` supplies the matching
mechanism: reject an observed version different from the owner's current version.
The test does not establish that arbitrary callbacks are safe, that all response
errors are fenced, or that a general operation-generation scheme is unnecessary.
Rediscovery is injected through the fixture's discovery helper, not a real broker
failover. The eight-way recovery matrix uses regular heartbeat; rerunning share
and Streams suites does not make their coverage identical to this matrix.

Design implication: this named cross-owner protection works with either decision
boundary and with inline callbacks; extra polling is not its prerequisite.
Continue to retain owner checks and captured request context regardless of the
event-loop scheduling choice. No new RM method, production queue or callback
deferral requirement is selected by these tests.

Validation: **607 tests in six suites passed twice**, zero failures/errors/skips,
retries disabled: ConsumerBatchedDecisionTest, CommitRequestManagerTest,
CoordinatorRequestManagerTest, ConsumerHeartbeatRequestManagerTest,
ShareHeartbeatRequestManagerTest and StreamsGroupHeartbeatRequestManagerTest.
JDK 17 / Gradle 9.7.1 offline; Checkstyle and Spotless Java passed on the first
run. The expanded matrix adds six cases to the prior two-case characterization.
Production source and external resources were unchanged.

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

### Integrated public commit follow-up and decision gate

`testPublicCommitSyncObservesDecisionBoundaryWinner` and
`testPublicCommitAsyncErrorAudienceAndRecovery` add four cases, both with and
without the extra post-I/O pass. They call real `AsyncKafkaConsumer` commit APIs,
enqueue their actual events, run the real `ApplicationEventProcessor`, Commit RM,
Coordinator RM, Heartbeat and `ConsumerNetworkThread`, and obtain responses from
MockClient. The bridge does not synthesize an event result or mark offsets ready.

The scheduling seam is explicit: a mocked ApplicationEventHandler enqueues each
commit and runs one background iteration synchronously before `add` returns.
This models background completion before the application starts waiting, using
one physical test thread, not two independently scheduled threads. Membership,
metadata, fetch, background ErrorEvent handoff and non-commit shutdown inputs are
controlled/mocked. The consumer and loop do not share all construction state;
these cases use explicit offsets and do not claim implicit-position capture,
normal bootstrap, application consumption of ErrorEvent, or complete close.
The comparison still suppresses the extra pass on current sources rather than
running the complete historical binary.

Observed public results:

- Zero-duration `commitSync`: next-pass control throws TimeoutException;
  post-I/O mode throws GroupAuthorizationException in the selected schedule.
- Two sequential `commitAsync` calls: first operation fails in both modes.
  The second receives the same authorization error in next-pass mode; in
  post-I/O mode it stays pending and later succeeds after coordinator recovery.
- A third commit succeeds after simulated recovery in both modes. Earlier
  failed callbacks retain their outcome and are delivered once. Recovery respects
  discovery backoff by advancing MockTime, then supplies successful discovery and
  commit responses. It is not a real broker authorization change.

This goes beyond the earlier disconnected facade seam: the loop now selects the
result that reaches the actual public method/callback. It establishes a public
observation difference in an integrated component schedule. It does not establish
that Kafka's API specification forbids both outcomes, nor demonstrate the exact
schedule against a real broker or the complete historical revision.

**Decision required before claiming behavior-preserving migration:** should the
new architecture preserve the existing next-iteration error audience, or accept
the earlier decision boundary as an intentional behavior change? Recommendation:
retain the next-iteration boundary as the default candidate, preserve the current
post-I/O experiment in history, and continue proving the narrower owner and
notification contracts. No such production change is made in this follow-up.
This is independent of the retained-retry-snapshot decision.

### Broader acceptance inventory (not closure)

| Area | Evidence exercised / inspected | Still required |
| --- | --- | --- |
| Coordinator / commit / heartbeat | Integrated public commit comparison and existing normal/error/backoff/admission suites | Select completion boundary; full pinned-baseline comparison; actual discovery/authorization recovery |
| Fetch / application observation | FetchCollector capture schedules; FetchBuffer synchronization; public poll metadata delivery and checkpoint tests | Broker committed-offset/crash proof; retained-snapshot freshness decision; no full at-least-once claim |
| Regular/share/Streams variants | Heartbeat, membership, ShareConsumerImpl, share fetch/buffer and Streams topology suites; separate source branches inspected | Equivalent whole-loop recovery per variant; raw-delay/EMPTY migration remains incomplete |
| Timeout/wakeup/close | ConsumerNetworkThread, AsyncKafkaConsumer and ShareConsumerImpl lifecycle suites; close remains its own pollOnClose path | Exact original KAFKA-18160/19357/18569 schedules and end-to-end acknowledgement/discovery deadlines remain open |

These categories cover the acceptance inventory, not every runtime interleaving.
Existing green regressions do not close the explicitly unverified original issues.
No new benchmark, remote job, broker, original KIP edit or production flag change
is part of this work. The semantic decision above is the stopping point, not a
claim that all verification is complete.

Broad regression receipt: **1,594 tests in 24 suites, zero failures/errors/skips**,
on the final behavioral test additions, JDK 17 / Gradle 9.7.1 offline, one test
fork, two workers, retries disabled. Selection (all names end in `Test`):
CoordinatorRequestManager, CommitRequestManager, ConsumerHeartbeatRequestManager,
ShareHeartbeatRequestManager, StreamsGroupHeartbeatRequestManager,
ConsumerBatchedDecision, ConsumerAdmissionContract, RequestManagers,
ConsumerNetworkThread, AsyncKafkaConsumer, ShareConsumerImpl,
ConsumerMembershipManager, ShareMembershipManager, StreamsMembershipManager,
FetchCollector, FetchRequestManager, ShareFetchRequestManager, FetchBuffer,
ShareFetchBuffer, NetworkClientDelegate, ApplicationEventProcessor,
ConsumerPublicationContract, ConsumerAsyncPollMetadata,
StreamsGroupTopologyDescriptionRequestManager. Counts were read from test XML;
the broad selection ran once. The four new public-loop cases also passed in a
prior focused run. Initial fixture failures (discovery backoff omitted) and a
test-class fan-out checkstyle violation were corrected before these green runs;
they were not production regressions or silently retried test failures.
After the final comment/import-order cleanup, the complete 51-case
ConsumerBatchedDecisionTest suite passed again with no failures/errors/skips;
clients Checkstyle and Spotless Java checks passed. A root-level Spotless task
lookup failed before execution and was corrected to the clients module task.

### Follow-up: application-facing propagation, not an integrated race proof

`ApplicationEventProcessor.process(SyncCommitEvent/AsyncCommitEvent)` connects the
Commit RM result to the event future after marking offsets ready.
`AsyncKafkaConsumer.commitSync` waits on that event future through
`ConsumerUtils.getResult`; `commitAsync` enqueues the user callback when the event
future completes. Future completion does not directly invoke user code on the
network thread.

Six new `AsyncKafkaConsumerTest` cases verify the facade seam:

- `testCommitSyncExposesAlreadySelectedBoundaryError`: an already completed
  authorization or timeout failure is propagated with the same exception identity,
  with both zero and positive public timeout. Zero timeout does not replace an
  already selected authorization result in this fixture.
- `testCommitAsyncDeliversSelectedBoundaryErrorOnlyWhenCallbacksAreDrained`:
  completing the event exceptionally queues but does not immediately invoke the
  callback; a subsequent empty-offset commit drains it on the application test
  thread, exactly once, retaining error identity.

The async timeout case tests generic failure delivery only: it does not assert
that an AsyncCommitEvent has the same reaper deadline as a SyncCommitEvent.
These tests deliberately mock ApplicationEventHandler and supply the selected
event outcome. They do not run the network loop, real broker, or competing
application waiter. Together with the boundary tests they show a plausible
application-visible consequence, not an end-to-end reproduction of that race.
An integrated public call plus controlled real loop and a full historical
baseline comparison remain open. No timeout priority or production policy changes
are selected from this evidence.

Validation: the six new cases plus the 47-case boundary suite passed first;
then both complete suites passed: **190 tests, zero failures/errors/skips**,
retries disabled, JDK 17 / Gradle 9.7.1 offline. The new cases therefore passed
twice; the full 190-case selection ran once. Production files were unchanged.

### Follow-up: controlled decision-boundary comparison

Three parameterized tests in `ConsumerBatchedDecisionTest` add eight cases:
`testDecisionBoundaryChangesTimeoutWinner`,
`testDecisionBoundaryChangesLaterCommitErrorAudience`, and
`testDecisionBoundaryPreservesContinuationErrorCutoff`.

Both modes use identical current production managers, real `runOnce`, MockTime,
MockClient responses and application inputs. The control stubs only
`completedRequestsInLastPoll()` to false, suppressing the extra manager pass.
Actual transport callbacks still execute inline. This isolates the effect of the
post-I/O decision boundary; it is NOT execution of the complete pinned historical
binary, nor a public-consumer or real-broker test.

| Same controlled schedule | Next-iteration control | Post-I/O pass enabled |
| --- | --- | --- |
| Sync event deadline equals round start; discovery authorization error arrives during I/O | Operation ends with TimeoutException; fatal error remains for next pass | Operation ends with GroupAuthorizationException; error consumed this round |
| Second commit is queued after the first round returns | Both pending commits receive the discovery error on next pass | First commit fails; second stays pending because error was already consumed |
| Second commit is queued during I/O | Both commits receive the error next round | Same: queued input suppresses post-I/O pass |
| First failure callback queues the second commit | Failure delivery is one round later; second commit does not inherit the consumed error | Same error audience, earlier failure delivery |

The tests check stable timeout/failure outcome after another iteration, exact
shared error identity where applicable, and one ErrorEvent handoff. They do not
prove broker recovery of the pending second commit; existing configured-loop
recovery coverage is a separate test, not a paired recovery result.

Conclusion: the added pass is observably significant at the component boundary,
not merely a performance optimization. This establishes the causal boundary
difference, not a public API violation. Local `KafkaConsumer.commitSync(Duration)`
documentation lists authorization and timeout outcomes, but is not by itself a
proof of which must win this controlled race. A real public API path and full
pinned-baseline comparison remain necessary before deciding compatibility.
No production fix or changed completion priority is selected here.

The focused suite passed with all eight new cases, retries disabled; the previous
614-case receipt below predates these additions and is not a current combined run.

On candidate `0234b4f8fe`, **614 tests across nine suites passed**, with zero
failures, errors or skips and retries disabled. Suites: CoordinatorRequestManager,
CommitRequestManager, ConsumerHeartbeatRequestManager, ShareHeartbeatRequestManager,
StreamsGroupHeartbeatRequestManager, ConsumerBatchedDecision, ConsumerAdmissionContract,
RequestManagers and ConsumerNetworkThread (each with the `Test` suffix).

JDK 17 / Gradle 9.7.1, `:clients:test --rerun`, offline, two workers, one test fork,
`-PmaxTestRetries=0`. XML totals were read from `clients/build/test-results/test`.
No new test was added, no baseline differential test or broker run was performed,
and historical counts are not added to this receipt.
