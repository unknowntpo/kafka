# Cross-round error delivery: preserve the input and terminal-result boundaries

Local baseline: `05d703279a`, 2026-09-06. This investigation changes tests and
documentation only. No manager-event queue, dispatch implementation, public KIP
change, or new error-retention policy is introduced.

## Intent and forces

Allow staged work across `runOnce` calls while preserving operation outcomes,
local-owner decisions and necessary ordering. The iteration number is not itself
a correctness requirement, but moving work across an input or timeout boundary
can change observable behavior.

- Local reasoning favors passing explicit values instead of sharing a consumable field.
- Compatibility requires preserving which operations see an error, not merely its type.
- Bounded execution favors deferral rather than recursively draining new callbacks.
- Liveness requires pending handoffs to receive another execution opportunity, even
  when all managers report waiting for input.
- Shutdown and timeout may select terminal outcomes before a deferred event runs.

These forces do not establish that all event types need the same delivery phase.

## Existing paths and the operation-B counterexample

Let commit A already be waiting in Commit RM. A FindCoordinator failure F arrives
inside network I/O. B is a new AsyncCommitEvent, processed through the real
ApplicationEventProcessor in this component fixture.

| Input schedule | Round N | Round N+1 | Baseline result for B |
| --- | --- | --- | --- |
| B enters the application queue during I/O | Queued input suppresses the additional manager pass; F is not consumed | Process B's command, then Commit reads F and Heartbeat consumes it | B fails with F, along with A |
| B arrives after the eligible post-I/O pass | Commit fails A, then Heartbeat consumes F | Process B's command; there is no retained F | B waits for discovery |

Both schedules are legal in the existing POC. No public API bug is asserted.
`testQueuedInputCutoffDeterminesWhichCommitsSeeDiscoveryFailure` characterizes
them using real RequestManagers, application processor, loop, coordinator and
commit paths. MockClient controls response delivery and input arrival. No
production phase ordering is changed.

Two naive cross-round schemes would differ from one of these paths:

1. Always dispatch F before next-round application commands: B can escape F in
   the first schedule even though the baseline fails it.
2. Always process next-round application commands before dispatching F to all
   current pending operations: B can inherit F in the second schedule even though
   the baseline waits for recovery.

Capturing recipients at the discovery callback is not automatically equivalent:
B may be in the application queue but not yet registered with Commit RM. Operation
selection belongs to the receiving owner at a defined logical boundary, not to a
generic broadcaster guessing the contents of another manager's pending list.

## Callback-generated input

Completing A's future can synchronously execute a continuation. If that continuation
queues B through the application-command path during the post-I/O manager pass,
the pass continues and Heartbeat clears F. B is processed at the next input boundary
and does not inherit F. The fixture exercises this with
`testFailureContinuationQueuesAnInputAfterTheCurrentErrorConsumption`.

This is not evidence for recursively calling Commit RM directly from a continuation;
that bypass and mutation of the pending-operation collection are outside this probe.
An event dispatcher must define whether handler-generated work belongs to its
current batch or a later one rather than recursively expanding a delivery batch.

## Terminal-result boundary

`runOnce` handles the eligible post-I/O manager pass before calling the application
event reaper. The reaper times out only still-incomplete event futures. Consequently,
moving error completion to the next round can let the reaper select timeout first.
Once that future completes, later error delivery cannot rewrite its result.

`testPostIoFatalDeliveryPrecedesEndOfRoundApplicationTimeout` exercises a sync-commit
event whose deadline is due at the round's starting time. The coordinator is unknown;
the discovery response supplies F before the end-of-round reaper. This isolates the
ordering at that boundary, not every public commitSync timeout race.
The observed terminal result is GroupAuthorizationException, identical to the
ErrorEvent handoff, rather than TimeoutException. The hypothetical next-round
dispatcher is not implemented; the consequence of moving completion past the
reaper is a source-based inference, not a measured alternative-runtime result.

The normal loop also stops admitting iterations on close and enters cleanup. A
new deferred mailbox cannot assume another normal round will occur. Cleanup and
handoff failure semantics remain open; no new shutdown guarantee is inferred.

## Candidate contract, not a selected implementation

An inter-manager handoff needs four separately specified properties:

1. **Fact:** what happened, with enough source identity for owner validation.
2. **Eligibility boundary:** which admitted operations the receiving owner may affect.
3. **Execution boundary:** which inputs and terminal-outcome decisions must precede
   or follow delivery. A later iteration is permitted only within those constraints.
4. **Disposition:** delivery/consumption, handler-generated work, retry after handler
   failure, and close behavior. Pending work must not be stranded behind network wait.

These are review obligations, not a proposal for four new classes. An immutable
event value alone proves none of them.

## Alternatives and next gate

| Alternative | Benefit | Constraint or cost |
| --- | --- | --- |
| Keep the current ordered full passes | Known local behavior, no new transport mechanism | Reader-before-consumer coupling remains |
| Explicit value delivery at the current logical boundary | Could remove shared consumption without moving operation selection | Must preserve callback-visible order and error clearing; still needs routing |
| Defer all fatal-error handling to the next round | Simple iteration-level staging | Naive event-first/input-first schemes change B's result; timeout can win first |
| Select operation consequences at the existing boundary, defer only allowed downstream work | May retain outcomes while staging some work | Adds retained selection state and lifecycle obligations; not yet justified |

The next bounded gate is to distinguish **terminal operation-result selection**
from **later work that can legally be staged**. Do not implement a universal
next-round fatal-error dispatcher or freeze recipients at callback time merely
to fit an event-bus abstraction. If exact existing behavior cannot be retained,
that is a semantic decision for the user, not an implicit architecture change.

## Validation receipt

Final test sources passed **117 cases across five suites twice**, zero failures,
errors or skips, retries disabled. Suites: ConsumerBatchedDecisionTest (30),
CoordinatorRequestManagerTest (12), RequestManagersTest (2),
ConsumerNetworkThreadTest (27), ApplicationEventProcessorTest (46).
JDK 17 / Gradle 9.7.1; `:clients:test --rerun --offline --max-workers=2
-PmaxParallelForks=1 -PmaxTestRetries=0`. Checkstyle main/test, Spotless Java and
SpotBugs main passed or remained up-to-date. The real application processor now
replaces the previous processor mock in the component fixture. Unrelated managers
remain idle mocks, membership/payload are controlled seams, transport is MockClient,
and application-error delivery is a mocked handoff. No broker or wall-clock sleeps.

Production sources are unchanged from `05d703279a`. No runtime event dispatcher,
full-client regression, benchmark, remote push, or Confluence update is claimed.
