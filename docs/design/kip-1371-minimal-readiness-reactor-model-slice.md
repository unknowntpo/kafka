# KIP-1371 Experiment: Minimal Readiness and Reactor Model Slice

## Status

This is an experimental design for branch `codex/reactor-readiness-model-slice`, based on
`codex/async-consumer-reactor-poc` commit `fd6ce3bc1d2664f35b7fe3c7b7c899e7da37eb33`.

It is not part of the main KIP. Code must prove the model before the main proposal adopts it. If the proof needs more
machinery than the failure requires, delete the experiment.

| Gate | Result |
| --- | --- |
| Question and delete unnecessary mechanisms | Passed |
| Coordinator and Commit code slice | Passed |
| Heartbeat dry run | Passed; no framework required |
| Independent review | Pending |

## Decision

Keep one small rule:

> A request manager derives both its local action and its next activation from one manager-local plan.
> `ConsumerReactor` remains responsible for cross-manager ordering, aggregate timing publication, and
> publish-before-effect ordering.

This combines the useful parts of the two reviewed designs without combining their class hierarchies.

## Why this is necessary

### The concrete failure

The same manager can encode the same eligibility rule twice:

```text
auto-commit action: cannot run because an earlier commit is in flight
application wait: timer expired, therefore wait 0
```

The first answer prevents duplicate work. The second can create a zero-timeout loop. Reactor boundary validation can
contain the invalid result, but it cannot remove the duplicated predicate that produced it.

### What remains necessary

| Mechanism | Why it remains |
| --- | --- |
| Manager-local plan | Prevents action eligibility and wait calculation from disagreeing. |
| `PollResult` production validation | Contains mistakes from legacy or unmigrated managers. |
| Owner snapshot and version | Prevents an old response built with coordinator version 7 from invalidating version 9. |
| `ConsumerReactor` | Finalizes ordered cross-manager facts, the global wait, and application-visible effect ordering. |

### What this experiment deletes

- No `WakeupSignal` or `SignalRegistry`: existing network completion, application-event enqueue, cancellation, and
  shutdown paths already wake the loop.
- No generic readiness registry: two private plans are cheaper evidence.
- No second readiness algebra: reuse `NextPollCondition`.
- No second epoch counter: reuse the state owner's version.
- No event bus, dynamic dependency graph, or global snapshot registry.
- No new operation ID: the application event and future still own operation identity.

## Minimal model

### One activation vocabulary

`NextPollCondition` answers why one manager should run again:

```java
PollImmediately                 // usable output exists now
RetryAfter(delayMs)             // time alone may make work legal
AwaitInput(cause)               // an existing input must change feasibility
```

The only causes needed by this slice are:

```java
NETWORK_COMPLETION
COORDINATOR_CHANGE
SHUTDOWN
LEGACY_UNSPECIFIED              // migration adapter only
```

The cause is diagnostic data. It does not register a callback or create another wakeup channel.

The invariants are:

- `PollImmediately` requires output that the reactor can consume in this phase.
- `RetryAfter` is finite and positive.
- `AwaitInput` has no timer deadline.

### One local plan per migrated work source

The plan is a deterministic projection of manager-local state. It answers:

1. what local step is legal now; and
2. when or why that work source next needs evaluation.

Coordinator example:

```text
planFindCoordinator(t0) -> send now
poll(t0) builds the request and marks it in flight
planFindCoordinator(t0) -> await network completion
PollResult carries the post-send condition
```

The pre-step and post-step values must remain distinct. Reusing “send now” after the request is in flight would create
an immediate loop.

Commit auto-commit example:

```text
timer not expired                    -> retry after remaining time
timer expired, no commit in flight  -> run now
timer expired, commit in flight     -> await network completion
```

Both auto-commit creation and `maximumTimeToWait(now)` read this same plan.

### `PollResult` remains the atomic manager output

Conceptually:

```java
record PollResult(
    List<NetworkCommand> networkCommands,
    List<ManagerEvent> events,
    NextPollCondition nextActivation
) {}
```

It reports the result after the manager's local step. It does not infer domain policy.

This shape is always invalid:

```text
no network command + no event + PollImmediately
```

The reactor keeps production validation for that boundary even after migrated managers make the state impossible at
its source.

### Cross-manager coordination remains separate

Local readiness cannot answer whether an old observation may mutate newer peer state. `ManagerEvent` remains the only
cross-manager fact channel:

```text
response callback
  -> versioned ManagerEvent
  -> ordered reactor phase
  -> composition-owned handler derives a targeted ManagerCommand
  -> the single state owner validates the captured version
```

`CoordinatorSnapshot.version` is the only coordinator version. Do not add an `EpochGuard` for the same owner.

## Responsibility boundary

| Component | Responsibility |
| --- | --- |
| Request manager | Own local state, plan local work, build requests, track in-flight work. |
| State owner | Publish a small immutable snapshot when peers need it; validate stale observations. |
| `RequestManagers` | Wire consumer variants and route typed facts or commands to one owner. |
| `ConsumerReactor` | Run stable phases, aggregate deadlines, publish `ReactorSchedule`, then execute `ReactorAction`. |
| `NetworkClientDelegate` | Accept transport commands, perform I/O, and deliver completions. |

The reactor does not own heartbeat, commit, fetch, regular-consumer, or share-consumer policy.

## One end-to-end story

```text
1. Commit reads CoordinatorSnapshot(node-1, version=7).
2. Its local plan permits one request.
3. poll() builds the request, captures version 7, and records it in flight.
4. PollResult carries the NetworkCommand and AwaitInput(NETWORK_COMPLETION).
5. ConsumerReactor stages the command and publishes ReactorSchedule.
6. The response reports that coordinator version 7 is unavailable.
7. Commit emits CoordinatorUnavailableObserved(version=7).
8. Reactor routes it to CoordinatorRequestManager through RequestManagers.
9. The coordinator owner applies it only if version 7 is still current.
10. Reactor runs the next full manager pass and publishes the new schedule before any visible action.
```

If coordinator version 9 was published before step 6, step 9 ignores the old observation. Local readiness and owner
version fencing solve different problems; neither replaces the other.

## POC scope and evidence

The branch changes only:

- `CoordinatorRequestManager`: `planFindCoordinator(now)` supplies send eligibility and the post-send condition.
- `CommitRequestManager.AutoCommitState`: `nextActivation(now)` supplies auto-commit eligibility and application wait.
- `NextPollCondition`: `AwaitInput` may retain one diagnostic cause.
- `PollResult`: a no-progress result can carry an already-derived activation condition.
- focused tests for these contracts.

No reactor manager-specific branch or new runtime coordination channel was added.

The proof covers:

| Scenario | Required result |
| --- | --- |
| FindCoordinator is sent | Exactly one command; post-send condition is `AwaitInput(NETWORK_COMPLETION)`. |
| FindCoordinator remains in flight | No duplicate command and no timer retry. |
| Auto-commit timer expires while a commit is in flight | No duplicate commit; application wait is input-driven, not zero. |
| Empty output requests immediate polling | Production boundary rejects it. |
| Old coordinator response arrives after rediscovery | Current coordinator remains unchanged. |
| Manager event creates an application effect | Schedule publication precedes the action. |

Focused validation passed for `CoordinatorRequestManagerTest`, `CommitRequestManagerTest`,
`NetworkClientDelegateTest`, and all 39 `ConsumerReactorTest` cases. The Gradle lifecycle also ran Checkstyle and
SpotBugs successfully.

## Heartbeat dry run

Heartbeat fits the same vocabulary without moving membership policy into the reactor:

| Heartbeat state | Local result |
| --- | --- |
| Pending peer observation | Publish the event, then await the next ordered phase. |
| Coordinator unavailable or membership says skip | Await an existing coordinator or membership input. |
| Poll interval expired | Build the leave heartbeat, then derive the post-send condition. |
| Heartbeat due and no request is in flight | Build one heartbeat, then await completion or the next interval. |
| Request in flight | `AwaitInput(NETWORK_COMPLETION)`. |
| Retry or heartbeat timer not expired | `RetryAfter(remainingMs)`. |

Heartbeat is more tightly coupled to membership than Coordinator or Commit. That is evidence against extracting a
generic registry now. Migrate it only if doing so removes real duplicated guards.

## Decision gate

Keep this slice only if independent review confirms all of the following:

- migrated work sources do not retain parallel eligibility and wait guards;
- `AwaitCause` remains data, not a hidden signal mechanism;
- `ConsumerReactor` gains no manager-specific policy;
- no second owner version or coordination channel appears;
- the code removes at least as much duplicated gating logic as it adds.

Otherwise delete or reduce the slice. Passing this POC proves feasibility, not the need for a generic readiness
framework.
