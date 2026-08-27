# KIP-1371 / ConsumerReactor design logic dry run — 2026-08-27

## Purpose

This is a review workbook, not an approved specification and not an implementation plan. It collects the open design
issues discovered in the KIP, the POC, the Fable reviews, the PR 23014 audit, and the subsequent design discussion so
that each decision can be revisited in dependency order before changing the authoritative KIP or production code.

No behavior is approved merely because it appears under a candidate model below. A decision becomes authoritative only
after it is accepted in the KIP and backed by the listed evidence gate.

## Review baseline

- Branch: `codex/async-consumer-reactor-poc`
- Reviewed HEAD: `4b11a43891b365f830ad3a378cc8d0d7e2c93c6d`
- Working tree at capture time: KIP and architecture-diagram edits are uncommitted; this workbook does not reinterpret
  those edits as approved design.
- Primary design: `docs/design/kip-introduce-consumer-reactor-state-management-event-processing.md`
- Prior implementation review: `docs/reviews/consumer-reactor-fable-review-2026-08-27.md`
- Prior readability review: `docs/reviews/consumer-reactor-fable-readability-review-2026-08-27.md`
- Latest fresh Fable review raw output:
  `/Users/unknowntpo/.claude/plans/perform-a-fresh-read-only-joyful-valiant.md`
- Relevant upstream fixes: [KAFKA-20854 / PR 23014](https://github.com/apache/kafka/pull/23014) and
  [KAFKA-20970 / PR 23227](https://github.com/apache/kafka/pull/23227).

## Source-of-truth rule

For this review:

1. Current code and executable tests establish current POC behavior.
2. The KIP establishes intended behavior only when it explicitly distinguishes current implementation from target
   migration work.
3. This workbook records questions, alternatives, and acceptance gates. It must not become a second specification.
4. Older review findings are reopened only when current code or current wording still supports them.

## Proposed review order

The issues should be reviewed in this order because later decisions depend on earlier vocabulary and ownership rules:

1. Manager output vocabulary and `PollResult` algebra.
2. Request-manager ownership and event handling authority.
3. One reactor iteration, event phases, scheduling, and action ordering.
4. Liveness and compatibility paths.
5. Operation identity, stale-work fencing, and bounded retention.
6. Public metrics, variant extensibility, documentation, diagrams, and tests.

## Candidate model to challenge

The discussion currently points toward this candidate, but only the facts-unification direction is agreed. Command and
next-poll names remain open:

```text
PollResult
├─ networkCommands  // zero or more transport intents
├─ managerEvents    // zero or more immutable manager-produced facts
└─ nextPoll         // exactly one manager-local re-evaluation condition
```

Possible type sketch:

```java
record PollResult(
    List<NetworkCommand> networkCommands,
    List<ManagerEvent> managerEvents,
    NextPoll nextPoll
) {}

sealed interface NetworkCommand {
    record SendRequest(UnsentRequest request) implements NetworkCommand {}
}

sealed interface NextPoll {
    record RetryAfter(long delayMs) implements NextPoll {}
    record NoDeadline() implements NextPoll {}
}
```

This sketch deliberately keeps commands, facts, and future wait constraints distinct. It does not imply a generic
event bus, a generic command queue, or dynamic dependency-graph traversal.

## Decision register

### DR-01 — Are `StateTransition` and `ManagerEvent` separate concepts?

- **Status:** Agreed direction; KIP and code are not aligned.
- **Current POC:** `PollResult` has both `Set<StateTransition>` and `List<ManagerEvent>`. Fetch produces
  `FETCH_BUFFER_HAS_DATA`, `FETCH_PREPARATION_FAILED`, and `FETCH_REQUEST_TERMINATED` as `StateTransition` values;
  coordinator observations use `ManagerEvent`.
- **Problem:** Both are immutable facts produced from manager-local state. The names force the producer to choose an
  ontology based on downstream handling, and `FETCH_BUFFER_HAS_DATA` is not necessarily a state transition.
- **Agreed direction:** All manager-produced facts use one explicit `ManagerEvent` family. Concrete event type and the
  deterministic handler mapping decide the handling rule.
- **Important constraint:** Unifying the fact type must not erase phase ordering. A fetch-data fact may select a
  post-publication application wake in the current phase; a coordinator-unavailable fact is retained until the next
  input phase and then offered to the coordinator owner before the next full manager pass.
- **Evidence gate:** Replace the parallel fields without changing request send order, schedule publication, wake
  coalescing, coordinator fencing, or bounded event retention. Existing fetch and coordinator regressions must pass.

### DR-02 — Is an unsent network request an event, a request, or a command?

- **Status:** Open naming and API decision.
- **Current POC:** A manager returns `List<UnsentRequest>`; `ConsumerReactor` immediately adds each value to
  `NetworkClientDelegate` before network polling.
- **Observation:** The value contains enough request context and completion behavior to execute, but it has not been
  sent, correlated on the wire, or completed. It is an imperative transport intent, not a fact that already happened.
- **Candidate:** Expose `NetworkCommand.SendRequest(UnsentRequest)` or rename the payload to
  `NetworkRequestCommand`. Avoid `ManagerCommand`, which sounds like a command addressed to a manager.
- **Alternative:** Keep `UnsentRequest` directly. This is smaller and already names the current transport state, but
  the `PollResult.requests` field should explain that it contains send intents, not transmitted requests.
- **Non-goal:** Do not wrap the value in `RequestCreated` merely to call everything an event.
- **Evidence gate:** One manager may emit multiple send commands; `NetworkClientDelegate` remains the sole component
  that resolves the target, attempts send, owns transport correlation and timeout, and reports completion.

### DR-03 — Should raw `timeUntilNextPollMs` become a typed next-poll condition?

- **Status:** Open; strong candidate.
- **Current POC:** `PollResult` uses a raw `long`, plus factories named `progress(...)`, `retryAfter(...)`, and
  `awaitEvent()`.
- **Problem:** `Long.MAX_VALUE`, zero, and positive values overload duration, no deadline, and immediate progress.
  `awaitEvent()` also sounds like a targeted event subscription even though every loop activation performs another
  full manager pass.
- **Candidate:** Exactly one `NextPoll` value: `RetryAfter(positiveFiniteDelay)` or `NoDeadline`. Immediate output and
  next-poll condition remain orthogonal, so a manager may emit a command/event and still provide its next deadline.
- **Naming question:** `NoDeadline` is semantically literal. `AwaitInput` communicates the enabling condition but may
  still imply targeted delivery. `AwaitEvent` should not remain unless the KIP explicitly defines it as “contributes no
  local timer.”
- **Evidence gate:** The type must make empty output plus immediate retry unrepresentable, permit progress plus a zero
  non-blocking network poll when intentional, and preserve finite-deadline withdrawal.

### DR-04 — What is the final `PollResult` algebra?

- **Status:** Blocked on DR-01 through DR-03.
- **Current POC:** `unsentRequests + stateTransitions + managerEvents + timeUntilNextPollMs` with compatibility
  constructors and runtime validation.
- **Candidate invariant:** Zero or more immediate outputs plus exactly one next-poll condition. Empty output with an
  immediate retry must be unrepresentable. A command/event is consumed or marked in flight before it can be emitted
  again.
- **Open question:** Does a `progress(...)` factory still add clarity after outputs and `NextPoll` are typed, or should
  construction use named builders/factories such as `withOutputs(..., nextPoll)`?
- **Migration requirement:** Compatibility constructors must not silently synthesize semantics that the strict model
  rejects. Their removal plan and error behavior must be explicit.
- **Evidence gate:** Table-driven tests cover every legal shape, every illegal shape, multiple outputs, zero-delay
  progress, deadline withdrawal, and bounded repeated polling.

### DR-05 — Who decides whether work can make progress?

- **Status:** Current invariant; retain.
- **Decision:** The request manager owns domain feasibility. For fetch, `AbstractFetch` derives fetch-domain blockers
  and `FetchRequestManager` maps them to output events/commands and a next-poll condition. The reactor does not infer
  assignment, coordinator, leader, in-flight, backoff, or buffer policy.
- **Reactor authority:** Validate generic result invariants, retain per-manager timing, order handling, publish the
  aggregate schedule, and execute the selected effects.
- **Risk if violated:** Fetch, heartbeat, commit, regular/share, and Streams rules leak into the shared kernel.
- **Evidence gate:** Adding a new manager-local blocker changes its manager and tests, not `ConsumerReactor`.

### DR-06 — If every fact is a `ManagerEvent`, who decides its handling rule?

- **Status:** Direction agreed; handler boundary open.
- **Decision so far:** The producing manager reports what it observed. It does not choose peer mutation or execute an
  application-facing effect directly.
- **Required authority split:**
  - `ConsumerReactor` owns the deterministic phase and publish-before-effect ordering.
  - The selected request-manager composition or protocol driver owns type-to-handler routing so the shared reactor
    does not branch on regular/share/Streams policy.
  - A mutable-state owner alone validates and applies an observation about its state.
- **Rejected shape:** A shared queue that peer managers poll competitively, or event types that expose live mutable
  peer objects.
- **Open design:** Define a small dispatch result that can stage reactor actions without allowing the composition to
  execute them before schedule publication.
- **Evidence gate:** One event type has exactly one explicit handler path; unknown types fail deterministically in
  tests; no recursive manager polling is introduced.

### DR-07 — What are the event phases after facts are unified?

- **Status:** Open and correctness-critical.
- **Current POC:** Fetch transitions are collected into same-phase actions. Cross-owner manager events are deferred and
  routed at the beginning of the next iteration. Network completions re-poll only their owning manager post-I/O.
- **Required behavior:** Unification must preserve these timing properties without relying on separate storage types.
- **Candidate processing:**
  1. Collect typed manager events from a `PollResult`.
  2. Classify them through a static composition-owned handler map.
  3. Retain owner observations for the next input phase.
  4. Stage application-visible effects but execute them only after the current schedule publication.
- **Risk:** Dispatching all events immediately can mutate owner state after the schedule reflecting the old state has
  already been published. Deferring all events can add unnecessary application latency.
- **Evidence gate:** Explicit tests cover one same-phase application effect and one next-phase owner observation.

### DR-08 — What exactly does `ReactorSchedule` schedule?

- **Status:** Clarified concept; wording still needs consolidation.
- **Decision:** It is an immutable global timing publication. It gives the latest time by which the reactor must resume
  and bounds network polling; it does not select an individual manager to run. The full ordered manager pass remains
  the correctness baseline.
- **Overdue semantics:** Deadlines are latest-by bounds, not queued timer events. If the next full pass starts after
  several deadlines, every manager is polled once in stable order; missed intervals are not replayed.
- **Potential naming follow-up:** `ReactorWaitPlan` or `ReactorTimingSnapshot` is more literal, but renaming is not yet
  justified by evidence.
- **Evidence gate:** Multiple manager deadlines, early network return, overdue deadlines, deadline withdrawal, and
  stable tie ordering are deterministic tests.

### DR-09 — Can pre-I/O work make the computed network timeout stale?

- **Status:** Confirmed implementation gap; not yet proposed as a KIP requirement.
- **Current POC:** `ConsumerReactor.runOnce()` captures `currentTimeMs`, calculates `pollWaitTimeMs`, executes pre-I/O
  collection/actions, and calls `NetworkClientDelegate.poll(pollWaitTimeMs, currentTimeMs)` without recomputing against
  the latest clock.
- **Example:** A 50 ms earliest deadline is computed at `t`; pre-I/O work or a long pause advances the clock to
  `t+130`; the delegate may still receive the old 50 ms timeout and block again. A 100 ms fetch deadline is not lost,
  but may be serviced later than its bound.
- **Candidate fix:** Immediately before network polling, recompute the published schedule's remaining timeout using a
  fresh clock and pass the fresh time into the delegate.
- **Evidence gate:** A deterministic time-advance test proves the delegate receives zero when the published absolute
  deadline has already elapsed before the I/O call.

### DR-10 — What does publish-before-effect currently guarantee?

- **Status:** Current guarantee is intentionally narrower than the target.
- **Current POC:** Migrated `ReactorAction` values and staged `BackgroundEvent` publication occur after the corresponding
  schedule publication. Direct fetch-buffer updates, generic futures, callback invokers, acknowledgement paths, and
  timeout reaping still contain compatibility paths.
- **Risk:** Stating a universal guarantee makes the KIP factually false. Stating only implementation details hides the
  target architecture.
- **Required wording:** Separate “current guaranteed boundary” from “Phase 3 migration target.”
- **Evidence gate:** Every migrated effect test records schedule generation before effect execution. Remaining direct
  paths are enumerated once in Migration.

### DR-11 — What is an operation identity?

- **Status:** Agreed semantic contract; no mandatory new class.
- **Decision:** One admitted application API operation remains stable across manager polls, schedule publications, and
  network attempts and receives exactly one terminal result. Existing application event plus future may be sufficient;
  there is no ID per `ReactorAction`.
- **Distinct identities:** Operation identity, transport request-attempt identity, owner snapshot version/scope, and
  broker protocol generation/member epoch must not be conflated.
- **Example:** One `commitSync` operation spans OffsetCommit attempt 1, coordinator rediscovery, and OffsetCommit
  attempt 2; only the final completion resolves the original `SyncCommitEvent` future.
- **Evidence gate:** Retry, cancellation, timeout, interruption, and close races each produce exactly one terminal
  outcome and cannot let an older completion mutate newer scoped state.

### DR-12 — Is the PR 23014 fetch busy loop solved?

- **Status:** Upstream specific bug fixed; POC has a structural guard; full manager migration remains incomplete.
- **Upstream:** PR 23014 is merged. It stops unconditional fetch-buffer wakeup when no request can be built and only
  immediate application progress is possible in the buffered-data case.
- **Current POC:** Fetch preparation classifies buffered data, no fetchable partitions, missing leader, reconnect
  backoff, request in flight, and buffer-drain waits. `FETCH_BUFFER_HAS_DATA` selects a wake; other blockers produce a
  finite retry or no local deadline. Generic `PollResult` validation replaces empty zero-delay output with a no-deadline
  wait.
- **Caveat:** Some fetch paths still use compatibility constructors; `retry.backoff.ms=0` must remain covered by
  production-contract tests.
- **Evidence gate:** Table-driven blocker coverage plus a no-application-input idle test with bounded reactor/network
  polls and zero unnecessary application wakes.

### DR-13 — Is the hidden auto-commit zero-wait loop solved?

- **Status:** Upstream follow-up open; POC contains repeated looping but retains legacy semantics.
- **Upstream:** PR 23227 remains open at capture time. It addresses an expired auto-commit/Streams heartbeat
  application wait that stays zero while the coordinator is unknown.
- **Current POC:** `CommitRequestManager.poll()` returns no local deadline while the coordinator is unknown, but commit
  and Streams heartbeat still opt into legacy `maximumTimeToWait()`. `ManagerPollCache` marks one expired compatibility
  deadline delivered, preventing the same zero from repeatedly waking both loops.
- **Current limit:** Containment may still permit one unnecessary zero-timeout poll/wake and does not replace the local
  compatibility rule.
- **Evidence gate:** Real commit/coordinator/reactor test with coordinator unknown, bounded application wakes, no
  repeated network `poll(0)`, and one commit after discovery; equivalent Streams heartbeat regression.

### DR-14 — What does `awaitEvent()` mean, and what happens on malformed output?

- **Status:** Semantics clarified; name and fallback behavior open.
- **Current meaning:** It carries no event identity and creates no manager-local deadline. Any application input,
  network completion, cancellation, shutdown, or bounded safety poll may activate the loop; the next full pass polls
  the manager again.
- **Problem:** The name suggests targeted subscription. Runtime replacement of malformed output with `awaitEvent()`
  can delay genuine work until another activation, bounded today by `MAX_POLL_TIMEOUT_MS`.
- **Candidate:** Replace the name with typed `NextPoll.NoDeadline`. Keep production diagnostics but decide whether a
  malformed legacy result should fail fast, log and wait, or use a separate bounded recovery policy.
- **Evidence gate:** No busy loop, no lost network I/O, bounded recovery from one malformed manager, and a test stating
  the maximum delay explicitly.

### DR-15 — Are the proposed metrics public, stable, and actionable?

- **Status:** Open; latest Fable review requests changes.
- **Current KIP/POC:** Four public-looking counters use the `reactor-` prefix: poll-result contract violation, manager
  poll failure, action failure, and application wakeup.
- **Issues:** Metric group, MBean, type, and full descriptions are absent from the KIP. `reactor` exposes an internal
  class name. The contract-violation counter may lose meaning after legacy constructors disappear. Raw wake counts need
  a comparison signal to be operationally useful.
- **Candidate direction:** Keep contract violations as rate-limited error diagnostics rather than a permanent public
  metric; consider one background-thread error counter for isolated internal failures; retain a clearly named
  application-thread wake counter only if its diagnostic interpretation is documented.
- **Evidence gate:** Compare with existing queue, queue-time, network-poll, poll-idle, and time-between-poll metrics;
  document a user diagnosis for every public metric and preserve naming compatibility across migration phases.

### DR-16 — Are owner snapshots and stale-observation fencing sufficiently general and bounded?

- **Status:** Coordinator vertical slice implemented; general extension rules incomplete.
- **Current POC:** Only `CoordinatorSnapshot` exists. It is latest-only and contains coordinator plus owner version.
  In-flight work retains the observed version and bounded request context. The owner rejects a stale unavailable
  observation.
- **Known documentation defect:** The KIP currently shows `CoordinatorSnapshot(READY, node-1, version=7)`, but the class
  has no `READY` field.
- **Extensibility concern:** `PendingManagerEvents.add()` hard-codes greatest-version retention for
  `CoordinatorUnavailableObserved`; a new event type otherwise risks silently receiving different coalescing semantics.
- **Required rule:** Coalescing policy is explicit per event type. Snapshots remain opt-in; there is no global snapshot
  registry or history.
- **Evidence gate:** Same-pass consumers observe a coherent version; older observation cannot invalidate a newer
  coordinator; retention remains latest-only and bounded; a second event type must declare its policy.

### DR-17 — Where do regular, share, and Streams differences live?

- **Status:** Shared kernel works; target driver boundaries remain proposed.
- **Current POC:** Separate reactor instances use `RequestManagers` compositions. The shared reactor has no consumer-type
  branch. Heartbeat/membership coupling remains protocol-specific and deeper than coordinator snapshot observation.
- **Question:** Before adding more event types, define which components form one ownership aggregate inside proposed
  `RegularConsumerDriver`, `ShareConsumerDriver`, and `StreamsConsumerDriver` boundaries.
- **Risk:** Adding a consumer variant expands constructor/optional/null unions; adding a dependency changes both
  `RequestManagers` and event routing.
- **Non-goal:** No dynamic dependency DAG. Stable full ordered traversal remains the correctness baseline; an optional
  static dirty-set index requires measurement first.
- **Evidence gate:** Run the same kernel contract tests with regular and share compositions; adding a new variant does
  not add `isShareConsumer` or equivalent policy branches to `ConsumerReactor`.

### DR-18 — Does the KIP tell one coherent story and distinguish current from target?

- **Status:** Improved, but current uncommitted edits and new decisions conflict.
- **Readability finding:** Earlier Fable review found the content sufficient but ordered incorrectly. The KIP has since
  moved the conceptual model earlier and added concrete examples.
- **Current contradiction:** The KIP now explains `StateTransition` as a separate concept, while DR-01 records the
  agreed direction to remove that distinction. It still uses `unsentRequests`, raw `timeUntilNextPollMs`, and
  `awaitEvent()` while DR-02 through DR-04 remain open.
- **Additional defects to verify:** Correct the nonexistent `READY` snapshot field; visually recheck sequence-diagram
  group labels and the origin of `FindCoordinator`; keep application-visible state outside the background-thread box;
  retain static diagrams only in the formal KIP.
- **Evidence gate:** A fresh reviewer can explain, after one pass: the problem, manager ownership, one `PollResult`, one
  reactor iteration, one coordinator-fencing story, operation identity, current POC scope, and migration gaps without
  source-code knowledge.

### DR-19 — Which tests are regression guards, and which prove the reactor?

- **Status:** Evidence exists but attribution must remain explicit.
- **Existing layers:** Unit/contract tests cover `PollResult`, deadline cache, schedule, actions, event coalescing, and
  fencing. Component tests use real request managers with controlled transport. The real-broker
  `testConsumerProtocolCoordinatorFailoverReactorRecovery` proves externally observable failover recovery for the
  consumer protocol.
- **Attribution rule:** A test for an upstream fix, such as paused-partition wake suppression from PR 23014, is a
  regression guard unless it directly exercises the new reactor boundary. Do not claim that passing an inherited
  behavior proves the new mechanism.
- **Missing gates after the candidate model changes:** Unified event dispatch; unknown event handling; network command
  execution; typed `NextPoll`; stale timeout recomputation; commit/Streams hidden zero-wait; same-version view in one
  pass; bounded retention with a second event type; regular/share kernel parity; current-HEAD performance gates.
- **Evidence gate:** Every accepted architecture decision maps to at least one repeatable assertion with precondition,
  action, observable result, and named test evidence.

## End-to-end logic dry run

The following stories are meant to expose contradictions in the candidate model before implementation.

### Story A — Fetch already has buffered data

```text
FetchRequestManager polls its fetch domain
  -> observes that a fetchable partition already has buffered records
  -> PollResult.managerEvents += FetchBufferHasData
  -> PollResult.nextPoll = NoDeadline

ConsumerReactor collects the event
  -> ManagerCoordinationPolicy evaluates the current-phase event batch
  -> FetchBufferHasData produces a WAKE_APPLICATION reaction
  -> publishes the current ReactorSchedule
  -> ConsumerReactor executes the wake last

Application thread resumes
  -> consumes the buffered records
```

Questions to settle: Is `FetchBufferHasData` coalesced once per phase or iteration? Which composition-owned typed
handler owns the event-to-action rule? How is a latched wake represented without making the event itself executable?

### Story B — Heartbeat observes a stale coordinator

```text
Heartbeat manager reads CoordinatorSnapshot(node-1, version=7)
  -> emits NetworkCommand.SendRequest(heartbeat targeting node-1)
  -> captures observedCoordinatorVersion=7 with that attempt

Network completion reports NOT_COORDINATOR
  -> heartbeat manager emits CoordinatorUnavailableObserved(version=7)
  -> no peer mutation occurs in the callback

ConsumerReactor retains the event
  -> next input phase routes it through the selected composition
  -> CoordinatorRequestManager compares 7 with current version
  -> applies only if 7 is still current

ConsumerReactor runs the next stable full pass
  -> coordinator may emit SendRequest(FindCoordinator)
  -> dependent managers observe the resulting current snapshot on a later pass
```

Questions to settle: Does one unified event list retain the necessary next-phase routing rule? Can an unsent command
built from an obsolete snapshot remain queued? Does the command carry only bounded target/version context?

### Story C — Two deadlines and a late reactor

```text
Fetch nextPoll = RetryAfter(100 ms)
Heartbeat nextPoll = RetryAfter(50 ms)
  -> ReactorSchedule retains absolute deadlines t+100 and t+50
  -> network poll is bounded by t+50

If the next full pass begins at t+130
  -> both deadlines are overdue
  -> every manager is polled once in stable order
  -> expired intervals are not replayed
  -> each old deadline is replaced or withdrawn by the new PollResult
```

Separate implementation edge: if the timeout is computed at `t` but pre-I/O execution advances the clock to `t+130`,
the current POC may still pass the stale 50 ms timeout to `NetworkClientDelegate`. DR-09 owns that issue.

### Story D — One commit operation across retries

```text
Application submits one SyncCommitEvent + future
  -> operation identity is admitted once

OffsetCommit attempt 1 observes NOT_COORDINATOR
  -> request-attempt identity ends
  -> operation remains pending
  -> coordinator-unavailable event is routed and fenced

Coordinator rediscovery completes
  -> OffsetCommit attempt 2 is emitted for the same operation

Attempt 2 succeeds
  -> publish corresponding schedule/state
  -> terminal ReactorAction completes the original future once
```

Questions to settle: Which remaining completion paths bypass `ReactorAction`? How are timeout, cancellation, close,
and a late response arbitrated so only one terminal result wins?

## ManagerCoordinationPolicy fit dry run

This candidate separates cross-manager decision logic from reactor mechanics:

```text
ManagerCoordinationPolicy.evaluate(events, snapshots)
  -> CoordinationPlan(managerCommands, reactorActions)

ConsumerReactor
  -> collects the ordered event batch
  -> freezes any opt-in immutable snapshots needed by the selected composition
  -> asks the policy for a plan
  -> applies owner commands at the defined input boundary
  -> publishes ReactorSchedule
  -> executes ReactorAction values after publication
```

The policy is not another event queue and does not own request-manager state. The selected regular/share composition
owns the policy and its typed handlers. `ConsumerReactor` owns when the policy is evaluated and when the returned
commands and actions become visible.

| Story | Policy input | Policy output | Fit verdict |
|---|---|---|---|
| A — fetch buffer has data | `FetchBufferHasData`; no cross-manager snapshot required | current-phase `WAKE_APPLICATION` | Fits, but proves that an event may require an immediate action in the same publication phase. Deferring every `ManagerEvent` to the next iteration could block the application behind an unbounded network poll. |
| B — stale coordinator observation | one or more `CoordinatorUnavailableObserved(version)` facts; optionally the current coordinator projection for diagnostics | one coalesced `InvalidateCoordinator(observedVersion)` command | Fits. The command is applied to `CoordinatorRequestManager`, which performs the final version check. The policy must not mutate coordinator state itself. |
| C — two deadlines | none | none | Intentionally bypasses the policy. `NextPoll` remains manager-local output and `ReactorSchedule` remains a generic reactor aggregation. Moving deadline calculation into the policy would recreate a God object. |
| D — commit retry | only the coordinator-unavailable observation produced by the failed request attempt | coordinator invalidation command | Partially involved. Commit operation identity, retry ownership, timeout, and exactly-once terminal completion remain with the admitted operation/commit domain and reactor action ordering; they are not policy state. |

### Historical-issue fit for a batch decision

The issue inventory does not justify inventing a global snapshot registry. It supports a narrower phase-level batch:

| Evidence | What must be combined | Batch-policy verdict |
|---|---|---|
| KAFKA-20854, KAFKA-20397, and fetch-position failure paths | Different fetch/offset facts may all require release of the same application fetch wait. | Genuine multi-event effect coalescing: collect the whole phase and emit one `WAKE_APPLICATION`. No owner snapshot is required. |
| KAFKA-19357 and KAFKA-18569 | Auto-commit, coordinator discovery, and leave work must occur in the correct close sequence. | Cross-component lifecycle coordination, but not a same-phase multi-snapshot rule. The established fixes sequence application close commands and let the commit/coordinator owners terminate their local work. |
| KAFKA-18641 | Membership reconciliation and auto-commit request generation must finish before the application begins fetching again. | A real phase barrier driven by one `PollEvent`; it belongs to application-input orchestration, not a `ManagerEvent` policy. |
| KAFKA-17066 and KAFKA-17674 | An in-flight offset operation's captured partition scope must be checked against current assignment. | Operation-context fencing against owner state, not correlation of several owner snapshots. |

Therefore the second slice must prove that events from several manager results are evaluated once as an ordered
phase batch and that equivalent effects are coalesced. Multiple owner snapshots remain opt-in and require a future
rule with direct evidence; they are not added speculatively.

### Phase constraint exposed by the stories

One event family does not imply one delivery phase. The reaction type determines the boundary:

```text
current manager pass emits ManagerEvent
  -> policy derives ReactorAction
       -> stage in the current phase
       -> publish schedule/state
       -> execute action

post-I/O owner poll emits cross-manager ManagerEvent
  -> policy derives ManagerCommand
       -> retain as bounded next-input work
       -> apply before application events and the next stable full manager pass
       -> the next network poll observes the new owner state
```

The prototype must reject or explicitly schedule any cross-owner command produced during the pre-I/O full manager
pass. Silently deferring such a command while entering a long network poll is unsafe. The first slice should prove
the currently intended case—coordinator-unavailable observations originate from response completion and are
published by the post-I/O owner poll—while keeping a fail-fast guard for an unexpected pre-I/O owner command.

### Boundary verdict before implementation

- **Accepted hypothesis:** a composition-owned `ManagerCoordinationPolicy` can prevent domain-specific event
  switches from accumulating in `ConsumerReactor`.
- **Required split:** `ManagerEventHandler` owns one event's reaction; the policy batches, coalesces, and combines
  handler results; the reactor controls phase ordering and execution.
- **Non-goal:** the policy does not calculate `NextPoll`, build requests, retain operations, mutate manager state, or
  become a runtime dependency graph.
- **Smallest vertical proof:** support `FetchBufferHasData -> WAKE_APPLICATION` and
  `CoordinatorUnavailableObserved -> InvalidateCoordinator`, then prove same-phase publication-before-wake and
  next-input stale-version fencing respectively.
- **Evidence gate:** tests must fail if a fetch wake is deferred behind network I/O, if the policy directly mutates
  coordinator state, if a stale observation invalidates a newer coordinator snapshot, or if a manager-specific
  event switch is added to `ConsumerReactor`.

### Prototype evidence — `codex/manager-coordination-policy-poc`

The isolated branch starts from POC commit `4b11a43891`. Its first two vertical slices implement:

- `FetchBufferHasData` as a `ManagerEvent` rather than a `StateTransition`;
- one `ManagerEventHandler` per supported fact;
- a composition-owned `ManagerCoordinationPolicy` that combines handler results into `CoordinationPlan`;
- `ManagerCommand.InvalidateCoordinatorIfCurrent` as data routed to the coordinator state owner;
- current-phase `ReactorAction` staging for fetch wakeup;
- next-input command application for post-I/O coordinator observations; and
- a fail-fast guard that prevents a pre-I/O manager pass from silently deferring a cross-owner command while stale
  requests or a long network wait remain possible;
- one policy evaluation for the complete ordered `ManagerEvent` set produced by each pre-I/O or post-I/O phase;
- `FETCH_PREPARATION_FAILED`, `FETCH_REQUEST_TERMINATED`, and `FETCH_POSITIONS_UPDATE_FAILED` as manager facts rather
  than the old `StateTransition` shape; and
- phase-local coalescing of different manager facts into one `WAKE_APPLICATION` action.

Observed evidence:

- `ManagerCoordinationPolicyTest`: 4 tests, 0 failures;
- `RequestManagersTest`: 5 tests, 0 failures;
- `ConsumerReactorTest`: 38 tests, 0 failures;
- `FetchRequestManagerTest`: 119 tests, 0 failures; and
- `OffsetsRequestManagerTest`: 37 tests, 0 failures; and
- Spotless Java, Checkstyle main/test, and SpotBugs main passed.

What this proves:

- `ConsumerReactor` can remain free of manager-event type switches while retaining publication and execution order;
- one event family can produce either a current-phase action or a next-input owner command;
- different event types from different manager results are presented to the policy as one ordered phase batch and
  produce only one application wake effect;
- the coordinator owner still performs the final stale-version check; and
- the existing regular consumer heartbeat rediscovery vertical component still routes invalidation before the next
  network poll.

What this does **not** prove yet:

- The policy now performs a real batch-level effect coalescing rule, but it still has no evidence-backed rule that
  must read several owner snapshots atomically.
- The prototype does not yet coalesce equivalent commands emitted by different producers in the same batch.
- `ManagerCoordinationPolicy.standard()` still selects one shared handler set; regular/share-specific handler
  composition has not been demonstrated.
- Compatibility support for legacy `StateTransition` remains in `PollResult` and `ConsumerReactor`, although all
  current producers of the three fetch/offset wake facts have migrated to `ManagerEvent` in this branch.
- Failing fast on an unexpected pre-I/O owner command is a safe experiment boundary, not necessarily the final
  production contract. A final design must either prove such commands cannot originate there or define a bounded
  re-evaluation mechanism that does not discard manager poll side effects.

Prototype verdict: phase-level batch evaluation is now real and useful for effect coalescing, so a policy/plan layer
is more than a handler lookup. It still must not claim multi-snapshot decision-making until a concrete rule requires
it. If no such rule emerges, snapshots should stay outside the policy except as bounded context carried by the
specific fact or command that needs fencing.

## First walkthrough checkpoint

The first review session should decide only DR-01 through DR-04:

1. Confirm one `ManagerEvent` family for all manager-produced facts.
2. Choose between direct `UnsentRequest` output and a named `NetworkCommand` abstraction.
3. Choose the typed next-poll names and exact semantics.
4. Write the legal `PollResult` shapes and invalid states without referring to current constructors.

Do not modify event routing, scheduling, metrics, diagrams, or tests until that algebra is accepted; otherwise later
changes will encode vocabulary that is still under review.
