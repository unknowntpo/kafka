# KIP-1371 Fable readability review — 2026-08-27

## Review metadata

- Reviewer: Claude Fable (`claude-fable-5`), read-only
- Perspective: a fresh Apache Kafka PMC-level reviewer familiar with Kafka consumer concepts but unfamiliar with this branch, its source code, prior discussions, and earlier reviews
- Reviewed artifact: `docs/design/kip-introduce-consumer-reactor-state-management-event-processing.md`
- Excluded evidence: source code and `docs/reviews`
- Scope: readability, narrative comprehension, terminology, and progressive disclosure; not implementation correctness
- Requested standard: a KIP reviewer must be able to understand the proposal from the document alone

## Executive verdict

> **Needs restructuring before circulating on dev@.**

Fable assessed the progressive-disclosure violation as **severe**. The document contains enough information, but introduces implementation-specific names and target/POC distinctions before readers have the conceptual model needed to interpret them.

| Dimension | Score (1–5) | Finding |
| --- | ---: | --- |
| Why / motivation | 4 | The KAFKA-20253 table communicates the failure clearly. |
| What changes | 2.5 | The answer is distributed across Summary, Proposed Changes, the responsibility table, and schedule sections. |
| One reactor iteration | 2 | Three overlapping lifecycle descriptions require readers to reconstruct the order themselves. |
| First-use terminology | 2 | 26 terms appear before their definition; at least 11 are not adequately defined. |
| Progressive disclosure | 2 | Internal method and field names appear before the conceptual contract. |
| Current-versus-target honesty | 4 | Accurate, but repeated often enough to interrupt the main narrative. |
| Overall | **2.5** | Content is sufficient; ordering and layering need restructuring. |

After one reading, Fable judged that a reviewer can explain **why** the KIP is needed, can only partially explain **what changes**, and cannot reliably explain **one reactor iteration** without rereading and mapping several sections together.

## Fable's concise retelling

The async consumer background thread already has an event loop and multiple request managers. However, each manager independently contributes answers to “how long may we wait?” and completion paths independently decide “when should the application thread be notified?” Those decisions may be based on different views of state. KAFKA-20253 demonstrates the resulting failure shape: heartbeat and coordinator work can both report a zero delay while neither can produce work, causing an empty zero-timeout loop.

The proposal names and narrows the responsibility of the existing background loop as `ConsumerReactor`. The reactor becomes the final authority for cross-manager timing and application-visible effect ordering, but does not own manager-local state or policy. Fable understood three principal mechanisms:

1. `PollResult` classifies local progress as `progress(...)`, `retryAfter(delay)`, or `awaitEvent()`. An empty result cannot request an immediate retry.
2. `ConsumerReactor` retains manager deadlines and publishes one immutable `ReactorSchedule`; it executes application-visible `ReactorAction` values only after that schedule is visible.
3. A manager reports a versioned `ManagerEvent` rather than mutating peer state. The reactor routes it to the single state owner, which rejects a stale observation by comparing the captured version with its current version.

The proposal does not change the protocol, public consumer API, thread count, or callback thread. Migration is phased; the current POC implements Phase 1 and a coordinator-focused Phase 2 slice.

Fable's central observation was that this retelling had to be reconstructed after multiple passes. It does not currently appear in one self-contained place in the KIP.

## Section-by-section progressive-disclosure audit

### Summary — medium

- `timeUntilNextPollMs`, `ReactorSchedule`, `ReactorAction`, “one poll snapshot,” and “thin reactor kernel” appear before a plain-language model exists.
- “Final authority for input ordering, cross-manager timing, and application-visible effect ordering” is accurate but combines three KIP-specific abstractions without concrete translations.
- Recommendation: keep the Summary in natural language and introduce identifiers in Proposed Changes.

### Motivation — medium; bug-evidence table is high

- The opening explanation and the KAFKA-20253 table are the strongest part of the document.
- “Application wait,” “network poll delay,” and “cached application wait” assume the reader already knows how the application thread waits for the background thread.
- The “Role in this KIP” column introduces `versioned scope`, `Phase 3`, `ReactorAction`, `PollResult`, staged `BackgroundEvent`, and publish-before-wake before any of them are defined.
- The paragraph about peer calls appears to rebut a causal claim before the reader knows that peer mutation is part of the proposal.
- Recommendation: keep Motivation focused on the failure and historical evidence. Move solution mapping to Test Plan or Migration.

### Public Interfaces — low

- The four metrics are understandable.
- “One count per phase-coalesced primitive wake” uses `phase-coalesced` before pre-I/O, post-I/O, and final-drain phases are explained.
- Recommendation: say when each metric increments in plain language.

### Proposed Changes introduction — high

This is the largest progressive-disclosure problem.

- The opening bullets use `ManagerEvent`, “stable manager pass,” `driver`, `ReactorSchedule`, `ReactorAction`, staged `BackgroundEvent`, and Phase 3 before defining them.
- The heartbeat lifecycle introduces `PendingManagerEvents`, `PollResult.managerEvents()`, post-I/O polling, `stagePollResult(...)`, `deferredManagerEvents`, and routing method names before explaining the abstract contracts.
- The table that explains which question each concept answers appears only after the lifecycle that depends on those concepts.
- Invariants and non-goals, which would give readers the necessary frame, also appear after the example.
- The paragraph defending the name `ManagerEvent` interrupts the request story.
- Recommendation: introduce the three mechanisms and their invariants first, then show a simple deadline example, and only then show the coordinator lifecycle. Move class-level paths to an implementation appendix.

### Responsibility boundary — medium

- The responsibility table is the clearest answer to “what changes” and should appear earlier.
- Its bullets largely duplicate the Proposed Changes introduction.
- Current POC wake coalescing limitations appear repeatedly in later sections.
- `ConsumerReactorGateway` is a code rename rather than a top-level KIP decision and should move to migration details or an appendix.

### Processing model — medium; PollResult subsection is low

- The eight-step iteration and the seven-step heartbeat lifecycle describe overlapping flows with different boundaries.
- Snapshot fencing repeats the first four lifecycle steps.
- The `PollResult` subsection is clear and self-contained: it presents the problem, the three legal shapes, the invalid shape, and examples. It should be introduced much earlier.
- `WAIT_FOREVER` and `FETCH_BUFFER_HAS_DATA` appear once without explaining whether they are aliases for `awaitEvent()` or a fourth result shape.
- Fetch waiter merging is implementation detail and should move to Phase 1 evidence or Test Plan.

### Operation identity — medium

- The identity table is valuable, but input sequence and reactor publication generation are not meaningfully used later.
- Operation identity is defined three times.
- The `commitSync` example is helpful but should focus only on what differs from the primary lifecycle example.

### Schedule and action ordering — low

- The schedule properties are concrete and testable.
- The publish-before-action invariant repeats an earlier invariant.
- TRACE fields, `ManagerPollCache`, and zero-timeout diagnostics are useful implementation evidence but not necessary in the main conceptual path.

### Shared reactor kernel — low

- The section is short and clear.
- The statement that proposed driver types do not yet exist duplicates an earlier statement.

### Reconnect-backoff example — low, but positioned too late

- This is the simplest demonstration of cross-manager timing: two deadlines, choose the earliest, do not wake the application.
- It should precede the coordinator lifecycle as the first example.

### Compatibility, migration, and tests — low

- The three-phase migration is clear and the test plan distinguishes current and target evidence.
- The document should state once, here, that the current POC is Phase 1 plus the coordinator slice of Phase 2.
- Named test methods and detailed code paths can remain as evidence, but they are not needed in the main explanation.

## First-use terminology audit

| Term | Fable assessment |
| --- | --- |
| request manager | Assumed known; add one phrase such as “a per-domain component for heartbeat, commit, fetch, or coordinator work.” |
| `ConsumerReactor` | Used in Summary, sufficiently defined later. |
| `timeUntilNextPollMs` | Used far before the `PollResult` contract. |
| one `poll()` snapshot | Not defined; replace with “one manager poll call.” |
| `ReactorSchedule` | Used far before definition. |
| `ReactorAction` | Used far before definition. |
| thin reactor kernel | Used before the regular/share boundary section. |
| application wait / network poll delay | Not defined. |
| cached application wait | Not defined. |
| versioned scope | Used before snapshot fencing. |
| Phase 1 / 2 / 3 | Introduced in Motivation before Migration. |
| `PollResult` | Used far before its clear subsection. |
| staged `BackgroundEvent` | “Staged” is not defined. |
| publish-before-wake | Used before the ordering contract. |
| peer calls / peer mutation | Not defined. |
| phase-coalesced | Used before execution phases are defined. |
| `ManagerEvent` | Defined shortly after first use, but in the middle of the lifecycle. |
| stable manager pass | “Stable” is never explicitly defined as fixed ordered traversal. |
| driver | Used before the boundary table. |
| publish-before-action boundary | Used before the action ordering section. |
| `CoordinatorSnapshot` | Used before owner-published snapshots are defined. |
| bounded context | Weakly defined later. |
| `PendingManagerEvents` | Code-level term used before its local explanation. |
| pre-I/O / post-I/O | Used before the iteration phases. |
| `RequestManagers` composition | Used before the responsibility table. |
| fence / fencing | Used before the fencing subsection. |
| `WAKE_APPLICATION` | Never defined and appears only once. |
| protocol aggregate | Not defined. |
| synthetic wakeup | Not defined. |
| compatibility path | Not defined. |
| capacity release | Not defined. |
| latch | Not defined. |
| `WAIT_FOREVER` / `FETCH_BUFFER_HAS_DATA` | Not defined; relationship to `awaitEvent()` is unclear. |
| `ManagerPollCache` | Not defined. |
| final-drain | Not defined. |

Fable counted 43 reviewed terms, with 26 used before definition and at least 11 lacking an adequate definition.

## Required restructuring

Fable classified these as required before broad KIP circulation:

1. Rewrite the Proposed Changes introduction to explain three mechanisms in plain language, followed immediately by the concept table, invariants, and non-goals.
2. Move the `PollResult` subsection forward so it is the first detailed mechanism.
3. Move the reconnect-backoff example before the coordinator lifecycle: simple timing first, cross-manager fencing second.
4. Remove implementation method and field paths from the lifecycle. Put them in an “Implementation mapping” appendix.
5. Move the bug-evidence table’s solution-mapping column out of Motivation.
6. Centralize current POC status and Phase 3 compatibility paths in Migration rather than interleaving them throughout the target model.
7. Define or remove the undefined terms, especially application wait, stable pass, synthetic wakeup, and `WAIT_FOREVER`.

Optional improvements:

- Merge the seven-step lifecycle and eight-step iteration, or make one a short cross-reference.
- Remove or demote identity categories that are not used later.
- Move fetch waiter, gateway rename, diagnostics, and named test paths to appendices.
- Reference the rejected raw-delay alternative from the `PollResult` section.

## Proposed replacement: Proposed Changes introduction

> This KIP keeps the existing application/background thread topology and renames the responsibility of the background loop: `ConsumerReactor` becomes the single place that decides, from one consistent view of all request managers, **how long to wait next** and **when the application thread may be notified**. Request managers keep their state and their rules; the reactor only orders their inputs, aggregates their timing, and sequences their application-visible effects.
>
> Three mechanisms carry that responsibility:
>
> 1. **Manager progress classification (`PollResult`).** Each manager poll returns one of three shapes: *progress* (it produced a request or a completed state transition, plus its next poll delay), *retry after a finite delay* (nothing now, but time alone will make it runnable), or *await event* (nothing now, and only another input can make it runnable). An empty result may not ask for an immediate retry; the reactor validates that shape and nothing else.
> 2. **One published schedule before any effect (`ReactorSchedule` → `ReactorAction`).** The reactor keeps every manager's absolute deadline, publishes the earliest as one immutable schedule, and derives both the network poll timeout and the application wait from that same publication. Application-visible effects—waking the application thread, completing an async poll—are `ReactorAction` values executed only after the schedule is visible, with the wakeup last.
> 3. **Cross-manager facts as routed events (`ManagerEvent`).** A manager that observes a change in state owned by another manager does not mutate it. It records a versioned fact; at the start of the next iteration the reactor routes the fact to the owning manager, which applies it only if the captured version is still current. Stale observations are dropped.
>
> The concepts these mechanisms introduce answer different questions. The following table defines them before the examples use them.

Fable recommends following this introduction with the current concept table, then the three invariants, non-goals, and a roadmap of the remaining sections. POC implementation status should appear once in Migration.

## Proposed replacement: heartbeat/coordinator lifecycle

> Consider a heartbeat sent while the coordinator becomes unavailable. Each numbered step uses one of the mechanisms defined above.
>
> 1. **Admit from current truth.** The heartbeat manager reads the coordinator owner’s current published snapshot—`(node-1, version 7)`—builds the request for `node-1`, and records version 7 with the in-flight request.
> 2. **Observe, do not mutate.** The response reports the coordinator unavailable. The heartbeat manager does not touch the coordinator manager. It records the fact *coordinator unavailable, observed at version 7* and returns it in its next `PollResult` together with any requests, transitions, and next-poll delay.
> 3. **Route to the owner.** The reactor holds the fact until the start of the next iteration, then hands it to `CoordinatorRequestManager` before draining application commands. Only that owner decides whether it changes coordinator state.
> 4. **Fence stale observations.** The owner compares version 7 with its current version. If rediscovery has already published version 9, the observation is ignored; it cannot invalidate the newer coordinator.
> 5. **Re-evaluate every manager.** The reactor polls all managers in fixed order. The coordinator manager returns *progress* with a `FindCoordinator` request; heartbeat and commit return *await event* because discovery completion is what will make them runnable. Requests are handed to the network layer but not yet sent.
> 6. **Publish the schedule.** The reactor publishes one `ReactorSchedule` with the earliest retained deadline. This is a deadline for the reactor, not a promise that network I/O happens then; I/O, application input, or shutdown may wake it earlier.
> 7. **Then execute effects.** Staged background events are moved to the application queue, `ReactorAction` values run (wakeup last), and the network poll blocks no longer than the published timeout. A network completion repeats steps 5–7 for the owning manager. A failed action is logged and counted; it cannot suppress later actions or the network poll. An iteration that only changes coordinator state publishes a schedule and no application-visible action.

The class-level path—`PendingManagerEvents` → `PollResult.managerEvents()` → reactor deferral → `RequestManagers.routeManagerEvents(...)`—should be listed in an implementation appendix for reviewers who choose to map the model to code. It should not be required for understanding the lifecycle.

## Duplicate or overly early details

| Content | Recommendation |
| --- | --- |
| Phase 3 compatibility-path lists | Keep once in Migration. |
| Target one-wake-per-iteration versus current per-phase coalescing | Keep once in the action section and once as a test target. |
| Proposed driver types do not yet exist | Keep once in the regular/share boundary section. |
| Coordinator fencing story | Keep one primary lifecycle; make commitSync describe only its differences. |
| Operation identity definitions | Keep the comparison table and one concrete example. |
| Reactor responsibility bullets | Keep the responsibility table as the authoritative version. |
| `stagePollResult`, `deferredManagerEvents`, and routing method paths | Move to an implementation appendix. |
| `ConsumerReactorGateway` rename | Move to Phase 1 implementation status. |
| Fetch waiter merging | Move to Phase 1 evidence or Test Plan. |
| `ManagerPollCache` and TRACE field details | Move to diagnostics appendix. |

## Final judgment

- **Why:** understandable after one reading.
- **What changes:** only partially understandable; the ownership boundary and interaction contract require rereading.
- **One iteration:** not reliably explainable after one reading because the document supplies three overlapping flows before their concepts are defined.

Fable’s concise diagnosis is:

> **The content is sufficient; the order is wrong.**

Applying the seven required restructuring changes should raise readability and comprehension to approximately 4/5 without changing the design itself.

## Recorder note

This file preserves the complete substance, scores, terminology audit, required changes, and replacement wording returned by Fable. Repeated line-number commentary was normalized into section references for long-term maintainability. No KIP or source-code change was made as part of this review.
