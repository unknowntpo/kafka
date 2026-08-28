<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# KIP-1371: Introduce a Consumer Reactor for State Management and Event Processing

## Status

Current state: Draft

Discussion thread: TBD

Jira: [KAFKA-20995](https://issues.apache.org/jira/browse/KAFKA-20995)

## Summary

The regular consumer and share consumer already use a background event loop, request managers, network I/O, event
queues, futures, and data buffers. However, no single component currently owns the final answer to three related
questions: in what order should inputs be applied, how long may the consumer wait before checking work again, and
when may the application thread observe the resulting effects? Separate paths can answer those questions from
different views of consumer state. This has contributed to busy loops, lost or duplicate wakeups, stale completion
handling, and state-publication races.

This KIP refactors the existing background loop into `ConsumerReactor`. Request managers continue to own their local
state and rules. Each manager reports whether it produced work now, must be checked again after a finite delay, or
cannot make progress until another event occurs. The reactor orders those inputs, combines the managers' timing
requirements into one published wait decision, and only then releases application-visible completion, notification,
or wakeup effects that have been routed through the reactor boundary. Direct data-buffer publication remains an
explicit migration path rather than an implicit claim of the current proof of concept.

The proposal does not add a thread, replace the existing event-driven topology, change Kafka protocols or public
consumer APIs, move user callbacks off the application thread, or move consumer-specific policy into the shared
loop. Regular, share, and Streams consumers use the same execution model while retaining separate protocol rules.
`ClassicKafkaConsumer` is not retrofitted into this model.

## Decision Requested

This KIP asks the community to approve three invariants, not the current POC class layout:

1. **Manager output is explicit.** A request manager reports produced facts or transport intents together with one
   typed next-poll condition. An empty result cannot request immediate retry.
2. **Cross-manager coordination has one owner.** Mutable state has one owner. Other managers use an immutable owner
   snapshot or report a versioned fact; they do not mutate that state directly.
3. **Publication precedes application-visible effects.** `ConsumerReactor` publishes the state-derived wait decision
   before completing, notifying, or waking the application for that decision.

The internal names and migration sequence may evolve as long as these invariants and the public compatibility
requirements remain true.

### Terminology and scope

| Term | Meaning in this KIP | Included in the proposal |
| --- | --- | --- |
| `ClassicKafkaConsumer` | The existing synchronous consumer implementation. | No. It remains a compatibility reference and is not moved to the reactor model. |
| `AsyncKafkaConsumer` | The event-driven regular-consumer implementation backed by the application/background thread split. | Yes. Its existing background loop is refactored into `ConsumerReactor`. |
| `ShareConsumerImpl` | The share-consumer implementation with share membership, acquisition, acknowledgement, and fetch rules. | Yes. It reuses the reactor kernel but retains share-specific policy. |
| Streams consumer paths | Streams group-protocol request managers, including heartbeat and topology-description work, that run on the async background model. | Yes where they use the shared background kernel; Streams protocol and task-assignment policy remain outside the reactor. |
| `ConsumerReactor` | The existing background execution loop after its ordering, timing-publication, and effect boundaries become explicit. | Yes. This is a responsibility refactor, not a new thread. |

## Motivation

A local deadline can be urgent even when the Consumer as a whole cannot make progress. Today, no single component
combines those facts before selecting the next wait or application-visible action.

### A motivating failure: urgent work without progress

[KAFKA-20253 / PR 22836](https://github.com/apache/kafka/pull/22836) fixed a high-CPU loop after failed
re-authentication. Once the coordinator became unavailable, a heartbeat could be due but could not be sent. At the
same time, coordinator discovery or auto-commit could already be in flight. Two zero-delay paths were enough to spin
the application and background threads, and auto-commit had the same missing check for whether it could make
progress:

| Component | Local observation | Local result | Global reality |
| --- | --- | --- | --- |
| Heartbeat request manager | The heartbeat deadline had expired and no heartbeat was in flight. | Application wait `0`. | No coordinator existed, so no heartbeat request could be created. |
| Coordinator request manager | The retry backoff had elapsed. | Network poll delay `0`. | A `FindCoordinator` request was already in flight, so another request could not be created. |
| Auto-commit state | The commit interval had elapsed. | Application wait `0`. | A previous commit was still in flight, so another commit could not start. |

The application thread repeatedly observed a zero wait, while the background thread repeatedly called
`NetworkClient.poll(0)`. Both threads ran, but neither could change the state required for progress.

PR 22836 correctly added guards that check whether the affected path can make progress now by producing a request or
completed state transition. This fixed the bug, but it also demonstrates the maintenance problem: the result type did
not distinguish "retry now" from "nothing can change until another event." Diagnosing the failure required
correlating manager timers, in-flight state, the previously published wait used by the application thread, and the
network poll timeout.

### Bug evidence

KAFKA-20253 is one instance of a recurring failure pattern. The affected features differ, but each case involved
decisions made on separate paths that could observe different snapshots of consumer state.

| Evidence | Observed failure | Decisions split across paths |
| --- | --- | --- |
| [KAFKA-17066 / PR 16885](https://github.com/apache/kafka/pull/16885), [KAFKA-17674 / PR 17342](https://github.com/apache/kafka/pull/17342) | An older position-initialization completion could affect a partition added while its request was in flight. | Assignment changes, the captured partition scope, and completion handling. |
| [KAFKA-18641 / PR 18737](https://github.com/apache/kafka/pull/18737), [KAFKA-15529 / PR 21476](https://github.com/apache/kafka/pull/21476) | Position and consumed-state publication could race with commit or application observation. | State mutation, publication, and dependent observation. |
| [KAFKA-20426 / PR 22018](https://github.com/apache/kafka/pull/22018), [KAFKA-20253 / PR 22836](https://github.com/apache/kafka/pull/22836) | Heartbeat urgency produced a zero wait while coordinator, assignment, or in-flight state made progress impossible. | Manager deadlines, progress blockers, application waiting, and network polling. |
| [KAFKA-20854 / PR 23014](https://github.com/apache/kafka/pull/23014) | An ambiguous empty fetch result caused application/background wakeup ping-pong. | Fetch result classification, application waiting, and an application wakeup that did not correspond to progress. |
| [KAFKA-20970 / PR 23227](https://github.com/apache/kafka/pull/23227) | An expired auto-commit or Streams heartbeat compatibility timer could remain at zero while the coordinator was unknown and no request could be built. | Manager feasibility, legacy application-wait projection, and network poll timeout. |
| [KAFKA-20397 / PR 21991](https://github.com/apache/kafka/pull/21991) | Metadata-error publication raced with an application thread entering its fetch-buffer wait. | Error publication, retained notification, and wait entry. |
| [KAFKA-18160 / PR 18089](https://github.com/apache/kafka/pull/18089) | Wakeup or interruption could skip callback acknowledgement. | Interruption, callback completion, and lifecycle handling. |
| [KAFKA-19357 / PR 19914](https://github.com/apache/kafka/pull/19914), [KAFKA-18569 / PR 18590](https://github.com/apache/kafka/pull/18590) | During close, coordinator discovery could stop while a pending commit still required it, or continue after commit and leave work no longer required it. | Pending-operation dependencies, coordinator discovery, and shutdown progress. |

These bugs establish the problem and the regression scenarios that the Test Plan must cover. The proposed ownership
model and mechanisms are introduced in Proposed Changes.

These issues share one narrower failure shape: a wait, wakeup, completion, or state-publication decision was made by a
component that could not atomically observe all state on which the decision depended. This KIP does not claim that one
call mechanism directly caused every issue. KAFKA-20253, for example, was fixed by correcting local progress checks.
The proposal instead defines one place where cross-manager ordering and timing are finalized, while keeping each
manager's local policy with that manager.

## Public Interfaces

The initial migration changes no Kafka protocol, public `Consumer` or `ShareConsumer` API, callback execution
guarantee, runtime thread name, or existing metric name. `Consumer.wakeup()` retains its current user-visible
semantics. The coordination types described in Proposed Changes are internal. The implementation adds four public
diagnostic counters:

| Metric name | Group | Type and scope | Incremented when | Operational use and stability |
| --- | --- | --- | --- | --- |
| `reactor-poll-result-contract-violation-total` | `consumer-metrics` or `consumer-share-metrics` | Monotonic cumulative counter per client instance. | A compatibility producer returns an empty result that requests immediate retry; the reactor replaces that result with `AwaitInput`. | A non-zero value identifies an incomplete manager migration and a prevented busy-loop condition. Name and cumulative meaning are stable after release. |
| `reactor-manager-poll-failure-total` | `consumer-metrics` or `consumer-share-metrics` | Monotonic cumulative counter per client instance. | A request-manager poll throws an unexpected runtime exception that the reactor isolates. | Diagnoses degraded manager execution without requiring manager-name tags. Name and cumulative meaning are stable after release. |
| `reactor-action-failure-total` | `consumer-metrics` or `consumer-share-metrics` | Monotonic cumulative counter per client instance. | A selected application-visible `ReactorAction` fails during execution. | Distinguishes effect-delivery failure from manager or transport failure. Name and cumulative meaning are stable after release. |
| `reactor-application-wakeup-total` | `consumer-metrics` or `consumer-share-metrics` | Monotonic cumulative counter per client instance. | The reactor invokes the primitive application-thread wakeup after phase coalescing. | Its rate helps detect wakeup ping-pong; the absolute total is not by itself an error signal. Name and cumulative meaning are stable after release. |

These metrics follow the existing async-consumer metric lifecycle: they inherit the client metric registry's common
tags, introduce no manager/event/reason tag, and are removed when the consumer metrics manager closes. Recording is
constant-time and performs no event-list formatting or dynamic metric registration on the reactor hot path. A
diagnostic counter is not a recovery mechanism: after a contract violation the reactor must prevent the immediate
retry, so a busy loop cannot continue merely to generate the metric. Tests cover registration, recording, and
removal in both metric groups.

Any future public capacity configuration or overload behavior requires a separately complete compatibility
proposal.

## Proposed Changes

This KIP keeps the existing application/background thread topology and refactors the responsibility of the existing
background loop into `ConsumerReactor`. A request manager is a per-domain component such as the heartbeat, commit,
fetch, or coordinator manager. Request managers keep their mutable state and domain rules. The reactor becomes the
single place that finalizes, from one ordered view of all managers, how long the consumer may wait and when the
application thread may observe an effect routed through this boundary.

One manager poll returns one `PollResult`. The result is a typed envelope with three independent outputs:

1. **`ManagerEvent`: an immutable fact.** For example, fetch reports that buffered data is available, or heartbeat
   reports that the coordinator used by its completed request is unavailable. A composition-owned coordination
   policy maps the ordered event batch to commands for a state owner or to application-facing effects. The reactor
   performs only generic collection and ordering; it does not contain a switch for fetch, heartbeat, commit, regular,
   or share policy.
2. **`NetworkCommand`: an intent for the transport owner.** For example, a heartbeat manager asks
   `NetworkClientDelegate` to stage a built heartbeat request. The command is not evidence that the request has
   already been sent; connection selection, transmission, correlation, and timeout handling remain transport work.
3. **`NextPollCondition`: the manager's next local activation condition.** `PollImmediately` is legal only after the
   same result produced output, `RetryAfter(delayMs)` means time alone may make work possible, and `AwaitInput` means
   only a new application input, network completion, cancellation, shutdown, or another explicitly admitted input
   can change the answer.

The envelope deliberately separates a fact, an intent, and a wait condition. They are all manager outputs, but they
do not have the same handling rule. Calling every value an event would hide whether the reactor must evaluate a
policy, stage transport work, or retain a deadline.

The remaining concepts describe how those outputs form one reactor iteration:

| Concept | Scoped meaning | Concrete example |
| --- | --- | --- |
| `PollResult` | One manager's complete atomic output for this poll. | Heartbeat returns one `NetworkCommand` plus `AwaitInput(network completion)` after recording the request in flight; an in-flight discovery also returns no immediate output plus `AwaitInput`. |
| `ManagerEvent` | A fact produced by one manager that requires a consequence outside that producer's local state. | `FetchBufferHasData` derives a post-publication application wake; `CoordinatorUnavailableObserved(version=7)` derives a version-fenced command for the coordinator owner. |
| Owner snapshot | An opt-in immutable, versioned view used only when another manager needs coherent owner state. | Heartbeat builds for `CoordinatorSnapshot(node-1, version=7)` and retains version 7 so its later observation cannot invalidate version 9. |
| `ManagerCoordinationPolicy` | Composition-owned typed handlers that map an ordered event batch to `ManagerCommand` or `ReactorAction` values. | It maps `FetchBufferHasData` to one coalesced wake and routes a coordinator observation to the coordinator owner without adding domain branches to `ConsumerReactor`. |
| `ReactorSchedule` | The immutable aggregate of all retained `NextPollCondition` deadlines. | Fetch can retry in 100 ms and heartbeat in 50 ms, so the next network poll is bounded to 50 ms. |
| `ReactorAction` | An application-facing effect executed by `ConsumerReactor` only after the corresponding state and schedule are published. | Complete an async poll, publish a metadata error, or perform one phase-coalesced application wake. |

The design relies on three invariants:

1. **One state owner.** Each mutable state has one execution-context owner. Another ownership domain may read an
   immutable snapshot or report a `ManagerEvent`, but it may not mutate that state directly. Components deliberately
   grouped behind one protocol driver, such as heartbeat and membership state for one group protocol, may still call
   one another because the driver defines them as one ownership domain.
2. **No wakeup without a cause.** Every application wakeup or timed reschedule corresponds to produced work, a
   positive deadline, an input, or a capacity change. An empty manager result cannot request an immediate retry.
3. **Publish before effect.** State and the resulting `ReactorSchedule` are published before any corresponding
   `ReactorAction` or staged application event is released.

This KIP does not add a queue or thread, move user callbacks to the background thread, combine regular and share
state machines, make the reactor own manager policy, introduce a dynamic dependency graph, retrofit
`ClassicKafkaConsumer`, or migrate every compatibility path in one patch. The sections below first define ownership,
then manager poll results and timing publication, and finally the full iteration and cross-manager lifecycle.

### 1. Define the responsibility boundary

Today, `AsyncKafkaConsumer` and `ShareConsumerImpl` each create a background execution context. They reuse the same
event-loop and network infrastructure, but state, wait, completion, publication, and wakeup decisions can still be
made by different components along the path. The proposal changes responsibility, not topology:

![ConsumerReactor ownership and execution boundary](../images/kip-1371-reactor-architecture.png)

| Component | Target responsibility after the KIP |
| --- | --- |
| `ConsumerReactor` | Order inputs, invoke managers in a fixed order, retain their timing contributions, publish the final timing decision, and then execute application-visible effects. |
| `RequestManagers` composition | Select the regular, share, or Streams manager set, own its typed event handlers, and apply derived `ManagerCommand` values to one state owner. |
| Request manager | Own one domain of mutable consumer state and decide whether it can produce work now, needs a finite retry, or must wait for another input. It may publish a small immutable snapshot when another manager needs a coherent view. |
| Regular / share / Streams protocol driver | Keep membership, assignment or acquisition, commit or acknowledgement, topology, and callback coordination outside the shared reactor loop. These are proposed internal boundaries, not current code types. |
| `NetworkClientDelegate` | Own transport, connection handling, request correlation, and timeouts; do not decide whether to complete an application event or wake the application thread. |
| Application thread | Execute user callbacks and consume published data, events, and operation results. |

The reactor coordinates these owners; it does not absorb their local rules or transport mechanics. A manager's
`NetworkCommand` is handed to `NetworkClientDelegate`; it is neither a `ManagerEvent` nor an application-visible
`ReactorAction`.

### 2. Define manager poll results

A manager poll answers one narrow question: what did this manager produce now, and what can make another poll useful?
The manager owns the state needed to answer, such as coordinator availability, in-flight requests, retry backoff,
metadata, or buffer capacity. The reactor validates and routes the typed answer but does not reimplement those rules.

The manager must not encode the same feasibility rule independently in work admission and wait calculation. For each
migrated work source, it derives one manager-local activation projection that answers both:

1. whether a local step is legal now; and
2. after that step, whether time or another input can make the next poll useful.

This is a local calculation over state the manager already owns, not a global readiness registry. It may be a private
method or small private value; the KIP does not require a new framework or class hierarchy. The resulting condition is
published through the existing `PollResult` type.

`PollResult` has the following form:

```java
record PollResult(
    List<NetworkCommand> networkCommands,
    List<ManagerEvent> events,
    NextPollCondition nextPoll
) {}

PollResult.progress(networkCommands, events, nextPollCondition);
PollResult.retryAfter(delayMs);
PollResult.awaitInput(cause);
```

Two proven examples show why the pre-step and post-step conditions must be derived from the same state projection:

```text
Coordinator discovery
  retry elapsed, no request in flight -> PollImmediately -> build one request
  after recording the request in flight -> AwaitInput(network completion)

Auto-commit
  timer not expired -> RetryAfter(remainingMs)
  timer expired, no commit in flight -> PollImmediately -> enqueue one commit
  timer expired, commit in flight -> AwaitInput(commit completion)

Regular/share heartbeat
  coordinator unknown or membership blocks heartbeat -> AwaitInput(owner state change)
  interval/backoff pending -> RetryAfter(remainingMs)
  request legal now -> PollImmediately -> build one heartbeat
  after recording the heartbeat in flight -> AwaitInput(network completion)
```

Reusing the pre-step `PollImmediately` condition after creating a request would admit a duplicate or immediate loop.
Using an arbitrary timer while a request is in flight avoids that loop but periodically rechecks work that only a
completion can enable. The single local projection removes both forms of disagreement. During migration,
`maximumTimeToWait(...)` may remain a compatibility projection, but it must read the same activation calculation as
the work-admission path rather than reproduce its predicate.

| Result shape | Immediate output | Meaning |
| --- | --- | --- |
| `progress(...)` | At least one `ManagerEvent` or `NetworkCommand`. | Consume the output now; the typed condition describes the next poll after that output. |
| `retryAfter(delayMs)` | None. | Time alone may make the manager runnable; poll again after a positive, finite delay. |
| `awaitInput()` | None. | Time alone cannot help; poll again after a relevant admitted input. |

`PollImmediately` is not a general zero-delay retry. It is valid only when the same `PollResult` contains output that
the reactor consumes or stages. This makes the invalid shape structural rather than a convention:

```text
events=[] + networkCommands=[] + nextPoll=PollImmediately
```

The empty output says that no progress occurred, while the zero delay asks for an immediate retry. Returning the same
shape repeatedly creates a busy loop. A manager returning `progress(...)` must also consume or mark its output in
flight so that the next poll cannot emit the same logical progress again.

`AwaitInput` does not subscribe to or name one `ManagerEvent`. It means that this manager has no time-based deadline.
For an in-flight coordinator discovery, the enabling input is that request's network completion; for an in-flight
commit, it is that commit's completion; for paused fetch partitions, it may be an application resume or assignment
change. The existing bounded network poll is a safety safeguard, not a semantic retry interval.

Examples across managers are:

| Manager state | Result | Related evidence |
| --- | --- | --- |
| Heartbeat is due but coordinator discovery is in flight. | `awaitInput()`; the `FindCoordinator` network completion is the enabling input. | [KAFKA-20253 / PR 22836](https://github.com/apache/kafka/pull/22836) |
| Heartbeat is due and a request can be built. | `progress([heartbeatRequest], [], AwaitInput(network completion))` after recording the attempt in flight. The current `UnsentRequest` implements `NetworkCommand`. | Request-producing case for KAFKA-20253 |
| Auto-commit is due but an earlier commit is in flight. | `awaitInput()`; the earlier commit's completion is the enabling input. | [KAFKA-20253 / PR 22836](https://github.com/apache/kafka/pull/22836) |
| Coordinator retry backoff has 75 ms remaining. | `retryAfter(75)` | Finite-retry case |
| Fetch preparation finds buffered data. | `progress([], [FetchBufferHasData], AwaitInput)`; policy derives a post-publication wake. | [KAFKA-20854 / PR 23014](https://github.com/apache/kafka/pull/23014) |
| Fetch request is in flight. | `awaitInput()`; the fetch response or disconnect is the enabling input. | Expected in-flight wait case for KAFKA-20854 |
| Topic-metadata request is in retry backoff. | `retryAfter(remainingBackoffMs)` | Finite-retry case |
| Share acknowledgement request is in flight. | `awaitInput()`; acknowledgement completion is the enabling input. | Share-consumer case |

Strict factories validate only these generic result-shape rules. They do not infer why heartbeat, commit, fetch,
coordinator, or share work can make progress. During migration, `UnsentRequest`, raw-delay fields, and compatibility
constructors remain adapters around the typed fields and factories. `StateTransition` has been removed; manager facts
now use only `ManagerEvent`. Production validation also detects an
empty immediate result created through an adapter, records the manager, and treats it as `AwaitInput` so it cannot
force `NetworkClientDelegate.poll(0)`.

### 3. Publish one wait decision before executing effects

For every manager result, the reactor interprets `NextPollCondition` generically. `RetryAfter` and
`PollImmediately` become an absolute manager deadline; `AwaitInput` withdraws that manager's finite deadline.
`ReactorSchedule` is the immutable publication of the retained deadlines. It selects the earliest deadline, so
updating one manager cannot erase an earlier deadline from another manager and an unrelated early network return
cannot postpone existing work.

The deadline means that the reactor must activate by that time. It does not select which manager runs and does not
promise that network I/O occurs exactly then. Network I/O, application input, cancellation, or shutdown may wake the
loop earlier. The same publication supplies both the network poll bound and the application thread's current wait
projection.

The schedule has these properties:

- a shorter deadline is published before a waiter using an older value is released;
- an early finite re-poll cannot move an unexpired manager deadline later;
- `AwaitInput` withdraws that manager's prior finite deadline because time alone can no longer help;
- an expired deadline is consumed by polling the manager again rather than repeatedly waking the application; and
- an event-only wait does not create a periodic manager poll.

`ReactorAction` represents an application-visible effect such as an async-poll completion or application wakeup.
Network commands remain transport intents, next-poll conditions remain timing contributions, and manager commands
remain targeted state-owner intents; none of those are `ReactorAction` values. The ordering rule is:

> Apply owner state updates, publish state and `ReactorSchedule`, then execute the corresponding `ReactorAction`
> values.

Equivalent wake reasons may share one primitive application wakeup, but terminal operation completions are never
merged. Wakeup executes last so the application thread can observe the state, schedule, data, event, or completion
that caused it. An action failure is logged, counted, and isolated; it cannot suppress later independent actions or
network I/O.

#### Example: two managers with different deadlines

Assume fetch reconnect backoff expires in 100 ms while heartbeat must be polled in 50 ms:

```text
FetchRequestManager.poll()     -> retryAfter(100)
HeartbeatRequestManager.poll() -> retryAfter(50)

ConsumerReactor
  -> publishes ReactorSchedule(deadline=heartbeat at 50 ms)
  -> does not wake the application
```

At 50 ms, the reactor polls heartbeat work. The retained fetch deadline remains at 100 ms even if heartbeat network
I/O returns early. At 100 ms, fetch is polled again. Backoff and in-flight states do not wake the application because
they are not application-visible progress.

### 4. Process one reactor iteration

An input event is an existing application command, network completion, deadline expiry, network send capacity
becoming available, or callback acknowledgement. It is a semantic term, not a requirement to add another queue:
application commands use the existing
`ApplicationEventQueue`, while network completions occur directly during `NetworkClientDelegate.poll()` on the
reactor thread.

Input events are not merged into one state mutation. The reactor applies them in a deterministic order, then polls
the manager set in its fixed composition order and aggregates the results. Requests, callbacks, and terminal
operation results retain their identities and ordering. Equivalent application wake reasons may share one primitive
wakeup while all reasons remain observable.

For example, if an assignment adds `tp2` before an older `OffsetFetch(scope={tp1})` completes, the assignment event is
applied first and the older completion can update only `tp1`. A later ordered position operation initializes `tp2`.

One reactor iteration is event-driven with deadline-bounded waiting:

1. Apply `ManagerCommand` values derived from the previous post-I/O completion phase to their single state owners
   through the selected regular or share composition.
2. Drain producer-local `ManagerEvent` values recorded by response callbacks, evaluate that input-boundary batch,
   and apply its owner commands. This occurs before any manager can build transport work from owner state.
3. Drain ready application commands and callback acknowledgements and apply them through the same composition.
4. Capture the current references to any opt-in owner-published snapshots used by new cross-manager work in this
   ordered pass.
5. Poll the stable manager set in order and collect their `PollResult` values.
6. Evaluate that phase's ordered `ManagerEvent` batch through `ManagerCoordinationPolicy`, stage every
   `NetworkCommand`, and retain each manager's `NextPollCondition`.
7. Form and publish one `ReactorSchedule`, publish staged `BackgroundEvent` values, and execute the phase's
   `ReactorAction` values.
8. Poll network I/O no longer than the published deadline.
9. Re-poll each completion's owning manager, evaluate the post-I/O event batch, publish the updated schedule, then
   execute its actions. Any resulting cross-owner `ManagerCommand` is applied at the beginning of the next iteration,
   before the next full manager pass and before the next network poll.

Managers do not call one another recursively and do not invoke mutation methods across ownership domains. A network
completion updates its owning manager, which may return an immutable fact. Because I/O has already occurred, a
cross-owner command derived from that fact is applied at the beginning of the next iteration and dependent work is
produced by the next full ordered pass, before the next network poll. The reactor does not traverse a dependency graph
or repeatedly poll to a fixed point.

### 5. Route cross-manager observations to their state owner

Consider a heartbeat sent while the group coordinator becomes unavailable:

1. **Build from the current view.** The heartbeat manager reads the coordinator owner's published view:
   `(node-1, version 7)`. It builds the request for `node-1` and records version `7` with that request attempt.
2. **Observe, do not mutate.** The response reports that the coordinator is unavailable. The heartbeat manager does
   not change coordinator state. Its post-I/O `PollResult` reports
   `CoordinatorUnavailableObserved(observedVersion=7, cause)` as a `ManagerEvent`.
3. **Derive one owner command.** `ManagerCoordinationPolicy` handles the event and emits
   `InvalidateCoordinatorIfCurrent(observedVersion=7, cause)`. The reactor defers that `ManagerCommand` until the next
   iteration, then `RequestManagers` routes it to `CoordinatorRequestManager`, which owns coordinator state.
4. **Fence stale observations.** The owner compares version `7` with its current version. If rediscovery has already
   published version `9`, the old observation is ignored and cannot invalidate the newer coordinator.
5. **Re-evaluate every manager.** The reactor polls all managers in fixed order. The coordinator manager may produce
   a `FindCoordinator` request; heartbeat and commit wait for discovery rather than scheduling arbitrary retries.
6. **Publish the next wait.** The reactor publishes the resulting `ReactorSchedule` before releasing any
   application-visible effect. The `FindCoordinator` request is handed to the network layer before the next network
   poll.

If the heartbeat manager's post-I/O `poll()` throws before it can attach the callback-recorded fact to a
`PollResult`, the fact remains in that manager's bounded `PendingManagerEvents`. The next iteration drains it at the
input boundary and applies the coordinator command before any manager builds the next request. The error path
therefore changes when the fact is admitted, but does not lose it or allow a long wait before owner re-evaluation.

Cross-owner facts have one normative admission rule: they must originate from an input or network-completion path and
must be available no later than the post-I/O owner poll. They must not be created for the first time by the ordinary
pre-I/O full manager pass. At that point other managers may already have built transport commands from the previous
owner snapshot, so applying a newly derived owner command would leave those commands stale. The POC retains and logs
such an unexpected pre-I/O fact as migration containment, then applies its command at the next input boundary. The
target model does not add dependency-aware request cancellation or replay; adding that mechanism requires separate
motivation and evidence.

![Coordinator observation routed and fenced](../images/kip-1371-coordinator-observation-sequence.png)

#### Owner-published snapshots and stale-observation fencing

A snapshot is an opt-in cross-manager state view, not a required output of every `RequestManager`. A state owner
publishes a small immutable snapshot only when another manager needs a coherent set of decision-relevant fields.
Fields which must be observed atomically, such as coordinator target and coordinator version, belong to the same
snapshot. Snapshot versions are owner-local internal identities; they are not Kafka protocol `generationId` or
`memberEpoch` values. The owner increments the version only when published decision-relevant truth changes, not for
every poll, log, or metric update.

An admitted request captures the snapshot version and only the bounded target or scope required for that attempt. An
old version may remain as captured context for an in-flight request, but an old snapshot cannot admit new work. The
fencing path is:

```text
read current CoordinatorSnapshot(READY, node-1, version=7)
  -> build the heartbeat request and capture target=node-1, observedVersion=7
  -> the response produces CoordinatorUnavailableObserved(observedVersion=7, cause)
  -> a bounded pending slot retains the newest observation of that type
  -> the observation is returned in PollResult and routed to the coordinator owner
  -> the owner compares observedVersion with its current CoordinatorSnapshot version
  -> apply only if the versions still match; otherwise ignore the stale observation
```

If several observations arrive before publication, the producer-local slot retains the greatest observed version.
That bounded buffer does not decide whether an observation is current. The state owner always performs the final
comparison. Each producer retains at most one pending fact of each bounded type, the reactor retains it only until the
next routing phase, and in-flight work retains only the version and bounded request context rather than snapshot
history. `ReactorSchedule` is a timing publication, not a registry of manager domain state.

### 6. Preserve operation identity across retries

One application API operation may require several network attempts and several reactor iterations. Those identities
must not be conflated:

| Identity | Purpose |
| --- | --- |
| Operation identity | Tracks one admitted application API operation until exactly one terminal result. It lets the reactor associate later retries and completions with the correct future, and reject a late or duplicate completion after timeout, cancellation, or termination. It may be represented by the existing application event and future or by an explicit internal ID; it is not assigned once per `ReactorAction`. |
| Request attempt identity | Distinguishes network attempts and retries within an operation; transport correlation remains in `NetworkClientDelegate`. |
| Owner snapshot version and scope | Prevents an older completion from mutating newer owner state or partitions outside the operation's captured scope. |
| Protocol generation or member epoch | Retains broker-defined group identity and fencing semantics; it is not reused as an internal snapshot version. |

One operation can span several schedule publications and network attempts before producing one terminal completion.
Conversely, one deduplicated wake action can notify the application about several progress reasons, so an action cannot
serve as the operation identity. The target ordering for `commitSync` encountering a coordinator change is a concrete
example:

```text
Application thread calls commitSync(offsets, timeout)
  -> one SyncCommitEvent and its future represent the pending commit operation

OffsetCommit attempt 1 returns NOT_COORDINATOR
  -> the commit operation remains pending
  -> CommitRequestManager reports CoordinatorUnavailableObserved with the request's coordinator version
  -> the next reactor iteration routes the fact to CoordinatorRequestManager
  -> CoordinatorRequestManager applies it only if that version is still current
  -> the full ordered pass schedules FindCoordinator without completing the commit

FindCoordinator response records the new coordinator
  -> the next full ordered pass lets commit and heartbeat observe the new coordinator snapshot
  -> OffsetCommit attempt 2 is sent for the same commit operation

OffsetCommit attempt 2 succeeds
  -> ConsumerReactor publishes the resulting schedule
  -> the terminal completion action completes the SyncCommitEvent future once
```

The two `OffsetCommit` attempts have separate transport correlation, but they do not become two application
operations. `FindCoordinator` is shared infrastructure work that may also unblock heartbeat, join, or other commit
work; it is not owned by the commit action. Pending operations remain in their existing managers rather than being
copied into an unbounded central dependency graph.

Stable operation identity is required so retry, cancellation, interruption, and close races cannot produce missing
or duplicate terminal completion. This KIP requires that stability and exactly-once terminal outcome, but does not
require a new public ID or a new generic `OperationId` class when the existing event and future already provide the
necessary identity. These identities do not change Kafka protocol correlation IDs or public Consumer APIs.

### 7. Share the reactor kernel, not consumer rules

The regular consumer and share consumer use separate reactor instances but the same execution kernel because they
share the same concurrency topology. Their rules remain separate. `RegularConsumerDriver`, `ShareConsumerDriver`,
and `StreamsConsumerDriver` are proposed internal boundaries; they do not yet exist in the codebase:

| Component | State and behavior retained outside the reactor |
| --- | --- |
| Proposed `RegularConsumerDriver` | assignment, positions, offset fetch/commit, membership, rebalance transitions, and regular fetch behavior |
| Proposed `ShareConsumerDriver` | share membership, acquisition, lock renewal/release, acknowledgement, and share fetch behavior |
| Proposed `StreamsConsumerDriver` | Streams membership, topology description, task assignment, heartbeat, and Streams group configuration behavior |

The shared reactor must not branch on consumer type. An `isShareConsumer` switch or a regular/share union in the
reactor loop indicates that consumer-specific behavior has leaked across the boundary.

## Case Studies

The reactor model is useful only if the motivating failures can be replayed as component interactions and reduced to
repeatable assertions. The following studies group related issues by the boundary they exercise. **Verified** means
the named production-component path has a deterministic test. **Partial** and **pending** identify migration work;
they are not claims that the POC fixes the historical issue end to end.

All POC references below are pinned to the code-complete evidence baseline
[`d2e5ff6eb7`](https://github.com/unknowntpo/kafka/tree/d2e5ff6eb7ffe18fc84d53d0e651d13a3d5942a0)
so that later branch changes cannot alter the cited evidence.

### Case study 1: position scope and publication ordering

This study covers [KAFKA-17066](https://issues.apache.org/jira/browse/KAFKA-17066),
[KAFKA-17674](https://issues.apache.org/jira/browse/KAFKA-17674),
[KAFKA-18641](https://issues.apache.org/jira/browse/KAFKA-18641), and
[KAFKA-15529](https://issues.apache.org/jira/browse/KAFKA-15529).

**Before.** Assignment, an in-flight offset operation, position mutation, auto-commit snapshot creation, and fetch
publication could proceed on different paths. An older completion could therefore act on state admitted after its
request was created, or the application could observe data before the matching position mutation.

```text
ApplicationEventProcessor assigns tp1
  -> OffsetsRequestManager starts OffsetFetch(scope={tp1})

ApplicationEventProcessor later assigns tp2
  -> the older OffsetFetch completes
  -> completion code observes the current assignment rather than only scope={tp1}
  -> tp2 may be reset or initialized by the wrong operation

Fetch or auto-commit concurrently reads position state
  -> application-visible data or a commit snapshot may observe a different ordering
```

**After applying the reactor model.** The application input and network completion are separate ordered operations.
The offset operation retains its admitted partition scope, and the state owner applies only that scope. The next full
manager pass observes the resulting current state. Any migrated data publication or completion is released only after
the corresponding state and schedule publication.

```text
ConsumerReactor input phase
  -> applies assignment(tp2)
  -> preserves OffsetFetch attempt context(scope={tp1})

NetworkClientDelegate.poll()
  -> returns the older OffsetFetch completion
  -> OffsetsRequestManager applies results only to tp1
  -> tp2 remains owned by the later assignment/position operation

ConsumerReactor next ordered pass
  -> CommitRequestManager derives its snapshot from the updated positions
  -> fetch owner applies the matching position mutation
  -> ConsumerReactor publishes state and ReactorSchedule
  -> only then releases the corresponding application-visible effect
```

**Current evidence.** Captured-scope protection is verified through the manager and application-event/reactor paths.
The position/publication and position-versus-auto-commit races remain partial. Exact tests, baselines, and remaining
gates are listed in [Appendix A](#appendix-a-poc-evidence-and-open-gates).

### Case study 2: heartbeat and commit cannot progress yet

This study covers [KAFKA-20426](https://issues.apache.org/jira/browse/KAFKA-20426),
[KAFKA-20253](https://issues.apache.org/jira/browse/KAFKA-20253), and
[KAFKA-20970](https://issues.apache.org/jira/browse/KAFKA-20970).

**Before.** Heartbeat, coordinator discovery, auto-commit, the application wait, and network polling could calculate
urgency independently. A timer could remain at zero even though coordinator state or an in-flight request made new
work impossible.

```text
HeartbeatRequestManager      -> heartbeat timer expired -> application wait 0
CoordinatorRequestManager    -> retry timer expired     -> network wait 0
CommitRequestManager         -> auto-commit expired     -> application wait 0

but coordinator is unknown, FindCoordinator is in flight, or commit is in flight
  -> no manager can create distinct work
  -> application poll and NetworkClient.poll(0) repeat
```

**After applying the reactor model.** Each manager derives request admission and its next useful poll from one local
activation view. The reactor does not reproduce heartbeat or commit rules; it retains and combines only their typed
conditions.

```text
HeartbeatRequestManager
  -> coordinator or membership blocks request -> AwaitInput(owner change)
  -> heartbeat interval/backoff remains        -> RetryAfter(remainingMs)
  -> request is legal                          -> NetworkCommand(heartbeat)
                                                  + AwaitInput(network completion)

CommitRequestManager
  -> auto-commit not due                       -> RetryAfter(remainingMs)
  -> commit request already in flight          -> AwaitInput(network completion)
  -> commit is due and legal                   -> NetworkCommand(offset commit)

ConsumerReactor stable manager pass
  -> retains every manager condition in ManagerPollCache
  -> publishes the earliest finite deadline as ReactorSchedule
  -> produces no application wake for coordinator, backoff, or in-flight blockers

NetworkClientDelegate.poll(published timeout)
  -> completion re-polls its owning manager
  -> ConsumerReactor publishes the updated schedule before any wake action
```

**Current evidence.** Regular/share heartbeat activation, heartbeat-to-coordinator routing, manual-assignment
no-spin, and the auto-commit completion/publication slice are verified. Exact historical KAFKA-20253 CPU reproduction
and KAFKA-20970 end-to-end evidence remain open; see [Appendix A](#appendix-a-poc-evidence-and-open-gates).

### Case study 3: fetch progress and application wakeup

This study covers [KAFKA-20854](https://issues.apache.org/jira/browse/KAFKA-20854) and
[KAFKA-20397](https://issues.apache.org/jira/browse/KAFKA-20397).

**Before.** An empty fetch-preparation result could be interpreted as a reason to wake the application. The
application would find no data, re-enter its wait, and trigger another background pass. A metadata error could also
be published while the application was crossing into the fetch-buffer wait, losing the intended notification order.

```text
FetchRequestManager produces no request and no buffered data
  -> generic fetch path wakes the application
  -> application poll observes no data and waits again
  -> background fetch preparation repeats the same wake

Metadata error publication races with FetchBuffer.awaitNotEmpty()
  -> the application may enter the wait after the notification decision
```

**After applying the reactor model.** Fetch reports the fact it actually observed. A composition-owned typed handler
derives an application action only for facts that represent application-visible progress. `ConsumerReactor` controls
when that action executes.

```text
FetchRequestManager.poll()
  -> buffered records available
       -> PollResult(events=[FetchBufferHasData], nextPoll=AwaitInput)
  -> paused/no fetchable partition or request in flight
       -> PollResult(events=[], nextPoll=AwaitInput)
  -> metadata or reconnect backoff
       -> PollResult(events=[], nextPoll=RetryAfter(remainingMs))

ManagerCoordinationPolicy
  -> maps FetchBufferHasData to ReactorAction.WAKE_APPLICATION
  -> maps no-progress blockers to no application action

ConsumerReactor
  -> publishes fetch state, BackgroundEvent values, and ReactorSchedule
  -> executes the phase-coalesced WAKE_APPLICATION last

Application thread
  -> target: wakes only after the data, error, or completion that caused the wake is visible
```

**Current evidence.** The paused-partition no-wakeup regression and typed fact-to-action ordering are verified.
Direct `FetchBuffer` signalling remains a classic-consumer compatibility path, and the exact KAFKA-20397
metadata-error/wait-entry reproduction remains open; see [Appendix A](#appendix-a-poc-evidence-and-open-gates).

## Reviewer FAQ

**What problem does `ConsumerReactor` solve?**

It gives cross-manager timing, ordering, and application-visible effects one final authority. Request managers still
own their mutable state and decide whether they can produce local work. The reactor combines those typed decisions,
publishes one wait decision, and only then executes application-visible effects. This prevents one component from
choosing a wait or wake without atomically observing the other manager results on which that choice depends.

**How is this different from the existing `ConsumerNetworkThread` and request-manager model?**

The application/background thread topology and the network client do not change. The difference is responsibility:
the existing loop polls managers and transports requests, but timing, cross-manager mutation, completion, publication,
and wake decisions can still be distributed across managers and callbacks. `ConsumerReactor` makes the loop's
ordering and publication boundary explicit without moving heartbeat, commit, fetch, coordinator, or share-consumer
domain rules into one class.

**Why is a deadline-only result insufficient?**

A number answers *when* to poll again but not *what can make another poll useful*. Once a deadline expires, a manager
may still be blocked by an in-flight request, unknown coordinator, missing assignment, or another external input. An
empty result with a zero delay can therefore create a busy loop, while replacing zero with an arbitrary interval can
delay real progress. `NextPollCondition` makes the distinction explicit:

- `PollImmediately` accompanies work produced now;
- `RetryAfter(delay)` is used only when passage of time can change local eligibility; and
- `AwaitInput(cause)` is used when a network completion, coordinator change, membership change, command, or shutdown
  must occur first.

**Are deadlines removed by this proposal?**

No. Positive time-driven retries remain deadlines. `ConsumerReactor` retains each manager's current deadline and
publishes the earliest one in `ReactorSchedule`. Input-driven waits create no semantic timer, although application
input, network I/O, cancellation, or shutdown may activate the reactor earlier. The model is event-driven with
deadline-bounded waiting, not timer-free.

**Does the reactor decide whether heartbeat, commit, fetch, or share work is legal?**

No. That decision remains manager-local because the manager owns the relevant protocol state. A manager atomically
returns the work it produced, facts it observed, and the condition for its next useful poll. The reactor validates the
generic result shape and combines results; it does not reproduce coordinator, membership, backoff, or fetch policy.

**How do `NetworkCommand`, `ManagerEvent`, and `ReactorAction` differ?**

- A `NetworkCommand` is transport work selected by a manager, such as an unsent heartbeat request.
- A `ManagerEvent` is an immutable fact whose consequence crosses the producer's local ownership boundary, such as a
  heartbeat response observing that coordinator version 7 is unavailable.
- A `ReactorAction` is an application-visible effect selected from current inputs, such as publishing an error or
  waking the application after the corresponding schedule is visible.

An event is not an action and a command is not proof that network I/O has already occurred.

**Must every `ManagerEvent` be created and consumed within one `runOnce()`?**

No. A local pre-I/O manager fact may select an action in the same iteration. A cross-owner fact originates from an
input or network-completion path: the post-I/O owner poll may publish it in that iteration, or its producer-local
buffer may retain it for the next input boundary. Its owner command is applied before the next full manager pass and
network poll. Events that cross this boundary are immutable and retained in bounded latest-pending storage; versioned
observations are validated by the state owner before mutation. A cross-owner fact first created during the ordinary
pre-I/O pass violates the target admission rule and is only retained as a migration diagnostic.

**Why not let one request manager directly mutate another?**

Direct mutation hides the dependency and allows a delayed response to change newer peer state. The proposal instead
routes an immutable observation to one state owner. The owner applies it only if its captured version is still
current. Components intentionally grouped inside one regular, share, or Streams protocol driver may still interact
directly because they form one ownership domain; the rule applies when state ownership is crossed.

**Does every request manager need to publish a snapshot or define a new event?**

No. Snapshots are opt-in, latest-only projections used only when another manager needs coherent owner state. Events
are needed only when an observation requires a consequence outside the producer's local state. Purely local work and
retry decisions remain local `PollResult` output.

**Is the POC evidence equivalent to fixing every motivating issue end to end?**

No. The case studies label deterministic POC mechanisms separately from pending historical reproductions. The
[current implementation status](#current-implementation-status) records compatibility paths such as direct classic
fetch-buffer signalling and remaining raw-delay producers. A mechanism is considered complete only when its named
ordering, retry, stale-response, or wake assertion is exercised on the production-component path.

## Compatibility, Deprecation, and Migration Plan

### Current implementation status

The current proof of concept implements Phase 1 plus coordinator ownership, heartbeat/commit readiness, finite retry,
and Streams topology slices from Phase 2. The target model above also defines later migration work. The distinction is:

| Area | Current POC | Target after migration |
| --- | --- | --- |
| Manager poll result | Canonical `PollResult` storage has `NetworkCommand`, `ManagerEvent`, and `NextPollCondition`; generic reactor/cache consumers use the typed accessors. `StateTransition` and `awaitEvent()` have been removed. Raw-delay compatibility constructors remain for unmigrated producers. | Every manager produces only the typed result and the compatibility constructors are removed. |
| Manager progress | `AwaitInput`, positive finite `RetryAfter`, and output-gated `PollImmediately` are present. Coordinator, commit, regular/share/Streams heartbeat, topic metadata, share acknowledgement retry, and Streams topology-description paths publish typed conditions. | Migrate the remaining raw-delay producers without introducing a global readiness registry. |
| Manager-local activation projection | Coordinator discovery, auto-commit, and regular/share/Streams heartbeat derive work admission and the next condition from manager-owned state. Auto-commit completion is proven to re-poll the real manager and publish a newer schedule before application wakeup; an admitted heartbeat or topology request waits for network completion. | Apply the rule only where a manager otherwise duplicates an eligibility predicate. |
| Cross-manager ownership | Coordinator target/version snapshot, typed event handlers, phase-batch policy evaluation, and version-fenced coordinator invalidation are present. Coordinator fatal errors are emitted once by the coordinator owner and converted after schedule publication into an application error plus the final wake; heartbeat managers no longer read and clear coordinator fatal state. | Other cross-owner mutation paths use the same owner/fact/command rule or are placed inside an explicit protocol driver. |
| Snapshot retention | Only the current `CoordinatorSnapshot` is retained; there is no global registry or snapshot history. | Additional snapshots remain opt-in and latest-only when a real cross-manager decision needs them. |
| Publish-before-effect | `ReactorAction` and staged `BackgroundEvent` paths publish the schedule first. Direct `FetchBuffer` signalling remains a documented classic-consumer compatibility side channel. | Generic operation completion, async data publication, callback acknowledgement, and timeout paths cross the same boundary. |
| Application wait projection | `AsyncKafkaConsumer.poll(...)` uses the published reactor decision; the former assignment/position mutable-state rescans have been removed in the POC. Assignment publication and position update/failure paths must therefore provide the corresponding wake or completion signal. | All remaining compatibility waits are derived from immutable schedule or operation results, with integration tests proving that each enabling input wakes a blocked application poll. |
| Wake coalescing | Equivalent wakes are combined separately in the pre-I/O, post-I/O, and final-drain phases. | Equivalent reasons produce at most one primitive wake per complete reactor iteration. |
| Cross-owner fact admission | Callback-produced facts are drained at the input boundary and their owner commands are applied before the full manager pass builds transport work. A post-I/O poll failure leaves the fact in the producer-local bounded buffer for that next input drain. An unexpected fact first produced by the pre-I/O manager pass is preserved and diagnosed, but its command is deferred because transport work may already have been built. | Require cross-owner facts to enter through input or network-completion paths and become available no later than the post-I/O owner poll. First admission during the ordinary pre-I/O pass is invalid. Dependency-aware request cancellation/replay is not part of this KIP. |
| Diagnostics | Contract, manager-poll, action-failure, and application-wakeup counters are present. | TRACE adds publication generation, deadline source, action reason, and destination without hot-path collection formatting. |
| Consumer variants | Regular, share, and Streams heartbeat paths use the typed progress model; Streams topology push uses the coordinator snapshot/version rule. Share fetch production still has compatibility `PollResult` construction and lacks an equivalent reactor-level recovery test. | Regular, share, and Streams compositions each prove all typed outputs and cross-owner recovery without consumer-type branches in `ConsumerReactor`. |

The following names map the model to the current POC. They are implementation evidence, not prerequisites for
understanding the design:

```text
response callback -> producer-local PendingManagerEvents
  -> RequestManagers.drainPendingManagerEvents() at the next input boundary
  -> ConsumerReactor evaluates the stable ManagerEvent batch
  -> owner commands apply before the full manager pass

ordinary manager-poll output
  -> PollResult.managerEvents()
  -> ConsumerReactor.stageManagerEventBatch(...)
  -> RequestManagers.planManagerEvents(...)
  -> ManagerCoordinationPolicy.evaluate(...)
  -> CoordinationPlan.managerCommands() or reactorActions()
  -> ConsumerReactor applies the owner command or executes the action at its phase boundary
```

`pending` means retained by the producing manager before publication. Callback-produced pending facts are drained at
the input boundary so their owner commands apply before request building. `stageManagerEventBatch(...)` evaluates the
complete ordered event set from one pre-I/O or post-I/O phase. A post-I/O owner command is retained until the next
iteration so it is applied before application inputs, the next full manager pass, and the next network poll.
`PendingManagerEvents` retains the greatest observed coordinator version for one event type so an older response
cannot overwrite a newer pending observation. The coordinator owner still performs the final comparison against its
current version.

A cross-owner fact first created by the pre-I/O manager pass violates the target admission rule: by then other managers
may have already built transport commands from the earlier owner snapshot. The POC retains and diagnoses that fact
instead of losing it, but this is migration containment rather than generic stale-send recovery. Such a producer must
move the fact to an input or network-completion path. Dependency-aware cancellation/replay is outside this KIP.

Other current implementation details include:

- `ConsumerReactorGateway` names the application-side submit, wait, wake, schedule-read, and close boundary; it
  replaces the less precise `ApplicationEventHandler` name without changing runtime behavior.
- `FetchRequestManager.createFetchRequests()` retains at most one current waiter for equivalent fetch intent, so a
  later preparation failure reaches that waiter without creating an unbounded waiter list.
- `UnsentRequest` currently implements `NetworkCommand`; its `transportRequest()` accessor is a compatibility bridge
  to `NetworkClientDelegate.addAll(List<UnsentRequest>)`, not a claim that the request was sent.
- the bounded `ManagerPollCache` stores only the current manager result and retained deadline; it does not retain
  history. The existing maximum network poll timeout remains a safety safeguard rather than the semantic retry
  interval for `AwaitInput`.
- commit and Streams heartbeat still expose legacy application-wait calculations. The reactor contains repeated
  delivery of a persistent zero compatibility value, but eliminating every unnecessary one-shot zero wait remains
  manager migration work.

### Phase 1: Establish the decision boundary

- Establish the existing background loop as `ConsumerReactor` without changing thread topology.
- Publish one `ReactorSchedule` and execute actions only after publication.
- Add canonical `PollResult.progress(...)`, `retryAfter(...)`, and `awaitInput()` factories over `ManagerEvent`,
  `NetworkCommand`, and `NextPollCondition`. Keep raw delays, `UnsentRequest`, and compatibility constructors as
  isolated adapters while producers migrate; remove the redundant `StateTransition` family and `awaitEvent()` alias.
  Strict factories reject contradictory shapes;
  production validation identifies and counts an adapter-created empty immediate result and replaces it with
  `AwaitInput` rather than allowing a zero-timeout loop.
- Migrate fetch reconnect, in-flight, paused, missing-leader, and buffered-data decisions to the strengthened
  `PollResult`.
- Stage manager- and transport-produced `BackgroundEvent` values until the corresponding schedule is published, then
  publish them and execute the coalesced application wake last.
- Isolate manager polling and `ReactorAction` failures so one fault cannot skip network I/O or suppress later effects.

Exit evidence: reconnect-backoff, schedule aggregation, publish-before-wakeup, and busy-loop tests pass with no public
API change.

### Phase 2: Migrate manager and consumer-specific decisions

- Move remaining manager deadlines into `NextPollCondition`; all completed manager facts already use `ManagerEvent`.
- Replace mutation calls that cross ownership domains with typed immutable `ManagerEvent` values routed through the
  selected composition to one state owner. Calls inside an explicitly defined regular/share/Streams protocol driver
  are not prohibited by this rule.
- Where a manager needs a coherent cross-manager view, publish a small opt-in owner-local snapshot and fence delayed
  observations with the captured snapshot version.
- Introduce the regular/share driver boundary around the existing managers and remove consumer-type decisions from
  the shared reactor loop.
- Remove hard-coded coordinator dependency branches from `ConsumerReactor`; the next stable full manager pass observes
  cross-manager changes before the next network poll without a central dependency graph.
- Complete the liveness proof for the POC's removal of application-side assignment/position rescans: each assignment,
  position update, and failure that can unblock `poll(...)` must publish a corresponding background event or reactor
  action, and real consumer integration tests must cover those waits.

Exit evidence: multi-manager, stale-completion, regular-consumer, and share-consumer suites pass independently.

### Phase 3: Remove remaining direct application effects

- Route remaining operation completions, data/event publication, callback requirements, and internally selected
  application wakeups through the publish-before-action boundary.
- Combine equivalent wake actions across the complete reactor iteration, not separately at each execution phase.
- Remove direct application completion and wakeup decisions from `NetworkClientDelegate` while retaining transport
  correlation locally.
- Drain already selected actions during close before queues, buffers, and pending-operation resources are closed.
- Remove compatibility adapters only after every success, error, timeout, cancellation, interruption, and close path
  has a terminal result.

Exit evidence: no manager or transport callback directly selects an application wake/error path, and every admitted
operation completes once.

Each phase leaves the repository runnable and can be reverted independently.

## Test Plan

Tests assert observable event-processing behavior rather than private method coverage. Items explicitly marked
target are migration exit criteria rather than claims about the current POC. Required coverage includes:

- reconnect backoff creates no request or wake before its deadline and creates the request when the deadline expires;
- an earlier heartbeat or commit deadline cannot erase or postpone a retained fetch deadline;
- schedule publication occurs before actions selected before network polling and after network completions;
- repeated no-progress manager results do not produce a zero-timeout loop or wakeup ping-pong;
- an adapter-created empty `PollImmediately` result is diagnosed and converted to `AwaitInput` in production, while
  finite retries and input waits produce the intended schedule;
- current Phase 1 behavior combines equivalent wake reasons once per execution phase; the Phase 3 target combines
  them into at most one primitive application wakeup per complete reactor iteration;
- heartbeat, auto-commit, fetch, coordinator, metadata, and share-acknowledgement tests distinguish immediate output,
  finite retry, and input-wait states without moving their local rules into the reactor;
- current readiness slice: a real `CommitRequestManager` remains input-driven after its auto-commit timer expires;
  completion during network polling re-polls that manager, publishes a newer `ReactorSchedule` generation, and only
  then wakes the application thread;
- current heartbeat slice: the shared regular/share manager derives coordinator, membership, timer, and in-flight
  outcomes from one activation projection; after admitting a heartbeat, its `PollResult` names network completion
  rather than retaining a periodic reactor deadline, while the legacy application-wait projection remains compatible;
- a coordinator completion makes dependent work visible in the next ordered pass, and any resulting request is sent
  by the following network poll without a hard-coded same-phase manager branch;
- a request target and its owner version come from one immutable snapshot;
- an observation captured from an older coordinator version cannot invalidate a later coordinator, while an
  observation matching the current version is applied;
- target: every manager consuming an opt-in owner-published view in one ordered pass uses the same captured snapshot
  version;
- current coordinator slice: pending `ManagerEvent` values remain latest-only and bounded, the owner retains only
  its current `CoordinatorSnapshot`, and in-flight work retains a version rather than snapshot history; target:
  equivalent evidence accompanies each additional snapshot family;
- target: no request manager directly invokes a mutation API owned by a different manager or driver; explicitly
  defined calls inside a regular, share, or Streams protocol driver remain permitted;
- an older offset-fetch completion cannot mutate a partition outside its captured scope;
- regular and share paths use the same kernel without consumer-type branches;
- current: close preserves an async-poll completion selected immediately before close; target: callback,
  interruption, cancellation, and every application-event family produce exactly one terminal outcome;
- a real broker test proves public coordinator-loss recovery under the normal application poll loop, while a
  deterministic component test proves the internal deferred-event routing and next-network-poll ordering.
- metric tests verify the four counters in both metric groups, removal on close, and absence of event/manager tags;
  the A/B runs include the counters so their constant-time recording cost is included in the performance gate.

Each issue cited in Motivation requires a deterministic reproduction through the relevant production components. In
particular, `AsyncKafkaConsumerTest.testReactorPreservesNewPartitionAcrossOlderOffsetFetchCompletion` exercises the
real application-event, reactor, request-manager, `MockClient`, and subscription-state path for KAFKA-17066/17674.

Performance validation compares the proposed implementation with the immediately preceding async consumer under the
same group protocol, broker, workload, and client configuration. `ClassicKafkaConsumer` remains a secondary
historical reference for manual-assignment workloads. The required gates cover saturated throughput,
idle-to-first-record latency, idle CPU/wakeup rate, allocation per record, and reconnect recovery. A regression beyond
the predeclared threshold must be investigated before the corresponding migration phase is accepted. Detailed run
ordering, statistics, raw samples, commit ids, and Jenkins artifacts remain in the companion benchmark evidence
rather than this KIP.

## Appendix A: POC Evidence and Open Gates

This appendix records implementation evidence without making the POC structure part of the community decision. All
links are pinned to the code-complete evidence baseline
[`d2e5ff6eb7`](https://github.com/unknowntpo/kafka/tree/d2e5ff6eb7ffe18fc84d53d0e651d13a3d5942a0).

| Case | Verified evidence | Remaining gate |
| --- | --- | --- |
| Position scope | [`OffsetsRequestManagerTest.testUpdatePositionsDoesNotResetPositionBeforeRetrievingOffsetsForNewlyAddedPartition`](https://github.com/unknowntpo/kafka/blob/d2e5ff6eb7ffe18fc84d53d0e651d13a3d5942a0/clients/src/test/java/org/apache/kafka/clients/consumer/internals/OffsetsRequestManagerTest.java) and [`AsyncKafkaConsumerTest.testReactorPreservesNewPartitionAcrossOlderOffsetFetchCompletion`](https://github.com/unknowntpo/kafka/blob/d2e5ff6eb7ffe18fc84d53d0e651d13a3d5942a0/clients/src/test/java/org/apache/kafka/clients/consumer/internals/AsyncKafkaConsumerTest.java). The component test fails at pre-fix baseline `6744a718c2`. | Cross-component position-before-data publication and the exact KAFKA-18641 position/auto-commit race. |
| Heartbeat and commit readiness | [`ConsumerHeartbeatRequestManagerTest`](https://github.com/unknowntpo/kafka/blob/d2e5ff6eb7ffe18fc84d53d0e651d13a3d5942a0/clients/src/test/java/org/apache/kafka/clients/consumer/internals/ConsumerHeartbeatRequestManagerTest.java), [`ShareHeartbeatRequestManagerTest`](https://github.com/unknowntpo/kafka/blob/d2e5ff6eb7ffe18fc84d53d0e651d13a3d5942a0/clients/src/test/java/org/apache/kafka/clients/consumer/internals/ShareHeartbeatRequestManagerTest.java), [`ConsumerReactorTest.testRealHeartbeatInvalidationIsRoutedBeforeNextNetworkPoll`](https://github.com/unknowntpo/kafka/blob/d2e5ff6eb7ffe18fc84d53d0e651d13a3d5942a0/clients/src/test/java/org/apache/kafka/clients/consumer/internals/ConsumerReactorTest.java), and [`ConsumerReactorCommitReadinessTest.testCompletionPublishesScheduleBeforeApplicationWakeup`](https://github.com/unknowntpo/kafka/blob/d2e5ff6eb7ffe18fc84d53d0e651d13a3d5942a0/clients/src/test/java/org/apache/kafka/clients/consumer/internals/ConsumerReactorCommitReadinessTest.java). | Exact historical KAFKA-20253 CPU reproduction and KAFKA-20970 end-to-end reproduction. |
| Fetch wakeup | [`AsyncKafkaConsumerTest.testPausedPartitionDoesNotProduceNoProgressWakeup`](https://github.com/unknowntpo/kafka/blob/d2e5ff6eb7ffe18fc84d53d0e651d13a3d5942a0/clients/src/test/java/org/apache/kafka/clients/consumer/internals/AsyncKafkaConsumerTest.java) observes the invalid wake at pre-fix baseline `9521d77da3` and its absence in the POC. [`ManagerCoordinationPolicyTest`](https://github.com/unknowntpo/kafka/blob/d2e5ff6eb7ffe18fc84d53d0e651d13a3d5942a0/clients/src/test/java/org/apache/kafka/clients/consumer/internals/ManagerCoordinationPolicyTest.java) covers the typed fact-to-action mapping. | Remove the direct async `FetchBuffer` notification compatibility path and reproduce KAFKA-20397 metadata-error/wait-entry ordering. |
| Operation lifecycle and close | [`ConsumerReactorTest.testCleanupExecutesStagedAsyncPollCompletion`](https://github.com/unknowntpo/kafka/blob/d2e5ff6eb7ffe18fc84d53d0e651d13a3d5942a0/clients/src/test/java/org/apache/kafka/clients/consumer/internals/ConsumerReactorTest.java) proves cleanup executes an already selected completion. | KAFKA-18160 exactly-once callback acknowledgement and interruption; KAFKA-19357/KAFKA-18569 coordinator-discovery lifecycle during close. |

### Historical A/B benchmark

The completed A/B run compared proposal revision `032899a6ab` with async-consumer baseline `9d940a6537` under
byte-identical inputs. Across five repetitions it measured a 41.5% reduction in idle CPU and a 60.2% reduction in
idle network polls per second. First-record p50, p95, and p99 differences were not statistically significant and the
predeclared p99 regression gate passed. The supported conclusion is therefore narrower than “the reactor is faster”:
the tested revision performed less unnecessary idle polling without a demonstrated first-record latency regression.

This is preliminary historical evidence, not acceptance evidence for the current POC. The candidate revision still
requires saturated-throughput, allocation-per-record, reconnect-recovery, rebalance, and share-consumer workloads.
Run configuration, statistics, limitations, and artifacts are retained in
[Consumer Reactor A/B benchmark results](consumer-reactor-ab-benchmark-results.md).

## Documentation Plan

Contributor documentation will describe the reactor, driver, and manager ownership boundary, schedule and action
ordering, and the evidence required before each compatibility path is removed. Public migration documentation is
unnecessary unless a later proposal adds capacity configuration, overload behavior, or observable name changes.

## Rejected Alternatives

### Keep distributed decisions with local fixes

Local timeout conditions, application-side rescans, and generic wakeup booleans can address individual failures but
retain multiple decision authorities. They cannot consistently preserve reason and publication ordering.

### Let the reactor infer whether a manager can make progress from a raw delay

A raw zero delay is ambiguous: it can mean executable work is ready, or that a local timer is overdue while another
condition still prevents progress. Making the reactor interpret heartbeat, commit, fetch, metadata, and share
acknowledgement state would duplicate manager rules in the shared kernel. The constrained `PollResult` shapes instead
require each manager to classify its own result and let the reactor validate only the generic no-progress invariant.

### Add a generic readiness kernel or signal registry

The existing reactor already receives application inputs, network completions, cancellation, and shutdown wakeups.
A second signal registry would duplicate those channels and add lifecycle, boundedness, and missed-signal questions.
The minimal rule is local: a manager derives one activation projection and reports its result with
`NextPollCondition`. New readiness infrastructure is justified only if a later manager demonstrates a dependency that
the existing input paths and stable ordered pass cannot express.

### Put all consumer logic in one reactor class

This centralizes code rather than decisions and would mix regular/share behavior into the execution kernel. Managers
and drivers keep their rules local while the reactor remains the final coordination owner.

### Implement separate regular and share reactor stacks

Duplicating queue drain, scheduling, publication, shutdown, and action ordering invites different concurrency bugs.
The consumers share the kernel but not their state machines.

### Migrate all managers in one patch

An all-at-once rewrite removes comparison seams and makes regressions difficult to localize. The phased approach
keeps each slice runnable and independently testable.

### Traverse a dynamic manager dependency graph

The manager set is small and fixed for one consumer instance. A stable full ordered pass before each network poll is
the correctness baseline and has `O(active managers)` cost. If measurement later justifies incremental polling, the
composition may own a static fact-type-to-manager index and poll a stable dirty set. Recursive polling, fixed-point
evaluation, and a dynamic dependency DAG are non-goals.

### Replay transport work after a pre-I/O cross-owner fact

Replaying or cancelling requests already built in the same pre-I/O pass would require dependency metadata for every
command, a definition of which manager state may be replayed, and new duplicate-attempt semantics. The simpler
invariant prevents that state: cross-owner facts enter through input or network-completion paths, before the next full
manager pass builds transport work. The POC's pre-I/O containment remains diagnostic support for migration errors,
not a second target execution model.

## Related Work

- [KAFKA-14246](https://issues.apache.org/jira/browse/KAFKA-14246) and the
  [consumer threading refactor design](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217393224/Consumer+threading+refactor+design)
  deliberately introduced an event-driven communication boundary between the application and network threads. That
  design did not attempt to model all same-thread request-manager coordination as events, so cross-manager progress,
  lifecycle, scheduling, and application-visible effect decisions could remain distributed across managers and
  completion paths. This KIP builds on that topology by defining `ConsumerReactor` as the background-thread
  orchestrator and by making cross-manager ordering, state ownership, schedule publication, and action ordering
  explicit; it does not reject or replace the original refactor.
- [KIP-945](https://cwiki.apache.org/confluence/display/KAFKA/KIP-945%3A%2BUpdate%2Bthreading%2Bmodel%2Bfor%2BConsumer)
  is a WIP proposal documenting the broader threading-model intent. It remains related history, not this KIP's
  baseline or a prerequisite.
- [KAFKA-20854 / PR #23014](https://github.com/apache/kafka/pull/23014) narrows one busy-loop cause. This proposal uses
  the same problem decomposition and generalizes the scheduling and action boundary across managers.
