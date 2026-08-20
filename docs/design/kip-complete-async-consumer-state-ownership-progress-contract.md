# Complete the Async Consumer State Ownership and Progress Contract

## Draft status

This is a local, KIP-shaped design draft. It has not been assigned a KIP number, submitted to the Apache Kafka
wiki, associated with a Jira, or sent to the Kafka development mailing list.

This proposal builds on the async consumer threading refactor tracked by
[KAFKA-14246](https://issues.apache.org/jira/browse/KAFKA-14246) and its
[consumer threading refactor design](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217393224/Consumer+threading+refactor+design).
It does not propose the background event loop as new work. It completes contracts which remain implicit after that
refactor: mutable-state ownership, progress feasibility, deadline and wakeup delivery, transport-policy boundaries,
and resource admission.

[KIP-945](https://cwiki.apache.org/confluence/display/KAFKA/KIP-945%3A%2BUpdate%2Bthreading%2Bmodel%2Bfor%2BConsumer)
is related history, not a dependency or approval gate. It remains WIP, with its detailed threading, data-transfer,
and network-I/O sections still marked TBD. This proposal can therefore be reviewed independently while reusing the
terminology documented by KIP-945 and the implementation delivered through KAFKA-14246.

## Summary

The async regular consumer and share consumer already use a background event loop, request managers, network I/O,
cross-thread event queues, futures, and data buffers. However, no single component currently owns the complete
answer to two questions:

1. What work can make progress now?
2. Which state transition, completion, capacity change, or deadline should cause the next execution or application
   notification?

This proposal makes `ConsumerReactor` the final decision authority for those questions. Protocol-specific state
machines continue to decide protocol policy, but they report typed `ProgressIntent`s and ordered transition outputs
to a thin shared reactor kernel. The kernel derives `ReactorEffect`s, orders application commands and network
outcomes, aggregates deadlines and capacity constraints, publishes synthetic-wake decisions before notification,
and coordinates a resource-capacity plan enforced at each ownership boundary.

The shared kernel serves both regular `AsyncKafkaConsumer` and `ShareConsumerImpl` because they have the same
concurrency topology. Their protocol policy remains separate in `RegularConsumerDriver` and `ShareConsumerDriver`.
This proposal does not unify assignment and offset semantics with share acquisition, lock renewal, acknowledgement,
or share membership.

## Motivation

### The missing contract

The current implementation contains the right broad pieces, but final decisions remain distributed:

- the application thread inspects subscription and fetch state, submits events, waits on futures, and waits on
  `FetchBuffer`;
- `ConsumerReactor` drains application events, polls request managers, performs network I/O, and publishes an
  application wait bound;
- request managers separately report network delays and application delays;
- response paths can complete futures, enqueue background events, add fetch data, or request application wakeups;
- application and background queues, callback queues, pending requests, futures, and retained buffers have
  independent or missing admission policies.

These mechanisms can observe different state snapshots. A local timeout may express urgency without showing that
progress is feasible. A wakeup may report only that a method returned no request, rather than that state actually
changed. A correct notification can also be lost when the decision is published after the application begins
waiting, or duplicated when both a data signal and a synthetic wakeup represent the same transition.

The concise failure mode is:

> State is distributed, notification is distributed, and no owner combines the facts into one next-step decision.

### Bug evidence

A bug supports this proposal only if its failure path demonstrates split ownership, incompatible progress snapshots,
or a missing/duplicate cross-thread acknowledgement. This avoids attributing unrelated consumer defects to the
reactor design.

| Evidence | Invalid intermediate state | Missing contract |
| --- | --- | --- |
| [KAFKA-17066 / PR 16885](https://github.com/apache/kafka/pull/16885) | Position update work was split across application and background threads. | One owner for a complete position transition. |
| [KAFKA-18641 / PR 18737](https://github.com/apache/kafka/pull/18737) | Application position advancement raced with a background auto-commit snapshot. | Ordered position update and commit-snapshot creation. |
| [KAFKA-15529 / PR 21476](https://github.com/apache/kafka/pull/21476) | Consumed state became visible before its corresponding position. | Atomic publication of a completed transition. |
| [KAFKA-17439 / PR 17035](https://github.com/apache/kafka/pull/17035) | Application and background threads inspected fetch-buffer state at different times. | One fetch scheduling decision from one snapshot. |
| [KAFKA-17182 / PR 18795](https://github.com/apache/kafka/pull/18795) | A buffer-state race caused unnecessary fetch-session removal and recreation. | Decision and execution under one owner. |
| [KAFKA-20426 / PR 22018](https://github.com/apache/kafka/pull/22018) | A zero wait was returned although manual assignment made heartbeat progress impossible. | Combine urgency with progress feasibility. |
| [KAFKA-20253 / PR 22836](https://github.com/apache/kafka/pull/22836) | Heartbeat urgency caused CPU spin while coordinator progress was blocked. | One decision across heartbeat urgency and coordinator state. |
| [KAFKA-20854 / PR 23014](https://github.com/apache/kafka/pull/23014) | An ambiguous empty fetch result caused application/background wakeup ping-pong. | Typed outcome and transition-specific notification. |
| [KAFKA-20397 / PR 21991](https://github.com/apache/kafka/pull/21991) | Metadata error publication raced with the application waiting on the fetch buffer. | One publication and wait-set protocol. |
| [KAFKA-18160 / PR 18089](https://github.com/apache/kafka/pull/18089) | Wakeup or interruption could skip a callback-completed event. | Exactly-once terminal acknowledgement. |

The claim is not that a reactor automatically prevents every bug. The narrower claim is that explicit ownership,
progress, publication, and acknowledgement contracts remove recurring invalid intermediate states and provide a
stable boundary for deterministic tests.

## Goals

1. Give each mutable protocol state one execution-context owner.
2. Make `ConsumerReactor` the only component that combines cross-manager constraints into final scheduling,
   application-wait, synthetic-notification, and aggregate capacity decisions. Boundary owners still perform atomic
   admission before ownership transfer.
3. Require every wakeup, retry, and reschedule to name a real transition, deadline, completion, command, or capacity
   change that can enable progress.
4. Bound every queue, pending-operation set, in-flight collection, and retained data buffer.
5. Preserve application-thread callback isolation and the existing consumer public APIs.
6. Reuse one thin execution kernel across regular and share consumers without combining their protocol state
   machines.
7. Migrate incrementally with behavior-preserving compatibility adapters and runnable evidence at every phase.

## Non-goals

- Introducing the application/background event-queue architecture; it already exists.
- Combining regular consumer and share consumer protocol semantics.
- Moving user callbacks onto the reactor thread.
- Adding another queue between request managers and `NetworkClientDelegate`; these components execute on the same
  owner thread.
- Rewriting every request manager in one change.
- Assigning a KIP number or making KIP-945 a prerequisite for this local draft.
- Changing the Kafka protocol or public `Consumer`/`ShareConsumer` APIs in the initial migration.

## Current architecture

`AsyncKafkaConsumer` and `ShareConsumerImpl` each create their own background execution context. They reuse much of
the same event-loop and network infrastructure, but do not share a consumer instance or protocol state.

```text
Application thread
  -> ApplicationEventHandler
     -> ApplicationEventQueue -- enqueue then wake --> ConsumerReactor

ConsumerReactor (one per consumer instance)
  -> ApplicationEventProcessor
  -> RequestManagers
  -> NetworkClientDelegate                         same-thread direct calls

ConsumerReactor
  -> BackgroundEventQueue
  -> FetchBuffer or ShareFetchBuffer
  -> CompletableFuture
  -> callback-required event                       cross-thread publication
```

`ApplicationEventHandler` is not the handler which executes protocol commands. It is the application-side gateway
and lifecycle handle: submit, submit-and-wait, wake, read the published wait decision, and close. The actual command
logic is in `ApplicationEventProcessor`, while `ConsumerReactor` drains and orders the commands. A later mechanical
rename to `ConsumerReactorHandle` would make that role clearer, but the rename is not required for the ownership
contract.

The reactor, request managers, and network delegate already execute on one thread, so thread ownership is mostly
clear in that path. Decision ownership is not. For example, `NetworkClientDelegate` currently has a
`notifyMetadataErrorsViaErrorQueue` policy flag: regular consumers pull metadata errors through one path, while share
consumers ask transport to publish an `ErrorEvent` directly. This puts application-visible policy in the transport
boundary even though callbacks still run on the reactor thread.

The application event queue, background event queue, and offset callback queue currently use unbounded
`LinkedBlockingQueue` instances. This makes the topology asynchronous without making its memory and overload
behavior complete.

## Proposed changes

### 1. Establish a thin shared `ConsumerReactor` kernel

Each consumer instance owns one reactor. The kernel has these responsibilities:

- drain application commands in a defined order and within a per-iteration work budget;
- invoke one protocol-specific driver synchronously;
- aggregate `ProgressIntent`s across state machines;
- enqueue requests and bound the network poll by the earliest real deadline;
- apply network outcomes on the same owner thread;
- publish an immutable decision snapshot before its associated notification;
- coalesce and dispatch `ReactorEffect`s;
- coordinate startup, callback acknowledgement, close, and failure termination;
- own the aggregate capacity plan while each queue, mailbox, and transport boundary atomically enforces its permits;
- expose metrics for queue depth, saturation, decision reason, wake reason, and loop progress.

The kernel is the final decision center, not the home of every protocol rule. It asks drivers and state machines for
facts, intents, requests, and transition outputs, then derives the one cross-component decision and its externally
visible effects.

### 2. Keep regular and share protocol policy in separate drivers

The shared contract is their concurrency topology, not the granularity of their protocol state.

| Component | Protocol policy owned |
| --- | --- |
| `RegularConsumerDriver` | assignment, positions, offset fetch/commit, classic and consumer group membership, rebalance transitions, regular fetch policy |
| `ShareConsumerDriver` | share membership, record acquisition, acquisition-lock renewal/release, acknowledgement, share fetch policy |

Share consumption remains partition-aware, and acquisition, lock, and acknowledgement state may be record- or
batch-granular. It is not modeled as a coarser form of the regular consumer.

The kernel must not branch on regular/share protocol types; construction may select one driver implementation.
`isShareConsumer` switches or policy booleans in the running kernel indicate protocol policy has leaked into the
execution contract. A driver may implement a common interface, but its internal state and transitions remain
protocol-specific.

### 3. Make mutable state ownership explicit

Target ownership is:

| State | Owner | Cross-thread representation |
| --- | --- | --- |
| Membership, assignment/acquisition, position, retry, request-session, commit/ack state | Protocol driver on reactor thread | Immutable snapshot or terminal operation result |
| Application API call and timeout budget | Application thread until admitted | Typed application command with operation id and deadline |
| Network correlation and transport buffers | Network delegate on reactor thread | Typed `NetworkOutcome` returned to driver/kernel |
| Fetched or acquired records after publication | Data mailbox with explicit capacity ownership | Immutable or exclusively transferred batch |
| User callback execution | Application thread | Required event plus epoch-tagged completion acknowledgement |
| Final scheduling and notification decision | Reactor kernel | Immutable published decision snapshot |

`SubscriptionState` is migrated behind reactor commands. Application APIs that require immediate reads receive an
immutable view or a future completed from reactor-owned state. No mutable protocol object is concurrently read to
derive a second scheduling decision on the application thread.

### 4. Introduce `ProgressIntent`

A `ProgressIntent` is a declarative constraint from a driver/state machine to the reactor. It says what prevents or
enables the next transition; it does not wake a thread or choose the final poll timeout.

The target vocabulary is:

```text
READY(reason, generation)
AWAIT_EVENT(source, generation)
AWAIT_DEADLINE(source, absoluteDeadlineMs, generation)
AWAIT_CAPACITY(resource, generation)
AWAIT_CALLBACK(operationId, epoch)
```

Absolute deadlines prevent time spent between computation, publication, and reading from being added again. The
identity of a deadline is `source + mode + absolute deadline + semantic generation`. Re-observing the same blocked
operation preserves its deadline and generation. A new operation at the same clock value gets a new generation and
therefore one new deadline transition.

Intent reduction is deterministic:

1. `READY` work executes fairly across managers until no `READY` intent remains or the iteration work budget is
   exhausted.
2. If `READY` remains after budget exhaustion, the reactor performs a non-blocking network poll and continues the
   work in the next iteration. `READY` alone never synthesizes an application wake; only an externally observable
   transition does.
3. If no work is ready, the earliest finite deadline bounds the network poll and application wait.
4. Event, capacity, and callback blockers remain observable but cannot hide a finite deadline contributed by other
   work.
5. Urgency cannot make work ready when its prerequisite is unavailable.

For example, an in-flight fetch on one node does not hide reconnect backoff on another node. The per-iteration work
budget prevents a continuously ready manager from starving network polling or another manager.

During migration, an adapter may translate `RequestManager.maximumTimeToWait()` into a compatibility intent. The
adapter is explicitly tagged because a relative legacy delay needs conservative preservation across early network
returns. Native intent producers must not inherit that heuristic.

### 5. Introduce `ReactorEffect`

A `ReactorEffect` is the reactor's final description of an externally visible action. Drivers report protocol facts
and ordered transition outputs such as a completed operation, produced data, or required callback. Only the reactor
derives, orders, coalesces, and dispatches `ReactorEffect`s. In particular, a driver cannot request a synthetic
application wake directly.

Representative effects are:

```text
COMPLETE_OPERATION(operationId, outcome)
PUBLISH_BACKGROUND_EVENT(event)
PUBLISH_DATA(batch, ownershipToken)
REQUIRE_CALLBACK(operationId, epoch, callback)
WAKE_APPLICATION(reason, generation)
TERMINATE_CONSUMER(cause)
```

`COMPLETE_OPERATION` is the only terminal operation truth source. `ProgressIntent` contains only readiness and
blocking facts. `WAKE_APPLICATION` is derived by the reactor from a published state transition, deadline expiry, or
capacity release; it is never a driver-selected scheduling policy.

Effect identities make duplicate delivery testable. Different synthetic reasons or generations covered by the same
published snapshot collapse into one primitive signal, while the full reason/generation set remains available for
diagnostics and tests. Explicit `Consumer.wakeup()` is a user interruption and never participates in this
coalescing. Operation completion and callback acknowledgement are not freely coalesced: each operation id has
exactly one terminal result.

The current POC uses `ApplicationProgressEffect` as a bounded manager-to-reactor transition set. That is an
incremental implementation of the wake-related subset of `ReactorEffect`; it need not become the final public name.

Actual data insertion and explicit `Consumer.wakeup()` remain distinct primitives during migration. Adding data to
a mailbox signals the condition that truly changed; an explicit user wakeup is an interruption contract. A data
response must not also enqueue a synthetic terminal wake for the same transition.

### 6. Define the reactor iteration and publication order

One iteration follows this order:

1. Drain a bounded number of application commands and callback acknowledgements.
2. Apply commands through the selected protocol driver.
3. Collect proposed requests, progress intents, and ordered pre-I/O transition outputs; derive pre-I/O effects.
4. Aggregate and publish the pre-I/O decision which bounds network polling.
5. Dispatch only effects whose required state and decision have already been published.
6. Poll network I/O no longer than the earliest aggregate deadline.
7. Convert transport completions into typed `NetworkOutcome`s and apply them through the driver.
8. Drain post-I/O intents and transition outputs; derive post-I/O effects.
9. Publish the post-I/O decision.
10. Dispatch coalesced post-I/O effects and one-shot deadline notifications.

For synthetic wake decisions, this produces one ordering rule:

> Apply transition, publish resulting state and decision, then notify.

Until `PUBLISH_DATA` migration is complete, atomic mailbox insertion remains a state-bearing notification
exception. Today `FetchBuffer.add()` inserts data and signals its condition during the network callback, before the
post-I/O decision is published. This is safe because the notification carries the state that satisfies the waiter;
it must not be described as a synthetic scheduling decision. A later phase moves data publication into post-I/O
effect dispatch so the general effect ordering also covers mailbox delivery.

Request managers and `NetworkClientDelegate` are direct, same-thread collaborators. Adding a queue between them
would add latency and another unbounded ownership boundary without isolating any mutable state.

### 7. Define the deadline and wakeup protocol

The reactor must satisfy all of these properties:

1. A published decision is immutable and safe for the application thread to read.
2. A newly shorter wait decision is published before releasing an application waiter that may hold an older
   snapshot.
3. The same decision bounds the network poll, so progress does not depend on the application polling again.
4. Expiry is delivered once for each semantic decision generation.
5. Expiry is marked delivered before notification, preventing repeated observation of stale `0 ms` waits.
6. An event-only wait does not create a periodic wakeup unless a compatibility deadline is explicitly present.
7. Every synthetic wake records a reason and generation; a no-progress method return is not a wake reason.

This protocol replaces application-side safety rescans. A safety rescan is a second calculation performed by the
application thread before it waits, such as re-reading `SubscriptionState` or `FetchBuffer` to shorten a cached
reactor timeout. It can mask a missing reactor decision, but it creates two decision authorities and two snapshots.
It may avoid one lost wake while introducing drift, spin, or inconsistent wait behavior elsewhere.

### 8. Make transport report outcomes, not application policy

`NetworkClientDelegate` remains responsible for request correlation, serialization, sending, receiving, transport
timeouts, and connection state. It reports typed `NetworkOutcome`s to the reactor/driver. Each outcome contains the
request and attempt identity, terminal kind, completion time, payload/error ownership, and the winning terminal
transition. Response, timeout, cancellation, and disconnect race through one compare-and-complete point, so exactly
one outcome owns terminal completion. The driver and kernel then decide whether to retry, complete an operation,
publish an error, or derive a synthetic application wake.

Internal completion of an `UnsentRequest` future may remain in the transport boundary because it is correlation.
Directly selecting an application-visible error queue or notification policy does not. Policy flags such as
`notifyMetadataErrorsViaErrorQueue` are removed as protocol drivers adopt the outcome boundary.

### 9. Define and enforce hard resource limits

`ConsumerReactor` owns the aggregate capacity plan. Each boundary owner atomically reserves and releases typed
permits before ownership transfer; it does not synchronously ask the reactor for every enqueue. Queue slots,
retained bytes, and pending-operation permits are separate budgets because releasing one does not necessarily
release the others.

Every asynchronous boundary must declare capacity, admission behavior, ownership transfer, and release event.

| Resource | Required bound | Admission/backpressure contract | Capacity release |
| --- | --- | --- | --- |
| Application event queue | Per-consumer command count and retained-byte budget | Producer atomically reserves queue, byte, and operation permits within the API deadline | Queue slot at dequeue; bytes when command payload is released; operation permit at terminal completion |
| Background event queue | Count and retained-byte budget, with terminal delivery reserved | Boundary atomically reserves permits; coalesce replaceable snapshots; throttle producer work; never silently drop terminal/error events | Queue slot at dequeue; retained bytes after application processing |
| Callback queue | Pending callback count | Stop admitting dependent transitions and report `AWAIT_CALLBACK`; reactor never executes user code | Epoch-tagged callback completion |
| Fetch/share data mailbox | Configured memory budget plus per-partition/acquisition accounting | Stop new fetch/acquisition work and report `AWAIT_CAPACITY` | Application consumes or discards owned batch |
| Unsent requests | Global and per-node count/bytes | Driver reports `AWAIT_CAPACITY`; do not grow an unbounded transport queue | Send, timeout, cancellation, or failure |
| In-flight requests | Global and per-node count/bytes | Request production is gated by protocol and transport capacity | Response, disconnect, timeout, or cancellation |
| Pending operations/futures | Per-consumer count | Admission observes API deadline; every admitted id gets exactly one terminal acknowledgement | Success, failure, timeout, cancellation, or close |
| Retained response/deserialization buffers | Per-consumer byte budget | Transfer ownership only when destination capacity is reserved | Parse completion, delivery, discard, or close |

Each admitted operation reserves the control-path capacity needed for exactly one terminal result. The consumer also
reserves fixed control permits at construction for close and fatal failure. Callback completion and cancellation use
their operation's reserved permit; close and fatal failure use the construction-time reserve. None depends on space
becoming available in a saturated data queue. Replaceable snapshots may be coalesced but terminal outcomes may not
be silently dropped.

Protocol operations not created by an application API require the same reservation discipline. Before entering a
broker-triggered rebalance or share callback-required state, the driver atomically reserves a callback slot and its
terminal permit from an internal control budget. If reservation is unavailable, it reports `AWAIT_CAPACITY` without
publishing a half-admitted callback state; any applicable protocol deadline remains visible to intent reduction.

Exact default capacities and whether any require public configuration are unresolved in this local draft. Phase 7
is not compatibility-neutral and cannot be implemented or externally proposed until the capacity defaults,
accounting units, overload exception/timeout behavior, configuration/metric surface, and upgrade impact are part of
the proposal. The invariant is not satisfied by a nominal capacity if producers can bypass it or if terminal events
can deadlock behind replaceable work.

## Public interfaces

Phases 1 through 6 require no changes to Kafka protocols, public consumer APIs, or callback execution guarantees.
`Consumer.wakeup()` retains its existing user-visible semantics.

Internal types such as `ProgressIntent`, `ReactorEffect`, `NetworkOutcome`, protocol drivers, and immutable decision
snapshots are implementation contracts. Phase 7 may change overload behavior and add public configuration or
metrics. This local draft intentionally records that gap and is not ready for external publication until the full
Phase 7 surface is specified, or resource admission is split into a separately reviewed follow-up proposal.

Runtime thread names and existing metric names are not changed merely because the implementation class is named
`ConsumerReactor`. Any externally observable rename is a separate compatibility decision.

## Compatibility, deprecation, and migration plan

### Phase 1: Name the final decision owner

- Rename the execution component from `ConsumerNetworkThread` to `ConsumerReactor` without changing runtime thread
  names or metrics.
- Publish one immutable application-wait decision.
- Use that decision to bound network polling.

Exit evidence: aggregation, elapsed-time, publish-before-wakeup, and early-network-return tests pass with no public
API change.

### Phase 2: Introduce typed fetch progress

- Represent fetch preparation as requests plus typed blocking conditions.
- Migrate fetch retry deadlines to `ProgressIntent`.
- Preserve compatibility adapters for other managers.
- Treat PR #23014 as the baseline if it merges; evolve its boolean wake decision into typed conditions instead of
  introducing a competing result type.

Exit evidence: missing leader, reconnect backoff, in-flight, buffer drain, no-fetchable, paused partition, and mixed
condition tests prove that only real deadlines or transitions cause progress.

### Phase 3: Centralize synthetic application effects

- Route empty/failing/expired fetch completion through the reactor.
- Coalesce equivalent wake effects.
- Publish post-I/O decisions before dispatching post-I/O wake effects.
- Remove the application-side `SubscriptionState`/`FetchBuffer` safety rescan once deterministic replacement coverage
  is present.

Exit evidence: a groupless/manual-assignment consumer retries after reconnect backoff; unsent expiration wakes one
real blocked waiter; data plus completion does not leave a duplicate retained wake.

### Phase 4: Move subscription and acquisition transitions behind drivers

- Move regular assignment, position, seek, pause/resume, commit snapshots, and reconciliation behind
  `RegularConsumerDriver` commands.
- Move share acquisition, locks, acknowledgements, and share membership behind `ShareConsumerDriver` commands.
- Replace application-thread mutable reads with immutable snapshots or operation futures.

Exit evidence: ownership tests reject off-reactor mutation; regular and share protocol suites pass independently.

### Phase 5: Extract transport outcomes and driver boundaries

- Replace application-visible `NetworkClientDelegate` policy with typed outcomes.
- Remove regular/share policy booleans and branches from shared infrastructure.
- Keep transport correlation local.

Exit evidence: identical transport outcomes can be handled differently by regular/share drivers without branching in
the kernel or delegate.

### Phase 6: Unify lifecycle and callback acknowledgement

- Use operation ids and callback epochs across startup, rebalance/share callbacks, close, and pending commits/acks.
- Complete every admitted operation exactly once on success, failure, timeout, cancellation, or shutdown.

Exit evidence: interruption and close tests cover every terminal edge; a blocked user callback cannot block reactor
network progress.

### Phase 7: Enforce resource admission

- Finalize the public compatibility decision: specify capacity defaults, accounting units, overload behavior,
  exceptions/timeouts, metrics, and any configuration before implementation.
- Replace unbounded queues and collections one boundary at a time.
- Add saturation metrics and deterministic overload tests before enabling each bound by default.
- Reserve terminal control-path capacity at operation admission so terminal events cannot be starved by replaceable
  work or saturated data queues.

Exit evidence: sustained producer pressure reaches a documented bound, memory stabilizes, overload behavior is
deterministic, and shutdown still terminates all admitted operations.

Each phase leaves the repository runnable and can be reverted independently. Compatibility adapters are removed
only after their producers and all equivalent terminal paths have migrated.

## Test plan

Testing is organized around contracts rather than private method coverage.

### Deterministic component tests

- decision aggregation across event waits, absolute deadlines, and capacity waits;
- a continuously `READY` manager plus another manager's finite deadline, proving that the fair work budget and
  non-blocking network poll still deliver the deadline on time;
- elapsed-time subtraction and preservation of native deadline generations;
- publish-before-wakeup ordering for synthetic pre-I/O and post-I/O transitions;
- one-shot expiry and no stale zero-timeout decision;
- mixed partition/node conditions where a finite reconnect deadline survives an in-flight blocker;
- empty, failed, disconnected, timed-out, and cancelled request terminal paths;
- atomic state-bearing data publication without a second synthetic wake, followed by ordered `PUBLISH_DATA`
  coverage when that migration occurs;
- bounded effect coalescing and exactly-once operation completion;
- regular/share driver isolation with no kernel protocol branches;
- queue and buffer saturation, release, timeout, close, and reserved terminal capacity.

### Cross-component tests

- real `AsyncKafkaConsumer -> reactor handle -> ConsumerReactor -> FetchRequestManager -> FetchBuffer` with only the
  socket replaced by `MockClient`;
- groupless manual assignment with a valid position and reconnect backoff;
- unsent fetch expiration through the real network delegate, post-I/O effect drain, and blocking application waiter;
- equivalent share-consumer acquisition/acknowledgement paths through `ShareConsumerDriver`;
- callback-required/completed handshake under wakeup, interruption, and close.

### Broker integration and system tests

- real-socket broker stop/restart while a groupless manual-assignment consumer waits;
- coordinator loss and reconnect while heartbeat or lock-renewal deadlines are urgent;
- regular rebalance callbacks and share acknowledgement callbacks under slow user code;
- long-running bounded-memory saturation tests for application events, background events, fetch/share data, and
  pending operations;
- existing regular consumer, share consumer, KRaft, protocol, upgrade, and compatibility suites.

### Intent-to-assertion acceptance matrix

| Design intent | Required observable assertion |
| --- | --- |
| One final decision owner | Application wait and network poll derive from the same published reactor decision; no application rescan changes it. |
| No no-progress wakeups | The same generation produces at most one synthetic wake; a fixed virtual-time window has a bounded poll/wakeup count while paused, backing off, or in flight. |
| No lost shorter deadline | An application waiter using an older snapshot is released only after the shorter decision is visible. |
| Exactly-once deadline transition | One semantic generation produces at most one expiry notification, including same-timestamp retries. |
| Protocol isolation | Regular and share outcomes are tested through separate drivers with no type switch in the kernel. |
| Transport/policy separation | Network delegate returns an outcome and never chooses an application event channel. |
| Hard resource limits | Load beyond each bound has documented timeout/backpressure behavior and stable retained memory. |
| Callback isolation | Slow or failing user callbacks do not execute on or block the reactor; each required callback is acknowledged once. |
| Terminal completeness | Every admitted operation completes once across success, error, timeout, cancellation, startup failure, and close. |

## Rejected alternatives

### Continue adding local busy-loop conditions

Local timeout exceptions can fix one reproduction but preserve multiple decision authorities. New protocol states or
mixed conditions then recreate the same class of bug elsewhere.

### Keep an application-side safety rescan

The rescan can compensate for a stale reactor snapshot, but it cannot atomically observe reactor state and creates a
second scheduler. A publish-before-wakeup protocol directly fixes the stale-snapshot problem.

### Use a generic wakeup boolean

A boolean cannot distinguish data availability, in-flight completion, reconnect deadline, missing metadata, callback
completion, or capacity release. It also cannot represent mixed conditions or semantic generations.

### Put all protocol logic in one omniscient reactor class

This centralizes code rather than decisions. It produces regular/share branches, weakens protocol test boundaries,
and makes the kernel harder to evolve. Drivers keep policy local while the kernel remains the final execution
authority.

### Implement two complete reactor stacks

Duplicating queue drain, polling, publication, shutdown, and resource-limit logic invites different concurrency bugs
in regular and share consumers. Their common execution topology justifies a shared kernel; their protocol semantics
justify separate drivers.

### Add cross-thread queues between all components

Queues are useful at ownership boundaries. Between components already running on the reactor thread, they weaken
ordering, add wakeups, and introduce more capacity and shutdown problems.

### Leave queues unbounded and rely on caller discipline

An asynchronous client cannot guarantee bounded memory if internal admission is implicit. Caller discipline also
cannot reserve capacity for terminal events or coordinate multiple internal producers.

### Make KIP-945 completion a prerequisite

KIP-945 documents the broader threading model, but its key detailed sections remain WIP/TBD while the implementation
and bug history continue to evolve. Treating it as related history preserves continuity without blocking a focused,
testable ownership and progress contract.

### Migrate all managers and state in one patch

An all-at-once rewrite removes comparison seams and makes regressions difficult to localize. Tagged compatibility
adapters permit staged replacement with explicit exit criteria.

## Related work

- [KAFKA-14246](https://issues.apache.org/jira/browse/KAFKA-14246) and the
  [consumer threading refactor design](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217393224/Consumer+threading+refactor+design)
  established the asynchronous application/background event topology and partially implemented the network,
  request-manager, and subscription-state seams. The Jira was resolved for Kafka 3.8.0; this proposal completes and
  converges those partially implemented seams.
- [KIP-945](https://cwiki.apache.org/confluence/display/KAFKA/KIP-945%3A%2BUpdate%2Bthreading%2Bmodel%2Bfor%2BConsumer)
  documents terminology and the broader threading-model intent and remains WIP. This proposal reuses that terminology
  but does not depend on KIP-945's completion.
- [KAFKA-20854 / PR #23014](https://github.com/apache/kafka/pull/23014) is open and narrows one busy-loop cause by
  distinguishing whether an empty fetch-preparation result may wake the fetch buffer. This proposal adopts its
  problem decomposition; if the PR merges, Phase 2 evolves that result into typed progress conditions instead of
  introducing a competing result type.
- KAFKA-20426 / PR 22018 and KAFKA-20253 / PR 22836 demonstrate that raw wait-time minima are insufficient when
  urgency and feasibility are computed in separate components.

## Open decisions before external publication

1. Final internal names: `ProgressIntent`, `ReactorEffect`, `NetworkOutcome`, `RegularConsumerDriver`, and
   `ShareConsumerDriver` are working names.
2. Whether `ApplicationEventHandler` should be mechanically renamed to `ConsumerReactorHandle` in the same proposal.
3. Exact default capacities, byte-accounting rules, overload behavior, and any required public configuration.
4. Whether callback and application-wait notification share a single internal signal or retain specialized
   primitives behind one reactor decision.
5. Which immutable subscription/acquisition views are required for non-blocking application API reads.
6. Whether this material should eventually be submitted as a standalone KIP or used to complete another accepted
   community design after mailing-list feedback.
