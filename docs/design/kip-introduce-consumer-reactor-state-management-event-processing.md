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

Jira: TBD

## Motivation

### Summary

The regular consumer and share consumer already use a background event loop, request managers, network I/O,
cross-thread event queues, futures, and data buffers. However, state transitions and the decisions to poll, wait, or
wake are still distributed across the application thread, background thread, request managers, and completion
paths. These components can act on different snapshots, causing repeated wakeups, busy loops, lost notifications,
and state-publication races.

This proposal introduces `ConsumerReactor` as the single execution owner and coordinator of consumer-level logical
resources: consumer state, queued work, deadlines, in-flight operations, buffer capacity, and
application-visible actions. It serializes their transitions and derives poll, wait, and notification decisions.

The reactor is shared infrastructure for the regular consumer and share consumer because they use the same
event-processing topology. Their membership, assignment or acquisition, fetch, commit or acknowledgement, and
callback rules remain in separate protocol-specific components. `ClassicKafkaConsumer` remains a legacy
compatibility and performance reference and is not retrofitted into the reactor by this proposal.

### Current problem: decisions are distributed

The current consumer implementation has the required execution components, but final state, progress, wait, and
notification decisions remain distributed:

- the application thread inspects subscription and fetch state, submits events, waits on futures, and waits on
  `FetchBuffer`;
- `ConsumerNetworkThread` drains application events, polls request managers, performs network I/O, and publishes an
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
proposed design.

| Evidence | Invalid intermediate state | Required behavior |
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

The claim is not that a reactor automatically prevents every bug. The narrower claim is that one event-loop owner,
ordered state publication, and explicit acknowledgement rules remove recurring invalid intermediate states and
provide a stable boundary for deterministic tests.

### Why local fixes are not enough

The current bugs can be fixed one condition at a time. For example, one path may add a retry backoff, another may
special-case a paused partition, and the application thread may rescan `SubscriptionState` or `FetchBuffer` before
waiting. Each fix can address its immediate reproduction, but it leaves more than one component able to decide the
next wait or wakeup.

This creates three recurring problems:

1. A local timeout can express urgency without knowing whether the required network or protocol precondition is
   available.
2. Two components can notify the application for the same transition, producing wakeup ping-pong or a retained
   duplicate wakeup.
3. A component can publish a shorter deadline after the application has already started waiting on an older
   snapshot, turning a busy-loop fix into an excessive wait or lost wakeup.

Adding more shared flags, cached timeouts, or application-side rescans does not remove these competing decision
points. It makes their ordering harder to define and test.

### Why the Reactor Pattern

The Reactor Pattern gives event-driven resources one execution owner. `ConsumerReactor` applies that ownership to
consumer state, work, deadlines, capacity, and actions. Resource-specific components encapsulate their representation
and local rules, but only the reactor drives their transitions.

For each iteration, the reactor must answer two questions from one ordered view of state:

1. What work can make progress now?
2. Which state transition, completion, capacity change, or deadline should cause the next execution or application
   notification?

Request managers continue to own their local state and transition rules. The reactor asks each manager to reconcile
that state with the latest input. Managers return ready work and the conditions for their next reconciliation. The
reactor combines those results, performs network polling, publishes the new schedule, and only then dispatches
application-visible actions. This does not move manager-specific rules into one large class.

The proposal changes resource ownership and coordination, not the existing execution topology.

The same reactor mechanics apply to the regular consumer and share consumer because both have the same concurrency
topology. Their manager-owned state remains separate: regular assignment and offsets are not modeled as share
acquisition, locks, and acknowledgements.

### Goals

1. Use one state-management and event-processing model for the complete regular- and share-consumer workflows.
2. Give each mutable state one execution-context owner.
3. Make `ConsumerReactor` the only component that combines cross-manager constraints into final scheduling,
   application-wait, synthetic-notification, and aggregate capacity decisions. Boundary owners still perform atomic
   admission before ownership transfer.
4. Require every wakeup, retry, and reschedule to name a real transition, deadline, completion, command, or capacity
   change that can enable progress.
5. Bound every queue, pending-operation set, in-flight collection, and retained data buffer.
6. Preserve application-thread callback isolation and the existing consumer public APIs.
7. Reuse one thin reactor execution layer across regular and share consumers without combining their state or rules.
8. Migrate incrementally with behavior-preserving compatibility adapters and runnable evidence at every phase.

### Non-goals

- Introducing the application/background event-queue architecture; it already exists.
- Replacing the existing event-driven application-thread/background-reactor threading topology.
- Combining regular consumer and share consumer protocol semantics.
- Moving user callbacks onto the reactor thread.
- Adding another queue between request managers and `NetworkClientDelegate`; these components execute on the same
  owner thread.
- Rewriting every request manager in one change.
- Retrofitting `ClassicKafkaConsumer` into the reactor or changing which regular-consumer implementation is
  selected by default.
- Making KIP-945 a prerequisite for this proposal.
- Changing the Kafka protocol or public `Consumer`/`ShareConsumer` APIs in the initial migration.

### Current execution flow

`AsyncKafkaConsumer` and `ShareConsumerImpl` each create their own background execution context. They reuse much of
the same event-loop and network infrastructure, but do not share a consumer instance or manager-owned state.

`ClassicKafkaConsumer` uses the legacy caller-thread execution model. It is part of Kafka's current consumer product
surface, but not part of the proposed shared reactor implementation. The proposal therefore distinguishes product
scope from implementation reuse: it defines the target execution model for regular and share consumers without
forcing two incompatible execution models into one reactor.

```text
Application thread
  -> ApplicationEventHandler
     -> ApplicationEventQueue -- enqueue then wake --> ConsumerNetworkThread

ConsumerNetworkThread (one per consumer instance)
  -> ApplicationEventProcessor
  -> RequestManagers
  -> NetworkClientDelegate                         same-thread direct calls

ConsumerNetworkThread
  -> BackgroundEventQueue
  -> FetchBuffer or ShareFetchBuffer
  -> CompletableFuture
  -> callback-required event                       cross-thread publication
```

`ApplicationEventHandler` is not the handler which executes protocol commands. It is the application-side gateway
and lifecycle handle: submit, submit-and-wait, wake, read the published `ReactorSchedule`, and close. The actual command
logic is in `ApplicationEventProcessor`, while `ConsumerNetworkThread` drains and orders the commands. A later
mechanical rename to `ConsumerReactorHandle` would make that role clearer, but the rename is not required for this
state ownership change.

The background loop, request managers, and network delegate already execute on one thread, but their consumer-level
resources do not have one coordinator for lifecycle, deadlines, capacity, and notification. Sharing a thread does
not by itself define that ownership. For example, `NetworkClientDelegate` currently has a
`notifyMetadataErrorsViaErrorQueue` policy flag: regular consumers pull metadata errors through one path, while share
consumers ask transport to publish an `ErrorEvent` directly. This puts application-visible policy in the transport
boundary even though transport completion callbacks still run on the existing background thread.

The application event queue, background event queue, and offset callback queue currently use unbounded
`LinkedBlockingQueue` instances. This makes the topology asynchronous without making its memory and overload
behavior complete.

## Public Interfaces

Phases 1 through 6 require no changes to Kafka protocols, public consumer APIs, or callback execution guarantees.
`Consumer.wakeup()` retains its existing user-visible semantics.

The coordination types introduced below are implementation details. Phase 7 may change overload behavior and add
public configuration or metrics. This draft intentionally records that gap and cannot enter a vote or Phase 7
implementation until the full surface is specified, or resource admission is split into a separately reviewed
follow-up proposal.

Runtime thread names and existing metric names are not changed merely because the implementation class is named
`ConsumerReactor`. Any externally observable rename is a separate compatibility decision.

## Proposed Changes

### 1. Introduce the reactor without changing the threading model

The proposal changes the responsibility boundary, not the number or placement of threads:

```text
Before
  Application thread
    -> ApplicationEventQueue
       -> ConsumerNetworkThread
          -> RequestManagers
          -> NetworkClientDelegate

  wait and wake decisions may also be recomputed or triggered
  by the application thread, request managers, and completion paths

After
  Application thread
    -> ApplicationEventQueue
       -> ConsumerReactor                 one resource and state-transition owner
          -> protocol-specific consumer logic
          -> RequestManagers
          -> NetworkClientDelegate        same-thread direct calls

  ConsumerReactor
    -> publishes state and ReactorSchedule
    -> then completes futures, publishes data/events, or wakes the application
```

The application thread and background thread remain. Application commands still cross the existing event queue,
network I/O still runs on the background thread, and user callbacks still run on the application thread. The change
is that application-visible reactor schedules are no longer recomputed in multiple places.

At a high level, one reactor iteration is:

1. Drain application commands from the existing queue.
2. Apply the corresponding consumer state transitions.
3. Ask request managers for requests and for the conditions that currently allow or block progress.
4. Combine those facts into one `ReactorSchedule` and use its earliest deadline to bound network polling.
5. Apply completed network responses, publish the resulting state and schedule, and only then dispatch any required
   application notification.

The proposal names this controller-style flow as follows:

```text
Input Event → ConsumerReactor → RequestManager.reconcile()
            → manager-owned state transition + ManagerReconcileResult
            → NextReconcile → ReactorSchedule → ReactorAction
```

An input event is an existing application command, network completion, deadline expiry, capacity release, or callback
acknowledgement. `ConsumerReactor` initiates reconciliation. A manager updates only its own state and returns a
`ManagerReconcileResult`; it does not call another manager or choose the global schedule. The reactor merges the
returned `NextReconcile` values, publishes one schedule, and then performs any externally visible action.

Ownership is split deliberately: each request manager owns its local state and rules; `ConsumerReactor` owns input
ordering and reconciliation execution; `ConsumerReactor` also owns the final cross-manager schedule and actions.

The remaining sections define each responsibility and its internal representation in that order.

### 2. Define the `ConsumerReactor` ownership boundary

Each consumer instance owns one `ConsumerReactor`. It has these responsibilities:

- drain application commands in a defined order and within a per-iteration work budget;
- invoke protocol-specific consumer logic synchronously;
- combine ready work, blockers, and deadlines reported by request managers;
- enqueue requests and bound the network poll by the earliest real deadline;
- apply network outcomes on the same owner thread;
- publish an immutable `ReactorSchedule` before its associated notification;
- coalesce and dispatch application-visible actions;
- coordinate startup, callback acknowledgement, close, and failure termination;
- own the aggregate capacity plan while each queue, mailbox, and transport boundary atomically enforces its permits;
- expose metrics for queue depth, saturation, schedule reason, wake reason, and loop progress.

The reactor owns coordination, not every resource implementation or protocol rule. It asks resource-specific
components for facts, requests, and completed transitions, then serializes the cross-resource decision and its
application-visible actions.

### 3. Make mutable state ownership explicit

Target ownership is:

| State | Owner | Cross-thread representation |
| --- | --- | --- |
| Membership, assignment/acquisition, position, retry, request-session, commit/ack state | Corresponding request manager on reactor thread | Immutable snapshot or terminal operation result |
| Application API call and timeout budget | Application thread until admitted | Typed application command with operation id and deadline |
| Network correlation and transport buffers | Network delegate on reactor thread | Completion returned on the same thread; Section 11 defines its precise form |
| Fetched or acquired records after publication | Data mailbox with explicit capacity ownership | Immutable or exclusively transferred batch |
| User callback execution | Application thread | Required event plus epoch-tagged completion acknowledgement |
| Final scheduling and notification decision | `ConsumerReactor` | Immutable published `ReactorSchedule` |

`SubscriptionState` is migrated behind reactor commands. Application APIs that require immediate reads receive an
immutable view or a future completed from reactor-owned state. No mutable manager-owned object is concurrently read to
derive a second scheduling decision on the application thread.

### 4. Reconcile manager-owned state

Request managers already tell the event loop whether they have requests and how long it may wait. Raw relative
timeouts are not enough to distinguish ready work from a blocked retry or an event-only wait. The reactor therefore
calls `RequestManager.reconcile()`. The manager reads the latest inputs, advances its own state, and returns one
`ManagerReconcileResult` containing proposed network work, one or more `NextReconcile` values, and state transitions
which may require a reactor action.

`NextReconcile` says when or under which condition the same manager must be reconciled again. It does not wake a
thread, invoke another manager, or choose the final poll timeout.

Migration is incremental. The initial default implementation adapts `poll()` plus `nextReconcile()` into this
shape. `FetchRequestManager` is the first native implementation and returns its work, schedule inputs, and state
transitions from one reactor-thread snapshot. The adapter is removed manager by manager; `reconcile()` becomes a
required implementation only after all managers have migrated.

The target vocabulary is:

```text
NextReconcile.IMMEDIATE(reason, generation)
NextReconcile.ON_EVENT(source, generation)
NextReconcile.AT_DEADLINE(source, absoluteDeadlineMs, generation)
NextReconcile.ON_CAPACITY(resource, generation)
NextReconcile.ON_CALLBACK(operationId, epoch)
```

Absolute deadlines prevent time spent between computation, publication, and reading from being added again. The
identity of a deadline is `source + mode + absolute deadline + semantic generation`. Re-observing the same blocked
operation preserves its deadline and generation. A new operation at the same clock value gets a new generation and
therefore one new deadline transition.

### 5. Form one `ReactorSchedule`

The reactor calls `ReactorSchedule.merge(...)` to reduce all `ManagerReconcileResult` values into one immutable
schedule. `merge` is a pure calculation, not another component or decision owner:

1. `IMMEDIATE` work executes fairly across managers until no immediate check remains or the iteration work budget is
   exhausted.
2. If `IMMEDIATE` remains after budget exhaustion, the reactor performs a non-blocking network poll and continues
   the work in the next iteration. `IMMEDIATE` alone never synthesizes an application wake; only an externally
   observable transition does.
3. If no work is ready, the earliest finite deadline bounds the network poll. The earliest application-visible
   deadline bounds the application wait. Both are projections of the same immutable `ReactorSchedule`.
4. Event, capacity, and callback blockers remain observable but cannot hide a finite deadline contributed by other
   work.
5. Urgency cannot make work ready when its prerequisite is unavailable.

For example, an in-flight fetch on one node does not hide reconnect backoff on another node. The per-iteration work
budget prevents a continuously ready manager from starving network polling or another manager.

During migration, an adapter may translate `RequestManager.maximumTimeToWait()` into a compatibility `NextReconcile`.
The adapter is explicitly tagged because a relative legacy delay needs conservative preservation across early
network returns. Native `NextReconcile` producers must not inherit that heuristic.

The reactor retains the latest scheduling contribution from each manager. Updating one manager therefore cannot
erase an earlier deadline from an unaffected manager. Once a compatibility deadline has been delivered, repeatedly
reporting the same zero wait does not create a new deadline or wakeup; the manager must report new progress or a new
positive wait. This is deliberately conservative: the legacy API cannot distinguish a new zero-delay operation from
the previously delivered one. Managers which require that distinction migrate to a native semantic generation.

The resulting schedule is the only scheduling decision consumed by the application wait and network poll. It records
whether another iteration is immediate, the earliest Reactor deadline, the earliest application-visible deadline,
the observed event/capacity/callback conditions, and their semantic generations. A Reactor-only deadline may shorten
network polling without synthesizing an application wakeup.

#### Multi-manager scheduling rules

`NextReconcile` values are triggers, not transferred protocol policy. When one trigger fires, the reactor rechecks the
subscribed manager, which evaluates all of its current prerequisites again. This permits a manager to report both an
event dependency and an operation deadline without teaching the reactor that manager's state machine.

The reactor's merge and execution rules are:

1. Merge checks from every manager before blocking. An event, capacity, or callback wait cannot hide another
   manager's finite deadline.
2. Execute `IMMEDIATE` managers in stable round-robin order under a per-iteration budget. Budget exhaustion forces a
   non-blocking network poll before more immediate work, so one manager cannot starve I/O, deadlines, or another
   manager.
3. Require each `IMMEDIATE` recheck to advance a state generation, request cursor, or bounded work item. Reissuing the
   same-generation `IMMEDIATE` without progress is a livelock invariant violation, not a reason to spin.
4. Represent cross-manager dependencies as named events. For example, heartbeat and commit may wait on
   `COORDINATOR_DISCOVERED`, while `CoordinatorRequestManager` independently waits on a response or retry deadline.
5. Do not invoke another manager recursively from a completion callback. A completion produces an input event or
   transition output; the reactor schedules dependent managers in its normal ordered pass.
6. Require every wait chain to terminate in an external event, absolute deadline, capacity release, callback
   acknowledgement, cancellation, or shutdown. A manager cannot wait for an action that is itself withheld until
   that manager becomes ready.
7. Tag events, schedules, callbacks, and terminal actions with operation id plus semantic generation or epoch. Stale
   completions are ignored, while each admitted operation still receives exactly one terminal action.
8. Keep user callbacks off the reactor thread and reserve control-path capacity for callback completion, fatal error,
   cancellation, and close, even when data queues are saturated.

For coordinator discovery, this produces one ordered causal chain:

```text
DISCONNECTED
  -> Reactor reconciles CoordinatorRequestManager
  -> READY becomes UNKNOWN; NextReconcile.AT_DEADLINE(retry)
retry deadline expires
  -> Reactor reconciles CoordinatorRequestManager
  -> UNKNOWN becomes DISCOVERING; FindCoordinatorRequest + NextReconcile.ON_EVENT(response)
response succeeds
  -> coordinator becomes READY(node, generation); COORDINATOR_DISCOVERED
  -> Reactor reconciles dependent heartbeat work
```

The coordinator and heartbeat managers do not call each other recursively.

These rules replace several existing local timeout substitutions:

| Existing condition | `NextReconcile` representation | Failure avoided |
| --- | --- | --- |
| FindCoordinator request is in flight while its remaining backoff reads as zero | `NextReconcile.ON_EVENT(FIND_COORDINATOR_COMPLETION)` | network-thread busy spin |
| Heartbeat is due but coordinator is unknown or the member cannot heartbeat | `NextReconcile.ON_EVENT(COORDINATOR_DISCOVERED)` or `NextReconcile.ON_EVENT(MEMBER_STATE_CHANGED)` | application/network wakeup loop |
| Auto-commit is due while a previous commit remains in flight | `NextReconcile.ON_EVENT(COMMIT_COMPLETION)` plus its absolute operation deadline | arbitrary-delay retry and application spin |
| Reconciliation waits for commit and then an application callback | `NextReconcile.ON_EVENT(COMMIT_COMPLETION)`, then `NextReconcile.ON_CALLBACK(operationId, epoch)` while heartbeat deadlines remain scheduled | callback/commit/heartbeat cyclic wait |
| Share acknowledgement waits for a busy node while fetch work also exists | `NextReconcile.ON_EVENT(NODE_REQUEST_COMPLETION)` plus acknowledgement deadlines and capacity checks | per-node spin and cross-manager starvation |

The first three rows generalize the local fixes behind KAFKA-20426 and KAFKA-20253 instead of preserving their
special timeout values. The action ordering and generation rules likewise cover the duplicate fetch wakeup in
KAFKA-20854, the metadata-error publication race in KAFKA-20397, and the skipped callback acknowledgement in
KAFKA-18160.

### 6. Execute `ReactorAction` after publishing the schedule

Today, different completion paths may complete a future, publish data, enqueue a background event, or wake the
application directly. This proposal first routes those existing application-visible actions through the reactor. A
small internal value named `ReactorAction` records the external action after the reactor has published the schedule.
Protocol-specific components report facts and completed transitions; only the reactor orders, coalesces, and
dispatches actions. In particular, protocol-specific code cannot request a synthetic application wake directly.

Representative actions are:

```text
ReactorAction.COMPLETE_OPERATION(operationId, outcome)
ReactorAction.PUBLISH_BACKGROUND_EVENT(event)
ReactorAction.PUBLISH_DATA(batch, ownershipToken)
ReactorAction.REQUIRE_CALLBACK(operationId, epoch, callback)
ReactorAction.WAKE_APPLICATION(reason, generation)
ReactorAction.TERMINATE_CONSUMER(cause)
```

`COMPLETE_OPERATION` is the only terminal operation truth source. `NextReconcile` contains only scheduling conditions and
blocking facts. `WAKE_APPLICATION` is derived by the reactor from a published state transition, deadline expiry, or
capacity release; it is never a protocol-component-selected scheduling policy.

Action identities make duplicate delivery testable. Different synthetic reasons or generations covered by the same
published snapshot collapse into one primitive signal, while the full reason/generation set remains available for
diagnostics and tests. Explicit `Consumer.wakeup()` is a user interruption and never participates in this
coalescing. Operation completion and callback acknowledgement are not freely coalesced: each operation id has
exactly one terminal result.

The current POC uses `StateTransition` as a bounded manager-to-reactor transition set. The reactor combines those
transitions with the published `ReactorSchedule` and derives `ReactorAction.WAKE_APPLICATION`; managers do not select
the external action. Other action kinds remain part of the staged migration.

Actual data insertion and explicit `Consumer.wakeup()` remain distinct primitives during migration. Adding data to
a mailbox signals the condition that truly changed; an explicit user wakeup keeps its existing interruption
semantics. A data response must not also enqueue a synthetic terminal wake for the same transition.

### 7. Fetch reconnect-backoff example

A fetch reconnect backoff follows the same flow without treating an empty request result as progress:

```text
Input Event: network disconnect
  -> ConsumerReactor calls FetchRequestManager.reconcile()
  -> FetchRequestManager records reconnectDeadline in its owned state
  -> ManagerReconcileResult includes NextReconcile.AT_DEADLINE(reconnectDeadline)
  -> ReactorSchedule uses reconnectDeadline as its earliest deadline
  -> deadline expiry becomes the next Input Event
  -> ConsumerReactor reconciles FetchRequestManager and a request is prepared
  -> NextReconcile.ON_EVENT(fetch response)
  -> fetch response becomes an Input Event and produces a batch
  -> NextReconcile.IMMEDIATE(next fetch)
  -> publish ReactorSchedule
  -> ReactorAction.PUBLISH_DATA
```

Entering backoff does not wake the application merely because no fetch request was produced. A compatibility path
which still requires the application to resubmit work may emit one `WAKE_APPLICATION` action when that specific
deadline generation expires, after the new schedule is published.

### 8. Define the reactor iteration and publication order

One iteration follows this order:

1. Drain a bounded number of application commands and callback acknowledgements.
2. Apply commands through the selected protocol-specific consumer component.
3. Call each manager's `reconcile()` and collect its `ManagerReconcileResult`.
4. Aggregate and publish the pre-I/O `ReactorSchedule` which bounds network polling.
5. Dispatch only actions whose required state and schedule have already been published.
6. Poll network I/O no longer than the earliest aggregate deadline.
7. Collect completed transport responses and mark the owning managers as affected.
8. Reconcile a stable snapshot of affected managers; merge their updated results with the cached contributions of
   unaffected managers, then derive post-I/O actions.
9. Publish the resulting `ReactorSchedule`.
10. Dispatch coalesced post-I/O actions and one-shot deadline notifications.

For synthetic wake actions, this produces one ordering rule:

> Apply transition, publish resulting state and schedule, then notify.

Until `PUBLISH_DATA` migration is complete, atomic mailbox insertion remains a state-bearing notification
exception. Today `FetchBuffer.add()` inserts data and signals its condition during the network callback, before the
post-I/O schedule is published. This is safe because the notification carries the state that satisfies the waiter;
it must not be described as a synthetic scheduling decision. A later phase moves data publication into post-I/O
action dispatch so the general action ordering also covers mailbox delivery.

Request managers and `NetworkClientDelegate` are direct, same-thread collaborators. Adding a queue between them
would add latency and another unbounded ownership boundary without isolating any mutable state.

`Input Event` is a semantic term, not a requirement that all inputs share one queue. Application commands arrive
through `ApplicationEventQueue`; network completions occur directly during `NetworkClientDelegate.poll()` on the
reactor thread; schedule deadlines are materialized by the reactor from `Time`; capacity and callback completions
become typed inputs as their migration phases land.

During migration, every iteration still performs a full pre-I/O reconciliation. This is the correctness fallback
for metadata changes, disconnects, and cross-manager dependencies which are not yet explicit affected-manager inputs.
Managers marked during post-I/O reconciliation are deferred to that next ordered pass, preventing recursive or
unbounded reconciliation in one iteration.

### 9. Keep regular and share protocol rules outside the reactor

The shared behavior is their event-processing topology, not the granularity of their manager-owned state. To make this
boundary explicit and independently testable, this proposal uses a small protocol-specific driver on each side of
the shared reactor:

| Component | Protocol policy owned |
| --- | --- |
| `RegularConsumerDriver` | assignment, positions, offset fetch/commit, classic and consumer group membership, rebalance transitions, regular fetch policy |
| `ShareConsumerDriver` | share membership, record acquisition, acquisition-lock renewal/release, acknowledgement, share fetch policy |

Share consumption remains partition-aware, and acquisition, lock, and acknowledgement state may be record- or
batch-granular. It is not modeled as a coarser form of the regular consumer.

The reactor must not branch on regular/share protocol types; construction may select one driver implementation.
`isShareConsumer` switches or policy booleans in the running reactor indicate protocol policy has leaked into the
shared event-processing layer. A driver may implement a common interface, but its internal state and transitions
remain protocol-specific.

### 10. Define the schedule and wakeup protocol

The reactor must satisfy all of these properties:

1. A published `ReactorSchedule` is immutable and safe for the application thread to read.
2. A newly shorter schedule is published before releasing an application waiter that may hold an older
   snapshot.
3. The same schedule bounds the network poll, so progress does not depend on the application polling again.
4. Expiry is delivered once for each semantic schedule generation.
5. Expiry is marked delivered before notification, preventing repeated observation of stale `0 ms` waits.
6. An event-only wait does not create a periodic wakeup unless a compatibility deadline is explicitly present.
7. Every synthetic wake records a reason and generation; a no-progress method return is not a wake reason.

This protocol replaces application-side safety rescans. A safety rescan is a second calculation performed by the
application thread before it waits, such as re-reading `SubscriptionState` or `FetchBuffer` to shorten a cached
reactor timeout. It can mask a missing reactor schedule, but it creates two decision authorities and two snapshots.
It may avoid one lost wake while introducing drift, spin, or inconsistent wait behavior elsewhere.

### 11. Make transport report outcomes, not application policy

`NetworkClientDelegate` remains responsible for request correlation, serialization, sending, receiving, transport
timeouts, and connection state. Instead of also choosing an application error or wakeup path, it returns one typed
completion record named `NetworkOutcome` to the reactor and protocol driver. Each outcome contains the
request and attempt identity, terminal kind, completion time, payload/error ownership, and the winning terminal
transition. Response, timeout, cancellation, and disconnect race through one compare-and-complete point, so exactly
one outcome owns terminal completion. The driver and reactor then decide whether to retry, complete an operation,
publish an error, or derive a synthetic application wake.

Internal completion of an `UnsentRequest` future may remain in the transport boundary because it is correlation.
Directly selecting an application-visible error queue or notification policy does not. Policy flags such as
`notifyMetadataErrorsViaErrorQueue` are removed as protocol drivers adopt the outcome boundary.

### 12. Define and enforce hard resource limits

`ConsumerReactor` owns the aggregate capacity plan. Each boundary owner atomically reserves and releases typed
permits before ownership transfer; it does not synchronously ask the reactor for every enqueue. Queue slots,
retained bytes, and pending-operation permits are separate budgets because releasing one does not necessarily
release the others.

Every asynchronous boundary must declare capacity, admission behavior, ownership transfer, and release event.

These are consumer admission limits over queues, buffers, and in-flight work; they do not pause network I/O.

| Resource | Required bound | Admission/backpressure behavior | Capacity release |
| --- | --- | --- | --- |
| Application event queue | Per-consumer command count and retained-byte budget | Producer atomically reserves queue, byte, and operation permits within the API deadline | Queue slot at dequeue; bytes when command payload is released; operation permit at terminal completion |
| Background event queue | Count and retained-byte budget, with terminal delivery reserved | Boundary atomically reserves permits; coalesce replaceable snapshots; throttle producer work; never silently drop terminal/error events | Queue slot at dequeue; retained bytes after application processing |
| Callback queue | Pending callback count | Stop admitting dependent transitions and report `NextReconcile.ON_CALLBACK`; reactor never executes user code | Epoch-tagged callback completion |
| Fetch/share data mailbox | Configured memory budget plus per-partition/acquisition accounting | Stop new fetch/acquisition work and report `NextReconcile.ON_CAPACITY` | Application consumes or discards owned batch |
| Unsent requests | Global and per-node count/bytes | Driver reports `NextReconcile.ON_CAPACITY`; do not grow an unbounded transport queue | Send, timeout, cancellation, or failure |
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
terminal permit from an internal control budget. If reservation is unavailable, it reports `NextReconcile.ON_CAPACITY` without
publishing a half-admitted callback state; any applicable protocol deadline remains visible to schedule reduction.

Exact default capacities and whether any require public configuration are unresolved in this draft. Phase 7 is not
compatibility-neutral and cannot enter a vote or implementation until the capacity defaults, accounting units,
overload exception/timeout behavior, configuration/metric surface, and upgrade impact are part of the proposal. The
invariant is not satisfied by a nominal capacity if producers can bypass it or if terminal events can deadlock
behind replaceable work.

## Compatibility, Deprecation, and Migration Plan

### Phase 1: Establish the `ConsumerReactor` decision boundary

- Establish the existing background event loop as the single coordinator of consumer resource transitions, waits,
  and notifications, and rename its implementation class from `ConsumerNetworkThread` to `ConsumerReactor` without
  changing runtime thread names or metrics.
- Publish one immutable `ReactorSchedule` for application waiting and network polling.
- Execute `ReactorAction` values only after their required state and schedule are published.

Exit evidence: aggregation, elapsed-time, publish-before-wakeup, and early-network-return tests pass with no public
API change.

### Phase 2: Introduce typed fetch blockers and reconciliation

- Represent fetch preparation as requests plus `FetchRequestPreparationBlocker` values.
- Migrate fetch retry deadlines to `NextReconcile`.
- Preserve compatibility adapters for other managers.
- Treat PR #23014 as the baseline if it merges; evolve its boolean wake decision into typed blockers and
  `NextReconcile` instead of introducing a competing result type.

Exit evidence: missing leader, reconnect backoff, in-flight, buffer drain, no-fetchable, paused partition, and mixed
condition tests prove that only real deadlines or transitions cause progress.

### Phase 3: Centralize synthetic reactor actions

- Route empty/failing/expired fetch completion through the reactor.
- Coalesce equivalent wake actions.
- Publish post-I/O schedules before dispatching post-I/O wake actions.
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
the reactor or delegate.

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

## Test Plan

Testing is organized around observable event-processing behavior rather than private method coverage.

### Deterministic component tests

- schedule aggregation across event waits, absolute deadlines, and capacity waits;
- a continuously `IMMEDIATE` manager plus another manager's finite deadline, proving that the fair work budget and
  non-blocking network poll still deliver the deadline on time;
- same-generation `IMMEDIATE` without a state, cursor, request, or bounded-work change is detected as no progress;
- coordinator discovery in flight while heartbeat is due and commit is pending, proving that the response event
  rechecks all dependents once without a zero-timeout loop;
- an expired auto-commit timer with an in-flight commit plus an independent heartbeat deadline, proving that the
  commit wait neither spins nor hides the heartbeat;
- reconciliation waiting first for commit completion and then for an application callback, proving that heartbeat
  and reconciliation deadlines continue while user code runs;
- a stale callback or response from an older operation generation cannot mutate state or complete the replacement
  operation;
- elapsed-time subtraction and preservation of native deadline generations;
- publish-before-wakeup ordering for synthetic pre-I/O and post-I/O transitions;
- one-shot expiry and no stale zero-timeout schedule;
- mixed partition/node conditions where a finite reconnect deadline survives an in-flight blocker;
- empty, failed, disconnected, timed-out, and cancelled request terminal paths;
- atomic state-bearing data publication without a second synthetic wake, followed by ordered `PUBLISH_DATA`
  coverage when that migration occurs;
- bounded action coalescing and exactly-once operation completion;
- regular/share driver isolation with no reactor protocol branches;
- queue and buffer saturation, release, timeout, close, and reserved terminal capacity.

### Cross-component tests

- real `AsyncKafkaConsumer -> reactor handle -> ConsumerReactor -> FetchRequestManager -> FetchBuffer` with only the
  socket replaced by `MockClient`;
- groupless manual assignment with a valid position and reconnect backoff;
- unsent fetch expiration through the real network delegate, the next ordered manager reconciliation, and a
  blocking application waiter;
- equivalent share-consumer acquisition/acknowledgement paths through `ShareConsumerDriver`;
- share acknowledgement and fetch work for the same busy node, proving protocol priority without reactor starvation
  or repeated empty polling;
- callback-required/completed handshake under wakeup, interruption, and close.

### Broker integration and system tests

- real-socket broker stop/restart while a groupless manual-assignment consumer waits;
- coordinator loss and reconnect while heartbeat or lock-renewal deadlines are urgent;
- regular rebalance callbacks and share acknowledgement callbacks under slow user code;
- long-running bounded-memory saturation tests for application events, background events, fetch/share data, and
  pending operations;
- existing regular consumer, share consumer, KRaft, protocol, upgrade, and compatibility suites.

### Schedule-to-assertion acceptance matrix

| Design goal | Required observable assertion |
| --- | --- |
| One resource coordinator | Application wait and network poll derive from the same published `ReactorSchedule`; no application rescan changes it. |
| No no-progress wakeups | The same generation produces at most one synthetic wake; a fixed virtual-time window has a bounded poll/wakeup count while paused, backing off, or in flight. |
| No lost shorter deadline | An application waiter using an older snapshot is released only after the shorter schedule is visible. |
| Exactly-once deadline transition | One semantic generation produces at most one expiry notification, including same-timestamp retries. |
| No manager starvation | A continuously immediate manager cannot prevent network polling, another manager's deadline, or a terminal action. |
| No cyclic wait | Commit, coordinator, heartbeat, membership callback, and share acknowledgement waits each retain an external event, deadline, capacity, callback, cancellation, or shutdown edge. |
| No reentrant transition | Completion callbacks enqueue facts; dependent manager transitions occur only in the reactor's ordered pass. |
| No stale completion | An older operation id/generation cannot mutate current state or consume the current operation's terminal action. |
| Protocol isolation | Regular and share outcomes are tested through separate drivers with no type switch in the reactor. |
| Transport/policy separation | Network delegate returns an outcome and never chooses an application event channel. |
| Hard resource limits | Load beyond each bound has documented timeout/backpressure behavior and stable retained memory. |
| Callback isolation | Slow or failing user callbacks do not execute on or block the reactor; each required callback is acknowledged once. |
| Terminal completeness | Every admitted operation completes once across success, error, timeout, cancellation, startup failure, and close. |

### Performance Validation

Correctness tests alone are insufficient for this refactor. Validation uses three separately built client artifacts:

- `ClassicKafkaConsumer`, as the established regular-consumer performance reference;
- the reactor-backed regular consumer immediately before these changes, as the causal no-regression baseline; and
- the proposed implementation.

Manual-assignment workloads compare all three variants without group-protocol behavior in the measured path. Group
workloads use the same `group.protocol=consumer` configuration for the pre-change and proposed variants; this is the
primary no-regression comparison. Classic-protocol results remain a secondary historical reference because a result
that also changes group protocol cannot isolate reactor overhead. Every comparison uses the same broker, workload,
and data, with commit ids and client-selection settings recorded in the result manifest.

The benchmark protocol first runs each baseline against itself to establish the noise floor. Formal pairwise
comparisons use at least five independent JVMs per variant and alternate order by repetition (`AB`, `BA`, `AB`,
`BA`, `AB`). Three-way workloads rotate all orderings to avoid assigning warm-broker or host drift to one client.
Raw samples, commit ids, JVM/OS, broker configuration, median, median absolute deviation, and a two-sided statistical
test are retained. A threshold breach without sufficient statistical evidence is inconclusive, not a pass.

The required workload matrix includes:

- saturated consumption at 1, 8, and 64 partitions and multiple record sizes;
- high partition counts to expose scheduling allocation and partition-scan cost;
- an empty assigned partition with a long public poll, including CPU and thread-park/network-poll diagnostics;
- idle-to-first-record p50/p95/p99 latency to detect a lost wakeup or excessive wait;
- all partitions paused, slow application callbacks, rebalance/coordinator churn, and throttled record arrival;
- real-socket broker stop/restart during a groupless/manual-assignment wait;
- bounded-resource saturation after Phase 7 capacities are specified.

The following gates apply to the pre-change versus proposed comparison. Equivalent Classic results are reported for
manual-assignment workloads and must be investigated when the proposal is slower, but they do not replace the
causal gate because Classic uses a different execution architecture and may use a different group protocol.

Initial no-regression gates are:

| Metric | Initial failure threshold |
| --- | --- |
| Saturated fetch throughput | Median records/sec decreases by more than 3% with `p < 0.05`. |
| Idle-to-first-record latency | p99 increases by more than `max(10 ms, 10%)` with `p < 0.05`. |
| Idle CPU or wake/park rate | Increases by more than 10% with `p < 0.05`. |
| Allocation per record | Increases by more than 5% without an accepted profile-backed explanation. |
| Broker-reconnect recovery | p99 increases by more than 20% with `p < 0.05`. |
| Queue or retained-buffer high-water mark | Exceeds 2x baseline under identical admitted load. |

These engineering thresholds are calibrated only from the baseline-against-baseline noise run and must be fixed
before viewing the corresponding baseline-versus-reactor result. The repository-local benchmark harness and raw
result format are developed alongside the POC; system-test coverage remains the authoritative integration layer.

Long-running and real-broker experiments run through the existing `kafka-e2e` Jenkins job. The tested fork and
exact revision are selected through `ACCOUNT` and `REVISION`, while `TC_PATHS` selects a dedicated Ducktape benchmark
path. The test publishes its manifest, raw per-run samples, environment metadata, comparison summary, and profiles as
build artifacts. A benchmark result is attributable only when the manifest records the exact commits and client
selection used for all three variants; a mutable branch name alone is insufficient.

## Documentation Plan

Phases 1 through 6 are internal implementation changes and do not require user migration documentation. The
architecture and contributor documentation will describe the reactor/driver ownership boundary, schedule and
action ordering, callback isolation, and the benchmark protocol.

Before Phase 7 enters a vote or implementation, this section will list every new or changed consumer configuration,
metric, overload exception/timeout behavior, and upgrade note. Any runtime thread-name or existing metric-name change
will be documented as a separate compatibility item rather than implied by the `ConsumerReactor` class name.

## Rejected Alternatives

### Continue adding local busy-loop conditions

Local timeout exceptions can fix one reproduction but preserve multiple decision authorities. New manager-owned states or
mixed conditions then recreate the same class of bug elsewhere.

### Keep an application-side safety rescan

The rescan can compensate for a stale reactor snapshot, but it cannot atomically observe reactor state and creates a
second scheduler. A publish-before-wakeup protocol directly fixes the stale-snapshot problem.

### Use a generic wakeup boolean

A boolean cannot distinguish data availability, in-flight completion, reconnect deadline, missing metadata, callback
completion, or capacity release. It also cannot represent mixed conditions or semantic generations.

### Put all protocol logic in one omniscient reactor class

This centralizes code rather than decisions. It produces regular/share branches, weakens protocol test boundaries,
and makes the reactor harder to evolve. Drivers keep policy local while the reactor remains the final execution
authority.

### Implement two complete reactor stacks

Duplicating queue drain, polling, publication, shutdown, and resource-limit logic invites different concurrency bugs
in regular and share consumers. Their common execution topology justifies a shared reactor layer; their protocol semantics
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
testable ownership and progress design.

### Migrate all managers and state in one patch

An all-at-once rewrite removes comparison seams and makes regressions difficult to localize. Tagged compatibility
adapters permit staged replacement with explicit exit criteria.

## Related work

- [KAFKA-14246](https://issues.apache.org/jira/browse/KAFKA-14246) and the
  [consumer threading refactor design](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217393224/Consumer+threading+refactor+design)
  established the asynchronous application/background event topology and partially implemented the network,
  request-manager, and subscription-state seams. The Jira was resolved for Kafka 3.8.0; this proposal builds on that
  topology by giving the existing event loop explicit ownership of cross-component progress and notification
  decisions.
- [KIP-945](https://cwiki.apache.org/confluence/display/KAFKA/KIP-945%3A%2BUpdate%2Bthreading%2Bmodel%2Bfor%2BConsumer)
  documents terminology and the broader threading-model intent and remains WIP. This proposal reuses that terminology
  but does not depend on KIP-945's completion.
- [KAFKA-20854 / PR #23014](https://github.com/apache/kafka/pull/23014) is open and narrows one busy-loop cause by
  distinguishing whether an empty fetch-preparation result may wake the fetch buffer. This proposal adopts its
  problem decomposition; if the PR merges, Phase 2 evolves that result into typed blockers and `NextReconcile`
  instead of introducing a competing result type.
- KAFKA-20426 / PR 22018 and KAFKA-20253 / PR 22836 demonstrate that raw wait-time minima are insufficient when
  urgency and feasibility are computed in separate components.

## Open decisions before vote

1. Whether `ApplicationEventHandler` should be mechanically renamed to `ConsumerReactorHandle` in the same proposal.
2. Exact default capacities, byte-accounting rules, overload behavior, and any required public configuration.
3. Whether callback and application-wait notification share a single internal signal or retain specialized
   primitives behind one reactor schedule.
4. Which immutable subscription/acquisition views are required for non-blocking application API reads.
5. Whether resource admission remains in this proposal after its complete public configuration and overload surface
   is specified, or moves to a separately reviewed follow-up proposal.
