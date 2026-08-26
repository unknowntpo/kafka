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

## Summary

The regular consumer and share consumer already use a background event loop, request managers, network I/O, event
queues, futures, and data buffers. However, cross-manager scheduling and application-visible notification decisions
are still made on separate paths that may observe different state snapshots. This has contributed to busy loops,
lost or duplicate wakeups, stale completion handling, and state-publication races.

This proposal establishes `ConsumerReactor` as the final owner of input ordering, cross-manager scheduling, and
application-visible actions. Request managers retain their local state and rules and return requests, completed state
transitions, and `timeUntilNextPollMs` from one `poll()` snapshot. The reactor combines those results into one
published `ReactorSchedule` and executes `ReactorAction` values only after the corresponding state and schedule are
visible.

The proposal does not add a thread, replace the existing event-driven topology, change Kafka protocols or public
consumer APIs, or move user callbacks off the application thread. The same thin reactor kernel is used by the regular
and share consumers, while their rules remain in separate components. `ClassicKafkaConsumer` is not retrofitted into
this model.

## Motivation

A local deadline can be urgent even when the Consumer as a whole cannot make progress. Today, no single component
combines those facts before selecting the next wait or application-visible action.

### A motivating failure: urgent work without progress

[KAFKA-20253 / PR 22836](https://github.com/apache/kafka/pull/22836) fixed a high-CPU loop after failed
re-authentication. Once the coordinator became unavailable, a heartbeat could be due but could not be sent. At the
same time, coordinator discovery or auto-commit could already be in flight. Two zero-delay paths were enough to spin
the application and background threads, and the same missing feasibility check also existed in auto-commit:

| Component | Local observation | Local result | Global reality |
| --- | --- | --- | --- |
| Heartbeat request manager | The heartbeat deadline had expired and no heartbeat was in flight. | Application wait `0`. | No coordinator existed, so no heartbeat request could be created. |
| Coordinator request manager | The retry backoff had elapsed. | Network poll delay `0`. | A `FindCoordinator` request was already in flight, so another request could not be created. |
| Auto-commit state | The commit interval had elapsed. | Application wait `0`. | A previous commit was still in flight, so another commit could not start. |

The application thread repeatedly observed a zero wait, while the background thread repeatedly called
`NetworkClient.poll(0)`. Both threads ran, but neither could change the state required for progress.

PR 22836 correctly added a feasibility guard to each path. This fixed the bug, but it also demonstrates the
maintenance problem: one progress rule had to be recognized and implemented independently in heartbeat scheduling,
coordinator discovery, and auto-commit scheduling. Diagnosing the failure required correlating their timers,
in-flight state, the cached application wait, and the network poll timeout.

### Proposed decision boundary

This proposal makes the shared rule explicit. Request managers retain their local state and protocol rules, but
return typed results describing proposed requests, completed state transitions, and `timeUntilNextPollMs`.
`ConsumerReactor` orders those results, publishes one `ReactorSchedule`, and then executes the required
`ReactorAction`s.

For the KAFKA-20253 state, the combined result is:

```text
Heartbeat: deadline expired; coordinator required; no request created
Coordinator: FindCoordinator request in flight
Auto-commit: deadline expired; commit request in flight
                              ↓
                       ConsumerReactor
                              ↓
No immediately executable request or completed application-visible transition
                              ↓
Wait for network completion or another valid deadline; do not wake the application
```

The reactor does not replace manager-local rules or invent their deadlines. It is the single owner that combines
their outputs into the next cross-manager schedule and application-visible action.

### Bug evidence and expected benefit

KAFKA-20253 is one instance of a recurring decision-ownership problem. The following rows state the expected Reactor
benefit and the corresponding test obligation; they do not claim that implementation coverage is already complete.

| Evidence | Before Reactor | With ConsumerReactor | Expected benefit and test obligation |
| --- | --- | --- | --- |
| [KAFKA-17066 / PR 16885](https://github.com/apache/kafka/pull/16885), [KAFKA-17674 / PR 17342](https://github.com/apache/kafka/pull/17342) | An older position-initialization completion could affect a partition added while its request was in flight. | Reactor orders assignment input and completion; the operation may mutate only its captured partition scope. | A stale completion cannot reset a new partition; verify the complete application-event, network-completion, and next-position-update path. |
| [KAFKA-18641 / PR 18737](https://github.com/apache/kafka/pull/18737), [KAFKA-15529 / PR 21476](https://github.com/apache/kafka/pull/21476) | Position and consumed-state publication could race with commit or application observation. | Reactor applies and publishes the complete transition before dependent work or notification. | Commit and fetch observation use one ordered snapshot; verify publish-before-dependent-action ordering. |
| [KAFKA-20426 / PR 22018](https://github.com/apache/kafka/pull/22018), [KAFKA-20253 / PR 22836](https://github.com/apache/kafka/pull/22836) | Heartbeat urgency produced a zero wait while coordinator, assignment, or in-flight state made progress impossible. | Reactor combines manager deadlines with whether any request or transition can execute. | No zero-delay loop without progress; reproduce the original multi-manager ordering through the Consumer path. |
| [KAFKA-20854 / PR 23014](https://github.com/apache/kafka/pull/23014) | An ambiguous empty fetch result caused application/background wakeup ping-pong. | Only a named application-visible transition can produce `WAKE_APPLICATION`. | A no-progress fetch result does not wake the application; verify the real fetch-buffer wait path. |
| [KAFKA-20397 / PR 21991](https://github.com/apache/kafka/pull/21991) | Metadata-error publication raced with an application thread entering its fetch-buffer wait. | Reactor publishes state and schedule before issuing a retained notification. | No lost wakeup or stale observation; reproduce the publication/wait ordering. |
| [KAFKA-18160 / PR 18089](https://github.com/apache/kafka/pull/18089) | Wakeup or interruption could skip callback acknowledgement. | Reactor orders each admitted operation to exactly one terminal action. | No missing or duplicate acknowledgement; verify callback, interruption, cancellation, and close outcomes. |

### Ownership and progress invariants

This proposal relies on three invariants:

1. Each mutable state has one execution-context owner. Request managers own their local state; `ConsumerReactor`
   owns the final cross-manager schedule and actions.
2. Every synthetic wakeup or reschedule names a real state transition, deadline, completion, command, or capacity
   change. A no-progress method return is not a wake reason.
3. State and the resulting `ReactorSchedule` are published before completing futures, publishing data or events, or
   waking the application.

### Goals

- Centralize input ordering, cross-manager scheduling, and synthetic notification decisions in `ConsumerReactor`.
- Keep manager state and regular/share rules local to their existing domains.
- Derive application waits and network poll bounds from one published schedule.
- Preserve callback isolation and existing public behavior.
- Migrate incrementally with deterministic correctness and performance evidence.

### Non-goals

- Changing the number or placement of application and background threads.
- Combining regular consumer and share consumer state machines.
- Moving user callbacks onto the reactor thread.
- Retrofitting `ClassicKafkaConsumer` into the reactor.
- Rewriting every request manager in one patch.
- Defining new public queue-capacity or overload configuration. Resource admission can be proposed separately after
  the decision boundary is established.

### Current and proposed execution flow

Today, `AsyncKafkaConsumer` and `ShareConsumerImpl` each create a background execution context. They reuse the same
event-loop and network infrastructure, but state, wait, completion, publication, and wakeup decisions can still be
made by different components along the path:

![Current and proposed Consumer execution flow](../images/kip-1371-consumer-reactor-before-after.png)

The proposal keeps the event-driven topology and the existing application/background thread boundary. It changes
which component owns the following final cross-component decisions:

- the order in which input events and request managers are processed, including which managers are polled again
  after network completion;
- the earliest retained manager deadline, the resulting network poll timeout, and the application wait projection;
- the ordering of completed state transitions and the `ReactorAction`s they require; and
- publication of `ReactorSchedule` before completing futures, publishing data or errors, or issuing one retained
  application wakeup.

Request managers continue to own their local state and rules. `ConsumerReactor` becomes the only component that
orders inputs, combines manager results into the final cross-manager schedule, and orders application-visible
actions after publication. `NetworkClientDelegate` retains transport and correlation responsibilities, and user
callbacks remain on the application thread. The change is decision ownership, not a new thread model.

## Public Interfaces

The initial migration changes no Kafka protocol, public `Consumer` or `ShareConsumer` API, callback execution
guarantee, runtime thread name, or existing metric name. `Consumer.wakeup()` retains its current user-visible
semantics.

`ConsumerReactor`, `ReactorSchedule`, `ReactorAction`, and the strengthened manager `PollResult` are internal
coordination types. Any future public capacity configuration or overload behavior requires a separately complete
compatibility proposal.

## Proposed Changes

### 1. Define the responsibility boundary

The proposal changes responsibility, not topology:

```text
Before
  Application thread
    -> ApplicationEventHandler
       -> ApplicationEventQueue
          -> ConsumerNetworkThread
             -> RequestManagers
             -> NetworkClientDelegate

  wait, completion, publication, and wake decisions may also occur
  in the application thread, managers, and response paths

After
  Application thread
    -> ConsumerReactorGateway
       -> ApplicationEventQueue
          -> ConsumerReactor                 final cross-resource decision owner
             -> protocol-specific driver
             -> RequestManagers              local state and rules
             -> NetworkClientDelegate        transport and correlation

  ConsumerReactor
    -> publishes state and ReactorSchedule
    -> then executes ReactorAction values
```

`ConsumerReactorGateway` describes the application-side submit, submit-and-wait, wake, schedule-read, and close
boundary. It replaces the less precise `ApplicationEventHandler` name without changing runtime thread names or
behavior.

| Component | Responsibility after the KIP |
| --- | --- |
| `ConsumerReactor` | Order inputs, invoke managers, retain their scheduling contributions, publish the final schedule, and dispatch actions. |
| Request manager | Own one domain of consumer state and rules; return requests, completed transitions, and `timeUntilNextPollMs` from one snapshot. |
| Regular/share driver | Own consumer-specific assignment or acquisition, commit or acknowledgement, and callback policy. |
| `NetworkClientDelegate` | Own transport, connection handling, request correlation, and timeouts; do not select application-visible policy. |
| Application thread | Execute user callbacks and consume published data, events, and operation results. |

The reactor coordinates these owners; it does not absorb their local rules or transport mechanics.

### 2. Define the processing model

The controller-style flow is:

```text
Input Event -> ConsumerReactor -> RequestManager.poll()
            -> requests + state transitions + timeUntilNextPollMs
            -> ReactorSchedule -> ReactorAction
```

An input event is an existing application command, network completion, deadline expiry, capacity release, or callback
acknowledgement. It is a semantic term, not a requirement to add another queue: application commands use the existing
`ApplicationEventQueue`, while network completions occur directly during `NetworkClientDelegate.poll()` on the
reactor thread.

Input events are not merged into one state mutation. The reactor applies them in a deterministic order, then polls
the affected managers and aggregates their scheduling results. Only semantically equivalent external actions, such
as multiple synthetic wake reasons in one iteration, are coalesced. Requests, callbacks, and terminal operation
results retain their identities and ordering.

For example, if an assignment adds `tp2` before an older `OffsetFetch(scope={tp1})` completes, the assignment event is
applied first and the older completion can update only `tp1`. A later ordered position operation initializes `tp2`.

One reactor iteration is:

1. Drain ready application commands and callback acknowledgements.
2. Apply them through the selected regular or share component.
3. Poll request managers and collect their `PollResult` values.
4. Retain per-manager deadlines, form one `ReactorSchedule`, and publish it.
5. Execute pre-I/O actions derived from completed transitions.
6. Poll network I/O no longer than the published deadline.
7. Poll managers affected by network completions, publish the updated schedule, and execute post-I/O actions.

Managers do not call one another recursively. A completion marks its owning manager, while cross-manager
dependencies are observed in the next ordered pass.

### 3. Preserve operation identity and shared dependencies

Input events, logical operations, and network attempts are not interchangeable. The implementation retains the
smallest identity required at each layer:

| Identity | Purpose |
| --- | --- |
| Input sequence | Orders events accepted by the reactor and supports causal tracing. |
| Operation ID | Tracks an admitted application-visible operation until exactly one terminal result. |
| Request attempt identity | Distinguishes network attempts and retries within an operation; transport correlation remains in `NetworkClientDelegate`. |
| State generation and scope | Prevents an older completion from mutating newer manager state or partitions outside the operation's captured scope. |

These identities do not imply a one-to-one relationship. One application event can produce multiple operations, and
shared infrastructure work can serve multiple events and operations. Coordinator discovery is an example:

```text
Input 101: authentication failure
  -> coordinator UNKNOWN, generation=7
  -> FindCoordinator(discovery=7, attempt=1, origin=101)

Input 102: commit operation=55
  -> waits for coordinator generation 7

Input 103: heartbeat deadline
  -> also waits for coordinator generation 7

Network completion: discovery=7, attempt=1
  -> apply only if coordinator generation is still 7
  -> coordinator READY
  -> Reactor polls the affected heartbeat and commit managers
```

The `FindCoordinator` request has one originating input for tracing, but it is not owned by that event. Later
heartbeat, commit, join, or acknowledgement work may depend on the same discovery. Dependent operations remain in
their owning managers rather than being copied into an unbounded central dependency graph. A shared request retains
only compact diagnostic identity and manager-owned generation; bounded manager queues retain application operation
IDs.

If a completion belongs to an older generation, the manager treats it as stale and does not publish a transition for
the current state. If it is current, the reactor orders the resulting transitions, publishes the new schedule, and
only then completes dependent operations or executes application-visible actions. These identities are internal and
do not change Kafka protocol correlation IDs or public Consumer APIs.

### 4. Publish one schedule before executing actions

For every manager result, the reactor converts `timeUntilNextPollMs` into an absolute deadline and retains that
manager's contribution. `ReactorSchedule` selects the earliest retained deadline. Updating one manager cannot erase
an earlier deadline from another manager, and an unrelated early network return cannot postpone existing work.

This absolute value is the reactor deadline, not a promise of network I/O. It means the reactor must poll the manager
again by that time; reconnect backoff, metadata, capacity, callback, or local-state progress may be the reason. The
reactor derives `networkPollTimeoutMs(now)` from the remaining reactor deadline only when it calls the existing
network client.

The published schedule satisfies these properties:

- the same snapshot bounds network polling and application waiting;
- a newly shorter deadline is visible before releasing a waiter using an older snapshot;
- an expired deadline is consumed by polling its manager again, not by repeatedly waking the application;
- an event-only wait does not create periodic wakeups.

`ReactorAction` represents an application-visible effect selected after state and schedule publication. The current
implementation covers application wakeup and async-poll progress or completion. Later migration may generalize these
into operation completion, application publication, and application notification. Network requests remain manager
outputs, retry remains a schedule deadline, and state transitions remain internal; they are not `ReactorAction`
values.

Equivalent wake actions are coalesced, but terminal operation completions are not. Wakeup executes last so the
application thread observes the state, schedule, data, event, or completion that caused it.

The ordering invariant is:

> Apply transitions, publish the resulting state and `ReactorSchedule`, then execute `ReactorAction` values.

### 5. Share the reactor kernel, not consumer rules

The regular consumer and share consumer use separate reactor instances but the same execution kernel because they
share the same concurrency topology. Their rules remain separate:

| Component | State and policy retained |
| --- | --- |
| `RegularConsumerDriver` | assignment, positions, offset fetch/commit, membership, rebalance transitions, and regular fetch policy |
| `ShareConsumerDriver` | share membership, acquisition, lock renewal/release, acknowledgement, and share fetch policy |

The shared reactor must not branch on consumer type. An `isShareConsumer` switch, optional union, or policy boolean in
the kernel indicates that consumer-specific rules have leaked across the boundary.

### 6. Example: reconnect backoff with another manager deadline

Assume the fetch manager is blocked by a reconnect backoff that expires in 100 ms while the heartbeat manager must be
polled in 50 ms:

```text
FetchRequestManager.poll()
  -> requests=[]
  -> transitions=[]
  -> timeUntilNextPollMs=100

HeartbeatRequestManager.poll()
  -> timeUntilNextPollMs=50

ConsumerReactor
  -> publishes ReactorSchedule(deadline=heartbeat at 50 ms)
  -> no application wakeup
```

At 50 ms, the reactor polls heartbeat work. The retained fetch deadline remains at 100 ms even if heartbeat network
I/O returns early. At 100 ms, the reactor polls the fetch manager again and creates the fetch request from its retained
intent. Backoff and in-flight states do not wake the application because they do not represent application-visible
progress.

When data becomes available, the reactor publishes the corresponding state and schedule before the data/completion
action and any coalesced application wakeup. This single flow covers the two central requirements: cross-manager
deadlines cannot hide one another, and only real progress notifies the application.

## Compatibility, Deprecation, and Migration Plan

### Phase 1: Establish the decision boundary

- Establish the existing background loop as `ConsumerReactor` without changing thread topology.
- Publish one `ReactorSchedule` and execute actions only after publication.
- Migrate fetch reconnect, in-flight, paused, missing-leader, and buffered-data decisions to the strengthened
  `PollResult`.

Exit evidence: reconnect-backoff, schedule aggregation, publish-before-wakeup, and busy-loop tests pass with no public
API change.

### Phase 2: Migrate manager and consumer-specific decisions

- Move remaining manager deadlines and completed transitions into `PollResult`.
- Establish the regular/share driver boundary and remove consumer-type policy from the shared kernel.
- Replace application-side mutable-state rescans with immutable schedule or operation results after equivalent tests
  exist.

Exit evidence: multi-manager, stale-completion, regular-consumer, and share-consumer suites pass independently.

### Phase 3: Remove remaining direct application effects

- Route remaining operation completions, data/event publication, callback requirements, and synthetic wakeups through
  the publish-before-action boundary.
- Remove application-visible policy from `NetworkClientDelegate` while retaining transport correlation locally.
- Remove compatibility adapters only after every success, error, timeout, cancellation, interruption, and close path
  has a terminal result.

Exit evidence: no manager or transport callback directly selects an application wake/error path, and every admitted
operation completes once.

Each phase leaves the repository runnable and can be reverted independently.

## Test Plan

Tests assert observable event-processing behavior rather than private method coverage. Required coverage includes:

- reconnect backoff creates no request or wake before its deadline and creates the request when the deadline expires;
- an earlier heartbeat or commit deadline cannot erase or postpone a retained fetch deadline;
- schedule publication occurs before pre-I/O and post-I/O actions;
- repeated no-progress manager results do not produce a zero-timeout loop or wakeup ping-pong;
- an older offset-fetch completion cannot mutate a partition outside its captured scope;
- regular and share paths use the same kernel without consumer-type branches;
- callback, interruption, cancellation, and close produce exactly one terminal outcome;
- broker stop/restart and coordinator loss make progress without application polling as a scheduler.

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

## Documentation Plan

Contributor documentation will describe the reactor/manager/driver ownership boundary, schedule and action ordering,
and the evidence required before each compatibility path is removed. Public migration documentation is unnecessary
unless a later proposal adds capacity configuration, overload behavior, or observable name changes.

## Rejected Alternatives

### Keep distributed decisions with local fixes

Local timeout conditions, application-side rescans, and generic wakeup booleans can address individual failures but
retain multiple decision authorities. They cannot consistently preserve reason and publication ordering.

### Put all consumer logic in one reactor class

This centralizes code rather than decisions and would mix regular/share policy into the execution kernel. Managers
and drivers keep rules local while the reactor remains the final coordination owner.

### Implement separate regular and share reactor stacks

Duplicating queue drain, scheduling, publication, shutdown, and action ordering invites different concurrency bugs.
The consumers share the kernel but not their state machines.

### Migrate all managers in one patch

An all-at-once rewrite removes comparison seams and makes regressions difficult to localize. The phased approach
keeps each slice runnable and independently testable.

## Related Work

- [KAFKA-14246](https://issues.apache.org/jira/browse/KAFKA-14246) and the
  [consumer threading refactor design](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217393224/Consumer+threading+refactor+design)
  established the current application/background event topology. This proposal completes its cross-component
  decision boundary.
- [KIP-945](https://cwiki.apache.org/confluence/display/KAFKA/KIP-945%3A%2BUpdate%2Bthreading%2Bmodel%2Bfor%2BConsumer)
  documents the broader threading-model intent and remains related history, not a prerequisite.
- [KAFKA-20854 / PR #23014](https://github.com/apache/kafka/pull/23014) narrows one busy-loop cause. This proposal uses
  the same problem decomposition and generalizes the scheduling and action boundary across managers.
