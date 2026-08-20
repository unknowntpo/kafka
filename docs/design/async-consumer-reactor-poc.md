# Async Consumer Reactor POC

## Status

This is an exploratory design and migration plan. It does not change the Kafka consumer public API or protocol.

The immediate goal is to test whether the current `AsyncKafkaConsumer` implementation can evolve toward one
owner for protocol state and one place that decides whether the consumer can make progress.

## Problem Statement

The current implementation has a background event loop, but it does not yet have a single progress model.
Several components independently decide when work should run or when another thread should wake:

* `AsyncKafkaConsumer` reads `SubscriptionState`, collects records, and waits on `FetchBuffer`.
* `ConsumerReactor` processes application events, polls request managers, and polls the network client.
* Each `RequestManager` independently returns a network poll delay and an application-thread wait delay.
* Legacy request-manager and response paths may wake `FetchBuffer` directly.
* Futures, background events, callback-completed events, and user wakeups use separate notification paths.

These mechanisms are individually reasonable, but their decisions are not based on one state snapshot. A local
condition can therefore say "run now" even when another component knows that no useful transition is currently
possible.

## Before

| Execution context | State read or mutated | Progress mechanism |
| --- | --- | --- |
| Application thread | assignment, positions, buffered fetches, in-flight poll state | event enqueue, future wait, `FetchBuffer.awaitWakeup` |
| Consumer reactor | application events, request-manager state, network responses | network poll and cached `maximumTimeToWait` |
| Request managers | membership, coordinator, commit, offset and fetch sub-state | `PollResult`, `maximumTimeToWait`, direct buffer wakeup |

The effective flow is:

```text
poll()
  -> inspect application-thread state
  -> enqueue one or more events
  -> read a cached background-thread timeout
  -> wait on FetchBuffer

ConsumerReactor.runOnce()
  -> drain events
  -> ask every manager for requests and a network timeout
  -> perform network I/O
  -> ask every manager for an application timeout
  -> publish the minimum timeout

Any manager / response callback
  -> complete a future, enqueue a background event, or wake FetchBuffer
```

There is no object that explains the complete decision: what transition is ready, what prerequisite is missing,
and which event or deadline can change that answer.

## After

The target is a `ConsumerReactor`: the single execution-context owner of mutable consumer protocol state and the
only component that combines cross-manager constraints into final progress decisions.

The implementation class is now named `ConsumerReactor`. The name describes its responsibility rather than its
executor: it orders events, invokes protocol state machines, aggregates their constraints, decides network polling
and application waiting, and publishes the resulting effects. It still uses a dedicated thread internally, but the
thread is an implementation detail. The rename marks the intended boundary; it does not imply that every legacy
decision source has already migrated.

| Component | Responsibility after migration |
| --- | --- |
| Application facade | Validate API calls, submit bounded commands, return records, execute user callbacks |
| Consumer reactor | Own protocol state, apply events in order, choose the next progress action |
| Request-manager state machines | Reduce reactor-owned state and propose effects; do not independently schedule threads |
| Network delegate | Transport only: send requests and publish completions |
| Fetch mailbox | Bounded transfer of immutable/owned fetch results; not a generic wakeup condition |
| Callback mailbox | Publish callback work and require an epoch-tagged completion acknowledgement |

### Ownership and communication topology

`AsyncKafkaConsumer` indirectly owns the reactor through `ApplicationEventHandler`. Despite its current name,
`ApplicationEventHandler` is not the Reactor-pattern handler: it is the application-side cross-thread gateway and
lifecycle handle (`add`, `addAndGet`, `wakeup`, wait-snapshot access, and close). A later mechanical rename to
`ConsumerReactorHandle` would make this role clearer. `ApplicationEventProcessor` executes application-command
logic; `ConsumerReactor` drains and dispatches the commands.

```text
AsyncKafkaConsumer
  -> ConsumerReactorHandle (currently ApplicationEventHandler)
     -> ApplicationEventQueue -- enqueue, then wake --> ConsumerReactor

ConsumerReactor
  -> RequestManagers -> NetworkClientDelegate       (same-thread direct calls)
  -> BackgroundEventQueue / FetchBuffer / futures   (cross-thread publication)
```

There should be no queue between the reactor, request managers, and network delegate: they share one owner thread,
so an extra hop would weaken ordering and add capacity management without isolating mutable state. Cross-thread
paths require explicit admission bounds. The current application and background queues are unbounded
`LinkedBlockingQueue` instances, so resource ownership is not complete yet.

Regular and share consumers should reuse a thin reactor kernel, not one combined protocol policy. The kernel owns
queue drain, ordering, deadline aggregation, network polling, publish-before-wakeup, shutdown, and resource limits.
A `RegularConsumerDriver` can own membership/fetch/offset policy; a `ShareConsumerDriver` can own acquisition,
lock-renewal, and acknowledgement policy. Branches such as `RequestManagers.wakeupApplicationThread()` and boolean
transport policy flags are evidence that this boundary has not yet been extracted. New `isShareConsumer` branches,
union-shaped optionals, or policy booleans in the kernel should be treated as boundary leaks.

`NetworkClientDelegate` currently owns a `BackgroundEventHandler` and
`notifyMetadataErrorsViaErrorQueue`: regular consumers ask the reactor to pull a metadata error, while share
consumers let the delegate enqueue an `ErrorEvent`. Thread ownership is intact because polling and callbacks run on
the reactor thread, but decision ownership leaks into transport. The target is for transport to return a typed
`NetworkOutcome`; the reactor/driver then decides whether to retry, complete, publish, or wake. Internal
`UnsentRequest` future completion may remain transport-local because it performs correlation rather than
application-visible policy.

Every reactor iteration should produce one explicit decision:

```text
SEND(requests)
DELIVER(fetchBatch)
AWAIT_NETWORK(deadline)
AWAIT_APPLICATION(commandType)
AWAIT_CALLBACK(epoch)
AWAIT_CAPACITY(resource)
AWAIT_DEADLINE(deadline)
TERMINAL(result)
```

The decision is computed from state owned by the reactor. A wakeup is an effect of a concrete transition, not a
substitute for describing why progress became possible.

## Target Invariants

1. Each mutable protocol state has one execution-context owner.
2. A transition is applied before its result, data, or callback is published.
3. Every wakeup, retry, and reschedule names the state transition, capacity change, completion, command, or deadline
   that can enable progress.
4. Every cross-thread operation has one terminal success, failure, cancellation, or timeout acknowledgement.
5. Every queue, in-flight collection, and data mailbox has an explicit capacity bound.
6. User callbacks remain isolated on the application thread.

## Current Decision Coverage

The architectural boundary is: request managers own protocol-specific transitions and report constraints; the
reactor alone combines those constraints into cross-component scheduling and notification decisions. Centralizing
the final decision must not turn the reactor into a switch statement containing every protocol rule.

| Decision | Current authority | Coverage | Remaining gap |
| --- | --- | --- | --- |
| Earliest application deadline across managers | Reactor | absolute deadlines, elapsed-time subtraction, same-source preservation | most managers still use the compatibility timeout adapter |
| Network poll duration | Reactor | request delays and progress deadlines cap every poll | inputs are not yet typed by reason |
| Publish-before-wakeup ordering | Reactor | shorter decisions, deadline expiry, and named fetch progress effects are coalesced after publication | dedicated application-wait primitive remains a possible follow-up |
| Deadline delivery | Reactor | one-shot delivery, no stale `0 ms`, distinct source and native fetch generation at the same timestamp | compatibility managers still lack explicit operation generations |
| Fetch blocked by missing leader | Fetch manager reports typed condition; reactor schedules | bounded retry deadline | metadata completion is not yet a named reactor effect |
| Fetch blocked by reconnect backoff | Fetch manager reports actual connection delay; reactor schedules | exponential connection backoff and mixed partitions | real-socket broker-restart smoke test remains |
| Fetch blocked by in-flight request or buffer drain | Fetch manager reports event-driven condition; reactor schedules | terminal request completion is a bounded, named effect | buffer-capacity transitions still use the data mailbox signal |
| Empty or failed fetch response | Fetch manager reports a named terminal/preparation effect; reactor coalesces and applies it | empty, error, preparation-failure, duplicate-coalescing, and unsent-expiration tests | real-socket failure smoke remains |
| Groupless manual assignment | Reactor deadline limits the full async chain | deterministic cross-component test without caller rescan | real-socket broker-restart smoke test remains |
| No assignment or invalid positions | Fetch manager reports `NO_FETCHABLE_PARTITIONS`; reactor schedules | behavior-equivalent retry deadline | replace the periodic compatibility deadline with explicit assignment/position events |
| Fetchable/unbuffered partitions | Fetch manager reports the concrete preparation condition; reactor schedules | missing leader, reconnect, in-flight, and buffer-drain paths no longer need caller inference | completion/capacity effects are not yet first-class types |
| Rebalance callbacks and lifecycle handshakes | Split between reactor and application thread | existing Required/Completed event protocol | operation/epoch ownership and terminal-path unification remain |
| Queue and buffer admission | Individual queues and producers | existing local behavior | no unified capacity decision or overload policy |

## Incremental Migration

### Phase 1: Make the progress decision explicit

Replace the anonymous cached timeout with an immutable decision snapshot. Record when it was computed and which
manager supplied the limiting deadline. Publish the pre-I/O decision which bounds the network poll, deliver its
expiry, and then recompute from a post-I/O time snapshot. This ordering matters for compatibility adapters which
still express a relative delay: recomputing first would move an expired deadline into the future.

This phase is implemented by this POC. It intentionally preserves behavior while creating an observable seam for
later phases.

Exit criteria:

* one tested aggregation function chooses the application wait deadline;
* the published decision is immutable and safe to read from the application thread;
* the decision which bounded I/O is delivered before a fresh post-I/O decision replaces it;
* no public API changes.

### Phase 2: Replace manager-local delays with progress intents

Replace `RequestManager.maximumTimeToWait()` with typed intents such as `READY_TO_SEND`, `WAITING_FOR_COORDINATOR`,
`WAITING_FOR_RESPONSE`, and `WAITING_FOR_DEADLINE`. The reactor combines feasibility and urgency instead of taking
the minimum of unrelated numbers.

The focused regression scenario is a manager that reports urgent work while its prerequisite is unavailable. The
reactor must park on the prerequisite or its deadline rather than return a zero-duration wait.

The third POC slice implements the first typed intent boundary:

```text
AWAIT_EVENT
AWAIT_DEADLINE(absoluteDeadlineMs)
```

Managers that have not migrated use a compatibility adapter around `maximumTimeToWait()`. `FetchRequestManager`
produces its intent directly from the latest typed fetch-preparation result instead of reducing every non-in-flight
state to the same anonymous backoff. Absolute deadlines prevent time spent between publication and reading from
being added to the wait again. Re-observing the same blocking condition preserves its existing deadline; otherwise
unrelated events could continuously move the retry forward and create starvation. The compatibility adapter is
explicitly tagged so the reactor conservatively preserves its earlier deadline across early network returns without
applying that heuristic to native typed intents.

The fourth POC slice removes the application-side `SubscriptionState` / `FetchBuffer` safety rescan. Fetchable
partitions now rely on their concrete preparation condition: missing leader and reconnect use reactor deadlines,
while in-flight requests and buffer drain wait for real completion or capacity transitions. Until assignment and
position changes become explicit reactor events, `NO_FETCHABLE_PARTITIONS` retains the old retry bound as a named
manager constraint. The application thread only applies the immutable timeout published by the reactor.

The publication protocol is part of the type boundary, not an implementation detail:

1. publish the immutable wait snapshot;
2. wake the application-side fetch wait if the new deadline is earlier;
3. wake once when each semantic decision's absolute deadline expires, even if it is later than the previous
   deadline;
4. atomically mark that expiry as delivered, so the application cannot repeatedly observe a stale `0 ms` wait.

The third rule is required because an application thread may already be blocked using the previous snapshot and
cannot read a newly shortened `maximumTimeToWait()` until it is released. The typed deadline also limits the
background network poll, so the reactor reaches the deadline without depending on application-thread polling.

### Phase 3: Remove generic fetch wakeups

Make fetch preparation return a discriminated result:

* requests created;
* data already buffered;
* blocked by an in-flight request;
* blocked by reconnect backoff;
* missing metadata or position;
* no fetchable partitions.

Only `data already buffered` publishes `DATA_AVAILABLE`. Backoff publishes a deadline; request completion publishes
a completion event. `FetchBuffer.wakeup()` is no longer used for an ambiguous empty result.

The second POC slice implements this boundary as a request map plus a set of typed conditions, because different
partitions can be blocked for different reasons in the same preparation pass:

```text
DATA_ALREADY_BUFFERED
NO_FETCHABLE_PARTITIONS
MISSING_LEADER
RECONNECT_BACKOFF
REQUEST_IN_FLIGHT
WAITING_FOR_BUFFER_DRAIN
CLOSING
```

The fifth POC slice makes synthetic application notification a reactor-owned effect. `FetchRequestManager` reports a
bounded, duplicate-coalescing enum set containing `FETCH_BUFFER_HAS_DATA`, `FETCH_PREPARATION_FAILED`, or
`FETCH_REQUEST_TERMINATED`. The reactor drains these facts before and after network I/O, coalesces them with deadline
and publication reasons, and applies at most one application wake per phase. The manager does not directly signal
the buffer for these outcomes. A response that already added a `CompletedFetch` does not also report a terminal
effect, preventing a second retained wake after the data signal. Fetch-session close is intentionally not an
application progress effect: close runs inside reactor cleanup while the application waits for reactor termination,
not fetch data.

Two direct signals remain intentionally separate. Adding records to `FetchBuffer` signals the mailbox condition
that actually changed; `Consumer.wakeup()` is an explicit user interruption. Neither is a synthetic scheduling
policy. Reconnect contributes the network client's actual remaining connection delay, including exponential
backoff, instead of substituting the configured base retry interval or an immediate wakeup. Missing metadata
contributes the configured retry deadline.

Native fetch deadlines carry a manager-owned semantic generation. Deadline delivery identity is therefore
`source + mode + absolute deadline + generation`, rather than only a timestamp. Two distinct zero-delay retries from
the same manager in the same clock tick each receive one notification, while re-observing the same blocked state
preserves both its absolute deadline and generation.

When conditions are mixed across partitions, a retry deadline wins over an event-only blocker. For example,
`REQUEST_IN_FLIGHT` on one node does not hide `RECONNECT_BACKOFF` on another node.

### Phase 4: Move subscription transitions behind reactor commands

Move assignment, position, seek, pause/resume, commit snapshots, and reconciliation transitions behind typed reactor
commands. `SubscriptionState` becomes reactor-private. The application thread receives immutable views needed by
the public API.

### Phase 5: Unify callback and lifecycle handshakes

Use operation and epoch identifiers for rebalance callbacks, startup, close, and pending commits. Every terminal
path completes the same acknowledgement exactly once.

### Phase 6: Enforce resource bounds

Define capacities and overload behavior for application commands, background notifications, fetch data, pending
callbacks, in-flight requests, and retained response buffers.

## POC Scope and Non-goals

The POC now names the execution component `ConsumerReactor`, but does not yet move `SubscriptionState`, change
callback threading, rename `ApplicationEventHandler` or existing runtime thread/metric identifiers, extract
regular/share protocol drivers, or claim to fix every busy loop. It has established a progress-decision publication
seam, a typed fetch-preparation result, the first direct typed-intent producer, and a bounded/coalesced application
progress-effect boundary.

The POC includes a deterministic `FetchBuffer` concurrency test plus a cross-component test of the complete async
consumer chain with only its socket replaced by `MockClient`. In the latter, a consumer without a group id and with
a valid position proves that the reactor caps network polling to the 100 ms reconnect deadline and eventually
generates a new fetch. Because the application-side rescan is now absent, that test directly exercises reactor
publication and expiry wakeups. `testPollWaitUsesOnlyPublishedReactorDecision` separately asserts that fetchable
application state cannot shorten a published unbounded decision. The real KRaft `PlaintextConsumerPollTest` suite
remains green. Before production integration, a broker-restart smoke test should repeat the invariant against real
sockets. Unsent-request expiration is covered deterministically through the real fetch manager, network delegate,
reactor post-I/O drain, and blocking fetch waiter.

## Validation Checkpoint (2026-08-20)

* Focused reactor, fetch-manager, and groupless cross-component regressions passed with Checkstyle and SpotBugs.
* After removing the application-side rescan, the focused no-fetchable, published-wait, and groupless reconnect
  regressions passed with Checkstyle and SpotBugs.
* The complete `clients` unit suite passed: 13,584 tests, zero failures.
* The KRaft `PlaintextConsumerPollTest` integration suite passed: 24 tests, zero failures.
* A final read-only adversarial review passed after semantic-generation, double-wakeup, post-poll ordering, and
  unsent-expiration findings were addressed.

These results validate the deterministic POC checkpoint. Production integration still requires the real-socket
broker-restart smoke test and terminal-path coverage listed in the evidence document.
