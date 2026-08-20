# Async Consumer Reactor POC

## Status

This is an exploratory design and migration plan. It does not change the Kafka consumer public API or protocol.

The immediate goal is to test whether the current `AsyncKafkaConsumer` implementation can evolve toward one
owner for protocol state and one place that decides whether the consumer can make progress.

## Problem Statement

The current implementation has a background event loop, but it does not yet have a single progress model.
Several components independently decide when work should run or when another thread should wake:

* `AsyncKafkaConsumer` reads `SubscriptionState`, collects records, and waits on `FetchBuffer`.
* `ConsumerNetworkThread` processes application events, polls request managers, and polls the network client.
* Each `RequestManager` independently returns a network poll delay and an application-thread wait delay.
* A request manager may wake `FetchBuffer` directly.
* Futures, background events, callback-completed events, and user wakeups use separate notification paths.

These mechanisms are individually reasonable, but their decisions are not based on one state snapshot. A local
condition can therefore say "run now" even when another component knows that no useful transition is currently
possible.

## Before

| Execution context | State read or mutated | Progress mechanism |
| --- | --- | --- |
| Application thread | assignment, positions, buffered fetches, in-flight poll state | event enqueue, future wait, `FetchBuffer.awaitWakeup` |
| Consumer network thread | application events, request-manager state, network responses | network poll and cached `maximumTimeToWait` |
| Request managers | membership, coordinator, commit, offset and fetch sub-state | `PollResult`, `maximumTimeToWait`, direct buffer wakeup |

The effective flow is:

```text
poll()
  -> inspect application-thread state
  -> enqueue one or more events
  -> read a cached background-thread timeout
  -> wait on FetchBuffer

ConsumerNetworkThread.runOnce()
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

The target is a `ConsumerReactor`: the single owner of mutable consumer protocol state and the only scheduler for
background progress.

`ConsumerReactor` is also the intended replacement name for `ConsumerNetworkThread`. The current name describes an
execution resource, while the target abstraction owns events, protocol state, transitions, and progress decisions.
The reactor may still use a dedicated thread internally, but that thread is its executor rather than its identity.
The rename should happen only after these decision and ownership contracts exist; a mechanical rename would
overstate the current implementation.

| Component | Responsibility after migration |
| --- | --- |
| Application facade | Validate API calls, submit bounded commands, return records, execute user callbacks |
| Consumer reactor | Own protocol state, apply events in order, choose the next progress action |
| Request-manager state machines | Reduce reactor-owned state and propose effects; do not independently schedule threads |
| Network delegate | Transport only: send requests and publish completions |
| Fetch mailbox | Bounded transfer of immutable/owned fetch results; not a generic wakeup condition |
| Callback mailbox | Publish callback work and require an epoch-tagged completion acknowledgement |

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
being added to the wait again. The application-side `SubscriptionState` / `FetchBuffer` safety rescan added by PR
23014 remains in place until the typed publication protocol has equivalent end-to-end coverage. Re-observing the
same blocking condition preserves its existing deadline; otherwise unrelated events could continuously move the
retry forward and create starvation. The compatibility adapter is explicitly tagged so the reactor conservatively
preserves its earlier deadline across early network returns without applying that heuristic to native typed intents.

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

An empty result wakes the application thread only for `DATA_ALREADY_BUFFERED` or `CLOSING`. Completing an in-flight
request wakes it on every terminal response or failure. Reconnect contributes the network client's actual remaining
connection delay, including exponential backoff, instead of substituting the configured base retry interval or an
immediate wakeup. Missing metadata contributes the configured retry deadline.

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

The POC does not yet rename `ConsumerNetworkThread`, move `SubscriptionState`, change callback threading, or claim
to fix every busy loop. It has established a progress-decision publication seam, a typed fetch-preparation result,
and the first direct typed-intent producer.

The POC includes a deterministic `FetchBuffer` concurrency test plus a cross-component test of the complete async
consumer chain with only its socket replaced by `MockClient`. In the latter, a consumer without a group id and with
a valid position proves that the reactor caps network polling to the 100 ms reconnect deadline and eventually
generates a new fetch. The separate concurrency test proves publication and expiry wakeups, because current trunk's
application-side safety rescan also bounds the caller's next wait. The real KRaft `PlaintextConsumerPollTest` suite
remains green. Before production integration, a broker-restart smoke test should repeat the invariant against real
sockets, and direct fetch-buffer notifications should migrate into named reactor effects. The application-side
safety checks in `AsyncKafkaConsumer.pollForFetches()` remain intentionally in place until typed progress decisions
have equivalent end-to-end coverage; removing those independent bounds now would introduce a liveness regression.

## Validation Checkpoint (2026-08-20)

* Focused reactor, fetch-manager, and groupless cross-component regressions passed with Checkstyle and SpotBugs.
* The complete `clients` unit suite passed: 13,576 tests, zero failures.
* The KRaft `PlaintextConsumerPollTest` integration suite passed: 24 tests, zero failures.

These results validate this POC checkpoint, not the later removal of the application-side safety rescan. That step
still requires the real-socket broker-restart smoke test and terminal-path coverage listed in the evidence document.
