# Issue Draft: Unify AsyncKafkaConsumer Progress Decisions

## Suggested title

AsyncKafkaConsumer lacks a unified progress contract between request managers and application waits

## Type

Improvement

## Summary

`AsyncKafkaConsumer` has a background event loop, but progress decisions are still distributed across the
application thread, `ConsumerReactor`, individual request managers, network callbacks, futures, and fetch
buffer wakeups.

This makes it difficult to answer a basic scheduling question from one state snapshot:

> What can make progress now, and which event, capacity change, completion, or deadline should cause the next poll?

The lack of a unified progress contract has contributed to recurring busy-loop, delayed-progress, and wakeup
coordination fixes. This issue proposes establishing that contract incrementally without changing the public
consumer API.

## Current model

The background thread currently combines several independent mechanisms:

- `RequestManager.poll()` returns requests and a delay for the next network poll.
- `RequestManager.maximumTimeToWait()` separately limits how long the application thread may wait.
- `AsyncKafkaConsumer.pollForFetches()` performs additional application-thread checks before waiting on the fetch
  buffer.
- Request managers and response handlers may wake the fetch buffer directly.
- Futures and background events provide additional completion and notification paths.

Each mechanism is locally reasonable, but they may describe different state snapshots. A timeout can express
urgency without expressing whether progress is feasible, while a wakeup may indicate only that a method returned an
empty result rather than that manager-owned state actually changed.

## Example failure mode

Fetch preparation may produce no request because partitions are waiting for one of several different reasons:

- an existing request is in flight;
- a broker is in reconnect backoff;
- leader metadata is unavailable;
- buffered data must be drained;
- no partition is currently fetchable.

Treating all empty results as the same outcome can cause application/network wakeup ping-pong. Suppressing the
wakeup without publishing a real retry deadline can instead leave the application waiting with a stale snapshot.

Mixed conditions are also possible. For example, one node may have a request in flight while another node is in
reconnect backoff. The in-flight completion is an event-driven wait, but reconnect backoff still has a finite retry
deadline. A single boolean or an early `Long.MAX_VALUE` result cannot accurately represent both.

## Proposed direction

Introduce a typed progress contract between request-manager state machines and the background event loop. The first
minimal form only needs to distinguish:

```text
NextReconcile.ON_EVENT
NextReconcile.AT_DEADLINE(absoluteDeadlineMs)
```

The event loop aggregates these intents and owns the resulting scheduling decision. That decision should be used
consistently for both network polling and application-wait publication.

This requires a publication protocol, not just a new return type:

1. compute the decision from background-thread-owned state;
2. publish an immutable snapshot;
3. only then notify an application waiter when the new deadline is earlier or has expired;
4. deliver at most one deadline notification for the same state transition.

Request managers can migrate incrementally. Existing managers may initially adapt their current
`maximumTimeToWait()` result, while fetch preparation can be the first producer of typed conditions.

## Desired invariants

1. Every mutable manager-owned state has one execution-context owner.
2. Every retry or reschedule names the event or deadline that can enable progress.
3. A newly published decision is visible before its corresponding wakeup is delivered.
4. Mixed manager or partition conditions preserve the earliest real deadline.
5. An expired deadline causes one progress transition, not an unbounded zero-timeout poll loop.
6. Every queue, in-flight collection, and cross-thread buffer has an explicit bound.

## Initial scope

- Define a typed progress-intent abstraction.
- Aggregate progress intents in the background event loop.
- Represent deadlines as absolute times so publication latency is not added to the wait.
- Derive fetch progress from explicit fetch-preparation conditions.
- Remove the application-side `SubscriptionState` / `FetchBuffer` safety rescan after the publication and wakeup
  protocol has deterministic equivalent coverage.
- Preserve the existing public consumer API and callback threading model.

## Non-goals

- Rename existing runtime thread names or public metrics as part of the class rename.
- Move all `SubscriptionState` mutations behind reactor commands in one patch.
- Redesign rebalance callback execution in this issue.
- Remove the `NO_FETCHABLE_PARTITIONS` compatibility deadline before assignment and position changes have explicit
  reactor events.
- Replace KIP-945 or claim that the background-thread architecture itself is new.

## Acceptance criteria

- A request manager can express event-driven and deadline-driven waits without encoding both as an anonymous delay.
- The background event loop uses the aggregated decision to bound every network poll until the deadline is handled.
- The application thread cannot remain blocked on an older, longer decision after a shorter decision is published.
- The application thread applies the published reactor decision without rescanning `SubscriptionState` or
  `FetchBuffer` to derive another timeout.
- A consumer without a group id can still be notified when a fetch retry deadline expires.
- Mixed `REQUEST_IN_FLIGHT` and `RECONNECT_BACKOFF` conditions retain the reconnect deadline.
- Focused tests cover publish-before-wakeup ordering, elapsed-time subtraction, one-shot deadline notification, early
  network-poll return, empty fetch responses, failures, request completion, and duplicate effect coalescing.
- Existing async consumer and share consumer tests remain green.

## Related work

- KAFKA-14246 and the consumer threading refactor established the application/background event-queue model.
- KIP-945 discusses updating the consumer threading model but leaves parts of the detailed threading and data flow
  open.
- KAFKA-20426 and KAFKA-20253 addressed busy waits caused by locally computed wait times.
- KAFKA-20854 / PR 23014 distinguishes fetch-preparation outcomes to prevent paused-partition wakeup ping-pong.

This issue should complement that work by defining the common progress and publication contract. KIP-945 is related
history, not a dependency or approval gate: the focused ownership and progress proposal can proceed independently
while explicitly building on the existing async consumer threading refactor.

## Open questions

- Should progress intents remain a request-manager interface, or become the output of reactor-owned state reducers?
- Which current application-thread state checks can be removed only after equivalent completion events exist?
- Should deadline delivery use the fetch-buffer latch temporarily, or introduce a dedicated application-wait signal?
