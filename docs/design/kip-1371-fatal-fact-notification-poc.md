# Approach 2: separate the fatal fact from notification consumption

Local experiment, 2026-09-06. Parent: `971a163277`. This is not a published KIP
change or a selected public-consumer error policy.

## Question and forces

Can Commit and Heartbeat read the same discovery failure independently without
importing Approach 3's generic event/command/action framework?

The parent characterization established an implementation-induced ordering edge:
Commit must read before Heartbeat clears the shared fatal slot. We want independent
readers, but must also avoid repeated application notifications and stale errors
surviving discovery recovery. A dependency scheduler would preserve the ordering
edge; separating the two responsibilities can remove that particular edge.

## Candidate contract

Coordinator retains the latest fatal discovery outcome and one boolean indicating
whether its application notification remains available. Heartbeat calls
`takeFatalErrorForApplication()` instead of `getAndClearFatalError()`. Taking consumes
only the notification marker. Commit's existing `fatalError()` read is unchanged.
Regular/share use the common heartbeat base; Streams uses its own updated caller.

- Each fatal discovery response makes one notification available, even if it has
  the same error kind as the previous response. This is not once per consumer lifetime.
- Taking a notification does not prove application observation or successful queue
  delivery. Existing handoff failure behavior remains; no acknowledgement protocol
  or retry queue is introduced.
- Starting another discovery attempt does not clear the fact.
- The next discovery response supersedes both the fact and an unclaimed notification.
  A successful or retriable outcome clears them; another fatal outcome replaces them.
  This preserves the parent's response-driven reset boundary, not durable error history.
- While that fact remains current, a newly queued commit also fails on its next poll.
  This deliberately differs from the parent after Heartbeat consumed the shared slot.
  It is a candidate policy to evaluate, not a proven compatibility-neutral change.

Only the configured application-error route consumes the notification. This is not
multi-subscriber fan-out, an immutable snapshot API, a cross-thread primitive, or a
general capability restriction. No loop order, broker protocol, public API, scheduler,
thread name, or extra queue changes.

## Alternatives and limits

1. Keep the fixed read-before-clear order and its characterization test: smallest
   behavior change, but new readers must know the ordering rule.
2. Split fact and notification locally (this experiment): removes that ordering
   edge with constant-size state, but exposes the need to define new-operation policy.
3. Route an owner event through the Approach 3 policy/effect boundary: centralizes
   application delivery and its publication ordering, with more migration machinery.

This experiment does not establish Approach 2 as sufficient for all dependencies.
In particular, it retains Heartbeat as the application-error handoff caller and does
not provide schedule-before-every-effect ordering. A later response may supersede an
unclaimed error; arbitrary interleaving with that response is not reader independence.
Close/cancellation, public-consumer recovery, all variant operation paths, and failure
of the handoff itself still need separate evidence before adopting the policy.

## Validation

The strengthened two-order component test failed against parent production code in
both orders because notification consumption erased the retained fact. The parent
test already distinguished successful operation error delivery in the normal order
from pending futures in the reversed order.

The candidate test asserts the same exception in commit and offset-fetch futures
under both reader orders, a single ErrorEvent handoff across repeated polls, and a
newly queued commit seeing the still-current fact. After discovery recovery, a new
commit can build an attempt without changing the old operation's failure. The
background handler is mocked:
this proves handoff invocation, not application observation. Coordinator tests cover
consumed/unconsumed notifications, an in-flight retry, replacement by success or
retriable error, and another fatal discovery notifying once again.

All fixtures are in-memory with MockTime; no broker or wall-clock sleep is required.
Final Java/test sources passed **572 cases across seven suites twice**, with zero
failures, errors or skips and retries disabled. The suites were
`ConsumerBatchedDecisionTest`, `CoordinatorRequestManagerTest`,
`CommitRequestManagerTest`, `ConsumerHeartbeatRequestManagerTest`,
`ShareHeartbeatRequestManagerTest`, `StreamsGroupHeartbeatRequestManagerTest`, and
`ConsumerNetworkThreadTest`. Each run forced `:clients:test --rerun` with
`-PmaxParallelForks=1 -PmaxTestRetries=0`, offline, using JDK 17 and Gradle 9.7.1.
Checkstyle main/test, Spotless Java and SpotBugs main passed or remained up-to-date
on the unchanged production sources. No real-broker, performance or full-client
suite claim is made. The final report is `clients/build/reports/tests/test/index.html`.

Result: this reader-order edge can be removed locally without storing a dependency
graph. Adoption remains conditional on the operation policy described above;
retention semantics are not proven correct merely because the reversed-order test
now passes. No push, Confluence edit, or modification to another POC was performed.
