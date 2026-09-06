# KIP-1371 coordination design v0

Status: proposed architectural baseline, not a completed migration.
Evidence baseline: `265988ee3c`, Approach 2 isolated POC.

## Intent and constraints

Keep request-manager domain ownership and existing operation semantics while making
cross-manager execution obligations explicit and harder to bypass. The purpose is
not to replace every future with a message or to make every operation transactional
across one reactor iteration.

The forces are:

- Local owners know request eligibility, retry rules, and which operations fail.
- A local decision can depend on another owner's state or affect another owner.
- A synchronous future continuation can run inside network I/O or manager polling.
- Changing input cutoffs can change recipients and the winner between error and timeout.
- Cross-round delivery needs bounded retention and a guaranteed opportunity to run.
- Authors should not have to rediscover the complete manager ordering for each extension.

Behavior preservation wins over a uniform phase model when they conflict. Existing
component behavior is a conservative migration baseline; characterization alone
does not establish that every internal ordering is a public API guarantee.

## Selected direction

Retain local owners, a bounded reactor loop, and statically configured protocol
routes. Centralize execution obligations, not domain decisions. Do not introduce
a dynamic dependency graph, per-manager actor runtime, or universal effect queue.

| Responsibility | Owner |
| --- | --- |
| State, request eligibility, retry policy, affected operation selection | Request manager or explicitly scoped protocol owner |
| Which typed fact reaches which owner | Consumer/protocol configuration |
| Execute configured routes at their required boundary; retain runnable work without stranding it behind a network wait | Reactor |
| Validate an observation's operation scope and owner version | Receiving state owner |
| Publish data/error and signal through its synchronization contract | Narrow handoff capability |

Static routing still requires someone to define protocol dependencies. It makes
them explicit and reviewable; it does not infer them or eliminate their complexity.
The current immutable manager order and regression tests are a baseline, not yet
an implementation of all these route guarantees.

## Four distinct interaction contracts

| Interaction | Contract | Kafka example |
| --- | --- | --- |
| Owner view | Non-consuming read; coherent fields under the documented confinement rule | Coordinator target and version used to build a request |
| Cross-owner observation | Immutable fact with the necessary captured identity; only the receiving owner validates and mutates | A response for coordinator C7 must not invalidate C9 |
| Operation result | Completes the named operation under its existing success/error/cancellation semantics | A successful OffsetCommit remains successful if another response invalidates the coordinator |
| Application handoff | Supporting data/error is observable before its corresponding signal | Store fetched data before signalling; publish an earlier aggregate wait before its scheduling wake |

These are conceptual categories, not a requirement for four new class families.
Reuse futures, owner methods, and synchronized buffers where they already express
the contract. A future is an operation-result handle, not a state-ownership boundary.

Do not add a general `publish(event, phase)` API that makes each RM author choose
when delivery is safe. A supported route fixes its boundary and prerequisites.
Narrow capabilities should prevent unsupported effects where practical, such as
giving fetch production code a producer interface rather than unrestricted buffer
wakeup access. Tests remain necessary for semantic constraints that types cannot encode.

## Timing rule: preserve the logical boundary, not a universal round number

One `runOnce` is an execution unit, not an atomic transaction. A consequence can
cross iterations only after answering all of these questions:

1. Which fact and operation/owner identity must survive?
2. At which input cutoff does the owner select affected operations?
3. What must finish before another request, timeout, or application effect can proceed?
4. What disposes of pending delivery on cancellation, failure, or close?

The owner selects affected operations. The reactor must not inspect Commit's pending
lists or invent failure policy. Selecting recipients in the discovery callback is
not automatically equivalent to selecting them at the subsequent manager pass.

### Concrete dry-run results

| Schedule | Required interpretation |
| --- | --- |
| Coordinator unknown; discovery in flight | Dependent work waits for enabling input; an expired timer alone does not make a request legal. Independent work may proceed. |
| Old response reports C7 unavailable after C9 is installed | Receiving owner rejects the stale observation; single-thread execution alone is insufficient. |
| Commit succeeds while another response invalidates the coordinator | Keep the completed operation result. The changed coordinator affects subsequent admission, not the meaning of the earlier success. |
| Discovery fails; B enters the application queue during I/O | Current loop skips the post-I/O manager pass. Next iteration admits B before the error-consuming pass, so A and B can both fail. |
| Discovery fails; B arrives after an eligible post-I/O pass | A has failed and the error has been consumed. B must not automatically inherit that old error. |
| A's failure continuation queues B | B runs at the next input boundary, after the current error-consumption pass; no recursive queue drain is required. |
| Eligible post-I/O error delivery precedes end-of-round reaping | Deferring completion past the reaper can replace the existing authorization failure with timeout. Do not make that change implicitly. |
| Heartbeat poll timeout and commit admission coincide | Moving every state transition before every request can change the membership epoch captured by commit. Do not introduce a global timeout-first rule as a refactor. |
| Fetch request completes without records | Completion can free preparation capacity; no records does not universally mean no useful signal. |
| Metadata error races with application wait or user wakeup | Preserve error retention, wait-entry signalling, and recovery semantics; a schedule publication alone is not proof of delivery. |

The input-cutoff and timeout counterexamples rule out both unconditional
event-before-command and unconditional next-round delivery. They do not rule out
typed routing at the existing logical boundary.

## Coordinator fatal-error migration

Today, Commit reads the coordinator error without consuming it; Heartbeat consumes
it and forwards an application error. The configured order is therefore behavior-bearing.
The field is a consumable response outcome, not a permanent consumer-terminal flag.

The candidate improvement is value-based delivery with explicit recipient and
consumption rules at the same logical boundary. Keep the legacy read-before-clear
adapter until equivalence is demonstrated. Do not retain a fatal value indefinitely
or broadcast it indiscriminately to future operations.

This route is not yet implemented. A fieldless implementation is desirable only
if it removes implicit consumption coupling without changing the schedules above.
If preserving those schedules requires a small route-local batch, evaluate that
batch specifically; do not infer a need for a global event bus.

## Bounds, lifecycle, and extension safety

- Keep bounded input batches and manager passes; no recursive fixed-point drain.
- Ready retained work must prevent a blocking network wait from stranding it.
- No promise of another normal iteration after shutdown; every retained route needs
  an explicit cleanup disposition consistent with the existing operation outcome.
- Observation delivery and operation completion are distinct: do not silently replay
  a completed operation when a route is retried.
- A future continuation may enqueue supported input. Arbitrary direct peer mutation
  is not made safe by running on one thread.
- New RM behavior must declare its owner, enabling condition, output route, and
  lifecycle. Shared route tests enforce ordering; the author still owns domain correctness.

These are migration acceptance requirements. Existing tests do not establish a
universal runtime guarantee for every callback, close path, or consumer variant.

## Alternatives and costs

| Alternative | Decision |
| --- | --- |
| Keep current order plus comments/tests only | Valid immediate baseline, but implicit consumption and capability hazards remain. |
| Narrow capabilities and explicit routes at compatible boundaries | Selected direction; adds small interfaces and route tests, while retaining existing scheduling complexity where required. |
| Uniform next-round mailbox for all facts/results | Rejected as a behavior-preserving default: recipient cutoffs, timeout winners, and shutdown obligations change. |
| All state transitions, then all request construction | Not selected: the commit/poll-timeout example needs an explicit protocol-policy decision. |
| Approach 3's broader publication/action boundary | Potentially stronger uniform guarantees, but extra staging and migration burden; not necessary for operation-local results already correctly represented by futures. |

The selected approach trades a universally simple phase story for incremental
compatibility. Its success criterion is fewer implicit cross-owner obligations,
not the fewest classes or the largest shared framework.

## Migration and remaining proof

1. Preserve and test the existing input/order/outcome baseline (present).
2. Introduce narrow owner capabilities without changing invocation points.
3. Migrate one coordinator observation/error route at the same logical boundary;
   rerun recipient-cutoff, reentrancy, timeout, stale-version, and recovery schedules.
4. Defer only routes with proven liveness and lifecycle dispositions. Keep compatible
   direct delivery for routes where postponement changes results.
5. Extend the proof to regular, share, and Streams variants, then real-broker/public
   lifecycle tests and current-revision throughput/latency measurements.

A mechanical `ConsumerNetworkThread` to `ConsumerReactor` rename is independent of
these semantics and should remain a separately reviewable change.

The design direction is sufficiently settled to guide step 2 and one bounded step-3
experiment. Universal next-round delivery, global publication-before-every-result,
and changed timeout precedence are not accepted requirements.

## Evidence receipt and limits

At baseline `265988ee3c`, the combined local regression selection produced XML
reports for **1,545 tests across 33 suites, with 0 failures, 0 errors, and 0 skipped**.
It includes manager, reactor, application-event, fetch-buffer, and operation-result
coverage. The terminal process receipt was unavailable after session continuation;
this is an XML test-result statement, not a claim that every Gradle task completed.
Historical counts are separate selections and are not additive.

This run did not implement the proposed typed routes, exercise a real broker, prove
all lifecycle paths, or measure performance. See the linked documents for exact
fixtures and limitations:

- [Cross-round error dry run](kip-1371-cross-round-error-dry-run.md)
- [Behavior-preserving order contract](kip-1371-behavior-preserving-order-contract.md)
- [RM boundary and timeout dry run](kip-1371-rm-boundary-dry-run.md)
- [Effect-specific observation validation](kip-1371-effect-observation-validation.md)
- [Operation-result contract reuse](kip-1371-operation-result-contract-reuse.md)
- [Fetch capability POC](kip-1371-fetch-capability-poc.md)
