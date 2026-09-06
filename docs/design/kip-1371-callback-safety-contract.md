# Callback safety: validate authority before selecting execution location

Context: Approach 2 at baseline `bcc1e49d4e`, with an optional ordered-response
application experiment. This note supersedes the inference that a changed
intermediate completion observation alone proves a business-correctness failure.

## Intent and forces

The intent is correct operation outcomes and owner state despite response
interleavings, not a requirement to eliminate callbacks.

- Hard constraint: a stale response must not invalidate a newer coordinator.
- Hard constraint: a valid commit success is not undone by an independent failure.
- Hard constraint: another request must satisfy its current owner's eligibility rules.
- Desired quality: extensions should use guarded owner capabilities, rather than
  duplicate state-validation rules inside each callback.
- Competing qualities: inline delivery minimizes staging; a named response phase
  makes execution boundaries easier to inspect but adds retention and allocations.
- Unknown: full lifecycle, public-consumer compatibility, and current performance
  of a broad response-application phase.

## Two mechanisms, one set of safety assertions

Inline completion and ordered post-I/O delivery are both retained as alternatives.
Inline remains the default. The POC uses a package-local opt-in, not a public flag.

The same component fixture exercises:

1. Commit success before/after Heartbeat `NOT_COORDINATOR`: the first commit remains
   successful, the coordinator becomes unknown, and the continuation's commit is
   not sent until discovery permits it. Recovery admits the requested offset and
   repeated polling does not duplicate the in-flight attempt.
2. Rediscovery before an old heartbeat response: the newer coordinator survives,
   its version is unchanged by the old observation, and follow-up work can proceed.
3. Intermediate completion observations: a callback can see an earlier owner view
   even though the batch finishes in another state. This is recorded separately
   from final safety assertions and is not a promised all-owner snapshot.

These tests retain the existing one-I/O-poll and bounded follow-up admission loop.
They do not prove arbitrary recursive request sending is safe or that all possible
response permutations commute.

Negative control: temporarily removing the `observedVersion != coordinatorVersion`
guard from `markCoordinatorUnknownIfCurrent` caused both variants of
`testLateHeartbeatInvalidationCannotClearRediscoveredOwner` to fail because the
rediscovered coordinator was cleared (`deferred=false` and `deferred=true`). The
guard was restored before final validation. This isolates owner validation as a
necessary protection for this schedule; ordered callback delivery is not a substitute.

## Responsibility allocation

- Callback carries captured operation/attempt context and invokes a supported owner
  entry point. Running inline does not grant unrestricted access to peer state.
- Owner validates scope/version and applies its domain transition. Validation and
  mutation must form one protected operation; checking outside and mutating later
  recreates the stale-observation problem.
- Operation completion must not expose inconsistent owner state to a synchronous
  continuation. A fixed reactor phase alone does not prevent such reentrancy.
- Reactor supplies only the ordering actually required by non-interchangeable steps,
  such as delaying new request admission until the relevant completion processing
  finishes. It does not interpret every protocol response.

## Publication is not notification

An application thread already running can consume a published fetch without waiting
for a new wakeup. Therefore a wakeup barrier is not a visibility barrier. Every handoff
must identify when data becomes readable and ensure its required supporting state is
consistent by that point. This is an analysis obligation, not a claim that every
existing Kafka handoff is defective or has been migrated.

## Design verdict and next proof

Do not choose centralized response delivery merely because callbacks can execute
inline. First require guarded owner transitions and operation-specific completion
contracts; use explicit execution boundaries where two operations genuinely cannot
be exchanged. The response-phase experiment remains useful as a comparison.

After the coordinator schedules, the next bounded proof is owner consistency at
`future.complete`: determine which fields a supported reentrant continuation can
observe, then check success, failure, and retry transitions. Do not invent a blanket
rule that every future needs another queue. Timeout/cancellation winners and fetch
visibility need their own contracts rather than inference from this coordinator slice.

## Validation receipt

Final source selection: **991 tests across 16 suites passed twice**, zero failures,
errors, or skipped tests; JDK 17, offline Gradle, one test fork, `:clients:test --rerun`,
`maxTestRetries=0`. Checkstyle main/test, SpotBugs main, and Spotless Java passed.
The temporary unfenced variant failed both selected stale-owner cases and is not
present in the final code.

Suites: ConsumerBatchedDecisionTest, ConsumerAdmissionContractTest,
NetworkClientDelegateTest, ConsumerNetworkThreadTest, CommitRequestManagerTest,
CoordinatorRequestManagerTest, RequestManagersTest, ConsumerHeartbeatRequestManagerTest,
ShareHeartbeatRequestManagerTest, StreamsGroupHeartbeatRequestManagerTest,
OffsetsRequestManagerTest, FetchRequestManagerTest, AsyncKafkaConsumerTest,
ShareConsumerImplTest, ConsumerAsyncPollMetadataTest, ConsumerOperationResultContractTest.

Only explicitly parameterized/opt-in fixtures exercise response batching; this suite
count is not 991 tests of each mode or an all-variant batching proof. No real broker,
new benchmark, remote push, or KIP publication is included.
