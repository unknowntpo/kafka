# KIP-1371 Approach 2: late metadata delivery and the effect boundary

Local POC evidence, 2026-09-06. No public KIP, remote branch, benchmark, or
other worktree is changed by this experiment.

## Closed component gate: delivery after admission

`AsyncPollEvent` implements `MetadataErrorNotifiableEvent`, but is not a
`CompletableEvent`. The existing network loop checked metadata errors when
admitting it, then checked only the completable-event reaper after I/O. A pending
async poll could therefore miss an error first stored by the delegate during a
later network poll. Error publication and a latched wake were already supported
by the event; the missing part was reaching that event after admission.

Three tests in `ConsumerAsyncPollMetadataTest` failed before the production
change: a pending event did not receive a later error, multiple live declared
dependents were not notified, and a late position completion still began fetch
preparation. Three companion cases (error before admission, successful completion,
and expiry) already passed. This is a current-POC component reproduction, not a
pinned historical-release or real-broker reproduction of KAFKA-20397.

The small correction is:

1. The loop retains admitted async-poll metadata dependents separately from the
   existing completable-event reaper. It does not retain only the latest event.
2. After I/O, completed events and validated-position events past their deadline
   are removed. Remaining dependents participate in the existing metadata-error
   delivery with the reaper's eligible events. If none exist, the error remains
   in the delegate for a later eligible admission.
3. Delivery uses each event's existing error-before-notification path. Failed
   events are removed from the tracking, and cleanup clears retained references.
4. A later position/fetch future completion does not advance or overwrite an
   already completed async-poll event. A synchronous processing exception also
   completes and notifies the async event rather than merely logging it.

This follows the existing metadata-dependency contract, not a topic/request-level
correlation protocol: all live declared metadata dependents can receive the same
cached metadata error. Unrelated events are not newly subscribed to it. There is
no new global action queue, publication transaction, or operation cancellation
protocol. Existing position work may finish; ending its observer does not undo
that owner work. Retention is pruned at loop boundaries, not a new admission
capacity/backpressure guarantee.

## Validation paths

The ten metadata component cases cover:

- admission before and after the delegate stores an error;
- completed and expired events not consuming an error for a later operation;
- multiple live metadata dependents retaining their identities;
- no new fetch stage after error delivery and no replacement by a later fetch error;
- synchronous processing failure;
- a real FetchBuffer waiter entering before and after error delivery.

The loop, event processor, delegate, event, and buffer are real components.
Metadata exceptions and manager futures are controlled seams; transport uses
MockClient. The wait tests use a real application test thread with bounded joins
and explicit cleanup, not a sleep-dependent race.

`AsyncKafkaConsumerTest.testMetadataErrorFromDelegateAfterAdmissionSurfacesThroughPoll`
also exercises public `poll()`: its admitted event reaches the real processor,
then the real loop/delegate captures the injected metadata error at controlled
wait entry. Public poll throws that same error. This test controls scheduling and
uses a mock wait buffer; the separate real-buffer tests prove latching. It does
not start a broker or prove all actual wire-error/recovery paths.

## Major decision exposed: what does a completion promise?

`ConsumerBatchedDecisionTest.testCompletionObserverIsNotABatchSnapshot` tests
both orders of commit success and heartbeat coordinator invalidation within the
same I/O batch. An inline dependent of the internal commit future sees:

| Response order | Coordinator known when the dependent executes | Coordinator after the batch |
| --- | --- | --- |
| Commit, then heartbeat invalidation | Yes | Unknown |
| Heartbeat invalidation, then commit | No | Unknown |

Both commit operations succeed. Both batches end with the correct coordinator
state. New dependent request admission is separately ordered after the batch.
The observer reads internal state as test instrumentation; this is not evidence
that a public consumer API exposes that coordinator value or that a user-facing
result is wrong. It distinguishes two architectural promises:

- **Operation-local completion:** the operation's supporting state/outcome is
  ready. It does not promise completion of unrelated owner updates in the same
  batch. Aggregate wait changes still require their own publication and latched
  notification. This is compatible with the current smaller approach.
- **Batch-wide publication before application effects:** selected completions
  and notifications wait until the batch's relevant owner updates and aggregate
  publication finish. This requires defining the batch/effect set and staging
  those effects, preserving exceptions, reentrancy, delivery, and close outcomes.
  It need not imply the old full class hierarchy, but is a stronger contract and
  a material migration decision.

Recommendation: retain operation-local completion plus explicit shared wait
invalidation unless the KIP requires a demonstrated batch-wide observation
guarantee. Do not describe the current POC as offering the stronger promise.
The author should select the intended promise before broad effect migration,
all-variant completion, or acceptance benchmarks are finalized. Neither result
justifies making a manager's domain policy part of the generic reactor.

## Remaining work after that decision

Complete the selected contract across remaining producers/variants and affected
lifecycle paths; validate real-broker recovery; compare the parent next-iteration
boundary with the extra-pass candidate using pinned performance runs; then
update the isolated KIP draft and migration/evidence record. Existing Share and
Streams regression passes are not complete variant migration proof. Historical
benchmark results are not evidence for this code.

## Final local validation

**1,462 tests across 30 suites passed twice** on identical final Java sources,
with zero failures/errors/skips and retries disabled. The second run forced
`:clients:test --rerun`. Checkstyle main/test, Spotless Java and SpotBugs main
passed. The selection combines the earlier completion-batch and capability
suites with `ConsumerAsyncPollMetadataTest` and `CompletableEventReaperTest`;
the complete selectors are below. This is not an all-clients or real-broker run.

Using JDK 17 and the cached Gradle 9.7.1 distribution (or an installed wrapper):

```sh
./gradlew :clients:test --rerun \
  --tests '*ConsumerBatchedDecisionTest' --tests '*ConsumerAdmissionContractTest' \
  --tests '*ConsumerNetworkThreadTest' --tests '*NetworkClientDelegateTest' \
  --tests '*CommitRequestManagerTest' --tests '*CoordinatorRequestManagerTest' \
  --tests '*ConsumerHeartbeatRequestManagerTest' --tests '*ShareHeartbeatRequestManagerTest' \
  --tests '*StreamsGroupHeartbeatRequestManagerTest' --tests '*StreamsGroupTopologyDescriptionRequestManagerTest' \
  --tests '*OffsetsRequestManagerTest' --tests '*ConsumerPublicationContractTest' \
  --tests '*FetchRequestManagerTest' --tests '*AsyncKafkaConsumerTest' \
  --tests '*ApplicationEventProcessorTest' --tests '*FetchCollectorTest' \
  --tests '*FetchBufferTest' --tests '*ShareConsumerImplTest' \
  --tests '*ApplicationEventHandlerTest' --tests '*RequestStateTest' \
  --tests '*FetchBufferProducerTest' --tests '*FetcherTest' \
  --tests '*ConsumerAsyncPollMetadataTest' --tests '*CompletableEventReaperTest' \
  :clients:spotlessJavaCheck --offline --max-workers=2 \
  -PmaxParallelForks=1 -PmaxTestRetries=0 -PtestLoggingEvents=failed
```

Reports are under `clients/build/reports/tests/test` and XML results under
`clients/build/test-results/test`; future test selections can overwrite them.
