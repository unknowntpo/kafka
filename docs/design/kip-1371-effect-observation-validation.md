# Approach 2: effect-specific observation requirements

Local validation, 2026-09-06, following production revision `24b3e3fa8c`.
This follow-up adds tests, not a new effect queue, stronger completion contract,
or production change. The original KIP and other worktrees remain unchanged.

## Question and scope

Which state must be observable when each effect is released? An operation-local
completion, a data notification, and an aggregate-wait notification need not
have identical prerequisites. Conversely, a successful commit example cannot
justify early release of every other effect.

The following ledger distinguishes implemented ordering from obligations still
requiring proof. The scope is the current local POC, not all Kafka variants or
the public KIP's target model.

| Effect | Recipient and necessary prerequisite | Existing mechanism/evidence | Limit |
| --- | --- | --- | --- |
| Internal commit future completion | Its dependent sees the admitted operation's result; commit success is not undone by a later coordinator invalidation | `ConsumerBatchedDecisionTest.testCompletionObserverIsNotABatchSnapshot` exchanges the two response orders | Internal instrumentation, not a proof that user callbacks require or receive a whole-batch snapshot |
| Fetch data notification | The application can acquire the synchronized buffer and find the published fetch | `FetchBufferProducer.add` delegates to store-before-signal under the buffer lock; `FetchBufferProducerTest.testDataPublicationIsVisibleWhenWaitReturns` | Buffered data is not accepted record delivery; assignment/position validation still applies |
| Fetch request-completion notification | A completed pending request can enable another preparation, even with no records | `requestCompleted` removes pending-node bookkeeping before waking; repeated completion without a new registration does not wake; empty/failed/session-error response tests exist | Not every empty preparation is progress; node bookkeeping is not a generation fence, and no universal no-spin guarantee follows |
| Async-poll error notification | The affected event's error and completed flag are visible before its observer is released | `AsyncPollEvent.completeExceptionally` publishes before reconciliation-future completion and latched buffer wake; delegate/admission paths are exercised below | Error visibility does not mean owner work was cancelled, or that all manager state is current |
| Earlier aggregate-wait notification | An application using an older wait must be able to observe the new bound and leave that old wait | `ConsumerNetworkThread` writes its cached wait before schedule wake; `testEarlyFetchWakeAndAggregateSchedule` covers both wait-entry orders and notification-disabled negative controls | Local fetch/error wakes can precede this publication; this is not a schedule-before-every-effect barrier |

Application wait still contains compatibility rescans in
`AsyncKafkaConsumer.pollForFetches`. These tests do not prove those rescans can
be removed or that every enabling input has a complete notification path.
Existing Share/Streams regression tests are not equivalent to migrating all of
their effects through the regular-fetch mechanisms in this ledger.

## New tests: error observation without waiting for the aggregate

`ConsumerAsyncPollMetadataTest.testAdmissionErrorCanBeObservedBeforeAggregatePublication`
retains an error in the real delegate before an eligible event arrives. The
next input phase delivers it before the manager pass computes a changed wait:

1. Previous published wait is `Long.MAX_VALUE`; the manager's next contribution
   will be 42 ms.
2. The input phase publishes the event error and wakes a real FetchBuffer waiter.
3. The notification seam holds the network loop until the observer has read the
   correct error and completed flag while the aggregate is still the old value.
4. After that observation, the loop continues and publishes 42 ms.

Both wake-before-wait and wait-before-wake are controlled with latches. The
30-second waiter timer is only a failure fallback; bounded joins and assertions
require release without depending on it. Cleanup interrupts/releases the waiter.
This deliberately instrumented component schedule is not a public API proof of
every pre-publication error path, but demonstrates that this event-error
observation does not require aggregate publication first.

Two further component cases cover operation identity:

- An expired poll does not consume the metadata error intended for a still-live
  metadata-dependent poll.
- After an error, completion of the old position work does not start fetch work
  or complete the replacement poll. The replacement's own position completion
  can proceed and succeed; the old error remains attached to the old event.

## Public poll: error, user wakeup, and subsequent recovery

`AsyncKafkaConsumerTest.testMetadataErrorFromDelegateAfterAdmissionSurfacesThroughPoll`
now covers three schedules: metadata error alone, user wakeup before metadata
delivery, and user wakeup after metadata delivery, all at controlled buffer-wait
entry. The two operations are not conflated:

- With user wakeup, the current call throws `WakeupException` at the next loop
  entry, and the next poll surfaces the retained metadata error.
- Without user wakeup, the current poll surfaces that error.
- After the error is consumed, a fresh poll returns a controlled record result;
  the previous event does not poison the replacement or repeat the notification.

The public consumer, processor, loop and delegate are real; managers, metadata
failure injection, the collector and wait scheduling are test seams. A real
empty interceptor chain preserves the returned records. Separate component
tests exercise the real buffer latch. This is public-call routing evidence,
not real-broker fetch/recovery, cancellation of remote work, or all lifecycle
coverage. The tested error/wakeup order is characterization of this POC, not a
new proposed public precedence guarantee for every possible interleaving.

## Implication and remaining gates

The evidence supports keeping operation-error publication distinct from
aggregate-wait publication on these paths. It does not establish that a shared
barrier is never needed. No new production abstraction was necessary for this
follow-up.

Remaining gates include effects triggered by rebalance callbacks, fatal cleanup
and close, Share/Streams-specific delivery, real-broker recovery, and the cost
of the extra post-I/O manager pass. Any newly demonstrated observation requiring
a stronger boundary should identify its producer, recipient, state dependency,
and failing schedule before introducing staging.

Related context: [contract assessment](kip-1371-approach2-contract-summary.md),
[late metadata fix](kip-1371-async-poll-metadata-delivery.md), and
[React/Vue discussion](kip-1371-react-vue-publication-lessons.md).


## Validation result and reproduction

Final test sources passed **1,489 tests across 31 suites twice**, with zero
failures, errors or skips; retries were disabled. Compared with the previous
1,462-case selection, this adds six cases plus the existing 21-case
`WakeupTriggerTest` suite. Counts from different runs are not additive evidence.
Checkstyle main/test, Spotless Java, and SpotBugs main checks passed or were
up-to-date on these sources. This was not an all-clients or real-broker run.

The new tests did not require a production fix. During test development the
recovery fixture's mock interceptor returned null; using the real empty
interceptor chain allowed the public return value to be verified.

Reproduce with JDK 17 and Gradle 9.7.1 (this machine used its cached distribution):

```sh
./gradlew :clients:test --rerun \
  --tests '*ConsumerBatchedDecisionTest' \
  --tests '*ConsumerAdmissionContractTest' \
  --tests '*ConsumerNetworkThreadTest' \
  --tests '*NetworkClientDelegateTest' \
  --tests '*CommitRequestManagerTest' \
  --tests '*CoordinatorRequestManagerTest' \
  --tests '*ConsumerHeartbeatRequestManagerTest' \
  --tests '*ShareHeartbeatRequestManagerTest' \
  --tests '*StreamsGroupHeartbeatRequestManagerTest' \
  --tests '*StreamsGroupTopologyDescriptionRequestManagerTest' \
  --tests '*OffsetsRequestManagerTest' \
  --tests '*ConsumerPublicationContractTest' \
  --tests '*FetchRequestManagerTest' \
  --tests '*AsyncKafkaConsumerTest' \
  --tests '*ApplicationEventProcessorTest' \
  --tests '*FetchCollectorTest' \
  --tests '*FetchBufferTest' \
  --tests '*ShareConsumerImplTest' \
  --tests '*ApplicationEventHandlerTest' \
  --tests '*RequestStateTest' \
  --tests '*FetchBufferProducerTest' \
  --tests '*FetcherTest' \
  --tests '*ConsumerAsyncPollMetadataTest' \
  --tests '*CompletableEventReaperTest' \
  --tests '*WakeupTriggerTest' \
  :clients:spotlessJavaCheck --offline --max-workers=2 -PmaxParallelForks=1 -PmaxTestRetries=0 -PtestLoggingEvents=failed
```

Reports: `clients/build/reports/tests/test/index.html`; machine-readable results:
`clients/build/test-results/test/TEST-*.xml`. Later test runs overwrite them.
