# Approach 2: RM extension and programmer knowledge burden

Local POC experiment, 2026-09-06. Baseline production revision: `5ab284519d`.
This is an executable extension probe, not a reproduction of a historical Kafka
issue or evidence that the existing fetch manager currently throws this error
from this entry point.

## Question

Can an author add a validation failure to an existing RM operation without
learning whether its caller is currently inside a future continuation, whether
that continuation executes inline, or when an application notification is safe?

The desired contract is that an operation rejection reaches the existing
operation-error publication path. A manager must not need to choose between
"complete now" and "complete after publication" to achieve that behavior.

## Counterexample

`ApplicationEventProcessor.process(AsyncPollEvent)` waits for position work,
then calls `FetchRequestManager.createFetchRequests()` inside `whenComplete`.
That returned preparation future is observed, but the outer `whenComplete`
dependent future is not.

Consider a future extension that rejects a fetch preparation in one of two ways:

- throw an exception from validation before returning the preparation future;
- return a preparation future completed exceptionally with the same exception.

The component fixture injects those alternatives at the manager entry point.
It also varies whether position work is already completed when its observer is
registered, or completes after event admission. The processor, event and network
loop are real; manager work is mocked and controlled without a broker.

Before the correction, four new cases produced **two failures and two passes**:

| Rejection shape | Positions already ready | Positions complete later |
| --- | --- | --- |
| Synchronous throw | Event remains incomplete | Event remains incomplete |
| Exceptional preparation future | Error published and one notification | Error published and one notification |

The synchronous exception is captured by CompletableFuture in the unobserved
outer dependent, not thrown back through the network loop's input handler.
Thus even an already-completed position future does not make the outer loop's
catch sufficient. This is a concrete dependency on failure delivery style that
an RM author should not have to infer from the caller's implementation.

## Small correction and its boundary

The processor's private `createFetchRequestsForAsyncPoll()` adapter converts a
synchronous throw into an exceptional preparation future, preserving its cause.
Both shapes then use the existing continuation and `AsyncPollEvent` error path:
store the error and completed flag, release reconciliation observers, and latch
the buffer notification. No new queue, phase choice, or public API is added.

The adapter is deliberately scoped to the fetch-preparation invocation inside
the async-poll continuation. It is not a universal safe-async framework and does
not make arbitrary future chains, newly added callbacks, or other RM APIs safe.
The first-stage position invocation and other event handlers retain their
existing handling. Observer exceptions, invalid/null future returns, cross-owner
mutation, and side effects performed before rejection are not repaired by this
adapter. No scheduling-thread migration or rollback guarantee is introduced.

## Verification obligations

`ConsumerAsyncPollMetadataTest.testNewFetchValidationFailureDoesNotDependOnFutureTiming`
requires the four schedules to publish the same error, complete the affected
event, invoke preparation once, and notify once. Only the manager rejection
shape/timing changes; the author does not arrange publication or wakeup.

`testFetchRejectionPreservesExistingTimeoutPolicy` adds four cases for Kafka and
JDK timeout exceptions, delivered by throw or exceptional future. The adapter
must not wrap a cause and accidentally alter existing classification: these
timeouts end preparation without publishing an application error. A JDK checked
timeout thrown directly is a defensive test seam, not a newly declared Java
throws contract. This experiment does not redesign the timeout policy or prove
that all wrapped timeout representations are equivalent.

Existing late-metadata, failed-operation continuation, reentrant preparation,
observer isolation, public-poll recovery, and wait-latching tests remain relevant
regressions. No new real-broker, cancellation, close, or performance claim follows.

## What knowledge remains with whom?

| Responsibility | Owner after this correction |
| --- | --- |
| Whether preparation is legal and why it failed | RM/domain author |
| Which operation/result the failure belongs to | RM and the registered operation path |
| Whether this fetch validation rejects by throw or future | Both are accepted by the async-poll adapter |
| Whether the position future completes inline or later | The caller boundary, verified by both timings |
| Error-state-before-notification ordering | Existing event publication/handoff implementation |
| Whether a new operation bypasses that path | Still an interface/review obligation; not globally prevented |

Verdict: the user's knowledge-burden concern is supported by a reproducible
extension hazard. The small correction removes one caller-detail dependency;
it does not demonstrate that Approach 2 is generally simpler to extend than the
original model. If every new operation needs another bespoke adapter, that cost
must be counted against Approach 2 rather than hidden in manager author guidance.

The next independent extension should test reuse of an existing contracted
result path, not introduce an arbitrary bypass merely to make the model fail.
A common operation/result boundary is justified if repeated examples show it
removes recurring caller knowledge. Its scope must be selected before broad
migration; this experiment alone does not choose a new generic abstraction.

Related records: [effect observation](kip-1371-effect-observation-validation.md)
and [contract assessment](kip-1371-approach2-contract-summary.md).

## Final validation

**1,497 tests across 31 suites passed twice** on identical final Java sources,
with zero failures/errors/skips and retries disabled. This uses the full
selection recorded in the effect-observation document, with eight new cases in
`ConsumerAsyncPollMetadataTest`. Checkstyle main/test, Spotless Java, and
SpotBugs main passed; no acceptance benchmark was run.

Focused reproduction with JDK 17 and Gradle 9.7.1:

```sh
./gradlew :clients:test --rerun \
  --tests '*ConsumerAsyncPollMetadataTest.testNewFetchValidationFailureDoesNotDependOnFutureTiming' \
  --tests '*ConsumerAsyncPollMetadataTest.testFetchRejectionPreservesExistingTimeoutPolicy' \
  :clients:spotlessJavaCheck --offline --max-workers=2 \
  -PmaxParallelForks=1 -PmaxTestRetries=0 -PtestLoggingEvents=failed
```

The first four cases expose the original extension gap when run against the
baseline production implementation; the timeout cases additionally protect the
scoped correction's classification. Reports are under
`clients/build/reports/tests/test` and `clients/build/test-results/test` and may
be overwritten by later runs.
<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
