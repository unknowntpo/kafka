# Contract-Guided Coordination: close and broker-restart evidence

## Scope

Code baseline: `6c15425d87`, plus the integration test introduced with this record.
This extends the previous controlled public-close component proof with a real
consumer background thread, network client and test-owned three-broker KRaft
cluster. No externally configured broker, Jenkins job, upstream branch or
Confluence page is used or changed.

Test: `PlaintextConsumerCommitTest.testAsyncCloseJoinsBackgroundThreadAndOffsetsSurviveBrokerRestart`
under `clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/`.

## Observable assertions

1. Produce 100 records to one partition. The consumer protocol and auto-commit
   are enabled, with a very long periodic commit interval and max.poll.records=10.
2. Subscribe and receive exactly offsets 0 through 49 through the real public
   poll path. Verify position 50. The test treats these returned records as
   processed before close; it does not model asynchronous application processing.
3. Identify the newly created consumer background Thread object, excluding
   threads that existed before constructing this consumer. Call public close
   with a 30-second budget and verify that exact thread is no longer alive when
   close returns. There is no mocked input bridge or manual runOnce invocation.
4. Without an explicit commit or seek, query the broker through Admin and verify
   committed offset 50. Close alone returning successfully is not used as proof
   that its best-effort auto-commit succeeded.
5. Shut down all test-owned brokers, restart them with their existing storage,
   and wait for broker readiness. A new consumer in the same group must read
   committed offset 50, then receive exactly offsets 50 through 99.

The test fixture creates a fresh cluster/topics for every invocation and owns
their teardown. Consumers and Admin are closed with structured resource cleanup.
Stable topic/group names are scoped to this recreated cluster. It uses state
predicates rather than sleeps for broker readiness and record delivery.

## What this adds, and what remains open

This is real normal-close thread-termination evidence and persistence/recovery
across a graceful full broker restart. It supplements the earlier component
checks of commit-before-stop-discovery and assignment-before-leave completion;
it does not replace their controlled ordering assertions.

It is **not** a process kill, power-loss/fsync guarantee, or a proof that every
rebalance retry captures offsets safely. It does not test the opt-in retained
snapshot strategy, introduce new production code, or establish the exact
historical KAFKA-18160/19357/18569 failure/repaired baselines. Fault-time shutdown,
interrupted callback acknowledgements, coordinator-loss schedules and
all applicable consumer variants remain separate gates. No historical issue is
declared fully closed by this successful happy-path integration test.

The resumed consumer checks no skip/replay for this successful committed-prefix
scenario, not a stronger global exactly-once claim. At-least-once safety under
arbitrary application processing or crash schedules cannot be inferred from it.

## Validation receipt

The new integration test passed in two independent invocations, each with a
fresh test cluster. The second invocation also passed the existing
`testAsyncConsumerAutoCommitOnClose` and
`testAsyncConsumerAutoCommitOnCloseAfterWakeup` regressions: **3 tests, zero
failures/errors/skips**. Retries were disabled. The first invocation contained
only the new test; the two receipts are not reported as four distinct tests.

JDK 17, Gradle 9.7.1 offline, `:clients:clients-integration-tests:test --rerun`,
two workers, one test fork, `-PmaxTestRetries=0`. Module Checkstyle and Spotless
passed. Reports are under
`clients/clients-integration-tests/build/test-results/test/` and
`clients/clients-integration-tests/build/reports/tests/test/`.
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
