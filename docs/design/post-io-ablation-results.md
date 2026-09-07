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

# Post-I/O pass ablation: local correctness diagnostic

## Scope

Base: `d3c6d866adbdbb15a4c2f5223707e2ba926770fc`, the revision measured by Jenkins 930.
Branch: `codex/kip-1371-post-io-ablation`.

The experiment removes only the extra full manager poll after a completed network batch in `ConsumerNetworkThread.runOnce()`. Response handling, owner validation, typed waits, wait publication and notification remain unchanged. No test expectations were changed. The measured worktree remains unchanged; this experiment has not been pushed or submitted to Jenkins.

## Results

Java: Temurin 17. Gradle: `:clients:test --no-daemon --max-workers=2 -PmaxParallelForks=1` with the selectors below.

The original revision passed all 157 tests in these six suites:

- ConsumerNetworkThreadTest (27)
- ConsumerBatchedDecisionTest (76)
- ConsumerPublicationContractTest (4)
- ConsumerAdmissionContractTest (4)
- ConsumerOperationResultContractTest (38)
- FetchBufferProducerTest (8)

The initial full ablation run stalled in `testOperationAfterConsumedDiscoveryErrorUsesOwnTimeoutOrClose`, at an unbounded future join. The fixture assumes same-round completion for its post-pass case and does not drive the additional iteration needed after removal. This is not evidence of a production deadlock. Only the experiment's test JVM was terminated; the thread dump was retained.

A completed diagnostic run selected the other five full suites plus four methods from ConsumerBatchedDecisionTest:

- testPublicCommitSyncObservesDecisionBoundaryWinner
- testPublicCommitAsyncErrorAudienceAndRecovery
- testDecisionBoundaryChangesTimeoutWinner
- testExtraPassChangesAdmissionButNotFollowupTransportPoll

Result: **91 tests, 84 passed, 7 failed**.

| Failure category | Count | Observation |
| --- | ---: | --- |
| Poll/staging expectations | 4 | The extra decision pass or same-round request staging no longer occurs. |
| Terminal error winner | 2 | Public commitSync and the event-level deadline test expected GroupAuthorizationException but observed TimeoutException. |
| Async commit error audience | 1 | A later commit received the discovery error that the extra pass would already have consumed before its admission. |

These failures are not seven independent production bugs. They demonstrate that removing the pass changes the behavior encoded by the current tests, including public operation results under controlled schedules. Passing publication and operation-result suites does not establish full equivalence.

## Interpretation and next step

No CPU or throughput measurement of this ablation has been performed. Jenkins 930's throughput -4.36% and whole-JVM CPU +5.67% remain observations about the original candidate, not evidence attributing cost to this pass.

A safe next investigation should separate response/error settlement from new request admission. First specify which pending operations must observe a completed discovery failure, and preserve their terminal-outcome ordering. Then investigate whether follow-up request construction can wait until the ordinary next pass without changing those contracts. Do not merely rewrite expected failures or remove correctness checks to obtain a faster candidate.

Local receipts (temporary, not portable): `/tmp/kip1371-post-io-ablation/control.log`, `ablation.log`, `selected-ablation.log`, and `stalled-thread.txt`.
