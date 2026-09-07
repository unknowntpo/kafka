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

## Follow-up performance experiment

The user authorized performance diagnosis without first imposing the new same-round test expectations on the ablation. The production change is pinned at `4289215a07df6b4d2150d43b1025b75787860a50`, client tree `bc63ddf445fd624ea862bea9709f1c442f3bbdd2`. The control is Jenkins 930 revision `d3c6d866adbdbb15a4c2f5223707e2ba926770fc`, not the older pre-KIP baseline.

Predeclared local method: an isolated 10,000-record one-pair fixture smoke, followed by 70,000,000 records of 256 bytes, four partitions, five independent JVM pairs alternating AB/BA, Java 17, subscribed consumer protocol, and auto-commit enabled. Reuse one unchanged control broker for both variants. Finish compilation before measurement. Keep exact consumed counts, CPU/RSS, per-run throughput and median/MAD; each measured fetch should last at least 30 seconds. No JFR overhead in timed samples. This is a diagnostic comparison, not correctness acceptance or a direct comparison against Linux Jenkins percentages. Do not change the seven differing test assertions to claim acceptance.

### Completed local measurement

Both runtime exports succeeded. Runner validation: 11 tests, 10 passed and one skipped. The isolated smoke consumed exactly 10,000 records per variant and shut down successfully; its short timings are not acceptance evidence.

The five-pair run completed successfully: all ten JVMs consumed exactly 70,000,000 records, with fetch durations of 31.743–41.298 seconds. The task-owned broker was stopped and only its generated broker data was removed. No Jenkins build, push, or external publication occurred.

| Metric | Control median (MAD) | No-extra-pass median (MAD) | Ratio of medians change |
| --- | ---: | ---: | ---: |
| Fetch records/sec | 1,813,330.57 (118,333.23) | 2,068,496.79 (136,713.80) | +14.07% |
| Whole-JVM CPU seconds | 24.39 (0.55) | 22.76 (0.57) | -6.68% |
| Peak RSS bytes | 1,121,107,968 (819,200) | 1,125,908,480 (6,127,616) | +0.43% |

Do not interpret the ratio of medians alone as the causal benefit. Both variants got faster toward the end of the local run. The physical host was not exclusive, and the cause of that drift was not isolated.

| Pair | Execution order | Throughput change | Whole-JVM CPU change |
| --- | --- | ---: | ---: |
| 1 | control → no-pass | +4.95% | +0.82% |
| 2 | no-pass → control | -1.87% | +3.77% |
| 3 | control → no-pass | +20.05% | -8.74% |
| 4 | no-pass → control | +6.48% | -5.12% |
| 5 | control → no-pass | +0.40% | -0.14% |

The median paired changes are **throughput +4.95%, CPU -0.14%, peak RSS +0.82%**. This supplies a throughput improvement signal but does not establish a reliable CPU reduction or explain the earlier Jenkins CPU regression. The runner's numeric throughput gate passed; this is not a correctness or statistical equivalence verdict. No significance claim is made.

Portable receipts: `benchmarks/contract-guided/results/post-io-ablation-local/{results,summary,manifest}.json`. Full temporary raw commands/logs: `/tmp/kip1371-post-io-ablation/kip1371-throughput-eueuf5uz/`. Smoke receipts: `/tmp/kip1371-post-io-ablation/kip1371-throughput-k7lcv3s2/`.

Next: judge next-round completion against the pre-KIP contract rather than automatically enforcing the added post-pass behavior. Keep the ablation separate pending that review. A further performance confirmation should repeat matched pairs under a stable worker with reduced time drift; it requires separate Jenkins authorization if run there.
