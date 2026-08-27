<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Consumer Reactor A/B benchmark results

> **Preliminary historical evidence.** These results cover proposal revision `032899a6ab`, not the current POC HEAD.
> They validate only the scenarios listed below and are not Phase 3 acceptance evidence. Before migration acceptance,
> rerun against the candidate HEAD and add the KIP-required saturated-throughput, allocation-per-record, and
> reconnect-recovery gates.

## Summary

The formal Jenkins A/B run compares the current async consumer baseline with the Consumer Reactor proposal under byte-identical benchmark inputs. Both builds and all five repetitions completed successfully.

The proposal significantly reduced idle CPU usage and network-poll frequency. First-record latency did not show a statistically significant regression. The p50 and p95 point estimates increased slightly, while p99 improved; all latency changes remained within the predefined 10% relative and 10 ms absolute p99 gate.

These results support the narrower conclusion that the current proposal removes unnecessary idle polling without a demonstrated first-record latency regression. They do not establish a statistically significant latency improvement.

## Compared revisions

| Variant | Revision | Jenkins build | Result |
| --- | --- | --- | --- |
| Baseline | `9d940a6537c65357684d63d6defc573807c8831b` | [#910](https://jenkins.opensource4you.tw/job/kafka-e2e/910/) | SUCCESS |
| Proposal | `032899a6ab0acbd94a28d3583a46aa1c7a244e8d` | [#912](https://jenkins.opensource4you.tw/job/kafka-e2e/912/) | SUCCESS |

Build #911 is excluded because it stopped at Spotless import-order validation before executing the benchmark. Revision `032899a6ab0acbd94a28d3583a46aa1c7a244e8d` contains only that test import-order correction relative to the submitted proposal.

## Methodology

Both variants ran `ConsumerReactorABTest.test_current_revision` with the `formal` profile and the same workload:

- 5 repetitions
- 60-second idle observation
- 10 first-record warmups
- 100 measured first-record samples
- 1,000 ms idle application poll timeout
- 30,000 ms first-record poll timeout
- combined KRaft metadata quorum

The only benchmark parameter that differed was `reactor_ab_variant`: `pre-refactor-async-baseline` for #910 and `proposal` for #912.

Both builds ran on `jenkins-worker-2`, Java `17.0.20`, the same Linux kernel, and broker configuration hash `87372b8c2bd85...`. Each build produced 10 result rows and 10 raw logs. Ducktape reported one passing and zero failing tests for each variant. The benchmark input files were byte-identical.

## Results

Values below are medians across the five repetitions. Exact two-sided Mann-Whitney p-values compare the per-repetition samples.

| Metric | Baseline | Proposal | Change | p-value | Interpretation |
| --- | ---: | ---: | ---: | ---: | --- |
| Idle CPU | 2.450000% | 1.433333% | -41.50% | 0.0079365 | Significant reduction |
| Idle application poll returns/s | 0.200000 | 0.116667 | -41.67% | 0.9524 | Not significant |
| Idle network polls/s | 15.440509 | 6.143074 | -60.21% | 0.0079365 | Significant reduction |
| Mean network poll duration | 64.765 ms | 162.785 ms | +151.35% | 0.0079365 | Reactor sleeps longer while idle |
| First-record CPU | 2.331970% | 2.134482% | -8.47% | 0.15079 | Not significant |
| First-record latency p50 | 10.490 ms | 11.154 ms | +6.33% | 0.22222 | Not significant |
| First-record latency p95 | 12.488 ms | 13.029 ms | +4.33% | 0.22222 | Not significant |
| First-record latency p99 | 15.321 ms | 14.631 ms | -4.50% | 0.54762 | Not significant |

The p99 regression check used `--relative-threshold 10 --absolute-threshold 10` and passed. The comparison script's self-test also passed.

## Reproducibility and artifacts

- Baseline artifacts: [results.zip](https://jenkins.opensource4you.tw/job/kafka-e2e/910/artifact/results.zip)
- Proposal artifacts: [results.zip](https://jenkins.opensource4you.tw/job/kafka-e2e/912/artifact/results.zip)
- Proposal console: [build #912 console](https://jenkins.opensource4you.tw/job/kafka-e2e/912/console)

The raw logs contain no benchmark errors. Both variants emit the known missing-SLF4J-binding warning, which is unrelated to the comparison.

## Limitations and next runs

This run measures one idle profile and one first-record profile on a single Jenkins worker. It does not yet cover sustained fetch throughput, rebalance churn, share-group acquisition and acknowledgement, or coordinator-loss recovery. Follow-up runs should keep the same paired-revision method and add those workloads before making broader performance claims.
