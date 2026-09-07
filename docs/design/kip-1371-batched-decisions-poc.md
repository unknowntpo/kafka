# KIP-1371 approach 2: completion-batch decision POC

Status: local experiment, 2026-09-05. This is the admission/ordering slice of approach 2,
not a complete implementation of every proposed effect or lifecycle contract.

- Branch: `codex/kip-1371-batched-decisions-poc`.
- Base: contracts POC `e995457b05e4483aa21c57553e5bc154d613039e`.
- Worktree: sibling `kip-1371-batched-decisions-poc` under the Kafka container directory.
- The earlier POC, DOC worktree, public KIP, PRs, and benchmark results are unchanged.
- The inherited `kip-1371-contracts-poc-evidence.md` records the parent experiment, not this delta.

## Question and alternative

Can a small change to the existing loop make a new decision after the current completion batch,
without a generic manager-event/command/action framework or recursive polling?

The parent POC already keeps follow-up request admission outside network callbacks, at the next
iteration's full pass. That is a valid alternative, not a correctness defect demonstrated here.
This experiment adds one post-I/O full pass to evaluate approach 2's more explicit update-then-decision
boundary within the same `runOnce`. It does not establish that the extra pass is worth its cost.

## Exact behavior

1. Drain the existing application-command batch and perform the existing pre-I/O manager pass.
2. Run **one** `NetworkClientDelegate.poll()`. Synchronous request callbacks apply their owner updates
   and may register follow-up operations. They must not recursively poll managers or send new requests.
3. After that poll returns, if it completed requests, the thread is still running, and no application
   command is already queued, perform **one** additional full manager pass with the post-I/O time.
4. Stage the resulting requests in the delegate queue. Publish the existing aggregate application wait,
   then issue its existing latched scheduling notification if the bound moved earlier.
5. Return. The next normal network poll sends staged attempts; there is no second I/O call here.

The batch marker is a network-thread-local boolean. It is reset on each delegate poll and observes
returned client responses plus failures of queued requests, including pre-send timeout and close cleanup.
It does not allocate another future dependent, retain events, identify dependencies, or represent progress
for application wakeup. A completion can justify re-evaluation without justifying an application effect.

`NetworkClient.poll()` invokes `completeResponses()` before returning its response list. This is the
existing synchronous boundary reused here. No new global state owner or thread is introduced.

### Cutoffs and admission

- **Input cutoff:** request completions handled by the one delegate poll. It does not await other
  in-flight requests. Later responses belong to another batch.
- **Application priority:** commands already queued at the post-I/O check defer the additional pass;
  the next iteration drains them first. The queue check is not a global atomic snapshot of future
  arrivals: commands arriving after that check belong to the next iteration.
- **Decision cutoff:** one full manager pass, not a fixed-point loop. A fact first created by a later
  manager poll is not retroactively part of the completed I/O batch. Such paths need separate review.
- **Request admission:** a manager builds an attempt and records its bookkeeping, then hands it to
  the transport queue. This is **not** proof of a socket write or a broker effect.
- **Retained attempts:** requests admitted before a subsequent owner/input change keep their captured
  attempt context. This POC does not drop them or invent a new cancellation/rollback policy.
- **Close:** if close is observed at the post-I/O boundary, skip additional normal admission and leave
  shutdown work to the existing cleanup path. A close arriving after that check is not an atomic abort.

## Forces and evidence

| Force | Mechanism and test evidence | Remaining cost or limit |
| --- | --- | --- |
| Owner updates before dependent admission | Real heartbeat, coordinator and commit managers; both orders of heartbeat `NOT_COORDINATOR` and commit success. Only discovery is staged while the owner is unknown. | Does not cover cross-owner facts first produced by manager polling. |
| Independent work must not wait for unrelated futures | A completed commit registers a second commit while heartbeat remains in flight; the second commit is staged in the same iteration. | Not a general scheduler fairness proof. |
| Stale observations must not overwrite current truth | A late heartbeat invalidation retains its captured version and cannot clear rediscovered coordinator state. | Version fencing remains necessary; batching does not replace it. |
| No recursive work expansion | Even a prepared immediate response for the follow-up is not consumed in the post-I/O pass. A later iteration handles it. | Bounds pass count, not callback duration, input-batch size, or memory. |
| Publish before scheduling notification | Thread test records decision, I/O, post-I/O decision, wait publication, then wake; uses advanced MockTime. | Does not stage every application future or buffer notification. |
| Preserve input/lifecycle boundaries | Queued application input and close observed during I/O skip the extra pass. Response, timeout and disconnect markers are tested. | Not complete timeout/cancel/close compatibility proof. |
| Avoid unnecessary work | No extra pass on an idle/no-completion poll; marker resets between polls. | Completion-heavy workloads incur an extra full manager pass. No performance benefit is established. |

The heartbeat test uses the actual heartbeat request manager, request state, coordinator, commit manager,
delegate and network loop, with `MockClient`. Membership and heartbeat payload construction are controlled
test seams. Recovery is exercised through a prepared FindCoordinator response; the separate stale-version
test injects an owner rediscovery before the old response. These are component tests, not broker integration.

## Validation

Three assertions were observed failing against unchanged parent production code before implementation:

- Both callback orders expected one discovery request staged in the completion iteration; parent staged zero.
- Independent follow-up commit expected one staged request in that iteration; parent staged zero.

These distinguish admission timing, not an upstream bug or historical-release failure. All three passed
after the change. Additional tests cover bounded follow-ups, stale owner observations, idle-poll behavior,
fresh post-I/O time, scheduling publication, queued input priority, close, timeout and disconnect markers.

Final validation: **1,262 tests across 24 suites, zero failures/errors/skips, passed twice on the same
code/test tree**. The second run forced `:clients:test --rerun`; it was not an up-to-date test result.
Retries were disabled. Checkstyle (main/test), Spotless and SpotBugs main passed. SpotBugs test was
requested but **disabled by the repository's existing build configuration**, so it is not claimed as passed.
This is 9 additional test cases relative to the parent's 1,253-case selection, plus strengthened existing
assertions. The complete test report is under `clients/build/reports/tests/test/index.html` in this worktree.

Use JDK 17 and Gradle 9.7.1. The local run used the already cached distribution directly because the fresh
worktree had no ignored wrapper JAR; no dependency download was needed. With the wrapper installed, run:

```sh
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home ./gradlew :clients:test \
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
  :clients:spotlessCheck :clients:spotbugsTest \
  --offline --max-workers=2 -PmaxParallelForks=1 -PmaxTestRetries=0
```

To force a repeat, add `--rerun` immediately after `:clients:test`. Fixtures use per-test in-memory
state and MockTime; no shared broker, topic, port, or wall-clock delay is required.

## What this POC does not establish

1. **Not all effects are staged.** Commit futures can still complete inside a response callback, before
   later callbacks in the batch and before the aggregate wait is refreshed. Existing per-operation
   outcomes are preserved. A consumer-wide transaction/publication claim would therefore be false.
2. **Admission is not transmission.** Both parent and candidate normally send this follow-up on the next
   network poll. Earlier construction alone is not a measured latency improvement. Sending and receiving
   again in this iteration would introduce another input batch and is deliberately not implemented.
3. **No new global eligibility or pending-operation registry.** Manager policies, raw-delay adapters,
   deadlines, captured scopes, owner versions, buffers and existing failure handlers remain in place.
4. **Finite passes are not universal boundedness.** A blocking callback or large existing queue can still
   take unbounded or excessive time; operation admission backpressure is outside this slice.
5. **No performance or all-variant readiness claim.** The extra pass can increase CPU/allocation and
   expose additional poll side effects. Real-broker recovery, full share/Streams loop interleavings,
   metadata error delivery after async-poll admission, and full lifecycle evidence remain open.
6. **The callback contract is not mechanically enforced for arbitrary future extensions.** The selected
   paths register work without directly invoking another manager's poll. This POC does not install a
   universal phase guard that prevents a new callback from violating that convention.

The next decision is whether to retain this post-I/O pass, rotate/restructure the existing single pass,
or keep the parent next-iteration boundary. Compare actual work and latency before adopting either
same-iteration admission or a larger event/command/action framework as the KIP requirement.
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
