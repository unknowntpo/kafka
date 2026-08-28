<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Consumer Reactor POC Phase Map

This file is the implementation index for the KIP-1371 proof of concept. The KIP defines the target behavior; this
index records which production paths and tests currently support each migration phase. A phase is complete only when
its exit evidence passes through production components.

## Branch rules

- Keep `codex/async-consumer-reactor-poc` runnable after every phase commit.
- Use one commit for one phase assertion. Keep review transcripts and working notes outside this branch.
- Prefix experimental work with `EXPERIMENT:` until its behavior and exit test are accepted.
- Do not rewrite the existing branch history. Superseded experiments remain traceable through their commits.
- Record the exact evidence commit in the KIP only after the working tree is clean and the named tests pass.

## Phase status

| Phase | Behavior | Current status | Representative evidence | Remaining exit gates |
| --- | --- | --- | --- | --- |
| 1. Decision boundary | `ConsumerReactor` runs the existing background loop; managers return typed output; the reactor retains deadlines, publishes `ReactorSchedule`, then executes `ReactorAction`. | Implemented in the POC. Compatibility producers remain. | `8e76de11fd`, `57b973bc6a`, `64c2cd33e2`, `68a978bb9f`; `ConsumerReactorTest`, `ManagerPollCacheTest`, `NetworkClientDelegateTest`. | Remove raw-delay adapters only after all producers migrate. Legal `RetryAfter(0)`, maximum-delay saturation, overflow, and per-pass delivery are verified at `68a978bb9f`. |
| 1A. Application-wait separation | Legacy `maximumTimeToWait(...)` contributes an application wait independently from the manager's reactor retry condition. | Implemented as an isolated migration adapter. | `5c587c4db4`; `ApplicationWait`; `ManagerPollCacheTest.testManagerRetryAndApplicationWaitRemainIndependent`; focused reactor/cache/delegate/schedule suites. | Prove each legacy application wait has an enabling input before removing the adapter. |
| 2. Manager and ownership migration | Manager-local state decides readiness. Cross-owner observations use typed events, an owner snapshot when needed, and owner-side version fencing. | Partial. Coordinator, commit, regular/share/Streams heartbeat, topic metadata, share acknowledgement retry, and Streams topology slices exist. | `72b759fc77`, `831710fd02`, `804ca84868`, `5ab585059a`, `edd4c5b1ab`; coordinator failover and stale-observation tests. | Migrate remaining raw-delay producers, prove share fetch recovery, add each opt-in snapshot family only with a real dependency and liveness test. |
| 3. Application-effect migration | State and schedule publication precede reactor-owned completion, notification, and wakeup effects. | Partial. Async-poll completion, metadata error, fatal coordinator error, and staged close actions cross the boundary. | `857b570e21`, `ac76b08109`, `07559ffa69`, `d9fafb212d`; publish-before-action and close regression tests. | Remove remaining direct application effects, coalesce equivalent wakes across a complete iteration, and prove exactly one terminal outcome for success, failure, timeout, cancellation, interruption, and close. |
| Follow-up lifecycle work | Fatal cleanup, bounded close, and public timeout guarantees remain compatible with existing consumer behavior. | Separate follow-up; not a new KIP-1371 state model. | `fea1e3746d`, `42c7c423ba`. | Expand public-method timeout audit and integration coverage before any lifecycle guarantee changes. |

## Reviewer surface

The branch keeps only the artifacts required to inspect or reproduce the proposal:

1. **Phase 1A code slice (`5c587c4db4`)**
   - `ApplicationWait.java`
   - `ConsumerReactor.java`
   - `ManagerPollCache.java`
   - `NetworkClientDelegate.java`
   - `NextPollCondition.java`
   - the corresponding three test files
2. **Zero-delay time-driven retry (`68a978bb9f`)**
   - accepts `RetryAfter(0)` without treating it as immediate progress;
   - saturates maximum and overflowing absolute deadlines;
   - preserves separate manager-retry and compatibility application-wait projections;
   - verifies diagnostic counter values in regular and share metric groups.
3. **Proposal and implementation index**
   - `kip-introduce-consumer-reactor-state-management-event-processing.md`
   - this phase map
4. **Performance evidence**
   - `consumer-reactor-ab-benchmark-results.md`
   - `benchmarks/consumer-reactor-ab/`
5. **Static diagrams referenced by the KIP**
   - `kip-1371-reactor-architecture.png`
   - `kip-1371-coordinator-observation-sequence.png`

Superseded design drafts, generated diagram sources, interactive previews, review transcripts, and comparison prompts
remain available in Git history but are not part of the current reviewer surface.

## Current review gates

The next safe sequence is:

1. prevent a leaving heartbeat with an in-flight request from producing a second heartbeat command, with a
   deterministic regression test;
2. align the KIP with the POC lifecycle behavior, legal `RetryAfter(0)` semantics, and partial share/Streams
   migration status;
3. add positive typed-event assertions where older tests only removed direct coordinator mutation checks;
4. prove manager-event routing and deferred-command exception behavior;
5. rerun the focused correctness gates, then repeat the A/B benchmark with accurately named measurements and its
   workload limitations disclosed.

## Evidence naming

Every phase claim should use this form:

```text
Behavior -> production path -> named test -> evidence commit -> result
```

Use `Verified` only when the complete chain exists. Use `Partial` when the mechanism exists but a variant, lifecycle,
or end-to-end assertion is missing. Use `Pending` when the target behavior has not been implemented.
