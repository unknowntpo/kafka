# Approach 2: reusing operation-result contracts

Local component experiment, 2026-09-06, against production revision
`589235f030`. This follow-up adds tests only; it adds no production adapter,
queue, effect stage, or public API. It tests an extension seam, not a historical
issue reproduction or a new benchmark.

## Question and result

Does an RM author need to choose when an operation result becomes observable,
or can the existing caller boundary handle successful and rejected results?

For three existing input routes, the result path can be reused without another
production correction:

| Input route | Manager operation | Result being observed |
| --- | --- | --- |
| `CreateFetchRequestsEvent` | `FetchRequestManager.createFetchRequests()` | Request preparation finishes; not transmission or record delivery. |
| `AsyncCommitEvent` | `CommitRequestManager.commitAsync(...)` | The manager's commit-operation future result, forwarded to the event future. |
| `SyncCommitEvent` | `CommitRequestManager.commitSync(...)` | The manager's commit-operation future result, subject to the event observer's deadline. |

The experiment supplies controlled manager outcomes. It does not establish when
a real commit manager should report broker success, nor does the internal event
future stand in for the public `OffsetCommitCallback` execution contract.

## Mechanism, not a new timing choice

The real `ApplicationEventProcessor` forwards each manager future's outcome to
the corresponding event future. A synchronous fetch-entry exception reaches the
real loop's input exception handler; the commit handlers catch an invocation
exception themselves. An already-completed or later-completed manager future
uses the same result forwarding. These paths preserve the injected rejection
identity and complete the event observer once without an RM-selected effect phase.

This differs from the preceding [extension probe](kip-1371-rm-extension-contract-probe.md):
`AsyncPollEvent` invoked fetch preparation inside a future continuation whose
dependent result was ignored. A synchronous throw there could be captured by
that ignored dependent. The direct fetch input tested here already has a catch
boundary that receives the throw. The difference is exception routing, not a
reason to invent stronger or weaker meanings for the same preparation result.

The existing `CompletableEventReaper` retires expired or cleaned-up event
observers. Later owner results do not overwrite their terminal outcome.
Retiring an observer does not cancel the owner's work or roll back a commit.
Async commit keeps its existing unbounded event deadline; this experiment does
not introduce a finite public async-commit timeout.

## Executable obligations

`ConsumerOperationResultContractTest` uses a real input loop, event processor,
network delegate with `MockClient`, and event reaper. RM operations are mocked
extension seams. Each case has its own fixture, controlled futures and
`MockTime`; it needs no broker, real sleeps, or background consumer thread.

| Cases | Schedules checked |
| --- | --- |
| 15 | Three routes: synchronous rejection, exceptional future ready/later, successful future ready/later. |
| 6 | Cancelling an admitted event observer, followed by owner success/failure. |
| 4 | Fetch/sync-commit observer deadline expiry, followed by owner success/failure. |
| 6 | Loop cleanup of admitted observers, followed by owner success/failure. |
| 3 | Cleanup of queued inputs without invoking the manager operation. |
| 1 | Async commit remains pending rather than acquiring an invented finite deadline. |
| 3 | A completion callback queues a separate operation; that operation is invoked only when the loop processes the input. |

The last cases characterize the supported enqueue path. They do not prevent an
arbitrary callback from directly invoking another RM. Cancellation here means
`event.future().cancel(false)`, not public `Consumer.wakeup()`. Cleanup here is
the component cleanup path with empty mocked close polls, not a proof of full
public close, fatal cleanup, or resource recovery under real in-flight requests.

## What this says about programmer burden

An extension that reports its domain outcome through these existing result
paths need not decide "complete now or after the aggregate schedule." The
caller already handles the tested result shapes and observer lifecycle.

The remaining responsibility is to bind an operation to the correct result
path and preserve its domain prerequisites. New future chains still need error
routing review; raw mutable futures and arbitrary callback side effects are
not mechanically excluded. Cross-owner state updates, fetch-buffer signalling,
metadata-error delivery, and aggregate wait invalidation have different
observation prerequisites and are not proven by these tests.

Therefore the evidence supports reusing existing operation-result boundaries,
not introducing another adapter for every operation. It does not establish that
Approach 2 is globally simpler or that a shared publication rule is unnecessary.
The next architectural comparison should test an actual cross-owner or
application-notification extension against the narrow capabilities already
available, rather than add more variants of the same local future result.

## Validation

The focused class contains 38 cases, all passing with retries disabled.
The identical final Java sources passed **1,535 tests across 32 suites twice**,
with zero failures, errors, or skips. Checkstyle main/test, Spotless Java, and
SpotBugs main checks passed or remained up to date; no performance test was run.
Expanded regression uses the selection in
[effect-specific observation validation](kip-1371-effect-observation-validation.md),
adding `--tests '*ConsumerOperationResultContractTest'`. This adds 38 cases to
the preceding 1,497-case selection; counts from the two documents are not additive.

Focused reproduction with JDK 17 and Gradle 9.7.1:

```sh
./gradlew :clients:test --rerun \
  --tests '*ConsumerOperationResultContractTest' \
  :clients:spotlessJavaCheck --offline --max-workers=2 \
  -PmaxParallelForks=1 -PmaxTestRetries=0 -PtestLoggingEvents=failed
```

Reports are under `clients/build/reports/tests/test` and
`clients/build/test-results/test`; later runs can overwrite them. No remote
branch, published KIP, separate DOC worktree, or benchmark was changed.
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
