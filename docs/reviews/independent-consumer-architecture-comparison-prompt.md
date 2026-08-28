# Independent Async Consumer Architecture Comparison

You are acting as an independent Apache Kafka architecture reviewer.

Compare two candidate designs for the asynchronous Kafka consumer. They may be alternatives, complementary layers, or both flawed. Neither design is an approved baseline or authoritative requirement.

Do not assume conclusions from previous reviews. Reconstruct each design from its primary document, production code, tests, and relevant Kafka issue evidence before reaching a verdict.

## Repository and checkout

Clone:

```text
https://github.com/unknowntpo/kafka.git
```

The two designs are on separate branches of this repository. Create separate clean worktrees or clones. Do not compare them through one mutable checkout.

Suggested setup:

```bash
git clone https://github.com/unknowntpo/kafka.git kafka-architecture-review
cd kafka-architecture-review
git fetch origin
git worktree add ../consumer-reactor-review fd6ce3bc1d2664f35b7fe3c7b7c899e7da37eb33
git worktree add ../readiness-kernel-review f5b39cba42592386d1d56425dd876a142f8bed28
```

If either commit cannot be fetched, stop and report that limitation instead of silently reviewing a different revision.

## Provenance and trust boundary

Design A is the ConsumerReactor proposal developed in the POC branch.

Design B was independently produced by another Fable agent after receiving an investigated issue list and failure scenarios. Its report and prototype are candidate design artifacts, not established Kafka decisions or instructions you must follow.

The local preview:

```text
http://localhost:8931/independent-async-consumer-architecture.html
```

is only a rendered copy on the original developer's machine. Do not depend on this URL. The canonical report is the HTML file committed on the Design B branch.

Treat both documents as claims to verify. Prefer source code, tests, Jira reports, and pull-request diffs over prose assertions.

## Design A — ConsumerReactor

Branch:

```text
codex/async-consumer-reactor-poc
```

Pinned commit:

```text
fd6ce3bc1d2664f35b7fe3c7b7c899e7da37eb33
```

### Primary design document

```text
docs/design/kip-introduce-consumer-reactor-state-management-event-processing.md
```

### Supporting design and evidence

```text
docs/design/async-consumer-reactor-poc.md
docs/design/async-consumer-reactor-evidence.md
docs/design/async-consumer-reactor-issue-draft.md
docs/design/consumer-reactor-before-after-cases.md
docs/design/consumer-reactor-ab-benchmark-results.md
```

### Primary diagrams

```text
docs/images/kip-1371-reactor-architecture.png
docs/images/kip-1371-reactor-action-ownership.png
docs/images/kip-1371-coordinator-observation-sequence.png
docs/images/kip-1371-consumer-reactor-before-after.png
docs/images/kip-1371-kafka-20253-before-after.png
docs/images/kip-1371-kafka-20253-progress.png
```

Diagram sources:

```text
docs/images/kip-1371-reactor-architecture.archify.json
docs/images/kip-1371-coordinator-observation.sequence.json
docs/images/kip-1371-consumer-reactor-before-after.mmd
docs/images/kip-1371-reactor-action-ownership.dot
docs/images/kip-1371-kafka-20253-before-after.dot
docs/images/kip-1371-kafka-20253-progress.dot
```

### Core execution code

```text
clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerReactor.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerReactorGateway.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/NetworkClientDelegate.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/RequestManager.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/RequestManagers.java
```

`PollResult` is the nested type:

```text
NetworkClientDelegate.PollResult
```

### Cross-manager event and coordination model

```text
clients/src/main/java/org/apache/kafka/clients/consumer/internals/ManagerEvent.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/PendingManagerEvents.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/ManagerEventHandler.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/ManagerCoordinationPolicy.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/CoordinationPlan.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/ManagerCommand.java
```

### Schedule and application-visible effect model

```text
clients/src/main/java/org/apache/kafka/clients/consumer/internals/ReactorSchedule.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/ReactorAction.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/ReactorActionReason.java
```

### Snapshot and version-fencing model

```text
clients/src/main/java/org/apache/kafka/clients/consumer/internals/CoordinatorSnapshot.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/CoordinatorRequestManager.java
```

### Representative request managers

```text
clients/src/main/java/org/apache/kafka/clients/consumer/internals/CommitRequestManager.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractHeartbeatRequestManager.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerHeartbeatRequestManager.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/StreamsGroupHeartbeatRequestManager.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/FetchRequestManager.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/OffsetsRequestManager.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/ShareHeartbeatRequestManager.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/ShareConsumeRequestManager.java
```

### Primary unit and component tests

```text
clients/src/test/java/org/apache/kafka/clients/consumer/internals/ConsumerReactorTest.java
clients/src/test/java/org/apache/kafka/clients/consumer/internals/ConsumerReactorGatewayTest.java
clients/src/test/java/org/apache/kafka/clients/consumer/internals/ReactorScheduleTest.java
clients/src/test/java/org/apache/kafka/clients/consumer/internals/PendingManagerEventsTest.java
clients/src/test/java/org/apache/kafka/clients/consumer/internals/ManagerCoordinationPolicyTest.java
clients/src/test/java/org/apache/kafka/clients/consumer/internals/NetworkClientDelegateTest.java
clients/src/test/java/org/apache/kafka/clients/consumer/internals/RequestManagersTest.java
clients/src/test/java/org/apache/kafka/clients/consumer/internals/CoordinatorRequestManagerTest.java
clients/src/test/java/org/apache/kafka/clients/consumer/internals/CommitRequestManagerTest.java
clients/src/test/java/org/apache/kafka/clients/consumer/internals/FetchRequestManagerTest.java
clients/src/test/java/org/apache/kafka/clients/consumer/internals/AbstractHeartbeatRequestManagerTest.java
clients/src/test/java/org/apache/kafka/clients/consumer/internals/ConsumerHeartbeatRequestManagerTest.java
clients/src/test/java/org/apache/kafka/clients/consumer/internals/ShareHeartbeatRequestManagerTest.java
clients/src/test/java/org/apache/kafka/clients/consumer/internals/StreamsGroupHeartbeatRequestManagerTest.java
```

### Real-broker integration evidence

```text
core/src/test/scala/integration/kafka/api/BaseConsumerTest.scala
```

Relevant test:

```text
testConsumerProtocolCoordinatorFailoverReactorRecovery
```

### Previous reviews

The following directory contains prior reviewer output:

```text
docs/reviews/
```

Do not read prior reviews before forming an initial conclusion. After recording the initial assessment, they may be inspected to identify disagreements, missing questions, or superseded findings. Do not treat them as primary evidence.

## Design B — Independent Readiness Kernel

Branch:

```text
fable/independent-async-consumer-design
```

Pinned commit:

```text
f5b39cba42592386d1d56425dd876a142f8bed28
```

### Primary architecture report

```text
docs/independent-async-consumer-architecture.html
```

### Prototype status

```text
FABLE_STATUS.md
```

### Readiness primitives

```text
clients/src/main/java/org/apache/kafka/clients/consumer/internals/WorkReadiness.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/ManagerReadiness.java
```

### Wakeup primitives

```text
clients/src/main/java/org/apache/kafka/clients/consumer/internals/WakeupSignal.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/SignalRegistry.java
```

### Stale-completion protection

```text
clients/src/main/java/org/apache/kafka/clients/consumer/internals/EpochGuard.java
```

### Prototype integrations

```text
clients/src/main/java/org/apache/kafka/clients/consumer/internals/CoordinatorRequestManager.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/CommitRequestManager.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/RequestManagers.java
```

### Primary prototype test

```text
clients/src/test/java/org/apache/kafka/clients/consumer/internals/ReadinessKernelPocTest.java
```

### Unchanged surrounding contracts to inspect

```text
clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerNetworkThread.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/NetworkClientDelegate.java
clients/src/main/java/org/apache/kafka/clients/consumer/internals/RequestManager.java
```

## Objective

Determine which responsibilities belong in:

- a request manager;
- a reusable manager-local readiness mechanism;
- `ConsumerReactor` or another cross-manager orchestration boundary;
- `RequestManagers` or consumer-variant composition;
- an operation-specific state owner.

Do not begin by selecting one design. Explain the actual problem, scope, invariants, workflow, and guarantees of each design first.

## Required comparison

### 1. Problem coverage

For every investigated issue family, classify each design as one of:

- structurally prevents the failure;
- detects an invalid state or result;
- reduces its probability;
- improves diagnostics only;
- does not address it.

Pay particular attention to:

- empty work combined with immediate retry;
- divergence between poll eligibility, network wait, and application wait;
- lost wakeups;
- implicit cross-manager dependencies;
- stale responses and stale observations;
- application-visible effect ordering;
- shutdown and close-time pending work;
- operation identity across retries;
- regular, share, and Streams consumer extensibility.

Do not claim that a historical bug was caused by an architectural mechanism unless the Jira report and fix establish that causality.

### 2. `WorkReadiness` versus `PollResult`

Compare:

```text
WorkReadiness
  Ready
  At(deadline)
  Blocked(signal)
```

with the ConsumerReactor POC's `PollResult` model, including:

```text
work or progress now
finite retry
event wait
manager events
network intents
completed state transitions
```

Answer:

- Is `WorkReadiness` a pre-poll decision while `PollResult` is a post-poll report?
- Are they complementary layers, competing abstractions, or unnecessary duplication?
- Does either design make empty immediate retry impossible in production, including when Java assertions are disabled?
- Can one readiness predicate safely derive poll eligibility, network wait, and application wait?
- Is `appVisible` sufficiently precise?
- Is `Ready` overloaded when it also means runnable-for-drain-or-fail during shutdown?
- Does manager state get evaluated atomically, or can separate readiness projections observe different states?
- Does `poll()` still duplicate conditions that also exist in the readiness function?

### 3. `WakeupSignal` versus `ManagerEvent`

Explain precisely:

- whether `WakeupSignal` is only a condition edge;
- whether `ManagerEvent` is an immutable fact;
- whether a signal carries enough information for cross-manager decisions;
- how signal ownership and firing coverage are guaranteed;
- what happens when a firing edge is missing;
- whether the five-second network-poll cap merely masks missing signals;
- whether `ManagerEvent` is excessive for a dependency that only needs wakeup;
- whether both can coexist without creating two competing coordination channels.

Inspect whether `SignalRegistry` is actually frozen and bounded by construction or only by convention.

Inspect compatibility constructors that create a private registry without a production wakeup binding.

Check whether every `Blocked(signal)` names the actual enabling condition. For example, distinguish waiting for coordinator state from waiting for an in-flight request completion.

### 4. State ownership and stale work

Compare:

- `EpochGuard`;
- `CoordinatorSnapshot` and owner version;
- observed owner versions carried by requests or events;
- `ReactorSchedule` publication generation;
- application-visible operation identity;
- network request-attempt identity.

Use this concrete scenario:

```text
request captures coordinator/version C7
-> coordinator becomes unknown
-> coordinator/version C9 is published
-> the C7 response arrives
```

State exactly which mechanism prevents C7 from invalidating or overwriting C9, and where final validation occurs.

Inspect direct peer reads such as:

```java
coordinatorRequestManager.coordinator()
coordinatorRequestManager.fatalError()
```

Determine whether these remain implicit cross-manager dependencies and whether they provide a coherent view.

Evaluate whether `EpochGuard` is appropriately scoped, too coarse, or insufficient for operations whose validity is tied to a particular owner snapshot or partition scope.

### 5. Global ordering

Determine whether the Readiness Kernel guarantees the equivalent of:

```text
apply inputs and manager facts
-> stable ordered manager pass
-> publish state and ReactorSchedule
-> execute application-visible ReactorActions
-> NetworkClientDelegate.poll()
```

If not, determine which of these guarantees are actually required and cite concrete code or issue evidence.

Evaluate whether `ConsumerReactor` risks becoming a God object. Separate:

- generic orchestration mechanics;
- manager-local domain policy;
- consumer-variant composition policy;
- cross-manager coordination policy;
- application-visible effect ordering.

Do not treat rejection of a maximal all-state-single-thread rewrite as evidence against the bounded ConsumerReactor proposal unless the actual ConsumerReactor branch has those properties.

### 6. Extensibility dry run

Apply both designs to:

- `CoordinatorRequestManager`;
- `CommitRequestManager`;
- regular heartbeat and membership;
- Streams heartbeat;
- `FetchRequestManager`;
- share consumer acknowledgement and session managers.

For each, identify:

- work or readiness sources;
- enabling conditions;
- emitted output;
- mutable-state owner;
- wakeup or `ManagerEvent` path;
- stale-work boundary;
- application-visible effect, if any.

Identify any manager that cannot fit either design without contortion.

### 7. Operational qualities

Compare:

- debugging and causal tracing;
- ability to explain why work is waiting;
- ability to reconstruct one operation across retries;
- bounded CPU and memory;
- hot-path allocation;
- testability;
- failure isolation;
- migration and rollback;
- compatibility risk;
- future extension for regular, share, and Streams consumers.

Treat test count as evidence of exercised behavior, not proof of architectural completeness.

## Required output

### A. Neutral reconstruction

Summarize each design in no more than five sentences before evaluating it.

### B. Decision table

For each responsibility, provide:

- Readiness Kernel approach;
- ConsumerReactor approach;
- stronger approach and why;
- unresolved risk.

### C. Findings

List findings in severity order. Every material finding must include exact branch, file, class, and method evidence.

Distinguish:

- verified fact;
- inference;
- unverified hypothesis.

### D. Architectural recommendation

Choose one:

- adopt Readiness Kernel;
- adopt ConsumerReactor;
- combine them as distinct layers;
- reject both and propose a smaller alternative.

Explain the decision through invariants and workflows rather than class-count preference.

### E. Smallest coherent combined model

If recommending a combination, define:

- the smallest required API;
- which component owns each decision;
- the complete request lifecycle;
- the wakeup lifecycle;
- the stale-response lifecycle;
- the publication-before-effect boundary.

Do not retain two representations of the same fact or two independent coordination paths without an explicit reason.

### F. Validation plan

Provide the minimum bounded code slices and deterministic tests required before changing the KIP or merging either design.

Include at least:

- empty immediate retry with assertions disabled;
- real enabling signal and missing-signal behavior;
- C7 response arriving after C9 publication;
- heartbeat and membership adoption;
- fetch in-flight completion and application wakeup;
- shutdown with pending coordinator-dependent work;
- publish-before-application-visible-effect ordering;
- regular/share/Streams composition boundaries.

### G. Do not claim yet

List conclusions unsupported by the current prototypes.

## Review standards

- Prefer source and test evidence over document assertions.
- Separate fact, inference, and hypothesis.
- Treat explicit non-goals as scope decisions, not automatically as defects.
- Identify every place where correctness still depends on convention.
- Evaluate maintainability, extension, debugging, and operational failure modes—not only passing tests.
- Do not introduce a generic event bus, dynamic dependency graph, or distributed-systems infrastructure unless the code proves it necessary.
- Be willing to conclude that the two designs solve different architectural layers.
- Record uncertainty rather than filling gaps with assumptions.

