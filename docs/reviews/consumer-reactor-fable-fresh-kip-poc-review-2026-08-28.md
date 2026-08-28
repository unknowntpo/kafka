# Fable Fresh Review: KIP-1371 and Consumer Reactor POC

## Review metadata

- Date: 2026-08-28
- Reviewed branch: `codex/async-consumer-reactor-poc`
- Reviewed commit: `631ba19176`
- KIP: `docs/design/kip-introduce-consumer-reactor-state-management-event-processing.md`
- Reviewer posture: fresh, read-only, PMC-level adversarial review; prior review conclusions were not supplied
- Main-agent verification: the P1 close/error findings, post-I/O ordering finding, `AwaitInput` safety-pass behavior,
  and benchmark broker-runtime confound were independently confirmed against the cited source files

## Verdict

**The problem statement is discussion-ready; the proposed decision is not. Status: NOT READY FOR `DISCUSS`.**

The KIP identifies a real recurring class of scheduling, publication, and cross-manager ownership failures. The
tri-state distinction between immediate work, finite retry, and input-driven wait is useful. However, the current
decision mixes abstract invariants, public metrics, an incomplete POC, nonexistent driver types, and target semantics
that current code still violates. Reviewers cannot yet tell exactly which behavior they are being asked to preserve.

No P0 was found.

## Findings

### P1 — `close(Duration)` ignores its timeout

`ConsumerReactor.closeInternal` computes `timeoutMs`, documents it as an upper bound, but calls unbounded `join()`:

- `ConsumerReactor.java:681-719`, especially `closeInternal` at lines 708-719;
- the “for up to” statement is at lines 703-704;
- the unbounded `join()` is at line 716.

This is a direct close-semantics regression risk. The existing cleanup test proves only that one already-staged
async-poll completion is executed; it does not prove bounded close or terminal completion.

**Before discussion:** fix bounded close and test it, or remove close correctness from the current evidence and
decision scope.

### P1 — manager-poll exceptions can leave operations without a terminal result

`ConsumerReactor.pollManager` catches `RuntimeException`, records a counter, and substitutes `AwaitInput` at
`ConsumerReactor.java:482-504`. No application error is published and no pending operation is completed. The outer
loop also catches and continues at lines 208-215.

A commit, fetch-position, heartbeat, or topology operation whose manager throws can remain pending until an unrelated
timeout or close. “Isolation” is not yet a defined failure policy.

**Before discussion:** decide whether an unexpected manager exception is fatal to the reactor, fatal to affected
operations, or recoverable. Implement and test exactly-one terminal outcome for that policy.

### P2 — post-I/O manager ordering is not stable composition order

Pre-I/O uses `RequestManagers.entries()`, but post-I/O completion owners are stored in an identity-backed set:

- `ConsumerReactor.java:103-108`, `affectedManagers`;
- `ConsumerReactor.java:516-536`, `pollAffectedManagers`;
- iteration of the `IdentityHashMap`-backed set occurs at lines 522-525.

When multiple managers complete in one network poll, event/action/command ordering is unspecified.

**Before discussion:** filter stable `requestManagers.entries()` against the affected identity set and add a
multi-completion ordering test.

### P2 — `AwaitInput` is semantic input-wait, not purely event-triggered polling

The POC bounds every network poll to five seconds and performs a full manager pass each iteration. `AwaitInput`
becomes `Long.MAX_VALUE`, but the global safety cap still wakes and re-polls all managers:

- `ConsumerReactor.java:73-75`, 270, 294-302;
- `NextPollCondition.java:46-61`.

**Before discussion:** distinguish “no manager deadline” from “no periodic evaluation.” Add a long-enough test that
establishes the maximum background poll rate when all managers return `AwaitInput`.

### P2 — invalid pre-I/O cross-owner facts can follow already-staged transport work

The pre-I/O pass stages network commands before evaluating the complete event batch. A cross-owner command discovered
afterward is logged and deferred, but transport work based on stale owner state may already exist.

**Before discussion:** enforce the target admission rule deterministically before staging transport work, or state
that the POC detects but does not enforce this Phase 2 invariant.

### P2 — historical A/B results do not isolate reactor causality

The benchmark uses two checkout revisions, and each broker runtime includes that checkout's `kafka-clients` jar. The
harness explicitly records but does not eliminate broker-runtime differences
(`benchmarks/consumer-reactor-ab/README.md:250-256`). The measured CPU/poll differences are observations between two
builds, not an isolated estimate of the reactor change.

**Before discussion:** put this confound beside any percentages, or keep exact percentages only in the evidence report
until one pinned broker runs both client artifacts.

### P2 — four public metrics are premature

The names and metric groups follow Kafka conventions, but semantics remain incomplete:

- `reactor-application-wakeup-total` counts only successful reactor primitive wakes, not user `Consumer.wakeup()` or
  remaining compatibility signals;
- no reason dimension exists, so expected and suspicious wakeups cannot be distinguished;
- manager-poll failure has no manager identity;
- constant-time `Sensor.record()` is not proof of negligible overhead;
- the historical benchmark predates current HEAD.

**Before discussion:** delete the wake counter until wake paths are centralized and an operational query is defined.
Treat the three failure counters independently and benchmark current HEAD before stabilizing names.

### P2 — pinned tests prove slices, not architectural necessity

The cited tests prove captured scope, no paused-partition wake, one invalidation ordering case, and one staged cleanup
action. They do not prove that the handler registry, generalized snapshots, or the complete action hierarchy are
necessary.

**Before discussion:** call this “mechanism coverage.” For each invariant, name a counterexample that a smaller local
fix cannot prevent.

### P3 — coordination policy is over-generalized

The current path maintains both `ManagerEvent.Type` and Java payload classes, performs runtime casts, and builds a
`CoordinationPlan` for only a few handlers. Prefer a direct composition-owned mapping or a smaller sealed event family.
Do not make the registry itself part of the approved invariant.

### P3 — abstraction necessity is uneven

- `PollResult`: justified as one atomic manager-output envelope.
- `NextPollCondition`: justified; its class hierarchy may be simplified.
- Owner snapshot: justified for coordinator target/version, not as a general framework.
- `ManagerEvent`: justified for true cross-owner observations; local wake facts may be unnecessary wrappers.
- `ManagerCoordinationPolicy`: not yet justified; inline or remove unless heterogeneity grows.
- `ReactorSchedule`: justified as published wait state.
- `ReactorAction`: ordering is justified, but the full hierarchy is not yet proven minimal.
- `NetworkCommand`: useful conceptual separation, but should not become a hierarchy before a second command exists.

### P3 — regular/share/Streams boundaries remain aspirational

The proposed drivers do not exist. Streams managers are still part of the regular composition, and share paths retain
compatibility results. Current evidence proves reuse of a kernel without an `isShareConsumer` branch in
`ConsumerReactor`; it does not prove consistent driver boundaries.

## KIP changes required before discussion

1. Replace `Decision Requested` with precise normative behavior: output states, pre/post-I/O ordering, `AwaitInput`
   plus the safety pass, error policy, close policy, and included effects.
2. Label every claim as target, current evidence, or compatibility behavior.
3. Qualify deterministic-order claims until post-I/O order is fixed.
4. Say the POC detects, rather than prevents, invalid pre-I/O cross-owner admission.
5. Inventory application-visible effects: futures, background events, fetch-buffer signals, callback acknowledgements,
   timeout, cancellation, interruption, and close.
6. Describe composition responsibilities without presenting nonexistent drivers as settled types.
7. Reduce public metrics and provide one operational query and exact counting boundary per retained metric.
8. Surface the benchmark broker-runtime confound or remove exact percentages from the KIP.
9. State that case studies validate mechanisms and regression slices, not architectural necessity.
10. Define mixed-path rollback and migration invariants.

## POC changes

### Required before discussion

- bound `close(Duration)` and test zero, short, normal, interrupted, and stuck-cleanup cases;
- define and implement manager-poll exception termination/propagation;
- preserve composition order in `pollAffectedManagers`;
- enforce invalid pre-I/O fact admission or narrow the proof claim;
- rerun current-HEAD metrics/performance evidence or reduce the public counters.

### Follow-ups before implementation acceptance

- remove raw-delay `PollResult` constructors and direct unsent-request compatibility access;
- complete share acknowledgement/fetch recovery tests;
- introduce explicit regular/share/Streams compositions only if they remove optional/null state;
- route remaining direct effects through publication ordering;
- prove exactly-one terminal outcome for every application-event family;
- inline `ManagerCoordinationPolicy` unless more heterogeneous policies justify it;
- coalesce wakes across a whole iteration only after latency evidence.

## Missing tests and benchmarks

- bounded `close(Duration)`;
- manager poll throws with a pending application operation;
- multiple manager completions preserve composition order;
- all managers `AwaitInput` for longer than the five-second safeguard;
- prompt wake from application command, network completion, cancellation, and shutdown;
- invalid pre-I/O fact cannot send stale transport work;
- coordinator versions `7,9,7` across multiple producers;
- regular/share/Streams broker coordinator-loss recovery;
- share fetch/ack timeout, retry, cancellation, and close;
- exact historical reproductions still marked pending by the KIP;
- current-HEAD single-broker/client-only A/B;
- throughput, allocation, reconnect, rebalance, Streams, share, and metric-overhead workloads.

## Claims the KIP must not make

- `AwaitInput` managers are never periodically polled.
- all manager/event ordering is currently deterministic.
- the POC enforces cross-owner fact admission.
- the POC proves exactly-once completion or close correctness.
- regular/share/Streams already have consistent driver boundaries.
- the reactor fixes every motivating historical issue.
- pinned tests prove architectural necessity.
- the reactor is categorically faster.
- historical CPU/poll reductions isolate only the reactor change.
- all four metrics have negligible overhead or complete wakeup coverage.

## Smallest prioritized next steps

1. Fix bounded close and manager-exception semantics.
2. Make post-I/O ordering deterministic.
3. Rewrite `Decision Requested` as precise target behavior.
4. Correct `AwaitInput`, fact-admission, boundary, and evidence claims.
5. Reduce the public metric proposal.
6. Run one current-HEAD, single-broker, client-only A/B.
7. Only then open `DISCUSS`; defer driver extraction and abstraction cleanup to implementation review.
