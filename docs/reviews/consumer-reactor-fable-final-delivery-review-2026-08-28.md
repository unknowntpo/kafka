# KIP-1371 final architecture and delivery review

Reviewed branch: `codex/async-consumer-reactor-poc`  
Reviewed commit: `35d93a0f14b5c0fa92fc9a4b71b35f1c21dcb227`  
Method: fresh independent read-only Fable review, ultra effort

## Verdict

**CONDITIONAL GO — 72% ready.**

The core architecture is coherent: request managers retain local policy and mutable state; `ConsumerReactor` owns
global ordering, deadline aggregation, and publication of reactor-owned effects. The three-way manager result is
close to the minimum useful representation. The KIP is not ready for `DISCUSS` unchanged because lifecycle and
operation semantics contain unresolved normative decisions.

## P1 findings

1. **Fatal-path narrative contradicts production control flow.** The KIP says a post-I/O fact retained after manager
   failure is drained in the next iteration. Production sets `running=false`, enters cleanup, and never runs that
   iteration. The cited test manually calls `runOnce()` again and does not prove production lifecycle behavior.
2. **Legal retry configurations lack typed semantics.** `retry.backoff.ms=0` is legal, while `RetryAfter` rejects zero
   and `Long.MAX_VALUE`. Rewriting an empty zero result to `AwaitInput` may lose a required retry.
3. **Fatal-phase cutoff is unspecified.** Later managers can still be polled and create commands after the first
   manager failure. The KIP does not define which commands, callbacks, or facts remain admissible.
4. **Bounded close has an unsafe ownership window.** Public close may return while daemon cleanup still uses metrics,
   managers, network code, or actions that the outer consumer immediately closes.
5. **Timeout/cancellation does not reach the operation owner.** A caller-side timed `Future.get` can return timeout
   while manager retries, broker effects, or later local mutations continue. The permitted late effects are not
   specified consistently.
6. **The requested vote is narrower than the normative proposal.** The three listed decisions omit public metrics,
   fatal semantics, cancellation, close behavior, operation identity, and driver boundaries.

## Evidence grades

| Issue | Grade | Review conclusion |
| --- | --- | --- |
| KAFKA-15529 | Partial | Exact cross-component publication race remains open. |
| KAFKA-17066 | Partial | Supports moving position ownership; current attribution overuses the KAFKA-17674 proof. |
| KAFKA-17674 | Verified | Captured partition scope is covered by manager and vertical tests. |
| KAFKA-18160 | Partial | Historical fix exists; exact reactor callback lifecycle proof remains. |
| KAFKA-18569 | Pending | No exact close lifecycle reproduction. |
| KAFKA-18641 | Partial | Exact position-versus-auto-commit race remains. |
| KAFKA-19357 | Pending | No exact pending-commit/coordinator close reproduction. |
| KAFKA-20253 | Partial | Manager slices exist; exact high-CPU reproduction is absent. |
| KAFKA-20397 | Partial | PR 21991 remains open; current test does not reproduce the exact check/wait gap. |
| KAFKA-20426 | Partial | Unit and mocked-schedule proof; no integrated heartbeat chain. |
| KAFKA-20854 | Verified | Real consumer/manager path proves fetch wake classification. |
| KAFKA-20970 | Pending | PR 23227 remains open; no exact end-to-end reproduction. |

## Edge-case status

| Edge case | Status |
| --- | --- |
| Zero retry and maximum sentinel | Pending normative decision |
| `AwaitInput` wake-path completeness | Partial |
| Manager/application/network deadlines | Partial |
| Atomic submit/close admission | Verified narrowly |
| Close/shared-resource ownership | Pending |
| Fatal cause and later-manager work | Pending |
| C7 to C9 coordinator fencing | Partial; owner unit proof only |
| Pre-I/O cross-owner fact | Partial containment |
| Duplicate facts and wake coalescing | Partial; per-phase only |
| Late result after timeout/cancel/replacement | Pending |
| Regular/share/Streams parity | Partial |
| Metrics overhead and public surface | Partial |
| Bounded state/history/queues | Partial; application queues remain unbounded |

## Required before `DISCUSS`

1. Align `Decision Requested` with the actual normative scope.
2. Define zero-delay and maximum-sentinel retry semantics.
3. Remove or repair the impossible next-iteration-after-fatal claim and its evidence.
4. Specify the fatal-phase cutoff.
5. Specify bounded-close resource ownership and original fatal-cause delivery.
6. Specify timeout, cancellation, and late-response behavior by operation category.
7. Add an `AwaitInput` wake-source table and variant compatibility gates.
8. Correct issue status and causality, especially KAFKA-17066, KAFKA-20397, and KAFKA-20970.
9. Rename the old code-complete baseline to a case-study evidence snapshot and identify the current reviewed HEAD.

## Safe to defer

- Exact helper and driver class names.
- Removal of compatibility constructors and migration of remaining manager families.
- Per-complete-iteration wake coalescing.
- Additional snapshot families and TRACE diagnostics.
- Stress and performance benchmarks.
- Capacity/backpressure configuration in a separate KIP.
- Exact close/cancellation implementation after the normative behavior is approved.

## Claims to avoid

- Do not claim the numeric wait field caused every cited issue.
- Do not claim typed results guarantee an enabling wake path.
- Do not claim the POC fixes all issues end to end.
- Do not claim no work is created after fatal detection or pending operations receive the original fatal cause.
- Do not claim full regular/share/Streams parity, once-per-iteration wake coalescing, or end-to-end bounded state.

## Suggested discussion framing

> KIP-1371 proposes three internal invariants without changing consumer APIs, protocols, thread topology, or callback
> thread semantics: managers explicitly distinguish immediate output, timed retry, and input-driven wait;
> cross-manager observations are routed to one version-fenced state owner; and the background reactor publishes its
> aggregate schedule before releasing application-visible effects. The POC demonstrates feasibility and directly
> validates the partition-scope and fetch-wakeup cases, while other cited issues remain regression obligations rather
> than claims of complete fixes. Feedback is especially requested on zero-delay retry semantics, fatal-phase cutoff,
> close-time resource ownership, and late completion after API timeout.
