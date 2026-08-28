# KIP-1371 Iterative Fable Review — 2026-08-28

## Review scope

Fable reviewed the current dirty worktree as a new Kafka maintainer who had not read the POC. The authoritative draft
was `docs/design/kip-introduce-consumer-reactor-state-management-event-processing.md`; the review standard was
`docs/design/kafka-kip-writing-and-review-standard.md`. The review was read-only.

## Round 1 — NO-GO

No P0 was found. The review identified five P1 groups:

1. The fatal-path narrative promised a later iteration after the production loop had already stopped.
2. Legal zero and maximum retry delays had no typed semantics.
3. Close, fatal-cause propagation, timeout, and cancellation experiments exceeded the stated KIP decision.
4. `AwaitInput` named a cause but did not require proof that an enabling input reactivated the manager.
5. Evidence mixed an immutable snapshot, a later commit, and dirty working-tree behavior.

The review also found ambiguity between one scheduling publication and two timing projections, combined historical
causality for KAFKA-17066/KAFKA-17674, overstated metric tests, and repeated FAQ/implementation-diary material.

## Applied corrections

- Limited the community decision to typed manager output, single-owner cross-manager changes,
  publish-before-effect, and four diagnostic counters.
- Restored existing public lifecycle semantics as the compatibility baseline. Fatal cleanup and lifecycle semantic
  changes require a separate design.
- Removed the impossible post-fatal “next iteration drains the fact” claim.
- Defined `RetryAfter(0)` as a time-driven next-pass retry and `Long.MAX_VALUE` with saturating deadline arithmetic.
- Added fairness and boundary-value assertions without claiming that an explicitly configured zero backoff cannot
  produce consecutive zero-delay iterations.
- Added an `AwaitInput` wake-source table and required deterministic production-path liveness tests.
- Separated the reactor deadline from the legacy application-wait projection inside one immutable scheduling
  publication.
- Classified `ApplicationWait` as an uncommitted working-tree experiment.
- Pinned evidence by revision and downgraded missing execution records.
- Split KAFKA-17066 ownership evidence from the KAFKA-17674 captured-scope failure; marked PR 21991 and PR 23227 as
  unmerged.
- Narrowed metric evidence to registration/removal in both groups plus recording call-path coverage.
- Removed duplicated FAQ and class-path implementation diary material.

## Round 2 — CONDITIONAL GO

Fable confirmed that `AwaitInput` liveness and evidence classification were resolved. It requested three final
clarifications:

1. Phase 3 close handling must preserve existing terminal outcomes rather than define new lifecycle semantics.
2. The no-spin assertion must exclude explicit `RetryAfter(0)` and apply only to input-blocked results and invalid
   empty `PollImmediately` results.
3. The KIP must distinguish one atomic schedule generation per publication phase from multiple generations in one
   reactor iteration.

It also requested a precise KAFKA-15529 description and a shorter Summary.

## Final corrections

- Defined Phase 3 close draining solely as preservation of existing terminal outcomes.
- Narrowed the no-spin assertion and retained a separate zero-retry fairness test.
- Defined one atomic `ReactorSchedule` generation per publication phase and allowed a newer post-I/O generation in
  the same iteration.
- Changed KAFKA-15529 to background fetch observation.
- Reduced the Summary while preserving problem, proposal, scope, and decision surface.

## Final verdict — GO

Fable reported no remaining P0 or P1 blocker. It confirmed:

- lifecycle wording consistently preserves current public behavior and leaves fatal cleanup changes to follow-up;
- zero and maximum retry semantics no longer contradict no-spin claims;
- publication-phase generations and iteration-level republishing are distinct;
- KAFKA-15529 and the open/merged PR statuses are precise.

The reviewer did not run tests or modify files. A second Claude CLI cross-check was unavailable because that local
CLI was not authenticated; no credential or configuration was changed.
