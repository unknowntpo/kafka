# KIP-1371 Approach 2: contract assessment

## Verdict and scope

Approach 2 has partial feasibility evidence: bounded loop ordering, narrow
capabilities, and owner-local checks can supply useful guarantees without a
generic event/command/action framework. It does not establish that every manager
can evolve independently or that a broader publication boundary is unnecessary.

Assessment date: 2026-09-06. Scope is the local
`codex/kip-1371-batched-decisions-poc` worktree, based on
`6647c7405417c61fbdb415dc1ed1e8d926e588d4` plus the commit-failure
characterization, normal-admission-entry experiment, and late metadata delivery
follow-up described below. This is a local review
summary, not a replacement published KIP or a production-readiness declaration.
The parent POC, separate DOC worktree, public KIP, and benchmarks are unchanged.

Local review sequence:

1. `7c74fdd93e`: centralize normal commit admission; characterize retries and
   the distinction between operation completion and batch-wide observation.
2. `24b3e3fa8c`: deliver late metadata errors to pending async polls, prevent
   late failed-operation continuations, and verify publication/wait routing.

The combined final Java sources passed 1,462 tests across 30 suites twice,
with zero failures, errors, or skips and retries disabled. These commits have
not been pushed as part of this assessment.

## Intent and competing forces

Intent: when extending an existing RM or adding a new one, an author should
reason primarily about its domain rules; shared infrastructure should make
cross-manager ordering and application observation obligations explicit.

- **Developer-local reasoning versus cross-owner dependencies:** an RM should
  not reconstruct other managers' timing or publication order. It must still
  understand domain dependencies such as membership or coordinator availability.
- **Stronger shared guarantees versus implementation cost:** a uniform phase or
  effect boundary can reduce conventions, but introduces routing, retention,
  lifecycle obligations, and migration work.
- **Fresh decisions versus stable admitted attempts:** later owner changes must
  inform new decisions, without silently rewriting or discarding existing work.
- **Prompt application progress versus excessive wakeups/work:** notifications
  must not be lost, but every completion does not justify an application wake
  or prove a need for an additional full manager pass.

These are requirements and tensions from the design discussion and local
experiments, not a claim that the larger design or the smaller one has won.

## Contract ledger

“Mechanism” below means enforced on the named implemented path, not universally
enforced across all managers, helpers, lifecycle paths, or future extensions.

| Obligation | Mechanism and current evidence | RM/owner responsibility | Remaining limitation |
| --- | --- | --- | --- |
| Distinguish useful activation from a numeric delay | Typed `PollResult` rejects empty immediate output; coordinator and blocked-commit slices have tests. | Derive work eligibility and the next useful activation from domain state. | Raw-delay adapters remain; the type does not prove that an enabling input will arrive and wake the loop. |
| Decide after the completed I/O batch | The loop performs at most one additional full pass after its one I/O poll, subject to queued-input and close checks. | Callbacks update owned state or queue operations without recursive admission. | Arbitrary callback bypasses are not mechanically prevented; updates first produced during polling are not retroactively included in that batch. |
| Preserve operation scope and owner identity | Captured coordinator versions and owner-side validation reject stale invalidation on tested paths; admitted request context is retained. | Define which changes invalidate which operations, and when retry has authority to proceed. | Fresh epoch alone is not proof of retry authority; no general owner/operation framework has been proven. |
| Bind local admission checks to request construction | Commit initial attempts and retries use `tryAdmit`: check, reserve, then build; in-flight/backoff cases are tested. | Supply complete domain rules; surrounding poll/drain handles coordinator and expiration. | Close retains an explicit exception. The enclosing class can still call the private builder; this is a smaller review surface, not universal compile-time enforcement. |
| Protect local handoff and completion authority | Fetch producer access omits raw buffer/unconditional wake; dependency tests guard the hierarchy. Preparation callers receive separate observer futures. | Report genuine state transitions and correctly route completion identities. | Not migrated to share fetch; node bookkeeping is not generation fencing; observer cancellation does not cancel shared work. |
| Publish aggregate timing and invalidate an obsolete wait | The loop publishes the earlier aggregate deadline before a latched scheduling notification; both wait-entry orders are tested. | Provide the correct local timing contribution; local handoff maintains its own state-before-notification rule. | A local wake/future may precede the new aggregate. There is no all-effects barrier or once-per-iteration wake coalescing. |
| Deliver each relevant result/error through lifecycle changes | Late metadata-error delivery to pending async polls, public poll routing, reentrant preparation, and wait notification have component tests. | Preserve the affected operation's result, scope, and error identity. | Real wire-error/recovery, complete cancellation/close, and all-variant recovery remain open. |

The four original concerns remain useful review obligations: activation,
ownership, application observation, and publication/effect order. They need not
map to four new abstractions. In particular, local state-before-notification and
aggregate-wait invalidation must not be advertised as a global
schedule-before-every-effect guarantee.

## Alternatives and the resulting decision

| Option | What it offers | What still needs justification |
| --- | --- | --- |
| Parent next-iteration admission plus narrow contracts | Reuses the normal next full pass after callbacks; avoids the extra post-I/O pass. | Complete application observation/delivery contracts and all enabling-input paths. |
| Approach 2 plus narrow contracts | Makes a bounded post-completion decision point explicit; selected component paths are implemented. | Extra polling cost, side effects, and benefit: earlier construction is not earlier network transmission. |
| Explicit cross-manager facts/commands and staged effects | Could make a stronger uniform boundary structurally visible. | Must prove migration completeness, terminal outcomes, retention costs, and that the stronger guarantee is actually required. |

The reusable forms supported by the experiments are restricted capabilities,
separation of observation from completion authority, bounded decision batches,
and publication followed by a latched notification. They complement each other:
restricting a local capability does not aggregate timing, and aggregating timing
does not validate an operation's domain authority. Each narrows a different
failure surface; none replaces the others.

## Stop condition and next decision

Stop expanding the commit/timeout investigation for now. It has exposed the
architectural distinction needed here: routing through a common entry does not
make domain predicates complete. Further commit details are not required to
justify this distinction, and no commit semantic change is selected.

The late-error gate now has a narrow correction and component/public-poll wiring
evidence: retain each live metadata-dependent async poll, route a later delegate
error, and prevent its late continuation from starting another stage. See the
[delivery follow-up](kip-1371-async-poll-metadata-delivery.md). This does not replace
real-broker verification or prove global publication ordering.

The next author decision is the promised completion scope: operation-local
outcome with a separate shared scheduling notification, or batch-wide
publication before application effects. Both callback-order characterizations
confirm that the current POC supplies the former, not the latter. They do not
demonstrate a public API bug requiring the latter. Do not silently migrate to a
stronger effect contract or claim it without implementing it. Complete the
selected lifecycle/variant gates after this contract is chosen.

Separately, adopting the extra post-I/O pass requires a pinned comparison with
the parent boundary. Historical benchmarks do not answer that question. No new
benchmark submission or performance claim is part of this assessment.

## Evidence and validation boundary

- [Actor/Future lessons](kip-1371-actor-future-lessons.md): distinguishes an
  operation-result handle from a state/behavior boundary; borrowing actor
  capabilities does not select a mailbox, runtime, or global publication rule.
- [RM boundary dry run](kip-1371-rm-boundary-dry-run.md): contrasts preserving
  the current manager order with a candidate shared protocol decision entry.
  Fifteen existing cases were rerun; timeout-first admission remains a policy
  decision, not a semantics-preserving manager merge or a new implementation.
- [Operation-result contract reuse](kip-1371-operation-result-contract-reuse.md):
  three existing fetch/commit input routes handle 38 result and observer-lifecycle
  schedules without another production adapter; this is not a cross-owner or
  global effect-ordering proof.
- [RM extension contract probe](kip-1371-rm-extension-contract-probe.md):
  a synchronous preparation rejection could disappear in a future continuation;
  a scoped adapter removes that caller-detail dependency, not all author burden.
- [Effect-specific observation validation](kip-1371-effect-observation-validation.md):
  per-effect prerequisites, pre-aggregate error observation, and public-poll
  error/wakeup/recovery schedules; no new production barrier is introduced.
- [React/Vue publication lessons](kip-1371-react-vue-publication-lessons.md):
  subsequent discussion distinguishes per-effect observation requirements from
  a global batch barrier; selective staging remains an unselected alternative.
- [Parent contract evidence](kip-1371-contracts-poc-evidence.md): inherited
  activation, captured ownership, and selected publication results.
- [Bounded completion-batch experiment](kip-1371-batched-decisions-poc.md):
  exact cutoffs, alternatives, and the earlier 1,262-case validation selection.
- [Capability and admission experiments](kip-1371-fetch-capability-poc.md):
  negative controls, extension hazards, commit characterization, and limits.

The admission-only follow-up recorded 970 tests across 18 suites passed twice.
The delivery follow-up records the latest combined selection and its result;
earlier counts belong to different selections and revisions and are not additive.
This summary does not promote historical or component evidence to end-to-end proof.
