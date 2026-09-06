# Approach 2: candidate coordination and observation semantics

Status: working draft, not a published KIP or proof of complete migration.
Code baseline: `8ab58b303c`, plus the public-close/publication tests introduced
with this document. Scope: the independent Approach 2 worktree. Approach 3,
the separate DOC branch, remote PRs and Confluence remain unchanged.

## Intent, forces and choice

Intent: an RM author concentrates on its domain rules while shared boundaries
make cross-owner communication and application observation obligations explicit.
Fewer framework types must not mean fewer correctness obligations.

The competing forces are local reasoning versus cross-owner dependencies,
compatibility versus stronger universal guarantees, fresh offsets versus safe
capture, and timely completion versus redundant polling/notification. Existing
callback code and close ordering are observed constraints; a universal need for
every effect to await a full-manager pass is not established by current evidence.

The alternatives remain: retain the existing loop with documented contracts;
use narrow owner capabilities and effect-specific synchronization (Approach 2);
or migrate all cross-manager facts and application effects behind a uniform
boundary (Approach 3). The candidate below selects the second as the POC's
working direction, not as a proven replacement for every original KIP claim.

Reusable forms: restricted owner capabilities preserve mutation authority;
captured operation context fences obsolete work; synchronized handoff makes
state readable; latched notification preserves enabling changes across wait
entry. Each addresses a different failure surface. None replaces the others.

## S1. Owner-authorized state transitions

Guarantee: each managed mutable domain has an identified owner. Cross-owner
observations are validated by that owner against the affected operation's scope
and, where relevant, captured identity/version before mutation is applied.
Request construction must use the admission rules for that operation and phase.

Non-guarantees: a single thread, immutable map, or current member epoch alone
does not authorize an old response. Not every state needs a version, nor does
this require routing every local callback through another queue.

Kafka evidence: CoordinatorAccess and stale-coordinator invalidation tests;
captured position-operation scope. Remaining gaps include complete assignment
and lifecycle authority across variants. Snapshot retention alone does not close
those gaps.

## S2. Safe application observation and handoff

This refines the earlier label "Published application view" to avoid implying
that every observable value must be part of one immutable global snapshot.

Guarantee: each cross-thread value has a defined synchronized observation or
handoff path. Fields requiring a coherent view must be observed coherently.
For records and offsets, define the safe capture/delivery point independently
from the buffer's thread safety. Preserve existing application-owned state;
do not infer that every mutation must be a reactor command.

Non-guarantees: immutable does not mean captured at a safe time; buffer insertion
does not mean records have been returned or processed by the application.

Kafka evidence: FetchBuffer synchronization, position-before-consumed-marker
regression, and public-poll capture checkpoint tests. Rebalance retry capture
and real-broker crash/restart safety remain open. Retained-first-snapshot retry
is an opt-in comparison, not the selected production offset policy.

## S3. Useful next activation and enabling-input delivery

Guarantee: a migrated RM derives work eligibility and its next useful poll from
its owned state. Distinguish immediate productive continuation, time-driven
retry, and input-driven waiting. The loop combines timing contributions without
reimplementing domain policy. Each input capable of unblocking work must reach
the relevant decision path; an earlier published aggregate wait must invalidate
an obsolete longer application wait through its scheduling notification.

Non-guarantees: a typed wait alone proves neither liveness nor absence of spin.
It does not imply a dependency graph, targeted subscription framework, or
repeated polling to a fixed point. Raw-delay compatibility paths remain partial.

Kafka evidence: blocked coordinator/commit activation and scheduling-latch tests.
All applicable regular/share/Streams wait-and-recovery paths still need coverage.

## S4. Required state before its effect

Guarantee: before releasing a result, error, or notification, make the state
needed to interpret that particular effect observable through S2's mechanism.
Wait-entry races must retain the enabling condition or notification. Do not
convert a no-progress result into an unconditional application wake.

S2 and S4 are independent: a synchronized buffer can be used with a wrongly
ordered wake; a correctly ordered write and wake can still lack safe cross-thread
visibility. S2 defines the observation path, S4 its prerequisite ordering.

Kafka examples: store the poll error before its wake; clear membership assignment
before exposing the corresponding leave completion; publish an earlier aggregate
wait before its scheduling wake. Public error delivery now exercises the real
FetchBuffer latch with the real loop/delegate error route.

Non-guarantees: operation completion does not promise every RM has re-polled or
that all future owner state is frozen. The current POC does not implement
schedule-before-every-effect. If a new effect depends on the aggregate schedule,
it must explicitly cross that boundary; authors cannot simply assume it does.

## S5. Operation-specific lifecycle and terminal outcomes

Guarantee: distinguish retry deadline, application waiting deadline and close
budget. State which one prevents another attempt, ends an observer's wait, or
terminates the underlying operation. A selected terminal future outcome cannot
be overwritten. Observer cancellation must not implicitly cancel shared work.
Completion means the named operation's outcome, not batch-wide quiescence.

Kafka examples: a rebalance retry deadline prevents retry after a late retriable
failure but does not itself reject a successful response. Public close performs
its synchronous auto-commit attempt before stopping discovery and leaving the
group, then passes the remaining budget to background shutdown. Close is not a
universal prohibition on processing responses or draining commits.

Non-guarantees: a local timeout cannot prove a sent request had no broker effect.
Successful public close does not prove all offsets committed: existing close
auto-commit errors are logged rather than propagated. Full callback-acknowledgement,
transport shutdown and historical close regressions remain separate gates.

## Cross-manager contract and extension review

S1-S5 require an explicit routing rule for each cross-manager dependency:
producer, receiving owner, processing boundary, affected operations, consumption
rule, and enabling input. Keep `poll()`; pre/post-I/O are invocation positions,
not newly required methods. The current Commit-read-before-Heartbeat-clear
discovery-error order is part of the configured loop contract, not hidden author
knowledge. Another full pass may change the error audience without being wrong;
its admission cutoff must be documented and tested.

When adding behavior, identify its owner and stale-response check; its output
and next activation; what readers may observe; prerequisites of each effect;
and timeout/close outcome. Narrow APIs and boundary tests should enforce these
obligations where possible. This is not a promise that arbitrary new callbacks
are automatically safe, nor justification for another global framework.

## Evidence and resulting context

See the [compatibility ledger](kip-1371-approach2-compatibility-ledger.md) for
public-close and notification test boundaries and validation receipts, and the
[original issue inventory](kip-1371-issue-coverage.md) for remaining acceptance.
S1/S2 cover position scope and capture; S3 covers blocked-progress issues; S4
covers publication/wakeup; S5 covers lifecycle, with overlap explicitly required.
No original issue is removed or declared closed by regrouping the semantics.

This smaller design trades a universal barrier for effect-specific contracts
and narrower enforcement. Acceptance still requires historical reproductions,
complete applicable variants, real-consumer/broker validation and current-code
performance evidence. Any changed public completion priority or snapshot
freshness policy remains an explicit decision, not a test-only refactoring.
