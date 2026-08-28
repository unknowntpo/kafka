# Consumer Reactor POC Phase Map

This file maps the proof-of-concept branch to the migration phases in KIP-1371. It is an implementation index, not
part of the community-facing proposal. A phase is complete only when its exit evidence passes through production
components; the existence of a class or unit test is not sufficient.

## Branch rules

- Keep `codex/async-consumer-reactor-poc` runnable after every phase commit.
- Use one commit for one phase assertion. Documentation and review records may use separate commits.
- Prefix experimental work with `EXPERIMENT:` until its behavior and exit test are accepted.
- Do not rewrite the existing branch history. Superseded experiments remain traceable through their commits.
- Record the exact evidence commit in the KIP only after the working tree is clean and the named tests pass.

## Phase status

| Phase | Behavior | Current status | Representative evidence | Remaining exit gates |
| --- | --- | --- | --- | --- |
| 1. Decision boundary | `ConsumerReactor` runs the existing background loop; managers return typed output; the reactor retains deadlines, publishes `ReactorSchedule`, then executes `ReactorAction`. | Implemented in the POC. Compatibility producers remain. | `8e76de11fd`, `57b973bc6a`, `64c2cd33e2`; `ConsumerReactorTest`, `ManagerPollCacheTest`, `NetworkClientDelegateTest`. | Implement legal `RetryAfter(0)` and saturating maximum-delay semantics; remove raw-delay adapters only after all producers migrate. |
| 1A. Application-wait separation | Legacy `maximumTimeToWait(...)` contributes an application wait independently from the manager's reactor retry condition. | Implemented as an isolated migration adapter. | `5c587c4db4`; `ApplicationWait`; `ManagerPollCacheTest.testManagerRetryAndApplicationWaitRemainIndependent`; focused reactor/cache/delegate/schedule suites. | Prove each legacy application wait has an enabling input before removing the adapter. |
| 2. Manager and ownership migration | Manager-local state decides readiness. Cross-owner observations use typed events, an owner snapshot when needed, and owner-side version fencing. | Partial. Coordinator, commit, regular/share/Streams heartbeat, topic metadata, share acknowledgement retry, and Streams topology slices exist. | `72b759fc77`, `831710fd02`, `804ca84868`, `5ab585059a`, `edd4c5b1ab`; coordinator failover and stale-observation tests. | Migrate remaining raw-delay producers, prove share fetch recovery, add each opt-in snapshot family only with a real dependency and liveness test. |
| 3. Application-effect migration | State and schedule publication precede reactor-owned completion, notification, and wakeup effects. | Partial. Async-poll completion, metadata error, fatal coordinator error, and staged close actions cross the boundary. | `857b570e21`, `ac76b08109`, `07559ffa69`, `d9fafb212d`; publish-before-action and close regression tests. | Remove remaining direct application effects, coalesce equivalent wakes across a complete iteration, and prove exactly one terminal outcome for success, failure, timeout, cancellation, interruption, and close. |
| Follow-up lifecycle work | Fatal cleanup, bounded close, and public timeout guarantees remain compatible with existing consumer behavior. | Separate follow-up; not a new KIP-1371 state model. | `fea1e3746d`, `42c7c423ba`. | Expand public-method timeout audit and integration coverage before any lifecycle guarantee changes. |

## Current working tree split

The branch keeps implementation and proposal records in independent commits:

1. **Phase 1A code slice (`5c587c4db4`)**
   - `ApplicationWait.java`
   - `ConsumerReactor.java`
   - `ManagerPollCache.java`
   - `NetworkClientDelegate.java`
   - `NextPollCondition.java`
   - the corresponding three test files
2. **KIP and review documentation (next commit)**
   - `kip-introduce-consumer-reactor-state-management-event-processing.md`
   - `kafka-kip-writing-and-review-standard.md`
   - `consumer-reactor-kip-iteration-fable-2026-08-28.md`
   - this phase map

The next safe sequence is:

1. commit the reviewed KIP, writing standard, review record, and phase map separately;
2. push the branch after both commits pass `git diff --check` and the relevant Java quality gates;
3. start each remaining Phase 2 or Phase 3 gate from that clean baseline and keep it in a separate commit.

## Evidence naming

Every phase claim should use this form:

```text
Behavior -> production path -> named test -> evidence commit -> result
```

Use `Verified` only when the complete chain exists. Use `Partial` when the mechanism exists but a variant, lifecycle,
or end-to-end assertion is missing. Use `Pending` when the target behavior has not been implemented.
