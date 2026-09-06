# Coordinator access capability: first migration slice

Baseline and rollback point: `checkpoint/kip-1371-design-v0-2026-09-06`
(`efb59de3873a761f8d3e3338e1c942293e229830`).

## Invariant and implementation

Commit may read coordinator state and ask its owner to validate a captured response
observation. It must not consume an error needed by other managers or drive the
coordinator's discovery/lifecycle directly.

`CoordinatorAccess` exposes only the five capabilities Commit currently needs:
node, owner version, non-consuming fatal-error read, version-fenced invalidation,
and version-fenced disconnect handling. `CoordinatorRequestManager` implements it;
both Commit constructors and its retained field now use the interface.

No additional state, allocation per request, queue, or delivery phase is introduced.
The existing owner methods still run at exactly the same call sites. Node/version
reads remain network-thread-confined, not a new atomic snapshot contract.

This is a source-level dependency boundary, not a security sandbox: deliberate
downcasts or widening the constructor can bypass it and still require review.
It does prevent an ordinary extension through Commit's declared dependency from
calling `getAndClearFatalError`, `poll`, `close`, or unconditional invalidation.

## Tests and scope

The new capability-only fixture supplies a `CoordinatorAccess` mock, not a concrete
coordinator manager. Real Commit logic still fails pending commit and offset-fetch
futures with the same error identity. A subsequent operation also sees that error
while no consuming route has run. This does not model the after-Heartbeat case.

A boundary regression checks Commit's retained dependency type and the absence of
consuming/lifecycle methods on that interface. Existing real-owner/component tests
cover stale responses, the after-Heartbeat case, recovery, input cutoffs, and
timeout precedence. These are in-memory tests, not a new real-broker result.

Validation: **195 tests in 5 suites passed twice**, with zero failures, errors,
or skipped cases and `maxTestRetries=0`. The selection was
`ConsumerAdmissionContractTest`, `ConsumerBatchedDecisionTest`,
`CommitRequestManagerTest`, `CoordinatorRequestManagerTest`, and
`RequestManagersTest`. Both runs used JDK 17, offline Gradle, a single test fork,
and `:clients:test --rerun`. Checkstyle main/test, Spotless Java, and SpotBugs main
also passed on the first run. No benchmark or public-consumer E2E was run.

## What this does not solve

Heartbeat still consumes the coordinator error through the legacy path. Commit
still depends on the configured read-before-clear ordering. This slice narrows
authority; it does not implement value-based fanout or eliminate that dependency.

The next route experiment must preserve both the Commit read point and the
Heartbeat notification/consumption point, including `onHeartbeatRequestSkipped`
ordering and regular/share/Streams eligibility. Merely polling a new error router
before every RM would change those boundaries. First extract the receiving-owner
operation from error lookup, then prove a configured route can supply the same
value at the same logical point. Only after equivalence should the legacy getter
be removed from the dependent capability.

Do not make error retention permanent, move all effects to the next iteration,
or change the winner between error delivery and application-event reaping as part
of this migration.
