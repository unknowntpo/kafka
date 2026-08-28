# KIP-1371 Discussion Readiness

This checklist separates evidence required to discuss the design from work required to finish every migration
phase. Discussion may start while disclosed Phase 2, Phase 3, lifecycle, and performance gates remain open.

## Current decision

**Ready for community discussion at evidence commit `68a978bb9f`.** The core invariants are implemented through
production components and have deterministic unit, component, and embedded-broker tests. The POC is not a claim
that all request managers or application effects have migrated.

## Evidence gates

| Gate | Evidence level | Result |
| --- | --- | --- |
| Typed manager output distinguishes progress, time-driven retry, and input-driven wait. | Unit and component | **Verified.** `NetworkClientDelegateTest`, `ManagerPollCacheTest`, `ConsumerReactorTest`, and `FetchRequestManagerTest.testZeroRetryBackoffMissingLeaderRetainsTimeDrivenRetry` cover legal `RetryAfter(0)`, invalid empty `PollImmediately`, maximum/overflow saturation, and one manager poll per stable pass. |
| Manager retry and legacy application wait remain separate during migration. | Component | **Verified.** `ConsumerReactorTest.testManagerRetryAndCompatibilityApplicationWaitRemainSeparateThroughReactor` retains the 100 ms manager retry while exposing the independent 25 ms application wait. |
| Coordinator, commit, and heartbeat dependencies do not create a zero-poll loop. | Component | **Verified for regular commit and regular/share heartbeat slices.** `ConsumerReactorCommitReadinessTest`, `CommitRequestManagerTest`, coordinator tests, and heartbeat suites pass. Public KAFKA-20970 reproduction and Streams-specific replacement proof remain Phase 2 gates. |
| Coordinator loss recovers through the public consumer path. | Embedded real-broker integration | **Verified.** `SslConsumerTest.testConsumerProtocolCoordinatorFailoverReactorRecovery` uses the consumer group protocol, a unique group id, embedded brokers, and predicate-based waits. It observes renewed `FindCoordinator`, heartbeat, consume, and commit progress after shutting down the coordinator broker. |
| Reactor diagnostic counters are usable in regular and share metric groups. | Unit with real metrics registry | **Verified.** `AsyncConsumerMetricsTest.shouldRecordReactorDiagnosticCounters` asserts each counter value in `consumer-metrics` and `consumer-share-metrics`. |
| Java formatting and static analysis accept the slice. | Build gate | **Verified.** Spotless, Checkstyle main/test, and SpotBugs main pass. |

## Recorded runs

All commands were run from `async-consumer-reactor-poc` on 2026-08-28 with `maxParallelForks=1`.

- Focused result-shape and scheduling run: 98 tests, 0 failures.
- Manager and consumer regression run: 805 tests, 0 failures.
- Embedded-broker coordinator failover: 1 test, 0 failures.

The unit and component suites create no external resources. The broker test owns its embedded cluster, uses a unique
consumer group id, waits on observable progress, and is safe to rerun without cleanup from a previous run.

## Disclosed implementation gates

These are not blockers for opening the design discussion:

- migrate remaining raw-delay producers and remove compatibility constructors;
- add replacement proof for each remaining regular, share, and Streams manager path;
- finish Phase 3 effect migration and coalesce equivalent wakes across one complete reactor iteration;
- complete lifecycle proofs for timeout, cancellation, interruption, fatal error, and close;
- run the remaining throughput, allocation, reconnect, rebalance, and share-consumer benchmarks.

Each future row becomes `Verified` only when the behavior, production path, named test, evidence commit, and result
are recorded together.
