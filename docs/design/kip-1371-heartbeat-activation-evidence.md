# Heartbeat activation evidence

## Initial interval is not a discovery retry deadline

The normal regular/share and Streams constructors initialize the heartbeat interval to zero.
Before a broker response negotiates an interval, unknown coordinator means no heartbeat can
be produced. The existing regular KAFKA-20253 guard returned that zero interval, while the
Streams path could fall through to joining urgency. Both new
`testZeroInitialHeartbeatIntervalAwaitsCoordinatorAndRecovers` tests failed on the unchanged
heartbeat code at parent `0829a57bfa` (the tested working tree also contained only unrelated
snapshot/callback changes). These are deterministic manager reproductions, not CPU benchmarks.

The correction preserves existing positive negotiated intervals and actual application poll
timer expiry. When blocked and the interval is zero, use half the remaining poll-timer budget,
clamped to at least 1 ms while the timer is live. The application retains its existing earlier
wait bounds; discovery completion can shorten the aggregate wait through the existing scheduling
notification. No new retry configuration or global dependency registry is introduced.

The shared abstract test runs against both regular and share managers and checks the final
1 ms rounding boundary and actual expiry. Regular and Streams tests also check that repeated
unknown-coordinator polls produce no requests, and after the coordinator becomes known, a
real manager poll produces the heartbeat. Existing immediate/timed healthy Streams tests now
provide a known coordinator explicitly instead of accidentally relying on an absent dependency.

Validation: **362 tests in ConsumerHeartbeatRequestManagerTest, ShareHeartbeatRequestManagerTest
and StreamsGroupHeartbeatRequestManagerTest passed twice**, zero failures/errors/skips, retries
disabled. JDK 17, offline Gradle 9.7.1, one fork. Java formatting, main/test Checkstyle and main
SpotBugs passed. Raw receipts are `/tmp/kip1371-overnight-evidence.TC1I8d/heartbeat-first-green/`
and `heartbeat-second-green/`; failing controls are in `zero-heartbeat-red/`.

## In-flight completion, not an expired interval

At `95095ac064`, a second correction covers an already in-flight heartbeat. Five new cases
(regular/share with zero and negotiated intervals, plus Streams startup) failed before the change:
request admission refused duplicate work, but the result requested another immediate timer poll.
The result now explicitly awaits network completion. The application wait remains bounded by its
poll-timer refresh and, when available, the negotiated heartbeat interval. Actual poll timeout
and urgent leave retain their existing priority. The final live millisecond is not rounded to zero.

`ConsumerBatchedDecisionTest.testInFlightHeartbeatDoesNotSpinConfiguredLoop` checks ten consecutive
iterations through real RequestManagers configuration and NetworkClientDelegate, one positive
network wait per iteration, no duplicate request, and another heartbeat after completion/interval.
It passes with and without the extra post-I/O manager pass; batching is not the no-spin protection.
`testManualAssignmentDoesNotSpinConfiguredLoopAndCanResumeHeartbeats` checks the actual aggregate
Long.MAX_VALUE application wait while unsubscribed, then request production after a membership input.
Membership and KafkaClient are mocked in these loop tests; they are not public-consumer benchmarks.

First green: **443 tests in four suites, zero failures/errors/skips**, including the three heartbeat
suites and ConsumerBatchedDecisionTest. Formatting, Checkstyle and main SpotBugs passed.
Raw reports: `/tmp/kip1371-overnight-evidence.TC1I8d/inflight-first-green/`.
Failing control: `inflight-heartbeat-red/` (440 tests, five new failures).
An intermediate run also exposed two old Streams assertions expecting a timer while in flight;
their reports remain in `inflight-streams-old-timer-assertions/`. These now assert the typed condition.

These are correctness/progress receipts, not measured CPU or throughput improvements.
The final broad regression must be reported separately rather than inferred from these counts.
