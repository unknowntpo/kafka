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

This closes the demonstrated startup waiting defect only. In-flight heartbeat activation and
complete loop/public-consumer no-spin recovery need their own assertions; positive waiting
before discovery alone is not complete KAFKA-20253/20970 performance acceptance.
