# Event-only experiment

This is an isolated prototype, not a migration of Kafka production managers.
It compiles a generated ConsumerNetworkThread with the per-pass maximumTimeToWait
scan removed, a strict scheduler with no legacy fallback, and a versioned application
progress notification gate. Calling the old application wait API fails explicitly.
The real AsyncKafkaConsumer has NOT been wired to this new gate.

Run the parent validate.py and loop_validate.py first, then this directory's
validate.py and benchmark.py with Python 3. The harness uses the existing isolated
cached-dependency adaptations; no Gradle or broker is started.

The scheduler uses reusable bitsets for ready snapshots and the existing one-shot
signal subscriptions and ordered timer set. All managers must declare a condition.
Time passing is represented by an absolute deadline; only the earliest timer is
consulted to bound the network wait, and expired timers wake their owners.
Application progress is published once for each nonempty processed batch, including
timer-triggered batches. This conservative notification can be spurious.

The application must capture the gate generation BEFORE checking its state, then
wait for a generation change or the user timeout and recheck the state. Producers
update the state before publishing. This eliminates the check-to-wait race, but does
not by itself identify or connect every Kafka producer of relevant state changes.

JMH compares A1/A2 pinned trunk, STRICT_SCAN (same strict scheduler and gate, with
old maximum-wait scan), and STRICT (scan removed), for 32 minimal managers in
IDLE/SPARSE/BUSY/TIMER. Timers expire every 64 virtual milliseconds. No real socket
blocking or application waiter runs in the measured benchmark. Notification locking
is included, but application processing and thread wake latency are not.

Results and generated classes are in work/next-poll-condition/event-only-*.
The experiment deliberately has no production opt-in flag or fallback: it must not
be enabled for existing Kafka managers that do not publish sufficient conditions.

## Profiler-guided snapshot follow-up

BusyProfile records CPU samples and allocation samples separately for 5 seconds
after a 1-second warmup. These recordings locate candidate hot paths; their timing
and sample proportions are not benchmark speedups. summarize_snapshot_profile.py
filters allocation samples without workload frames (including profiler startup).
JFR allocation weights are estimates, not exact per-class allocated bytes.

SnapshotBeforeScheduler.java.in freezes the pre-change scheduler. Run validate.py,
then snapshot_profile.py to compile that control and record before/after profiles.
Run snapshot_benchmark.py separately, without profiling: duplicate trunk controls,
frozen BEFORE and AFTER, 32 managers, four patterns, same bounded JMH settings.
The corrected runner uses explicit group membership and saves every command.
The initial snapshot-results run is invalid because AFTER was mistakenly mapped
to trunk; use snapshot-results-corrected only.

The candidate changes only snapshot cleanup: clear the whole batch on success;
on exception, restore the unfinished suffix without replaying the completed prefix.
An additional 130-manager test crosses bitset word boundaries with a middle failure.
Subscription, condition construction, application notification and timer handling
are intentionally unchanged so the comparison has one implementation variable.

The first corrected run showed a lower BUSY point estimate, but its confidence
intervals overlapped. snapshot_busy_confirm.py ran a bounded BUSY-only ABBA
confirmation. It did not establish a repeatable speedup and includes a large
AFTER outlier, retained without filtering. The snapshot change remains an
unproven experimental candidate; do not use it as a KIP performance claim.
The application/heartbeat production migration remains outside this prototype.
