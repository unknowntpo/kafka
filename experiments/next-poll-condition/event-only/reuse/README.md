# Reusable registration and optimistic-ready experiment

This remains an isolated prototype. No Kafka production manager or application
waiting path was migrated in this experiment.

Run the parent event-only validate.py first, then this directory's validate.py and
benchmark.py with Python 3. The baseline and candidate each compile both their own
scheduler and condition classes; frozen BeforeScheduler/BeforeCondition controls
prevent classpath mixing. The prior snapshot candidate is common to both groups,
so this comparison isolates registration reuse, not snapshot cleanup.

The candidate keeps one Waiting per manager and reuses its subscription slots.
Signals remain network-thread confined. A node still owned by a detached publication
cannot be reused: cancellation clears its callback, and re-arm allocates a replacement
until traversal releases the old node. Public one-shot cancellation handles never
recycle behind their callers. Multi-signal storage retains its high-water capacity
until the manager is closed; this lowers allocation, not necessarily retained heap.

Tests cover object identity over 10,000 re-arms, multi/single-signal/timer transitions,
close detach, public stale handles, pending-publication fallback, Waiting re-arm
during another publication, partial registration failure, and prior loop semantics.
The latter two were requested by an independent read-only reviewer and added.
Trunk, before, after and oracle compare 40,000 cumulative output steps and total
work/network-pass counts. No broker or actual AsyncKafkaConsumer wait is exercised.

JMH has duplicate trunk controls, BEFORE, AFTER and ORACLE for 32 managers under
IDLE/SPARSE/BUSY/TIMER. ORACLE knows this workload's deterministic ready schedule;
it retains manager.poll, metrics/reaper/transport and application progress publishing,
but does not register conditions. Condition construction remains in source, although
JIT may eliminate unused objects. It is an optimistic reference, not a deployable
scheduler or a mathematical lower bound. It does not model arbitrary events,
exceptions or cross-manager wakeups beyond the fixed trace.

A separate FLOOR/IDLE case with zero managers measures fixed loop overhead, not
equal BUSY work. Both oracle and floor must remain separate from production claims.
Two forks, 3x500 ms warmup, 5x500 ms measurement, single worker, 128 MB heap,
GC profiler. All groups are interleaved in a fixed randomized order. Every command
and conditional parameter is saved and checked against the result JSON.

Evidence is in work/next-poll-condition/reuse-results and work/reuse-validation.log.

For the BUSY-only speed-potential comparison, run direct_reference.py after the
main benchmark. It first validates DIRECT output traces with conditional=false
and OracleScheduler (zero max-wait scans), then interleaves A1/A2/AFTER/ORACLE/DIRECT.
DIRECT removes unused signal/condition construction as well as ready inference;
its result is a more optimistic, multi-variable reference, not an attainable promise.
