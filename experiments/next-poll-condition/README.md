# NextPollCondition: subscription-cost experiment

Branch `codex/next-poll-condition-trunk` starts at trunk commit
`820533b870106cc0e0ac60e2076b8644d68bd85f`. Changes remain uncommitted.

## Scope

Coordinator conditions register a signal or absolute deadline. State updates precede publication;
publication activates only subscribers. The network loop drains one ordered ready snapshot and
preserves legacy manager polling. It does not scan waiting conditions. Cross-thread application
input retains Kafka's existing event queue plus network-thread wakeup; direct cross-thread signal
publication is unsupported. The separate maximumTimeToWait pass is unchanged.

This iteration changes only subscription management relative to the profiled event scheduler:

- Replace the subscriber hash set with directly linked cancellation handles.
- Detach a publication generation without allocating a subscriber array snapshot.
- Make Waiting implement the callback, avoiding a per-registration callback lambda.
- Store one cancellation handle directly; allocate an additional list only for multiple signals.

Generations, stale-cancellation isolation, stable dispatch order, duplicate coalescing, deadline
cancellation and bounded passes remain. Registration during publication waits for a later publish;
a cancelled sibling in the detached list does not fire. The callback is internal scheduler code.
The bitset dispatch draft is deferred; ready/legacy TreeSets and dispatch bookkeeping are unchanged.
The unused condition query interface and anyOf are also unchanged in this experiment.

## Reproduction

```sh
python3 experiments/next-poll-condition/validate.py
python3 experiments/next-poll-condition/memory.py
python3 experiments/next-poll-condition/benchmark.py
```

The local harness uses Homebrew JDK 17 and locally cached dependency jars, copied under this
worktree's ignored work directory. It never writes the original Kafka repo or shared Gradle caches.
JMH uses localhost sockets for fork coordination; memory.py attaches jcmd only to its own test JVM.
Do not run the memory check and benchmark concurrently.

Validation compiles the changed classes using a generated NetworkClientDelegate copy with only its
unused client-factory body excluded because cached dependency signatures predate trunk. Actual
PollResult, request callbacks, coordinator and scheduler sources are compiled. Exact trunk controls
receive the identical factory exclusion. This is not a full Gradle build or network-thread integration
suite. Seventeen JUnit methods run through a small reflection launcher, and 16,000 per-pass request,
wait, coordinator-presence and fatal-state observations match across trunk, current, old-event and
scan controls. Successful discovery/invalidation is included in the traces; wire bytes are not compared.

## Performance controls

| Group | Classes | Driver |
|---|---|---|
| A1 / A2 | Exact original trunk coordinator and PollResult | Full pass |
| OLD | Frozen pre-change event scheduler and conditions | Scheduled |
| NEW | Current subscription implementation | Scheduled |

EventRequestManagerScheduler.java.in and EventNextPollCondition.java.in are frozen source controls.
The same compiled benchmark runs in every group with a matching classpath. The ScanScheduler control
is retained for validation, but this run focuses on subscription-only attribution.

Workloads:

- QUIET: hold one discovery in flight.
- BURST: fail a discovery every 64 passes, with zero backoff.
- BUSY: fail a discovery every pass, with zero backoff.
- SUCCESS: complete discovery successfully on the next pass, keep the coordinator known, then
  invalidate every 64 passes to repeat the cycle. Response construction is included equally.

Every invocation executes 1,024 passes, with a common non-inlined per-pass boundary. Sixteen cells
run serially in a fixed randomized order; two forks, two 300 ms warmups, three 300 ms measurements,
one worker, 128 MB heap, GC profiler. Each cell has a 60-second timeout. These are short component
measurements, not production throughput or latency claims; no broker or real network is involved.

Memory validation keeps 1,000 owners and signals reachable across 500,000 notifications, checks
subscriber counts, and captures GC.class_histogram at four checkpoints including close. Separate
ThreadMXBean counters measure allocation for trunk, OLD and NEW without JFR, after warmup.
Histograms report shallow live object counts/bytes, not dominator retained size or proof against
all memory leaks. These tests do not cover native memory or RSS.

New benchmark data: work/next-poll-condition/subscription-results.
New memory data: work/subscription-memory. Previous reports/results remain preserved separately.

## Actual network-loop validation

After `validate.py`, run `loop_validate.py` to compile the actual trunk/current
ConsumerNetworkThread loop with current source metrics and reaper, and run six
network-loop integration tests. The generated fixture only adapts setup constructor
signatures to the cached dependency jar. The event processor and nonblocking
transport are boundary substitutes; this does not exercise real sockets or the
application event processor's business logic.

Run `loop_benchmark.py` for randomized A1/A2 trunk controls versus EVENT, with
8 and 32 minimal synthetic managers under IDLE, SPARSE, BUSY and MIXED input.
JMH includes signal publication and the full runOnce call, including metrics,
reaping and maximumTimeToWait scans. Setup-only Mockito mocks are never invoked
in the measured region. Two forks, one worker and a 128 MB heap bound each cell.
A separate ThreadMXBean probe records loop CPU/allocation; a separately instrumented
probe records publication-to-consumption latency within an awake loop.
These are synthetic loop costs, not production Kafka throughput or OS wake latency.
Results: work/next-poll-condition/loop-results.
