# Native three-way cross-check (exploratory)

These native real-client measurements are retained as supporting evidence. The user requested JMH as the primary framework while this run was in progress; the primary benchmark is documented separately in [the JMH comparison](trunk-jmh.md). Do not combine their scores: the measurement loops and warmup histories differ.

All three client jars were built from the same fixed trunk commit `820533b870106cc0e0ac60e2076b8644d68bd85f`: clean trunk, only the heartbeat no-spin patch, and the complete current prototype with the same no-spin policy. The third variant includes notification/application-wait changes as well as NextPollCondition and Waiting reuse, so its difference cannot be attributed solely to the scheduler.

## Selected native results

Medians of three fresh JVM runs. CPU is whole-process CPU milliseconds per wall second, including JVM workers. Busy CPU per million records counts actual validated records. Each pair's percentage is retained in the JSON; dividing table medians can give a different percentage.

| Scenario | Variant | CPU ms/s | Select calls/s | Records/s | CPU ms/million records |
|---|---|---:|---:|---:|---:|
| UNAVAILABLE | trunk | 500.60 | 16890.1 | — | — |
| UNAVAILABLE | nospin | 37.39 | 21.9 | — | — |
| UNAVAILABLE | condition | 40.39 | 21.7 | — | — |
| IDLE | trunk | 31.87 | 18.4 | — | — |
| IDLE | nospin | 24.96 | 17.8 | — | — |
| IDLE | condition | 24.97 | 37.9 | — | — |
| BUSY | trunk | 475.76 | 5155.8 | 1,401,128 | 339.6 |
| BUSY | nospin | 479.65 | 4840.7 | 1,315,802 | 368.2 |
| BUSY | condition | 491.36 | 4183.8 | 1,403,987 | 351.1 |

Unavailable-coordinator CPU fell consistently with the local no-spin fix. Normal throughput did not show a consistent full-prototype improvement; normal idle CPU varied substantially. These measurements are not evidence of a general throughput improvement or statistical equivalence.

## Workload and checks

- Real KafkaConsumer, consumer group protocol, one member, one partition, dedicated Kafka 4.1.0 broker; Native Intel N150 host. Client CPU 2, 0.5 CPU quota, 512 MiB memory, no swap, heap 64/256 MiB, Java 17.0.20 and ActiveProcessorCount=1. Broker CPUs 0/1; producer CPU 3 and stopped before measurement. Limits/ancestors checked inside the client and no OOM events.
- Unavailable: bound but unlistened unique loopback port, 15 seconds warmup, 20 seconds measurement, auto commit disabled; coordinator absent, heartbeat interval zero and no in-flight heartbeat verified before/after. Three rotated orders T/N/C, N/C/T, C/T/N.
- Normal: auto commit enabled; five million preloaded 128-byte records with encoded sequence. Repeated full drains warm the consumer for at least 15 seconds; then 15 seconds idle warmup and 20 seconds idle measurement. The measured consumption phase repeats complete drains with seek(0) for at least 30 seconds. Every record's partition, offset, payload length and sequence are checked. Seek and validation cost are included. This is repeated finite backlog, not an unconstrained infinite stream.
- The initial normal pilot drained the backlog in only 3.56–5.40 seconds. It was deliberately stopped and excluded from the main native normal comparison before rebuilding a longer common harness; all completed unavailable rows were retained. The original manifest says FAILED/KeyboardInterrupt because of that deliberate stop, not a failed correctness gate. Pilot rows remain in the first results directory for inspection.
- The selected data are nine unavailable rows from the first attempt plus eighteen idle/busy rows from the extended normal run. Frozen client jars did not change. All real-broker gates passed. No JFR was used in these native timed runs.
- No power was measured: RAPL energy files require root access and noninteractive sudo requires a password. CPU or select-call reductions must not be expressed as watts or joules.

## Evidence and cleanup

- [Summaries, all selected rows and paired changes](trunk-threeway/summary.json)
- [Source/build provenance and 251/338 passing regression-test counts](trunk-threeway/build-provenance.json)
- [Frozen inputs and changed jar entries](trunk-threeway/inputs.json)
- [No-spin patch](trunk-threeway/nospin.patch), [full candidate production-source patch](trunk-threeway/candidate-full-main.patch)
- [First attempt raw manifest, including excluded pilot](trunk-threeway/results/manifest.json)
- [Extended normal run manifest](trunk-threeway-long/results/manifest.json)
- [First cleanup](trunk-threeway/cleanup.log), [extended-run cleanup](trunk-threeway-long/cleanup.log)

Both dedicated brokers were stopped, their backlog/data directories removed, and their own Java processes cleared. No shared runtime libraries or unrelated services were removed.
