# Deadline TreeSet versus linear scan: experiment protocol

This is an isolated deadline-index ablation of the frozen full NextPollCondition candidate, including the same no-spin fixes in both versions. The original live worktree is unchanged. Byte comparison of client jars permits changes only to RequestManagerScheduler and its nested classes.

The scan iterates entriesByOrder and examines active, armed Waiting deadlines. It does not evaluate condition predicates, change signal subscriptions, remove legacy polling, change ready snapshots, or remove the application maximum-wait scan. It retains the original expire() call sites, including per-manager preparation. It is a direct scan implementation, not a cached-minimum or once-per-batch redesign.

Correctness gates: the same regression suites on both jars/source versions, four new deadline-index tests, and identical ordered poll/count/wait checksums over 100,000 logical passes per micro workload. Deadline Long.MAX_VALUE is distinct from no deadline.

## Component experiment

Real scheduler and seven deterministic lightweight RequestManagers, no mocks, no broker, no socket waits. One JMH operation is one scheduler pass. Workloads:

- IDLE: all seven managers wait on a distant deadline; logical time remains fixed. Measures repeated index inspection, not a realistic idle CPU duty cycle.
- DUE: all seven deadlines expire every logical step. Stresses removal/re-registration.
- SIGNAL: one rotating signal fires every step and cancels a far-future deadline.
- MIXED: one legacy manager, six signal/deadline managers with different periods, and periodic signal publication.

Three independent JVM forks per variant/workload; order T/S, S/T, T/S. Each fork has three 2-second warmups and three 3-second measurements. Use JMH throughput and GC profiler (bytes/pass); process CPU per pass is derived from the same iteration CPU rate and pass rate, with lifecycle-envelope limitations. Retain iteration data and report variation rather than treating iterations as independent forks.

## Real consumer experiment

Use the same existing AsyncConsumerBrokerBenchmark and CPU profiler for both variants: real single-member consumer group, single partition, 5M preloaded records, 128 bytes each, repeated seek at end, validation of offset/payload sequence, 1000 records per invocation with JMH operation normalization. The benchmark and data validation costs remain included.

First run short functional checks for consume/idle/unavailable, then a separate consume pilot with 60 seconds warmup and five 5-second measurements per version. Inspect drift before choosing the formal fixed settings. Pilot data is excluded from formal comparisons. A stable directional effect must be supported by independent paired forks; do not infer universal equivalence from a small or noisy difference.

Client JVM pinned to physical CPU 2, quota 0.5 CPU, 512 MiB memory, zero swap; verify inside each fork and record ancestor constraints. Dedicated broker on CPUs 0/1; producer on CPU 3 exits before measurement. Micro runs precede broker startup. Local builds do not execute on the measured host. Affinity is not exclusive CPU reservation.

Saturated consume CPU/s cannot measure efficiency alone. Primary decision metrics are records/s and process CPU per record. Micro results cannot establish end-to-end benefit or real idle CPU savings. No power measurements are claimed.

Each remote attempt owns unique ports, systemd units, broker/data paths and a bounded runner. Save raw logs, JSON, artifact hashes, failure records and cleanup verification. Remove only run-owned resources and bulk broker data.

## Formal settings fixed after pilot

Pilot allocation stabilized near 556 B/record for both jars; process CPU/record late-half versus early-half change was -4.7% tree and -3.5% scan, while throughput still varied. Formal consume uses five independent paired rounds, alternating T/S and S/T, with 6x10s warmup and 5x10s measurement per fork (longer measurement than pilot). All samples are retained. Formal scope is consume; idle/unavailable remain functional checks and component scenarios, so no end-to-end idle CPU conclusion is made. Reject adoption if the scan cannot demonstrate a reproducible benefit; absence of significance is not proof of equivalence.
