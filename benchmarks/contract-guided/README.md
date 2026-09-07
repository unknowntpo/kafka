# Contract-Guided Coordination: local performance acceptance

> This experimental branch repins `run-throughput.py` for the local post-I/O
> ablation: control `d3c6d866adbdbb15a4c2f5223707e2ba926770fc`, candidate
> `4289215a07df6b4d2150d43b1025b75787860a50`. See
> `docs/design/post-io-ablation-results.md` for results and limitations.
> The historical acceptance/Jenkins instructions below describe the original
> candidate; this branch is not an approved Jenkins submission configuration.

This is separate from the original reactor benchmark. Never import its percentages.
Implementation candidate: `37c6603a99` (behavior from `95095ac064`, comment-only clarification);
baseline: `820533b870106cc0e0ac60e2076b8644d68bd85f`.
Both use Java 17 on the same machine, with separate clean implementation classpaths.
The baseline worktree is `kip-1371-acceptance-baseline`; only generated build outputs
are written there. The local runner never publishes or submits a remote benchmark.
The separately authorized Jenkins entry point is described below.

## Predeclared measurement method

- Finish compilation and correctness tests before measurement. No concurrent Gradle tests.
- One task-owned local KRaft broker, bound only to loopback, with a unique temporary log directory.
  Never point this workload at an existing user cluster. Record configuration and readiness.
- Use the shipped `org.apache.kafka.tools.ConsumerPerformance` for backlog throughput.
  This is the class invoked by `kafka-consumer-perf-test.sh`.
- Explicitly configure `group.protocol=consumer`; otherwise a comparison may exercise the
  wrong consumer. Use unique groups, earliest offsets, fixed byte payload and partition count.
- Preload a fixed dataset, verify producer completion and broker end offsets. Run the same
  dataset for both clients; record actual consumed count, not just the CLI exit status.
  The CLI can print a warning and exit successfully after a short read, which is not a pass.
- Five independent JVM pairs, alternating AB/BA. Preserve each result and command, CPU time,
  peak resident memory if available, and consumer metrics. Do not pool per-second reports
  as independent replications. Report per-run rates and paired ratios plus median/MAD.
- Target at least 30 seconds of measured fetching per run. If the chosen volume finishes
  sooner, classify it as smoke only and increase the identical volume before acceptance.
  Predeclare throughput regression gate: candidate median no lower than 95% of baseline;
  a noisy or short run is inconclusive, not a passing gate.
- Idle CPU and first-record latency require separate workloads. A throughput CLI is not an
  idle/no-spin test. Any reused idle harness must be byte-identical across classpaths and its
  manual-assignment/auto-commit-disabled scope must be stated. Network-poll average-derived
  frequency is an estimate, not an exact counted event rate.
  For the reused idle harness, use 60-second idle windows, 100 first-record samples after
  ten warmups with 250 ms idle gaps, and five alternating independent-JVM pairs. Predeclared
  regression allowances: +0.2 percentage points of one-core process CPU while idle, and
  +10 ms median per-JVM first-record p99. Always show the actual deltas even when inside
  these allowances. Small-sample median gates are not statistical proof of equivalence.
- Profiling is a separate diagnostic run, not mixed into timed baseline/candidate samples.
  JFR can supply CPU stacks and allocation samples; heap peak is not process RSS.
- Close the task-owned broker and clients after measurement. Preserve logs, configs and
  raw results for audit; do not erase failure evidence. Remove only task-owned data if cleanup
  is necessary, never an existing topic/directory or another process.

`runtime-classpath.gradle` exports resolved runtime classpaths using a Gradle init script.
It works against both checkouts without editing baseline source or relying on ambiguous
jar wildcards. `contractGuidedOutput` names the task-owned output directory; optional
`contractGuidedBroker=true` includes the broker runtime for the candidate fixture.
Run its `contractGuidedRuntime` task once from each pinned checkout, supplying this
same init script and separate absolute output directories. Include the broker only
in the candidate export. The exported classpaths include distribution `releaseOnly`
logging jars as well as normal runtime dependencies. Do not replace these files with
an old distribution's client wildcard, or compare a NOP logger against a real logger.

These local measurements cannot establish rack/remote-broker performance, all consumer
compositions, abrupt-crash correctness, or an absence of regressions under every workload.
Do not mark performance accepted until real receipts are attached.

## Runner and smoke validation

`run-throughput.py` creates its own broker and unique topic; it accepts no existing
bootstrap address. It records the exact commands, runtime-content hashes, source revision,
counts and timing/RSS output, and stops only its owned processes. The full profile uses
50,000,000 records of 256 bytes (12.8 GB payload), four partitions, five pairs, auto-commit
enabled and 500 max poll records. Start with `--records 10000 --pairs 1` to validate the
fixture and parser, without interpreting those short results as performance acceptance.
Optional `--idle-harness-classes` runs the separately compiled, unchanged idle harness;
`--profile` records separate baseline and candidate JFRs after measurement. These are
whole-process diagnostics, including startup and close, not timed acceptance samples
or exact allocation-per-record counters.

`test_results.py` has six CSV-validation cases rejecting missing/ambiguous summaries,
incomplete consumption despite exit 0, warnings, nonfinite rates and zero-rate results,
plus three performance-event export checks. All nine pass. Partial-dataset profiling
allows at most one final poll batch of overshoot; whole-dataset acceptance remains exact.
`--profile-records` selects the diagnostic volume without changing timed runs.
Real broker lifecycle,
classpath separation, exact counts and cleanup passed two isolated fixture smokes.

The baseline export is built successfully. Gradle's absent output for a demonstrated
`NO-SOURCE` Java/resource task is omitted (including the Scala-only core module's Java
output); unexpected missing jars/classes still fail.
Before any topic mutation, the runner checks that the listener reports the unique
cluster ID it just formatted and that its owned broker process is still alive.
The manifest records the Java version and platform. CPU/RSS summaries explicitly
cover the whole CLI process, not only its reported fetch interval.

`profile.jfc` records performance events without environment variables or unrelated
process command lines. `summarize-jfr.py` exports only allowlisted CPU/allocation/GC/lock
events, a summary and Java CPU folded stacks. Sample weights are estimates, not exact
allocation-per-record counters. The early smokes and initial 50-million-record run used the JDK default profile:
keep those raw files private and exclude them from any evidence bundle; use only the
allowlisted extracts. No recording or evidence is uploaded by these scripts.

## Completed acceptance receipt

See `docs/design/kip-1371-local-acceptance.md` for the completed 70-million-record,
five-pair normal-logging run. All duration/count/pair requirements and the declared
throughput/idle/first-record allowances passed. Throughput is nevertheless 1.95%
lower and whole-process CPU time 6.71% higher; passing an allowance is not proof
of no cost or statistical equivalence. Earlier NOP-logging/short results are kept
separate. Full-dataset baseline/candidate profiles also completed successfully.

## Authorized Jenkins paired throughput entry point

`tests/kafkatest/tests/client/consumer_contract_benchmark_test.py::ConsumerContractBenchmarkTest.test_paired_throughput`
is one Ducktape test, allocating one worker for both variants and its own loopback
combined-KRaft broker. It does not use the old reactor A/B test or an existing cluster.
The job revision must contain this entry point; candidate behavior remains pinned
by the runner's client source-tree check. Baseline is the full SHA given above.
`jenkins-entry.py` clones two isolated local checkouts from `/opt/kafka-dev`, builds
both runtimes sequentially with UTF-8 and Java 17, and finishes compilation before timing.
It requires 35 GiB free disk. Because Jenkins uses a shallow checkout, the baseline
clone fetches only the pinned baseline SHA from `https://github.com/unknowntpo/kafka.git`.
Candidate source is copied locally from the submitted checkout and checked against
the pinned production tree hash. No push, retry or job mutation is performed by the
entry point. Gradle may download its normal build dependencies.

The fixed workload is 70,000,000 records × 256 bytes, four partitions, five alternating
AB/BA pairs, subscription with `group.protocol=consumer`, auto-commit enabled, and
the shipped `ConsumerPerformance` CLI. Two additional full-dataset JFR executions
are diagnostic only. They are not added to the ten timed samples. This run targets
the healthy-throughput CPU regression, not idle CPU, fault-triggered spin, or latency.

Linux `resource-time.py` uses `wait4` on each individual Java child: user + system
CPU seconds and maximum RSS (KiB converted to bytes). It excludes the Python wrapper,
does not accumulate earlier JVMs, and propagates unsuccessful CLI exits. CPU still
includes JVM startup, JIT and close; it is not fetch-only utilization. macOS retains
its original `/usr/bin/time -l` path. Compare variants within one platform, not raw
Linux and macOS CPU times as if interchangeable.

The same duration/count/throughput gates apply. Short runs are inconclusive, not
passes. There is no predeclared CPU acceptance gate: always report CPU deltas even
if throughput is within 5%. One worker is not proof of exclusive physical-host use.
Raw commands, classpath-content hashes, results, medians/MAD, RSS/CPU, JFR and safe
folded stacks are collected by Ducktape. Only owned broker data and isolated build
copies are removed after their processes stop; failed and partial receipts remain.

Use `--test-runner-timeout 14400000` (four hours) in `TC_PATHS` so Ducktape's default
30-minute watchdog cannot truncate preparation/measurement. Internal limits are
one hour per runtime build and 90 minutes for workload/profiling. Do not change a
Jenkins job-level timeout automatically if it is shorter; report that infrastructure
limit. Submission requires its own exact preview and confirmation, exactly once.
