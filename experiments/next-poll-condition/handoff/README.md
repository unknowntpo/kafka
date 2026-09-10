# Independent validation handoff

This branch is a development snapshot, not a production-ready or performance-approved change.

Start with [the Fable brief](FABLE-PROMPT.md), then read:

- [Current migration inventory](evidence/scheduler-migration-inventory.md)
- [Trunk / no-spin / full prototype comparison](evidence/trunk-jmh.md)
- [TreeSet / direct scan comparison](evidence/deadline-tree-vs-scan.md)
- [Background auto-commit behavior](evidence/a4-background-auto-commit.md)

Base trunk: `820533b870106cc0e0ac60e2076b8644d68bd85f`.
The pushed source retains TreeSet. The benchmark's condition candidate also had a no-spin policy adjustment; this live snapshot does not incorporate that adjustment. See `benchmark-nospin.patch` and provenance under `evidence/trunk-threeway`. This patch was made against trunk: inspect/adapt it for the condition wrapper rather than applying blindly.

The historical 342-tests-per-variant result applies to frozen benchmark inputs. Fresh pre-push results are recorded in `prepush-validation.json` and `prepush-validation.log`. Static-analysis exclusions in `gradle/spotbugs-exclude.xml` are part of this snapshot and need independent review of their thread-confinement assumptions. Historical benchmark warnings and a passing filtered analysis are different claims.

Evidence includes readable reports, summaries, manifests, patches and validation logs. Large archives, runtime jars and broker data are not checked in; original absolute paths in manifests identify the original run and are not portable commands. Rebuild artifacts from source and use a new isolated run directory. `AsyncConsumerBrokerBenchmark.java` and `ConsumerCpuProfiler.java` preserve the real-consumer harness; the profiler's raw Result warnings mean the previous scoped annotation-processor build passed while the full JMH module did not.

Older experiment directories are historical probes, not all current implementations. The migration inventory supersedes stale migration checklists. Other local worktrees remain separate and were not merged into this snapshot.

Validated source snapshot commit: `9f8dc35e285107ce9f926f35ab74486796161073`. The following handoff commit only adds documentation, evidence and standalone harness copies.
