# Jenkins benchmark preparation (2026-09-07)

User authorized preparing the entry point, pushing only to `unknowntpo/kafka`, and
one benchmark submission following an exact preview. No upstream, Jira or Confluence
publication is part of this change. No Jenkins build has been submitted by this commit.

## Validation

- Existing local Linux image: `ducker-ak-docker.io/library/eclipse-temurin-17-jdk-jammy:latest`.
- Network-disabled, read-only source mount: all 11 parser/JFR/resource tests passed.
  The Linux test executes a real child, verifies nonzero RSS/CPU, and preserves exit 7.
- Ducktape loads the new module and `--collect-only` selects exactly one Linux-worker
  test using the proposed selector and `--test-runner-timeout 14400000` argument.
  Initial discovery from a read-only working directory failed while creating
  `.ducktape`; discovery from a writable temporary directory passed. No tests ran
  during discovery.
- Actual Linux fixture smoke: 10,000 records, one baseline/candidate JVM pair, two
  separate JFR executions. Both timed JVMs read exactly 10,000 records; process CPU
  and RSS were captured; profile result parsing and JFR summaries passed. The owned
  topic was deleted, broker stopped, generated broker-data removed, receipts retained.
  Receipt directory: `/tmp/kip1371-jenkins-preflight.JWRQo5/kip1371-throughput-1leatleb`.
  `inconclusive-short` is the expected classification, not performance evidence.
- Smoke reused existing compiled classpaths; it does NOT prove the fresh Linux
  Gradle-build wrapper end to end. That preparation is part of the Jenkins execution.
- Prior Jenkins build 926 console confirms depth=1 checkout. The new entry fetches
  only the immutable baseline into its disposable clone and validates candidate
  source by the pinned Git tree hash; it does not assume ancestor objects exist.
- Reviewed pipeline revision `0341b9cdb923c604353334da451d4cbf6de72ca1` from
  `opensource4you/clip`, `kafka/kafka_e2e.pipeline`: Java 17 worker image, throttle
  category `kafka_one_per_node`, no explicit job timeout in that historical source.
  Current job configuration API returned 403; do not claim that historical source
  establishes the current global timeout. Build failures/limits must remain visible.
- Final parser arguments contain a plain test selector plus a numeric timeout, no
  nested JSON. Reviewed `run_tests.sh` and `ducker-ak` forwarding: selector and option
  reach Ducktape in the same form verified by its real collector.

## Interpretation

This is the same healthy subscribed auto-commit workload as the local +6.71% CPU
observation. It measures the cost, not the root cause. Additional polling, allocation,
JIT or host contention remain hypotheses until supported by the new profiles or a
controlled follow-up. A throughput gate pass must not hide a CPU increase.
The single job contains five paired baseline/candidate runs on one worker, not two
separate Jenkins jobs and not the old centralized-reactor benchmark.
