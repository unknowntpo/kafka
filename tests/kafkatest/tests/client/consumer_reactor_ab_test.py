# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import csv
import hashlib
import io
import json
import os
import shlex

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.services.background_thread import BackgroundThreadService
from ducktape.tests.test import Test

from kafkatest.services.kafka import KafkaService, quorum


class ConsumerReactorABService(BackgroundThreadService):
    """Run the repository-local harness against the client from this checkout."""

    ROOT = "/mnt/consumer-reactor-ab"
    RAW_DIR = ROOT + "/raw"
    CLASSES_DIR = ROOT + "/classes"
    RESULTS_CSV = ROOT + "/results.csv"
    MANIFEST = ROOT + "/manifest.json"
    BROKER_CONFIG = ROOT + "/broker.properties"
    REPOSITORY = "/opt/kafka-dev"
    HARNESS_SOURCE = REPOSITORY + "/benchmarks/consumer-reactor-ab/IdleWakeHarness.java"
    HARNESS_CLASS = "org.apache.kafka.tools.reactorbenchmark.IdleWakeHarness"

    logs = {
        "consumer_reactor_ab_results": {
            "path": ROOT,
            "collect_default": True,
        }
    }

    RESULT_COLUMNS = [
        "scenario",
        "wall_ms",
        "cpu_ms",
        "cpu_percent",
        "records",
        "poll_calls",
        "poll_calls_per_sec",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "network_poll_avg_ms",
        "network_poll_max_ms",
        "network_poll_rate_hz",
    ]

    def __init__(self, context, kafka, profile, variant, parameters):
        super(ConsumerReactorABService, self).__init__(context, 1)
        self.kafka = kafka
        self.profile = profile
        self.variant = variant
        self.parameters = parameters
        self.results = []

    @staticmethod
    def _capture(node, command):
        return "".join(node.account.ssh_capture(command)).strip()

    @staticmethod
    def _property_map(contents):
        result = {}
        for line in contents.splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                result[key] = value
        return result

    def _compile_harness(self, node):
        script = r"""
set -euo pipefail
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
repo=%(repository)s
root=%(root)s
rm -rf -- "$root"
mkdir -p "$root/classes" "$root/raw"

client_jar_count="$(find "$repo/clients/build/libs" -maxdepth 1 -type f \
    -name 'kafka-clients-*.jar' ! -name '*-test.jar' ! -name '*-test-fixtures.jar' \
    ! -name '*-sources.jar' ! -name '*-javadoc.jar' | wc -l)"
if [[ "$client_jar_count" -ne 1 ]]; then
    echo "Expected exactly one current-revision kafka-clients jar; found $client_jar_count" >&2
    exit 1
fi
client_jar="$(find "$repo/clients/build/libs" -maxdepth 1 -type f \
    -name 'kafka-clients-*.jar' ! -name '*-test.jar' ! -name '*-test-fixtures.jar' \
    ! -name '*-sources.jar' ! -name '*-javadoc.jar')"
classpath="$client_jar"
while IFS= read -r dependency; do
    classpath="$classpath:$dependency"
done < <(find "$repo/core/build" -type f -path '*/dependant-libs-*/*.jar' | sort)

if [[ "$classpath" == "$client_jar" ]]; then
    echo "No current-revision client runtime dependencies were found" >&2
    exit 1
fi
javac -encoding UTF-8 --release 11 -proc:none -classpath "$classpath" \
    -d "$root/classes" %(harness_source)s
printf 'classpath=%%s\n' "$classpath" >"$root/client.properties"
printf 'artifact=%%s\n' "$client_jar" >>"$root/client.properties"
printf 'artifact.sha256=%%s\n' "$(sha256sum "$client_jar" | awk '{ print $1 }')" \
    >>"$root/client.properties"
printf 'commit=%%s\n' "$(git -C "$repo" rev-parse HEAD)" >>"$root/client.properties"
if [[ -n "$(git -C "$repo" status --porcelain=v1 --untracked-files=no)" ]]; then
    echo "Tracked files changed during the Jenkins checkout; refusing non-reproducible run" >&2
    exit 1
fi
""" % {
            "repository": shlex.quote(self.REPOSITORY),
            "root": shlex.quote(self.ROOT),
            "harness_source": shlex.quote(self.HARNESS_SOURCE),
        }
        node.account.ssh("bash -lc %s" % shlex.quote(script), allow_fail=False)
        properties = self._capture(node, "cat %s/client.properties" % self.ROOT)
        return self._property_map(properties)

    def _run_scenario(self, node, repetition, scenario, client_properties):
        raw_name = "rep-%02d-%s.log" % (repetition, scenario)
        raw_path = "%s/%s" % (self.RAW_DIR, raw_name)
        if scenario == "idle":
            arguments = ["--duration-ms", str(self.parameters["idle_duration_ms"])]
            timeout_seconds = 120 + self.parameters["idle_duration_ms"] // 1000
        else:
            arguments = [
                "--warmup-samples", str(self.parameters["first_record_warmup_samples"]),
                "--samples", str(self.parameters["first_record_samples"]),
                "--idle-ms", str(self.parameters["first_record_idle_ms"]),
                "--poll-timeout-ms", str(self.parameters["poll_timeout_ms"]),
            ]
            attempts = (
                self.parameters["first_record_warmup_samples"]
                + self.parameters["first_record_samples"]
            )
            # The harness aborts on the first missed record. Account for every
            # deliberate idle interval, but only one poll timeout on that path.
            timeout_millis = (
                attempts * self.parameters["first_record_idle_ms"]
                + self.parameters["poll_timeout_ms"]
            )
            timeout_seconds = 120 + (timeout_millis + 999) // 1000

        command = [
            "timeout", "--signal=TERM", "%ss" % timeout_seconds,
            "java", "-cp", "%s:%s" % (self.CLASSES_DIR, client_properties["classpath"]),
            self.HARNESS_CLASS,
            "--bootstrap-server", self.kafka.bootstrap_servers(),
            "--scenario", scenario,
        ] + arguments
        shell_command = " ".join(shlex.quote(value) for value in command)
        node.account.ssh(
            "bash -lc %s" % shlex.quote("%s >%s 2>&1" % (shell_command, raw_path)),
            allow_fail=False,
        )

        result_lines = list(node.account.ssh_capture("grep '^RESULT,' %s" % raw_path))
        if len(result_lines) != 1:
            raise RuntimeError(
                "Expected one RESULT line in %s, found %d" % (raw_path, len(result_lines))
            )
        fields = result_lines[0].strip().split(",")
        if len(fields) != len(self.RESULT_COLUMNS) + 1 or fields[0] != "RESULT":
            raise RuntimeError("Malformed RESULT line in %s: %s" % (raw_path, result_lines[0]))
        values = dict(zip(self.RESULT_COLUMNS, fields[1:]))
        values.update({
            "repetition": repetition,
            "variant": self.variant,
            "commit": client_properties["commit"],
            "raw_log": "raw/%s" % raw_name,
        })
        return values

    def _write_results(self, node):
        fieldnames = ["repetition", "variant", "commit"] + self.RESULT_COLUMNS + ["raw_log"]
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(self.results)
        node.account.create_file(self.RESULTS_CSV, output.getvalue())

    def _broker_metadata(self, node):
        broker_node = self.kafka.nodes[0]
        broker_config = self._capture(broker_node, "cat %s" % KafkaService.CONFIG_FILE)
        node.account.create_file(self.BROKER_CONFIG, broker_config + "\n")
        artifact_data = self._capture(
            broker_node,
            "bash -lc %s" % shlex.quote(
                "artifact=$(find /opt/kafka-dev/core/build/libs -maxdepth 1 -type f "
                "-name 'kafka_*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' | "
                "sort | head -n 1); test -n \"$artifact\"; "
                "printf 'artifact=%s\\nartifact.sha256=%s\\n' \"$artifact\" "
                "\"$(sha256sum \"$artifact\" | awk '{ print $1 }')\""
            ),
        )
        metadata = self._property_map(artifact_data)
        runtime_artifact_data = self._capture(
            broker_node,
            "bash -lc %s" % shlex.quote(
                "set -euo pipefail; repo=/opt/kafka-dev; "
                "{ "
                "find \"$repo/core/build/libs\" -maxdepth 1 -type f "
                "-name 'kafka_*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar'; "
                "find \"$repo/clients/build/libs\" -maxdepth 1 -type f "
                "-name 'kafka-clients-*.jar' ! -name '*-test-fixtures.jar' "
                "! -name '*-sources.jar' ! -name '*-javadoc.jar'; "
                "find \"$repo/core/build\" -type f -path '*/dependant-libs-*/*.jar'; "
                "} | sort -u | while IFS= read -r artifact; do "
                "printf '%s\\t%s\\n' \"$artifact\" "
                "\"$(sha256sum \"$artifact\" | awk '{ print $1 }')\"; done"
            ),
        )
        metadata["runtime_artifacts"] = []
        for line in runtime_artifact_data.splitlines():
            artifact, artifact_sha256 = line.split("\t", 1)
            metadata["runtime_artifacts"].append({
                "artifact": artifact,
                "artifact_sha256": artifact_sha256,
            })
        if not metadata["runtime_artifacts"]:
            raise RuntimeError("No broker runtime artifacts were found")
        metadata["commit"] = self._capture(
            broker_node, "git -C /opt/kafka-dev rev-parse HEAD"
        )
        metadata["config"] = "broker.properties"
        metadata["config.sha256"] = hashlib.sha256(
            (broker_config + "\n").encode("utf-8")
        ).hexdigest()
        return metadata

    def _write_manifest(self, node, client_properties):
        manifest = {
            "schema_version": 1,
            "scope": "single-checkout-current-revision",
            "profile": self.profile,
            "collected_variant": {
                "name": self.variant,
                "commit": client_properties["commit"],
                "artifact": client_properties["artifact"],
                "artifact_sha256": client_properties["artifact.sha256"],
                "consumer_config": {
                    "enable.auto.commit": "false",
                    "group.protocol": "consumer",
                    "manual.assignment": True,
                },
            },
            "comparison_contract": {
                "merge_across_exact_revisions": True,
                "logical_variants": [
                    "legacy-classic-reference",
                    "pre-refactor-async-baseline",
                    "proposal",
                ],
            },
            "broker": self._broker_metadata(node),
            "workload": self.parameters,
            "execution_order": ["idle", "first-record"],
            "runtime": {
                "java": self._capture(node, "java -version 2>&1 | head -n 1"),
                "os": self._capture(node, "uname -a"),
            },
            "jenkins": {
                "build_number": os.environ.get("BUILD_NUMBER"),
                "build_url": os.environ.get("BUILD_URL"),
            },
            "artifacts": {
                "results_csv": "results.csv",
                "raw_logs": [result["raw_log"] for result in self.results],
            },
        }
        node.account.create_file(self.MANIFEST, json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    def _worker(self, idx, node):
        client_properties = self._compile_harness(node)
        for repetition in range(1, self.parameters["repetitions"] + 1):
            self.results.append(
                self._run_scenario(node, repetition, "idle", client_properties)
            )
            self.results.append(
                self._run_scenario(node, repetition, "first-record", client_properties)
            )
        self._write_results(node)
        self._write_manifest(node, client_properties)

    def wait_timeout_seconds(self):
        idle_timeout = 120 + self.parameters["idle_duration_ms"] // 1000
        attempts = (
            self.parameters["first_record_warmup_samples"]
            + self.parameters["first_record_samples"]
        )
        first_record_millis = (
            attempts * self.parameters["first_record_idle_ms"]
            + self.parameters["poll_timeout_ms"]
        )
        first_record_timeout = 120 + (first_record_millis + 999) // 1000
        return max(
            600,
            self.parameters["repetitions"]
            * (idle_timeout + first_record_timeout)
            + 120,
        )

    def stop_node(self, node):
        node.account.kill_java_processes(
            self.HARNESS_CLASS,
            clean_shutdown=True,
            allow_fail=True,
        )

    def clean_node(self, node):
        node.account.ssh("rm -rf -- %s" % self.ROOT, allow_fail=False)


class PairedConsumerReactorABService(ConsumerReactorABService):
    """Build and measure the pinned baseline and current proposal on one node."""

    ROOT = "/mnt/consumer-reactor-ab-paired"
    RESULTS_CSV = ROOT + "/results.csv"
    MANIFEST = ROOT + "/manifest.json"
    BROKER_CONFIG = ROOT + "/broker.properties"
    BASELINE_COMMIT = "9d940a6537c65357684d63d6defc573807c8831b"

    logs = {
        "consumer_reactor_paired_ab_results": {
            "path": ROOT,
            "collect_default": True,
        }
    }

    def __init__(self, context, kafka, profile, parameters):
        super(PairedConsumerReactorABService, self).__init__(
            context,
            kafka,
            profile,
            "paired",
            parameters,
        )

    def _prepare_and_run(self, node):
        script = r"""
set -euo pipefail
export LANG=C.UTF-8
export LC_ALL=C.UTF-8
repo=%(repository)s
root=%(root)s
baseline_commit=%(baseline_commit)s
baseline_worktree="$root/baseline-worktree"
build_dir="$root/build"
output_root="$root/results"

rm -rf -- "$root"
mkdir -p "$root"
git -C "$repo" worktree prune
if ! git -C "$repo" cat-file -e "${baseline_commit}^{commit}"; then
    git -C "$repo" fetch --no-tags --depth=1 origin \
        "+${baseline_commit}:refs/reactor-ab/baseline"
fi
git -C "$repo" worktree add --detach "$baseline_worktree" "$baseline_commit"
test "$(git -C "$baseline_worktree" rev-parse HEAD)" = "$baseline_commit"

cleanup_baseline_worktree() {
    git -C "$repo" worktree remove --force "$baseline_worktree" >/dev/null 2>&1 || true
}
trap cleanup_baseline_worktree EXIT

EXPECTED_BASELINE_COMMIT="$baseline_commit" \
BASELINE_WORKTREE="$baseline_worktree" \
REACTOR_AB_BUILD_DIR="$build_dir" \
    "$repo/benchmarks/consumer-reactor-ab/prepare.sh"

# The prepared jars are self-contained. Do not archive the baseline source tree
# with the benchmark results, and do not leave stale worktree metadata behind.
cleanup_baseline_worktree
trap - EXIT

REPETITIONS=%(repetitions)s \
IDLE_DURATION_MS=%(idle_duration_ms)s \
FIRST_RECORD_WARMUP_SAMPLES=%(first_record_warmup_samples)s \
FIRST_RECORD_SAMPLES=%(first_record_samples)s \
FIRST_RECORD_IDLE_MS=%(first_record_idle_ms)s \
POLL_TIMEOUT_MS=%(poll_timeout_ms)s \
REACTOR_AB_BUILD_DIR="$build_dir" \
REACTOR_AB_OUTPUT_ROOT="$output_root" \
    "$repo/benchmarks/consumer-reactor-ab/run-idle.sh" %(bootstrap_servers)s

results_csv="$(find "$output_root" -mindepth 2 -maxdepth 2 -name results.csv -print)"
test -n "$results_csv"
test "$(printf '%%s\n' "$results_csv" | wc -l)" -eq 1
cp "$results_csv" %(results_csv)s
printf '%%s\n' "$results_csv" >"$root/results-path"
""" % {
            "repository": shlex.quote(self.REPOSITORY),
            "root": shlex.quote(self.ROOT),
            "baseline_commit": shlex.quote(self.BASELINE_COMMIT),
            "repetitions": self.parameters["repetitions"],
            "idle_duration_ms": self.parameters["idle_duration_ms"],
            "first_record_warmup_samples": self.parameters["first_record_warmup_samples"],
            "first_record_samples": self.parameters["first_record_samples"],
            "first_record_idle_ms": self.parameters["first_record_idle_ms"],
            "poll_timeout_ms": self.parameters["poll_timeout_ms"],
            "bootstrap_servers": shlex.quote(self.kafka.bootstrap_servers()),
            "results_csv": shlex.quote(self.RESULTS_CSV),
        }
        node.account.ssh("bash -lc %s" % shlex.quote(script), allow_fail=False)
        contents = self._capture(node, "cat %s" % self.RESULTS_CSV)
        self.results = list(csv.DictReader(io.StringIO(contents)))

    def _write_paired_manifest(self, node):
        build_info = self._property_map(
            self._capture(node, "cat %s/build/build-info.properties" % self.ROOT)
        )
        variants = []
        for name in ("baseline", "reactor"):
            variants.append({
                "name": name,
                "commit": build_info["%s.commit" % name],
                "artifact": build_info["%s.artifact" % name],
                "artifact_sha256": build_info["%s.artifact.sha256" % name],
                "consumer_config": build_info["%s.consumer.config" % name],
            })
        manifest = {
            "schema_version": 1,
            "scope": "single-node-paired-revisions",
            "profile": self.profile,
            "collected_variants": variants,
            "broker": self._broker_metadata(node),
            "workload": self.parameters,
            "execution_order": [
                {
                    "repetition": result["repetition"],
                    "order": result["order"],
                    "sequence": result["sequence"],
                    "variant": result["variant"],
                    "scenario": result["scenario"],
                }
                for result in self.results
            ],
            "runtime": {
                "measurement_node": self._capture(node, "hostname"),
                "java": self._capture(node, "java -version 2>&1 | head -n 1"),
                "os": self._capture(node, "uname -a"),
            },
            "artifacts": {
                "results_csv": "results.csv",
                "raw_logs": [result["raw_log"] for result in self.results],
            },
        }
        node.account.create_file(
            self.MANIFEST,
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        )

    def _worker(self, idx, node):
        self._prepare_and_run(node)
        self._write_paired_manifest(node)

    def wait_timeout_seconds(self):
        return 1200 + 2 * super(PairedConsumerReactorABService, self).wait_timeout_seconds()


class ConsumerReactorABTest(Test):
    """Dedicated, current-checkout wrapper for the consumer reactor benchmark harness."""

    PAIRED_MEASUREMENT_BUDGET_MS = 20 * 60 * 1_000
    PROFILES = {
        "smoke": {
            "repetitions": 1,
            "idle_duration_ms": 5_000,
            "first_record_warmup_samples": 1,
            "first_record_samples": 5,
            "first_record_idle_ms": 250,
            "poll_timeout_ms": 30_000,
        },
        "formal": {
            "repetitions": 5,
            "idle_duration_ms": 60_000,
            "first_record_warmup_samples": 10,
            "first_record_samples": 100,
            # Keep the paired workload below Ducktape's 30-minute runner limit
            # after accounting for both client builds. This remains well above
            # the default retry backoff, so samples still cross a fresh idle
            # interval instead of measuring a tight retry loop.
            "first_record_idle_ms": 500,
            "poll_timeout_ms": 30_000,
        },
    }
    FORMAL_VARIANTS = {"pre-refactor-async-baseline", "proposal"}

    @staticmethod
    def scheduled_measurement_ms(parameters, variant_count=1):
        first_record_attempts = (
            parameters["first_record_warmup_samples"]
            + parameters["first_record_samples"]
        )
        per_variant_ms = (
            parameters["idle_duration_ms"]
            + first_record_attempts * parameters["first_record_idle_ms"]
        )
        return parameters["repetitions"] * variant_count * per_variant_ms

    def __init__(self, test_context):
        super(ConsumerReactorABTest, self).__init__(test_context)
        injected = test_context.injected_args or {}
        profile = injected.get("reactor_ab_profile", "smoke")
        if profile not in self.PROFILES:
            raise ValueError("Unknown reactor_ab_profile: %s" % profile)
        default_variant = "checkout-current-smoke"
        variant = injected.get("reactor_ab_variant", default_variant)
        if profile == "formal" and variant not in self.FORMAL_VARIANTS:
            raise ValueError(
                "Formal runs require reactor_ab_variant to be one of %s" %
                sorted(self.FORMAL_VARIANTS)
            )

        parameters = dict(self.PROFILES[profile])
        for name, value in parameters.items():
            if value <= 0:
                raise ValueError("Profile value %s must be positive" % name)

        self.profile = profile
        self.variant = variant
        self.parameters = parameters
        self.kafka = KafkaService(
            test_context,
            num_nodes=1,
            zk=None,
            controller_num_nodes_override=1,
        )
        self.benchmark = ConsumerReactorABService(
            test_context,
            self.kafka,
            profile,
            variant,
            parameters,
        )

    @cluster(num_nodes=2)
    @parametrize(metadata_quorum=quorum.combined_kraft)
    def test_current_revision(
        self,
        metadata_quorum=quorum.combined_kraft,
        reactor_ab_profile="smoke",
        reactor_ab_variant="checkout-current-smoke",
    ):
        """Run only the explicitly checked-out revision; never checks out another tree."""
        self.kafka.start()
        self.benchmark.start()
        try:
            self.benchmark.wait(timeout_sec=self.benchmark.wait_timeout_seconds())
        finally:
            self.benchmark.stop()
        expected_rows = self.parameters["repetitions"] * 2
        assert len(self.benchmark.results) == expected_rows, (
            "Expected %d result rows, found %d" %
            (expected_rows, len(self.benchmark.results))
        )


class ConsumerReactorPairedABTest(Test):
    """Run the pinned baseline and current proposal in one Jenkins allocation."""

    PROFILES = ConsumerReactorABTest.PROFILES

    def __init__(self, test_context):
        super(ConsumerReactorPairedABTest, self).__init__(test_context)
        injected = test_context.injected_args or {}
        profile = injected.get("reactor_ab_profile", "smoke")
        if profile not in self.PROFILES:
            raise ValueError("Unknown reactor_ab_profile: %s" % profile)
        parameters = dict(self.PROFILES[profile])
        scheduled_ms = ConsumerReactorABTest.scheduled_measurement_ms(
            parameters,
            variant_count=2,
        )
        if scheduled_ms > ConsumerReactorABTest.PAIRED_MEASUREMENT_BUDGET_MS:
            raise ValueError(
                "Paired profile schedules %d ms of measurement; budget is %d ms" %
                (scheduled_ms, ConsumerReactorABTest.PAIRED_MEASUREMENT_BUDGET_MS)
            )
        self.profile = profile
        self.parameters = parameters
        self.kafka = KafkaService(
            test_context,
            num_nodes=1,
            zk=None,
            controller_num_nodes_override=1,
        )
        self.benchmark = PairedConsumerReactorABService(
            test_context,
            self.kafka,
            profile,
            parameters,
        )

    @cluster(num_nodes=2)
    @parametrize(metadata_quorum=quorum.combined_kraft)
    def test_same_worker(
        self,
        metadata_quorum=quorum.combined_kraft,
        reactor_ab_profile="smoke",
    ):
        """Alternate the pinned baseline and proposal against one broker and node."""
        self.kafka.start()
        self.benchmark.start()
        try:
            self.benchmark.wait(timeout_sec=self.benchmark.wait_timeout_seconds())
        finally:
            self.benchmark.stop()
        expected_rows = self.parameters["repetitions"] * 4
        assert len(self.benchmark.results) == expected_rows, (
            "Expected %d result rows, found %d" %
            (expected_rows, len(self.benchmark.results))
        )
