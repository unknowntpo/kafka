#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Task-owned loopback broker and paired, shipped ConsumerPerformance JVMs.

No existing broker address is accepted. Does not build, push or submit anything.
Raw output and failed runs are retained. A short-read CLI success is a failure.
"""

import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import signal
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import uuid


CANDIDATE_REVISION = "37c6603a99"  # Same client behavior as 95095ac064; response-delivery comment clarified.
EXPECTED_CANDIDATE_CLIENT_TREE = 'be98bbf057dff6da04de6529c9146b4ea19f5051'


def free_ports():
    with socket.socket() as first, socket.socket() as second:
        first.bind(("127.0.0.1", 0))
        second.bind(("127.0.0.1", 0))
        return first.getsockname()[1], second.getsockname()[1]


def parse_result(label, output, expected_records, max_excess=0):
    rows = [row for row in csv.reader(output.splitlines())
            if len(row) == 10 and re.match(r"\d{4}-", row[0])]
    if len(rows) != 1:
        raise RuntimeError(f"missing unambiguous ConsumerPerformance summary for {label}")
    row = rows[0]
    if not expected_records <= int(row[4].strip()) <= expected_records + max_excess or "WARNING: Exiting before" in output:
        raise RuntimeError(f"consumed count outside the required range in {label}")
    result = {"records": int(row[4]), "fetch_ms": float(row[7]), "fetch_mib_s": float(row[8]),
              "fetch_records_s": float(row[9]), "overall_records_s": float(row[5])}
    if any(not math.isfinite(value) or value <= 0 for value in result.values()):
        raise RuntimeError(f"invalid/nonpositive measurement in {label}")
    return result


def parse_resources(data):
    cpu, rss = data['process_cpu_seconds'], data['maximum_resident_bytes']
    if not math.isfinite(cpu) or cpu <= 0 or not isinstance(rss, int) or rss <= 0:
        raise RuntimeError('Invalid process CPU/RSS measurement')
    return dict(process_cpu_seconds=cpu, maximum_resident_bytes=rss)


def main():
    def terminate(signum, frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, terminate)
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-runtime", type=Path, required=True)
    parser.add_argument("--candidate-runtime", type=Path, required=True)
    parser.add_argument("--java", type=Path, required=True)
    parser.add_argument("--records", type=int, default=50_000_000)
    parser.add_argument("--record-size", type=int, default=256)
    parser.add_argument("--pairs", type=int, default=5)
    parser.add_argument("--baseline-worktree", type=Path, required=True)
    parser.add_argument("--candidate-worktree", type=Path, required=True)
    parser.add_argument("--idle-harness-classes", type=Path)
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--profile-records", type=int, default=5_000_000)
    parser.add_argument("--artifact-parent", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--remove-owned-data", action="store_true",
                        help="After stopping the owned broker, remove only its generated broker-data")
    args = parser.parse_args()
    if min(args.records, args.record_size, args.pairs, args.profile_records) <= 0:
        parser.error("records, record-size, pairs and profile-records must be positive")
    if not args.java.is_file():
        parser.error("java executable not found")

    def git(worktree, *options):
        return subprocess.check_output(["git", "-C", str(worktree), *options], text=True).strip()

    expected_base = "820533b870106cc0e0ac60e2076b8644d68bd85f"
    # Jenkins uses depth=1. Validate the pinned tree without requiring old commit objects.
    expected_candidate_tree = EXPECTED_CANDIDATE_CLIENT_TREE
    if git(args.baseline_worktree, "rev-parse", "HEAD") != expected_base:
        parser.error("baseline revision differs from the predeclared baseline")
    if git(args.candidate_worktree, "rev-parse", "HEAD:clients/src/main") != expected_candidate_tree:
        parser.error("candidate production source differs from the predeclared candidate")
    for worktree in (args.baseline_worktree, args.candidate_worktree):
        if git(worktree, "status", "--porcelain", "--untracked-files=all", "--", "clients/src/main", "tools/src/main"):
            parser.error(f"uncommitted measured source: {worktree}")

    root = Path(tempfile.mkdtemp(prefix="kip1371-throughput-", dir=args.artifact_parent))
    print(f"ARTIFACTS {root}", flush=True)
    runtimes = {
        "baseline": (args.baseline_runtime / "tools-classpath.txt").read_text().strip(),
        "candidate": (args.candidate_runtime / "tools-classpath.txt").read_text().strip(),
        "broker": (args.candidate_runtime / "broker-classpath.txt").read_text().strip(),
    }
    for role, other in (("baseline", args.candidate_worktree), ("candidate", args.baseline_worktree)):
        if str(other.resolve()) in runtimes[role]:
            parser.error(f"{role} runtime contains the other worktree")
        if not all(Path(entry).exists() for entry in runtimes[role].split(":")):
            parser.error(f"{role} runtime contains unbuilt entries")
    broker_port, controller_port = free_ports()
    address = f"127.0.0.1:{broker_port}"
    topic = "kip1371-throughput-" + uuid.uuid4().hex
    config = root / "server.properties"
    config.write_text("\n".join([
        "process.roles=broker,controller", "node.id=1",
        f"listeners=PLAINTEXT://127.0.0.1:{broker_port},CONTROLLER://127.0.0.1:{controller_port}",
        f"advertised.listeners=PLAINTEXT://127.0.0.1:{broker_port}",
        "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "controller.listener.names=CONTROLLER", "inter.broker.listener.name=PLAINTEXT",
        f"controller.quorum.voters=1@127.0.0.1:{controller_port}",
        f"log.dirs={root / 'broker-data'}",
        "offsets.topic.replication.factor=1", "offsets.topic.num.partitions=1",
        "transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1",
        "group.initial.rebalance.delay.ms=0", "auto.create.topics.enable=false",
        "delete.topic.enable=true", "num.partitions=4",
    ]) + "\n")
    consumer_config = root / "consumer.properties"
    consumer_config.write_text("group.protocol=consumer\nenable.auto.commit=true\n"
                               "auto.commit.interval.ms=5000\nauto.offset.reset=earliest\n"
                               "max.poll.records=500\n")
    log_config = root / "log4j2.properties"
    log_config.write_text("status=error\nname=Benchmark\nappender.console.type=Console\n"
                          "appender.console.name=CONSOLE\nappender.console.layout.type=PatternLayout\n"
                          "appender.console.layout.pattern=%d %p %c %m%n\nrootLogger.level=warn\n"
                          "rootLogger.appenderRef.console.ref=CONSOLE\n")
    commands = []

    def java(role, main_class, *options, heap="1g"):
        return [str(args.java), f"-Xms{heap}", f"-Xmx{heap}", "-Dfile.encoding=UTF-8",
                f"-Dlog4j2.configurationFile={log_config}", "-cp", runtimes[role], main_class, *options]

    def run(label, command, timeout=900):
        commands.append({"label": label, "argv": command})
        (root / "commands.json").write_text(json.dumps(commands, indent=2))
        print(f"START {label}", flush=True)
        with (root / f"{label}.stdout").open("w") as out, (root / f"{label}.stderr").open("w") as err:
            process = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
            try:
                returncode = process.wait(timeout=timeout)
            except BaseException:
                # /usr/bin/time has a Java child: stop only this task-owned process group,
                # not merely its wrapper, before tearing down the broker.
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=10)
                raise
        if returncode:
            raise RuntimeError(f"{label} failed ({returncode}); see {root}")
        return (root / f"{label}.stdout").read_text()

    def runtime_fingerprint(classpath):
        fingerprint = {}
        for entry in classpath.split(":"):
            path = Path(entry)
            digest = hashlib.sha256()
            files = sorted(p for p in path.rglob("*") if p.is_file()) if path.is_dir() else [path]
            for child in files:
                digest.update(str(child.relative_to(path) if path.is_dir() else child.name).encode())
                with child.open("rb") as source:
                    for block in iter(lambda: source.read(1024 * 1024), b""):
                        digest.update(block)
            fingerprint[entry] = digest.hexdigest()
        return fingerprint

    manifest = {
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "baseline": expected_base,
        "candidate_head": git(args.candidate_worktree, "rev-parse", "HEAD"),
        "candidate_client_revision": CANDIDATE_REVISION,
        "candidate_behavior_revision": "95095ac064",
        "candidate_client_tree": expected_candidate_tree,
        "records": args.records, "record_size": args.record_size, "pairs": args.pairs,
        "partitions": 4, "broker_address": address, "topic": topic,
        "auto_commit": True, "minimum_fetch_ms": 30_000, "minimum_throughput_ratio": 0.95,
        "classpath_sha256": {k: hashlib.sha256(v.encode()).hexdigest() for k, v in runtimes.items()},
        "runtime_content_sha256": {k: runtime_fingerprint(v) for k, v in runtimes.items()},
        "environment": {"system": platform.system(), "release": platform.release(),
                        "machine": platform.machine(), "python": platform.python_version(),
                        "java_version": subprocess.check_output(
                            [str(args.java), "-version"], stderr=subprocess.STDOUT, text=True).strip()},
    }
    if args.idle_harness_classes:
        manifest["idle_harness_sha256"] = runtime_fingerprint(str(args.idle_harness_classes))
        manifest["idle_ms"] = 60_000
        manifest["first_record_samples"] = 100
        manifest["first_record_idle_ms"] = 250
        manifest["first_record_p99_allowance_ms"] = 10
        manifest["idle_cpu_allowance_percentage_points"] = 0.2
    manifest["environment"]["load_average_at_start"] = list(os.getloadavg())
    manifest["environment"]["exclusive_host"] = False
    if args.profile:
        manifest["profile_settings_sha256"] = hashlib.sha256(
            Path(__file__).with_name("profile.jfc").read_bytes()).hexdigest()
    if platform.system() == "Darwin":
        hardware = subprocess.check_output(
            ["/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string", "hw.memsize", "hw.logicalcpu"], text=True).splitlines()
        manifest["environment"]["cpu"] = hardware[0]
        manifest["environment"]["physical_memory_bytes"] = int(hardware[1])
        manifest["environment"]["logical_cpus"] = int(hardware[2])
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2))
    cluster_id = run("cluster-id", java("broker", "kafka.tools.StorageTool", "random-uuid")).strip()
    run("format", java("broker", "kafka.tools.StorageTool", "format", "-t", cluster_id, "-c", str(config)))
    broker_log = (root / "broker.log").open("w")
    broker = subprocess.Popen(java("broker", "kafka.Kafka", str(config), heap="2g"),
                              stdout=broker_log, stderr=subprocess.STDOUT)
    (root / "broker.pid").write_text(str(broker.pid))
    topic_created = False
    results = []
    try:
        deadline = time.monotonic() + 90
        while True:
            if broker.poll() is not None:
                raise RuntimeError("task-owned broker exited during startup")
            try:
                with socket.create_connection(("127.0.0.1", broker_port), timeout=1):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("broker listener did not become ready")
                time.sleep(0.2)
        identity = run("verify-owned-cluster", java("candidate", "org.apache.kafka.tools.ClusterTool",
                                                   "cluster-id", "--bootstrap-server", address))
        if f"Cluster ID: {cluster_id}" not in identity.splitlines() or broker.poll() is not None:
            raise RuntimeError("listener is not the live task-owned cluster; refusing topic writes")
        run("create-topic", java("candidate", "org.apache.kafka.tools.TopicCommand", "--bootstrap-server", address,
                                 "--create", "--topic", topic, "--partitions", "4", "--replication-factor", "1"))
        topic_created = True
        run("seed", java("candidate", "org.apache.kafka.tools.ProducerPerformance", "--topic", topic,
                         "--num-records", str(args.records), "--record-size", str(args.record_size),
                         "--throughput", "-1", "--producer-props", f"bootstrap.servers={address}",
                         "acks=all", "compression.type=none", "batch.size=65536", "linger.ms=5"), timeout=1800)
        offsets = run("end-offsets", java("candidate", "org.apache.kafka.tools.GetOffsetShell",
                                          "--bootstrap-server", address, "--topic", topic, "--time", "-1"))
        actual = sum(int(line.rsplit(":", 1)[1]) for line in offsets.splitlines() if line.startswith(topic + ":"))
        if actual != args.records:
            raise RuntimeError(f"seed count mismatch: {actual} != {args.records}")
        for pair in range(args.pairs):
            for role in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
                label = f"pair-{pair + 1}-{role}"
                command = java(role, "org.apache.kafka.tools.ConsumerPerformance", "--bootstrap-server", address,
                               "--topic", topic, "--num-records", str(args.records), "--group", topic + "-" + label,
                               "--command-config", str(consumer_config), "--timeout", "60000", "--print-metrics")
                if platform.system() == 'Linux':
                    resource_file = root / f'{label}.resources.json'
                    output = run(label, [sys.executable, str(Path(__file__).with_name('resource-time.py')),
                                         str(resource_file), *command], timeout=1800)
                    resources = parse_resources(json.loads(resource_file.read_text()))
                elif platform.system() == 'Darwin':
                    output = run(label, ["/usr/bin/time", "-l", *command], timeout=1800)
                    timing = (root / f"{label}.stderr").read_text()
                    cpu = re.search(r"([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys", timing)
                    rss = re.search(r"(\d+)\s+maximum resident set size", timing)
                    if not cpu or not rss:
                        raise RuntimeError(f"missing macOS process timing/RSS in {label}")
                    resources = parse_resources(dict(process_cpu_seconds=float(cpu[2]) + float(cpu[3]),
                                                     maximum_resident_bytes=int(rss[1])))
                else:
                    raise RuntimeError('Only Linux and macOS resource measurement is supported')
                result = {"pair": pair + 1, "role": role, **parse_result(label, output, args.records)}
                result.update(resources)
                results.append(result)
                (root / "results.json").write_text(json.dumps(results, indent=2))
                print(f"RESULT {json.dumps(result)}", flush=True)
        base = [r["fetch_records_s"] for r in results if r["role"] == "baseline"]
        candidate = [r["fetch_records_s"] for r in results if r["role"] == "candidate"]
        median = lambda values: statistics.median(values)
        mad = lambda values: median([abs(v - median(values)) for v in values])
        ratio = median(candidate) / median(base)
        summary = {"baseline_median": median(base), "baseline_mad": mad(base),
                   "candidate_median": median(candidate), "candidate_mad": mad(candidate),
                   "ratio": ratio, "paired_ratios": [c / b for b, c in zip(base, candidate)],
                   "sufficient_duration": all(r["fetch_ms"] >= 30_000 for r in results)}
        summary["gate"] = "inconclusive-short" if not summary["sufficient_duration"] else (
            "inconclusive-pairs" if args.pairs < 5 else ("pass" if ratio >= 0.95 else "regression"))
        # Whole CLI process costs include startup/close; do not label them fetch-only CPU.
        summary["whole_process_resources"] = {
            role: {
                metric: {"median": median([r[metric] for r in results if r["role"] == role]),
                         "mad": mad([r[metric] for r in results if r["role"] == role])}
                for metric in ("process_cpu_seconds", "maximum_resident_bytes")
            } for role in ("baseline", "candidate")
        }
        (root / "summary.json").write_text(json.dumps(summary, indent=2))
        print(f"SUMMARY {json.dumps(summary)}", flush=True)
        if args.idle_harness_classes:
            samples = []
            for pair in range(args.pairs):
                for role in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
                    for scenario in ("idle", "first-record"):
                        label = f"{scenario}-{pair + 1}-{role}"
                        options = ["--bootstrap-server", address, "--scenario", scenario]
                        options += (["--duration-ms", "60000"] if scenario == "idle" else
                                    ["--samples", "100", "--warmup-samples", "10", "--idle-ms", "250"])
                        command = java(role, "org.apache.kafka.tools.reactorbenchmark.IdleWakeHarness", *options)
                        cp_index = command.index("-cp") + 1
                        command[cp_index] = str(args.idle_harness_classes) + ":" + runtimes[role]
                        output = run(label, command, timeout=300)
                        rows = [line.split(",") for line in output.splitlines() if line.startswith("RESULT,")]
                        if len(rows) != 1 or len(rows[0]) != 14 or rows[0][1] != scenario:
                            raise RuntimeError(f"incomplete harness output in {label}")
                        row = rows[0]
                        if int(row[5]) != (0 if scenario == "idle" else 100):
                            raise RuntimeError(f"wrong record count in {label}")
                        if not all(math.isfinite(float(row[i])) and float(row[i]) >= 0 for i in (2, 3, 4)):
                            raise RuntimeError(f"invalid idle/latency timing in {label}")
                        if scenario == "first-record" and not math.isfinite(float(row[10])):
                            raise RuntimeError(f"invalid latency in {label}")
                        samples.append({"pair": pair + 1, "role": role, "scenario": scenario,
                                        "wall_ms": float(row[2]), "cpu_ms": float(row[3]),
                                        "cpu_percent": float(row[4]),
                                        "p99_ms": float(row[10]) if scenario == "first-record" else None,
                                        "estimated_network_poll_hz": float(row[13]) if math.isfinite(float(row[13])) else None})
                        (root / "idle-first-record-results.json").write_text(json.dumps(samples, indent=2))
                        print(f"RESULT {json.dumps(samples[-1])}", flush=True)
            small_summary = {}
            for scenario, metric, allowance in (("idle", "cpu_percent", 0.2), ("first-record", "p99_ms", 10)):
                values = {role: [r[metric] for r in samples if r["scenario"] == scenario and r["role"] == role]
                          for role in ("baseline", "candidate")}
                delta = median(values["candidate"]) - median(values["baseline"])
                small_summary[scenario] = {"metric": metric, "baseline_median": median(values["baseline"]),
                                           "candidate_median": median(values["candidate"]),
                                           "baseline_mad": mad(values["baseline"]), "candidate_mad": mad(values["candidate"]),
                                           "delta": delta, "allowance": allowance,
                                           "gate": "inconclusive-pairs" if args.pairs < 5 else (
                                               "pass" if delta <= allowance else "regression")}
            (root / "idle-first-record-summary.json").write_text(json.dumps(small_summary, indent=2))
            print(f"IDLE_LATENCY_SUMMARY {json.dumps(small_summary)}", flush=True)
        if args.profile:
            profile_results = []
            for role in ("baseline", "candidate"):
                profile_records = min(args.records, args.profile_records)
                recording = root / f"{role}.jfr"
                command = java(role, "org.apache.kafka.tools.ConsumerPerformance", "--bootstrap-server", address,
                               "--topic", topic, "--num-records", str(profile_records),
                               "--group", topic + "-profile-" + role,
                               "--command-config", str(consumer_config), "--timeout", "60000")
                settings = Path(__file__).with_name("profile.jfc").resolve()
                command.insert(1, f"-XX:StartFlightRecording=filename={recording},settings={settings},dumponexit=true")
                output = run(f"{role}-profile", command)
                # ConsumerPerformance finishes a poll batch, so a partial-dataset target
                # can be exceeded by at most max.poll.records - 1 (configured above as 500).
                # Whole-dataset throughput measurements remain exact-count checks.
                profile_results.append({"role": role, "requested_records": profile_records,
                                        **parse_result(f"{role}-profile", output, profile_records,
                                                       min(499, args.records - profile_records))})
                (root / "profile-results.json").write_text(json.dumps(profile_results, indent=2))
                run(f"jfr-summary-{role}", [str(args.java.with_name("jfr")), "summary", str(recording)])
    finally:
        try:
            if topic_created and broker.poll() is None:
                run("delete-owned-topic", java("candidate", "org.apache.kafka.tools.TopicCommand",
                                               "--bootstrap-server", address, "--delete", "--topic", topic), timeout=90)
        finally:
            broker.terminate()
            try:
                broker.wait(timeout=60)
            except subprocess.TimeoutExpired:
                broker.kill()
                broker.wait(timeout=10)
            broker_log.close()
            if args.remove_owned_data and (root / 'broker-data').exists():
                shutil.rmtree(root / 'broker-data')
            print(f"STOPPED task-owned broker {broker.pid}; artifacts retained at {root}", flush=True)


if __name__ == "__main__":
    main()
