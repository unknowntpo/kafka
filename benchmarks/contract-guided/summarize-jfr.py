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

"""Export only performance events, never JVM environment/process metadata.

CPU sample counts and allocation weights are diagnostics over the entire recorded
process, including startup. They are not exact allocation counters or acceptance gates.
"""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import subprocess


EVENTS = {"jdk.ExecutionSample", "jdk.NativeMethodSample", "jdk.ObjectAllocationSample",
          "jdk.GarbageCollection", "jdk.GCHeapSummary", "jdk.ThreadPark",
          "jdk.JavaMonitorEnter", "jdk.ThreadCPULoad"}


def frames(values):
    trace = values.get("stackTrace") or {}
    result = []
    for frame in trace.get("frames", []):
        method = frame.get("method") or {}
        owner = (method.get("type") or {}).get("name", "unknown").replace("/", ".")
        result.append(owner + "." + method.get("name", "unknown"))
    return result


def seconds(duration):
    match = re.fullmatch(r"PT(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?", duration)
    if not match:
        raise ValueError(f"Unsupported JFR duration: {duration}")
    return sum(float(value or 0) * scale for value, scale in zip(match.groups(), (3600, 60, 1)))


def summarize(events):
    counts, cpu, folded, allocation = Counter(), Counter(), Counter(), Counter()
    gc_pause_seconds = 0.0
    for event in events:
        kind = event["type"]
        if kind not in EVENTS:
            raise ValueError("Unexpected event type in allowlisted JFR export")
        counts[kind] += 1
        values = event["values"]
        stack = frames(values)
        if kind == "jdk.ExecutionSample" and stack:
            cpu[stack[0]] += 1
            folded[";".join(reversed(stack))] += 1
        if kind == "jdk.ObjectAllocationSample":
            allocation[stack[0] if stack else "unknown"] += int(values["weight"])
        if kind == "jdk.GarbageCollection":
            gc_pause_seconds += seconds(values["sumOfPauses"])
    return {"event_counts": dict(counts), "java_cpu_leaf_samples": cpu.most_common(30),
            "allocation_sample_weights_by_leaf": allocation.most_common(30),
            "total_allocation_sample_weight_bytes": sum(allocation.values()),
            "gc_pause_seconds": gc_pause_seconds,
            "scope": "whole recorded process; sampled diagnostics, not exact per-record allocation"}, folded


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("recording", type=Path)
    parser.add_argument("--jfr", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = subprocess.check_output([str(args.jfr), "print", "--json", "--events",
                                   ",".join(sorted(EVENTS)), str(args.recording)], text=True)
    document = json.loads(raw)
    summary, folded = summarize(document["recording"]["events"])
    summary["source_recording_sha256"] = hashlib.sha256(args.recording.read_bytes()).hexdigest()
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "events.json").write_text(json.dumps(document))
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2))
    (args.output / "java-cpu.folded").write_text("".join(f"{stack} {count}\n" for stack, count in folded.items()))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
