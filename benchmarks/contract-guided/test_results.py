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

import importlib.util
from pathlib import Path
import unittest
import subprocess
import sys
import tempfile
import json

spec = importlib.util.spec_from_file_location("throughput", Path(__file__).with_name("run-throughput.py"))
throughput = importlib.util.module_from_spec(spec)
spec.loader.exec_module(throughput)
jfr_spec = importlib.util.spec_from_file_location("jfr_summary", Path(__file__).with_name("summarize-jfr.py"))
jfr_summary = importlib.util.module_from_spec(jfr_spec)
jfr_spec.loader.exec_module(jfr_summary)


class ResultTest(unittest.TestCase):
    ROW = "2026-09-06 00:00:00:000,2026-09-06 00:01:00:000,244.14,4.07,1000000,16666.67,1000,59000,4.14,16949.15"

    def test_complete(self):
        result = throughput.parse_result("case", "header\n" + self.ROW + "\nmetric : 42", 1000000)
        self.assertEqual(1000000, result["records"])
        self.assertEqual(59000, result["fetch_ms"])

    def test_short_read(self):
        with self.assertRaises(RuntimeError):
            throughput.parse_result("case", self.ROW, 1000001)

    def test_partial_profile_allows_only_one_poll_overshoot(self):
        result = throughput.parse_result("profile", self.ROW, 999786, max_excess=499)
        self.assertEqual(1000000, result["records"])
        for target in (1000001, 999500):
            with self.assertRaises(RuntimeError):
                throughput.parse_result("profile", self.ROW, target, max_excess=499)

    def test_warning(self):
        with self.assertRaises(RuntimeError):
            throughput.parse_result("case", self.ROW + "\nWARNING: Exiting before consuming", 1000000)

    def test_missing_or_ambiguous(self):
        for output in ("", self.ROW + "\n" + self.ROW):
            with self.assertRaises(RuntimeError):
                throughput.parse_result("case", output, 1000000)

    def test_nonfinite_or_zero(self):
        for value in ("NaN", "Infinity", "0", "-1"):
            with self.assertRaises(RuntimeError):
                throughput.parse_result("case", self.ROW.replace("16949.15", value), 1000000)

    def test_process_resources(self):
        self.assertEqual(22.12, throughput.parse_resources({
            'process_cpu_seconds': 22.12, 'maximum_resident_bytes': 1048576})['process_cpu_seconds'])
        for cpu, rss in [(float('nan'), 1), (-1, 1), (0, 1), (1, 0), (1, '1024')]:
            with self.assertRaises(RuntimeError):
                throughput.parse_resources({'process_cpu_seconds': cpu, 'maximum_resident_bytes': rss})

    @unittest.skipUnless(sys.platform == 'linux', 'Linux wait4 RSS conversion')
    def test_real_linux_child_resources_and_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'resources.json'
            helper = str(Path(__file__).with_name('resource-time.py'))
            result = subprocess.run([sys.executable, helper, str(output), sys.executable, '-c',
                                     'import sys; sum(range(1000000)); sys.exit(7)'])
            self.assertEqual(7, result.returncode)
            usage = json.loads(output.read_text())
            self.assertEqual(7, usage['exit_code'])
            self.assertGreater(throughput.parse_resources(usage)['maximum_resident_bytes'], 1024 * 1024)


class JfrSummaryTest(unittest.TestCase):
    def test_rejects_unrelated_metadata(self):
        with self.assertRaises(ValueError):
            jfr_summary.summarize([{"type": "jdk.InitialEnvironmentVariable", "values": {}}])

    def test_duration(self):
        self.assertAlmostEqual(0.012, jfr_summary.seconds("PT0.012S"))
        self.assertEqual(62, jfr_summary.seconds("PT1M2S"))

    def test_cpu_allocation_and_gc(self):
        stack = {"stackTrace": {"frames": [{"method": {"type": {"name": "owner/Type"}, "name": "work"}}]}}
        summary, folded = jfr_summary.summarize([
            {"type": "jdk.ExecutionSample", "values": stack},
            {"type": "jdk.ObjectAllocationSample", "values": {**stack, "weight": 128}},
            {"type": "jdk.GarbageCollection", "values": {"sumOfPauses": "PT0.002S"}},
        ])
        self.assertEqual(1, folded["owner.Type.work"])
        self.assertEqual(128, summary["total_allocation_sample_weight_bytes"])
        self.assertAlmostEqual(0.002, summary["gc_pause_seconds"])


if __name__ == "__main__":
    unittest.main()
