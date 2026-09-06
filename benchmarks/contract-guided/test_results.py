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

spec = importlib.util.spec_from_file_location("throughput", Path(__file__).with_name("run-throughput.py"))
throughput = importlib.util.module_from_spec(spec)
spec.loader.exec_module(throughput)


class ResultTest(unittest.TestCase):
    ROW = "2026-09-06 00:00:00:000,2026-09-06 00:01:00:000,244.14,4.07,1000000,16666.67,1000,59000,4.14,16949.15"

    def test_complete(self):
        result = throughput.parse_result("case", "header\n" + self.ROW + "\nmetric : 42", 1000000)
        self.assertEqual(1000000, result["records"])
        self.assertEqual(59000, result["fetch_ms"])

    def test_short_read(self):
        with self.assertRaises(RuntimeError):
            throughput.parse_result("case", self.ROW, 1000001)

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


if __name__ == "__main__":
    unittest.main()
