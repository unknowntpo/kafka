#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""wait4 measures only this Java child, not previous JVMs or the Python wrapper."""
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    if sys.platform != 'linux':
        raise RuntimeError('Linux ru_maxrss is KiB; do not use this conversion on macOS')
    child = subprocess.Popen(sys.argv[2:])
    _, status, usage = os.wait4(child.pid, 0)
    child.returncode = os.waitstatus_to_exitcode(status)
    Path(sys.argv[1]).write_text(json.dumps({
        'process_cpu_seconds': usage.ru_utime + usage.ru_stime,
        'maximum_resident_bytes': usage.ru_maxrss * 1024,
        'exit_code': child.returncode,
    }))
    return child.returncode if child.returncode >= 0 else 128 - child.returncode


if __name__ == '__main__':
    sys.exit(main())
