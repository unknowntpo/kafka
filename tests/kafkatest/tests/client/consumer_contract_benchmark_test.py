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

import shlex
import uuid

from ducktape.mark.resource import cluster
from ducktape.services.background_thread import BackgroundThreadService
from ducktape.tests.test import Test


class ContractBenchmarkService(BackgroundThreadService):
    def __init__(self, context, profile='formal'):
        super(ContractBenchmarkService, self).__init__(context, 1)
        if profile not in ('formal', 'smoke'):
            raise ValueError('Unknown contract benchmark profile')
        self.profile = profile
        self.root = '/mnt/kip1371-contract-' + uuid.uuid4().hex
        self.logs = {'contract_benchmark': {'path': self.root, 'collect_default': True}}

    def _worker(self, idx, node):
        command = ['python3', '/opt/kafka-dev/benchmarks/contract-guided/jenkins-entry.py',
                   '--repository', '/opt/kafka-dev', '--artifacts', self.root, '--profile', self.profile]
        for line in node.account.ssh_capture(' '.join(shlex.quote(value) for value in command)):
            self.logger.info(line.strip())

    def clean_node(self, node):
        # Unique directory, retained for Ducktape artifact collection (also on failure).
        pass

    def stop_node(self, node):
        # Verify this exact invocation before terminating; never kill arbitrary Java processes.
        code = """
import os, pathlib, signal, sys
root = pathlib.Path(sys.argv[1])
pidfile = root / 'entry.pid'
if pidfile.exists():
    pid = int(pidfile.read_text())
    command = pathlib.Path('/proc/%s/cmdline' % pid)
    if command.exists():
        args = command.read_bytes().split(b'\\0')
        if str(root).encode() in args and b'/opt/kafka-dev/benchmarks/contract-guided/jenkins-entry.py' in args:
            os.kill(pid, signal.SIGTERM)
"""
        node.account.ssh('python3 -c %s %s' % (shlex.quote(code), shlex.quote(self.root)), allow_fail=True)


class ConsumerContractBenchmarkTest(Test):
    @cluster(num_nodes=1)
    def test_paired_throughput(self):
        """One job, one worker, five AB/BA JVM pairs and separate full-dataset JFRs."""
        profile = self.test_context.globals.get('contract_benchmark_profile', 'formal')
        benchmark = ContractBenchmarkService(self.test_context, profile)
        benchmark.start()
        # Two one-hour build limits plus 90 minutes benchmark and teardown margin.
        try:
            benchmark.wait(timeout_sec=13200)
        finally:
            benchmark.stop()
