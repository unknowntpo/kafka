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

from kafkatest.services.performance import ConsumerPerformanceService


class GroupPerNodeConsumerPerformanceService(ConsumerPerformanceService):
    """
    Each node reads the whole topic in its own group, so N nodes put N times the read load
    on the machine. Extra JVM options are passed to the tool, and the fetch-only rate is kept.
    """

    def __init__(self, context, num_nodes, kafka, topic, messages, group_prefix, jvm_opts="", settings={}):
        super(GroupPerNodeConsumerPerformanceService, self).__init__(
            context, num_nodes, kafka, topic=topic, messages=messages, settings=settings)
        self.group_prefix = group_prefix
        self.jvm_opts = jvm_opts
        self.fetch_mbps = [None] * num_nodes

    def start_cmd(self, node):
        self.group = "%s-%d" % (self.group_prefix, self.idx(node))
        cmd = super(GroupPerNodeConsumerPerformanceService, self).start_cmd(node)
        script = self.path.script("kafka-consumer-perf-test.sh", node)
        return cmd.replace(script, "KAFKA_OPTS=\"$KAFKA_OPTS %s\" %s" % (self.jvm_opts, script), 1)

    def _worker(self, idx, node):
        super(GroupPerNodeConsumerPerformanceService, self)._worker(idx, node)
        last = node.account.ssh_output("tail -1 %s" % ConsumerPerformanceService.STDOUT_CAPTURE).decode()
        # start, end, MB, MB/s, records, records/s, rebalance ms, fetch ms, fetch MB/s, fetch records/s
        self.fetch_mbps[idx - 1] = float(last.split(',')[8])
