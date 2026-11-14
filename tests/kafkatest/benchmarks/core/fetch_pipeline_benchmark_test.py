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

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test

from kafkatest.services.kafka import KafkaService
from kafkatest.services.performance import ProducerPerformanceService, ConsumerPerformanceService
from kafkatest.version import DEV_BRANCH

GB = 1024 * 1024 * 1024


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


class FetchPipelineBenchmark(Test):
    """
    Consumer throughput on data that is already in the page cache.
    One cluster and one data set per record size are shared by all variants, so the variants
    of one run are comparable. Compare revisions by running the same method on each revision.
    """

    TOPIC = "fetch-pipeline"
    PARTITIONS = 12
    NUM_PRODUCERS = 3
    REPS = 3
    # (record size, total bytes)
    WORKLOADS = [(100, 10 * GB), (1000, 20 * GB)]

    def __init__(self, test_context):
        super(FetchPipelineBenchmark, self).__init__(test_context)
        self.kafka = KafkaService(test_context, 3, zk=None)
        self.kafka.log_level = "INFO"

    def log_machine(self):
        node = self.kafka.nodes[0]
        out = node.account.ssh_output("nproc; free -g | sed -n 2p; df -h /mnt | tail -1").decode()
        self.logger.info("machine: %s" % out.replace("\n", " | "))

    def produce(self, record_size, total_bytes):
        per_producer = total_bytes // record_size // self.NUM_PRODUCERS
        self.kafka.create_topic({"topic": self.TOPIC, "partitions": self.PARTITIONS, "replication-factor": 1})
        producer = ProducerPerformanceService(
            self.test_context, self.NUM_PRODUCERS, self.kafka, topic=self.TOPIC,
            num_records=per_producer, record_size=record_size, throughput=-1, version=DEV_BRANCH,
            settings={"acks": 1, "batch.size": 64 * 1024, "linger.ms": 5, "buffer.memory": 128 * 1024 * 1024})
        producer.run()
        producer.free()
        return per_producer * self.NUM_PRODUCERS

    def consume(self, name, num_records, num_consumers, jvm_opts):
        """Run REPS times and return the median of the summed rates."""
        total, fetch = [], []
        for rep in range(self.REPS):
            consumer = GroupPerNodeConsumerPerformanceService(
                self.test_context, num_consumers, self.kafka, topic=self.TOPIC,
                # stop a little early so that a consumer never waits for the poll timeout
                messages=int(num_records * 0.99),
                group_prefix="%s-%d" % (name, rep), jvm_opts=jvm_opts,
                settings={"group.protocol": "consumer"})
            consumer.run()
            total.append(sum(r["mbps"] for r in consumer.results))
            fetch.append(sum(consumer.fetch_mbps))
            consumer.free()
        self.logger.info("%s: MB/s per rep %s, fetch MB/s per rep %s" % (name, total, fetch))
        return {"mb_per_sec": sorted(total)[self.REPS // 2], "fetch_mb_per_sec": sorted(fetch)[self.REPS // 2]}

    def run_variants(self, variants):
        """variants: list of (name, number of consumers, JVM options)."""
        self.kafka.start()
        self.log_machine()
        data = {}
        for record_size, total_bytes in self.WORKLOADS:
            num_records = self.produce(record_size, total_bytes)
            for name, num_consumers, jvm_opts in variants:
                key = "%dB-%s" % (record_size, name)
                data[key] = self.consume(key, num_records, num_consumers, jvm_opts)
            self.kafka.delete_topic(self.TOPIC)
        self.logger.info("result: %s" % data)
        return data

    @cluster(num_nodes=14)
    def test_consumer_scaling(self):
        """1, 2, 4 and 8 independent consumers. Runs on any revision."""
        return self.run_variants([("c%d" % n, n, "") for n in (1, 2, 4, 8)])

    @cluster(num_nodes=14)
    def test_decode_prototype(self):
        """
        Measurement-only switches from FETCH-PIPELINE-06-lean-decode-prototype.patch.
        They do nothing on a revision without that patch.
        """
        return self.run_variants([
            ("base", 1, ""),
            ("lean", 1, "-Dfp.lean=true"),
            ("decode2", 1, "-Dfp.decode.threads=2"),
            ("decode4", 1, "-Dfp.decode.threads=4"),
            ("lean-decode4", 1, "-Dfp.lean=true -Dfp.decode.threads=4"),
            ("lean-c4", 4, "-Dfp.lean=true"),
        ])
