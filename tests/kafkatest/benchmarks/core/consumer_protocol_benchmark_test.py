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

from ducktape.mark import matrix
from ducktape.mark.resource import cluster

from kafkatest.benchmarks.core.benchmark_test import Benchmark, TOPIC_REP_THREE, DEFAULT_RECORD_SIZE
from kafkatest.services.kafka import quorum
from kafkatest.services.performance import ProducerPerformanceService, ConsumerPerformanceService, \
    compute_aggregate_throughput
from kafkatest.version import DEV_BRANCH, KafkaVersion

TOPIC_ONE_PARTITION = "test-rep-one-p1"


class ProtocolConsumerPerformanceService(ConsumerPerformanceService):
    """ConsumerPerformanceService that selects the consumer group protocol and the poll batch size through
    --command-property, which the tool forwards to the consumer configuration."""

    def __init__(self, context, num_nodes, kafka, topic, messages, group_protocol, max_poll_records,
                 version=DEV_BRANCH):
        super(ProtocolConsumerPerformanceService, self).__init__(context, num_nodes, kafka, topic, messages,
                                                                 version=version)
        self.group_protocol = group_protocol
        self.max_poll_records = max_poll_records

    def args(self, version):
        # start_cmd renders each entry as "--<key> <value>"; a flag with an empty value renders as "--<key> ".
        args = super(ProtocolConsumerPerformanceService, self).args(version)
        args['command-property group.protocol=%s' % self.group_protocol] = ""
        args['command-property max.poll.records=%d' % self.max_poll_records] = ""
        return args


class ConsumerProtocolBenchmark(Benchmark):
    """Consumer throughput of the classic and the consumer (KIP-848) group protocols on the same revision, so two
    builds of this test on two revisions compare the implementations behind group.protocol=consumer while the
    classic consumer in each build serves as an in-build control. Consumes 10e6 100-byte records from a
    1-partition and a 6-partition topic with max.poll.records 500 (default) and 50 (high poll rate)."""

    def __init__(self, test_context):
        super(ConsumerProtocolBenchmark, self).__init__(test_context)
        self.topics[TOPIC_ONE_PARTITION] = {'partitions': 1, 'replication-factor': 1}

    @cluster(num_nodes=8)
    @matrix(group_protocol=['classic', 'consumer'], partitions=[1, 6], max_poll_records=[500, 50],
            metadata_quorum=[quorum.isolated_kraft])
    def test_consumer_throughput_by_protocol(self, group_protocol, partitions, max_poll_records,
                                             metadata_quorum=quorum.isolated_kraft):
        client_version = KafkaVersion(str(DEV_BRANCH))
        self.start_kafka('PLAINTEXT', 'PLAINTEXT', client_version)
        topic = TOPIC_ONE_PARTITION if partitions == 1 else TOPIC_REP_THREE
        num_records = 10 * 1000 * 1000

        self.producer = ProducerPerformanceService(
            self.test_context, 1, self.kafka,
            topic=topic,
            num_records=num_records, record_size=DEFAULT_RECORD_SIZE, throughput=-1, version=client_version,
            settings={
                'acks': 1,
                'compression.type': 'none',
                'batch.size': self.batch_size,
                'buffer.memory': self.buffer_memory
            }
        )
        self.producer.run()

        self.consumer = ProtocolConsumerPerformanceService(
            self.test_context, 1, self.kafka,
            topic=topic, messages=num_records,
            group_protocol=group_protocol, max_poll_records=max_poll_records)
        self.consumer.group = "protocol-benchmark-%s-p%d-mpr%d" % (group_protocol, partitions, max_poll_records)
        self.consumer.run()
        return compute_aggregate_throughput(self.consumer)
