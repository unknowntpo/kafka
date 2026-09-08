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
from ducktape.tests.test import Test

from kafkatest.benchmarks.core.benchmark_test import TOPIC_REP_THREE, DEFAULT_RECORD_SIZE
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.performance import ProducerPerformanceService, ConsumerPerformanceService, \
    compute_aggregate_throughput
from kafkatest.version import DEV_BRANCH, KafkaVersion

TOPIC_ONE_PARTITION = "test-rep-one-p1"


def consumer_performance_with_protocol(context, kafka, topic, messages, group_protocol, max_poll_records):
    """A ConsumerPerformanceService whose command line selects the group protocol and the poll batch size through
    --command-property. The override is per instance (not a subclass) so that ducktape keeps resolving the
    service's templates from the kafkatest.services.performance package."""
    service = ConsumerPerformanceService(context, 1, kafka, topic=topic, messages=messages)
    base_args = service.args

    def args_with_protocol(version):
        # start_cmd renders each entry as "--<key> <value>"; a flag with an empty value renders as "--<key> ".
        args = base_args(version)
        args['command-property group.protocol=%s' % group_protocol] = ""
        args['command-property max.poll.records=%d' % max_poll_records] = ""
        return args

    service.args = args_with_protocol
    return service


class ConsumerProtocolBenchmark(Test):
    """Consumer throughput of the classic and the consumer (KIP-848) group protocols on the same revision, so two
    builds of this test on two revisions compare the implementations behind group.protocol=consumer while the
    classic consumer in each build serves as an in-build control. Consumes 10e6 100-byte records from a
    1-partition and a 6-partition topic with max.poll.records 500 (default) and 50 (high poll rate).

    Extends Test rather than Benchmark on purpose: ducktape discovers every test_* method of a class, so a
    Benchmark subclass would also run the whole inherited producer/consumer matrix (74 extra cases)."""

    def __init__(self, test_context):
        super(ConsumerProtocolBenchmark, self).__init__(test_context)
        self.num_brokers = 3
        self.topics = {
            TOPIC_REP_THREE: {'partitions': 6, 'replication-factor': 3},
            TOPIC_ONE_PARTITION: {'partitions': 1, 'replication-factor': 1}
        }

    def start_kafka(self, security_protocol, interbroker_security_protocol, version):
        self.kafka = KafkaService(
            self.test_context, self.num_brokers,
            zk=None, security_protocol=security_protocol,
            interbroker_security_protocol=interbroker_security_protocol, topics=self.topics,
            version=version)
        self.kafka.log_level = "INFO"
        self.kafka.start()

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

        self.consumer = consumer_performance_with_protocol(
            self.test_context, self.kafka, topic, num_records, group_protocol, max_poll_records)
        self.consumer.group = "protocol-benchmark-%s-p%d-mpr%d" % (group_protocol, partitions, max_poll_records)
        self.consumer.run()
        return compute_aggregate_throughput(self.consumer)
