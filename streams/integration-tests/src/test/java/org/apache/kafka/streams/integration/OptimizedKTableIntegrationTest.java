/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.FixedPartitionPartitioner;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StandbyUpdateListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.NoRetryException;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class OptimizedKTableIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(OptimizedKTableIntegrationTest.class);
    private static final int NUM_BROKERS = 1;
    private static int port = 0;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final String TABLE_NAME = "source-table";

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private final List<KafkaStreams> streamsToCleanup = new ArrayList<>();
    private final MockTime mockTime = CLUSTER.time;

    @BeforeEach
    public void before() throws InterruptedException {
        CLUSTER.createTopic(INPUT_TOPIC_NAME, 2, 1);
    }

    @AfterEach
    public void after() {
        for (final KafkaStreams kafkaStreams : streamsToCleanup) {
            kafkaStreams.close(Duration.ofSeconds(60));
        }
    }

    @Test
    public void shouldApplyUpdatesToStandbyStore(final TestInfo testInfo) throws Exception {
        final int batch1NumMessages = 100;
        final int batch2NumMessages = 100;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .table(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()),
                Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as(TABLE_NAME)
                    .withCachingDisabled())
            .toStream()
            .peek((k, v) -> semaphore.release());

        final String safeTestName = safeUniqueTestName(testInfo);
        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration(safeTestName));
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration(safeTestName));
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        try {
            startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

            produceValueRange(INPUT_TOPIC_NAME, key, 0, batch1NumMessages);

            // Assert that all messages in the first batch were processed in a timely manner
            assertTrue(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS));

            final AtomicReference<ReadOnlyKeyValueStore<Integer, Integer>> newActiveStore = new AtomicReference<>(null);
            TestUtils.retryOnExceptionWithTimeout(() -> {
                final ReadOnlyKeyValueStore<Integer, Integer> store1 = IntegrationTestUtils.getStore(TABLE_NAME, kafkaStreams1, QueryableStoreTypes.keyValueStore());
                final ReadOnlyKeyValueStore<Integer, Integer> store2 = IntegrationTestUtils.getStore(TABLE_NAME, kafkaStreams2, QueryableStoreTypes.keyValueStore());

                final KeyQueryMetadata keyQueryMetadata = kafkaStreams1.queryMetadataForKey(TABLE_NAME, key, new FixedPartitionPartitioner<>(0));

                try {
                    // Assert that the current value in store reflects all messages being processed
                    if ((keyQueryMetadata.activeHost().port() % 2) == 1) {
                        assertEquals(batch1NumMessages - 1, store1.get(key));
                        kafkaStreams1.close();
                        newActiveStore.set(store2);
                    } else {
                        assertEquals(batch1NumMessages - 1, store2.get(key));
                        kafkaStreams2.close();
                        newActiveStore.set(store1);
                    }
                } catch (final InvalidStateStoreException e) {
                    LOG.warn("Detected an unexpected rebalance during test. Retrying if possible.", e);
                    throw e;
                } catch (final Throwable t) {
                    LOG.error("Caught non-retriable exception in test. Exiting.", t);
                    throw new NoRetryException(t);
                }
            });

            // Wait for failover
            TestUtils.retryOnExceptionWithTimeout(60 * 1000, 100, () -> {
                // Assert that after failover we have recovered to the last store write
                assertEquals(batch1NumMessages - 1, newActiveStore.get().get(key));
            });

            final int totalNumMessages = batch1NumMessages + batch2NumMessages;

            produceValueRange(INPUT_TOPIC_NAME, key, batch1NumMessages, totalNumMessages);

            // Assert that all messages in the second batch were processed in a timely manner
            assertTrue(semaphore.tryAcquire(batch2NumMessages, 60, TimeUnit.SECONDS));

            TestUtils.retryOnExceptionWithTimeout(60 * 1000, 100, () -> {
                // Assert that the current value in store reflects all messages being processed
                assertEquals(totalNumMessages - 1, newActiveStore.get().get(key));
            });
        } finally {
            kafkaStreams1.close();
            kafkaStreams2.close();
        }
    }

    @Test
    public void shouldNotFetchBeyondCommittedOffsetForStandby(final TestInfo testInfo) throws Exception {
        final String topic = "standby-limit-input-topic";
        final TopicPartition partition = new TopicPartition(topic, 0);
        CLUSTER.createTopic(topic, 1, 1);
        produceValueRange(topic, 1, 0, 1_000);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(topic, Consumed.with(Serdes.Integer(), Serdes.Integer()),
            Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as(TABLE_NAME).withCachingDisabled());

        final String safeTestName = safeUniqueTestName(testInfo);
        final AtomicLong standbyNextOffset = new AtomicLong();
        // which client gets the standby task is only known after the start, so both restore consumers share one gate.
        // The gate is harmless for the active task at first start: nothing is committed yet, so it has nothing to restore
        final AtomicLong gate = new AtomicLong(950L);
        final RestorePolls restorePolls1 = new RestorePolls(gate);
        final RestorePolls restorePolls2 = new RestorePolls(gate);
        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration(safeTestName), restorePolls1, standbyNextOffset);
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration(safeTestName), restorePolls2, standbyNextOffset);
        startApplicationAndWaitUntilRunning(Arrays.asList(kafkaStreams1, kafkaStreams2), Duration.ofSeconds(60));
        final String applicationId = "app-" + safeTestName;

        // the gate keeps the standby 50 records behind the committed offset of the active task (the limit of the standby)
        TestUtils.waitForCondition(
            () -> standbyNextOffset.get() >= 950L && committedOffset(applicationId, partition) == 1_000L,
            () -> "Standby did not reach the gate or the active task did not commit, next offset of the standby: " + standbyNextOffset.get());
        assertEquals(950L, standbyNextOffset.get());

        final boolean firstIsActive = kafkaStreams1.metadataForLocalThreads().stream().anyMatch(t -> !t.activeTasks().isEmpty());
        final KafkaStreams active = firstIsActive ? kafkaStreams1 : kafkaStreams2;
        final RestorePolls standbyPolls = firstIsActive ? restorePolls2 : restorePolls1;

        // the active task stops making progress, so its committed offset (the limit of the standby) stays at 1,000
        active.pause();
        produceValueRange(topic, 1, 1_000, 2_000);
        final long polledBefore = standbyPolls.records.get();
        // remove the gate
        gate.set(Long.MAX_VALUE);

        // the next poll returns records before and beyond the limit. The standby must pause the partition
        // instead of fetching all new records, and must still apply the 50 records before the limit
        TestUtils.waitForCondition(
            () -> standbyNextOffset.get() >= 1_000L && standbyPolls.paused.contains(partition),
            () -> "Standby did not apply the records before the limit or did not pause the partition, next offset: "
                + standbyNextOffset.get() + ", records polled: " + (standbyPolls.records.get() - polledBefore));
        assertEquals(1_000L, standbyNextOffset.get());
        // one poll of 100 records: the 50 records before the limit and 50 records beyond it
        final long polled = standbyPolls.records.get() - polledBefore;
        assertTrue(polled <= 100, "Standby polled " + polled + " records after the gate was removed");

        // the active task commits the new records, so the limit of the standby moves forward
        // and the standby must resume the partition
        active.resume();
        TestUtils.waitForCondition(
            () -> !standbyPolls.paused.contains(partition),
            () -> "Standby did not resume the partition after the committed offset of the active task moved forward");

        TestUtils.waitForCondition(
            () -> committedOffset(applicationId, partition) == 2_000L && standbyNextOffset.get() >= 2_000L,
            () -> "Active or standby task did not reach offset 2,000, next offset of the standby: " + standbyNextOffset.get());
    }

    // records returned by the restore consumer, the partitions it currently has paused,
    // and a gate shared by all restore consumers: they return only records before the gate offset
    private static final class RestorePolls {
        private final AtomicLong records = new AtomicLong();
        private final Set<TopicPartition> paused = ConcurrentHashMap.newKeySet();
        private final AtomicLong gate;

        private RestorePolls(final AtomicLong gate) {
            this.gate = gate;
        }

        // drops the records at or after the gate offset and seeks back to it, so they are fetched again after the gate is removed
        private ConsumerRecords<byte[], byte[]> applyGate(final Consumer<byte[], byte[]> consumer,
                                                          final ConsumerRecords<byte[], byte[]> records) {
            final long gateOffset = gate.get();
            if (gateOffset == Long.MAX_VALUE) {
                return records;
            }
            final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> kept = new HashMap<>();
            for (final TopicPartition partition : records.partitions()) {
                final List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(partition);
                final List<ConsumerRecord<byte[], byte[]>> beforeGate = partitionRecords.stream()
                    .filter(record -> record.offset() < gateOffset)
                    .collect(Collectors.toList());
                if (beforeGate.size() < partitionRecords.size()) {
                    consumer.seek(partition, gateOffset);
                }
                if (!beforeGate.isEmpty()) {
                    kept.put(partition, beforeGate);
                }
            }
            return new ConsumerRecords<>(kept, Map.of());
        }
    }

    // -1 if the group has not committed an offset for the partition yet
    private long committedOffset(final String groupId, final TopicPartition partition) throws Exception {
        try (final Admin admin = CLUSTER.createAdminClient()) {
            final OffsetAndMetadata committed = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS).get(partition);
            return committed == null ? -1L : committed.offset();
        }
    }

    private void produceValueRange(final String topic, final int key, final int start, final int endExclusive) {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(
            topic,
            IntStream.range(start, endExclusive)
                .mapToObj(i -> KeyValue.pair(key, i))
                .collect(Collectors.toList()),
            producerProps,
            mockTime);
    }

    private KafkaStreams createKafkaStreams(final StreamsBuilder builder,
                                            final Properties config,
                                            final RestorePolls restorePolls,
                                            final AtomicLong standbyNextOffset) {
        final KafkaStreams streams = new KafkaStreams(builder.build(config), config, new DefaultKafkaClientSupplier() {
            @Override
            public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> consumerConfig) {
                return new KafkaConsumer<>(consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer()) {
                    @Override
                    public ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
                        final ConsumerRecords<byte[], byte[]> records = restorePolls.applyGate(this, super.poll(timeout));
                        restorePolls.records.addAndGet(records.count());
                        return records;
                    }

                    @Override
                    public void pause(final Collection<TopicPartition> partitions) {
                        super.pause(partitions);
                        restorePolls.paused.addAll(partitions);
                    }

                    @Override
                    public void resume(final Collection<TopicPartition> partitions) {
                        super.resume(partitions);
                        restorePolls.paused.removeAll(partitions);
                    }
                };
            }
        });
        streams.setStandbyUpdateListener(new StandbyUpdateListener() {
            @Override
            public void onUpdateStart(final TopicPartition topicPartition, final String storeName, final long startingOffset) { }

            @Override
            public void onBatchLoaded(final TopicPartition topicPartition, final String storeName, final TaskId taskId,
                                      final long batchEndOffset, final long batchSize, final long currentEndOffset) {
                standbyNextOffset.accumulateAndGet(batchEndOffset + 1, Math::max);
            }

            @Override
            public void onUpdateSuspended(final TopicPartition topicPartition, final String storeName, final long storeOffset,
                                          final long currentEndOffset, final SuspendReason reason) { }
        });
        streamsToCleanup.add(streams);
        return streams;
    }

    private KafkaStreams createKafkaStreams(final StreamsBuilder builder, final Properties config) {
        final KafkaStreams streams = new KafkaStreams(builder.build(config), config);
        streamsToCleanup.add(streams);
        return streams;
    }

    private Properties streamsConfiguration(final String safeTestName) {
        final Properties config = new Properties();
        config.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + (++port));
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 200);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);
        return config;
    }
}
