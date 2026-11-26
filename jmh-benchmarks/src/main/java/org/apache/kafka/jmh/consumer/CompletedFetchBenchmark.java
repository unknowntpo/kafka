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
package org.apache.kafka.jmh.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.CompletedFetch;
import org.apache.kafka.clients.consumer.internals.Deserializers;
import org.apache.kafka.clients.consumer.internals.FetchConfig;
import org.apache.kafka.clients.consumer.internals.FetchMetricsManager;
import org.apache.kafka.clients.consumer.internals.FetchMetricsRegistry;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.ByteUtils;
import org.apache.kafka.common.utils.internals.CloseableIterator;
import org.apache.kafka.common.utils.internals.LogContext;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Measures the application-thread cost of turning one fetched partition response (about 1 MiB of
 * magic v2 batches, each about 32 KiB before compression) into {@code ConsumerRecord<byte[], byte[]>}.
 *
 * <p>{@link CompletedFetch}'s constructor, {@code fetchRecords} and {@code drain} are package-private,
 * and the jmh-benchmarks import control only covers {@code org.apache.kafka.jmh}, so they are called
 * through method handles. The handles are static final, and each is called once per response or once
 * per 500 records, so the indirection does not show in the per-record score.
 *
 * <p>Every benchmark method processes the whole response. The record count depends on
 * {@code recordSize}, so the per-record score comes from JMH's {@code -opi} option, one run per
 * record size: {@code -p recordSize=100 -opi 10464} and {@code -p recordSize=1024 -opi 1024}.
 * Without {@code -opi} the score is per response. {@link #setup()} fails if the record count does
 * not match these values.
 *
 * <ul>
 *   <li>{@code fetchRecords}: the real consumer path, {@code CompletedFetch.fetchRecords} with
 *       {@code max.poll.records = 500} and {@code check.crcs = true} (the default).</li>
 *   <li>{@code fetchRecordsNoCrc}: the same path with {@code check.crcs = false}.</li>
 *   <li>{@code iterateOnly}: only {@link RecordBatch#streamingIterator(BufferSupplier)}, which creates one
 *       {@link Record} object for each record.</li>
 *   <li>{@code iterateReusing}: only {@link DefaultRecordBatch#reusingIterator(BufferSupplier)}, the iterator
 *       {@link CompletedFetch} uses, which reuses one {@link Record} object for all records of a batch.</li>
 *   <li>{@code floor}: a hand-written single pass that builds each {@link ConsumerRecord} with one
 *       value copy; a lower bound for the current public API. A compressed batch is first decompressed
 *       into one array that is reused for all batches.</li>
 * </ul>
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CompletedFetchBenchmark {

    static final int BATCH_BYTES = 32 * 1024;
    static final int BATCHES = 32;
    private static final int MAX_POLL_RECORDS = ConsumerConfig.DEFAULT_MAX_POLL_RECORDS;
    private static final int LEADER_EPOCH = 5;
    private static final String TOPIC = "bench";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    private static final MethodHandle NEW_AGGREGATOR;
    private static final MethodHandle NEW_COMPLETED_FETCH;
    private static final MethodHandle FETCH_RECORDS;
    private static final MethodHandle DRAIN;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(CompletedFetch.class, MethodHandles.lookup());
            Class<?> aggregatorClass = Class.forName("org.apache.kafka.clients.consumer.internals.FetchMetricsAggregator");
            NEW_AGGREGATOR = lookup.findConstructor(aggregatorClass,
                MethodType.methodType(void.class, FetchMetricsManager.class, Set.class));
            NEW_COMPLETED_FETCH = lookup.findConstructor(CompletedFetch.class,
                MethodType.methodType(void.class, Logger.class, SubscriptionState.class, BufferSupplier.class,
                    TopicPartition.class, FetchResponseData.PartitionData.class, aggregatorClass, Long.class));
            FETCH_RECORDS = lookup.findVirtual(CompletedFetch.class, "fetchRecords",
                MethodType.methodType(List.class, FetchConfig.class, Deserializers.class, int.class));
            DRAIN = lookup.findVirtual(CompletedFetch.class, "drain", MethodType.methodType(void.class));
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Param({"100", "1024"})
    private int recordSize;

    @Param({"NONE", "LZ4"})
    private String compression;

    private ByteBuffer responseBuffer;
    private int recordCount;
    private Logger log;
    private SubscriptionState subscriptions;
    private Object metricAggregator;
    private BufferSupplier decompressionBufferSupplier;
    private FetchConfig fetchConfig;
    private FetchConfig fetchConfigNoCrc;
    private Deserializers<byte[], byte[]> deserializers;
    private Headers sharedEmptyHeaders;
    private byte[] floorScratch = new byte[4 * BATCH_BYTES];
    private byte[] reusingScratch;

    @Setup(Level.Trial)
    public void setup() throws Throwable {
        CompressionType type = CompressionType.valueOf(compression);
        Compression codec = Compression.of(type).build();
        int recordsPerBatch = BATCH_BYTES / recordSize;
        recordCount = recordsPerBatch * BATCHES;
        int expected = recordSize == 100 ? 10464 : recordSize == 1024 ? 1024 : -1;
        if (recordCount != expected)
            throw new IllegalStateException("record count " + recordCount + " does not match the documented -opi value");

        // Payload like kafka-producer-perf-test: random upper-case letters; key is null; no headers.
        Random random = new Random(42);
        ByteBuffer all = ByteBuffer.allocate(BATCHES * (2 * BATCH_BYTES + 128));
        long baseTimestamp = 1_700_000_000_000L;
        long offset = 0;
        for (int b = 0; b < BATCHES; b++) {
            MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(2 * BATCH_BYTES),
                RecordBatch.MAGIC_VALUE_V2, codec, TimestampType.CREATE_TIME, offset,
                RecordBatch.NO_TIMESTAMP, LEADER_EPOCH);
            for (int i = 0; i < recordsPerBatch; i++) {
                builder.append(baseTimestamp + offset, null, randomPayload(random, recordSize));
                offset++;
            }
            all.put(builder.build().buffer());
        }
        all.flip();
        responseBuffer = all.slice();

        LogContext logContext = new LogContext();
        log = logContext.logger(CompletedFetch.class);
        subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.NONE);
        subscriptions.assignFromUser(Set.of(TP));
        subscriptions.seek(TP, 0);
        FetchMetricsManager metrics = new FetchMetricsManager(new Metrics(), new FetchMetricsRegistry());
        metricAggregator = NEW_AGGREGATOR.invoke(metrics, Set.of(TP));
        decompressionBufferSupplier = BufferSupplier.create();
        fetchConfig = newFetchConfig(true);
        fetchConfigNoCrc = newFetchConfig(false);
        deserializers = new Deserializers<>(new ByteArrayDeserializer(), new ByteArrayDeserializer(), null);
        sharedEmptyHeaders = new RecordHeaders();

        verify();
    }

    private static byte[] randomPayload(Random random, int size) {
        byte[] payload = new byte[size];
        for (int j = 0; j < size; j++)
            payload[j] = (byte) (random.nextInt(26) + 65);
        return payload;
    }

    private void verify() {
        CompletedFetch completedFetch = newCompletedFetch();
        int n = 0;
        long expectedOffset = 0;
        while (true) {
            List<ConsumerRecord<byte[], byte[]>> records = fetchRecords(completedFetch, fetchConfig);
            if (records.isEmpty())
                break;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (record.offset() != expectedOffset++ || record.value().length != recordSize
                        || record.key() != null || record.leaderEpoch().orElse(-1) != LEADER_EPOCH)
                    throw new IllegalStateException("unexpected record " + record);
            }
            n += records.size();
        }
        drain(completedFetch);
        if (n != recordCount)
            throw new IllegalStateException("fetchRecords returned " + n + " records, expected " + recordCount);
        CountingConsumer counter = new CountingConsumer();
        int floorCount = floorPass(responseBuffer.duplicate(), counter);
        if (floorCount != recordCount || counter.matched != recordCount)
            throw new IllegalStateException("floor parsed " + floorCount + " records (" + counter.matched
                + " matched), expected " + recordCount);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        deserializers.close();
    }

    private static FetchConfig newFetchConfig(boolean checkCrcs) {
        return new FetchConfig(
            ConsumerConfig.DEFAULT_FETCH_MIN_BYTES,
            ConsumerConfig.DEFAULT_FETCH_MAX_BYTES,
            ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS,
            ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES,
            MAX_POLL_RECORDS,
            checkCrcs,
            ConsumerConfig.DEFAULT_CLIENT_RACK,
            IsolationLevel.READ_UNCOMMITTED);
    }

    private CompletedFetch newCompletedFetch() {
        FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
            .setPartitionIndex(TP.partition())
            .setHighWatermark(recordCount)
            .setRecords(MemoryRecords.readableRecords(responseBuffer.duplicate()));
        try {
            return (CompletedFetch) NEW_COMPLETED_FETCH.invoke(log, subscriptions, decompressionBufferSupplier,
                TP, partitionData, metricAggregator, (Long) 0L);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<ConsumerRecord<byte[], byte[]>> fetchRecords(CompletedFetch completedFetch, FetchConfig config) {
        try {
            return (List<ConsumerRecord<byte[], byte[]>>) FETCH_RECORDS.invoke(completedFetch, config, deserializers, MAX_POLL_RECORDS);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static void drain(CompletedFetch completedFetch) {
        try {
            DRAIN.invoke(completedFetch);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private void drainAll(FetchConfig config, Blackhole bh) {
        CompletedFetch completedFetch = newCompletedFetch();
        while (true) {
            List<ConsumerRecord<byte[], byte[]>> records = fetchRecords(completedFetch, config);
            if (records.isEmpty())
                break;
            for (int i = 0; i < records.size(); i++)
                bh.consume(records.get(i).value().length);
        }
        drain(completedFetch);
    }

    @Benchmark
    public void fetchRecords(Blackhole bh) {
        drainAll(fetchConfig, bh);
    }

    @Benchmark
    public void fetchRecordsNoCrc(Blackhole bh) {
        drainAll(fetchConfigNoCrc, bh);
    }

    @Benchmark
    public void iterateOnly(Blackhole bh) {
        MemoryRecords records = MemoryRecords.readableRecords(responseBuffer.duplicate());
        for (RecordBatch batch : records.batches()) {
            try (CloseableIterator<Record> it = batch.streamingIterator(decompressionBufferSupplier)) {
                while (it.hasNext())
                    bh.consume(it.next());
            }
        }
    }

    @Benchmark
    public void iterateReusing(Blackhole bh) {
        MemoryRecords records = MemoryRecords.readableRecords(responseBuffer.duplicate());
        for (RecordBatch batch : records.batches()) {
            try (DefaultRecordBatch.ReusingRecordIterator it =
                     ((DefaultRecordBatch) batch).reusingIterator(decompressionBufferSupplier, reusingScratch)) {
                while (it.hasNext())
                    bh.consume(it.next());
                reusingScratch = it.decompressionArray();
            }
        }
    }

    @Benchmark
    public int floor(Blackhole bh) {
        return floorPass(responseBuffer.duplicate(), bh::consume);
    }

    private interface RecordSink {
        void accept(ConsumerRecord<byte[], byte[]> record);
    }

    private final class CountingConsumer implements RecordSink {
        int matched;
        long nextOffset;

        @Override
        public void accept(ConsumerRecord<byte[], byte[]> record) {
            if (record.offset() == nextOffset++ && record.value().length == recordSize && record.key() == null
                    && record.leaderEpoch().orElse(-1) == LEADER_EPOCH && record.timestampType() == TimestampType.CREATE_TIME)
                matched++;
        }
    }

    // Batch header layout of magic v2, see DefaultRecordBatch.
    private static final int LOG_OVERHEAD = 12;
    private static final int LENGTH_OFFSET = 8;
    private static final int PARTITION_LEADER_EPOCH_OFFSET = 12;
    private static final int MAGIC_OFFSET = 16;
    private static final int ATTRIBUTES_OFFSET = 21;
    private static final int BASE_TIMESTAMP_OFFSET = 27;
    private static final int MAX_TIMESTAMP_OFFSET = 35;
    private static final int RECORDS_COUNT_OFFSET = 57;
    private static final int RECORDS_OFFSET = 61;
    private static final int COMPRESSION_MASK = 0x07;
    private static final int TIMESTAMP_TYPE_MASK = 0x08;
    private static final int CONTROL_MASK = 0x20;

    /**
     * One pass over magic v2 batches, without CRC check. Builds one ConsumerRecord per record with one
     * value copy, one shared empty Headers object and one leader-epoch Optional per batch. A compressed
     * batch is decompressed into {@link #floorScratch} first.
     */
    private int floorPass(ByteBuffer buf, RecordSink sink) {
        int n = 0;
        while (buf.remaining() >= LOG_OVERHEAD) {
            int batchStart = buf.position();
            int batchSize = LOG_OVERHEAD + buf.getInt(batchStart + LENGTH_OFFSET);
            if (buf.get(batchStart + MAGIC_OFFSET) != RecordBatch.MAGIC_VALUE_V2)
                throw new IllegalStateException("only magic v2 is supported");
            short attributes = buf.getShort(batchStart + ATTRIBUTES_OFFSET);
            if ((attributes & CONTROL_MASK) == 0) {
                ByteBuffer records;
                if ((attributes & COMPRESSION_MASK) == 0) {
                    records = buf.duplicate();
                    records.position(batchStart + RECORDS_OFFSET);
                } else {
                    ByteBuffer batchBuffer = buf.duplicate();
                    batchBuffer.position(batchStart).limit(batchStart + batchSize);
                    records = decompress(batchBuffer.slice());
                }
                n += floorBatch(buf, batchStart, attributes, records, sink);
            }
            buf.position(batchStart + batchSize);
        }
        return n;
    }

    private ByteBuffer decompress(ByteBuffer batchBuffer) {
        DefaultRecordBatch batch = (DefaultRecordBatch) MemoryRecords.readableRecords(batchBuffer).batches().iterator().next();
        int size = 0;
        try (InputStream in = batch.recordInputStream(decompressionBufferSupplier)) {
            while (true) {
                if (size == floorScratch.length)
                    floorScratch = Arrays.copyOf(floorScratch, 2 * floorScratch.length);
                int read = in.read(floorScratch, size, floorScratch.length - size);
                if (read < 0)
                    break;
                size += read;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return ByteBuffer.wrap(floorScratch, 0, size);
    }

    private int floorBatch(ByteBuffer buf, int batchStart, short attributes, ByteBuffer records, RecordSink sink) {
        long baseOffset = buf.getLong(batchStart);
        int epoch = buf.getInt(batchStart + PARTITION_LEADER_EPOCH_OFFSET);
        Optional<Integer> leaderEpoch = epoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(epoch);
        boolean logAppendTime = (attributes & TIMESTAMP_TYPE_MASK) != 0;
        TimestampType timestampType = logAppendTime ? TimestampType.LOG_APPEND_TIME : TimestampType.CREATE_TIME;
        long baseTimestamp = buf.getLong(batchStart + BASE_TIMESTAMP_OFFSET);
        long maxTimestamp = buf.getLong(batchStart + MAX_TIMESTAMP_OFFSET);
        int count = buf.getInt(batchStart + RECORDS_COUNT_OFFSET);
        for (int i = 0; i < count; i++) {
            // Record layout, see DefaultRecord.readFrom: length varint, attributes int8,
            // timestampDelta varlong, offsetDelta varint, keyLength varint, key,
            // valueLength varint, value, headerCount varint, headers.
            int bodySize = ByteUtils.readVarint(records);
            int recordEnd = records.position() + bodySize;
            records.get(); // record attributes, unused
            long timestampDelta = ByteUtils.readVarlong(records);
            int offsetDelta = ByteUtils.readVarint(records);
            int keySize = ByteUtils.readVarint(records);
            byte[] key = copyBytes(records, keySize);
            int valueSize = ByteUtils.readVarint(records);
            byte[] value = copyBytes(records, valueSize);
            if (ByteUtils.readVarint(records) != 0)
                throw new IllegalStateException("headers are not supported by the floor pass");
            if (records.position() != recordEnd)
                throw new IllegalStateException("record size mismatch");
            sink.accept(new ConsumerRecord<>(TOPIC, TP.partition(), baseOffset + offsetDelta,
                logAppendTime ? maxTimestamp : baseTimestamp + timestampDelta, timestampType,
                keySize < 0 ? ConsumerRecord.NULL_SIZE : keySize,
                valueSize < 0 ? ConsumerRecord.NULL_SIZE : valueSize,
                key, value, sharedEmptyHeaders, leaderEpoch));
        }
        return count;
    }

    private static byte[] copyBytes(ByteBuffer buf, int size) {
        if (size < 0)
            return null;
        byte[] bytes = new byte[size];
        System.arraycopy(buf.array(), buf.arrayOffset() + buf.position(), bytes, 0, size);
        buf.position(buf.position() + size);
        return bytes;
    }
}
