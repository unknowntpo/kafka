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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests of decoding a fetch on a decoder thread. A test that needs a decode to be in flight passes a
 * {@link BlockingBufferSupplier} to {@link CompletedFetch#predecode}: the decoder stops when it starts to decompress
 * the second batch, after it has decoded the first one.
 */
public class FetchPredecoderTest {
    private static final TopicPartition TP = new TopicPartition("topic", 0);
    private static final int SIZE = 512 * 1024;
    private static final int RECORDS_PER_BATCH = 5;
    private static final long WAIT_MS = 30_000;
    private static final FetchConfig CONFIG = new FetchConfig(
            ConsumerConfig.DEFAULT_FETCH_MIN_BYTES,
            ConsumerConfig.DEFAULT_FETCH_MAX_BYTES,
            ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS,
            ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES,
            ConsumerConfig.DEFAULT_MAX_POLL_RECORDS,
            true,
            ConsumerConfig.DEFAULT_CLIENT_RACK,
            IsolationLevel.READ_UNCOMMITTED);

    private final ReceiveBufferPool pool = new ReceiveBufferPool(64 * 1024 * 1024);
    private final Deserializers<byte[], byte[]> bytes =
            new Deserializers<>(new ByteArrayDeserializer(), new ByteArrayDeserializer(), null);

    @AfterEach
    public void tearDown() {
        pool.close();
        bytes.close();
    }

    /**
     * drain() waits for a running decode, and only then gives its part of the response back. Otherwise the array
     * could hold the next response while the decoder still reads it.
     */
    @Test
    public void testDrainWaitsForRunningDecodeBeforeReleasingTheResponse() throws Exception {
        ByteBuffer response = pool.tryAllocate(SIZE);
        writeBatches(response, Compression.lz4().build(), 2, i -> ("value-" + i).getBytes());
        ReceiveBufferPool.Lease lease = spy(new ReceiveBufferPool.Lease(pool));
        CompletedFetch fetch = newFetch(response, mock(FetchMetricsAggregator.class), lease);
        BlockingBufferSupplier supplier = new BlockingBufferSupplier();

        Thread decoder = start(() -> fetch.predecode(CONFIG, bytes, supplier));
        assertTrue(supplier.entered.await(WAIT_MS, TimeUnit.MILLISECONDS));
        Thread drainer = start(fetch::drain);
        TestUtils.waitForCondition(() -> drainer.getState() == Thread.State.TIMED_WAITING, WAIT_MS,
                "drain() should wait for the running decode");

        // only the creator of the lease has released it
        verify(lease, times(1)).release();
        assertTrue(pool.handedOut(response.array()));
        assertTrue(drainer.isAlive());

        supplier.proceed.countDown();
        join(decoder);
        join(drainer);
        verify(lease, times(2)).release();
        // the decode was stopped before the end, so the array is not used again
        verify(lease).taint();
        assertTrue(fetch.isConsumed());
        assertTrue(fetch.fetchRecords(CONFIG, bytes, 10).isEmpty());
    }

    /**
     * A fetch that a decoder decoded and the application thread took to the end gives the array back, as it does
     * without a decoder.
     */
    @Test
    public void testFullyTakenPredecodedFetchGivesArrayBack() throws Exception {
        ByteBuffer response = pool.tryAllocate(SIZE);
        writeBatches(response, Compression.lz4().build(), 2, i -> ("value-" + i).getBytes());
        CompletedFetch fetch = newFetch(response, mock(FetchMetricsAggregator.class), new ReceiveBufferPool.Lease(pool));
        predecodeOnOtherThread(fetch, bytes);

        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        while (!fetch.isExhausted())
            records.addAll(fetch.fetchRecords(CONFIG, bytes, 3));
        fetch.drain();

        ByteBuffer next = pool.tryAllocate(SIZE);
        assertSame(response.array(), next.array());
        Arrays.fill(next.array(), (byte) 0x7F);
        assertEquals(2 * RECORDS_PER_BATCH, records.size());
        for (int i = 0; i < records.size(); i++) {
            assertEquals(i, records.get(i).offset());
            assertArrayEquals(("value-" + i).getBytes(), records.get(i).value());
        }
    }

    /**
     * A deserialization error that the decoder found is thrown by fetchRecords() at the same record, and in the
     * same call, as without a decoder.
     */
    @Test
    public void testDeserializationErrorFoundByDecoderIsThrownAtTheSameRecord() throws Exception {
        // IntegerDeserializer fails on offset 3: its value is not 4 bytes long
        IntFunction<byte[]> values = i -> i == 3 ? "bad".getBytes() : new byte[] {0, 0, 0, (byte) i};
        try (Deserializers<byte[], Integer> deserializers =
                     new Deserializers<>(new ByteArrayDeserializer(), new IntegerDeserializer(), null)) {
            List<String> expected = outcomes(newFetch(values, 1, Compression.lz4().build()), deserializers);
            assertEquals(List.of("records [0, 1] next 2", "records [2] next 3", "deserialization error at 3",
                    "deserialization error at 3", "deserialization error at 3"), expected);

            CompletedFetch fetch = newFetch(values, 1, Compression.lz4().build());
            predecodeOnOtherThread(fetch, deserializers);
            assertEquals(expected, outcomes(fetch, deserializers));
        }
    }

    /**
     * A corrupt batch that the decoder found is reported by fetchRecords() at the same record, with the same
     * message and cause, as without a decoder.
     */
    @Test
    public void testCorruptBatchFoundByDecoderIsThrownAtTheSameRecord() throws Exception {
        List<String> expected = outcomes(newCorruptFetch(), bytes);
        assertEquals("records [0, 1] next 2", expected.get(0));
        assertEquals("records [2, 3] next 4", expected.get(1));
        assertEquals("records [4] next 5", expected.get(2));
        assertTrue(expected.get(3).startsWith("KafkaException: Received exception when fetching the next record"), expected.get(3));
        assertTrue(expected.get(3).contains("Record batch for partition topic-0 at offset 5 is invalid"), expected.get(3));
        assertEquals(expected.get(3), expected.get(4));

        CompletedFetch fetch = newCorruptFetch();
        predecodeOnOtherThread(fetch, bytes);
        assertEquals(expected, outcomes(fetch, bytes));
    }

    /**
     * After a seek, the fetch is dropped while a decode is in flight. The application thread reads its own position
     * without waiting, and the position of the decode, which is ahead, never reaches it.
     */
    @Test
    public void testSeekWhileDecodeIsInFlight() throws Exception {
        FetchMetricsAggregator aggregator = mock(FetchMetricsAggregator.class);
        ByteBuffer response = pool.tryAllocate(SIZE);
        writeBatches(response, Compression.lz4().build(), 2, i -> ("value-" + i).getBytes());
        CompletedFetch fetch = newFetch(response, aggregator, new ReceiveBufferPool.Lease(pool));
        BlockingBufferSupplier supplier = new BlockingBufferSupplier();

        Thread decoder = start(() -> fetch.predecode(CONFIG, bytes, supplier));
        // the decoder has decoded the first batch, offsets 0 to 4
        assertTrue(supplier.entered.await(WAIT_MS, TimeUnit.MILLISECONDS));

        // The application has sought to offset 5. As FetchCollector does, it compares the position with the
        // offset of the fetch, and it does not match.
        assertEquals(0, fetch.nextFetchOffset());
        assertFalse(fetch.isExhausted());
        Thread drainer = start(fetch::drain);
        TestUtils.waitForCondition(() -> drainer.getState() == Thread.State.TIMED_WAITING, WAIT_MS,
                "drain() should wait for the running decode");

        supplier.proceed.countDown();
        join(decoder);
        join(drainer);
        assertTrue(fetch.fetchRecords(CONFIG, bytes, 10).isEmpty());
        assertEquals(0, fetch.nextFetchOffset());
        // none of the decoded records was handed out
        verify(aggregator).record(TP, 0, 0);
    }

    /**
     * When the application took only part of a predecoded fetch, drain() records the same bytes and records as
     * without a decoder. The records have different sizes, so an estimate of the bytes would not match.
     */
    @Test
    public void testPartialConsumeThenDrainRecordsTheSameMetrics() throws Exception {
        IntFunction<byte[]> values = i -> new byte[10 + 37 * i];
        int[] expected = partialConsumeMetrics(values, false);
        assertEquals(3, expected[1]);
        assertArrayEquals(expected, partialConsumeMetrics(values, true));
    }

    @Test
    public void testSubmitAfterCloseIsDiscarded() throws Exception {
        FetchPredecoder<byte[], byte[]> predecoder = new FetchPredecoder<>(CONFIG, bytes, 1);
        predecoder.close();
        CompletedFetch fetch = newFetch(i -> ("value-" + i).getBytes(), 2, Compression.lz4().build());

        predecoder.submit(fetch);

        // the application thread decodes the fetch itself
        List<ConsumerRecord<byte[], byte[]>> records = fetch.fetchRecords(CONFIG, bytes, 100);
        assertEquals(2 * RECORDS_PER_BATCH, records.size());
        assertTrue(fetch.isExhausted());
    }

    @Test
    public void testDecoderThreadsDecodeBufferedFetches() throws Exception {
        FetchPredecoder<byte[], byte[]> predecoder = new FetchPredecoder<>(CONFIG, bytes, 2);
        try {
            FetchBuffer buffer = new FetchBuffer(new LogContext());
            List<CompletedFetch> submitted = new ArrayList<>();
            buffer.setAddListener(fetch -> {
                submitted.add(fetch);
                predecoder.submit(fetch);
            });
            CompletedFetch fetch = newFetch(i -> ("value-" + i).getBytes(), 3, Compression.lz4().build());
            buffer.add(fetch);
            // a fetch that is put back is not new, so the listener is not told
            buffer.addAll(List.of(newFetch(i -> new byte[0], 1, Compression.NONE)));
            assertEquals(List.of(fetch), submitted);

            List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
            while (!fetch.isExhausted())
                records.addAll(fetch.fetchRecords(CONFIG, bytes, 4));
            assertEquals(3 * RECORDS_PER_BATCH, records.size());
            for (int i = 0; i < records.size(); i++)
                assertArrayEquals(("value-" + i).getBytes(), records.get(i).value());
            buffer.close();
        } finally {
            predecoder.close();
        }
    }

    private int[] partialConsumeMetrics(IntFunction<byte[]> values, boolean predecode) throws Exception {
        FetchMetricsAggregator aggregator = mock(FetchMetricsAggregator.class);
        ByteBuffer buffer = ByteBuffer.allocate(SIZE);
        writeBatches(buffer, Compression.lz4().build(), 2, values);
        CompletedFetch fetch = newFetch(buffer, aggregator, null);
        if (predecode)
            predecodeOnOtherThread(fetch, bytes);
        assertEquals(3, fetch.fetchRecords(CONFIG, bytes, 3).size());
        fetch.drain();

        ArgumentCaptor<Integer> bytesRead = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> recordsRead = ArgumentCaptor.forClass(Integer.class);
        verify(aggregator).record(eq(TP), bytesRead.capture(), recordsRead.capture());
        return new int[] {bytesRead.getValue(), recordsRead.getValue()};
    }

    private static <K, V> List<String> outcomes(CompletedFetch fetch, Deserializers<K, V> deserializers) {
        List<String> outcomes = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            try {
                List<ConsumerRecord<K, V>> records = fetch.fetchRecords(CONFIG, deserializers, 2);
                if (records.isEmpty())
                    break;
                outcomes.add("records " + records.stream().map(ConsumerRecord::offset).collect(Collectors.toList())
                        + " next " + fetch.nextFetchOffset());
            } catch (RecordDeserializationException e) {
                outcomes.add("deserialization error at " + e.offset());
            } catch (KafkaException e) {
                outcomes.add(e.getClass().getSimpleName() + ": " + e.getMessage() + " / cause: "
                        + (e.getCause() == null ? null : e.getCause().getMessage()));
            }
        }
        return outcomes;
    }

    private static void predecodeOnOtherThread(CompletedFetch fetch, Deserializers<?, ?> deserializers) throws InterruptedException {
        join(start(() -> fetch.predecode(CONFIG, deserializers, BufferSupplier.create())));
    }

    private static Thread start(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void join(Thread thread) throws InterruptedException {
        thread.join(WAIT_MS);
        assertFalse(thread.isAlive());
    }

    /** Writes batches of {@link #RECORDS_PER_BATCH} records each, from offset 0, and flips the buffer. */
    private static void writeBatches(ByteBuffer buffer, Compression compression, int batches, IntFunction<byte[]> values) {
        ByteBuffer writable = buffer.duplicate();
        int offset = 0;
        for (int b = 0; b < batches; b++) {
            try (MemoryRecordsBuilder builder = MemoryRecords.builder(writable, compression, TimestampType.CREATE_TIME, offset)) {
                for (int i = 0; i < RECORDS_PER_BATCH; i++, offset++)
                    builder.append(offset, ("key-" + offset).getBytes(), values.apply(offset));
                builder.build();
            }
        }
        writable.flip();
        buffer.limit(writable.limit());
    }

    private CompletedFetch newFetch(IntFunction<byte[]> values, int batches, Compression compression) {
        ByteBuffer buffer = ByteBuffer.allocate(SIZE);
        writeBatches(buffer, compression, batches, values);
        return newFetch(buffer, mock(FetchMetricsAggregator.class), null);
    }

    /** Two batches without compression; the second one fails its CRC check. */
    private CompletedFetch newCorruptFetch() {
        ByteBuffer buffer = ByteBuffer.allocate(SIZE);
        writeBatches(buffer, Compression.NONE, 2, i -> ("value-" + i).getBytes());
        // the last byte belongs to the last record of the second batch
        buffer.put(buffer.limit() - 1, (byte) (buffer.get(buffer.limit() - 1) + 1));
        return newFetch(buffer, mock(FetchMetricsAggregator.class), null);
    }

    /**
     * A fetch of the records in the buffer. If a lease is given, the fetch is the only part of the response, and
     * the creator of the lease is already done.
     */
    private CompletedFetch newFetch(ByteBuffer buffer, FetchMetricsAggregator aggregator, ReceiveBufferPool.Lease lease) {
        MemoryRecords records = MemoryRecords.readableRecords(buffer.duplicate());
        LogContext logContext = new LogContext();
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.NONE);
        subscriptions.assignFromUser(Set.of(TP));
        CompletedFetch fetch = new CompletedFetch(
                logContext.logger(CompletedFetch.class),
                subscriptions,
                BufferSupplier.create(),
                TP,
                new FetchResponseData.PartitionData().setPartitionIndex(0).setRecords(records),
                aggregator,
                0L);
        if (lease != null) {
            fetch.receiveLease(lease, records.buffer());
            lease.release();
        }
        return fetch;
    }

    /**
     * Blocks the decoder thread when it asks for a buffer after it has given one back, that is when it starts to
     * decompress the second batch, until {@link #proceed} opens.
     */
    private static final class BlockingBufferSupplier extends BufferSupplier {
        private final BufferSupplier delegate = BufferSupplier.create();
        private final AtomicBoolean released = new AtomicBoolean();
        private final AtomicBoolean blocked = new AtomicBoolean();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch proceed = new CountDownLatch(1);

        @Override
        public ByteBuffer get(int capacity) {
            if (released.get() && blocked.compareAndSet(false, true)) {
                entered.countDown();
                try {
                    if (!proceed.await(WAIT_MS, TimeUnit.MILLISECONDS))
                        throw new IllegalStateException("the test did not let the decoder go on");
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            return delegate.get(capacity);
        }

        @Override
        public void release(ByteBuffer buffer) {
            released.set(true);
            delegate.release(buffer);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
