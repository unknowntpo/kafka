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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The array of a fetch response may be used for the next response only when nothing can read the old response
 * any more. Each test of a fetch ends the same way: it asks the pool for an array of the same size, overwrites
 * all of it, and checks the records the application already holds.
 */
public class ReceiveBufferPoolTest {
    private static final TopicPartition TP = new TopicPartition("topic", 0);
    private static final int SIZE = 512 * 1024;
    private static final int RECORDS = 5;

    private final ReceiveBufferPool pool = new ReceiveBufferPool(64 * 1024 * 1024);

    @Test
    public void testArrayIsUsedAgainOnlyAfterItIsGivenBack() {
        ByteBuffer first = pool.tryAllocate(SIZE);
        assertEquals(SIZE, first.remaining());
        assertEquals(0, first.arrayOffset());

        // not given back: the next request gets another array, also after NetworkReceive.close() calls release()
        pool.release(first);
        ByteBuffer second = pool.tryAllocate(SIZE);
        assertNotSame(first.array(), second.array());

        pool.recycle(first.array());
        assertSame(first.array(), pool.tryAllocate(SIZE).array());
    }

    @Test
    public void testArrayFromElsewhereIsNotTaken() {
        byte[] foreign = new byte[1024 * 1024];
        pool.recycle(foreign);
        assertNotSame(foreign, pool.tryAllocate(SIZE).array());
    }

    @Test
    public void testArrayGivenBackTwiceIsHandedOutOnce() {
        ByteBuffer first = pool.tryAllocate(SIZE);
        pool.recycle(first.array());
        pool.recycle(first.array());
        assertSame(first.array(), pool.tryAllocate(SIZE).array());
        assertNotSame(first.array(), pool.tryAllocate(SIZE).array());
    }

    @Test
    public void testSmallResponsesAreNotPooled() {
        ByteBuffer small = pool.tryAllocate(1024);
        assertEquals(1024, small.capacity());
        assertFalse(pool.handedOut(small.array()));
    }

    @Test
    public void testOldestFreeArrayGoesWhenThePoolIsFull() {
        ReceiveBufferPool tiny = new ReceiveBufferPool(2 * SIZE);
        ByteBuffer a = tiny.tryAllocate(SIZE);
        ByteBuffer b = tiny.tryAllocate(SIZE);
        ByteBuffer c = tiny.tryAllocate(SIZE);
        tiny.recycle(a.array());
        tiny.recycle(b.array());
        tiny.recycle(c.array());   // over the bound: a, which waited longest, is dropped
        List<byte[]> next = List.of(tiny.tryAllocate(SIZE).array(), tiny.tryAllocate(SIZE).array(), tiny.tryAllocate(SIZE).array());
        assertTrue(next.contains(b.array()));
        assertTrue(next.contains(c.array()));
        assertFalse(next.contains(a.array()));
    }

    @Test
    public void testNewArrayHasExactlyTheRequestedSize() {
        assertEquals(300 * 1024, pool.tryAllocate(300 * 1024).array().length);
    }

    @Test
    public void testFreeArrayIsUsedForASomewhatSmallerResponse() {
        ByteBuffer first = pool.tryAllocate(SIZE);
        pool.recycle(first.array());
        ByteBuffer smaller = pool.tryAllocate(300 * 1024);
        assertSame(first.array(), smaller.array());
        assertEquals(300 * 1024, smaller.remaining());
    }

    @Test
    public void testFreeArrayIsNotUsedForAMuchSmallerResponse() {
        ByteBuffer large = pool.tryAllocate(8 * 1024 * 1024);
        pool.recycle(large.array());
        assertNotSame(large.array(), pool.tryAllocate(SIZE).array());
        assertSame(large.array(), pool.tryAllocate(6 * 1024 * 1024).array());
    }

    @Test
    public void testLeaseGivesArrayBackAfterTheLastPart() {
        ByteBuffer response = pool.tryAllocate(SIZE);
        ReceiveBufferPool.Lease lease = new ReceiveBufferPool.Lease(pool);
        lease.attach(response.duplicate());
        lease.attach(response.duplicate());
        lease.release();   // the creator
        lease.release();   // first part
        assertTrue(pool.handedOut(response.array()));
        lease.release();   // second part
        assertFalse(pool.handedOut(response.array()));
        assertSame(response.array(), pool.tryAllocate(SIZE).array());
    }

    @Test
    public void testTaintedLeaseKeepsArray() {
        ByteBuffer response = pool.tryAllocate(SIZE);
        ReceiveBufferPool.Lease lease = new ReceiveBufferPool.Lease(pool);
        lease.attach(response.duplicate());
        lease.taint();
        lease.release();
        lease.release();
        assertNotSame(response.array(), pool.tryAllocate(SIZE).array());
    }

    @Test
    public void testPartsInDifferentArraysKeepArray() {
        ByteBuffer response = pool.tryAllocate(SIZE);
        ByteBuffer other = pool.tryAllocate(SIZE);
        ReceiveBufferPool.Lease lease = new ReceiveBufferPool.Lease(pool);
        lease.attach(response.duplicate());
        lease.attach(other.duplicate());
        lease.release();
        lease.release();
        lease.release();
        ByteBuffer next = pool.tryAllocate(SIZE);
        assertNotSame(response.array(), next.array());
        assertNotSame(other.array(), next.array());
    }

    @Test
    public void testFullyConsumedFetchGivesArrayBack() {
        checkFullyConsumed(Compression.NONE);
    }

    @Test
    public void testFullyConsumedCompressedFetchGivesArrayBack() {
        checkFullyConsumed(Compression.lz4().build());
    }

    private void checkFullyConsumed(Compression compression) {
        ByteBuffer response = pool.tryAllocate(SIZE);
        CompletedFetch fetch = newCompletedFetch(response, compression, false);
        List<ConsumerRecord<byte[], byte[]>> records = fetchAll(fetch, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        fetch.drain();

        assertSame(response.array(), overwrite(pool.tryAllocate(SIZE)).array());
        assertEquals(RECORDS, records.size());
        for (int i = 0; i < RECORDS; i++) {
            assertArrayEquals(("key-" + i).getBytes(), records.get(i).key());
            assertArrayEquals(("value-" + i).getBytes(), records.get(i).value());
        }
    }

    @Test
    public void testRecordsWithHeadersKeepArray() {
        ByteBuffer response = pool.tryAllocate(SIZE);
        CompletedFetch fetch = newCompletedFetch(response, Compression.NONE, true);
        List<ConsumerRecord<byte[], byte[]>> records = fetchAll(fetch, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        fetch.drain();

        // a header copies its value out of the response only when it is first read
        assertNotSame(response.array(), overwrite(pool.tryAllocate(SIZE)).array());
        for (int i = 0; i < RECORDS; i++)
            assertArrayEquals(("header-" + i).getBytes(), records.get(i).headers().lastHeader("h").value());
    }

    @Test
    public void testDeserializerThatKeepsItsInputKeepsArray() {
        ByteBuffer response = pool.tryAllocate(SIZE);
        CompletedFetch fetch = newCompletedFetch(response, Compression.NONE, false);
        List<ConsumerRecord<ByteBuffer, ByteBuffer>> records = fetchAll(fetch, new ByteBufferDeserializer(), new ByteBufferDeserializer());
        fetch.drain();

        assertNotSame(response.array(), overwrite(pool.tryAllocate(SIZE)).array());
        for (int i = 0; i < RECORDS; i++)
            assertArrayEquals(("value-" + i).getBytes(), Utils.toArray(records.get(i).value()));
    }

    @Test
    public void testFetchDroppedBeforeItsEndKeepsArray() {
        ByteBuffer response = pool.tryAllocate(SIZE);
        CompletedFetch fetch = newCompletedFetch(response, Compression.NONE, false);
        Deserializers<byte[], byte[]> deserializers = new Deserializers<>(new ByteArrayDeserializer(), new ByteArrayDeserializer(), null);
        List<ConsumerRecord<byte[], byte[]>> records = fetch.fetchRecords(fetchConfig(), deserializers, 2);
        assertEquals(2, records.size());
        fetch.drain();   // what seek, pause, a lost partition and close do

        assertNotSame(response.array(), overwrite(pool.tryAllocate(SIZE)).array());
    }

    private <K, V> List<ConsumerRecord<K, V>> fetchAll(CompletedFetch fetch, Deserializer<K> key, Deserializer<V> value) {
        Deserializers<K, V> deserializers = new Deserializers<>(key, value, null);
        List<ConsumerRecord<K, V>> records = new java.util.ArrayList<>();
        while (!fetch.isExhausted())
            records.addAll(fetch.fetchRecords(fetchConfig(), deserializers, 2));
        return records;
    }

    private static ByteBuffer overwrite(ByteBuffer buffer) {
        Arrays.fill(buffer.array(), (byte) 0x7F);
        return buffer;
    }

    /** A fetch whose records are written into the given buffer, as the network layer would have read them. */
    private CompletedFetch newCompletedFetch(ByteBuffer response, Compression compression, boolean withHeaders) {
        ByteBuffer writable = response.duplicate();
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(writable, compression, TimestampType.CREATE_TIME, 0L)) {
            for (int i = 0; i < RECORDS; i++) {
                Header[] headers = withHeaders ? new Header[] {new RecordHeader("h", ("header-" + i).getBytes())} : new Header[0];
                builder.append(0L, ("key-" + i).getBytes(), ("value-" + i).getBytes(), headers);
            }
            builder.build();
        }
        writable.flip();
        MemoryRecords records = MemoryRecords.readableRecords(writable);
        assertSame(response.array(), records.buffer().array());

        LogContext logContext = new LogContext();
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.NONE);
        subscriptions.assignFromUser(Set.of(TP));
        FetchMetricsManager metrics = new FetchMetricsManager(new Metrics(), new FetchMetricsRegistry());
        CompletedFetch fetch = new CompletedFetch(
                logContext.logger(CompletedFetch.class),
                subscriptions,
                BufferSupplier.create(),
                TP,
                new FetchResponseData.PartitionData().setPartitionIndex(0).setHighWatermark(RECORDS).setLogStartOffset(0).setRecords(records),
                new FetchMetricsAggregator(metrics, Collections.singleton(TP)),
                0L);
        ReceiveBufferPool.Lease lease = new ReceiveBufferPool.Lease(pool);
        fetch.receiveLease(lease, records.buffer());
        lease.release();
        return fetch;
    }

    private static FetchConfig fetchConfig() {
        return new FetchConfig(
                ConsumerConfig.DEFAULT_FETCH_MIN_BYTES,
                ConsumerConfig.DEFAULT_FETCH_MAX_BYTES,
                ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS,
                ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES,
                ConsumerConfig.DEFAULT_MAX_POLL_RECORDS,
                true,
                ConsumerConfig.DEFAULT_CLIENT_RACK,
                IsolationLevel.READ_UNCOMMITTED);
    }
}
