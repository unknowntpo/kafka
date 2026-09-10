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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.common.utils.BufferSupplier;

import java.nio.ByteBuffer;
import java.util.Collections;

public final class PrefetchWindowTest {
    private static void check(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }
    private static void rejects(Runnable r) {
        try { r.run(); } catch (IllegalStateException expected) { return; }
        throw new AssertionError("Expected rejection");
    }
    private static MemoryRecords records(long offset) {
        return MemoryRecords.withRecords(offset, Compression.NONE,
            new SimpleRecord(0, new byte[] {1}), new SimpleRecord(0, new byte[] {2}));
    }
    private static void transactionAndGap() {
        MemoryRecords aborted = MemoryRecords.withTransactionalRecords(0, Compression.NONE,
            7L, (short) 0, 0, 0, new SimpleRecord(0, new byte[] {9}));
        MemoryRecords marker = MemoryRecords.withEndTransactionMarker(1, 0, 0,
            7L, (short) 0, new EndTransactionMarker(ControlRecordType.ABORT, 0));
        MemoryRecords visible = records(10);
        ByteBuffer bytes = ByteBuffer.allocate(aborted.sizeInBytes() + marker.sizeInBytes() + visible.sizeInBytes());
        bytes.put(aborted.buffer().duplicate()).put(marker.buffer().duplicate()).put(visible.buffer().duplicate()).flip();
        MemoryRecords response = MemoryRecords.readableRecords(bytes);
        PrefetchWindow window = new PrefetchWindow(10000, 2, 0);
        PrefetchWindow.Lease lease = window.complete(window.begin(), response);
        check(window.cursor() == 12, "speculative cursor spans aborted, control, and gap");
        try (Metrics metrics = new Metrics(); BufferSupplier buffers = BufferSupplier.create()) {
            TopicPartition tp = new TopicPartition("test", 0);
            LogContext log = new LogContext();
            CompletedFetch decoded = new CompletedFetch(log.logger(CompletedFetch.class),
                new SubscriptionState(log, AutoOffsetResetStrategy.NONE), buffers, tp,
                new FetchResponseData.PartitionData().setRecords(response).setAbortedTransactions(
                    Collections.singletonList(new FetchResponseData.AbortedTransaction().setProducerId(7).setFirstOffset(0))),
                new FetchMetricsAggregator(new FetchMetricsManager(metrics, new FetchMetricsRegistry()), Collections.singleton(tp)), 0L);
            FetchConfig config = new FetchConfig(1, 10000, 500, 10000, 100, true, "", IsolationLevel.READ_COMMITTED);
            Deserializers<byte[], byte[]> deserializers = new Deserializers<>(new ByteArrayDeserializer(), new ByteArrayDeserializer(), null);
            java.util.List<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>> delivered = decoded.fetchRecords(config, deserializers, 100);
            check(delivered.size() == 2 && delivered.get(0).offset() == 10 && delivered.get(1).offset() == 11,
                "aborted/control filtered, legal offset gap retained");
            check(decoded.nextFetchOffset() == window.cursor(), "decoded and speculative endpoints agree after exhaustion");
            decoded.drain();
        }
        window.release(lease);
        PrefetchWindow slots = new PrefetchWindow(100000, 1, 0);
        PrefetchWindow.Lease held = slots.complete(slots.begin(), visible);
        check(!slots.ready(), "slot cap applies independently of bytes");
        check(slots.release(held), "slot release activates work");
        PrefetchWindow.Request late = slots.begin();
        slots.close();
        check(slots.complete(late, visible) == null && slots.retained() == 0, "close rejects late payload");
        rejects(() -> slots.complete(late, visible));
        System.out.println("PASS READ_COMMITTED aborted/control filtering, offset gap, slot cap, close late response");
    }
    public static void main(String[] args) {
        transactionAndGap();
        MemoryRecords data = records(10);
        PrefetchWindow w = new PrefetchWindow(data.sizeInBytes() * 2L, 2, 10);
        PrefetchWindow.Request first = w.begin();
        PrefetchWindow.Lease lease = w.complete(first, data);
        check(w.cursor() == 12 && w.begin().offset == 12,
            "Must prefetch next batch before application consumption");
        try (Metrics metrics = new Metrics(); BufferSupplier buffers = BufferSupplier.create()) {
            TopicPartition tp = new TopicPartition("test", 0);
            LogContext log = new LogContext();
            CompletedFetch decoded = new CompletedFetch(log.logger(CompletedFetch.class),
                new SubscriptionState(log, AutoOffsetResetStrategy.NONE), buffers, tp,
                new FetchResponseData.PartitionData().setRecords(data),
                new FetchMetricsAggregator(new FetchMetricsManager(metrics, new FetchMetricsRegistry()),
                    Collections.singleton(tp)), lease.offset);
            FetchConfig config = new FetchConfig(1, 1024, 500, 1024, 1, true, "", IsolationLevel.READ_COMMITTED);
            Deserializers<byte[], byte[]> deserializers = new Deserializers<>(
                new ByteArrayDeserializer(), new ByteArrayDeserializer(), null);
            check(decoded.fetchRecords(config, deserializers, 1).get(0).offset() == 10, "first offset");
            check(decoded.nextFetchOffset() == 11 && w.cursor() == 12, "separate consumed cursor");
            check(w.retained() == data.sizeInBytes(), "partial decode must retain full response");
            check(decoded.fetchRecords(config, deserializers, 1).get(0).offset() == 11, "second offset");
            check(decoded.fetchRecords(config, deserializers, 1).isEmpty(), "exhaustion");
            decoded.drain();
            w.release(lease);
            check(w.retained() == 0, "release after decode");
        }
        System.out.println("PASS prefetch before consume, pinned CompletedFetch partial decode");

        w = new PrefetchWindow(1, 1, 0);
        PrefetchWindow.Request request = w.begin();
        lease = w.complete(request, data);
        check(!w.ready() && w.retained() > 1, "oversized response charged, no further fetch");
        w.seek(100);
        check(!w.valid(lease) && !w.ready(), "seek fences but keeps bytes charged");
        check(w.release(lease) && w.begin().offset == 100, "credit restores progress at seek position");
        final PrefetchWindow duplicateWindow = w;
        final PrefetchWindow.Lease duplicateLease = lease;
        rejects(() -> duplicateWindow.release(duplicateLease));
        System.out.println("PASS oversize, retained stale lease, release wake transition, duplicate release");

        w = new PrefetchWindow(1024, 2, 0);
        request = w.begin();
        w.seek(50);
        check(w.begin() == null, "seek must preserve old in-flight request");
        check(w.complete(request, data) == null && w.cursor() == 50, "late response discarded");
        request = w.begin();
        w.fail(request);
        request = w.begin();
        PrefetchWindow.Lease empty = w.complete(request, MemoryRecords.EMPTY);
        check(empty != null && w.cursor() == 50 && w.outstandingLeases() == 1, "empty metadata lease");
        w.close();
        check(w.outstandingLeases() == 1 && w.retained() == 0, "close must distinguish empty lease from no lease");
        w.release(empty);
        check(w.outstandingLeases() == 0, "empty lease release");
        w.close();
        check(w.begin() == null, "close admission");
        System.out.println("PASS in-flight seek, late response, failure, empty response, close");

        for (int i = 0; i < 10000; i++) {
            PrefetchWindow loop = new PrefetchWindow(1, 1, 0);
            PrefetchWindow.Lease l = loop.complete(loop.begin(), data);
            loop.close();
            check(!loop.valid(l) && !loop.release(l) && loop.retained() == 0, "close releases without restart");
        }
        PrefetchWindow corrupt = new PrefetchWindow(1024, 2, 10);
        ByteBuffer copy = ByteBuffer.allocate(data.sizeInBytes());
        copy.put(data.buffer().duplicate()).flip();
        copy.put(copy.limit() - 1, (byte) (copy.get(copy.limit() - 1) ^ 1));
        boolean rejected = false;
        try { corrupt.complete(corrupt.begin(), MemoryRecords.readableRecords(copy)); }
        catch (RuntimeException expected) { rejected = true; }
        check(rejected && corrupt.cursor() == 10 && corrupt.retained() == 0, "CRC failure must not advance");
        System.out.println("PASS 10000 close/release cycles and corrupt batch no-progress");
    }
}
