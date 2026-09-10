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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;

import java.util.Collections;
import java.util.List;

/** Single-partition app owner; uses the cached Kafka decoder unchanged. No group/commit integration. */
final class AppFetchDecoder implements AutoCloseable {
    private final Thread owner = Thread.currentThread();
    private final FetchBridge bridge;
    private final Metrics metrics = new Metrics();
    private final BufferSupplier buffers = BufferSupplier.create();
    private final LogContext log = new LogContext();
    private final SubscriptionState subscriptions = new SubscriptionState(log, AutoOffsetResetStrategy.NONE);
    private final FetchMetricsManager fetchMetrics = new FetchMetricsManager(metrics, new FetchMetricsRegistry());
    private final Deserializers<byte[], byte[]> deserializers = new Deserializers<>(
        new ByteArrayDeserializer(), new ByteArrayDeserializer(), null);
    private final FetchConfig config = new FetchConfig(1, 4096, 500, 4096, 500, true, "", IsolationLevel.READ_COMMITTED);
    private final TopicPartition partition;
    private PrefetchWindow.Lease lease;
    private CompletedFetch decoded;
    private long position;
    private long highWatermark = -1;
    private long lastStableOffset = -1;
    private long logStartOffset = -1;

    AppFetchDecoder(FetchBridge bridge) { this(bridge, "test"); }
    AppFetchDecoder(FetchBridge bridge, String topic) { this.bridge = bridge; partition = new TopicPartition(topic, 0); }
    private void checkOwner() {
        if (owner != Thread.currentThread()) throw new IllegalStateException("App owner only");
    }
    boolean trySeek(long offset) {
        checkOwner();
        if (!bridge.trySeek(offset)) return false;
        discard();
        position = offset;
        highWatermark = lastStableOffset = logStartOffset = -1;
        return true;
    }
    List<ConsumerRecord<byte[], byte[]>> poll(long timeoutNanos, int maxRecords) {
        checkOwner();
        if (maxRecords <= 0 || timeoutNanos < 0 || timeoutNanos > 60_000_000_000L) throw new IllegalArgumentException();
        if (lease != null && !bridge.valid(lease)) discard();
        if (lease == null) {
            lease = bridge.await(timeoutNanos);
            if (lease == null) return Collections.emptyList();
            if (lease.offset != position) throw new IllegalStateException("Noncontiguous response request cursor");
            FetchResponseData.PartitionData data = lease.partitionData;
            if (data.highWatermark() >= 0) highWatermark = data.highWatermark();
            if (data.lastStableOffset() >= 0) lastStableOffset = data.lastStableOffset();
            if (data.logStartOffset() >= 0) logStartOffset = data.logStartOffset();
            decoded = new CompletedFetch(log.logger(CompletedFetch.class), subscriptions, buffers, partition,
                data, new FetchMetricsAggregator(fetchMetrics, Collections.singleton(partition)), lease.offset);
        }
        List<ConsumerRecord<byte[], byte[]>> records = decoded.fetchRecords(config, deserializers, maxRecords);
        position = decoded.nextFetchOffset();
        // A short nonempty result may contain a deferred decoder error; don't release it early.
        // An empty successful read establishes exhaustion, including control/aborted-only responses.
        if (records.isEmpty()) discard();
        return records;
    }
    private void discard() {
        if (lease == null) return;
        if (decoded != null) decoded.drain();
        decoded = null;
        PrefetchWindow.Lease released = lease;
        lease = null;
        bridge.release(released);
    }
    long position() { checkOwner(); return position; }
    long highWatermark() { checkOwner(); return highWatermark; }
    long lastStableOffset() { checkOwner(); return lastStableOffset; }
    long logStartOffset() { checkOwner(); return logStartOffset; }
    public void close() {
        checkOwner();
        try { discard(); } finally {
            deserializers.close();
            buffers.close();
            metrics.close();
        }
    }
}
