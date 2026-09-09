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
package org.apache.kafka.clients.consumer.ng;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.clients.consumer.internals.FetchMetricsManager;
import org.apache.kafka.common.record.internal.MemoryRecords;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The records one fetch response carried for one partition, still inside the pooled receive buffer they arrived
 * in (no copy). Produced by the fetch engine's I/O thread, consumed by the application thread. Segments of the
 * same response share one {@link Owner}; the buffer goes back to the pool when the last segment is drained.
 */
public final class FetchSegment {

    /** Reference count over the segments of one receive buffer. */
    public static final class Owner {
        private final ByteBuffer buffer;
        private final MemoryPool pool;
        private final FetchMetricsManager metrics;
        private final AtomicInteger refs = new AtomicInteger();
        /** Application thread: bytes and records delivered from this response, per topic (metrics are per fetch). */
        private final Map<String, long[]> deliveredPerTopic = new HashMap<>();

        /**
         * The engine holds one reference while it is still creating segments from the response, so a segment
         * drained by the application thread in the meantime cannot free the buffer under the remaining ones.
         */
        public Owner(ByteBuffer buffer, MemoryPool pool) {
            this(buffer, pool, null);
        }

        public Owner(ByteBuffer buffer, MemoryPool pool, FetchMetricsManager metrics) {
            this.buffer = buffer;
            this.pool = pool;
            this.metrics = metrics;
            refs.set(1);
        }

        void retain() {
            refs.incrementAndGet();
        }

        void release() {
            if (refs.decrementAndGet() == 0) {
                if (buffer != null)
                    pool.release(buffer);
                recordFetchMetrics();
            }
        }

        /** Application thread: a segment of this response is done; its bytes and delivered records count for the fetch. */
        void delivered(String topic, int bytes, int records) {
            long[] totals = deliveredPerTopic.computeIfAbsent(topic, t -> new long[2]);
            totals[0] += bytes;
            totals[1] += records;
        }

        /** Same shape as the previous implementation's per-fetch aggregation: recorded once the whole response is drained. */
        private void recordFetchMetrics() {
            if (metrics == null || deliveredPerTopic.isEmpty())
                return;
            long bytes = 0;
            long records = 0;
            for (Map.Entry<String, long[]> e : deliveredPerTopic.entrySet()) {
                bytes += e.getValue()[0];
                records += e.getValue()[1];
                metrics.recordBytesFetched(e.getKey(), (int) e.getValue()[0]);
                metrics.recordRecordsFetched(e.getKey(), (int) e.getValue()[1]);
            }
            metrics.recordBytesFetched((int) bytes);
            metrics.recordRecordsFetched((int) records);
        }

        /** Engine: all segments of the response have been created; drop the creator's reference. */
        public void creationDone() {
            release();
        }
    }

    public final TopicPartition partition;
    /** Offset the fetch asked for: the first record delivered is at or after it. */
    public final long fetchOffset;
    public final MemoryRecords records;
    public final long highWatermark;
    public final long lastStableOffset;
    public final int sizeInBytes;
    private final Owner owner;
    private boolean released;

    public FetchSegment(TopicPartition partition, long fetchOffset, MemoryRecords records,
                        long highWatermark, long lastStableOffset, Owner owner) {
        this.partition = partition;
        this.fetchOffset = fetchOffset;
        this.records = records;
        this.highWatermark = highWatermark;
        this.lastStableOffset = lastStableOffset;
        this.sizeInBytes = records.sizeInBytes();
        this.owner = owner;
        owner.retain();
    }

    /** Application thread: every record of this segment has been delivered (or skipped); free its share of the buffer. */
    public void release() {
        release(0);
    }

    /** As {@link #release()}, counting the records the application received from this segment. */
    public void release(int deliveredRecords) {
        if (!released) {
            released = true;
            owner.delivered(partition.topic(), sizeInBytes, deliveredRecords);
            owner.release();
        }
    }
}
