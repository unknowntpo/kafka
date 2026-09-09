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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.CloseableIterator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.LockSupport;

/**
 * Application-thread side of the first cut: turns queued {@link FetchSegment}s into {@link ConsumerRecord}s,
 * round-robin over partitions, releasing each segment's share of the receive buffer once its last record has
 * been handed out. Records are read in place from the receive buffer; whether the value is copied is the
 * deserializer's decision (a {@code ByteBuffer}-aware deserializer copies nothing).
 */
public final class RecordReader<K, V> {

    private static final class Cursor {
        final FetchEngine.PartitionQueue queue;
        FetchSegment segment;
        Iterator<MutableRecordBatch> batches;
        CloseableIterator<Record> records;
        RecordBatch batch;
        /** Per-batch values shared by every record of the batch (one Optional per batch, not per record). */
        Optional<Integer> batchLeaderEpoch = Optional.empty();
        TimestampType batchTimestampType = TimestampType.NO_TIMESTAMP_TYPE;
        long position;

        Cursor(FetchEngine.PartitionQueue queue, long position) {
            this.queue = queue;
            this.position = position;
        }
    }

    private final FetchEngine engine;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final boolean checkCrcs;
    private final BufferSupplier decompressionBuffers = BufferSupplier.create();
    private final List<Cursor> cursors = new ArrayList<>();
    private final Thread applicationThread;
    private volatile boolean dataSignal;
    private int nextCursor;

    public RecordReader(FetchEngine engine, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, boolean checkCrcs) {
        this.engine = engine;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.checkCrcs = checkCrcs;
        this.applicationThread = Thread.currentThread();
    }

    /** I/O thread: data was queued; wake the application thread if it is parked in {@link #poll}. */
    public void onData() {
        dataSignal = true;
        LockSupport.unpark(applicationThread);
    }

    public void track(TopicPartition partition, long position) {
        cursors.add(new Cursor(engine.queue(partition), position));
    }

    /** Application thread: drop everything buffered for every partition and restart at {@code offset}. */
    public void seekAll(long offset) {
        for (Cursor c : cursors) {
            if (c.records != null) {
                c.records.close();
                c.records = null;
            }
            if (c.segment != null) {
                FetchSegment done = c.segment;
                c.segment = null;
                c.batches = null;
                c.batch = null;
                c.queue.poll();
                engine.released(done);
            }
            FetchSegment s;
            while ((s = c.queue.poll()) != null)
                engine.released(s);
            c.position = offset;
            engine.seek(c.queue.partition(), offset);
        }
    }

    public long position(TopicPartition partition) {
        for (Cursor c : cursors)
            if (c.queue.partition().equals(partition))
                return c.position;
        return -1L;
    }

    /**
     * @return up to {@code maxRecords} records, waiting at most {@code timeoutMs} for the first one.
     */
    public List<ConsumerRecord<K, V>> poll(int maxRecords, long timeoutMs) {
        List<ConsumerRecord<K, V>> out = new ArrayList<>(Math.min(maxRecords, 1024));
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (true) {
            drain(out, maxRecords);
            if (!out.isEmpty())
                return out;
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0)
                return out;
            // Park until the engine queues a segment (permit-style: a signal that arrived before we park is kept).
            if (!dataSignal)
                LockSupport.parkNanos(this, remaining);
            dataSignal = false;
        }
    }

    private void drain(List<ConsumerRecord<K, V>> out, int maxRecords) {
        int n = cursors.size();
        if (n == 0)
            return;
        int idle = 0;
        while (out.size() < maxRecords && idle < n) {
            Cursor c = cursors.get(nextCursor);
            nextCursor = (nextCursor + 1) % n;
            if (fill(c, out, maxRecords))
                idle = 0;
            else
                idle++;
        }
    }

    /** @return true if at least one record was added from this cursor */
    private boolean fill(Cursor c, List<ConsumerRecord<K, V>> out, int maxRecords) {
        int before = out.size();
        while (out.size() < maxRecords) {
            if (c.records != null && c.records.hasNext()) {
                Record record = c.records.next();
                if (record.offset() < c.position)
                    continue; // records before the position inside the first batch of a segment
                out.add(toConsumerRecord(c, record));
                c.position = record.offset() + 1;
                continue;
            }
            if (c.records != null) {
                c.records.close();
                c.records = null;
            }
            if (c.batches != null && c.batches.hasNext()) {
                MutableRecordBatch batch = c.batches.next();
                if (checkCrcs && batch.compressionType() == CompressionType.NONE)
                    batch.ensureValid();
                c.batch = batch;
                int epoch = batch.partitionLeaderEpoch();
                c.batchLeaderEpoch = epoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(epoch);
                c.batchTimestampType = batch.timestampType();
                c.records = batch.streamingIterator(decompressionBuffers);
                continue;
            }
            if (c.segment != null) {
                // Skipping to the next segment; the position is at least the end of this one.
                c.position = Math.max(c.position, c.batch == null ? c.position : c.batch.lastOffset() + 1);
                FetchSegment done = c.segment;
                c.segment = null;
                c.batches = null;
                c.batch = null;
                c.queue.poll();
                engine.released(done);
            }
            FetchSegment next = c.queue.peek();
            if (next == null)
                break;
            c.segment = next;
            c.batches = next.records.batches().iterator();
        }
        return out.size() > before;
    }

    private ConsumerRecord<K, V> toConsumerRecord(Cursor c, Record record) {
        TopicPartition tp = c.queue.partition();
        ByteBuffer keyBytes = record.key();
        ByteBuffer valueBytes = record.value();
        Header[] rawHeaders = record.headers();
        Headers headers = rawHeaders.length == 0 ? new RecordHeaders() : new RecordHeaders(rawHeaders);
        K key = keyBytes == null ? null : keyDeserializer.deserialize(tp.topic(), headers, keyBytes);
        V value = valueBytes == null ? null : valueDeserializer.deserialize(tp.topic(), headers, valueBytes);
        return new ConsumerRecord<>(tp.topic(), tp.partition(), record.offset(), record.timestamp(), c.batchTimestampType,
                keyBytes == null ? ConsumerRecord.NULL_SIZE : keyBytes.remaining(),
                valueBytes == null ? ConsumerRecord.NULL_SIZE : valueBytes.remaining(),
                key, value, headers, c.batchLeaderEpoch);
    }
}
