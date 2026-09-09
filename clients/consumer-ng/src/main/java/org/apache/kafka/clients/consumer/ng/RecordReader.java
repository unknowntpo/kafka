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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Application-thread side of the fetch path: turns queued {@link FetchSegment}s into {@link ConsumerRecord}s,
 * round-robin over the assigned partitions, following the {@link SubscriptionState}'s positions (a record is
 * delivered only if it is at or after the position, and the position advances as records are delivered, as in
 * the previous implementation). Records are read in place from the receive buffer; whether the value is copied
 * is the deserializer's decision.
 */
public final class RecordReader<K, V> {

    private static final class Cursor {
        final TopicPartition partition;
        FetchPipeline.PartitionQueue queue;
        FetchSegment segment;
        Iterator<MutableRecordBatch> batches;
        CloseableIterator<Record> records;
        Optional<Integer> batchLeaderEpoch = Optional.empty();
        TimestampType batchTimestampType = TimestampType.NO_TIMESTAMP_TYPE;

        Cursor(TopicPartition partition) {
            this.partition = partition;
        }
    }

    private final ConsumerEngine engine;
    private final SubscriptionState subscriptions;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final boolean checkCrcs;
    private final BufferSupplier decompressionBuffers = BufferSupplier.create();
    private final Map<TopicPartition, Cursor> cursors = new HashMap<>();
    private final List<Cursor> order = new ArrayList<>();
    private int nextCursor;
    private int assignmentId = -1;

    public RecordReader(ConsumerEngine engine, SubscriptionState subscriptions, Deserializer<K> keyDeserializer,
                        Deserializer<V> valueDeserializer, boolean checkCrcs) {
        this.engine = engine;
        this.subscriptions = subscriptions;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.checkCrcs = checkCrcs;
    }

    /**
     * @param nextOffsets filled with the position after the delivered records, per partition delivered from
     * @return up to {@code maxRecords} records grouped by partition, without waiting
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> drain(int maxRecords, Map<TopicPartition, OffsetAndMetadata> nextOffsets) {
        syncCursors();
        Map<TopicPartition, List<ConsumerRecord<K, V>>> out = new LinkedHashMap<>();
        int n = order.size();
        int total = 0;
        int idle = 0;
        while (n > 0 && total < maxRecords && idle < n) {
            Cursor c = order.get(nextCursor);
            nextCursor = (nextCursor + 1) % n;
            int before = total;
            total += fill(c, out, maxRecords - total, nextOffsets);
            idle = total > before ? 0 : idle + 1;
        }
        return out;
    }

    private void syncCursors() {
        int id = subscriptions.assignmentId();
        if (id == assignmentId)
            return;
        assignmentId = id;
        Set<TopicPartition> assigned = subscriptions.assignedPartitions();
        cursors.entrySet().removeIf(e -> {
            if (assigned.contains(e.getKey()))
                return false;
            dropSegment(e.getValue());
            return true;
        });
        for (TopicPartition tp : assigned)
            cursors.computeIfAbsent(tp, Cursor::new);
        order.clear();
        order.addAll(cursors.values());
        nextCursor = 0;
    }

    private void dropSegment(Cursor c) {
        if (c.records != null) {
            c.records.close();
            c.records = null;
        }
        if (c.segment != null) {
            FetchSegment done = c.segment;
            c.segment = null;
            c.batches = null;
            if (c.queue != null)
                c.queue.poll();
            engine.released(done);
        }
    }

    /** @return the number of records added from this cursor */
    private int fill(Cursor c, Map<TopicPartition, List<ConsumerRecord<K, V>>> out, int max, Map<TopicPartition, OffsetAndMetadata> nextOffsets) {
        TopicPartition tp = c.partition;
        if (c.queue == null) {
            c.queue = engine.fetch.queue(tp);
            if (c.queue == null)
                return 0;
        }
        if (!subscriptions.isFetchable(tp))
            return 0;
        SubscriptionState.FetchPosition position = subscriptions.position(tp);
        if (position == null)
            return 0;
        long next = position.offset;
        int added = 0;
        List<ConsumerRecord<K, V>> list = null;
        while (added < max) {
            Record record = nextRecord(c, next);
            if (record == null)
                break;
            if (list == null)
                list = out.computeIfAbsent(tp, k -> new ArrayList<>());
            list.add(toConsumerRecord(c, record));
            next = record.offset() + 1;
            added++;
        }
        if (added > 0) {
            subscriptions.position(tp, new SubscriptionState.FetchPosition(next, c.batchLeaderEpoch, position.currentLeader));
            nextOffsets.put(tp, new OffsetAndMetadata(next, c.batchLeaderEpoch, ""));
        }
        return added;
    }

    /** @return the next record at or after {@code position} for this cursor, or null if nothing is buffered */
    private Record nextRecord(Cursor c, long position) {
        while (true) {
            if (c.records != null) {
                while (c.records.hasNext()) {
                    Record record = c.records.next();
                    if (record.offset() >= position)
                        return record;
                }
                c.records.close();
                c.records = null;
            }
            if (!advanceBatch(c, position) && !advanceSegment(c, position))
                return null;
        }
    }

    /** @return true if the cursor moved to the next batch of the current segment that may contain the position */
    private boolean advanceBatch(Cursor c, long position) {
        while (c.batches != null && c.batches.hasNext()) {
            MutableRecordBatch batch = c.batches.next();
            if (batch.lastOffset() < position)
                continue; // whole batch before the position
            if (checkCrcs && batch.compressionType() == CompressionType.NONE)
                batch.ensureValid();
            int epoch = batch.partitionLeaderEpoch();
            c.batchLeaderEpoch = epoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(epoch);
            c.batchTimestampType = batch.timestampType();
            c.records = batch.streamingIterator(decompressionBuffers);
            return true;
        }
        return false;
    }

    /** Releases the drained segment and takes the next queued one; false if the queue is empty or ran ahead of the position. */
    private boolean advanceSegment(Cursor c, long position) {
        if (c.segment != null) {
            FetchSegment done = c.segment;
            c.segment = null;
            c.batches = null;
            c.queue.poll();
            engine.released(done);
        }
        FetchSegment head = c.queue.peek();
        if (head == null || head.fetchOffset > position)
            return false; // nothing buffered, or the fetch ran ahead of a position that moved backwards (the seek resets the pipeline)
        c.segment = head;
        c.batches = head.records.batches().iterator();
        return true;
    }

    private ConsumerRecord<K, V> toConsumerRecord(Cursor c, Record record) {
        TopicPartition tp = c.partition;
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

    /** Application thread: forget any buffered state for {@code partition} (its position was moved by a seek). */
    public void reset(TopicPartition partition) {
        Cursor c = cursors.get(partition);
        if (c != null)
            dropSegment(c);
    }

    public void close() {
        for (Cursor c : cursors.values())
            dropSegment(c);
        cursors.clear();
        order.clear();
    }
}
