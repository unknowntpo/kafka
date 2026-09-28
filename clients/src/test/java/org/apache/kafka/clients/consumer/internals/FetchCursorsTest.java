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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FetchCursorsTest {

    private static final TopicPartition TP = new TopicPartition("topic", 0);
    private static final boolean BUFFERED = true;
    private static final boolean NOT_BUFFERED = false;

    private final FetchCursors cursors = new FetchCursors(new LogContext());

    @Test
    public void testSeedsFromThePosition() {
        assertEquals(5L, cursors.nextFetchOffset(TP, 5L, NOT_BUFFERED));
    }

    @Test
    public void testAdvancesPastTheBatchesOfAResponse() {
        cursors.nextFetchOffset(TP, 0L, NOT_BUFFERED);
        cursors.advance(TP, 0L, records(0L, 3));

        // The cursor is the offset after the last record, so the next fetch continues where the response stopped,
        // even though the application has not consumed any of it yet.
        assertEquals(3L, cursors.nextFetchOffset(TP, 0L, BUFFERED));
    }

    @Test
    public void testIgnoresAResponseForAnOffsetTheCursorIsNotAt() {
        cursors.nextFetchOffset(TP, 10L, NOT_BUFFERED);
        cursors.advance(TP, 0L, records(0L, 3));

        assertEquals(10L, cursors.nextFetchOffset(TP, 10L, NOT_BUFFERED));
    }

    @Test
    public void testDoesNotReSeedWhileDataIsOutstanding() {
        cursors.nextFetchOffset(TP, 0L, NOT_BUFFERED);
        cursors.advance(TP, 0L, records(0L, 3));

        // The position lags the cursor by exactly the data the application has not been given yet. Re-seeding from it
        // would re-fetch that data.
        assertEquals(3L, cursors.nextFetchOffset(TP, 0L, BUFFERED));

        cursors.sent(List.of(TP));
        assertEquals(3L, cursors.nextFetchOffset(TP, 0L, NOT_BUFFERED));
    }

    @Test
    public void testReSeedsAfterAForwardSeekOnceNothingIsOutstanding() {
        cursors.nextFetchOffset(TP, 0L, NOT_BUFFERED);
        cursors.advance(TP, 0L, records(0L, 3));

        cursors.completed(List.of(TP));
        assertEquals(50L, cursors.nextFetchOffset(TP, 50L, NOT_BUFFERED));
    }

    @Test
    public void testReSeedsAfterABackwardSeekOnceNothingIsOutstanding() {
        cursors.nextFetchOffset(TP, 100L, NOT_BUFFERED);
        cursors.advance(TP, 100L, records(100L, 3));

        // A seek backwards leaves the cursor ahead of the position with nothing in between. Without re-seeding, every
        // fetch would return data the collector discards for not matching the position, forever.
        cursors.completed(List.of(TP));
        assertEquals(20L, cursors.nextFetchOffset(TP, 20L, NOT_BUFFERED));
    }

    @Test
    public void testCorruptBatchHeadersLeaveTheCursorWhereItIs() {
        cursors.nextFetchOffset(TP, 0L, NOT_BUFFERED);

        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1});
        assertDoesNotThrow(() -> cursors.advance(TP, 0L, MemoryRecords.readableRecords(buffer)));

        // The application's poll() is what has to report corrupt data, so the next fetch asks for the same offset
        // again, which is what happens with no cursor at all.
        assertEquals(0L, cursors.nextFetchOffset(TP, 0L, BUFFERED));
    }

    @Test
    public void testForgetsUnassignedPartitions() {
        cursors.nextFetchOffset(TP, 0L, NOT_BUFFERED);
        cursors.advance(TP, 0L, records(0L, 3));
        assertEquals(3L, cursors.cursorFor(TP));

        cursors.retainAll(Set.of());
        assertEquals(-1L, cursors.cursorFor(TP));
    }

    private static MemoryRecords records(long baseOffset, int count) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE, TimestampType.CREATE_TIME, baseOffset, System.currentTimeMillis(), 0);

        for (int i = 0; i < count; i++)
            builder.append(0L, "key".getBytes(), "value".getBytes());

        return builder.build();
    }
}
