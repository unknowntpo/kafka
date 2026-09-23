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
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The offset each partition's next fetch request will ask for.
 *
 * <p>This is deliberately not the {@link SubscriptionState} position. The position is where the application has
 * consumed to; the cursor is where the fetcher has requested to. They are equal only when nothing has been fetched
 * that the application has not yet been given. Keeping them apart is what allows a fetch request to go out for a
 * partition that still has data waiting in the {@link FetchBuffer}: the request asks for the offset after the
 * buffered data rather than for the offset the application is at, so the partition never has to be left out of the
 * request. That matters beyond prefetch depth, because leaving a partition out of an incremental fetch request
 * removes it from the broker's fetch session.
 *
 * <p>Owned by the background thread; every method must be called from it.
 */
class FetchCursors {

    private final Logger log;

    private static final class Cursor {
        /** The offset the next fetch asks for, or {@code -1} before the position has been read. */
        long nextFetchOffset = -1;
        /** A fetch request carrying this partition has been sent and has not completed. */
        boolean inFlight;
    }

    private final Map<TopicPartition, Cursor> cursors = new HashMap<>();

    FetchCursors(LogContext logContext) {
        this.log = logContext.logger(FetchCursors.class);
    }

    /**
     * The offset to request for {@code partition}, seeding or re-seeding the cursor from the position first if the
     * cursor cannot be trusted.
     *
     * <p>The cursor is a prediction: it is the offset after the data already requested, so it is only valid while
     * that data still exists somewhere between the fetcher and the application. A {@code seek} or an offset reset
     * moves the position to an unrelated offset and invalidates it. Rather than being told about those, the cursor
     * re-seeds whenever it can prove it has nothing outstanding to predict for: no request in flight and nothing
     * buffered means the cursor must equal the position, so any difference is a position change that did not come
     * from delivery. That covers seeks in both directions, and cannot re-seed while data is still in flight, whose
     * offsets the position has not reached yet.
     *
     * @param buffered whether the fetch buffer still holds data for this partition
     */
    long nextFetchOffset(TopicPartition partition, long positionOffset, boolean buffered) {
        Cursor cursor = cursors.computeIfAbsent(partition, tp -> new Cursor());

        if (cursor.nextFetchOffset < 0 || (!cursor.inFlight && !buffered && cursor.nextFetchOffset != positionOffset))
            cursor.nextFetchOffset = positionOffset;

        return cursor.nextFetchOffset;
    }

    /** A fetch request carrying these partitions is about to be sent. */
    void sent(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            Cursor cursor = cursors.get(partition);

            if (cursor != null)
                cursor.inFlight = true;
        }
    }

    /**
     * The request carrying these partitions completed, however it completed. Called for every outcome, including a
     * disconnection or a fetch session error, so that a cursor cannot be left in flight forever, which would stop it
     * from ever re-seeding.
     */
    void completed(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            Cursor cursor = cursors.get(partition);

            if (cursor != null)
                cursor.inFlight = false;
        }
    }

    /**
     * Moves the cursor past {@code records}, which were returned for a fetch that asked for {@code fetchOffset}.
     *
     * <p>Only batch headers are read, not records. A response that returned nothing usable, or that is for an offset
     * the cursor has already moved past, leaves the cursor alone.
     *
     * <p>The cursor is only a prediction of where the next fetch should start, so a response whose headers cannot be
     * read does not advance it and does not fail here. Corrupt data has to be reported to the application from its
     * own {@code poll()}, which is where the records are parsed; raising it on this thread instead would turn a
     * per-partition error the application can act on into a fetch-wide failure. Not advancing leaves the next fetch
     * asking for the same offset, which is what happens without a cursor at all.
     */
    void advance(TopicPartition partition, long fetchOffset, Records records) {
        Cursor cursor = cursors.get(partition);

        if (cursor == null || cursor.nextFetchOffset != fetchOffset)
            return;

        long next = fetchOffset;

        try {
            for (RecordBatch batch : records.batches())
                next = Math.max(next, batch.lastOffset() + 1);
        } catch (RuntimeException e) {
            log.debug("Not advancing the fetch cursor for partition {} past offset {}, because the batch headers of " +
                    "the response could not be read; the application's poll() will report it", partition, fetchOffset, e);
            return;
        }

        if (next > fetchOffset)
            cursor.nextFetchOffset = next;
    }

    /** Drops the cursors of partitions that are no longer assigned. */
    void retainAll(Set<TopicPartition> assigned) {
        cursors.keySet().retainAll(assigned);
    }

    // Visible for testing
    long cursorFor(TopicPartition partition) {
        Cursor cursor = cursors.get(partition);
        return cursor == null ? -1 : cursor.nextFetchOffset;
    }
}
