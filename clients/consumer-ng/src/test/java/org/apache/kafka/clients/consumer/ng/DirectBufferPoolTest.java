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
import org.apache.kafka.common.record.internal.MemoryRecords;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DirectBufferPoolTest {

    @Test
    public void allocationIsBoundedByTheCapacityAndResumesAfterRelease() {
        AtomicInteger releases = new AtomicInteger();
        DirectBufferPool pool = new DirectBufferPool(3L << 16, releases::incrementAndGet); // room for three 64 KiB buffers
        ByteBuffer a = pool.tryAllocate(1000);
        ByteBuffer b = pool.tryAllocate(65536);
        ByteBuffer c = pool.tryAllocate(10);
        assertNotNull(a);
        assertNotNull(b);
        assertNotNull(c);
        assertTrue(a.isDirect());
        assertEquals(1000, a.remaining(), "the buffer is limited to the requested size");
        assertNull(pool.tryAllocate(1), "the fourth allocation exceeds the capacity");
        assertTrue(pool.isOutOfMemory());

        pool.release(a);
        assertEquals(1, releases.get(), "a release notifies the waiter");
        ByteBuffer d = pool.tryAllocate(64);
        assertSame(a, d, "a released buffer of the right class is reused, not reallocated");
        assertEquals(3L << 16, pool.allocatedBytes(), "no new direct memory was allocated for the reuse");
    }

    @Test
    public void largerRequestsGetLargerClasses() {
        DirectBufferPool pool = new DirectBufferPool(1L << 30, () -> { });
        ByteBuffer small = pool.tryAllocate(100);
        ByteBuffer big = pool.tryAllocate(1_000_000);
        assertEquals(1 << 16, small.capacity());
        assertEquals(1 << 20, big.capacity());
        assertEquals(1_000_000, big.remaining());
    }

    /**
     * The race that corrupted records on one core: a segment drained by the application thread must not free the
     * receive buffer while the engine is still creating the other segments of the same response.
     */
    @Test
    public void receiveBufferIsFreedOnlyAfterCreationIsDoneAndEverySegmentIsReleased() {
        DirectBufferPool pool = new DirectBufferPool(1L << 16, () -> { }); // room for exactly one 64 KiB buffer
        ByteBuffer receive = pool.tryAllocate(1000);
        FetchSegment.Owner owner = new FetchSegment.Owner(receive, pool);
        MemoryRecords empty = MemoryRecords.readableRecords(ByteBuffer.allocate(0));

        FetchSegment first = new FetchSegment(new TopicPartition("t", 0), 0L, empty, 0L, 0L, owner);
        first.release(); // the application thread got here before the engine created the second segment
        assertNull(pool.tryAllocate(1000), "the buffer is still owned: nothing was returned to the pool");

        FetchSegment second = new FetchSegment(new TopicPartition("t", 1), 0L, empty, 0L, 0L, owner);
        owner.creationDone();
        assertNull(pool.tryAllocate(1000), "the second segment still points into the buffer");

        second.release();
        second.release(); // idempotent
        assertSame(receive, pool.tryAllocate(1000), "now the buffer is back in the pool");
    }
}
