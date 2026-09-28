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
package org.apache.kafka.common.memory;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.memory.CachingMemoryPool.LARGE_BUFFER_BYTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

public class CachingMemoryPoolTest {

    @Test
    public void testSmallBuffersAreNeverCached() {
        CachingMemoryPool pool = new CachingMemoryPool(10L * LARGE_BUFFER_BYTES);
        ByteBuffer small = pool.tryAllocate(LARGE_BUFFER_BYTES - 1);
        pool.release(small);
        assertEquals(0, pool.cachedBuffers());
        assertNotSame(small, pool.tryAllocate(LARGE_BUFFER_BYTES - 1));
    }

    @Test
    public void testLargeBufferIsReusedWithTheRequestedLimit() {
        CachingMemoryPool pool = new CachingMemoryPool(10L * LARGE_BUFFER_BYTES);
        ByteBuffer first = pool.tryAllocate(2 * LARGE_BUFFER_BYTES);
        first.position(first.limit()); // simulate a fully read receive
        pool.release(first);
        assertEquals(1, pool.cachedBuffers());

        ByteBuffer reused = pool.tryAllocate(LARGE_BUFFER_BYTES + 5);
        assertSame(first, reused);
        assertEquals(0, reused.position());
        assertEquals(LARGE_BUFFER_BYTES + 5, reused.limit());
        assertEquals(0, pool.cachedBuffers());
    }

    @Test
    public void testCachedBufferTooSmallForRequestIsDropped() {
        CachingMemoryPool pool = new CachingMemoryPool(10L * LARGE_BUFFER_BYTES);
        ByteBuffer small = pool.tryAllocate(LARGE_BUFFER_BYTES);
        pool.release(small);

        ByteBuffer bigger = pool.tryAllocate(3 * LARGE_BUFFER_BYTES);
        assertNotSame(small, bigger);
        assertEquals(3 * LARGE_BUFFER_BYTES, bigger.capacity());
        assertEquals(0, pool.cachedBuffers());
    }

    @Test
    public void testCacheIsBounded() {
        CachingMemoryPool pool = new CachingMemoryPool(3L * LARGE_BUFFER_BYTES);
        ByteBuffer a = pool.tryAllocate(2 * LARGE_BUFFER_BYTES);
        ByteBuffer b = pool.tryAllocate(2 * LARGE_BUFFER_BYTES);
        pool.release(a);
        pool.release(b); // would exceed the bound, so it is dropped
        assertEquals(1, pool.cachedBuffers());
        assertSame(a, pool.tryAllocate(2 * LARGE_BUFFER_BYTES));
        assertEquals(0, pool.cachedBuffers());

        // once the cache drains, the bound frees up again
        pool.release(b);
        assertEquals(1, pool.cachedBuffers());
    }

    @Test
    public void testReleasingNullIsIgnored() {
        CachingMemoryPool pool = new CachingMemoryPool(LARGE_BUFFER_BYTES);
        pool.release(null);
        assertEquals(0, pool.cachedBuffers());
    }
}
