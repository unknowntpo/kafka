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

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link MemoryPool} that keeps released large buffers for reuse instead of allocating a fresh one for every
 * receive. Reuse skips the zeroing of a new buffer, which is a large share of the network thread's CPU when
 * responses are big and frequent, as with fetch responses.
 * <p>
 * Small allocations (below {@link #LARGE_BUFFER_BYTES}) are plain heap buffers that are never cached, matching
 * {@link MemoryPool#NONE}. Only large buffers go through the cache, which is bounded by {@code maxCachedBytes}.
 * Allocation is expected from a single thread (the network thread); release may come from any thread.
 * <p>
 * The pool never blocks or refuses an allocation: when the cache has no fitting buffer, a new one is allocated.
 * A buffer that is never released is simply garbage collected, so a missed release costs a reuse, not correctness.
 * The caller must not touch a buffer after releasing it.
 */
public class CachingMemoryPool implements MemoryPool {

    public static final int LARGE_BUFFER_BYTES = 1 << 20;

    private final long maxCachedBytes;
    private final ConcurrentLinkedDeque<ByteBuffer> cache = new ConcurrentLinkedDeque<>();
    private final AtomicLong cachedBytes = new AtomicLong();

    public CachingMemoryPool(long maxCachedBytes) {
        this.maxCachedBytes = maxCachedBytes;
    }

    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        if (sizeBytes < LARGE_BUFFER_BYTES)
            return ByteBuffer.allocate(sizeBytes);
        ByteBuffer cached;
        while ((cached = cache.pollFirst()) != null) {
            cachedBytes.addAndGet(-cached.capacity());
            if (cached.capacity() >= sizeBytes) {
                cached.clear();
                cached.limit(sizeBytes);
                return cached;
            }
            // too small for this receive: drop it rather than search further
        }
        return ByteBuffer.allocate(sizeBytes);
    }

    @Override
    public void release(ByteBuffer previouslyAllocated) {
        if (previouslyAllocated == null || previouslyAllocated.capacity() < LARGE_BUFFER_BYTES)
            return;
        if (cachedBytes.addAndGet(previouslyAllocated.capacity()) <= maxCachedBytes)
            cache.offerFirst(previouslyAllocated);
        else
            cachedBytes.addAndGet(-previouslyAllocated.capacity());
    }

    @Override
    public long size() {
        return maxCachedBytes;
    }

    @Override
    public long availableMemory() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean isOutOfMemory() {
        return false;
    }

    // visible for tests
    int cachedBuffers() {
        return cache.size();
    }

    @Override
    public String toString() {
        return "CachingMemoryPool(maxCachedBytes=" + maxCachedBytes + ")";
    }
}
