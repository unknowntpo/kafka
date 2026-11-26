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

import org.apache.kafka.common.memory.MemoryPool;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Arrays for large responses (in practice: fetch responses) that are used again once the fetch code has proven
 * that nothing reads the response any more.
 *
 * <p>Without this pool, every fetch response is read into a new array of the size of the response. Such arrays
 * are often several megabytes, and the garbage collector handles them as a special, expensive kind of object.
 *
 * <p>The rule that keeps this safe: an array comes back only through a {@link Lease}. A lease gives the array
 * back only if every part of the response was read to its end and no record that was handed out can still
 * point into the array. In every other case the array is dropped and collected as before. Not giving an array
 * back is always safe. Giving it back too early would corrupt records that the application holds.
 */
final class ReceiveBufferPool implements MemoryPool {

    /**
     * Free arrays of all pools of this JVM together stay within this many bytes, however many consumers there are.
     */
    private static final long JVM_MAX_RETAINED_BYTES = Runtime.getRuntime().maxMemory() / 16;
    /** Bytes in the free lists of all pools of this JVM. It is only a count and holds no array. */
    private static final AtomicLong JVM_RETAINED_BYTES = new AtomicLong();

    /** Smaller responses get a plain new buffer. They are cheap for the garbage collector. */
    static final int MIN_POOLED_BYTES = 256 * 1024;
    private static final int MAX_SLACK_BYTES = 1024 * 1024;

    private final long maxRetainedBytes;
    private final ArrayList<byte[]> free = new ArrayList<>();
    /** Arrays handed out and not given back yet. The map is weak, so an array that never comes back is collected. */
    private final Map<byte[], Boolean> handedOut = new WeakHashMap<>();
    private long retainedBytes;
    private boolean closed;

    /**
     * A pool for one consumer. Arrays are never shared between consumers.
     */
    static ReceiveBufferPool forConsumer() {
        return new ReceiveBufferPool(JVM_MAX_RETAINED_BYTES);
    }

    /**
     * @param maxRetainedBytes the most bytes this pool keeps in its free list
     */
    ReceiveBufferPool(long maxRetainedBytes) {
        this.maxRetainedBytes = maxRetainedBytes;
    }

    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        if (sizeBytes < MIN_POOLED_BYTES)
            return ByteBuffer.allocate(sizeBytes);
        byte[] array = null;
        synchronized (this) {
            // Take the smallest free array that fits. An array that is much larger than needed stays free for a
            // larger response.
            int best = -1;
            for (int i = 0; i < free.size(); i++) {
                int length = free.get(i).length;
                if (length >= sizeBytes && length - sizeBytes <= Math.max(sizeBytes, MAX_SLACK_BYTES)
                        && (best < 0 || length < free.get(best).length))
                    best = i;
            }
            if (best >= 0) {
                array = free.remove(best);
                forget(array.length);
            }
        }
        // A new array has exactly the requested size, as without the pool. A response whose array is never given
        // back then costs no more than before.
        if (array == null)
            array = new byte[sizeBytes];
        synchronized (this) {
            handedOut.put(array, Boolean.TRUE);
        }
        // A buffer of exactly the requested size, which starts at the first byte of the array.
        return ByteBuffer.wrap(array, 0, sizeBytes).slice();
    }

    /**
     * Does nothing. The network layer calls it when it is done with a response, but it cannot know whether the
     * records of the response are still in use. Arrays come back through {@link Lease} instead.
     */
    @Override
    public void release(ByteBuffer previouslyAllocated) {
    }

    /**
     * Takes back an array of this pool that nothing reads any more. Arrays from elsewhere are ignored, and so is
     * an array that is given back a second time.
     */
    synchronized void recycle(byte[] array) {
        if (handedOut.remove(array) == null)
            return;
        if (closed || array.length > maxRetainedBytes)
            return;
        // The pools of the other consumers already fill the share of the heap. Do not drop own arrays for it.
        if (retainedBytes + array.length <= maxRetainedBytes
                && JVM_RETAINED_BYTES.get() + array.length > JVM_MAX_RETAINED_BYTES)
            return;
        // Over the bound of this pool: the arrays that have waited longest go first. No response asked for their
        // size lately, so the pool follows a change of the response size.
        while (retainedBytes + array.length > maxRetainedBytes)
            forget(free.remove(0).length);
        if (JVM_RETAINED_BYTES.addAndGet(array.length) > JVM_MAX_RETAINED_BYTES) {
            JVM_RETAINED_BYTES.addAndGet(-array.length);
            return;
        }
        free.add(array);
        retainedBytes += array.length;
    }

    private void forget(int length) {
        retainedBytes -= length;
        JVM_RETAINED_BYTES.addAndGet(-length);
    }

    /** Drops the free arrays. Arrays that are given back later are dropped as well. */
    synchronized void close() {
        closed = true;
        free.clear();
        JVM_RETAINED_BYTES.addAndGet(-retainedBytes);
        retainedBytes = 0;
    }

    // Visible for testing
    synchronized boolean handedOut(byte[] array) {
        return handedOut.containsKey(array);
    }

    // Visible for testing
    synchronized long retainedBytes() {
        return retainedBytes;
    }

    @Override
    public long size() {
        return Long.MAX_VALUE;
    }

    @Override
    public long availableMemory() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean isOutOfMemory() {
        return false;
    }

    /**
     * Tracks the parts of one response. Each {@link CompletedFetch} of the response is one part. The array goes
     * back to the pool when the creator and every part have called {@link #release()}, and nobody called
     * {@link #taint()}.
     */
    static final class Lease {
        private final ReceiveBufferPool pool;
        // Starts at 1 for the creator, so that the array cannot go back while parts are still being added.
        private final AtomicInteger holders = new AtomicInteger(1);
        private volatile byte[] array;
        private volatile boolean tainted;

        Lease(ReceiveBufferPool pool) {
            this.pool = pool;
        }

        /**
         * Adds one part of the response, whose bytes are in the given buffer. Call it on the thread that creates
         * the lease, before the part is shared with other threads.
         */
        void attach(ByteBuffer buffer) {
            holders.incrementAndGet();
            if (buffer == null || !buffer.hasArray()) {
                tainted = true;
                return;
            }
            byte[] a = buffer.array();
            if (array == null)
                array = a;
            else if (array != a)
                tainted = true;
        }

        /** Somebody may still read the array, so it must never go back to the pool. */
        void taint() {
            tainted = true;
        }

        /** Called once by the creator and once by each part. */
        void release() {
            if (holders.decrementAndGet() == 0 && !tainted && array != null)
                pool.recycle(array);
        }
    }
}
