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
 * Arrays for large responses (in practice: fetch responses) that are used again after the fetch code has proven
 * that nothing reads the response any more.
 *
 * <p>Without it every fetch response is read into a new array of the size of the response. Such arrays are
 * several megabytes, which the garbage collector handles as a special, expensive kind of object.
 *
 * <p>The rule that keeps this safe: an array comes back only through {@link Lease}, and a lease gives the array
 * back only if every part of the response was consumed to its end and no record that was handed out can still
 * point into the array. In every other case the array is simply dropped and collected as before. Not giving an
 * array back is always safe; giving it back too early would corrupt records.
 */
final class ReceiveBufferPool implements MemoryPool {
    /** Experiment switch (CONSUMER-3X D-36): -Dc3x.receivePool=false reads every response into a new array. */
    static final boolean ENABLED = !"false".equals(System.getProperty("c3x.receivePool"));

    /**
     * A pool for one consumer. Arrays are never shared between consumers, but the free arrays of all pools of
     * this JVM together stay within 1/16 of the heap, however many consumers there are.
     */
    static ReceiveBufferPool forConsumer() {
        return new ReceiveBufferPool(JVM_MAX_RETAINED_BYTES);
    }

    private static final long JVM_MAX_RETAINED_BYTES = Runtime.getRuntime().maxMemory() / 16;
    /** Bytes in the free lists of all pools of this JVM. Only a count: it holds no array. */
    private static final AtomicLong JVM_RETAINED_BYTES = new AtomicLong();

    private static final int MIN_POOLED_BYTES = 256 * 1024;
    private static final int MAX_SLACK_BYTES = 1024 * 1024;

    private final long maxRetainedBytes;
    private final ArrayList<byte[]> free = new ArrayList<>();
    /** Arrays handed out and not given back yet. Weak, so an array that never comes back can be collected. */
    private final Map<byte[], Boolean> handedOut = new WeakHashMap<>();
    private long retainedBytes;
    private boolean closed;
    private long reused;
    private long created;

    ReceiveBufferPool(long maxRetainedBytes) {
        this.maxRetainedBytes = maxRetainedBytes;
    }

    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        if (sizeBytes < MIN_POOLED_BYTES)
            return ByteBuffer.allocate(sizeBytes);
        byte[] array = null;
        synchronized (this) {
            // The smallest free array that fits. One that is much larger than needed stays for a larger response.
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
                reused++;
            } else {
                created++;
            }
        }
        // A new array has exactly the requested size, as without the pool: a response that is never given back
        // (headers, other deserializers, errors) then costs no more than before.
        if (array == null)
            array = new byte[sizeBytes];
        synchronized (this) {
            handedOut.put(array, Boolean.TRUE);
        }
        // a buffer of exactly the requested size, starting at the first byte of the array
        return ByteBuffer.wrap(array, 0, sizeBytes).slice();
    }

    /** Does nothing: the network layer does not know whether the records of the response are still in use. */
    @Override
    public void release(ByteBuffer previouslyAllocated) {
    }

    /** Takes back an array of this pool that nothing reads any more. Arrays of other origin are ignored. */
    synchronized void recycle(byte[] array) {
        if (handedOut.remove(array) == null)
            return;
        if (closed || array.length > maxRetainedBytes)
            return;
        // The pools of the other consumers already fill the share of the heap: do not push out own arrays for it.
        if (retainedBytes + array.length <= maxRetainedBytes
                && JVM_RETAINED_BYTES.get() + array.length > JVM_MAX_RETAINED_BYTES)
            return;
        // Over the bound: the arrays that have waited longest go first. They are the ones whose size no response
        // has asked for lately, so the pool follows a change of the response size.
        while (retainedBytes + array.length > maxRetainedBytes)
            forget(free.remove(0).length);
        // The pools of the other consumers already fill the share of the heap: drop the array.
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

    /** Drops the free arrays. Arrays given back later are dropped as well. */
    synchronized void close() {
        closed = true;
        free.clear();
        JVM_RETAINED_BYTES.addAndGet(-retainedBytes);
        retainedBytes = 0;
    }

    synchronized boolean handedOut(byte[] array) {
        return handedOut.containsKey(array);
    }

    synchronized long reusedCount() {
        return reused;
    }

    synchronized long createdCount() {
        return created;
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
     * Tracks the parts of one response. The array goes back to the pool when the creator and every part have
     * called {@link #release()} and nobody called {@link #taint()}.
     */
    static final class Lease {
        private final ReceiveBufferPool pool;
        private final AtomicInteger holders = new AtomicInteger(1);   // the creator
        private volatile byte[] array;
        private volatile boolean tainted;

        Lease(ReceiveBufferPool pool) {
            this.pool = pool;
        }

        /** Adds one part of the response, which lives in the given buffer. */
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

        /** The array may still be read by somebody; it is never given back. */
        void taint() {
            tainted = true;
        }

        void release() {
            if (holders.decrementAndGet() == 0 && !tainted && array != null)
                pool.recycle(array);
        }
    }
}
