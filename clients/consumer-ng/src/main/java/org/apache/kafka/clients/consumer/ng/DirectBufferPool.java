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

import org.apache.kafka.common.memory.MemoryPool;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bounded pool of direct receive buffers for the network layer, plugged into {@code Selector} through
 * {@link MemoryPool}. Two things it buys over the default heap allocation per response (CONSUMER-NG-02 §3):
 * the JDK reads from a socket straight into a direct buffer (a heap target costs a copy through a temporary
 * direct buffer), and a reused buffer is not zeroed (a fresh {@code byte[]} is). The bound is the memory
 * guarantee: when the pool is exhausted {@code tryAllocate} returns {@code null}, the network layer stops
 * reading that connection, and the broker holds the data instead of the client (CONSUMER-NG-01 §1 goal 4).
 *
 * <p>Buffers come in power-of-two size classes so a released buffer serves the next response of similar size.
 * The network layer never releases a buffer on the client (a fetch response's records point into it); the
 * consumer releases it when every record it delivered from that buffer has been handed to the application.
 */
public final class DirectBufferPool implements MemoryPool {

    private static final int MIN_CLASS_SHIFT = 16; // 64 KiB
    private static final int MAX_CLASS_SHIFT = 30; // 1 GiB

    private final long capacityBytes;
    private final AtomicLong allocatedBytes = new AtomicLong();
    private final AtomicLong outstandingBytes = new AtomicLong();
    private final List<ConcurrentLinkedDeque<ByteBuffer>> free = new ArrayList<>(MAX_CLASS_SHIFT + 1);
    private final Runnable onRelease;

    /**
     * @param capacityBytes upper bound on bytes handed out at any time (in flight in the network layer plus
     *                      buffered for the application)
     * @param onRelease     called after a release, so a network thread parked for lack of memory can resume
     */
    public DirectBufferPool(long capacityBytes, Runnable onRelease) {
        this.capacityBytes = capacityBytes;
        this.onRelease = onRelease;
        for (int i = 0; i <= MAX_CLASS_SHIFT; i++)
            free.add(new ConcurrentLinkedDeque<>());
    }

    private static int classOf(int sizeBytes) {
        int shift = MIN_CLASS_SHIFT;
        while ((1 << shift) < sizeBytes && shift < MAX_CLASS_SHIFT)
            shift++;
        return shift;
    }

    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        int shift = classOf(sizeBytes);
        int classSize = 1 << shift;
        if (classSize < sizeBytes)
            return null; // larger than the largest class: let the caller fail the receive as too large
        ByteBuffer buffer = free.get(shift).pollFirst();
        if (buffer == null) {
            // Grow only within the capacity; otherwise report no memory (the network layer retries later).
            long after = outstandingBytes.addAndGet(classSize);
            if (after > capacityBytes) {
                outstandingBytes.addAndGet(-classSize);
                return null;
            }
            buffer = ByteBuffer.allocateDirect(classSize);
            allocatedBytes.addAndGet(classSize);
        } else {
            outstandingBytes.addAndGet(classSize);
        }
        buffer.clear();
        buffer.limit(sizeBytes);
        return buffer;
    }

    @Override
    public void release(ByteBuffer buffer) {
        int classSize = buffer.capacity();
        int shift = Integer.numberOfTrailingZeros(classSize);
        if (shift < MIN_CLASS_SHIFT || shift > MAX_CLASS_SHIFT || (1 << shift) != classSize)
            throw new IllegalArgumentException("Buffer of capacity " + classSize + " does not belong to this pool");
        free.get(shift).addFirst(buffer);
        outstandingBytes.addAndGet(-classSize);
        onRelease.run();
    }

    @Override
    public long size() {
        return capacityBytes;
    }

    @Override
    public long availableMemory() {
        return Math.max(0, capacityBytes - outstandingBytes.get());
    }

    @Override
    public boolean isOutOfMemory() {
        return outstandingBytes.get() >= capacityBytes;
    }

    /** @return bytes currently handed out (network layer plus consumer) */
    public long outstandingBytes() {
        return outstandingBytes.get();
    }

    /** @return bytes of direct memory this pool has ever allocated (its footprint) */
    public long allocatedBytes() {
        return allocatedBytes.get();
    }
}
