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
package org.apache.kafka.common.network;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * CONSUMER-3X experiment (D-19), NOT SAFE FOR USE: measures the upper bound of receiving into reused direct buffers.
 * A buffer is used again a fixed time after it was handed out, without knowing whether its records are still in use.
 * -Dc3x.directReceive=pool: reused direct buffers; =fresh: a new direct buffer for every response.
 */
final class ReceiveBufferExperiment {
    static final String MODE = System.getProperty("c3x.directReceive", "");
    private static final int MIN_SIZE = 64 * 1024;
    private static final long QUARANTINE_NS = Long.getLong("c3x.directReceiveQuarantineMs", 500) * 1_000_000L;

    private static final class Entry {
        final ByteBuffer buffer;
        final long handedOutNs;
        Entry(ByteBuffer buffer, long handedOutNs) {
            this.buffer = buffer;
            this.handedOutNs = handedOutNs;
        }
    }

    private static final java.util.List<ConcurrentLinkedDeque<Entry>> POOLS = new java.util.ArrayList<>();

    static {
        for (int i = 0; i < 16; i++)
            POOLS.add(new ConcurrentLinkedDeque<>());
    }

    static ByteBuffer allocate(int size) {
        if (MODE.isEmpty() || size < MIN_SIZE)
            return null;
        if (MODE.equals("fresh"))
            return ByteBuffer.allocateDirect(size);
        int sizeClass = Math.min(POOLS.size() - 1, 32 - Integer.numberOfLeadingZeros((size - 1) / MIN_SIZE));
        int capacity = MIN_SIZE << sizeClass;
        if (capacity < size)
            return ByteBuffer.allocateDirect(size);
        long now = System.nanoTime();
        Entry oldest = POOLS.get(sizeClass).peekFirst();
        ByteBuffer buffer = null;
        if (oldest != null && now - oldest.handedOutNs > QUARANTINE_NS) {
            Entry taken = POOLS.get(sizeClass).pollFirst();
            if (taken != null && now - taken.handedOutNs > QUARANTINE_NS)
                buffer = taken.buffer;
            else if (taken != null)
                POOLS.get(sizeClass).addFirst(taken);
        }
        if (buffer == null)
            buffer = ByteBuffer.allocateDirect(capacity);
        POOLS.get(sizeClass).addLast(new Entry(buffer, now));
        ByteBuffer view = buffer.duplicate();
        view.clear().limit(size);
        return view.slice();
    }

    private ReceiveBufferExperiment() { }
}
