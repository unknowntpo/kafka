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

import org.apache.kafka.common.utils.internals.BufferSupplier;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Decodes fetch responses before the application asks for them. A {@link CompletedFetch} is handed to a decoder
 * thread as soon as it is buffered; that thread verifies, decompresses and deserializes its records. The
 * application thread then only takes the decoded records. If the application thread reaches a
 * {@link CompletedFetch} that no decoder has started, it decodes it itself, so it never waits for a queue.
 *
 * <p>Deserializers are called from several threads at once, so this is only used with the deserializers of
 * this project that keep no state between calls.
 */
final class FetchPredecoder<K, V> implements AutoCloseable {

    private final FetchConfig fetchConfig;
    private final Deserializers<K, V> deserializers;
    private final BufferSupplier bufferSupplier = new SharedBufferSupplier();
    private final ThreadPoolExecutor executor;

    private FetchPredecoder(FetchConfig fetchConfig, Deserializers<K, V> deserializers, int threads) {
        this.fetchConfig = fetchConfig;
        this.deserializers = deserializers;
        AtomicInteger id = new AtomicInteger();
        this.executor = new ThreadPoolExecutor(threads, threads, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> {
            Thread t = new Thread(r, "consumer-fetch-decoder-" + id.getAndIncrement());
            t.setDaemon(true);
            return t;
        });
        this.executor.allowCoreThreadTimeOut(true);
    }

    /**
     * @return a decoder, or null if the deserializers may keep state or the machine has a single processor
     */
    static <K, V> FetchPredecoder<K, V> maybeCreate(FetchConfig fetchConfig, Deserializers<K, V> deserializers) {
        int processors = Runtime.getRuntime().availableProcessors();
        int threads = Integer.getInteger("c3x.predecodeThreads", Math.min(4, processors / 2));
        if (threads <= 0 || !deserializers.simple())
            return null;
        return new FetchPredecoder<>(fetchConfig, deserializers, threads);
    }

    // Experiment for CONSUMER-3X D-25: the thread that received a response also decodes it.
    private static final boolean DECODE_ON_RECEIVER = Boolean.getBoolean("c3x.decodeOnReceiver");

    void submit(CompletedFetch completedFetch) {
        FetchLaneTransport receiver = DECODE_ON_RECEIVER ? FetchLaneTransport.HANDLING_RESPONSE_OF.get() : null;
        (receiver != null ? AbstractFetch.LaneCounters.DECODED_ON_RECEIVER : AbstractFetch.LaneCounters.DECODED_ON_POOL).increment();
        if (receiver != null)
            receiver.execute(() -> completedFetch.predecode(fetchConfig, deserializers, bufferSupplier));
        else if (!executor.isShutdown())
            executor.execute(() -> completedFetch.predecode(fetchConfig, deserializers, bufferSupplier));
    }

    @Override
    public void close() {
        executor.shutdownNow();
        try {
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        bufferSupplier.close();
    }

    /** A decompression buffer cache that several threads can use. */
    private static final class SharedBufferSupplier extends BufferSupplier {
        private final ConcurrentHashMap<Integer, ConcurrentLinkedDeque<ByteBuffer>> cache = new ConcurrentHashMap<>();

        @Override
        public ByteBuffer get(int capacity) {
            ConcurrentLinkedDeque<ByteBuffer> buffers = cache.get(capacity);
            ByteBuffer buffer = buffers == null ? null : buffers.pollFirst();
            return buffer != null ? buffer : ByteBuffer.allocate(capacity);
        }

        @Override
        public void release(ByteBuffer buffer) {
            buffer.clear();
            cache.computeIfAbsent(buffer.capacity(), k -> new ConcurrentLinkedDeque<>()).addFirst(buffer);
        }

        @Override
        public void close() {
            cache.clear();
        }
    }
}
