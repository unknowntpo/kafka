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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Decodes fetch responses before the application asks for them. A {@link CompletedFetch} is handed to a decoder
 * thread as soon as it is buffered. That thread verifies, decompresses and deserializes its records, and the
 * application thread then only takes the decoded records. If the application thread reaches a
 * {@link CompletedFetch} that no decoder has started, it decodes it itself, so it never waits for the queue.
 *
 * <p>The deserializers are called from several threads at once, so this is only used with the deserializers of
 * this project that keep no state between calls, see {@link Deserializers#simple()}.
 */
final class FetchPredecoder<K, V> implements AutoCloseable {

    private final FetchConfig fetchConfig;
    private final Deserializers<K, V> deserializers;
    // The decompression buffers of all decoder threads. BufferSupplier.create() is not thread-safe.
    private final BufferSupplier bufferSupplier = new SharedBufferSupplier();
    private final ThreadPoolExecutor executor;

    FetchPredecoder(FetchConfig fetchConfig, Deserializers<K, V> deserializers, int threads) {
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
     * @return a decoder with {@code min(4, processors / 2)} threads, at least one, or null if the deserializers
     *         may keep state, in which case the application thread decodes the records
     */
    static <K, V> FetchPredecoder<K, V> maybeCreate(FetchConfig fetchConfig, Deserializers<K, V> deserializers) {
        if (!deserializers.simple())
            return null;
        int threads = Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors() / 2));
        return new FetchPredecoder<>(fetchConfig, deserializers, threads);
    }

    /**
     * Queues the fetch for decoding. Called on the network thread. After {@link #close()} the fetch is not queued,
     * and the application thread decodes it itself.
     */
    void submit(CompletedFetch completedFetch) {
        try {
            executor.execute(() -> completedFetch.predecode(fetchConfig, deserializers, bufferSupplier));
        } catch (RejectedExecutionException e) {
            // closed: the application thread decodes this fetch
        }
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

    /** A decompression buffer cache that several threads can use at once. */
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
