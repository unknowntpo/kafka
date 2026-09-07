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

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Producer-side access to the fetch handoff. It exposes neither consumption nor an unconditional wakeup.
 * Request bookkeeping has the same confinement as AbstractFetch: the network thread for async consumers,
 * or the Fetcher monitor for classic consumers. Buffer access retains the buffer's own synchronization.
 * This is not a global publication barrier or an operation-generation fence.
 */
final class FetchBufferProducer implements Closeable {
    private final FetchBuffer buffer;
    private final Set<Integer> pendingNodes = new HashSet<>();

    FetchBufferProducer(FetchBuffer buffer) {
        this.buffer = buffer;
    }

    boolean hasCompletedFetches() {
        return !buffer.isEmpty();
    }

    boolean hasAvailableFetches(Predicate<TopicPartition> isFetchable) {
        return buffer.hasCompletedFetches(fetch -> isFetchable.test(fetch.partition));
    }

    Set<TopicPartition> bufferedPartitions() {
        return Set.copyOf(buffer.bufferedPartitions());
    }

    void add(CompletedFetch fetch) {
        // FetchBuffer stores the result before signalling under its existing lock.
        buffer.add(fetch);
    }

    void signalIfAvailable(Predicate<TopicPartition> isFetchable) {
        // Include the partially consumed next-in-line fetch, not just the queue.
        if (bufferedPartitions().stream().anyMatch(isFetchable))
            buffer.wakeup();
    }

    boolean hasPendingRequests() {
        return !pendingNodes.isEmpty();
    }

    boolean isRequestPending(int nodeId) {
        return pendingNodes.contains(nodeId);
    }

    void requestStarted(int nodeId) {
        pendingNodes.add(nodeId);
    }

    void requestCompleted(int nodeId) {
        // Empty, failed and session-error responses can all enable another preparation.
        // An unregistered or already completed node is not a new enabling input.
        if (pendingNodes.remove(nodeId))
            buffer.wakeup();
    }

    @Override
    public void close() {
        buffer.close();
    }
}
