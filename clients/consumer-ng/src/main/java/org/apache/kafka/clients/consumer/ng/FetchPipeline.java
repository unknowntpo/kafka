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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.FetchMetricsManager;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The fetch path of the next-generation consumer (CONSUMER-NG-03), as a component of the engine's I/O thread:
 * one fetch in flight per broker, re-issued as soon as the previous one completes while the credit (bytes in
 * flight plus bytes queued but not yet delivered) allows; each partition's records handed to the application
 * thread as {@link FetchSegment}s that still point into the pooled receive buffer.
 *
 * <p>Positions are the {@link SubscriptionState}'s. The pipeline keeps a private prefetch cursor per partition
 * ({@code nextFetchOffset}) that runs ahead of the consumed position; the cursor starts from the subscription
 * position when a partition becomes fetchable and is reset to it on {@link #reset}. All methods run on the I/O
 * thread except the queue accessors and {@link #released}, which the application thread uses.
 */
public final class FetchPipeline {

    /** What the application thread sees for one partition. */
    public static final class PartitionQueue {
        final TopicPartition partition;
        final ConcurrentLinkedQueue<FetchSegment> segments = new ConcurrentLinkedQueue<>();
        /** I/O thread: the offset the next fetch will ask for; -1 until the position is known. */
        long nextFetchOffset = -1L;
        /** I/O thread: a fetch error was raised to the application; no refetch until its next poll (or a seek). */
        boolean errorRaised;

        PartitionQueue(TopicPartition partition) {
            this.partition = partition;
        }

        public TopicPartition partition() {
            return partition;
        }

        public FetchSegment peek() {
            return segments.peek();
        }

        public FetchSegment poll() {
            return segments.poll();
        }

        public boolean isEmpty() {
            return segments.isEmpty();
        }
    }

    private static final class NodeState {
        final int nodeId;
        final FetchSessionHandler sessionHandler;
        boolean inFlight;
        final Map<TopicPartition, Long> inFlightOffsets = new HashMap<>();
        FetchResponse pendingResponse;
        short pendingVersion;
        ByteBuffer pendingPayload;

        NodeState(LogContext logContext, int nodeId) {
            this.nodeId = nodeId;
            this.sessionHandler = new FetchSessionHandler(logContext, nodeId);
        }
    }

    private final Logger log;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final NetworkClient client;
    private final DirectBufferPool pool;
    private final int fetchMaxWaitMs;
    private final int fetchMinBytes;
    private final int fetchMaxBytes;
    private final int maxPartitionFetchBytes;
    private final int requestTimeoutMs;
    private final long creditBytes;
    private final IsolationLevel isolationLevel;
    private final String clientRackId;
    private final Runnable onData;
    private final java.util.function.Consumer<RuntimeException> onError;
    private final FetchMetricsManager metricsManager;

    private final Map<TopicPartition, PartitionQueue> queues = new ConcurrentHashMap<>();
    /** Receive buffers taken over from the network layer (a fetch response's records point into them). */
    private final Set<ByteBuffer> claimedPayloads = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Map<Integer, NodeState> nodes = new HashMap<>();
    private final AtomicLong queuedBytes = new AtomicLong();
    private long inFlightBytes;
    private volatile boolean starved;

    public FetchPipeline(ConsumerConfig config, LogContext logContext, SubscriptionState subscriptions, ConsumerMetadata metadata,
                         NetworkClient client, DirectBufferPool pool, long creditBytes, Runnable onData,
                         java.util.function.Consumer<RuntimeException> onError, FetchMetricsManager metricsManager) {
        this.log = logContext.logger(FetchPipeline.class);
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.client = client;
        this.pool = pool;
        this.creditBytes = creditBytes;
        this.onData = onData;
        this.onError = onError;
        this.metricsManager = metricsManager;
        this.fetchMaxWaitMs = config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
        this.fetchMinBytes = config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        this.fetchMaxBytes = config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG);
        this.maxPartitionFetchBytes = config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.isolationLevel = IsolationLevel.valueOf(config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT));
        this.clientRackId = config.getString(ConsumerConfig.CLIENT_RACK_CONFIG);
    }

    // ---- application thread -------------------------------------------------------------------------------------

    public PartitionQueue queue(TopicPartition partition) {
        return queues.get(partition);
    }

    /** Application thread: a segment was fully delivered; return its bytes to the credit. */
    public void released(FetchSegment segment, int deliveredRecords) {
        queuedBytes.addAndGet(-segment.sizeInBytes);
        segment.release(deliveredRecords);
    }

    /** @return true if a release from another thread should wake the I/O thread (it stopped fetching for lack of credit) */
    public boolean starved() {
        return starved;
    }

    public long queuedBytes() {
        return queuedBytes.get();
    }

    /** @return the earliest {@code fetch.max.wait.ms}-bounded time the I/O thread must wake for, or MAX if none */
    public long fetchMaxWaitMs() {
        return fetchMaxWaitMs;
    }

    // ---- I/O thread ----------------------------------------------------------------------------------------------

    /** Keeps the per-partition queues in step with the assignment; drops queues (and their credit) of removed partitions. */
    public void syncAssignment() {
        metricsManager.maybeUpdateAssignment(subscriptions);
        Set<TopicPartition> assigned = subscriptions.assignedPartitions();
        for (TopicPartition tp : assigned)
            queues.computeIfAbsent(tp, PartitionQueue::new);
        queues.entrySet().removeIf(e -> {
            if (assigned.contains(e.getKey()))
                return false;
            drain(e.getValue());
            return true;
        });
    }

    /** The subscription position of {@code partition} changed by a seek or reset: restart the prefetch cursor from it. */
    public void reset(TopicPartition partition) {
        PartitionQueue q = queues.get(partition);
        if (q == null)
            return;
        drain(q);
        q.nextFetchOffset = -1L;
        q.errorRaised = false;
    }

    /** I/O thread, once per application poll: partitions whose error the application has now seen may be fetched again. */
    public void resumeAfterErrors() {
        for (PartitionQueue q : queues.values())
            q.errorRaised = false;
    }

    /** @return true if the network layer's receive buffer belongs to a fetch response and must not be released by the sweep */
    public boolean claimed(ByteBuffer payload) {
        return claimedPayloads.contains(payload);
    }

    private void drain(PartitionQueue q) {
        FetchSegment s;
        while ((s = q.poll()) != null) {
            queuedBytes.addAndGet(-s.sizeInBytes);
            s.release();
        }
    }

    public void sendFetches(long now) {
        if (queuedBytes.get() + inFlightBytes >= creditBytes || pool.isOutOfMemory()) {
            starved = true;
            return;
        }
        Map<Node, FetchSessionHandler.Builder> builders = new LinkedHashMap<>();
        Cluster cluster = metadata.fetch();
        for (PartitionQueue q : queues.values()) {
            TopicPartition tp = q.partition;
            if (!subscriptions.isFetchable(tp) || q.errorRaised)
                continue;
            if (q.nextFetchOffset < 0) {
                SubscriptionState.FetchPosition position = subscriptions.position(tp);
                if (position == null)
                    continue;
                q.nextFetchOffset = position.offset;
            }
            Node leader = cluster.leaderFor(tp);
            if (leader == null) {
                metadata.requestUpdate(true);
                continue;
            }
            NodeState state = nodes.computeIfAbsent(leader.id(), id -> new NodeState(new LogContext("[ng-fetch node=" + id + "] "), id));
            if (state.inFlight || !client.ready(leader, now))
                continue;
            FetchSessionHandler.Builder builder = builders.computeIfAbsent(leader, n -> state.sessionHandler.newBuilder());
            Uuid topicId = cluster.topicId(tp.topic());
            Optional<Integer> leaderEpoch = metadata.currentLeader(tp).epoch;
            builder.add(tp, new FetchRequest.PartitionData(topicId, q.nextFetchOffset, -1L, maxPartitionFetchBytes, leaderEpoch));
            state.inFlightOffsets.put(tp, q.nextFetchOffset);
        }
        for (Map.Entry<Node, FetchSessionHandler.Builder> entry : builders.entrySet()) {
            Node node = entry.getKey();
            NodeState state = nodes.get(node.id());
            FetchSessionHandler.FetchRequestData data = entry.getValue().build();
            short maxVersion = data.canUseTopicIds() ? ApiKeys.FETCH.latestVersion() : (short) 12;
            FetchRequest.Builder request = FetchRequest.Builder
                    .forConsumer(maxVersion, fetchMaxWaitMs, fetchMinBytes, data.toSend())
                    .isolationLevel(isolationLevel)
                    .setMaxBytes(fetchMaxBytes)
                    .metadata(data.metadata())
                    .removed(data.toForget())
                    .replaced(data.toReplace())
                    .rackId(clientRackId);
            state.inFlight = true;
            starved = false;
            inFlightBytes += fetchMaxBytes;
            ClientRequest clientRequest = client.newClientRequest(node.idString(), request, now, true, requestTimeoutMs,
                response -> onFetchResponse(state, response));
            client.send(clientRequest, now);
        }
    }

    /** Network callback (I/O thread): the response is handled after the network poll returns. */
    private void onFetchResponse(NodeState state, ClientResponse response) {
        state.inFlight = false;
        inFlightBytes -= fetchMaxBytes;
        metricsManager.recordLatency(response.destination(), response.requestLatencyMs());
        if (response.hasResponse()) {
            state.pendingResponse = (FetchResponse) response.responseBody();
            state.pendingVersion = response.requestHeader().apiVersion();
            state.pendingPayload = response.payload();
            if (state.pendingPayload != null)
                claimedPayloads.add(state.pendingPayload);
        } else {
            state.sessionHandler.handleError(response.authenticationException() != null
                    ? response.authenticationException() : new KafkaException("fetch failed: " + response));
            state.inFlightOffsets.clear();
            metadata.requestUpdate(false);
        }
    }

    /** @return true if at least one segment was queued */
    public boolean handleResponses() {
        boolean queued = false;
        for (NodeState node : nodes.values()) {
            if (node.pendingResponse != null)
                queued |= handleFetchResponse(node);
        }
        if (queued)
            onData.run();
        return queued;
    }

    private boolean handleFetchResponse(NodeState state) {
        FetchResponse response = state.pendingResponse;
        state.pendingResponse = null;
        Map<TopicPartition, Long> asked = new HashMap<>(state.inFlightOffsets);
        state.inFlightOffsets.clear();
        ByteBuffer payload = state.pendingPayload;
        state.pendingPayload = null;
        claimedPayloads.remove(payload);
        if (!state.sessionHandler.handleResponse(response, state.pendingVersion)) {
            metadata.requestUpdate(false);
            pool.releaseIfPooled(payload);
            return false;
        }
        FetchSegment.Owner owner = new FetchSegment.Owner(payload, pool, metricsManager);
        boolean queued = false;
        for (Map.Entry<TopicPartition, FetchResponseData.PartitionData> entry
                : response.responseData(state.sessionHandler.sessionTopicNames(), state.pendingVersion).entrySet()) {
            TopicPartition tp = entry.getKey();
            PartitionQueue q = queues.get(tp);
            Long fetchOffset = asked.get(tp);
            if (q == null || fetchOffset == null || q.nextFetchOffset != fetchOffset)
                continue; // a seek, reset or unassignment raced with this fetch
            queued |= queuePartitionData(q, fetchOffset, entry.getValue(), owner);
        }
        owner.creationDone();
        return queued;
    }

    /** @return true if a segment was queued for the partition */
    private boolean queuePartitionData(PartitionQueue q, long fetchOffset, FetchResponseData.PartitionData data, FetchSegment.Owner owner) {
        TopicPartition tp = q.partition;
        if (!handlePartitionError(tp, q, Errors.forCode(data.errorCode())))
            return false;
        // Log offsets of the partition, for lag/lead metrics and currentLag(); the safe variants tolerate an unassignment race.
        if (data.highWatermark() >= 0)
            subscriptions.tryUpdatingHighWatermark(tp, data.highWatermark());
        if (data.logStartOffset() >= 0)
            subscriptions.tryUpdatingLogStartOffset(tp, data.logStartOffset());
        if (data.lastStableOffset() >= 0)
            subscriptions.tryUpdatingLastStableOffset(tp, data.lastStableOffset());
        MemoryRecords records = (MemoryRecords) FetchResponse.recordsOrFail(data);
        if (records.sizeInBytes() == 0)
            return false;
        long nextOffset = lastOffsetAfter(records, fetchOffset);
        if (nextOffset <= fetchOffset)
            return false;
        q.nextFetchOffset = nextOffset;
        FetchSegment segment = new FetchSegment(tp, fetchOffset, records, data.highWatermark(), data.lastStableOffset(), owner);
        queuedBytes.addAndGet(segment.sizeInBytes);
        q.segments.add(segment);
        return true;
    }

    /** @return true if the partition's data can be used; false if the error was handled and the data must be skipped */
    private boolean handlePartitionError(TopicPartition tp, PartitionQueue q, Errors error) {
        switch (error) {
            case NONE:
                return true;
            case NOT_LEADER_OR_FOLLOWER:
            case FENCED_LEADER_EPOCH:
            case UNKNOWN_TOPIC_OR_PARTITION:
            case UNKNOWN_TOPIC_ID:
            case UNKNOWN_LEADER_EPOCH:
            case LEADER_NOT_AVAILABLE:
                metadata.requestUpdate(false);
                return false;
            case OFFSET_OUT_OF_RANGE:
                if (subscriptions.hasDefaultOffsetResetPolicy()) {
                    // The subscription state decides how to reset (auto.offset.reset); the cursor follows the new position.
                    log.info("Fetch offset {} is out of range for partition {}, resetting offset", q.nextFetchOffset, tp);
                    subscriptions.requestOffsetResetIfPartitionAssigned(tp);
                } else {
                    log.info("Fetch offset {} is out of range for partition {}, raising error to the application since no reset policy is configured", q.nextFetchOffset, tp);
                    onError.accept(new OffsetOutOfRangeException("Fetch position " + q.nextFetchOffset + " is out of range for partition " + tp,
                            Map.of(tp, q.nextFetchOffset)));
                    q.errorRaised = true;
                }
                q.nextFetchOffset = -1L;
                return false;
            default:
                log.warn("Fetch error for {}: {}", tp, error);
                return false;
        }
    }

    /** @return the offset after the last complete batch, walking batch headers only (no record parsing) */
    private static long lastOffsetAfter(MemoryRecords records, long fetchOffset) {
        long next = fetchOffset;
        for (MutableRecordBatch batch : records.batches())
            next = Math.max(next, batch.lastOffset() + 1);
        return next;
    }
}
