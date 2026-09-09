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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.internals.AppInfoParser;
import org.apache.kafka.common.utils.internals.KafkaThread;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * The I/O side of the next-generation consumer, first cut (CONSUMER-NG-02 §4, levers (a) and (b)): one thread
 * that owns a {@link NetworkClient} whose selector reads into a bounded {@link DirectBufferPool}, keeps one
 * fetch in flight per broker while credit allows, and hands each partition's records to the application thread
 * as {@link FetchSegment}s that still point into the receive buffer.
 *
 * <p>Fetching is decoupled from the application's {@code poll()}: the next fetch to a broker is sent as soon as
 * the previous one completes and the credit (bytes in flight plus bytes queued but not yet delivered) is below
 * the limit. Idle, the only periodic activity is the broker's {@code fetch.max.wait.ms} long poll returning
 * empty; the thread has no timers of its own.
 *
 * <p>Scope of this cut: manual assignment only, no group membership, no offset commit.
 */
public final class FetchEngine implements AutoCloseable {

    /** What the application thread sees for one partition. */
    public static final class PartitionQueue {
        final TopicPartition partition;
        final ConcurrentLinkedQueue<FetchSegment> segments = new ConcurrentLinkedQueue<>();
        /** Written by the I/O thread as fetches are sent; the offset the next fetch will ask for. */
        volatile long nextFetchOffset;
        /** Set by the I/O thread when the position must be reset by the application. */
        volatile Errors error = Errors.NONE;

        PartitionQueue(TopicPartition partition, long fetchOffset) {
            this.partition = partition;
            this.nextFetchOffset = fetchOffset;
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

        public Errors error() {
            return error;
        }
    }

    private static final class NodeState {
        final int nodeId;
        final FetchSessionHandler sessionHandler;
        boolean inFlight;
        /** Partitions covered by the request in flight, with the offsets it asked for. */
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
    private final Time time;
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

    private final Map<TopicPartition, PartitionQueue> queues = new ConcurrentHashMap<>();
    private final Map<Integer, NodeState> nodes = new HashMap<>();
    private final ConcurrentLinkedQueue<Runnable> commands = new ConcurrentLinkedQueue<>();
    /** Bytes handed to the application side and not yet released (segments not drained). */
    private final AtomicLong queuedBytes = new AtomicLong();
    private long inFlightBytes;
    private final AtomicBoolean running = new AtomicBoolean(true);
    /** Set by the I/O thread when it could not fetch for lack of credit or pool memory; cleared when it fetches again. */
    private volatile boolean starved;
    private final KafkaThread thread;
    private final Consumer<Void> onData;

    /**
     * @param config     consumer configuration (fetch.* and network settings are honoured)
     * @param creditBytes upper bound on bytes in flight plus bytes queued for the application
     * @param onData     called on the I/O thread when a segment was queued (wakes the application thread)
     */
    public FetchEngine(ConsumerConfig config, long creditBytes, Consumer<Void> onData) {
        String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
        LogContext logContext = new LogContext("[ng-fetch clientId=" + clientId + "] ");
        this.log = logContext.logger(FetchEngine.class);
        this.time = Time.SYSTEM;
        this.creditBytes = creditBytes;
        this.onData = onData;
        this.fetchMaxWaitMs = config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
        this.fetchMinBytes = config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        this.fetchMaxBytes = config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG);
        this.maxPartitionFetchBytes = config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.isolationLevel = IsolationLevel.valueOf(config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(java.util.Locale.ROOT));
        this.clientRackId = config.getString(ConsumerConfig.CLIENT_RACK_CONFIG);

        // The subscription state only tells ConsumerMetadata which topics to ask the brokers about.
        this.subscriptions = new SubscriptionState(logContext, org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.EARLIEST);
        this.metadata = new ConsumerMetadata(config, subscriptions, logContext, new ClusterResourceListeners());
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG));
        metadata.bootstrap(addresses);

        // The pool's capacity leaves room above the credit for the response being read while the credit is spent.
        this.pool = new DirectBufferPool(creditBytes + 2L * fetchMaxBytes, this::memoryReleased);
        Metrics metrics = new Metrics(time);
        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext);
        Selector selector = new Selector(NetworkReceive.UNLIMITED,
                config.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                0,
                metrics, time, "ng-consumer", Map.of(), false, false,
                channelBuilder, pool, logContext);
        this.client = new NetworkClient(null, metadata, selector, clientId,
                1,
                config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG),
                config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                config.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG),
                config.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
                requestTimeoutMs,
                config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                time, true, new ApiVersions(), null, logContext,
                new org.apache.kafka.clients.DefaultHostResolver(), null,
                config.getLong(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG),
                MetadataRecoveryStrategy.forName(config.getString(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG)),
                ClientUtils.bootstrapConfiguration(config, config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)),
                config.getBoolean(CommonClientConfigs.METADATA_CLUSTER_CHECK_ENABLE_CONFIG));
        this.thread = new KafkaThread("ng-consumer-fetch-" + clientId, this::run, true);
        log.debug("Fetch engine created, kafka version {}", AppInfoParser.getVersion());
    }

    public void start() {
        thread.start();
    }

    // ---- application thread API --------------------------------------------------------------------------------

    /** Assigns partitions; positions are resolved by {@link #seek} or by a reset to the earliest offset. */
    public void assign(Collection<TopicPartition> partitions) {
        // Queues are created here so the caller can attach to them at once; the I/O thread picks them up on its
        // next pass (the map is concurrent) and refreshes metadata for any new topic.
        for (TopicPartition tp : partitions)
            queues.computeIfAbsent(tp, t -> new PartitionQueue(t, -1L));
        queues.keySet().removeIf(tp -> !partitions.contains(tp));
        subscriptions.assignFromUser(new java.util.HashSet<>(partitions));
        commands.add(metadata::requestUpdateForNewTopics);
        wakeup();
    }

    public void seek(TopicPartition partition, long offset) {
        commands.add(() -> {
            PartitionQueue q = queues.get(partition);
            if (q != null) {
                q.nextFetchOffset = offset;
                q.error = Errors.NONE;
                FetchSegment s;
                while ((s = q.poll()) != null) {
                    queuedBytes.addAndGet(-s.sizeInBytes);
                    s.release();
                }
            }
        });
        wakeup();
    }

    public PartitionQueue queue(TopicPartition partition) {
        return queues.get(partition);
    }

    public Collection<PartitionQueue> queues() {
        return queues.values();
    }

    /** Application thread: a segment was fully delivered; return its bytes to the credit. */
    public void released(FetchSegment segment) {
        queuedBytes.addAndGet(-segment.sizeInBytes);
        segment.release();
        if (starved)
            wakeup();
    }

    /**
     * Pool callback. Only a release from another thread while the I/O thread stopped fetching for lack of memory or
     * credit needs a wake-up; the I/O thread releasing an empty response's buffer to itself must not cost a syscall.
     */
    private void memoryReleased() {
        if (starved && Thread.currentThread() != thread)
            wakeup();
    }

    public void wakeup() {
        client.wakeup();
    }

    public long queuedBytes() {
        return queuedBytes.get();
    }

    public DirectBufferPool pool() {
        return pool;
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            wakeup();
            try {
                thread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            Utils.closeQuietly(client, "network client");
        }
    }

    // ---- I/O thread ----------------------------------------------------------------------------------------------

    private void run() {
        try {
            while (running.get()) {
                Runnable cmd;
                while ((cmd = commands.poll()) != null)
                    cmd.run();
                long now = time.milliseconds();
                resolveUnknownPositions(now);
                sendFetches(now);
                client.poll(fetchMaxWaitMs + 1_000L, now);
                for (NodeState node : nodes.values()) {
                    if (node.pendingResponse != null)
                        handleFetchResponse(node);
                }
            }
        } catch (Throwable t) {
            log.error("Fetch engine failed", t);
        }
    }

    private void resolveUnknownPositions(long now) {
        // First cut: partitions without a position start from the earliest offset (one ListOffsets per partition).
        for (PartitionQueue q : queues.values()) {
            if (q.nextFetchOffset >= 0 || q.error == Errors.OFFSET_OUT_OF_RANGE)
                continue;
            Node leader = leaderFor(q.partition);
            if (leader == null || !client.ready(leader, now))
                continue;
            q.error = Errors.OFFSET_OUT_OF_RANGE; // marks "lookup in progress" for this cut
            ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder.forConsumer(true, isolationLevel)
                    .setTargetTimes(List.of(new org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic()
                            .setName(q.partition.topic())
                            .setPartitions(List.of(new org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition()
                                    .setPartitionIndex(q.partition.partition())
                                    .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)))));
            ClientRequest request = client.newClientRequest(leader.idString(), builder, now, true, requestTimeoutMs, response -> {
                if (response.hasResponse() && response.responseBody() instanceof ListOffsetsResponse) {
                    ListOffsetsResponse lor = (ListOffsetsResponse) response.responseBody();
                    lor.topics().forEach(t -> t.partitions().forEach(p -> {
                        if (Errors.forCode(p.errorCode()) == Errors.NONE) {
                            q.nextFetchOffset = p.offset();
                            q.error = Errors.NONE;
                        } else {
                            q.error = Errors.NONE; // retry on the next pass
                        }
                    }));
                } else {
                    q.error = Errors.NONE;
                }
            });
            client.send(request, now);
        }
    }

    private Node leaderFor(TopicPartition tp) {
        Cluster cluster = metadata.fetch();
        Node leader = cluster.leaderFor(tp);
        if (leader == null)
            metadata.requestUpdate(true);
        return leader;
    }

    private void sendFetches(long now) {
        if (queuedBytes.get() + inFlightBytes >= creditBytes || pool.isOutOfMemory()) {
            starved = true;
            return;
        }
        Map<Node, FetchSessionHandler.Builder> builders = new LinkedHashMap<>();
        Cluster cluster = metadata.fetch();
        for (PartitionQueue q : queues.values()) {
            if (q.nextFetchOffset < 0 || q.error != Errors.NONE)
                continue;
            Node leader = leaderFor(q.partition);
            if (leader == null)
                continue;
            NodeState state = nodes.computeIfAbsent(leader.id(), id -> new NodeState(new LogContext("[ng-fetch node=" + id + "] "), id));
            if (state.inFlight || !client.ready(leader, now))
                continue;
            FetchSessionHandler.Builder builder = builders.computeIfAbsent(leader, n -> state.sessionHandler.newBuilder());
            Uuid topicId = cluster.topicId(q.partition.topic());
            Optional<Integer> leaderEpoch = metadata.currentLeader(q.partition).epoch;
            builder.add(q.partition, new FetchRequest.PartitionData(topicId, q.nextFetchOffset, -1L, maxPartitionFetchBytes, leaderEpoch));
            state.inFlightOffsets.put(q.partition, q.nextFetchOffset);
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

    /** Network callback (I/O thread): remember the response; the receive buffer is paired after poll returns. */
    private void onFetchResponse(NodeState state, ClientResponse response) {
        state.inFlight = false;
        inFlightBytes -= fetchMaxBytes;
        if (response.hasResponse()) {
            state.pendingResponse = (FetchResponse) response.responseBody();
            state.pendingVersion = response.requestHeader().apiVersion();
            state.pendingPayload = response.payload();
        } else {
            state.sessionHandler.handleError(response.authenticationException() != null
                    ? response.authenticationException() : new KafkaException("fetch failed: " + response));
            state.inFlightOffsets.clear();
            metadata.requestUpdate(false);
        }
    }

    private void handleFetchResponse(NodeState state) {
        FetchResponse response = state.pendingResponse;
        state.pendingResponse = null;
        Map<TopicPartition, Long> asked = new HashMap<>(state.inFlightOffsets);
        state.inFlightOffsets.clear();
        if (!state.sessionHandler.handleResponse(response, state.pendingVersion)) {
            metadata.requestUpdate(false);
            return;
        }
        // The records of the response point into the receive buffer; the segments' refcount returns it to the pool.
        FetchSegment.Owner owner = new FetchSegment.Owner(state.pendingPayload, pool);
        state.pendingPayload = null;
        boolean queued = false;
        for (Map.Entry<TopicPartition, FetchResponseData.PartitionData> entry
                : response.responseData(state.sessionHandler.sessionTopicNames(), state.pendingVersion).entrySet()) {
            TopicPartition tp = entry.getKey();
            FetchResponseData.PartitionData data = entry.getValue();
            PartitionQueue q = queues.get(tp);
            Long fetchOffset = asked.get(tp);
            if (q == null || fetchOffset == null || q.nextFetchOffset != fetchOffset)
                continue; // seek or unassign raced with this fetch
            if (!handlePartitionError(tp, q, Errors.forCode(data.errorCode())))
                continue;
            MemoryRecords records = (MemoryRecords) FetchResponse.recordsOrFail(data);
            if (records.sizeInBytes() == 0)
                continue;
            long nextOffset = lastOffsetAfter(records, fetchOffset);
            if (nextOffset <= fetchOffset)
                continue; // no complete batch (record larger than max.partition.fetch.bytes is out of scope for this cut)
            q.nextFetchOffset = nextOffset;
            FetchSegment segment = new FetchSegment(tp, fetchOffset, records, data.highWatermark(), data.lastStableOffset(), owner);
            queuedBytes.addAndGet(segment.sizeInBytes);
            q.segments.add(segment);
            queued = true;
        }
        owner.creationDone();
        if (queued)
            onData.accept(null);
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
                q.nextFetchOffset = -1L; // re-resolve from the earliest offset
                q.error = Errors.NONE;
                return false;
            default:
                log.warn("Fetch error for {}: {}", tp, error);
                return false;
        }
    }

    /** @return the offset after the last complete batch, walking batch headers only (no record parsing) */
    private static long lastOffsetAfter(MemoryRecords records, long fetchOffset) {
        long next = fetchOffset;
        for (org.apache.kafka.common.record.internal.MutableRecordBatch batch : records.batches())
            next = Math.max(next, batch.lastOffset() + 1);
        return next;
    }
}
