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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.clients.consumer.internals.FetchUtils.requestMetadataUpdate;

/**
 * {@code AbstractFetch} represents the basic state and logic for record fetching processing.
 */
public abstract class AbstractFetch implements Closeable {

    private final Logger log;
    // Calling LoggerFactory.getLogger() is pretty expensive with log4j2. See KAFKA-18046 for details.
    // We cache the logger used by CompletedFetch because it is created on every fetch responses.
    private final Logger completedFetchLog;
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();
    protected final LogContext logContext;
    protected final ConsumerMetadata metadata;
    protected final SubscriptionState subscriptions;
    protected final FetchConfig fetchConfig;
    protected final Time time;
    protected final FetchMetricsManager metricsManager;
    protected final FetchBuffer fetchBuffer;
    protected final BufferSupplier decompressionBufferSupplier;
    protected final Set<Integer> nodesWithPendingFetchRequests;

    private final Map<Integer, FetchSessionHandler> sessionHandlers;

    private final ApiVersions apiVersions;

    public AbstractFetch(final LogContext logContext,
                         final ConsumerMetadata metadata,
                         final SubscriptionState subscriptions,
                         final FetchConfig fetchConfig,
                         final FetchBuffer fetchBuffer,
                         final FetchMetricsManager metricsManager,
                         final Time time,
                         final ApiVersions apiVersions) {
        this.log = logContext.logger(AbstractFetch.class);
        this.completedFetchLog = logContext.logger(CompletedFetch.class);
        this.logContext = logContext;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.fetchConfig = fetchConfig;
        this.fetchBuffer = fetchBuffer;
        this.decompressionBufferSupplier = BufferSupplier.create();
        this.sessionHandlers = new java.util.concurrent.ConcurrentHashMap<>();
        this.nodesWithPendingFetchRequests = java.util.concurrent.ConcurrentHashMap.newKeySet();
        this.metricsManager = metricsManager;
        this.time = time;
        this.apiVersions = apiVersions;
    }

    /**
     * Check if the node is disconnected and unavailable for immediate reconnection (i.e. if it is in
     * reconnect backoff window following the disconnect).
     *
     * @param node {@link Node} to check for availability
     * @see NetworkClientUtils#isUnavailable(KafkaClient, Node, Time)
     */
    protected abstract boolean isUnavailable(Node node);

    /**
     * Checks for an authentication error on a given node and throws the exception if it exists.
     *
     * @param node {@link Node} to check for a previous {@link AuthenticationException}; if found it is thrown
     * @see NetworkClientUtils#maybeThrowAuthFailure(KafkaClient, Node)
     */
    protected abstract void maybeThrowAuthFailure(Node node);

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe. Has
     * visibility for testing.
     *
     * @return true if there are completed fetches, false otherwise
     */
    boolean hasCompletedFetches() {
        return !fetchBuffer.isEmpty();
    }

    /**
     * Return whether we have any completed fetches that are fetchable. This method is thread-safe.
     * @return true if there are completed fetches that can be returned, false otherwise
     */
    public boolean hasAvailableFetches() {
        return fetchBuffer.hasCompletedFetches(fetch -> subscriptions.isFetchable(fetch.partition));
    }

    /**
     * Implements the core logic for a successful fetch response.
     *
     * @param fetchTarget {@link Node} from which the fetch data was requested
     * @param data {@link FetchSessionHandler.FetchRequestData} that represents the session data
     * @param resp {@link ClientResponse} from which the {@link FetchResponse} will be retrieved
     */
    @SuppressWarnings("NPathComplexity")
    protected void handleFetchSuccess(final Node fetchTarget,
                                      final FetchSessionHandler.FetchRequestData data,
                                      final ClientResponse resp) {
        try {
            final FetchResponse response = (FetchResponse) resp.responseBody();
            final FetchSessionHandler handler = sessionHandler(fetchTarget.id());
            // True when this runs on the receiver thread of the connection. State that the consumer network
            // thread owns is then not touched here; the change is handed to that thread (CONSUMER-3X D-56).
            final boolean onReceiver = receiverFetches(fetchTarget);
            (onReceiver ? LaneCounters.PARSED_ON_RECEIVER : LaneCounters.PARSED_ON_NETWORK).increment();

            metricsManager.recordLatency(resp.destination(), resp.requestLatencyMs());
            // before the checks below: a throttled answer to a full fetch request is empty and is dropped by them
            if (response.throttleTimeMs() > 0) {
                if (onReceiver)
                    runOnNetworkThread(() -> brokerThrottled(fetchTarget.id()));
                else
                    brokerThrottled(fetchTarget.id());
            }

            if (handler == null) {
                log.error("Unable to find FetchSessionHandler for node {}. Ignoring fetch response.",
                        fetchTarget.id());
                return;
            }

            final short requestVersion = resp.requestHeader().apiVersion();

            if (!handler.handleResponse(response, requestVersion)) {
                if (response.error() == Errors.FETCH_SESSION_TOPIC_ID_ERROR) {
                    metadata.requestUpdate(false);
                }

                return;
            }

            final Map<TopicPartition, FetchResponseData.PartitionData> responseData = response.responseData(handler.sessionTopicNames(), requestVersion);
            // Null unless this consumer's network clients read responses into pooled arrays.
            ReceiveBufferPool receivePool = fetchAhead() ? receiveBufferPool() : null;
            ReceiveBufferPool.Lease receiveLease = receivePool != null ? new ReceiveBufferPool.Lease(receivePool) : null;
            final Set<TopicPartition> partitions = new HashSet<>(responseData.keySet());
            final FetchMetricsAggregator metricAggregator = new FetchMetricsAggregator(metricsManager, partitions);

            Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo = new HashMap<>();
            long responseBytes = 0;
            for (Map.Entry<TopicPartition, FetchResponseData.PartitionData> entry : responseData.entrySet()) {
                TopicPartition partition = entry.getKey();
                FetchRequest.PartitionData requestData = data.sessionPartitions().get(partition);

                if (requestData == null) {
                    String message;

                    if (data.metadata().isFull()) {
                        message = MessageFormatter.arrayFormat(
                                "Response for missing full request partition: partition={}; metadata={}",
                                new Object[]{partition, data.metadata()}).getMessage();
                    } else {
                        message = MessageFormatter.arrayFormat(
                                "Response for missing session request partition: partition={}; metadata={}; toSend={}; toForget={}; toReplace={}",
                                new Object[]{partition, data.metadata(), data.toSend(), data.toForget(), data.toReplace()}).getMessage();
                    }

                    // Received fetch response for missing session partition
                    throw new IllegalStateException(message);
                }

                long fetchOffset = requestData.fetchOffset;
                FetchResponseData.PartitionData partitionData = entry.getValue();

                log.debug("Fetch {} at offset {} for partition {} returned fetch data {}",
                        fetchConfig.isolationLevel, fetchOffset, partition, partitionData);

                Errors partitionError = Errors.forCode(partitionData.errorCode());
                if (partitionError == Errors.NOT_LEADER_OR_FOLLOWER || partitionError == Errors.FENCED_LEADER_EPOCH) {
                    log.debug("For {}, received error {}, with leaderIdAndEpoch {}", partition, partitionError, partitionData.currentLeader());
                    if (partitionData.currentLeader().leaderId() != -1 && partitionData.currentLeader().leaderEpoch() != -1) {
                        partitionsWithUpdatedLeaderInfo.put(partition, new Metadata.LeaderIdAndEpoch(
                            Optional.of(partitionData.currentLeader().leaderId()), Optional.of(partitionData.currentLeader().leaderEpoch())));
                    }
                }

                responseBytes += FetchResponse.recordsSize(partitionData);
                CompletedFetch completedFetch = new CompletedFetch(
                        completedFetchLog,
                        subscriptions,
                        decompressionBufferSupplier,
                        partition,
                        partitionData,
                        metricAggregator,
                        fetchOffset);
                if (receiveLease != null) {
                    if (partitionData.records() instanceof MemoryRecords)
                        completedFetch.receiveLease(receiveLease, ((MemoryRecords) partitionData.records()).buffer());
                    else
                        receiveLease.taint();
                }
                fetchBuffer.add(completedFetch);
            }
            if (receiveLease != null) {
                receiveLease.release();
                receiveLease = null;
            }

            lastResponseBytes.put(fetchTarget.id(), responseBytes);
            if (!onReceiver)   // the number of connections is fixed when the receiver threads fetch
                updateLanes(fetchTarget.id(), responseBytes, responseData.size());

            if (!partitionsWithUpdatedLeaderInfo.isEmpty() && onReceiver) {
                runOnNetworkThread(() -> updateLeadership(response, partitionsWithUpdatedLeaderInfo));
            } else if (!partitionsWithUpdatedLeaderInfo.isEmpty()) {
                updateLeadership(response, partitionsWithUpdatedLeaderInfo);
            }
        } finally {
            removePendingFetchRequest(fetchTarget, data.metadata().sessionId());
        }
    }

    private void updateLeadership(FetchResponse response, Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo) {
        List<Node> leaderNodes = new ArrayList<>();

        for (FetchResponseData.NodeEndpoint e : response.data().nodeEndpoints()) {
            Node node = new Node(e.nodeId(), e.host(), e.port(), e.rack());

            if (!node.equals(Node.noNode()))
                leaderNodes.add(node);
        }

        Set<TopicPartition> updatedPartitions = metadata.updatePartitionLeadership(partitionsWithUpdatedLeaderInfo, leaderNodes);
        updatedPartitions.forEach(
            tp -> {
                log.debug("For {}, as the leader was updated, position will be validated.", tp);
                subscriptions.maybeValidatePositionForCurrentLeader(apiVersions, tp, metadata.currentLeader(tp));
            }
        );
    }

    /**
     * Implements the core logic for a failed fetch response.
     *
     * @param fetchTarget {@link Node} from which the fetch data was requested
     * @param data        {@link FetchSessionHandler.FetchRequestData} from request
     * @param t           {@link Throwable} representing the error that resulted in the failure
     */
    protected void handleFetchFailure(final Node fetchTarget,
                                      final FetchSessionHandler.FetchRequestData data,
                                      final Throwable t) {
        try {
            final FetchSessionHandler handler = sessionHandler(fetchTarget.id());

            if (handler != null) {
                handler.handleError(t);
                handler.sessionTopicPartitions().forEach(subscriptions::clearPreferredReadReplica);
            }
        } finally {
            removePendingFetchRequest(fetchTarget, data.metadata().sessionId());
        }
    }

    protected void handleCloseFetchSessionSuccess(final Node fetchTarget,
                                                  final FetchSessionHandler.FetchRequestData data,
                                                  final ClientResponse ignored) {
        int sessionId = data.metadata().sessionId();
        removePendingFetchRequest(fetchTarget, sessionId);
        log.debug("Successfully sent a close message for fetch session: {} to node: {}", sessionId, fetchTarget);
    }

    public void handleCloseFetchSessionFailure(final Node fetchTarget,
                                               final FetchSessionHandler.FetchRequestData data,
                                               final Throwable t) {
        int sessionId = data.metadata().sessionId();
        removePendingFetchRequest(fetchTarget, sessionId);
        log.debug("Unable to send a close message for fetch session: {} to node: {}. " +
                "This may result in unnecessary fetch sessions at the broker.", sessionId, fetchTarget, t);
    }

    private void removePendingFetchRequest(Node fetchTarget, int sessionId) {
        log.debug("Removing pending request for fetch session: {} for node: {}", sessionId, fetchTarget);
        nodesWithPendingFetchRequests.remove(fetchTarget.id());

        // Wake the buffer whenever a node stops having a request in flight, whatever the outcome was: data, an
        // empty response, a fetch session error, or a failure. This ensures the caller is not left waiting on a
        // wakeup that only a completed request could have delivered.
        fetchBuffer.wakeup();
    }

    /**
     * Creates a new {@link FetchRequest fetch request} in preparation for sending to the Kafka cluster.
     *
     * @param fetchTarget {@link Node} from which the fetch data will be requested
     * @param requestData {@link FetchSessionHandler.FetchRequestData} that represents the session data
     * @return {@link FetchRequest.Builder} that can be submitted to the broker
     */
    protected FetchRequest.Builder createFetchRequest(final Node fetchTarget,
                                                      final FetchSessionHandler.FetchRequestData requestData) {
        // Version 12 is the maximum version that could be used without topic IDs. See FetchRequest.json for schema
        // changelog.
        final short maxVersion = requestData.canUseTopicIds() ? ApiKeys.FETCH.latestVersion() : (short) 12;

        final FetchRequest.Builder request = FetchRequest.Builder
                .forConsumer(maxVersion, fetchConfig.maxWaitMs, fetchConfig.minBytes, requestData.toSend())
                .isolationLevel(fetchConfig.isolationLevel)
                .setMaxBytes(fetchConfig.maxBytes)
                .metadata(requestData.metadata())
                .removed(requestData.toForget())
                .replaced(requestData.toReplace())
                .rackId(fetchConfig.clientRackId);

        log.debug("Sending {} {} to broker {}", fetchConfig.isolationLevel, requestData, fetchTarget);

        // We add the node to the set of nodes with pending fetch requests before adding the
        // listener because the future may have been fulfilled on another thread (e.g. during a
        // disconnection being handled by the heartbeat thread) which will mean the listener
        // will be invoked synchronously.
        log.debug("Adding pending request for node {}", fetchTarget);
        nodesWithPendingFetchRequests.add(fetchTarget.id());

        return request;
    }

    /**
     * Determine from which replica to read: the <i>preferred</i> or the <i>leader</i>. The preferred replica is used
     * iff:
     *
     * <ul>
     *     <li>A preferred replica was previously set</li>
     *     <li>We're still within the lease time for the preferred replica</li>
     *     <li>The replica is still online/available</li>
     * </ul>
     *
     * If any of the above are not met, the leader node is returned.
     *
     * @param partition {@link TopicPartition} for which we want to fetch data
     * @param leaderReplica {@link Node} for the leader of the given partition
     * @param currentTimeMs Current time in milliseconds; used to determine if we're within the optional lease window
     * @return Replica {@link Node node} from which to request the data
     * @see SubscriptionState#preferredReadReplica
     * @see SubscriptionState#updatePreferredReadReplica
     */
    Node selectReadReplica(final TopicPartition partition, final Node leaderReplica, final long currentTimeMs) {
        Optional<Integer> nodeId = subscriptions.preferredReadReplica(partition, currentTimeMs);

        if (nodeId.isPresent()) {
            Optional<Node> node = nodeId.flatMap(id -> metadata.fetch().nodeIfOnline(partition, id));
            if (node.isPresent()) {
                return node.get();
            } else {
                log.trace("Not fetching from {} for partition {} since it is marked offline or is missing from our metadata," +
                        " using the leader instead.", nodeId, partition);
                // Note that this condition may happen due to stale metadata, so we clear preferred replica and
                // refresh metadata.
                requestMetadataUpdate(metadata, subscriptions, partition);
                return leaderReplica;
            }
        } else {
            return leaderReplica;
        }
    }

    protected FetchRequestPreparationResult prepareCloseFetchSessionRequests() {
        final Cluster cluster = metadata.fetch();
        Map<Node, FetchSessionHandler.Builder> fetchable = new HashMap<>();

        closing = true;
        sessionHandlers.forEach((fetchTargetNodeId, sessionHandler) -> {
            Node laneNode = laneNodes.get(fetchTargetNodeId);
            if (laneNode != null && receiverFetches(laneNode)) {
                // the receiver thread owns this fetch session, so it closes the session itself
                runOnLane(laneNode, () -> closeLaneSession(laneNode, sessionHandler));
                return;
            }
            // set the session handler to notify close. This will set the next metadata request to send close message.
            sessionHandler.notifyClose();

            // FetchTargetNode may not be available as it may have disconnected the connection. In such cases, we will
            // skip sending the close request.
            final Node fetchTarget = laneNodes.containsKey(fetchTargetNodeId) ? laneNodes.get(fetchTargetNodeId) : cluster.nodeById(fetchTargetNodeId);

            if (fetchTarget == null || isUnavailable(fetchTarget)) {
                log.debug("Skip sending close session request to broker {} since it is not reachable", fetchTarget);
                return;
            }

            fetchable.put(fetchTarget, sessionHandler.newBuilder());
        });

        // Closing is a one-shot operation, not part of the steady-state polling loop, so always waking the
        // buffer here is safe even if this ends up empty.
        return new FetchRequestPreparationResult(convert(fetchable), true);
    }

    /**
     * Create fetch requests for all nodes for which we have assigned partitions
     * that have no existing requests in flight.
     */
    protected FetchRequestPreparationResult prepareFetchRequests() {
        // Update metrics in case there was an assignment change
        metricsManager.maybeUpdateAssignment(subscriptions);

        Map<Node, FetchSessionHandler.Builder> fetchable = new HashMap<>();
        long currentTimeMs = time.milliseconds();
        Map<String, Uuid> topicIds = metadata.topicIds();

        // Summary of the data that is buffered and not yet consumed, per partition
        Map<TopicPartition, FetchBuffer.BufferedState> buffered = fetchBuffer.bufferedStates();

        // All partitions in a fetchable state. A partition with buffered data is fetched ahead, starting
        // right after its buffered data, so that the network round trip overlaps with record processing.
        List<TopicPartition> fetchablePartitions = subscriptions.fetchablePartitions(tp -> true);

        if (fetchablePartitions.isEmpty()) {
            // No partitions are fetchable at all (for example, no assignment yet, invalid positions, or paused).
            // The state will not change until some external event occurs.
            return new FetchRequestPreparationResult(Map.of(), false);
        }

        Throwable laneFailure = this.laneFailure.getAndSet(null);
        if (laneFailure != null)
            throw laneFailure instanceof RuntimeException ? (RuntimeException) laneFailure : new org.apache.kafka.common.KafkaException(laneFailure);

        Set<Integer> blockedNodes = fetchAheadBlockedNodes(buffered, currentTimeMs);
        for (BrokerLanes state : brokerLanes.values())
            state.partitionsCounting = 0;
        // Only when the receiver threads fetch: the partitions of every such connection, found in this pass
        Map<Node, List<TopicPartition>> receiverLanes = null;

        for (TopicPartition partition : fetchablePartitions) {
            SubscriptionState.FetchPosition position = positionForPartition(partition);
            Optional<Node> nodeOpt = maybeNodeForPosition(partition, position, currentTimeMs, true);

            if (nodeOpt.isEmpty())
                continue;

            Node node = nodeOpt.get();

            if (receiverFetches(node)) {
                if (receiverLanes == null)
                    receiverLanes = new HashMap<>();
                receiverLanes.computeIfAbsent(node, n -> new ArrayList<>()).add(partition);
            } else if (isUnavailable(node)) {
                maybeThrowAuthFailure(node);

                // If we try to send during the reconnect backoff window, then the request is just
                // going to be failed anyway before being sent, so skip sending the request for now
                log.trace("Skipping fetch for partition {} because node {} is awaiting reconnect backoff", partition, node);
            } else if (nodesWithPendingFetchRequests.contains(node.id())) {
                // If there's already an inflight request for this node, don't issue another request.
                log.trace("Skipping fetch for partition {} because previous request to {} has not been processed", partition, node);
            } else if (blockedNodes.contains(node.id()) || blockedBrokers.contains(brokerIdOf(node.id()))) {
                // While a node has buffered data, don't fetch other partition data from it. Because the buffered
                // partitions are not included in the fetch request, those partitions will be inadvertently dropped
                // from the broker fetch session cache. In some cases, that could lead to the entire fetch session
                // being evicted.
                log.trace("Skipping fetch for partition {} because its leader node {} hosts buffered partitions", partition, node);
            } else {
                // if there is a leader and no in-flight requests, issue a new fetch
                FetchSessionHandler.Builder builder = fetchable.computeIfAbsent(node, k -> {
                    FetchSessionHandler fetchSessionHandler = sessionHandlers.computeIfAbsent(node.id(), n -> new FetchSessionHandler(logContext, n));
                    return fetchSessionHandler.newBuilder();
                });
                LaneCounters.PARTITIONS_ON_NETWORK.increment();
                Uuid topicId = topicIds.getOrDefault(partition.topic(), Uuid.ZERO_UUID);
                FetchBuffer.BufferedState bufferedState = buffered.get(partition);
                long fetchOffset = bufferedState != null ? bufferedState.fetchAheadOffset : position.offset;
                if (bufferedState == null)
                    fetchBuffer.clearFetchAheadInvalidated(partition);
                FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(topicId,
                        fetchOffset,
                        FetchRequest.INVALID_LOG_START_OFFSET,
                        fetchConfig.fetchSize,
                        position.currentLeader.epoch,
                        Optional.empty());
                builder.add(partition, partitionData);

                log.debug("Added {} fetch request for partition {} at position {} to node {}", fetchConfig.isolationLevel,
                        partition, position, node);
            }
        }

        for (BrokerLanes state : brokerLanes.values())
            state.partitions = state.partitionsCounting;
        publishReceiverLanes(receiverLanes, buffered);
        if (partitionLanes.size() > 2 * fetchablePartitions.size() + 16)
            partitionLanes.keySet().retainAll(new HashSet<>(fetchablePartitions));

        // If every fetchable-but-unbuffered partition was skipped (for example, due to reconnect backoff,
        // an in-flight request, or its node already hosting buffered partitions), the state will only
        // change over time. An immediate wakeup would therefore just busy-loop the caller instead of
        // respecting its normal backoff. This case is only relevant when fetchable partitions exist but
        // the resulting request map is empty; otherwise the caller ignores this flag.
        // If no request can be sent but data is buffered, progress is made by consuming the buffered data.
        boolean hasConsumableBufferedData = false;
        for (TopicPartition partition : buffered.keySet()) {
            if (subscriptions.isFetchable(partition)) {
                hasConsumableBufferedData = true;
                break;
            }
        }
        return new FetchRequestPreparationResult(convert(fetchable), fetchable.isEmpty() && hasConsumableBufferedData);
    }

    /**
     * The result of preparing fetch requests via {@link #prepareFetchRequests()} or
     * {@link #prepareCloseFetchSessionRequests()}.
     */
    protected static final class FetchRequestPreparationResult {

        private final Map<Node, FetchSessionHandler.FetchRequestData> requests;
        private final boolean canWakeBufferIfNoFetchRequestsToSend;

        FetchRequestPreparationResult(
                Map<Node, FetchSessionHandler.FetchRequestData> requests,
                boolean canWakeBufferIfNoFetchRequestsToSend
        ) {
            this.requests = requests;
            this.canWakeBufferIfNoFetchRequestsToSend = canWakeBufferIfNoFetchRequestsToSend;
        }

        /**
         * {@link Map} of {@link Node nodes} to the {@link FetchSessionHandler.FetchRequestData} to send them,
         * empty if there is nothing to fetch right now.
         */
        Map<Node, FetchSessionHandler.FetchRequestData> requests() {
            return requests;
        }

        /**
         * Whether, if {@link #requests()} is empty, this is a safe point to wake up the {@link FetchBuffer}
         * immediately, as opposed to a state that will only change once some other event happens (for example, a
         * metadata update, reconnect backoff expiration, or an in-flight response arriving). Ignored when
         * {@link #requests()} is non-empty.
         */
        boolean canWakeBufferIfNoFetchRequestsToSend() {
            return canWakeBufferIfNoFetchRequestsToSend;
        }
    }

    /**
     * This method converts {@link FetchSessionHandler.Builder} instances to
     * {@link FetchSessionHandler.FetchRequestData} instances. It intentionally forgoes use of the Java Collections
     * Streams API to reduce overhead in the critical network path.
     */
    private Map<Node, FetchSessionHandler.FetchRequestData> convert(Map<Node, FetchSessionHandler.Builder> fetchable) {
        Map<Node, FetchSessionHandler.FetchRequestData> map = new HashMap<>(fetchable.size());

        for (Map.Entry<Node, FetchSessionHandler.Builder> entry : fetchable.entrySet())
            map.put(entry.getKey(), entry.getValue().build());

        return map;
    }

    /**
     * Simple utility method that returns a {@link SubscriptionState.FetchPosition position} for the partition. If
     * no position exists, an {@link IllegalStateException} is thrown.
     */
    private SubscriptionState.FetchPosition positionForPartition(TopicPartition partition) {
        SubscriptionState.FetchPosition position = subscriptions.position(partition);

        if (position == null)
            throw new IllegalStateException("Missing position for fetchable partition " + partition);

        return position;
    }

    /**
     * Retrieves the node from which to fetch the partition data. If the given
     * {@link SubscriptionState.FetchPosition position} does not have a current
     * {@link Metadata.LeaderAndEpoch#leader leader} defined the method will return {@link Optional#empty()}.
     *
     * @return Three options: 1) {@link Optional#empty()} if the position's leader is empty, 2) the
     * {@link #selectReadReplica(TopicPartition, Node, long) read replica, if defined}, or 3) the position's
     * {@link Metadata.LeaderAndEpoch#leader leader}
     */
    private Optional<Node> maybeNodeForPosition(TopicPartition partition,
                                                SubscriptionState.FetchPosition position,
                                                long currentTimeMs,
                                                boolean mayMove) {
        Optional<Node> leaderOpt = position.currentLeader.leader;

        if (leaderOpt.isEmpty()) {
            log.debug("Requesting metadata update for partition {} since the position {} is missing the current leader node", partition, position);
            metadata.requestUpdate(false);
            return Optional.empty();
        }

        // Use the preferred read replica if set, otherwise the partition's leader
        Node node = selectReadReplica(partition, leaderOpt.get(), currentTimeMs);
        return Optional.of(laneNode(node, partition, mayMove));
    }

    // Experiment switch for CONSUMER-3X-07: a fixed number of connections per broker; 0 lets the consumer decide.
    private static final int FIXED_FETCH_LANES = Integer.getInteger("c3x.fetchLanes", 0);
    // The most connections to one broker. Measured on a 16-processor machine (CONSUMER-3X D-26): 8 connections
    // gave at most 13% more throughput than 4 for about 60% more CPU, and less throughput with small records.
    // Experiment switch: c3x.maxFetchLanes.
    static final int MAX_FETCH_LANES = Math.max(1, Integer.getInteger("c3x.maxFetchLanes", Math.min(4, Runtime.getRuntime().availableProcessors() / 2)));
    private static final int LANE_FLIP = Integer.getInteger("c3x.laneFlip", 0);
    // Experiment switch for CONSUMER-3X D-55: no partition is fetched over the broker's first connection, which
    // the consumer network thread reads. Every fetch connection then has a receiver thread of its own.
    private static final boolean NO_LANE_ZERO = Boolean.getBoolean("c3x.noLaneZero");
    private static final int LANE_ID_STRIDE = 10_000_000;
    private static final int ADD_LANE_SCORE = 8;
    private static final int REMOVE_LANE_SCORE = -64;

    /** How many connections ("lanes") are used to fetch from one broker, and the evidence for changing that. */
    private static final class BrokerLanes {
        int lanes = FIXED_FETCH_LANES > 0 ? FIXED_FETCH_LANES : 1;
        int score;              // goes up while the connections are the limit, down while they are not
        boolean bufferFull;     // a lane had to wait for the application since the last vote
        long windowStartNanos;  // start of the time over which the application's waiting is being measured
        long windowStartWaited; // FetchBuffer.waitedNanos() at that time
        int changes;            // how often the number of lanes changed
        int moves;              // how often a partition changed lane
        int partitions;         // partitions fetched from this broker, counted in the last pass
        int partitionsCounting;
        boolean throttled;      // a fetch response of this broker had a throttle time at least once
        long throttledNanos;    // when it last did
    }

    private final Map<Integer, BrokerLanes> brokerLanes = new HashMap<>();
    private final Map<Integer, Node> laneNodes = new HashMap<>();
    private final Map<TopicPartition, Integer> partitionLanes = new HashMap<>();

    /** Whether the node id stands for an extra connection to a broker, not for the broker's first connection. */
    static boolean isExtraLane(int nodeId) {
        return nodeId < -1;
    }

    /** The lane of a node id: 0 for the broker's own id, 1 and up for the extra connections. */
    static int laneOf(int nodeId) {
        return nodeId >= 0 ? 0 : (-nodeId - 2) / LANE_ID_STRIDE;
    }

    static int brokerIdOf(int nodeId) {
        return nodeId >= 0 ? nodeId : (-nodeId - 2) % LANE_ID_STRIDE;
    }

    /**
     * The partitions of one broker are spread over several connections, so that their fetch round trips run
     * at the same time. Every extra connection is a {@link Node} with its own id and the address of the broker,
     * the same way the classic consumer keeps a separate connection to its group coordinator. A lane has its own
     * fetch session, in-flight flag and fetch-ahead budget, because all of them are keyed by the node id.
     *
     * <p>A partition changes lane only when {@code mayMove} is set and its current lane has no request in
     * flight, so the same partition is never requested on two lanes at once.
     */
    private Node laneNode(Node node, TopicPartition partition, boolean mayMove) {
        if (!fetchAhead() || node.id() < 0 || node.id() >= LANE_ID_STRIDE)
            return node;
        BrokerLanes state = brokerLanes.computeIfAbsent(node.id(), id -> new BrokerLanes());
        if (mayMove)
            state.partitionsCounting++;
        if (!NO_LANE_ZERO && state.lanes <= 1 && partitionLanes.isEmpty())
            return node;
        int wanted = (NO_LANE_ZERO ? 1 : 0) + Math.floorMod(partition.partition(), state.lanes);
        // a partition that was never moved is on lane 0, the connection every broker starts with
        int current = partitionLanes.getOrDefault(partition, 0);
        int lane = current;
        if (current != wanted && mayMove && !nodesWithPendingFetchRequests.contains(laneId(node.id(), current))) {
            partitionLanes.put(partition, wanted);
            state.moves++;
            lane = wanted;
        }
        if (lane == 0)
            return node;
        int id = laneId(node.id(), lane);
        Node laneNode = laneNodes.get(id);
        if (laneNode == null || !laneNode.host().equals(node.host()) || laneNode.port() != node.port()) {
            laneNode = new Node(id, node.host(), node.port(), node.rack());
            laneNodes.put(id, laneNode);
        }
        return laneNode;
    }

    private static int laneId(int brokerId, int lane) {
        return lane == 0 ? brokerId : -(lane * LANE_ID_STRIDE + brokerId + 2);
    }

    // Experiment switch for CONSUMER-3X D-43: -Dc3x.laneRule=old keeps the rule that does not look at the application.
    private static final boolean WAIT_RULE = !"old".equals(System.getProperty("c3x.laneRule"));
    private static final long LANE_VOTE_WINDOW_NANOS = 10_000_000L;
    static final double ADD_LANE_WAIT_SHARE = 0.25;

    /**
     * One vote on the number of connections to a broker: +1 for one more, -1 for one less, 0 for no opinion.
     *
     * <p>One more connection helps only when the connections are the limit. The sign of that is an application
     * thread that waits for data although the responses come back full. An application thread that hardly ever
     * waits is itself the limit: one more connection would only buffer more data. One less is voted for only
     * when the responses are not full, that is when the broker has less data than the connections can carry.
     *
     * @param full          the response was at least half of what the broker may send in one response
     * @param laneBlocked   a connection had to stop fetching ahead because the application had not consumed
     * @param waitShare     the share of the time the application thread spent waiting for data, 0 to 1
     */
    static int laneVote(boolean full, boolean laneBlocked, double waitShare) {
        if (!full)
            return -1;
        if (laneBlocked || waitShare < ADD_LANE_WAIT_SHARE)
            return 0;
        return 1;
    }

    // A connection needs at least this many partitions. 2 was tried and taken back (CONSUMER-3X D-47, D-48):
    // with the broker on the same machine one partition per connection was slower, across a real network
    // (4 partitions, 10 KiB records, 1, 2 and 4 connections: 1590, 1920 and 2290 MB/s) it was faster.
    static final int MIN_PARTITIONS_PER_LANE = 1;

    /** The most connections worth having to a broker that this consumer fetches {@code partitions} from. */
    static int maxLanes(int partitions) {
        return Math.max(1, Math.min(MAX_FETCH_LANES, partitions / MIN_PARTITIONS_PER_LANE));
    }

    // A broker that throttled a fetch of this consumer is holding it to a quota, and more connections do not
    // raise a quota. Measured with a 200 MB/s quota (CONSUMER-3X D-49): without this the consumer went to the
    // most connections it may have, for the same throughput. The broker measures a quota over about 11 seconds
    // by default, so a consumer at its quota is throttled again well inside this time.
    static final long THROTTLE_MEMORY_NANOS = 30_000_000_000L;

    /** Whether a broker that last throttled this consumer {@code nanosSinceThrottle} ago still counts as limiting it. */
    static boolean brokerLimits(boolean throttled, long nanosSinceThrottle) {
        return throttled && nanosSinceThrottle < THROTTLE_MEMORY_NANOS;
    }

    /** Remembers that the broker behind this connection answered a fetch with a throttle time. */
    private void brokerThrottled(int nodeId) {
        BrokerLanes state = brokerLanes.get(brokerIdOf(nodeId));
        if (state != null) {
            state.throttled = true;
            state.throttledNanos = System.nanoTime();
        }
    }

    /**
     * Decides whether the broker needs one more or one less connection. Called after every fetch response
     * that the fetch session accepts; it votes at most once per {@link #LANE_VOTE_WINDOW_NANOS} (see {@link #laneVote}).
     */
    private void updateLanes(int nodeId, long responseBytes, int responsePartitions) {
        BrokerLanes state = brokerLanes.get(brokerIdOf(nodeId));
        if (state == null || FIXED_FETCH_LANES > 0 || !fetchAhead())
            return;
        if (LANE_FLIP > 0) {   // experiment: change the number of lanes every LANE_FLIP responses
            if (++state.score >= LANE_FLIP) {
                state.score = 0;
                state.lanes = state.lanes >= Math.min(MAX_FETCH_LANES, Math.max(1, state.partitions)) ? 1 : state.lanes + 1;
            }
            return;
        }
        long fullBytes = Math.min(fetchConfig.maxBytes, (long) responsePartitions * fetchConfig.fetchSize) / 2;
        boolean full = responsePartitions > 0 && responseBytes >= fullBytes;
        if (WAIT_RULE) {
            // The share of the time since the last vote that the application thread spent waiting for data.
            long nowNanos = System.nanoTime();
            long waited = fetchBuffer.waitedNanos();
            long elapsed = nowNanos - state.windowStartNanos;
            if (state.windowStartNanos == 0) {
                state.windowStartNanos = nowNanos;
                state.windowStartWaited = waited;
                return;
            }
            if (elapsed < LANE_VOTE_WINDOW_NANOS)
                return;
            double waitShare = (double) (waited - state.windowStartWaited) / elapsed;
            state.windowStartNanos = nowNanos;
            state.windowStartWaited = waited;
            state.score += laneVote(full, state.bufferFull, waitShare);
        } else if (full && !state.bufferFull) {
            state.score++;
        } else {
            state.score--;
        }
        state.bufferFull = false;
        int maxLanes = brokerLimits(state.throttled, System.nanoTime() - state.throttledNanos) ? 1 : maxLanes(state.partitions);
        if (state.score >= ADD_LANE_SCORE) {
            state.score = 0;
            if (state.lanes < maxLanes) {
                state.changes++;
                state.lanes++;
                log.debug("Fetching from broker {} over {} connections", brokerIdOf(nodeId), state.lanes);
            }
        } else if (state.score <= REMOVE_LANE_SCORE || state.lanes > maxLanes) {
            state.score = 0;
            if (state.lanes > 1) {
                state.changes++;
                state.lanes--;
                log.debug("Fetching from broker {} over {} connections", brokerIdOf(nodeId), state.lanes);
            }
        }
    }

    /**
     * Returns the set of IDs for {@link Node}s to which fetch requests should <em>not</em> be sent.
     *
     * <p>
     * When a partition has buffered data in {@link FetchBuffer}, that means that at some point in the <em>past</em>,
     * the following steps occurred:
     *
     * <ol>
     *     <li>The client submitted a fetch request to the partition's leader</li>
     *     <li>The leader responded with data</li>
     *     <li>The client received a response from the leader and stored that data in memory</li>
     * </ol>
     *
     * But it's possible that at the <em>current</em> point in time, that same partition might not be in a fetchable
     * state. For example:
     *
     * <ul>
     *     <li>
     *         The partition is no longer assigned to the client. This also includes when the partition assignment
     *         is either {@link SubscriptionState#markPendingRevocation(Set) pending revocation} or
     *         {@link SubscriptionState#markPendingOnAssignedCallback(Collection, boolean) pending assignment}.
     *     </li>
     *     <li>
     *         The client {@link Consumer#pause(Collection) paused} the partition. A paused partition remains in
     *         the fetch buffer, because {@link FetchCollector#collectFetch(FetchBuffer)} explicitly skips over
     *         paused partitions and does not return them to the user.
     *     </li>
     *     <li>
     *         The partition does not have a valid position on the client. This could be due to the partition
     *         awaiting validation or awaiting reset.
     *     </li>
     * </ul>
     *
     * For those reasons, a partition that was <em>previously</em> in a fetchable state might not <em>currently</em>
     * be in a fetchable state.
     * </p>
     *
     * <p>
     * Here's why this is important-in a production system, a given leader node serves as a leader for many partitions.
     * From the client's perspective, it's possible that a node has a mix of both fetchable and unfetchable partitions.
     * When the client determines which nodes to skip and which to fetch from, it's important that unfetchable
     * partitions don't block fetchable partitions from being fetched.
     * </p>
     *
     * <p>
     * So, when it's determined that a buffered partition is not in a fetchable state, it should be skipped.
     * Otherwise, its node would end up in the set of nodes with buffered data and no fetch would be requested.
     * </p>
     *
     * @param partitions Buffered partitions
     * @param currentTimeMs Current timestamp
     *
     * @return Set of zero or more IDs for leader nodes of buffered partitions
     */
    /**
     * How many buffered bytes a node may have while one more fetch is sent to it. The budget is the size of the
     * most recent response from the node: a fetch is sent ahead only while no more than that one response is
     * waiting, so at most two responses per node are held in memory.
     */
    private long fetchAheadBudget(int nodeId) {
        if (FETCH_AHEAD_DEEP)
            return fetchConfig.maxBytes;
        return lastResponseBytes.getOrDefault(nodeId, 0L);
    }

    // Experiment switch for CONSUMER-3X D-42: -Dc3x.fetchAheadTotalKB=N stops fetching ahead on every connection
    // to a broker while more than N KB from that broker are buffered and not yet consumed. 0 = no such bound.
    private static final long FETCH_AHEAD_TOTAL_BYTES = Long.getLong("c3x.fetchAheadTotalKB", 0L) * 1024;
    private final Set<Integer> blockedBrokers = new HashSet<>();

    // Diagnostic for CONSUMER-3X D-41: -Dc3x.bufferStats=true prints, at JVM exit, how much data was buffered and
    // not yet consumed each time the next fetches were prepared. To be removed with the other c3x switches.
    private static final boolean BUFFER_STATS = Boolean.getBoolean("c3x.bufferStats");

    private static final class BufferStats {
        private static long samples, bytes, maxBytes, fetches, partitions, inFlight;
        static volatile String LANES_AT_EXIT = "";

        static {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                synchronized (BufferStats.class) {
                    long n = Math.max(1, samples);
                    System.err.printf("LANESTATS %s%n", LANES_AT_EXIT);
                    System.err.printf("BUFFERSTATS samples=%d avgBufferedMB=%.2f maxBufferedMB=%.2f avgFetches=%.2f avgPartitions=%.2f avgInFlight=%.2f%n",
                            samples, bytes / 1048576.0 / n, maxBytes / 1048576.0, (double) fetches / n, (double) partitions / n, (double) inFlight / n);
                }
            }));
        }

        static synchronized void sample(Map<TopicPartition, FetchBuffer.BufferedState> buffered, int requestsInFlight) {
            long total = 0;
            for (FetchBuffer.BufferedState state : buffered.values()) {
                total += state.bytes;
                fetches += state.fetches;
            }
            samples++;
            bytes += total;
            maxBytes = Math.max(maxBytes, total);
            partitions += buffered.size();
            inFlight += requestsInFlight;
        }
    }

    // Experiment switch for CONSUMER-3X-04; to be removed once the depth is decided by measurement.
    private static final boolean FETCH_AHEAD_DEEP = Boolean.getBoolean("c3x.fetchAheadDeep");
    private final Map<Integer, Long> lastResponseBytes = new java.util.concurrent.ConcurrentHashMap<>();

    /** The pool that this consumer's network clients read fetch responses into, or null if there is none. */
    protected ReceiveBufferPool receiveBufferPool() {
        return null;
    }

    /**
     * Whether this fetcher sends the next fetch while earlier data is still buffered, and opens more than one
     * connection to a broker. The classic consumer keeps the old rule: no fetch to a node with buffered data,
     * and one connection per broker.
     */
    protected boolean fetchAhead() {
        return true;
    }

    private Set<Integer> fetchAheadBlockedNodes(Map<TopicPartition, FetchBuffer.BufferedState> buffered, long currentTimeMs) {
        Set<Integer> ids = new HashSet<>();
        Map<Integer, Long> bufferedBytes = new HashMap<>();
        if (BUFFER_STATS) {
            BufferStats.sample(buffered, nodesWithPendingFetchRequests.size());
            StringBuilder lanes = new StringBuilder();
            brokerLanes.forEach((broker, state) -> lanes.append("broker").append(broker).append(" lanes=").append(state.lanes)
                    .append(" changes=").append(state.changes).append(" moves=").append(state.moves).append(' '));
            BufferStats.LANES_AT_EXIT = lanes.append(LaneCounters.summary()).toString();
        }
        Map<Integer, Long> brokerBytes = new HashMap<>();
        blockedBrokers.clear();

        for (Map.Entry<TopicPartition, FetchBuffer.BufferedState> entry : buffered.entrySet()) {
            TopicPartition partition = entry.getKey();
            if (!subscriptions.isFetchable(partition))
                continue;

            SubscriptionState.FetchPosition position = positionForPartition(partition);
            Optional<Node> nodeOpt = maybeNodeForPosition(partition, position, currentTimeMs, false);
            if (nodeOpt.isEmpty())
                continue;

            // Every request must cover all partitions of the fetch session (KAFKA-17182), so one partition
            // that cannot be fetched ahead blocks its node until its buffered data has been consumed.
            int nodeId = nodeOpt.get().id();
            if (FETCH_AHEAD_TOTAL_BYTES > 0
                    && brokerBytes.merge(brokerIdOf(nodeId), entry.getValue().bytes, Long::sum) > FETCH_AHEAD_TOTAL_BYTES)
                blockedBrokers.add(brokerIdOf(nodeId));
            if (!fetchAhead() || !entry.getValue().eligible) {
                ids.add(nodeId);
            } else if (bufferedBytes.merge(nodeId, entry.getValue().bytes, Long::sum) > fetchAheadBudget(nodeId)) {
                ids.add(nodeId);
                BrokerLanes state = brokerLanes.get(brokerIdOf(nodeId));
                if (state != null)
                    state.bufferFull = true;
            }
        }

        return ids;
    }

    // ---- CONSUMER-3X D-57: the receiver thread of a connection builds its fetch requests and handles its
    // ---- responses. The consumer network thread says which partitions the connection has. A connection that
    // ---- went idle is asked to look again by the application thread (FetchLaneGate) or by the consumer
    // ---- network thread (publishReceiverLanes); see laneFetch.

    // Experiment switch. Needs a fixed number of connections and no partition on the broker's first connection
    // (-Dc3x.fetchLanes=N -Dc3x.noLaneZero=true): the rule that moves a partition to another connection relies
    // on the consumer network thread knowing whether a request is in flight.
    private static final boolean RECEIVER_FETCH = Boolean.getBoolean("c3x.receiverFetch") && NO_LANE_ZERO && FIXED_FETCH_LANES > 0;

    /** Proof of which thread did the work; printed with LANESTATS. */
    static final class LaneCounters {
        static final java.util.concurrent.atomic.LongAdder BUILT_ON_RECEIVER = new java.util.concurrent.atomic.LongAdder();
        static final java.util.concurrent.atomic.LongAdder PARSED_ON_RECEIVER = new java.util.concurrent.atomic.LongAdder();
        static final java.util.concurrent.atomic.LongAdder PARSED_ON_NETWORK = new java.util.concurrent.atomic.LongAdder();
        static final java.util.concurrent.atomic.LongAdder PARTITIONS_ON_NETWORK = new java.util.concurrent.atomic.LongAdder();
        static final java.util.concurrent.atomic.LongAdder KICKS = new java.util.concurrent.atomic.LongAdder();
        static final java.util.concurrent.atomic.LongAdder DECODED_ON_RECEIVER = new java.util.concurrent.atomic.LongAdder();
        static final java.util.concurrent.atomic.LongAdder DECODED_ON_POOL = new java.util.concurrent.atomic.LongAdder();
        // D-63: events of the application thread; each one wakes the consumer network thread.
        static final java.util.concurrent.atomic.LongAdder POLLS_WITH_DATA = new java.util.concurrent.atomic.LongAdder();
        static final java.util.concurrent.atomic.LongAdder POLL_EVENTS = new java.util.concurrent.atomic.LongAdder();
        // D-65. responses: fetch responses handled on a receiver thread. laneStops: a connection had nothing
        // to send. waitsForRoom: of those, stops because too much is buffered. gateRestarts: starts through
        // the gate (by the application thread, or by the receiver thread's own second look);
        // gateRestartsSent: those that sent a request. kicksLate: a start from the consumer network thread that found a request in
        // flight. kicksOfWaiting: a start from the consumer network thread that sent, for a connection that
        // waited for room while the application thread had no signal pending. Expected after a pause or a
        // changed assignment; in plain consumption it would show a start that the application thread missed
        // (it also counts the short time the application thread is inside signalRoom).
        static final java.util.concurrent.atomic.LongAdder LANE_STOPS = new java.util.concurrent.atomic.LongAdder();
        static final java.util.concurrent.atomic.LongAdder RESPONSES = new java.util.concurrent.atomic.LongAdder();
        static final java.util.concurrent.atomic.LongAdder KICKS_LATE = new java.util.concurrent.atomic.LongAdder();
        static final java.util.concurrent.atomic.LongAdder KICKS_OF_WAITING = new java.util.concurrent.atomic.LongAdder();
        static final java.util.concurrent.atomic.LongAdder WAITS_FOR_ROOM = new java.util.concurrent.atomic.LongAdder();
        static final java.util.concurrent.atomic.LongAdder GATE_RESTARTS = new java.util.concurrent.atomic.LongAdder();
        static final java.util.concurrent.atomic.LongAdder GATE_RESTARTS_SENT = new java.util.concurrent.atomic.LongAdder();

        static String summary() {
            return "builtOnReceiver=" + BUILT_ON_RECEIVER.sum() + " parsedOnReceiver=" + PARSED_ON_RECEIVER.sum()
                    + " parsedOnNetwork=" + PARSED_ON_NETWORK.sum() + " partitionsBuiltOnNetwork=" + PARTITIONS_ON_NETWORK.sum()
                    + " kicks=" + KICKS.sum() + " decodeToReceiver=" + DECODED_ON_RECEIVER.sum() + " decodeToPool=" + DECODED_ON_POOL.sum()
                    + " pollsWithData=" + POLLS_WITH_DATA.sum() + " pollEvents=" + POLL_EVENTS.sum()
                    + " laneStops=" + LANE_STOPS.sum() + " responses=" + RESPONSES.sum() + " kicksLate=" + KICKS_LATE.sum()
                    + " kicksOfWaiting=" + KICKS_OF_WAITING.sum() + " waitsForRoom=" + WAITS_FOR_ROOM.sum()
                    + " gateRestarts=" + GATE_RESTARTS.sum() + " gateRestartsSent=" + GATE_RESTARTS_SENT.sum();
        }
    }

    // Written by the consumer network thread, read by the receiver threads. The lists are never changed in place.
    private final Map<Integer, List<TopicPartition>> receiverLanePartitions = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicReference<Throwable> laneFailure = new java.util.concurrent.atomic.AtomicReference<>();
    private volatile boolean closing;
    private final Set<Integer> kicked = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** Whether the receiver thread of this connection builds and handles its fetches. */
    private boolean receiverFetches(Node node) {
        return RECEIVER_FETCH && isExtraLane(node.id()) && hasLaneThreads();
    }

    /** Whether extra connections have threads of their own. */
    protected boolean hasLaneThreads() {
        return false;
    }

    /** Throws if the receiver thread of the connection has stopped: the application must not wait for data that never comes. */
    protected void checkLane(Node laneNode) {
    }

    /** Runs the task on the receiver thread of the connection. */
    protected void runOnLane(Node laneNode, Runnable task) {
        throw new UnsupportedOperationException();
    }

    /** Sends the request over the connection. Called on its receiver thread; the handler runs on that thread. */
    protected void sendOnLane(Node laneNode, FetchRequest.Builder request, java.util.function.BiConsumer<ClientResponse, Throwable> handler) {
        throw new UnsupportedOperationException();
    }

    /** Runs the task on the consumer network thread. Safe to call from any thread. */
    protected void runOnNetworkThread(Runnable task) {
        task.run();
    }

    /**
     * Called on the consumer network thread. Tells every connection which partitions it has, and starts the
     * connections that have no request in flight and room to fetch ahead.
     */
    private void publishReceiverLanes(Map<Node, List<TopicPartition>> receiverLanes, Map<TopicPartition, FetchBuffer.BufferedState> buffered) {
        if (!RECEIVER_FETCH)
            return;
        if (receiverLanes == null)
            receiverLanes = Map.of();
        Set<Integer> current = new HashSet<>();
        for (Map.Entry<Node, List<TopicPartition>> entry : receiverLanes.entrySet()) {
            Node laneNode = entry.getKey();
            current.add(laneNode.id());
            checkLane(laneNode);
            if (!entry.getValue().equals(receiverLanePartitions.get(laneNode.id())))
                receiverLanePartitions.put(laneNode.id(), List.copyOf(entry.getValue()));
            // The same rule as on the receiver thread, on a snapshot from a moment ago; the receiver thread
            // decides again. A connection that should wait for room but is not registered at the gate (after an
            // error, for example) is started too, so that it registers. (blockedBrokers is the experiment
            // switch c3x.fetchAheadTotalKB, which only this thread knows.)
            if (!nodesWithPendingFetchRequests.contains(laneNode.id())
                    && !blockedBrokers.contains(brokerIdOf(laneNode.id()))
                    && mayStart(laneNode.id(), lookAtLane(laneNode.id(), buffered).state)
                    && kicked.add(laneNode.id())) {   // one start at a time: the receiver thread may be busy decoding
                LaneCounters.KICKS.increment();
                runOnLane(laneNode, () -> {
                    kicked.remove(laneNode.id());
                    if (nodesWithPendingFetchRequests.contains(laneNode.id()))
                        LaneCounters.KICKS_LATE.increment();
                    // Diagnostic: the connection waited for room, the application thread has nothing left
                    // to signal, and still this start is the one that sends.
                    boolean waitedQuietly = fetchBuffer.laneGate().isWaiting(laneNode.id()) && !fetchBuffer.roomSignalPending();
                    if (laneFetch(laneNode) && waitedQuietly)
                        LaneCounters.KICKS_OF_WAITING.increment();
                });
            }
        }
        for (Integer id : receiverLanePartitions.keySet()) {
            if (!current.contains(id))
                receiverLanePartitions.put(id, List.of());
        }
    }

    private boolean mayStart(int laneId, LaneState state) {
        return state == LaneState.SEND
                || (state == LaneState.WAIT_FOR_ROOM && DIRECT_RESTART && !fetchBuffer.laneGate().isWaiting(laneId));
    }

    /**
     * Called on the receiver thread of the connection. Sends the connection's next fetch request, unless one
     * is in flight or the rule in {@link #lookAtLane} says there is nothing to send. Returns whether it sent.
     *
     * <p>Who asks the connection to look again after it sent nothing:
     * <ul>
     *   <li>It waits for room: the application thread, through {@link FetchLaneGate}, as soon as it has
     *       consumed buffered data.</li>
     *   <li>Every other change (pause, resume, seek, assignment, leader, error): the consumer network thread,
     *       from {@link #publishReceiverLanes}, which looks at every connection on each poll event. It also
     *       asks a connection that should wait for room but is not registered at the gate.</li>
     *   <li>A change that came while this method was deciding: this method itself, by its second look.</li>
     * </ul>
     * All of them only ask. The decision is always made here, on the receiver thread.
     * This relies on a partition never moving between two connections that both fetch on receiver threads
     * (see RECEIVER_FETCH): a look that uses a partition list from a moment ago is then harmless.
     */
    private boolean laneFetch(Node laneNode) {
        final int id = laneNode.id();
        if (closing || !nodesWithPendingFetchRequests.add(id))
            return false;
        // this is a new decision; a wait from an earlier one is over
        fetchBuffer.laneGate().forget(id);
        boolean sent = false;
        try {
            LaneView view = lookAtLane(id, fetchBuffer.bufferedStates());
            if (view.state == LaneState.SEND) {
                FetchSessionHandler.FetchRequestData data = buildLaneRequest(laneNode, view);
                FetchRequest.Builder request = createFetchRequest(laneNode, data);
                sendOnLane(laneNode, request, (response, error) -> {
                    boolean again = false;
                    LaneCounters.RESPONSES.increment();
                    try {
                        if (error != null) {
                            // No new request here: a connection that fails would be tried again without a pause.
                            handleFetchFailure(laneNode, data, error);
                        } else {
                            handleFetchSuccess(laneNode, data, response);
                            // After an error for the whole response (a fetch session error) the consumer
                            // network thread starts the connection again; the cause may need a metadata update.
                            again = ((FetchResponse) response.responseBody()).error() == Errors.NONE;
                        }
                    } catch (Throwable t) {
                        laneFailure.compareAndSet(null, t);
                    }
                    if (again)
                        laneFetch(laneNode);
                });
                sent = true;
                LaneCounters.BUILT_ON_RECEIVER.increment();
            } else {
                // While the id was in the set, the consumer network thread saw "request in flight" and did
                // not start this connection. So take the id out first and then look once more: a change that
                // came during the decision above is then seen either by that thread or here.
                nodesWithPendingFetchRequests.remove(id);
                LaneCounters.LANE_STOPS.increment();
                if (view.state == LaneState.WAIT_FOR_ROOM && DIRECT_RESTART) {
                    LaneCounters.WAITS_FOR_ROOM.increment();
                    fetchBuffer.laneGate().waitForRoom(id, laneWaiter(laneNode));   // looks once more itself
                } else if (view.state == LaneState.NOTHING_TO_FETCH
                        && lookAtLane(id, fetchBuffer.bufferedStates()).state != LaneState.NOTHING_TO_FETCH) {
                    laneExecutor(laneNode).execute(() -> laneFetch(laneNode));
                }
            }
        } catch (Throwable t) {
            laneFailure.compareAndSet(null, t);
        } finally {
            if (!sent)
                nodesWithPendingFetchRequests.remove(id);   // for the exception path; otherwise already removed
        }
        if (!sent)
            fetchBuffer.wakeup();
        return sent;
    }

    // -Dc3x.directRestart=false: a connection that waits for room is started by the consumer network thread
    // only, as before CONSUMER-3X D-64. For comparison runs.
    private static final boolean DIRECT_RESTART = !"false".equals(System.getProperty("c3x.directRestart"));

    /**
     * What the gate needs to start this connection: the rule, and a task for the connection's own thread.
     * Made for each wait, from the node the connection has now (the address of a broker can change).
     */
    private FetchLaneGate.Waiter laneWaiter(Node laneNode) {
        final java.util.concurrent.Executor lane = laneExecutor(laneNode);
        return new FetchLaneGate.Waiter() {
            // Both run on the application thread too, inside poll. They must not throw there: records that
            // poll has already collected would be lost. A failure is reported like any failure of the connection.
            @Override
            public boolean hasRoom() {
                try {
                    return lookAtLane(laneNode.id(), fetchBuffer.bufferedStates()).state != LaneState.WAIT_FOR_ROOM;
                } catch (Throwable t) {
                    laneFailure.compareAndSet(null, t);
                    return false;
                }
            }

            @Override
            public void lookAgain() {
                try {
                    LaneCounters.GATE_RESTARTS.increment();
                    lane.execute(() -> {
                        if (laneFetch(laneNode))
                            LaneCounters.GATE_RESTARTS_SENT.increment();
                    });
                } catch (Throwable t) {
                    laneFailure.compareAndSet(null, t);
                }
            }
        };
    }

    /**
     * The executor of the connection's receiver thread. Called on that thread, so that the application
     * thread, which later uses the executor, never looks up the transport itself.
     */
    protected java.util.concurrent.Executor laneExecutor(Node laneNode) {
        throw new UnsupportedOperationException();
    }

    private enum LaneState { SEND, WAIT_FOR_ROOM, NOTHING_TO_FETCH }

    /** What {@link #lookAtLane} saw. Not kept: the next look may see something else. */
    private static final class LaneView {
        final LaneState state;
        final List<TopicPartition> partitions;
        final SubscriptionState.FetchPosition[] positions;   // null: not fetchable now
        final Map<TopicPartition, FetchBuffer.BufferedState> buffered;

        LaneView(LaneState state, List<TopicPartition> partitions, SubscriptionState.FetchPosition[] positions,
                 Map<TopicPartition, FetchBuffer.BufferedState> buffered) {
            this.state = state;
            this.partitions = partitions;
            this.positions = positions;
            this.buffered = buffered;
        }
    }

    /**
     * The one rule for whether a connection may send its next request, given what is buffered. No side
     * effects; safe on any thread.
     * The rule is that of {@link #prepareFetchRequests()} and {@link #fetchAheadBlockedNodes}: only partitions
     * that can be fetched now count; the connection waits for room while more than its budget is buffered or
     * while one of its partitions may not be fetched ahead (every request must cover the whole fetch session).
     */
    private LaneView lookAtLane(int laneId, Map<TopicPartition, FetchBuffer.BufferedState> buffered) {
        List<TopicPartition> partitions = receiverLanePartitions.getOrDefault(laneId, List.of());
        if (partitions.isEmpty())
            return new LaneView(LaneState.NOTHING_TO_FETCH, partitions, null, null);
        long budget = fetchAheadBudget(laneId);
        long bufferedBytes = 0;
        SubscriptionState.FetchPosition[] positions = new SubscriptionState.FetchPosition[partitions.size()];
        boolean any = false;
        for (int i = 0; i < positions.length; i++) {
            // null: paused, revoked or without a valid position since the consumer network thread last looked
            positions[i] = subscriptions.fetchablePositionOrNull(partitions.get(i));
            if (positions[i] == null)
                continue;
            any = true;
            FetchBuffer.BufferedState bufferedState = buffered.get(partitions.get(i));
            if (bufferedState != null) {
                bufferedBytes += bufferedState.bytes;
                if (!bufferedState.eligible || bufferedBytes > budget)
                    return new LaneView(LaneState.WAIT_FOR_ROOM, partitions, positions, buffered);
            }
        }
        return new LaneView(any ? LaneState.SEND : LaneState.NOTHING_TO_FETCH, partitions, positions, buffered);
    }

    /** Called on the receiver thread of the connection, for a view whose state is SEND. */
    private FetchSessionHandler.FetchRequestData buildLaneRequest(Node laneNode, LaneView view) {
        Map<String, Uuid> topicIds = metadata.topicIds();
        FetchSessionHandler.Builder builder = sessionHandlers.computeIfAbsent(laneNode.id(), n -> new FetchSessionHandler(logContext, n)).newBuilder();
        for (int i = 0; i < view.positions.length; i++) {
            if (view.positions[i] == null)
                continue;
            TopicPartition partition = view.partitions.get(i);
            FetchBuffer.BufferedState bufferedState = view.buffered.get(partition);
            if (bufferedState == null)
                fetchBuffer.clearFetchAheadInvalidated(partition);
            builder.add(partition, new FetchRequest.PartitionData(topicIds.getOrDefault(partition.topic(), Uuid.ZERO_UUID),
                    bufferedState != null ? bufferedState.fetchAheadOffset : view.positions[i].offset,
                    FetchRequest.INVALID_LOG_START_OFFSET,
                    fetchConfig.fetchSize,
                    view.positions[i].currentLeader.epoch,
                    Optional.empty()));
        }
        return builder.build();
    }

    /** Called on the receiver thread of the connection, when the consumer closes. */
    private void closeLaneSession(Node laneNode, FetchSessionHandler handler) {
        try {
            handler.notifyClose();
            FetchSessionHandler.FetchRequestData data = handler.newBuilder().build();
            FetchRequest.Builder request = createFetchRequest(laneNode, data);
            sendOnLane(laneNode, request, (response, error) -> {
                if (error != null)
                    handleCloseFetchSessionFailure(laneNode, data, error);
                else
                    handleCloseFetchSessionSuccess(laneNode, data, response);
            });
        } catch (Throwable t) {
            log.debug("Unable to send a close message for the fetch session of node {}", laneNode, t);
        }
    }

    // Visible for testing
    protected FetchSessionHandler sessionHandler(int node) {
        return sessionHandlers.get(node);
    }

    /**
     * This method is called by {@link #close(Timer)} which is guarded by the {@link IdempotentCloser}) such as to only
     * be executed once the first time that any of the {@link #close()} methods are called. Subclasses can override
     * this method without the need for extra synchronization at the instance level.
     *
     * @param timer Timer to enforce time limit
     */
    // Visible for testing
    protected void closeInternal(Timer timer) {
        // we do not need to re-enable wake-ups since we are closing already
        Utils.closeQuietly(fetchBuffer, "fetchBuffer");
        Utils.closeQuietly(decompressionBufferSupplier, "decompressionBufferSupplier");
    }

    public void close(final Timer timer) {
        idempotentCloser.close(() -> closeInternal(timer));
    }

    @Override
    public void close() {
        close(time.timer(Duration.ZERO));
    }

    /**
     * Defines the contract for handling fetch responses from brokers.
     * @param <T> Type of response, usually either {@link ClientResponse} or {@link Throwable}
     */
    @FunctionalInterface
    protected interface ResponseHandler<T> {

        /**
         * Handle the response from the given {@link Node target}
         */
        void handle(Node target, FetchSessionHandler.FetchRequestData data, T response);
    }
}
