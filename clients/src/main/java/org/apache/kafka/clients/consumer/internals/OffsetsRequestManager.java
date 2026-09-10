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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.StaleMetadataException;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetData;
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetResult;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.maybeWrapAsKafkaException;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.refreshCommittedOffsets;
import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.hasUsableOffsetForLeaderEpochVersion;
import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.regroupFetchPositionsByLeader;

/**
 * Manager responsible for building the following requests to retrieve partition offsets, and
 * processing its responses.
 * <ul>
 *      <li>ListOffset request</li>
 *      <li>OffsetForLeaderEpoch request</li>
 * </ul>
 * Requests are kept in-memory ready to be sent on the next call to {@link #poll(long)}.
 * <br>
 * Partition leadership information required to build ListOffset requests is retrieved from the
 * {@link ConsumerMetadata}, so this implements {@link ClusterResourceListener} to get notified
 * when the cluster metadata is updated.
 */
public final class OffsetsRequestManager implements RequestManager, ClusterResourceListener {

    private final ConsumerMetadata metadata;
    private final IsolationLevel isolationLevel;
    private final Logger log;
    private final OffsetFetcherUtils offsetFetcherUtils;
    private final SubscriptionState subscriptionState;

    private final Set<ListOffsetsRequestState> requestsToRetry;
    private final Set<ResetPositionsRequestState> resetRequests;
    private final Set<ValidationRequestState> validationRequests;
    private final List<NetworkClientDelegate.UnsentRequest> requestsToSend;
    private final int requestTimeoutMs;
    private final Time time;
    private final ApiVersions apiVersions;
    private final NetworkClientDelegate networkClientDelegate;
    private final CommitRequestManager commitRequestManager;
    private final long defaultApiTimeoutMs;

    /**
     * Exception that occurred while updating positions after the triggering event had already
     * expired. It will be propagated and cleared on the next call to update fetch positions.
     */
    private final AtomicReference<Throwable> cachedUpdatePositionsException = new AtomicReference<>();

    /**
     * This holds the last OffsetFetch request triggered to retrieve committed offsets to update
     * fetch positions that hasn't completed yet. When a response is received, it's used to
     * update the fetch positions and the pendingOffsetFetchEvent is cleared. If the update fetch
     * positions attempt runs out of time before this OffsetFetch gets a response, it will be
     * kept to be used on the next attempt to update fetch positions (if partitions remain the same)
     */
    private PendingFetchCommittedRequest pendingOffsetFetchEvent;

    public OffsetsRequestManager(final SubscriptionState subscriptionState,
                                 final ConsumerMetadata metadata,
                                 final IsolationLevel isolationLevel,
                                 final Time time,
                                 final long retryBackoffMs,
                                 final int requestTimeoutMs,
                                 final long defaultApiTimeoutMs,
                                 final ApiVersions apiVersions,
                                 final NetworkClientDelegate networkClientDelegate,
                                 final CommitRequestManager commitRequestManager,
                                 final PositionsValidator positionsValidator,
                                 final LogContext logContext) {
        requireNonNull(subscriptionState);
        requireNonNull(metadata);
        requireNonNull(isolationLevel);
        requireNonNull(time);
        requireNonNull(apiVersions);
        requireNonNull(networkClientDelegate);
        requireNonNull(logContext);

        this.metadata = metadata;
        this.isolationLevel = isolationLevel;
        this.log = logContext.logger(getClass());
        this.requestsToRetry = new HashSet<>();
        this.resetRequests = new HashSet<>();
        this.validationRequests = new HashSet<>();
        this.requestsToSend = new ArrayList<>();
        this.subscriptionState = subscriptionState;
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        this.apiVersions = apiVersions;
        this.networkClientDelegate = networkClientDelegate;
        this.offsetFetcherUtils = new OffsetFetcherUtils(logContext, metadata, subscriptionState,
                time, retryBackoffMs, apiVersions, positionsValidator);
        // Register the cluster metadata update callback. Note this only relies on the
        // requestsToRetry initialized above, and won't be invoked until all managers are
        // initialized and the network thread started.
        this.metadata.addClusterUpdateListener(this);
        this.commitRequestManager = commitRequestManager;
    }

    private static class PendingFetchCommittedRequest {
        final Set<TopicPartition> requestedPartitions;
        final CompletableFuture<Void> result;
        final Predicate<TopicPartition> scope;

        private PendingFetchCommittedRequest(final Set<TopicPartition> requestedPartitions,
                                             final Predicate<TopicPartition> scope,
                                             final CompletableFuture<Void> result) {
            this.requestedPartitions = Set.copyOf(requestedPartitions);
            this.scope = scope;
            this.result = Objects.requireNonNull(result);
        }
    }

    /**
     * Determine if there are pending fetch offsets requests to be sent and build a
     * {@link NetworkClientDelegate.PollResult}
     * containing it.
     */
    @Override
    public NextPollCondition nextPollCondition(long currentTimeMs) {
        // Commands, response callbacks and the metadata listener retain outgoing work in this queue.
        if (!requestsToSend.isEmpty())
            return NextPollCondition.ready();
        NextPollCondition condition = NextPollCondition.idle();
        for (ResetPositionsRequestState request : resetRequests) {
            if (!request.inFlight) {
                if (request.hasInvalidRemaining())
                    return NextPollCondition.ready();
                if (!request.awaitingMetadata)
                    condition = NextPollCondition.either(condition,
                        NextPollCondition.at(request.nextRetryMs));
            }
        }
        for (ValidationRequestState request : validationRequests) {
            if (!request.inFlight) {
                if (request.hasInvalidRemaining())
                    return NextPollCondition.ready();
                if (!request.awaitingMetadata)
                    condition = NextPollCondition.either(condition,
                        NextPollCondition.at(request.nextRetryMs));
            }
        }
        return condition;
    }

    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        new ArrayList<>(resetRequests).forEach(request -> {
            if (!request.inFlight && (request.hasInvalidRemaining() ||
                    (!request.awaitingMetadata && currentTimeMs >= request.nextRetryMs)))
                prepareResetPositionsRequests(request, currentTimeMs);
        });
        new ArrayList<>(validationRequests).forEach(request -> {
            if (!request.inFlight && (request.hasInvalidRemaining() ||
                    (!request.awaitingMetadata && currentTimeMs >= request.nextRetryMs)))
                prepareValidationRequests(request, currentTimeMs);
        });
        // Copy the outgoing request list and clear it.
        List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>(requestsToSend);
        requestsToSend.clear();
        return new NetworkClientDelegate.PollResult(unsentRequests);
    }

    /**
     * Retrieve offsets for the given partitions and timestamp. For each partition, this will
     * retrieve the offset of the first message whose timestamp is greater than or equals to the
     * target timestamp.
     *
     * @param timestampsToSearch Partitions and target timestamps to get offsets for
     * @param requireTimestamps  True if this should fail with an UnsupportedVersionException if the
     *                           broker does not support fetching precise timestamps for offsets
     * @return Future containing the map of {@link TopicPartition} and {@link OffsetAndTimestamp}
     * found .The future will complete when the requests responses are received and
     * processed, following a call to {@link #poll(long)}
     */
    public CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> fetchOffsets(
            Map<TopicPartition, Long> timestampsToSearch,
            boolean requireTimestamps) {
        return fetchOffsets(timestampsToSearch, requireTimestamps, false);
    }

    private CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> fetchOffsets(
            Map<TopicPartition, Long> timestampsToSearch,
            boolean requireTimestamps,
            boolean oneShot) {
        if (timestampsToSearch.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }
        metadata.addTransientTopics(OffsetFetcherUtils.topicsForPartitions(timestampsToSearch.keySet()));
        ListOffsetsRequestState listOffsetsRequestState = new ListOffsetsRequestState(
                timestampsToSearch,
                requireTimestamps,
                offsetFetcherUtils,
                isolationLevel,
                oneShot);
        listOffsetsRequestState.globalResult.whenComplete((result, error) -> {
            metadata.clearTransientTopics();
            if (error != null) {
                log.debug("Fetch offsets completed with error for partitions and timestamps {}.",
                        timestampsToSearch, error);
            } else {
                log.debug("Fetch offsets completed successfully for partitions and timestamps {}." +
                        " Result {}", timestampsToSearch, result);
            }
        });

        prepareFetchOffsetsRequests(timestampsToSearch, requireTimestamps, listOffsetsRequestState);
        return listOffsetsRequestState.globalResult.thenApply(
                result -> OffsetFetcherUtils.buildOffsetsForTimeInternalResult(
                        timestampsToSearch,
                        result.fetchedOffsets));
    }

    /**
     * Retrieve the consumer's lag on the given partition, i.e. the number of records between the consumer's
     * position and the end of the partition (the high watermark, or the last stable offset when reading with
     * {@link IsolationLevel#READ_COMMITTED}).
     *
     * <p/>
     *
     * If the end offset is not known, this issues a <code>LIST_OFFSETS</code> request in the background so that
     * the lag may be available on a subsequent call, and returns an empty result for now. Only one such request
     * is allowed in flight per partition at a time; that is tracked by the 'end offset requested' flag in
     * {@link SubscriptionState}, which is set here and cleared when the request completes, however it completes.
     *
     * @param topicPartition Partition to retrieve the lag for
     * @param isolationLevel Isolation level the lag should be calculated against
     * @return The lag, or empty if the end offset for the partition is not (yet) known
     */
    public OptionalLong currentLag(TopicPartition topicPartition, IsolationLevel isolationLevel) {
        final Long lag = subscriptionState.partitionLag(topicPartition, isolationLevel);

        if (lag == null) {
            // If the log end offset is unknown and there isn't already an in-flight list offset
            // request, issue one with the goal that the lag will be available the next time the
            // user calls currentLag().
            if (subscriptionState.partitionEndOffset(topicPartition, isolationLevel) == null &&
                offsetFetcherUtils.maybeSetPartitionEndOffsetRequest(topicPartition)) {

                Map<TopicPartition, Long> timestampToSearch = Map.of(
                    topicPartition,
                    ListOffsetsRequest.LATEST_TIMESTAMP
                );

                // The request is issued as 'one shot' so that it always completes rather than being retried
                // internally on a metadata update. That keeps the 'end offset requested' flag clearing below
                // reachable on every outcome, so a failed LIST_OFFSETS doesn't block all future lag lookups.
                // A successful response clears the flag as a side effect of updating the subscription state,
                // so the call below is a no-op in that case.
                fetchOffsets(timestampToSearch, false, true).whenComplete((__, error) ->
                    offsetFetcherUtils.clearPartitionEndOffsetRequests(Set.of(topicPartition)));
            }

            return OptionalLong.empty();
        }

        return OptionalLong.of(lag);
    }

    /**
     * Update fetch positions for assigned partitions that do not have a position. This will:
     * <ul>
     *     <li>check if all assigned partitions already have fetch positions and return right away if that's the case</li>
     *     <li>trigger an async request to validate positions (detect log truncation)</li>
     *     <li>fetch committed offsets if enabled, and use the response to update the positions</li>
     *     <li>fetch partition offsets for partitions that may still require a position, and use the response to
     *     update the positions</li>
     * </ul>
     *
     * @param deadlineMs Time in milliseconds when the triggering application event expires. Any error received after
     *                   this will be saved, and used to complete the result exceptionally on the next call to this
     *                   function.
     * @return Future that will complete with a boolean indicating if all assigned partitions have positions (based
     * on {@link SubscriptionState#hasAllFetchPositions()}). It will complete immediately, with true, if all positions
     * are already available. If some positions are missing, the future will complete once the offsets are retrieved and positions are updated.
     */
    public CompletableFuture<Void> updateFetchPositions(long deadlineMs) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        try {
            if (maybeCompleteWithPreviousException(result)) {
                return result;
            }

            validatePositionsIfNeeded();

            if (subscriptionState.hasAllFetchPositions()) {
                // All positions are already available
                result.complete(null);
                return result;
            }

            // Some positions are missing, so trigger requests to fetch offsets and update them.
            updatePositionsWithOffsets(deadlineMs).whenComplete((__, error) -> {
                if (error != null) {
                    result.completeExceptionally(error);
                } else {
                    result.complete(null);
                }
            });

        } catch (Exception e) {
            result.completeExceptionally(maybeWrapAsKafkaException(e));
        }
        return result;
    }

    private boolean maybeCompleteWithPreviousException(CompletableFuture<Void> result) {
        Throwable cachedException = cachedUpdatePositionsException.getAndSet(null);
        if (cachedException != null) {
            result.completeExceptionally(cachedException);
            return true;
        }
        return false;
    }

    /**
     * Generate requests to fetch offsets and update positions once a response is received. This will first attempt
     * to use the committed offsets if available. If no committed offsets available, it will use the partition
     * offsets retrieved from the leader.
     */
    private CompletableFuture<Void> updatePositionsWithOffsets(long deadlineMs) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        cacheExceptionIfEventExpired(result, deadlineMs);

        CompletableFuture<Void> updatePositions;
        final Set<TopicPartition> initializingPartitions = subscriptionState.initializingPartitions();
        final Predicate<TopicPartition> initializingLifetime =
                subscriptionState.initializingPartitionsScope(initializingPartitions);
        final Predicate<TopicPartition> initializingIntent =
                subscriptionState.assignedPartitionsScope(initializingPartitions);
        final Predicate<TopicPartition> initializingScope =
                partition -> initializingLifetime.test(partition) && initializingIntent.test(partition);
        final Set<TopicPartition> resetPartitions = subscriptionState.partitionsNeedingReset(time.milliseconds());
        final Predicate<TopicPartition> resetScope = subscriptionState.assignedPartitionsScope(resetPartitions);
        if (commitRequestManager != null) {
            CompletableFuture<Void> refreshWithCommittedOffsets =
                    initWithCommittedOffsetsIfNeeded(initializingPartitions, initializingScope, deadlineMs);

            // Reset positions for all partitions that may still require it (or that are awaiting reset)
            updatePositions = refreshWithCommittedOffsets.thenCompose(__ ->
                    initWithPartitionOffsetsIfNeeded(initializingScope, resetScope));

        } else {
            updatePositions = initWithPartitionOffsetsIfNeeded(initializingScope, resetScope);
        }

        updatePositions.whenComplete((__, resetError) -> {
            if (resetError == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(resetError);
            }
        });

        return result;
    }

    /**
     * Save exception that may occur while updating fetch positions. Note that since the update fetch positions
     * is triggered asynchronously, errors may be found when the triggering UpdateFetchPositionsEvent has already
     * expired. In that case, the exception is saved in memory, to be thrown when processing the following
     * UpdateFetchPositionsEvent.
     *
     * @param result     Update fetch positions future to get the exception from (if any)
     * @param deadlineMs Deadline of the triggering application event, used to identify if the event has already
     *                   expired when the error in the result future occurs.
     */
    private void cacheExceptionIfEventExpired(CompletableFuture<Void> result, long deadlineMs) {
        result.whenComplete((__, error) -> {
            boolean updatePositionsExpired = time.milliseconds() >= deadlineMs;
            if (error != null && updatePositionsExpired) {
                cachedUpdatePositionsException.set(error);
            }
        });
    }

    /**
     * If there are partitions still needing a position and a reset policy is defined, request reset using the default policy.
     *
     * @param initializingScope Captured partition lifetimes and position intents that may still be initialized.
     *                          Newly assigned partitions, including removed and re-added partitions with the same
     *                          name, are excluded.
     * @param resetScope        Captured partition lifetimes and position intents already awaiting a reset when the
     *                          operation started. Resets requested while an older committed-offset fetch is pending
     *                          are excluded.
     * @return Future that completes when the reset attempt initiated or observed by this update finishes. A retained
     * reset owner may continue waiting for metadata or retrying another partition after this future completes, so
     * partitions that obtained a position can be fetched without waiting for unrelated reset completion.
     * @throws NoOffsetForPartitionException If no reset strategy is configured.
     */
    private CompletableFuture<Void> initWithPartitionOffsetsIfNeeded(Predicate<TopicPartition> initializingScope,
                                                                     Predicate<TopicPartition> resetScope) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            // Mark partitions that need reset, using the configured reset strategy. If no
            // strategy is defined, this will raise a NoOffsetForPartitionException exception.
            subscriptionState.resetInitializingPositions(initializingScope);
        } catch (Exception e) {
            result.completeExceptionally(e);
            return result;
        }

        // For partitions awaiting reset, generate a ListOffset request to retrieve the partition
        // offsets according to the strategy (ex. earliest, latest), and update the positions.
        return resetPositionsIfNeeded(partition ->
                initializingScope.test(partition) || resetScope.test(partition));
    }

    /**
     * Fetch the committed offsets for partitions that require initialization. This will trigger an OffsetFetch
     * request and update positions in the subscription state once a response is received.
     *
     * @param initializingPartitions Set of partitions to update with a position. This same set will be kept
     *                               throughout the whole process (considered when fetching committed offsets, and
     *                               when resetting positions for partitions that may not have committed offsets).
     * @param scope                  Captured initializing partition lifetimes eligible for this response.
     * @param deadlineMs             Deadline of the application event that triggered this operation. Used to
     *                               determine how much time to allow for the reused offset fetch to complete.
     * @throws TimeoutException If offsets could not be retrieved within the timeout
     */
    private CompletableFuture<Void> initWithCommittedOffsetsIfNeeded(Set<TopicPartition> initializingPartitions,
                                                                     Predicate<TopicPartition> scope,
                                                                     long deadlineMs) {
        if (initializingPartitions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        log.debug("Refreshing committed offsets for partitions {}", initializingPartitions);
        CompletableFuture<Void> result = new CompletableFuture<>();

        // The shorter the timeout provided to poll(), the more likely the offsets fetch will time out. To handle
        // this case, on the first attempt to fetch the committed offsets, a FetchCommittedOffsetsEvent is created
        // (with potentially a longer timeout) and stored. The event is used for the first attempt, but in the
        // case it times out, subsequent attempts will also use the event in order to wait for the results.
        if (!canReusePendingOffsetFetchEvent(initializingPartitions)) {
            // Generate a new OffsetFetch request and update positions when a response is received
            final long fetchCommittedDeadlineMs = Math.max(deadlineMs, time.milliseconds() + defaultApiTimeoutMs);
            CompletableFuture<CommitRequestManager.OffsetFetchResult> fetchOffsets =
                    commitRequestManager.fetchOffsets(initializingPartitions, fetchCommittedDeadlineMs);
            PendingFetchCommittedRequest pending = new PendingFetchCommittedRequest(initializingPartitions, scope, result);
            // Publish before attaching the callback, since the source may already be complete.
            pendingOffsetFetchEvent = pending;
            fetchOffsets.thenApply(CommitRequestManager.OffsetFetchResult::toOffsetMapWithNulls)
                .whenComplete((offsets, error) -> {
                    // Another partition set may have installed a newer request while this one waited.
                    if (pendingOffsetFetchEvent == pending)
                        pendingOffsetFetchEvent = null;
                    try {
                        refreshOffsets(offsets, error, scope, result);
                    } catch (Exception e) {
                        result.completeExceptionally(e);
                    }
                });
        } else {
            // Reuse pending OffsetFetch request that will complete when positions are refreshed with the committed offsets retrieved
            pendingOffsetFetchEvent.result.whenComplete((__, error) -> {
                if (error == null) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(error);
                }
            });
        }

        return result;
    }

    /**
     * Use the given committed offsets to update positions for partitions that still require it.
     *
     * @param offsets Committed offsets to use to update positions for initializing partitions.
     * @param error   Error received in response to the OffsetFetch request. Will be null if the request was successful.
     * @param result  Future to complete once all positions have been updated with the given committed offsets
     */
    private void refreshOffsets(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                final Throwable error,
                                final Predicate<TopicPartition> scope,
                                final CompletableFuture<Void> result) {
        if (error == null) {

            // Ensure we only set positions for the partitions that still require one (ex. some partitions may have
            // been assigned a position manually)
            Map<TopicPartition, OffsetAndMetadata> offsetsToApply = offsetsForInitializingPartitions(offsets, scope);

            refreshCommittedOffsets(offsetsToApply, metadata, subscriptionState);

            result.complete(null);

        } else {
            log.error("Error fetching committed offsets to update positions", error);
            result.completeExceptionally(error);
        }
    }

    /**
     * Get the offsets, from the given collection, that belong to partitions that still require a position (partitions
     * that are initializing). This is expected to be used to filter out offsets that were retrieved for partitions
     * that do not need a position anymore.
     *
     * @param offsets Offsets per partition
     * @return Subset of the offsets associated to partitions that are still initializing
     */
    private Map<TopicPartition, OffsetAndMetadata> offsetsForInitializingPartitions(Map<TopicPartition, OffsetAndMetadata> offsets,
                                                                                   Predicate<TopicPartition> scope) {
        Set<TopicPartition> currentlyInitializingPartitions = subscriptionState.initializingPartitions();
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
        offsets.forEach((key, value) -> {
            if (currentlyInitializingPartitions.contains(key) && scope.test(key)) {
                result.put(key, value);
            }
        });
        return result;
    }

    /**
     * This determines if the {@link #pendingOffsetFetchEvent pending offset fetch event} can be reused. Reuse
     * is only possible if all the following conditions are true:
     *
     * <ul>
     *     <li>A pending offset fetch event exists</li>
     *     <li>The partition set of the pending offset fetch event is the same as the given partitions</li>
     * </ul>
     */
    private boolean canReusePendingOffsetFetchEvent(Set<TopicPartition> partitions) {
        if (pendingOffsetFetchEvent == null) {
            return false;
        }

        return pendingOffsetFetchEvent.requestedPartitions.equals(partitions) && partitions.stream().allMatch(pendingOffsetFetchEvent.scope);
    }

    /**
     * Reset offsets for all assigned partitions that require it. Offsets will be reset
     * with timestamps according to the reset strategy defined for each partition. This will
     * generate ListOffsets requests for the partitions and timestamps, and enqueue them to be sent
     * on the next call to {@link #poll(long)}.
     * <p/>
     * When a response is received, positions are updated in-memory, on the subscription state. If
     * an error is received in the response, it will be saved to be thrown on the next call to
     * this function (ex. {@link org.apache.kafka.common.errors.TopicAuthorizationException})
     */
    CompletableFuture<Void> resetPositionsIfNeeded() {
        Map<TopicPartition, AutoOffsetResetStrategy> partitionAutoOffsetResetStrategyMap;

        try {
            partitionAutoOffsetResetStrategyMap = offsetFetcherUtils.getOffsetResetStrategyForPartitions();
        } catch (Exception e) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            result.completeExceptionally(e);
            return result;
        }

        if (partitionAutoOffsetResetStrategyMap.isEmpty())
            return CompletableFuture.completedFuture(null);

        Predicate<TopicPartition> scope = subscriptionState.assignedPartitionsScope(
                partitionAutoOffsetResetStrategyMap.keySet());
        return retainOrStartResetPositions(partitionAutoOffsetResetStrategyMap, scope, false);
    }

    private CompletableFuture<Void> resetPositionsIfNeeded(Predicate<TopicPartition> scope) {
        Map<TopicPartition, AutoOffsetResetStrategy> partitionAutoOffsetResetStrategyMap;
        try {
            partitionAutoOffsetResetStrategyMap = offsetFetcherUtils.getOffsetResetStrategyForPartitions();
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        if (partitionAutoOffsetResetStrategyMap.isEmpty())
            return CompletableFuture.completedFuture(null);
        Predicate<TopicPartition> currentIntent = subscriptionState.assignedPartitionsScope(
                partitionAutoOffsetResetStrategyMap.keySet());
        return retainOrStartResetPositions(partitionAutoOffsetResetStrategyMap,
                partition -> scope.test(partition) && currentIntent.test(partition), true);
    }

    private CompletableFuture<Void> retainOrStartResetPositions(
            Map<TopicPartition, AutoOffsetResetStrategy> strategies,
            Predicate<TopicPartition> scope,
            boolean completeAfterCurrentAttempt) {
        Set<CompletableFuture<Void>> continuations = new HashSet<>();
        Map<TopicPartition, AutoOffsetResetStrategy> unowned = new HashMap<>();
        strategies.forEach((partition, strategy) -> {
            if (!scope.test(partition))
                return;
            Optional<ResetPositionsRequestState> owner = resetRequests.stream()
                    .filter(request -> request.owns(partition, strategy))
                    .findFirst();
            if (owner.isPresent()) {
                continuations.add(completeAfterCurrentAttempt
                        ? owner.get().attemptResult
                        : owner.get().result);
            } else {
                unowned.put(partition, strategy);
            }
        });
        if (!unowned.isEmpty()) {
            ResetPositionsRequestState owner = startResetPositions(unowned, scope);
            continuations.add(completeAfterCurrentAttempt ? owner.attemptResult : owner.result);
        }
        return CompletableFuture.allOf(continuations.toArray(new CompletableFuture<?>[0]));
    }

    /**
     * Validate positions for all assigned partitions for which a leader change has been detected.
     * This will generate OffsetsForLeaderEpoch requests for the partitions, with the known offset
     * epoch and current leader epoch. It will enqueue the generated requests, to be sent on the
     * next call to {@link #poll(long)}.
     *
     * <p/>
     *
     * When a response is received, positions are validated and, if a log truncation is detected, a
     * {@link LogTruncationException} will be saved in memory in cachedUpdatePositionsException, to be thrown on the
     * next call to this function.
     */
    void validatePositionsIfNeeded() {
        Map<TopicPartition, SubscriptionState.FetchPosition> partitionsToValidate = offsetFetcherUtils.refreshAndGetPartitionsToValidate();
        if (partitionsToValidate.isEmpty()) {
            return;
        }

        reconcileValidationRequests(partitionsToValidate, true);
    }

    private void reconcileValidationRequests(
            Map<TopicPartition, SubscriptionState.FetchPosition> positions,
            boolean forceExistingOwner) {
        new ArrayList<>(validationRequests).forEach(request -> {
            request.remaining.entrySet().removeIf(entry -> !isValidationPositionCurrent(
                    request, entry.getKey(), entry.getValue()));
            if (!request.inFlight && request.remaining.isEmpty())
                validationRequests.remove(request);
        });
        Map<TopicPartition, SubscriptionState.FetchPosition> unowned = new HashMap<>();
        Set<ValidationRequestState> owners = new HashSet<>();
        positions.forEach((partition, position) -> {
            Optional<ValidationRequestState> owner = validationRequests.stream()
                    .filter(request -> request.owns(partition, position))
                    .findFirst();
            if (owner.isPresent()) {
                owners.add(owner.get());
            } else {
                unowned.put(partition, position);
            }
        });
        long currentTimeMs = time.milliseconds();
        owners.forEach(request -> {
            if (forceExistingOwner && !request.inFlight && !request.awaitingMetadata) {
                request.nextRetryMs = currentTimeMs;
                prepareValidationRequests(request, currentTimeMs);
            }
        });
        if (!unowned.isEmpty()) {
            ValidationRequestState request = new ValidationRequestState(
                    unowned, subscriptionState.assignedPartitionsScope(unowned.keySet()));
            validationRequests.add(request);
            prepareValidationRequests(request, currentTimeMs);
        }
    }

    private boolean isValidationPositionCurrent(
            ValidationRequestState request,
            TopicPartition partition,
            SubscriptionState.FetchPosition position) {
        return subscriptionState.matchesPosition(partition, position, request.scope);
    }

    /**
     * Generate requests for partitions with known leaders. Update the listOffsetsRequestState by adding
     * partitions with unknown leader to the listOffsetsRequestState.remainingToSearch
     */
    private void prepareFetchOffsetsRequests(final Map<TopicPartition, Long> timestampsToSearch,
                                             final boolean requireTimestamps,
                                             final ListOffsetsRequestState listOffsetsRequestState) {
        try {
            List<NetworkClientDelegate.UnsentRequest> unsentRequests = buildListOffsetsRequests(
                    timestampsToSearch, requireTimestamps, listOffsetsRequestState);
            requestsToSend.addAll(unsentRequests);
        } catch (StaleMetadataException e) {
            if (listOffsetsRequestState.oneShot)
                listOffsetsRequestState.completeWithResultSoFar();
            else
                requestsToRetry.add(listOffsetsRequestState);
        }
    }

    @Override
    public void onUpdate(ClusterResource clusterResource) {
        // Retry requests that were awaiting a metadata update. Process a copy of the list to
        // avoid errors, given that the list of requestsToRetry may be modified from the
        // fetchOffsetsByTimes call if any of the requests being retried fails
        List<ListOffsetsRequestState> requestsToProcess = new ArrayList<>(requestsToRetry);
        requestsToRetry.clear();
        requestsToProcess.forEach(requestState -> {
            Map<TopicPartition, Long> timestampsToSearch =
                    new HashMap<>(requestState.remainingToSearch);
            requestState.remainingToSearch.clear();
            prepareFetchOffsetsRequests(timestampsToSearch, requestState.requireTimestamps, requestState);
        });
        new ArrayList<>(resetRequests).forEach(request -> {
            if (request.inFlight) {
                request.metadataUpdatedWhileInFlight = true;
            } else {
                request.awaitingMetadata = false;
            }
            if (!request.inFlight && time.milliseconds() >= request.nextRetryMs)
                prepareResetPositionsRequests(request, time.milliseconds());
        });
        Map<TopicPartition, SubscriptionState.FetchPosition> currentValidationPositions =
                offsetFetcherUtils.refreshValidationAfterMetadataUpdate();
        new ArrayList<>(validationRequests).forEach(request -> {
            if (request.inFlight) {
                request.metadataUpdatedWhileInFlight = true;
            } else {
                request.awaitingMetadata = false;
            }
        });
        reconcileValidationRequests(currentValidationPositions, false);
        new ArrayList<>(validationRequests).forEach(request -> {
            if (!request.inFlight && !request.awaitingMetadata && time.milliseconds() >= request.nextRetryMs)
                prepareValidationRequests(request, time.milliseconds());
        });
    }

    /**
     * Build ListOffsets requests to fetch offsets by target times for the specified partitions.
     *
     * @param timestampsToSearch the mapping between partitions and target time
     * @param requireTimestamps  true if we should fail with an UnsupportedVersionException if the broker does
     *                           not support fetching precise timestamps for offsets
     * @return A list of
     * {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest}
     * that can be polled to obtain the corresponding timestamps and offsets.
     */
    private List<NetworkClientDelegate.UnsentRequest> buildListOffsetsRequests(
            final Map<TopicPartition, Long> timestampsToSearch,
            final boolean requireTimestamps,
            final ListOffsetsRequestState listOffsetsRequestState) {
        log.debug("Building ListOffsets request for partitions {}", timestampsToSearch);
        Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(timestampsToSearch, Optional.of(listOffsetsRequestState.remainingToSearch));
        if (timestampsToSearchByNode.isEmpty()) {
            throw new StaleMetadataException();
        }

        final List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();
        MultiNodeRequest multiNodeRequest = new MultiNodeRequest(timestampsToSearchByNode.size());
        multiNodeRequest.onComplete((multiNodeResult, error) -> {
            // Done sending request to a set of known leaders
            if (error == null) {
                listOffsetsRequestState.fetchedOffsets.putAll(multiNodeResult.fetchedOffsets);
                listOffsetsRequestState.addPartitionsToRetry(multiNodeResult.partitionsToRetry);
                offsetFetcherUtils.updateSubscriptionState(multiNodeResult.fetchedOffsets,
                        isolationLevel);

                if (listOffsetsRequestState.remainingToSearch.isEmpty() || listOffsetsRequestState.oneShot) {
                    listOffsetsRequestState.completeWithResultSoFar();
                } else {
                    requestsToRetry.add(listOffsetsRequestState);
                    metadata.requestUpdate(false);
                }
            } else {
                log.debug("ListOffsets request failed with error", error);
                listOffsetsRequestState.globalResult.completeExceptionally(error);
            }
        });

        for (Map.Entry<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> entry : timestampsToSearchByNode.entrySet()) {
            Node node = entry.getKey();
            CompletableFuture<ListOffsetResult> partialResult = buildListOffsetRequestToNode(
                    node,
                    entry.getValue(),
                    requireTimestamps,
                    unsentRequests);

            partialResult.whenComplete((result, error) -> {
                if (error != null) {
                    multiNodeRequest.resultFuture.completeExceptionally(error);
                } else {
                    multiNodeRequest.addPartialResult(result);
                }
            });
        }
        return unsentRequests;
    }

    /**
     * Build ListOffsets request to send to a specific broker for the partitions and
     * target timestamps. This also adds the request to the list of unsentRequests.
     */
    private CompletableFuture<ListOffsetResult> buildListOffsetRequestToNode(
            Node node,
            Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> targetTimes,
            boolean requireTimestamps,
            List<NetworkClientDelegate.UnsentRequest> unsentRequests) {
        ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder
                .forConsumer(requireTimestamps, isolationLevel)
                .setTargetTimes(ListOffsetsRequest.toListOffsetsTopics(targetTimes))
                .setTimeoutMs(requestTimeoutMs);

        log.debug("Creating ListOffset request {} for broker {} to reset positions", builder,
                node);

        NetworkClientDelegate.UnsentRequest unsentRequest = new NetworkClientDelegate.UnsentRequest(
                builder,
                Optional.ofNullable(node));
        unsentRequests.add(unsentRequest);
        CompletableFuture<ListOffsetResult> result = new CompletableFuture<>();
        unsentRequest.whenComplete((response, error) -> {
            if (error != null) {
                log.debug("Sending ListOffset request {} to broker {} failed",
                        builder,
                        node,
                        error);
                result.completeExceptionally(error);
            } else {
                ListOffsetsResponse lor = (ListOffsetsResponse) response.responseBody();
                log.trace("Received ListOffsetResponse {} from broker {}", lor, node);
                try {
                    ListOffsetResult listOffsetResult = offsetFetcherUtils.handleListOffsetResponse(lor);
                    result.complete(listOffsetResult);
                } catch (RuntimeException e) {
                    result.completeExceptionally(e);
                }
            }
        });
        return result;
    }

    /**
     * Make asynchronous ListOffsets request to fetch offsets by target times for the specified
     * partitions. Use the retrieved offsets to reset positions in the subscription state.
     * This also adds the request to the list of unsentRequests.
     *
     * @param partitionAutoOffsetResetStrategyMap the mapping between partitions and AutoOffsetResetStrategy
     * @return the retained reset owner, including separate current-attempt and terminal completion futures.
     */
    private ResetPositionsRequestState startResetPositions(
            final Map<TopicPartition, AutoOffsetResetStrategy> partitionAutoOffsetResetStrategyMap,
            final Predicate<TopicPartition> scope) {
        ResetPositionsRequestState request = new ResetPositionsRequestState(
                partitionAutoOffsetResetStrategyMap, scope);
        resetRequests.add(request);
        prepareResetPositionsRequests(request, time.milliseconds());
        return request;
    }

    private void prepareResetPositionsRequests(ResetPositionsRequestState request, long currentTimeMs) {
        if (request.result.isDone() || request.inFlight)
            return;
        request.remainingToSearch.entrySet().removeIf(entry -> !request.scope.test(entry.getKey()));
        if (request.remainingToSearch.isEmpty()) {
            completeResetRequest(request);
            return;
        }
        if (request.awaitingMetadata || currentTimeMs < request.nextRetryMs)
            return;
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>(request.remainingToSearch);
        request.remainingToSearch.clear();
        Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(timestampsToSearch, Optional.of(request.remainingToSearch));
        request.awaitingMetadata = !request.remainingToSearch.isEmpty();
        final List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();
        timestampsToSearchByNode.forEach((node, resetTimestamps) ->
                prepareResetPositionsRequestToNode(request, node, resetTimestamps, unsentRequests));
        if (unsentRequests.isEmpty()) {
            if (request.remainingToSearch.isEmpty())
                completeResetRequest(request);
        } else {
            request.expectedResponses.set(unsentRequests.size());
            request.attemptResult = new CompletableFuture<>();
            request.inFlight = true;
            requestsToSend.addAll(unsentRequests);
        }
    }

    private void prepareResetPositionsRequestToNode(
            ResetPositionsRequestState request,
            Node node,
            Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> resetTimestamps,
            List<NetworkClientDelegate.UnsentRequest> unsentRequests) {
        subscriptionState.setNextAllowedRetry(resetTimestamps.keySet(),
                time.milliseconds() + requestTimeoutMs);
        CompletableFuture<ListOffsetResult> partialResult = buildListOffsetRequestToNode(
                node,
                resetTimestamps,
                false,
                unsentRequests);
        partialResult.whenComplete((result, error) -> {
            if (error == null) {
                handleSuccessfulResetPositionsResponse(request, result);
            } else {
                handleFailedResetPositionsResponse(request, resetTimestamps, error);
            }
            handleResetPositionsResponseCompletion(request);
        });
    }

    private void handleSuccessfulResetPositionsResponse(
            ResetPositionsRequestState request,
            ListOffsetResult result) {
        Map<TopicPartition, ListOffsetData> fetchedOffsets = new HashMap<>(result.fetchedOffsets);
        fetchedOffsets.entrySet().removeIf(entry -> !request.scope.test(entry.getKey()));
        Set<TopicPartition> partitionsToRetry = result.partitionsToRetry.stream()
                .filter(request.scope)
                .collect(Collectors.toSet());
        offsetFetcherUtils.onSuccessfulResponseForResettingPositions(
                new ListOffsetResult(fetchedOffsets, partitionsToRetry), request.strategies);
        request.addPartitionsToRetry(partitionsToRetry);
        if (!partitionsToRetry.isEmpty()) {
            request.nextRetryMs = time.milliseconds() + offsetFetcherUtils.retryBackoffMs();
            request.awaitingMetadata = true;
        }
    }

    private void handleFailedResetPositionsResponse(
            ResetPositionsRequestState request,
            Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> resetTimestamps,
            Throwable error) {
        RuntimeException failure = error instanceof RuntimeException
                ? (RuntimeException) error
                : new RuntimeException("Unexpected failure in ListOffsets request for resetting positions", error);
        Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> stillValid =
                resetTimestamps.entrySet().stream()
                        .filter(entry -> request.scope.test(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (stillValid.isEmpty())
            return;
        offsetFetcherUtils.onFailedResponseForResettingPositions(stillValid, failure);
        if (failure instanceof RetriableException) {
            request.addPartitionsToRetry(stillValid.keySet());
            request.nextRetryMs = time.milliseconds() + offsetFetcherUtils.retryBackoffMs();
            request.awaitingMetadata = true;
        }
    }

    private void handleResetPositionsResponseCompletion(ResetPositionsRequestState request) {
        if (request.expectedResponses.decrementAndGet() != 0)
            return;
        CompletableFuture<Void> completedAttempt = request.attemptResult;
        request.inFlight = false;
        if (request.metadataUpdatedWhileInFlight) {
            request.awaitingMetadata = false;
            request.metadataUpdatedWhileInFlight = false;
        }
        if (request.remainingToSearch.isEmpty()) {
            completeResetRequest(request);
        } else if (!request.awaitingMetadata && time.milliseconds() >= request.nextRetryMs) {
            prepareResetPositionsRequests(request, time.milliseconds());
        }
        completedAttempt.complete(null);
    }

    private void completeResetRequest(ResetPositionsRequestState request) {
        resetRequests.remove(request);
        request.result.complete(null);
    }

    /**
     * For each partition that needs validation, make an asynchronous request to get the end-offsets
     * for the partition with the epoch less than or equal to the epoch the partition last saw.
     * <p/>
     * Requests are grouped by Node for efficiency.
     * This also adds the request to the list of unsentRequests.
     *
     * @param partitionsToValidate a map of topic-partition positions to validate

     */
    private void prepareValidationRequests(ValidationRequestState request, long currentTimeMs) {
        if (request.inFlight)
            return;
        request.remaining.entrySet().removeIf(entry -> !isValidationPositionCurrent(
                request, entry.getKey(), entry.getValue()));
        if (request.remaining.isEmpty()) {
            validationRequests.remove(request);
            return;
        }
        if (request.awaitingMetadata || currentTimeMs < request.nextRetryMs)
            return;

        Map<TopicPartition, SubscriptionState.FetchPosition> positions = new HashMap<>(request.remaining);
        request.remaining.clear();
        Map<Node, Map<TopicPartition, SubscriptionState.FetchPosition>> regrouped =
                regroupFetchPositionsByLeader(positions);
        final List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();
        regrouped.forEach((node, fetchPositions) ->
                prepareValidationRequestToNode(request, node, fetchPositions, unsentRequests, currentTimeMs));

        if (unsentRequests.isEmpty()) {
            finishOrRetainValidationRequest(request);
        } else {
            request.expectedResponses.set(unsentRequests.size());
            request.inFlight = true;
            requestsToSend.addAll(unsentRequests);
        }
    }

    private void prepareValidationRequestToNode(
            ValidationRequestState request,
            Node node,
            Map<TopicPartition, SubscriptionState.FetchPosition> fetchPositions,
            List<NetworkClientDelegate.UnsentRequest> unsentRequests,
            long currentTimeMs) {
        Map<TopicPartition, SubscriptionState.FetchPosition> validPositions = fetchPositions.entrySet().stream()
                .filter(entry -> isValidationPositionCurrent(request, entry.getKey(), entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (validPositions.isEmpty())
            return;
        if (node.isEmpty()) {
            request.remaining.putAll(validPositions);
            request.awaitingMetadata = true;
            metadata.requestUpdate(true);
            return;
        }

        NodeApiVersions nodeApiVersions = apiVersions.get(node.idString());
        if (nodeApiVersions == null) {
            request.remaining.putAll(validPositions);
            request.nextRetryMs = currentTimeMs + offsetFetcherUtils.retryBackoffMs();
            networkClientDelegate.tryConnect(node);
            return;
        }

        if (!hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
            log.debug("Skipping validation of fetch offsets for partitions {} since the broker does not " +
                            "support the required protocol version (introduced in Kafka 2.3)",
                    validPositions.keySet());
            validPositions.forEach((partition, position) ->
                    subscriptionState.maybeCompleteValidationWithoutResponse(partition, position, request.scope));
            return;
        }

        subscriptionState.setNextAllowedRetry(validPositions.keySet(), currentTimeMs + requestTimeoutMs);
        CompletableFuture<OffsetsForLeaderEpochUtils.OffsetForEpochResult> partialResult =
                buildOffsetsForLeaderEpochRequestToNode(node, validPositions, unsentRequests);
        partialResult.whenComplete((offsetsResult, error) -> {
            if (error == null) {
                handleSuccessfulValidationResponse(request, validPositions, offsetsResult);
            } else {
                handleFailedValidationResponse(request, validPositions, error);
            }
            handleValidationResponseCompletion(request);
        });
    }

    private void handleSuccessfulValidationResponse(
            ValidationRequestState request,
            Map<TopicPartition, SubscriptionState.FetchPosition> fetchPositions,
            OffsetsForLeaderEpochUtils.OffsetForEpochResult offsetsResult) {
        Map<TopicPartition, SubscriptionState.FetchPosition> validPositions = fetchPositions.entrySet().stream()
                .filter(entry -> isValidationPositionCurrent(request, entry.getKey(), entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<TopicPartition, EpochEndOffset> endOffsets =
                offsetsResult.endOffsets().entrySet().stream()
                        .filter(entry -> validPositions.containsKey(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Set<TopicPartition> partitionsToRetry = offsetsResult.partitionsToRetry().stream()
                .filter(validPositions::containsKey)
                .collect(Collectors.toSet());
        offsetFetcherUtils.onSuccessfulResponseForValidatingPositions(
                validPositions,
                new OffsetsForLeaderEpochUtils.OffsetForEpochResult(endOffsets, partitionsToRetry));
        request.addPartitionsToRetry(partitionsToRetry);
        if (!partitionsToRetry.isEmpty()) {
            request.nextRetryMs = time.milliseconds() + offsetFetcherUtils.retryBackoffMs();
            request.awaitingMetadata = true;
        }
    }

    private void handleFailedValidationResponse(
            ValidationRequestState request,
            Map<TopicPartition, SubscriptionState.FetchPosition> fetchPositions,
            Throwable error) {
        RuntimeException failure = error instanceof RuntimeException
                ? (RuntimeException) error
                : new RuntimeException("Unexpected failure in OffsetsForLeaderEpoch request for validating positions", error);
        Map<TopicPartition, SubscriptionState.FetchPosition> validPositions = fetchPositions.entrySet().stream()
                .filter(entry -> isValidationPositionCurrent(request, entry.getKey(), entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (validPositions.isEmpty())
            return;
        offsetFetcherUtils.onFailedResponseForValidatingPositions(validPositions, failure);
        if (failure instanceof RetriableException) {
            request.remaining.putAll(validPositions);
            request.nextRetryMs = time.milliseconds() + offsetFetcherUtils.retryBackoffMs();
            request.awaitingMetadata = true;
        }
    }

    private void handleValidationResponseCompletion(ValidationRequestState request) {
        if (request.expectedResponses.decrementAndGet() != 0)
            return;
        request.inFlight = false;
        if (request.metadataUpdatedWhileInFlight) {
            request.awaitingMetadata = false;
            request.metadataUpdatedWhileInFlight = false;
        }
        finishOrRetainValidationRequest(request);
    }

    private void finishOrRetainValidationRequest(ValidationRequestState request) {
        if (request.remaining.isEmpty()) {
            validationRequests.remove(request);
        } else if (!request.awaitingMetadata && time.milliseconds() >= request.nextRetryMs) {
            prepareValidationRequests(request, time.milliseconds());
        }
    }

    /**
     * Build OffsetsForLeaderEpoch request to send to a specific broker for the partitions and
     * positions to fetch. This also adds the request to the list of unsentRequests.
     */
    private CompletableFuture<OffsetsForLeaderEpochUtils.OffsetForEpochResult> buildOffsetsForLeaderEpochRequestToNode(
            final Node node,
            final Map<TopicPartition, SubscriptionState.FetchPosition> fetchPositions,
            List<NetworkClientDelegate.UnsentRequest> unsentRequests) {
        AbstractRequest.Builder<OffsetsForLeaderEpochRequest> builder =
                OffsetsForLeaderEpochUtils.prepareRequest(fetchPositions);

        log.debug("Creating OffsetsForLeaderEpoch request request {} to broker {}", builder, node);

        NetworkClientDelegate.UnsentRequest unsentRequest = new NetworkClientDelegate.UnsentRequest(
                builder,
                Optional.ofNullable(node));
        unsentRequests.add(unsentRequest);
        CompletableFuture<OffsetsForLeaderEpochUtils.OffsetForEpochResult> result = new CompletableFuture<>();
        unsentRequest.whenComplete((response, error) -> {
            if (error != null) {
                log.debug("Sending OffsetsForLeaderEpoch request {} to broker {} failed",
                        builder,
                        node,
                        error);
                result.completeExceptionally(error);
            } else {
                OffsetsForLeaderEpochResponse offsetsForLeaderEpochResponse = (OffsetsForLeaderEpochResponse) response.responseBody();
                log.trace("Received OffsetsForLeaderEpoch response {} from broker {}", offsetsForLeaderEpochResponse, node);
                try {
                    OffsetsForLeaderEpochUtils.OffsetForEpochResult listOffsetResult =
                            OffsetsForLeaderEpochUtils.handleResponse(fetchPositions, offsetsForLeaderEpochResponse);
                    result.complete(listOffsetResult);
                } catch (RuntimeException e) {
                    result.completeExceptionally(e);
                }
            }
        });
        return result;
    }

    private static class ListOffsetsRequestState {

        private final Map<TopicPartition, Long> timestampsToSearch;
        private final Map<TopicPartition, ListOffsetData> fetchedOffsets;
        private final Map<TopicPartition, Long> remainingToSearch;
        private final CompletableFuture<ListOffsetResult> globalResult;
        final boolean requireTimestamps;
        final OffsetFetcherUtils offsetFetcherUtils;
        final IsolationLevel isolationLevel;

        /**
         * If true, this request is never held back to be retried on a metadata update. It completes on the
         * first attempt with whatever offsets were retrieved, leaving it to the caller to decide whether to
         * issue another request. This is used by
         * {@link OffsetsRequestManager#currentLag(TopicPartition, IsolationLevel)}, which relies on the
         * request always completing in order to clear its 'end offset requested' flag.
         */
        final boolean oneShot;

        private ListOffsetsRequestState(Map<TopicPartition, Long> timestampsToSearch,
                                        boolean requireTimestamps,
                                        OffsetFetcherUtils offsetFetcherUtils,
                                        IsolationLevel isolationLevel,
                                        boolean oneShot) {
            remainingToSearch = new HashMap<>();
            fetchedOffsets = new HashMap<>();
            globalResult = new CompletableFuture<>();

            this.timestampsToSearch = timestampsToSearch;
            this.requireTimestamps = requireTimestamps;
            this.offsetFetcherUtils = offsetFetcherUtils;
            this.isolationLevel = isolationLevel;
            this.oneShot = oneShot;
        }

        private void addPartitionsToRetry(Set<TopicPartition> partitionsToRetry) {
            remainingToSearch.putAll(partitionsToRetry.stream()
                    .collect(Collectors.toMap(tp -> tp, timestampsToSearch::get)));
        }

        /**
         * Completes {@link #globalResult} with the offsets retrieved so far, reporting any partitions that
         * are still outstanding as partitions to retry.
         */
        private void completeWithResultSoFar() {
            globalResult.complete(new ListOffsetResult(fetchedOffsets, remainingToSearch.keySet()));
        }
    }

    private static class ResetPositionsRequestState {
        private final Map<TopicPartition, AutoOffsetResetStrategy> strategies;
        private final Map<TopicPartition, Long> remainingToSearch;
        private final Predicate<TopicPartition> scope;
        private final CompletableFuture<Void> result = new CompletableFuture<>();
        private CompletableFuture<Void> attemptResult = CompletableFuture.completedFuture(null);
        private final AtomicInteger expectedResponses = new AtomicInteger();
        private boolean awaitingMetadata;
        private boolean metadataUpdatedWhileInFlight;
        private boolean inFlight;
        private long nextRetryMs;

        private ResetPositionsRequestState(Map<TopicPartition, AutoOffsetResetStrategy> strategies,
                                           Predicate<TopicPartition> scope) {
            this.strategies = Map.copyOf(strategies);
            this.scope = scope;
            this.remainingToSearch = strategies.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey, entry -> entry.getValue().timestamp().orElseThrow()));
        }

        private void addPartitionsToRetry(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                AutoOffsetResetStrategy strategy = strategies.get(partition);
                if (strategy != null)
                    remainingToSearch.put(partition, strategy.timestamp().orElseThrow());
            }
        }

        private boolean owns(TopicPartition partition, AutoOffsetResetStrategy strategy) {
            return !result.isDone() && Objects.equals(strategies.get(partition), strategy) && scope.test(partition);
        }

        private boolean hasInvalidRemaining() {
            return remainingToSearch.keySet().stream().anyMatch(scope.negate());
        }
    }

    private static class ValidationRequestState {
        private final Map<TopicPartition, SubscriptionState.FetchPosition> positions;
        private final Map<TopicPartition, SubscriptionState.FetchPosition> remaining;
        private final Predicate<TopicPartition> scope;
        private final AtomicInteger expectedResponses = new AtomicInteger();
        private boolean awaitingMetadata;
        private boolean metadataUpdatedWhileInFlight;
        private boolean inFlight;
        private long nextRetryMs;

        private ValidationRequestState(
                Map<TopicPartition, SubscriptionState.FetchPosition> positions,
                Predicate<TopicPartition> scope) {
            this.positions = Map.copyOf(positions);
            this.remaining = new HashMap<>(positions);
            this.scope = scope;
        }

        private boolean owns(TopicPartition partition, SubscriptionState.FetchPosition position) {
            return Objects.equals(positions.get(partition), position) && scope.test(partition);
        }

        private boolean hasInvalidRemaining() {
            return remaining.entrySet().stream().anyMatch(entry ->
                    !Objects.equals(positions.get(entry.getKey()), entry.getValue()) || !scope.test(entry.getKey()));
        }

        private void addPartitionsToRetry(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                SubscriptionState.FetchPosition position = positions.get(partition);
                if (position != null)
                    remaining.put(partition, position);
            }
        }
    }

    private static class MultiNodeRequest {
        final Map<TopicPartition, ListOffsetData> fetchedTimestampOffsets;
        final Set<TopicPartition> partitionsToRetry;
        final AtomicInteger expectedResponses;
        final CompletableFuture<ListOffsetResult> resultFuture;

        private MultiNodeRequest(int nodeCount) {
            fetchedTimestampOffsets = new HashMap<>();
            partitionsToRetry = new HashSet<>();
            expectedResponses = new AtomicInteger(nodeCount);
            resultFuture = new CompletableFuture<>();
        }

        private void onComplete(BiConsumer<? super ListOffsetResult, ? super Throwable> action) {
            resultFuture.whenComplete(action);
        }

        private void addPartialResult(ListOffsetResult partialResult) {
            try {
                fetchedTimestampOffsets.putAll(partialResult.fetchedOffsets);
                partitionsToRetry.addAll(partialResult.partitionsToRetry);

                if (expectedResponses.decrementAndGet() == 0) {
                    ListOffsetResult result =
                            new ListOffsetResult(fetchedTimestampOffsets,
                                    partitionsToRetry);
                    resultFuture.complete(result);
                }
            } catch (RuntimeException e) {
                resultFuture.completeExceptionally(e);
            }
        }
    }

    /**
     * Group partitions by leader. Topic partitions from `timestampsToSearch` for which
     * the leader is not known are kept as `remainingToSearch` in the `listOffsetsRequestState`
     *
     * @param timestampsToSearch      The mapping from partitions to the target timestamps
     * @param listOffsetsRequestState Optional request state that will be extended by adding to its
     *                                `remainingToSearch` map all partitions for which the
     *                                request cannot be performed due to unknown leader (need
     *                                metadata update).
     */
    private Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> groupListOffsetRequests(
            final Map<TopicPartition, Long> timestampsToSearch,
            final Optional<Map<TopicPartition, Long>> remainingToSearch) {
        final Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> partitionDataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            TopicPartition tp = entry.getKey();
            Long offset = entry.getValue();
            Metadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);

            if (leaderAndEpoch.leader.isEmpty()) {
                log.debug("Leader for partition {} is unknown for fetching offset {}", tp, offset);
                metadata.requestUpdate(true);
                remainingToSearch.ifPresent(remaining -> remaining.put(tp, offset));
            } else {
                int currentLeaderEpoch = leaderAndEpoch.epoch.orElse(ListOffsetsResponse.UNKNOWN_EPOCH);
                partitionDataMap.put(tp, new ListOffsetsRequestData.ListOffsetsPartition()
                        .setPartitionIndex(tp.partition())
                        .setTimestamp(offset)
                        .setCurrentLeaderEpoch(currentLeaderEpoch));
            }
        }
        Set<TopicPartition> partitionsSkippedInRegroup = new HashSet<>();
        Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> result =
                offsetFetcherUtils.regroupPartitionMapByNode(partitionDataMap, partitionsSkippedInRegroup);
        if (!partitionsSkippedInRegroup.isEmpty()) {
            metadata.requestUpdate(false);
            remainingToSearch.ifPresent(remaining ->
                    partitionsSkippedInRegroup.forEach(tp ->
                            remaining.put(tp, timestampsToSearch.get(tp))));
        }
        return result;
    }

    // Visible for testing
    int requestsToRetry() {
        return requestsToRetry.size();
    }

    // Visible for testing
    int requestsToSend() {
        return requestsToSend.size();
    }

    // Visible for testing
    int resetRequests() {
        return resetRequests.size();
    }

    // Visible for testing
    int validationRequests() {
        return validationRequests.size();
    }
}
