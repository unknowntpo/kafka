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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
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
import java.util.function.BooleanSupplier;
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
    private final Set<PendingFetchCommittedRequest> committedPositionRequests = new HashSet<>();
    private final Set<PendingMetadataReset> positionResets = new HashSet<>();
    private final NextPollCondition.Signal requestsAvailable = new NextPollCondition.Signal();
    // Network-thread owned: metadata callbacks retry only these unresolved partitions.
    private final List<PendingMetadataReset> pendingMetadataResets = new ArrayList<>();
    private boolean positionResetsClosed;

    // Futures are published only after the response has updated SubscriptionState/error caches.
    private final Map<CompletableFuture<Void>, Map<TopicPartition, Object>> validationCompletions = new HashMap<>();
    private final List<PositionValidationWait> validationWaits = new ArrayList<>();
    private final Runnable apiVersionsChanged = this::onValidationDependencyChanged;
    private long validationConnectRetryTimeMs = Long.MAX_VALUE;

    private static final class PositionValidationWait {
        private final long deadlineMs;
        private final BooleanSupplier canProceed;
        private final CompletableFuture<Void> result = new CompletableFuture<>();
        private boolean ready;
        private boolean awaitingCompletion;
        private long retryAtMs = Long.MAX_VALUE;
        private Throwable error;
        private int attempt;
        private Map<TopicPartition, Object> positionStates;

        private PositionValidationWait(long deadlineMs, BooleanSupplier canProceed) {
            this.deadlineMs = deadlineMs;
            this.canProceed = canProceed;
        }
    }

    private static final class PendingMetadataReset {
        private final Map<TopicPartition, AutoOffsetResetStrategy> strategies;
        private final CompletableFuture<Void> result = new CompletableFuture<>();
        private final Map<TopicPartition, Object> positionStates;
        private int outstandingRequests;

        private PendingMetadataReset(Map<TopicPartition, AutoOffsetResetStrategy> strategies,
                                     Map<TopicPartition, Object> positionStates) {
            this.strategies = new HashMap<>(strategies);
            this.positionStates = positionStates;
        }
    }

    public void closePendingPositionResets() {
        positionResetsClosed = true;
        apiVersions.removeChangeListener(apiVersionsChanged);
        for (PendingFetchCommittedRequest request : new ArrayList<>(committedPositionRequests))
            request.result.completeExceptionally(new KafkaException("Consumer closed while fetching committed positions"));
        pendingOffsetFetchEvent = null;
        for (PendingMetadataReset reset : new ArrayList<>(positionResets))
            reset.result.completeExceptionally(new KafkaException("Consumer closed while resetting positions"));
        for (PositionValidationWait wait : new ArrayList<>(validationWaits))
            wait.result.completeExceptionally(new KafkaException("Consumer closed during position validation"));
        for (CompletableFuture<Void> completion : new ArrayList<>(validationCompletions.keySet()))
            completion.completeExceptionally(new KafkaException("Consumer closed during position validation"));
        validationCompletions.clear();
        List<PendingMetadataReset> pending = new ArrayList<>(pendingMetadataResets);
        pendingMetadataResets.clear();
        for (PendingMetadataReset reset : pending)
            reset.result.completeExceptionally(new KafkaException("Consumer closed while awaiting offset metadata"));
    }

    private void enqueueRequests(List<NetworkClientDelegate.UnsentRequest> requests) {
        if (!requests.isEmpty()) {
            requestsToSend.addAll(requests);
            requestsAvailable.publish();
        }
    }

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
        this.apiVersions.addChangeListener(apiVersionsChanged);
    }

    private static class PendingFetchCommittedRequest {
        final Set<TopicPartition> requestedPartitions;
        final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result;
        final Map<TopicPartition, Object> positionStates;

        private PendingFetchCommittedRequest(final Set<TopicPartition> requestedPartitions,
                                             final Map<TopicPartition, Object> positionStates) {
            this.requestedPartitions = Objects.requireNonNull(requestedPartitions);
            this.positionStates = positionStates;
            this.result = new CompletableFuture<>();
        }
    }

    /**
     * Determine if there are pending fetch offsets requests to be sent and build a
     * {@link NetworkClientDelegate.PollResult}
     * containing it.
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        NextPollCondition input = requestsAvailable.await();
        retireObsoletePositionPreparations();
        completePositionValidationWaits();
        for (PositionValidationWait wait : validationWaits) {
            input = NextPollCondition.anyOf(input, NextPollCondition.after(currentTimeMs,
                    Math.max(0, wait.deadlineMs - currentTimeMs)));
            if (!wait.awaitingCompletion && wait.retryAtMs != Long.MAX_VALUE)
                input = NextPollCondition.anyOf(input, NextPollCondition.after(currentTimeMs,
                        Math.max(0, wait.retryAtMs - currentTimeMs)));
        }
        // Copy the outgoing request list and clear it.
        List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>(requestsToSend);
        requestsToSend.clear();
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, unsentRequests, input);
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

    /**
     * Like updateFetchPositions, but retains validation demand across dependency waits and RPCs.
     * A response first updates positions/errors, then activates this owner to recheck before
     * handing work to Fetch. Metadata/API-version publication activates the owner without an app
     * recheck; only actual retry eligibility and the operation's API deadline register timers.
     */
    public CompletableFuture<Void> updateFetchPositionsAndAwaitValidation(long deadlineMs, BooleanSupplier canProceed) {
        if (positionResetsClosed)
            return CompletableFuture.failedFuture(new KafkaException("Consumer closed during position validation"));
        if (!canProceed.getAsBoolean())
            return CompletableFuture.completedFuture(null);
        CompletableFuture<Void> preparation = updateFetchPositions(deadlineMs);
        // Preserve the initial nonblocking poll(0) attempt, but never retain an expired wait.
        if (preparation.isCompletedExceptionally() || !subscriptionState.hasPendingValidation()
                || subscriptionState.hasAllFetchPositions() || time.milliseconds() >= deadlineMs)
            return preparation;
        PositionValidationWait wait = new PositionValidationWait(deadlineMs, canProceed);
        validationWaits.add(wait);
        wait.result.whenComplete((ignored, error) -> validationWaits.remove(wait));
        awaitValidationBatch(wait, preparation);
        requestsAvailable.publish();
        return wait.result;
    }

    private void awaitValidationBatch(PositionValidationWait wait, CompletableFuture<Void> preparation) {
        wait.awaitingCompletion = true;
        int attempt = ++wait.attempt;
        wait.positionStates = positionStateSnapshot(subscriptionState.assignedPartitions());
        if (validationCompletions.isEmpty()) {
            boolean preparationPending = !preparation.isDone();
            long connectRetryTimeMs = validationConnectRetryTimeMs;
            preparation.whenComplete((ignored, error) -> {
                if (wait.result.isDone() || wait.attempt != attempt)
                    return;
                wait.awaitingCompletion = false;
                if (!samePositionStates(wait.positionStates)) {
                    wait.error = error;
                    wait.ready = true;
                    requestsAvailable.publish();
                    return;
                }
                if (!wait.canProceed.getAsBoolean())
                    wait.result.complete(null);
                else if (time.milliseconds() >= wait.deadlineMs)
                    wait.result.completeExceptionally(new TimeoutException("Position validation deadline expired"));
                else if (error != null)
                    wait.result.completeExceptionally(error);
                else if (subscriptionState.hasPendingValidation()) {
                    long nowMs = time.milliseconds();
                    wait.retryAtMs = Math.min(subscriptionState.nextValidationRetryTimeMs(nowMs), connectRetryTimeMs);
                    if (preparationPending) {
                        // Another partition's preparation may outlast this partition's backoff.
                        // Recheck once on completion, even if that eligibility time is now past.
                        wait.ready = true;
                        requestsAvailable.publish();
                    }
                } else
                    wait.result.complete(null);
            });
            return;
        }
        List<CompletableFuture<Void>> batch = new ArrayList<>(validationCompletions.keySet());
        batch.add(preparation);
        CompletableFuture.allOf(batch.toArray(new CompletableFuture<?>[0])).whenComplete((ignored, error) -> {
            if (wait.result.isDone() || wait.attempt != attempt)
                return;
            wait.awaitingCompletion = false;
            wait.error = error;
            wait.ready = true;
            requestsAvailable.publish();
        });
    }

    private void completePositionValidationWaits() {
        if (validationWaits.isEmpty())
            return;
        retireObsoleteValidationRequests();
        for (PositionValidationWait wait : new ArrayList<>(validationWaits)) {
            if (wait.ready && !samePositionStates(wait.positionStates)) {
                // The old batch may still finish, but it no longer owns this retained demand.
                wait.attempt++;
                wait.awaitingCompletion = false;
            }
            if (!wait.canProceed.getAsBoolean())
                wait.result.complete(null);
            else if (time.milliseconds() >= wait.deadlineMs)
                wait.result.completeExceptionally(new TimeoutException("Position validation deadline expired"));
            else if (!wait.awaitingCompletion && (wait.ready || time.milliseconds() >= wait.retryAtMs)) {
                wait.ready = false;
                wait.retryAtMs = Long.MAX_VALUE;
                if (wait.error != null)
                    wait.result.completeExceptionally(wait.error);
                else
                    awaitValidationBatch(wait, updateFetchPositions(wait.deadlineMs));
            }
        }
    }

    private Map<TopicPartition, Object> positionStateSnapshot(Set<TopicPartition> partitions) {
        Map<TopicPartition, Object> result = new HashMap<>();
        for (TopicPartition partition : partitions)
            result.put(partition, subscriptionState.positionStateToken(partition));
        return result;
    }

    private boolean isCurrentPositionState(TopicPartition partition, Map<TopicPartition, Object> snapshot) {
        return snapshot.containsKey(partition)
                && snapshot.get(partition) == subscriptionState.positionStateToken(partition);
    }

    private Set<TopicPartition> currentPositionStates(Map<TopicPartition, Object> snapshot) {
        return snapshot.keySet().stream().filter(partition -> isCurrentPositionState(partition, snapshot))
                .collect(Collectors.toSet());
    }

    private boolean samePositionStates(Map<TopicPartition, Object> snapshot) {
        return snapshot.keySet().equals(subscriptionState.assignedPartitions())
                && snapshot.keySet().stream().allMatch(partition -> isCurrentPositionState(partition, snapshot));
    }

    private void retireObsoletePositionPreparations() {
        for (PendingMetadataReset reset : new ArrayList<>(positionResets)) {
            reset.strategies.keySet().removeIf(partition -> !isCurrentPositionState(partition, reset.positionStates));
            if (currentPositionStates(reset.positionStates).isEmpty()) {
                pendingMetadataResets.remove(reset);
                reset.result.complete(null);
            }
        }
        for (PendingFetchCommittedRequest request : new ArrayList<>(committedPositionRequests)) {
            if (currentPositionStates(request.positionStates).isEmpty()) {
                if (pendingOffsetFetchEvent == request)
                    pendingOffsetFetchEvent = null;
                request.result.complete(Collections.emptyMap());
            }
        }
    }

    private void retireObsoleteValidationRequests() {
        for (Map.Entry<CompletableFuture<Void>, Map<TopicPartition, Object>> entry :
                new ArrayList<>(validationCompletions.entrySet())) {
            if (entry.getValue().keySet().stream().noneMatch(partition -> isCurrentPositionState(partition, entry.getValue()))) {
                validationCompletions.remove(entry.getKey());
                entry.getKey().complete(null);
            }
        }
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
        final Map<TopicPartition, Object> initialStates = positionStateSnapshot(initializingPartitions);
        if (commitRequestManager != null) {
            CompletableFuture<Void> refreshWithCommittedOffsets = initWithCommittedOffsetsIfNeeded(initializingPartitions, deadlineMs);

            // Reset positions for all partitions that may still require it (or that are awaiting reset)
            updatePositions = refreshWithCommittedOffsets.thenCompose(__ -> initWithPartitionOffsetsIfNeeded(
                    currentPositionStates(initialStates)));

        } else {
            updatePositions = initWithPartitionOffsetsIfNeeded(initializingPartitions);
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
     * @param initializingPartitions Set of partitions that should be initialized. This won't reset positions for
     *                               partitions that may have been added to the subscription state, but that are not
     *                               included in this set.
     * @return Future that will complete when the reset operation completes retrieving the offsets and setting
     * positions in the subscription state using them.
     * @throws NoOffsetForPartitionException If no reset strategy is configured.
     */
    private CompletableFuture<Void> initWithPartitionOffsetsIfNeeded(Set<TopicPartition> initializingPartitions) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            // Mark partitions that need reset, using the configured reset strategy. If no
            // strategy is defined, this will raise a NoOffsetForPartitionException exception.
            subscriptionState.resetInitializingPositions(initializingPartitions::contains);
        } catch (Exception e) {
            result.completeExceptionally(e);
            return result;
        }

        // For partitions awaiting reset, generate a ListOffset request to retrieve the partition
        // offsets according to the strategy (ex. earliest, latest), and update the positions.
        return resetPositionsIfNeeded();
    }

    /**
     * Fetch the committed offsets for partitions that require initialization. This will trigger an OffsetFetch
     * request and update positions in the subscription state once a response is received.
     *
     * @param initializingPartitions Set of partitions to update with a position. This same set will be kept
     *                               throughout the whole process (considered when fetching committed offsets, and
     *                               when resetting positions for partitions that may not have committed offsets).
     * @param deadlineMs             Deadline of the application event that triggered this operation. Used to
     *                               determine how much time to allow for the reused offset fetch to complete.
     * @throws TimeoutException If offsets could not be retrieved within the timeout
     */
    private CompletableFuture<Void> initWithCommittedOffsetsIfNeeded(Set<TopicPartition> initializingPartitions,
                                                                     long deadlineMs) {
        if (initializingPartitions.isEmpty())
            return CompletableFuture.completedFuture(null);
        if (canReusePendingOffsetFetchEvent(initializingPartitions))
            return pendingOffsetFetchEvent.result.thenApply(ignored -> null);

        PendingFetchCommittedRequest request = new PendingFetchCommittedRequest(initializingPartitions,
                positionStateSnapshot(initializingPartitions));
        pendingOffsetFetchEvent = request;
        committedPositionRequests.add(request);
        request.result.whenComplete((ignored, error) -> committedPositionRequests.remove(request));
        long fetchCommittedDeadlineMs = Math.max(deadlineMs, time.milliseconds() + defaultApiTimeoutMs);
        try {
            commitRequestManager.fetchOffsets(initializingPartitions, fetchCommittedDeadlineMs,
                    Collections.unmodifiableMap(request.positionStates)).whenComplete((fetched, error) ->
                            completeCommittedPositionRequest(request, fetched, error, deadlineMs));
        } catch (Throwable error) {
            completeCommittedPositionRequest(request, null, error, deadlineMs);
        }
        return request.result.thenApply(ignored -> null);
    }

    private void completeCommittedPositionRequest(PendingFetchCommittedRequest request,
                                                  CommitRequestManager.OffsetFetchResult fetched,
                                                  Throwable error,
                                                  long deadlineMs) {
        if (request.result.isDone())
            return;
        if (pendingOffsetFetchEvent == request)
            pendingOffsetFetchEvent = null;
        Set<TopicPartition> current = currentPositionStates(request.positionStates);
        if (error != null && !current.isEmpty() && !current.equals(request.requestedPartitions)) {
            // OffsetFetch may aggregate a removed partition's authorization error. Obtain a
            // result for the still-current subset before initializing or resetting any of it.
            initWithCommittedOffsetsIfNeeded(current, deadlineMs).whenComplete((ignored, failure) -> {
                if (failure == null)
                    request.result.complete(Collections.emptyMap());
                else
                    request.result.completeExceptionally(failure);
            });
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        if (error == null) {
            offsets.putAll(fetched.toOffsetMapWithNulls());
            offsets.keySet().retainAll(current);
        }
        CompletableFuture<Void> refreshed = new CompletableFuture<>();
        try {
            refreshOffsets(offsets, current.isEmpty() ? null : error, refreshed);
        } catch (Throwable failure) {
            refreshed.completeExceptionally(failure);
        }
        refreshed.whenComplete((ignored, failure) -> {
            if (failure == null)
                request.result.complete(offsets);
            else
                request.result.completeExceptionally(failure);
        });
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
                                final CompletableFuture<Void> result) {
        if (error == null) {

            // Ensure we only set positions for the partitions that still require one (ex. some partitions may have
            // been assigned a position manually)
            Map<TopicPartition, OffsetAndMetadata> offsetsToApply = offsetsForInitializingPartitions(offsets);

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
    private Map<TopicPartition, OffsetAndMetadata> offsetsForInitializingPartitions(Map<TopicPartition, OffsetAndMetadata> offsets) {
        Set<TopicPartition> currentlyInitializingPartitions = subscriptionState.initializingPartitions();
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
        offsets.forEach((key, value) -> {
            if (currentlyInitializingPartitions.contains(key)) {
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

        return pendingOffsetFetchEvent.requestedPartitions.equals(partitions)
                && currentPositionStates(pendingOffsetFetchEvent.positionStates).equals(partitions);
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

        return sendListOffsetsRequestsAndResetPositions(partitionAutoOffsetResetStrategyMap);
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
        validationConnectRetryTimeMs = Long.MAX_VALUE;
        Map<TopicPartition, SubscriptionState.FetchPosition> partitionsToValidate = offsetFetcherUtils.refreshAndGetPartitionsToValidate();
        if (partitionsToValidate.isEmpty()) {
            return;
        }

        sendOffsetsForLeaderEpochRequestsAndValidatePositions(partitionsToValidate);
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
            enqueueRequests(unsentRequests);
        } catch (StaleMetadataException e) {
            if (listOffsetsRequestState.oneShot)
                listOffsetsRequestState.completeWithResultSoFar();
            else
                requestsToRetry.add(listOffsetsRequestState);
        }
    }

    @Override
    public void onUpdate(ClusterResource clusterResource) {
        onValidationDependencyChanged();
        // Only metadata publication revisits these blocked resets; ordinary loop turns do not.
        List<PendingMetadataReset> resets = new ArrayList<>(pendingMetadataResets);
        pendingMetadataResets.clear();
        for (PendingMetadataReset reset : resets) {
            if (reset.result.isDone())
                continue;
            reset.strategies.entrySet().removeIf(entry ->
                    !isCurrentPositionState(entry.getKey(), reset.positionStates)
                    || !subscriptionState.isAssigned(entry.getKey())
                    || !subscriptionState.isOffsetResetNeeded(entry.getKey())
                    || !entry.getValue().equals(subscriptionState.resetStrategy(entry.getKey())));
            try {
                sendPositionResetRequests(reset);
            } catch (RuntimeException e) {
                reset.result.completeExceptionally(e);
            }
        }
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
    }

    /**
     * Called after an application event changes positions or assignment on the network thread.
     * The event mutation must be visible before notifying retained validation demand.
     */
    public void onPositionStateChanged() {
        onValidationDependencyChanged();
        if (!positionResetsClosed && (!positionResets.isEmpty() || !committedPositionRequests.isEmpty()))
            requestsAvailable.publish();
    }

    /** Network-thread notification only; validation/transport work runs in a later owner poll. */
    private void onValidationDependencyChanged() {
        if (positionResetsClosed || validationWaits.isEmpty())
            return;
        for (PositionValidationWait wait : validationWaits)
            wait.ready = true;
        requestsAvailable.publish();
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
                groupListOffsetRequests(timestampsToSearch, Optional.of(listOffsetsRequestState));
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
        return buildListOffsetRequestToNode(node, targetTimes, requireTimestamps, unsentRequests, null);
    }

    private CompletableFuture<ListOffsetResult> buildListOffsetRequestToNode(
            Node node,
            Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> targetTimes,
            boolean requireTimestamps,
            List<NetworkClientDelegate.UnsentRequest> unsentRequests,
            Map<TopicPartition, Object> positionStates) {
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
                    if (positionStates != null) {
                        var currentData = lor.data().duplicate();
                        currentData.topics().forEach(topic -> topic.partitions().removeIf(partition ->
                                !isCurrentPositionState(new TopicPartition(topic.name(), partition.partitionIndex()), positionStates)));
                        lor = new ListOffsetsResponse(currentData);
                    }
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
     * @return A {@link CompletableFuture} which completes when the requests are
     * complete.
     */
    private CompletableFuture<Void> sendListOffsetsRequestsAndResetPositions(
            final Map<TopicPartition, AutoOffsetResetStrategy> partitionAutoOffsetResetStrategyMap) {
        if (positionResetsClosed)
            return CompletableFuture.failedFuture(new KafkaException("Consumer closed while resetting positions"));
        for (PendingMetadataReset pending : pendingMetadataResets) {
            if (!pending.result.isDone() && pending.strategies.equals(partitionAutoOffsetResetStrategyMap)
                    && currentPositionStates(pending.positionStates).containsAll(partitionAutoOffsetResetStrategyMap.keySet()))
                return pending.result;
        }
        PendingMetadataReset reset = new PendingMetadataReset(partitionAutoOffsetResetStrategyMap,
                positionStateSnapshot(partitionAutoOffsetResetStrategyMap.keySet()));
        positionResets.add(reset);
        reset.result.whenComplete((ignored, error) -> {
            positionResets.remove(reset);
            pendingMetadataResets.remove(reset);
        });
        sendPositionResetRequests(reset);
        return reset.result;
    }

    private void sendPositionResetRequests(PendingMetadataReset reset) {
        Map<TopicPartition, AutoOffsetResetStrategy> partitionAutoOffsetResetStrategyMap = new HashMap<>(reset.strategies);
        Map<TopicPartition, Long> timestampsToSearch = partitionAutoOffsetResetStrategyMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().timestamp().get()));
        Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(timestampsToSearch, Optional.empty());

        timestampsToSearchByNode.values().forEach(partitions -> partitions.keySet().forEach(reset.strategies::remove));
        if (!reset.strategies.isEmpty())
            pendingMetadataResets.add(reset);
        reset.outstandingRequests += timestampsToSearchByNode.size();
        final List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();

        timestampsToSearchByNode.forEach((node, resetTimestamps) -> {
            subscriptionState.setNextAllowedRetry(resetTimestamps.keySet(),
                    time.milliseconds() + requestTimeoutMs);

            CompletableFuture<ListOffsetResult> partialResult = buildListOffsetRequestToNode(
                    node,
                    resetTimestamps,
                    false,
                    unsentRequests,
                    reset.positionStates);

            partialResult.whenComplete((result, error) -> completePositionResetRequest(reset,
                    resetTimestamps, partitionAutoOffsetResetStrategyMap, result, error));
        });

        enqueueRequests(unsentRequests);
        completePositionResetIfFinished(reset);
    }

    private void completePositionResetRequest(PendingMetadataReset reset,
                                              Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> resetTimestamps,
                                              Map<TopicPartition, AutoOffsetResetStrategy> strategies,
                                              ListOffsetResult result,
                                              Throwable error) {
        if (reset.result.isDone())
            return;
        Set<TopicPartition> current = currentPositionStates(reset.positionStates);
        current.retainAll(resetTimestamps.keySet());
        try {
            if (!current.isEmpty()) {
                if (error == null) {
                    var fetched = new HashMap<>(result.fetchedOffsets);
                    fetched.keySet().retainAll(current);
                    Set<TopicPartition> retries = new HashSet<>(result.partitionsToRetry);
                    retries.retainAll(current);
                    offsetFetcherUtils.onSuccessfulResponseForResettingPositions(
                            new ListOffsetResult(fetched, retries), strategies);
                } else {
                    RuntimeException failure = error instanceof RuntimeException ? (RuntimeException) error
                            : new RuntimeException("Unexpected failure in ListOffsets request for resetting positions", error);
                    var currentRequests = new HashMap<>(resetTimestamps);
                    currentRequests.keySet().retainAll(current);
                    offsetFetcherUtils.onFailedResponseForResettingPositions(currentRequests, failure);
                    if (!(failure instanceof RetriableException)) {
                        pendingMetadataResets.remove(reset);
                        reset.strategies.clear();
                    }
                }
            }
            reset.outstandingRequests--;
            completePositionResetIfFinished(reset);
        } catch (Throwable failure) {
            reset.result.completeExceptionally(failure);
        }
    }

    private void completePositionResetIfFinished(PendingMetadataReset reset) {
        if (reset.strategies.isEmpty() && reset.outstandingRequests == 0)
            reset.result.complete(null);
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
    private void sendOffsetsForLeaderEpochRequestsAndValidatePositions(
            Map<TopicPartition, SubscriptionState.FetchPosition> partitionsToValidate) {

        final Map<Node, Map<TopicPartition, SubscriptionState.FetchPosition>> regrouped =
                regroupFetchPositionsByLeader(partitionsToValidate);

        long nextResetTimeMs = time.milliseconds() + requestTimeoutMs;
        final List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();
        regrouped.forEach((node, fetchPositions) -> {

            if (node.isEmpty()) {
                metadata.requestUpdate(true);
                return;
            }

            NodeApiVersions nodeApiVersions = apiVersions.get(node.idString());
            if (nodeApiVersions == null) {
                networkClientDelegate.maybeThrowAuthFailure(node);
                networkClientDelegate.tryConnect(node);
                if (networkClientDelegate.isUnavailable(node)) {
                    long nowMs = time.milliseconds();
                    long delayMs = networkClientDelegate.connectionDelay(node, nowMs);
                    long retryAtMs = nowMs > Long.MAX_VALUE - delayMs ? Long.MAX_VALUE : nowMs + delayMs;
                    validationConnectRetryTimeMs = Math.min(validationConnectRetryTimeMs, retryAtMs);
                }
                return;
            }

            if (!hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
                log.debug("Skipping validation of fetch offsets for partitions {} since the broker does not " +
                                "support the required protocol version (introduced in Kafka 2.3)",
                        fetchPositions.keySet());
                for (TopicPartition partition : fetchPositions.keySet()) {
                    subscriptionState.completeValidation(partition);
                }
                return;
            }

            subscriptionState.setNextAllowedRetry(fetchPositions.keySet(), nextResetTimeMs);

            CompletableFuture<OffsetsForLeaderEpochUtils.OffsetForEpochResult> partialResult =
                    buildOffsetsForLeaderEpochRequestToNode(node, fetchPositions, unsentRequests);

            CompletableFuture<Void> completion = new CompletableFuture<>();
            Map<TopicPartition, Object> requestStates = positionStateSnapshot(fetchPositions.keySet());
            validationCompletions.put(completion, requestStates);
            partialResult.whenComplete((offsetsResult, error) ->
                    completeValidationRequest(completion, requestStates, fetchPositions, offsetsResult, error));
        });

        enqueueRequests(unsentRequests);
    }

    private void completeValidationRequest(CompletableFuture<Void> completion,
                                           Map<TopicPartition, Object> requestStates,
                                           Map<TopicPartition, SubscriptionState.FetchPosition> fetchPositions,
                                           OffsetsForLeaderEpochUtils.OffsetForEpochResult offsetsResult,
                                           Throwable error) {
        if (completion.isDone())
            return;
        Map<TopicPartition, SubscriptionState.FetchPosition> currentPositions = new HashMap<>();
        fetchPositions.forEach((partition, position) -> {
            if (isCurrentPositionState(partition, requestStates))
                currentPositions.put(partition, position);
        });
        try {
            if (!currentPositions.isEmpty()) {
                if (error == null) {
                    var endOffsets = new HashMap<>(offsetsResult.endOffsets());
                    endOffsets.keySet().retainAll(currentPositions.keySet());
                    Set<TopicPartition> retries = new HashSet<>(offsetsResult.partitionsToRetry());
                    retries.retainAll(currentPositions.keySet());
                    offsetFetcherUtils.onSuccessfulResponseForValidatingPositions(currentPositions,
                            new OffsetsForLeaderEpochUtils.OffsetForEpochResult(endOffsets, retries));
                } else {
                    RuntimeException failure = error instanceof RuntimeException ? (RuntimeException) error
                            : new RuntimeException("Unexpected failure in OffsetsForLeaderEpoch request for validating positions", error);
                    offsetFetcherUtils.onFailedResponseForValidatingPositions(currentPositions, failure);
                }
            }
            // Obsolete responses must not mutate retry state or cache errors for a new seek.
            validationCompletions.remove(completion);
            completion.complete(null);
        } catch (Throwable failure) {
            validationCompletions.remove(completion);
            completion.completeExceptionally(failure);
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
        Map<TopicPartition, Object> requestStates = positionStateSnapshot(fetchPositions.keySet());
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
                    Map<TopicPartition, SubscriptionState.FetchPosition> currentPositions = new HashMap<>(fetchPositions);
                    currentPositions.keySet().removeIf(partition -> !isCurrentPositionState(partition, requestStates));
                    OffsetsForLeaderEpochUtils.OffsetForEpochResult listOffsetResult =
                            OffsetsForLeaderEpochUtils.handleResponse(currentPositions, offsetsForLeaderEpochResponse);
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
            final Optional<ListOffsetsRequestState> listOffsetsRequestState) {
        final Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> partitionDataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            TopicPartition tp = entry.getKey();
            Long offset = entry.getValue();
            Metadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);

            if (leaderAndEpoch.leader.isEmpty()) {
                log.debug("Leader for partition {} is unknown for fetching offset {}", tp, offset);
                metadata.requestUpdate(true);
                listOffsetsRequestState.ifPresent(offsetsRequestState -> offsetsRequestState.remainingToSearch.put(tp, offset));
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
            listOffsetsRequestState.ifPresent(state ->
                    partitionsSkippedInRegroup.forEach(tp ->
                            state.remainingToSearch.put(tp, timestampsToSearch.get(tp))));
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
}
