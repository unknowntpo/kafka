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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.clients.consumer.internals.events.CreateFetchRequestsEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.LogContext;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * {@code FetchRequestManager} is responsible for generating {@link FetchRequest} that represent the
 * {@link SubscriptionState#fetchablePartitions(Predicate)} based on the user's topic subscription/partition
 * assignment.
 */
public class FetchRequestManager extends AbstractFetch implements RequestManager {

    private final NetworkClientDelegate networkClientDelegate;
    private final long retryBackoffMs;
    private CompletableFuture<Void> pendingFetchRequestFuture;
    // Network-owner handoffs, drained only up to the boundary captured by onPollBatchStart().
    private final ArrayDeque<FetchContinuation> continuations = new ArrayDeque<>();
    // Local demand/continuation changes activate this manager; network wakeup alone does not.
    private final NextPollCondition.Signal inputChanged = new NextPollCondition.Signal();
    private int continuationBatchSize = -1;
    private boolean continuationsClosed;
    private final List<PendingFetchDemand> reconnectDemands = new ArrayList<>();
    private long reconnectDeadlineMs = Long.MAX_VALUE;

    private static final class PendingFetchDemand {
        private final long deadlineMs;
        private final BooleanSupplier canFetch;
        private boolean attempted;
        private final CompletableFuture<Void> future = new CompletableFuture<>();

        private PendingFetchDemand(long deadlineMs, BooleanSupplier canFetch) {
            this.deadlineMs = deadlineMs;
            this.canFetch = canFetch;
        }
    }

    /**
     * Prepare requests, retaining work skipped by transport reconnect backoff until its actual
     * deadline. Completion still describes preparation, not receipt of records. The no-argument
     * createFetchRequests method remains a single attempt, including prefetch and close callers.
     * The first attempt is preserved even for poll(Duration.ZERO); the deadline bounds retries.
     * This does not yet retain work blocked on metadata, positions or buffer capacity.
     */
    public CompletableFuture<Void> createFetchRequestsWithReconnect(long deadlineMs, BooleanSupplier canFetch) {
        PendingFetchDemand demand = new PendingFetchDemand(deadlineMs, canFetch);
        if (continuationsClosed) {
            demand.future.completeExceptionally(new KafkaException("Consumer closed before fetch preparation"));
        } else {
            reconnectDemands.add(demand);
            inputChanged.publish();
        }
        return demand.future;
    }


    /** Network-thread-confined operation handoff; the owner rechecks permission in advance. */
    public interface FetchContinuation {
        void advance();
        void onClose();
    }

    public void enqueueFetchContinuation(FetchContinuation continuation) {
        if (continuationsClosed) {
            continuation.onClose();
            return;
        }
        continuations.addLast(continuation);
        inputChanged.publish();
    }

    /** Recheck retained work after an application poll loses permission, on the network owner. */
    public void onPollDemandChanged() {
        if (!continuationsClosed && (!continuations.isEmpty() || !reconnectDemands.isEmpty()))
            inputChanged.publish();
    }

    @Override
    public void onPollBatchStart() {
        continuationBatchSize = continuations.size();
    }

    public void closeFetchContinuations() {
        continuationsClosed = true;
        List<PendingFetchDemand> closingDemands = new ArrayList<>(reconnectDemands);
        reconnectDemands.clear();
        closingDemands.forEach(demand -> demand.future.completeExceptionally(
                new KafkaException("Consumer closed before fetch preparation")));
        while (!continuations.isEmpty())
            continuations.removeFirst().onClose();
    }

    FetchRequestManager(final LogContext logContext,
                        final Time time,
                        final ConsumerMetadata metadata,
                        final SubscriptionState subscriptions,
                        final FetchConfig fetchConfig,
                        final FetchBuffer fetchBuffer,
                        final FetchMetricsManager metricsManager,
                        final NetworkClientDelegate networkClientDelegate,
                        final ApiVersions apiVersions,
                        final long retryBackoffMs) {
        super(logContext, metadata, subscriptions, fetchConfig, fetchBuffer, metricsManager, time, apiVersions);
        this.networkClientDelegate = networkClientDelegate;
        this.retryBackoffMs = retryBackoffMs;
    }

    @Override
    protected boolean isUnavailable(Node node) {
        boolean unavailable = networkClientDelegate.isUnavailable(node);
        if (unavailable) {
            long nowMs = time.milliseconds();
            long delayMs = networkClientDelegate.connectionDelay(node, nowMs);
            long deadlineMs = nowMs > Long.MAX_VALUE - delayMs ? Long.MAX_VALUE : nowMs + delayMs;
            reconnectDeadlineMs = Math.min(reconnectDeadlineMs, deadlineMs);
        }
        return unavailable;
    }

    @Override
    protected void maybeThrowAuthFailure(Node node) {
        networkClientDelegate.maybeThrowAuthFailure(node);
    }

    /**
     * {@inheritDoc}
     *
     * If any request is in flight, its completion will wake the application thread regardless of the outcome, so
     * no separate bound is needed. Otherwise, the application thread's wait is bounded by {@code retryBackoffMs}
     * so it can re-evaluate subscription state changes promptly.
     */
    @Override
    public long maximumTimeToWait(long currentTimeMs) {
        return nodesWithPendingFetchRequests.isEmpty() ? retryBackoffMs : Long.MAX_VALUE;
    }

    /**
     * Signals the {@link Consumer} wants requests be created for the broker nodes to fetch the next
     * batch of records.
     *
     * @see CreateFetchRequestsEvent
     * @return Future on which the caller can wait to ensure that the requests have been created
     */
    public CompletableFuture<Void> createFetchRequests() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (pendingFetchRequestFuture != null) {
            // In this case, we have an outstanding fetch request, so chain the newly created future to be
            // completed when the "pending" future is completed.
            pendingFetchRequestFuture.whenComplete((value, exception) -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    future.complete(value);
                }
            });
        } else {
            pendingFetchRequestFuture = future;
        }

        inputChanged.publish();
        return future;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PollResult poll(long currentTimeMs) {
        int batch = continuationBatchSize < 0 ? continuations.size() : continuationBatchSize;
        continuationBatchSize = -1;
        for (int i = 0; i < batch && !continuations.isEmpty(); i++)
            continuations.removeFirst().advance();
        NextPollCondition input = inputChanged.await();
        reconnectDeadlineMs = Long.MAX_VALUE;
        PollResult result = pollInternal(
            this::prepareFetchRequests,
            this::handleFetchSuccess,
            this::handleFetchFailure
        );
        // Publications while executing this batch must survive condition registration.
        if (!continuations.isEmpty())
            inputChanged.publish();
        if (!reconnectDemands.isEmpty()) {
            long nowMs = time.milliseconds();
            long nextDelayMs = Math.max(0, reconnectDeadlineMs - nowMs);
            for (PendingFetchDemand demand : reconnectDemands)
                nextDelayMs = Math.min(nextDelayMs, Math.max(0, demand.deadlineMs - nowMs));
            input = NextPollCondition.anyOf(input, NextPollCondition.after(nowMs, nextDelayMs));
        }
        return new PollResult(result.timeUntilNextPollMs, result.unsentRequests, input);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PollResult pollOnClose(long currentTimeMs) {
        closeFetchContinuations();
        // There needs to be a pending fetch request for pollInternal to create the requests.
        createFetchRequests();

        // TODO: move the logic to poll to handle signal close
        return pollInternal(
                this::prepareCloseFetchSessionRequests,
                this::handleCloseFetchSessionSuccess,
                this::handleCloseFetchSessionFailure
        );
    }

    /**
     * Creates the {@link PollResult poll result} that contains a list of zero or more
     * {@link FetchRequest.Builder fetch requests}.
     *
     * @param fetchRequestPreparer {@link FetchRequestPreparer} to generate a {@link FetchRequestPreparationResult}
     *                             mapping {@link Node nodes} to their {@link FetchSessionHandler.FetchRequestData}
     * @param successHandler       {@link ResponseHandler Handler for successful responses}
     * @param errorHandler         {@link ResponseHandler Handler for failure responses}
     * @return {@link PollResult}
     */
    private PollResult pollInternal(FetchRequestPreparer fetchRequestPreparer,
                                    ResponseHandler<ClientResponse> successHandler,
                                    ResponseHandler<Throwable> errorHandler) {
        CompletableFuture<Void> currentFetchRequestFuture = pendingFetchRequestFuture;
        pendingFetchRequestFuture = null;
        List<PendingFetchDemand> currentDemands = takeActiveDemands();
        if (currentFetchRequestFuture == null && currentDemands.isEmpty()) {
            // If no explicit request for creating fetch requests was issued, just short-circuit.
            return PollResult.EMPTY;
        }

        try {
            FetchRequestPreparationResult result = fetchRequestPreparer.prepare();
            Map<Node, FetchSessionHandler.FetchRequestData> fetchRequests = result.requests();

            if (fetchRequests.isEmpty()) {
                if (result.canWakeBufferIfNoFetchRequestsToSend()) {
                    // If there's nothing to fetch because every fetchable partition already has buffered data,
                    // wake up the FetchBuffer so it doesn't needlessly wait for a wakeup that won't come until
                    // the data in the fetch buffer is consumed.
                    fetchBuffer.wakeup();
                }
                if (currentFetchRequestFuture != null)
                    currentFetchRequestFuture.complete(null);
                finishFetchDemands(currentDemands);
                return PollResult.EMPTY;
            }

            List<UnsentRequest> requests = fetchRequests.entrySet().stream().map(entry -> {
                final Node fetchTarget = entry.getKey();
                final FetchSessionHandler.FetchRequestData data = entry.getValue();
                final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);
                final BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                    if (error != null)
                        errorHandler.handle(fetchTarget, data, error);
                    else
                        successHandler.handle(fetchTarget, data, clientResponse);
                };

                return new UnsentRequest(request, Optional.of(fetchTarget)).whenComplete(responseHandler);
            }).collect(Collectors.toList());

            if (currentFetchRequestFuture != null)
                currentFetchRequestFuture.complete(null);
            finishFetchDemands(currentDemands);
            return new PollResult(requests);
        } catch (Throwable t) {
            // A "dummy" poll result is returned here rather than rethrowing the error because any error
            // that is thrown from any RequestManager.poll() method interrupts the polling of the other
            // request managers.
            if (currentFetchRequestFuture != null)
                currentFetchRequestFuture.completeExceptionally(t);
            currentDemands.forEach(demand -> demand.future.completeExceptionally(t));
            return PollResult.EMPTY;
        }
    }

    private List<PendingFetchDemand> takeActiveDemands() {
        List<PendingFetchDemand> currentDemands = reconnectDemands.isEmpty()
                ? List.of() : new ArrayList<>(reconnectDemands);
        reconnectDemands.clear();
        if (!currentDemands.isEmpty())
            currentDemands.removeIf(demand -> {
                if (continuationsClosed)
                    demand.future.completeExceptionally(new KafkaException("Consumer closed before fetch preparation"));
                else if (!demand.canFetch.getAsBoolean())
                    demand.future.complete(null);
                if (demand.attempted && time.milliseconds() >= demand.deadlineMs)
                    demand.future.completeExceptionally(new TimeoutException("Fetch preparation deadline expired"));
                demand.attempted = true;
                return demand.future.isDone();
            });
        return currentDemands;
    }

    @Override
    protected void closeInternal(Timer timer) {
        closeFetchContinuations();
        super.closeInternal(timer);
    }

    private void finishFetchDemands(List<PendingFetchDemand> demands) {
        for (PendingFetchDemand demand : demands) {
            if (demand.future.isDone())
                continue;
            if (continuationsClosed)
                demand.future.completeExceptionally(new KafkaException("Consumer closed before fetch preparation"));
            else if (reconnectDeadlineMs != Long.MAX_VALUE) {
                if (time.milliseconds() >= demand.deadlineMs)
                    demand.future.completeExceptionally(new TimeoutException("Fetch preparation deadline expired"));
                else
                    reconnectDemands.add(demand);
            } else
                demand.future.complete(null);
        }
    }

    /**
     * Simple functional interface to all passing in a method reference for improved readability.
     */
    @FunctionalInterface
    protected interface FetchRequestPreparer {

        FetchRequestPreparationResult prepare();
    }
}
