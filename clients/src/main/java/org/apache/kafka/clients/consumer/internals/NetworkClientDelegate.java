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
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.BootstrapResolutionException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.telemetry.internals.ClientTelemetrySender;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;

/**
 * A wrapper around the {@link org.apache.kafka.clients.NetworkClient} to handle network poll and send operations.
 */
public class NetworkClientDelegate implements AutoCloseable {

    private final KafkaClient client;
    private final BackgroundEventHandler backgroundEventHandler;
    private final Metadata metadata;
    private final Time time;
    private final Logger log;
    private final int requestTimeoutMs;
    private final Queue<UnsentRequest> unsentRequests;
    private final long retryBackoffMs;
    private Optional<Exception> metadataError;
    private final boolean notifyMetadataErrorsViaErrorQueue;
    private boolean bootstrapErrorPropagated = false;
    private final AsyncConsumerMetrics asyncConsumerMetrics;

    public NetworkClientDelegate(
            final Time time,
            final ConsumerConfig config,
            final LogContext logContext,
            final KafkaClient client,
            final Metadata metadata,
            final BackgroundEventHandler backgroundEventHandler,
            final boolean notifyMetadataErrorsViaErrorQueue,
            final AsyncConsumerMetrics asyncConsumerMetrics) {
        this.time = time;
        this.client = client;
        this.metadata = metadata;
        this.backgroundEventHandler = backgroundEventHandler;
        this.log = logContext.logger(getClass());
        this.unsentRequests = new ArrayDeque<>();
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.metadataError = Optional.empty();
        this.notifyMetadataErrorsViaErrorQueue = notifyMetadataErrorsViaErrorQueue;
        this.asyncConsumerMetrics = asyncConsumerMetrics;
    }

    // Visible for testing
    Queue<UnsentRequest> unsentRequests() {
        return unsentRequests;
    }

    public int inflightRequestCount() {
        return client.inFlightRequestCount();
    }

    /**
     * Check if the node is disconnected and unavailable for immediate reconnection (i.e. if it is in
     * reconnect backoff window following the disconnect).
     *
     * @param node {@link Node} to check for availability
     * @see NetworkClientUtils#isUnavailable(KafkaClient, Node, Time)
     */
    public boolean isUnavailable(Node node) {
        return NetworkClientUtils.isUnavailable(client, node, time);
    }

    /**
     * Return the remaining connection delay, including the client's exponential reconnect backoff.
     */
    public long connectionDelay(Node node, long currentTimeMs) {
        return client.connectionDelay(node, currentTimeMs);
    }

    /**
     * Checks for an authentication error on a given node and throws the exception if it exists.
     *
     * @param node {@link Node} to check for a previous {@link AuthenticationException}; if found it is thrown
     * @see NetworkClientUtils#maybeThrowAuthFailure(KafkaClient, Node)
     */
    public void maybeThrowAuthFailure(Node node) {
        NetworkClientUtils.maybeThrowAuthFailure(client, node);
    }

    /**
     * Initiate a connection if currently possible. This is only really useful for resetting
     * the failed status of a socket.
     *
     * @param node The node to connect to
     */
    public void tryConnect(Node node) {
        NetworkClientUtils.tryConnect(client, node, time);
    }

    /**
     * This method will try to send the unsent requests, poll for responses,
     * and check the disconnected nodes.
     *
     * @param timeoutMs     timeout time
     * @param currentTimeMs current time
     */
    public void poll(final long timeoutMs, final long currentTimeMs) {
        poll(timeoutMs, currentTimeMs, false);
    }

    /**
     * This method will try to send the unsent requests, poll for responses,
     * and check the disconnected nodes.
     *
     * @param timeoutMs     timeout time
     * @param currentTimeMs current time
     * @param onClose       True when the network thread is closing.
     */
    public void poll(final long timeoutMs, final long currentTimeMs, boolean onClose) {
        trySend(currentTimeMs);

        long pollTimeoutMs = timeoutMs;
        if (!unsentRequests.isEmpty()) {
            pollTimeoutMs = Math.min(retryBackoffMs, pollTimeoutMs);
        }
        this.client.poll(pollTimeoutMs, currentTimeMs);
        maybePropagateMetadataError();
        checkDisconnects(currentTimeMs, onClose);
        asyncConsumerMetrics.recordUnsentRequestsQueueSize(unsentRequests.size(), currentTimeMs);
    }

    private void maybePropagateMetadataError() {
        try {
            metadata.maybeThrowAnyException();
        } catch (BootstrapResolutionException e) {
            // Bootstrap failure is permanent and re-thrown on every check by Metadata;
            // only propagate it to the app thread once to avoid flooding the event queue.
            if (bootstrapErrorPropagated)
                return;
            bootstrapErrorPropagated = true;
            propagateMetadataError(e);
        } catch (Exception e) {
            propagateMetadataError(e);
        }
    }

    private void propagateMetadataError(Exception e) {
        if (notifyMetadataErrorsViaErrorQueue) {
            backgroundEventHandler.add(new ErrorEvent(e));
        } else {
            metadataError = Optional.of(e);
        }
    }

    /**
     * Return true if there is at least one in-flight request or unsent request.
     */
    public boolean hasAnyPendingRequests() {
        return client.hasInFlightRequests() || !unsentRequests.isEmpty();
    }

    /**
     * Tries to send the requests in the unsentRequest queue. If the request doesn't have an assigned node, it will
     * find the leastLoadedOne, and will be retried in the next {@code poll()}. If the request is expired, a
     * {@link TimeoutException} will be thrown.
     */
    private void trySend(final long currentTimeMs) {
        Iterator<UnsentRequest> iterator = unsentRequests.iterator();
        while (iterator.hasNext()) {
            UnsentRequest unsent = iterator.next();
            unsent.timer.update(currentTimeMs);
            if (unsent.timer.isExpired()) {
                iterator.remove();
                asyncConsumerMetrics.recordUnsentRequestsQueueTime(time.milliseconds() - unsent.enqueueTimeMs());
                unsent.handler.onFailure(currentTimeMs, new TimeoutException(
                    "Failed to send request after " + unsent.timer.timeoutMs() + " ms."));
                continue;
            }

            if (!doSend(unsent, currentTimeMs)) {
                // continue to retry until timeout.
                continue;
            }
            iterator.remove();
            asyncConsumerMetrics.recordUnsentRequestsQueueTime(time.milliseconds() - unsent.enqueueTimeMs());
        }
    }

    boolean doSend(final UnsentRequest r, final long currentTimeMs) {
        Node node = r.node.orElse(client.leastLoadedNode(currentTimeMs).node());
        if (node == null || nodeUnavailable(node)) {
            log.debug("No broker available to send the request: {}. Retrying.", r);
            return false;
        }
        ClientRequest request = makeClientRequest(r, node, currentTimeMs);
        if (!client.ready(node, currentTimeMs)) {
            // enqueue the request again if the node isn't ready yet. The request will be handled in the next iteration
            // of the event loop
            log.debug("Node is not ready, handle the request in the next event loop: node={}, request={}", node, r);
            return false;
        }
        client.send(request, currentTimeMs);
        return true;
    }

    protected void checkDisconnects(final long currentTimeMs, boolean onClose) {
        // Check the connection of the unsent request. Disconnect the disconnected node if it is unable to be connected.
        Iterator<UnsentRequest> iter = unsentRequests.iterator();
        while (iter.hasNext()) {
            UnsentRequest u = iter.next();
            if (u.node.isPresent() && client.connectionFailed(u.node.get())) {
                iter.remove();
                asyncConsumerMetrics.recordUnsentRequestsQueueTime(time.milliseconds() - u.enqueueTimeMs());
                AuthenticationException authenticationException = client.authenticationException(u.node.get());
                u.handler.onFailure(currentTimeMs, authenticationException);
            } else if (u.node.isEmpty() && onClose) {
                log.debug("Removing unsent request {} because the client is closing", u);
                iter.remove();
                asyncConsumerMetrics.recordUnsentRequestsQueueTime(time.milliseconds() - u.enqueueTimeMs());
                u.handler.onFailure(currentTimeMs, Errors.NETWORK_EXCEPTION.exception());
            }
        }
    }

    private ClientRequest makeClientRequest(
        final UnsentRequest unsent,
        final Node node,
        final long currentTimeMs
    ) {
        return client.newClientRequest(
            node.idString(),
            unsent.requestBuilder,
            currentTimeMs,
            true,
            (int) unsent.timer.remainingMs(),
            unsent.handler
        );
    }
    
    public Optional<Exception> getAndClearMetadataError() {
        Optional<Exception> metadataError = this.metadataError;
        this.metadataError = Optional.empty();
        return metadataError;
    }

    public Node leastLoadedNode() {
        return this.client.leastLoadedNode(time.milliseconds()).node();
    }

    public void wakeup() {
        client.wakeup();
    }

    /**
     * Check if the code is disconnected and unavailable for immediate reconnection (i.e. if it is in reconnect
     * backoff window following the disconnect).
     */
    public boolean nodeUnavailable(final Node node) {
        return client.connectionFailed(node) && client.connectionDelay(node, time.milliseconds()) > 0;
    }

    public void close() throws IOException {
        this.client.close();
    }

    public long addAll(PollResult pollResult) {
        Objects.requireNonNull(pollResult);
        for (NetworkCommand command : pollResult.networkCommands())
            add(command.transportRequest());
        return pollResult.nextPollCondition().delayMs();
    }

    public void addAll(final List<UnsentRequest> requests) {
        Objects.requireNonNull(requests);
        if (!requests.isEmpty()) {
            requests.forEach(this::add);
        }
    }

    public void add(final UnsentRequest r) {
        Objects.requireNonNull(r);
        r.setTimer(this.time, this.requestTimeoutMs);
        r.setEnqueueTimeMs(time.milliseconds());
        unsentRequests.add(r);
    }

    public static class PollResult {
        public static final long WAIT_FOREVER = Long.MAX_VALUE;
        public static final PollResult EMPTY = new PollResult(WAIT_FOREVER);
        /** Compatibility projection for manager producers that still return a numeric delay. */
        public final long timeUntilNextPollMs;
        /** Compatibility projection for manager producers and tests that still use the transport implementation. */
        public final List<UnsentRequest> unsentRequests;
        private final List<NetworkCommand> networkCommands;
        private final NextPollCondition nextPollCondition;
        /** Compatibility output retained until legacy state-transition producers migrate to manager events. */
        private final Set<StateTransition> stateTransitions;
        private final List<ManagerEvent> managerEvents;

        public PollResult(final long timeUntilNextPollMs, final List<UnsentRequest> unsentRequests) {
            this(timeUntilNextPollMs, unsentRequests, Set.of());
        }

        PollResult(final long timeUntilNextPollMs,
                   final List<UnsentRequest> unsentRequests,
                   final Set<StateTransition> stateTransitions) {
            this(timeUntilNextPollMs, unsentRequests, stateTransitions, List.of());
        }

        PollResult(final long timeUntilNextPollMs,
                   final List<UnsentRequest> unsentRequests,
                   final Set<StateTransition> stateTransitions,
                   final List<ManagerEvent> managerEvents) {
            this(
                List.copyOf(unsentRequests),
                stateTransitions,
                managerEvents,
                conditionFromLegacyDelay(timeUntilNextPollMs)
            );
        }

        private PollResult(final List<? extends NetworkCommand> networkCommands,
                           final Set<StateTransition> stateTransitions,
                           final List<ManagerEvent> managerEvents,
                           final NextPollCondition nextPollCondition) {
            this.networkCommands = List.copyOf(networkCommands);
            this.unsentRequests = compatibilityRequests(this.networkCommands);
            this.stateTransitions = Set.copyOf(stateTransitions);
            this.managerEvents = List.copyOf(managerEvents);
            this.nextPollCondition = Objects.requireNonNull(nextPollCondition, "Next-poll condition must be non-null");
            this.timeUntilNextPollMs = nextPollCondition.delayMs();
        }

        private static List<UnsentRequest> compatibilityRequests(final List<NetworkCommand> networkCommands) {
            List<UnsentRequest> requests = new ArrayList<>(networkCommands.size());
            for (NetworkCommand command : networkCommands) {
                UnsentRequest request = command instanceof UnsentRequest
                    ? (UnsentRequest) command
                    : command.transportRequest();
                requests.add(Objects.requireNonNull(request, "Transport request must be non-null"));
            }
            return List.copyOf(requests);
        }

        private static NextPollCondition conditionFromLegacyDelay(final long delayMs) {
            if (delayMs == WAIT_FOREVER)
                return NextPollCondition.awaitInput();
            if (delayMs == 0L)
                return NextPollCondition.pollImmediately();
            return NextPollCondition.retryAfter(delayMs);
        }

        /**
         * Target result factory: facts, transport intents, and the next local poll condition are independent,
         * typed categories. Legacy state transitions are deliberately absent from this API.
         */
        static PollResult progress(final List<? extends NetworkCommand> networkCommands,
                                   final List<ManagerEvent> managerEvents,
                                   final NextPollCondition nextPollCondition) {
            Objects.requireNonNull(networkCommands, "Network commands must be non-null");
            Objects.requireNonNull(managerEvents, "Manager events must be non-null");
            Objects.requireNonNull(nextPollCondition, "Next-poll condition must be non-null");
            if (networkCommands.isEmpty() && managerEvents.isEmpty())
                throw new IllegalArgumentException("Progress requires a network command or manager event");
            return new PollResult(networkCommands, Set.of(), managerEvents, nextPollCondition);
        }

        /**
         * Reports work completed by this poll and the earliest delay before the manager must be polled again.
         * Progress may request an immediate repoll because at least one request or state transition was produced.
         */
        static PollResult progress(final List<UnsentRequest> unsentRequests,
                                   final Set<StateTransition> stateTransitions,
                                   final long timeUntilNextPollMs) {
            return progress(unsentRequests, stateTransitions, List.of(), timeUntilNextPollMs);
        }

        static PollResult progress(final List<UnsentRequest> unsentRequests,
                                   final Set<StateTransition> stateTransitions,
                                   final List<ManagerEvent> managerEvents,
                                   final long timeUntilNextPollMs) {
            Objects.requireNonNull(unsentRequests, "Unsent requests must be non-null");
            Objects.requireNonNull(stateTransitions, "State transitions must be non-null");
            Objects.requireNonNull(managerEvents, "Manager events must be non-null");
            if (unsentRequests.isEmpty() && stateTransitions.isEmpty() && managerEvents.isEmpty())
                throw new IllegalArgumentException("Progress requires a request, state transition, or manager event");
            if (timeUntilNextPollMs < 0L)
                throw new IllegalArgumentException("Progress delay must be non-negative");
            return new PollResult(
                List.copyOf(unsentRequests),
                stateTransitions,
                managerEvents,
                conditionFromLegacyDelay(timeUntilNextPollMs)
            );
        }

        /** Reports no progress and a finite, positive delay before the manager should be polled again. */
        static PollResult retryAfter(final long delayMs) {
            return new PollResult(List.of(), Set.of(), List.of(), NextPollCondition.retryAfter(delayMs));
        }

        /** Reports no progress and no timer deadline; another input must make the manager runnable again. */
        static PollResult awaitInput() {
            return new PollResult(List.of(), Set.of(), List.of(), NextPollCondition.awaitInput());
        }

        /** Compatibility alias retained while manager producers migrate to the more precise {@link #awaitInput()}. */
        static PollResult awaitEvent() {
            return awaitInput();
        }

        public PollResult(final List<UnsentRequest> unsentRequests) {
            this(WAIT_FOREVER, unsentRequests);
        }

        public PollResult(final UnsentRequest unsentRequest) {
            this(Collections.singletonList(unsentRequest));
        }

        public PollResult(final long timeUntilNextPollMs) {
            this(timeUntilNextPollMs, Collections.emptyList());
        }

        Set<StateTransition> stateTransitions() {
            return stateTransitions;
        }

        List<NetworkCommand> networkCommands() {
            return networkCommands;
        }

        List<ManagerEvent> managerEvents() {
            return managerEvents;
        }

        NextPollCondition nextPollCondition() {
            return nextPollCondition;
        }

        /** Generic contract used by the reactor while legacy constructor call sites are migrated incrementally. */
        boolean satisfiesProgressContract() {
            return timeUntilNextPollMs >= 0L
                && (!unsentRequests.isEmpty()
                    || !stateTransitions.isEmpty()
                    || !managerEvents.isEmpty()
                    || timeUntilNextPollMs > 0L);
        }

        PollResult withStateTransitions(final Set<StateTransition> additionalTransitions) {
            if (additionalTransitions.isEmpty())
                return this;

            EnumSet<StateTransition> combinedTransitions = EnumSet.noneOf(StateTransition.class);
            combinedTransitions.addAll(stateTransitions);
            combinedTransitions.addAll(additionalTransitions);
            return progress(unsentRequests, combinedTransitions, managerEvents, timeUntilNextPollMs);
        }

        PollResult withManagerEvents(final Collection<ManagerEvent> additionalEvents) {
            if (additionalEvents.isEmpty())
                return this;

            List<ManagerEvent> combinedEvents = new ArrayList<>(managerEvents.size() + additionalEvents.size());
            combinedEvents.addAll(managerEvents);
            combinedEvents.addAll(additionalEvents);
            return progress(unsentRequests, stateTransitions, combinedEvents, timeUntilNextPollMs);
        }
    }

    public static class UnsentRequest extends NetworkCommand {
        private final AbstractRequest.Builder<?> requestBuilder;
        private final FutureCompletionHandler handler;
        private final Optional<Node> node; // empty if random node can be chosen

        private Timer timer;
        private long enqueueTimeMs; // time when the request was enqueued to unsentRequests, not duration in the queue.

        public UnsentRequest(final AbstractRequest.Builder<?> requestBuilder,
                             final Optional<Node> node) {
            Objects.requireNonNull(requestBuilder);
            this.requestBuilder = requestBuilder;
            this.node = node;
            this.handler = new FutureCompletionHandler();
        }

        void setTimer(final Time time, final long requestTimeoutMs) {
            this.timer = time.timer(requestTimeoutMs);
        }

        Timer timer() {
            return timer;
        }

        /**
         * Set the time when the request was enqueued to {@link NetworkClientDelegate#unsentRequests}.
         */
        private void setEnqueueTimeMs(final long enqueueTimeMs) {
            this.enqueueTimeMs = enqueueTimeMs;
        }

        /**
         * Return the time when the request was enqueued to {@link NetworkClientDelegate#unsentRequests}.
         */
        private long enqueueTimeMs() {
            return enqueueTimeMs;
        }

        CompletableFuture<ClientResponse> future() {
            return handler.future;
        }

        FutureCompletionHandler handler() {
            return handler;
        }

        UnsentRequest whenComplete(BiConsumer<ClientResponse, Throwable> callback) {
            handler.future().whenComplete(callback);
            return this;
        }

        @Override
        public void onCompletion(final BiConsumer<ClientResponse, Throwable> callback) {
            handler.future().whenComplete(callback);
        }

        @Override
        public UnsentRequest transportRequest() {
            return this;
        }

        AbstractRequest.Builder<?> requestBuilder() {
            return requestBuilder;
        }

        Optional<Node> node() {
            return node;
        }

        @Override
        public String toString() {
            String remainingMs;

            if (timer != null) {
                timer.update();
                remainingMs = String.valueOf(timer.remainingMs());
            } else {
                remainingMs = "<not set>";
            }

            return "UnsentRequest{" +
                    "requestBuilder=" + requestBuilder +
                    ", handler=" + handler +
                    ", node=" + node +
                    ", remainingMs=" + remainingMs +
                    '}';
        }
    }

    public static class FutureCompletionHandler implements RequestCompletionHandler {

        private long responseCompletionTimeMs;
        private final CompletableFuture<ClientResponse> future;

        FutureCompletionHandler() {
            future = new CompletableFuture<>();
        }

        public void onFailure(final long currentTimeMs, final RuntimeException e) {
            this.responseCompletionTimeMs = currentTimeMs;
            if (e != null) {
                this.future.completeExceptionally(e);
            } else {
                this.future.completeExceptionally(DisconnectException.INSTANCE);
            }
        }

        public long completionTimeMs() {
            return responseCompletionTimeMs;
        }

        @Override
        public void onComplete(final ClientResponse response) {
            long completionTimeMs = response.receivedTimeMs();
            if (response.authenticationException() != null) {
                onFailure(completionTimeMs, response.authenticationException());
            } else if (response.wasDisconnected()) {
                onFailure(completionTimeMs, DisconnectException.INSTANCE);
            } else if (response.versionMismatch() != null) {
                onFailure(completionTimeMs, response.versionMismatch());
            } else {
                responseCompletionTimeMs = completionTimeMs;
                this.future.complete(response);
            }
        }

        public CompletableFuture<ClientResponse> future() {
            return future;
        }
    }

    /**
     * Creates a {@link Supplier} for deferred creation during invocation by
     * {@link ConsumerReactor}.
     */
    public static Supplier<NetworkClientDelegate> supplier(final Time time,
                                                           final LogContext logContext,
                                                           final Metadata metadata,
                                                           final ConsumerConfig config,
                                                           final ApiVersions apiVersions,
                                                           final Metrics metrics,
                                                           final Sensor throttleTimeSensor,
                                                           final ClientTelemetrySender clientTelemetrySender,
                                                           final BackgroundEventHandler backgroundEventHandler,
                                                           final boolean notifyMetadataErrorsViaErrorQueue,
                                                           final AsyncConsumerMetrics asyncConsumerMetrics) {
        return new CachedSupplier<>() {
            @Override
            protected NetworkClientDelegate create() {
                KafkaClient client = ClientUtils.createNetworkClient(config,
                        config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
                        metrics,
                        CONSUMER_METRIC_GROUP_PREFIX,
                        logContext,
                        apiVersions,
                        time,
                        CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
                        metadata,
                        throttleTimeSensor,
                        clientTelemetrySender);
                return new NetworkClientDelegate(time, config, logContext, client, metadata, backgroundEventHandler, notifyMetadataErrorsViaErrorQueue, asyncConsumerMetrics);
            }
        };
    }

    /**
     * Creates a {@link Supplier} for deferred creation during invocation by
     * {@link ConsumerReactor}.
     */
    public static Supplier<NetworkClientDelegate> supplier(final Time time,
                                                           final ConsumerConfig config,
                                                           final LogContext logContext,
                                                           final KafkaClient client,
                                                           final Metadata metadata,
                                                           final BackgroundEventHandler backgroundEventHandler,
                                                           final boolean notifyMetadataErrorsViaErrorQueue,
                                                           final AsyncConsumerMetrics asyncConsumerMetrics) {
        return new CachedSupplier<>() {
            @Override
            protected NetworkClientDelegate create() {
                return new NetworkClientDelegate(
                    time,
                    config,
                    logContext,
                    client,
                    metadata,
                    backgroundEventHandler,
                    notifyMetadataErrorsViaErrorQueue,
                    asyncConsumerMetrics
                );
            }
        };
    }
}
