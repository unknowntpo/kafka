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

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.MetadataErrorNotifiableEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.KafkaThread;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.security.auth.spi.LoginModule;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * Single-owner event loop that consumes {@link ApplicationEvent}, polls manager-owned state, decides the
 * shared schedule, produces {@link BackgroundEvent}, and polls the network client.
 *
 * <p>The reactor is the only component that combines manager results into network-poll, application-wait, and
 * external-action decisions. Request managers retain their local state and request-building rules.</p>
 */
public class ConsumerReactor extends KafkaThread implements Closeable {

    // visible for testing
    static final long MAX_POLL_TIMEOUT_MS = 5000;
    // Keep the runtime thread name stable for diagnostics and compatibility with existing tooling.
    private static final String REACTOR_THREAD_NAME = "consumer_background_thread";
    private final Time time;
    private final Logger log;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final CompletableEventReaper applicationEventReaper;
    private final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier;
    private final Supplier<NetworkClientDelegate> networkClientDelegateSupplier;
    private final Supplier<RequestManagers> requestManagersSupplier;
    private final AsyncConsumerMetrics asyncConsumerMetrics;
    private ApplicationEventProcessor applicationEventProcessor;
    private NetworkClientDelegate networkClientDelegate;
    private RequestManagers requestManagers;
    private volatile boolean running;
    private final IdempotentCloser closer = new IdempotentCloser();
    private final CountDownLatch initializationLatch = new CountDownLatch(1);
    private final AtomicReference<KafkaException> initializationError = new AtomicReference<>();
    private volatile Duration closeTimeout = Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS);
    private volatile ReactorSchedule reactorSchedule;
    private final ManagerPollCache managerPollCache = new ManagerPollCache();
    private final Set<RequestManager> affectedManagers =
        Collections.newSetFromMap(new IdentityHashMap<>());
    private final EnumSet<StateTransition> pendingStateTransitions =
        EnumSet.noneOf(StateTransition.class);
    private final List<ReactorAction> pendingReactorActions = new ArrayList<>();
    private final EnumSet<ReactorActionReason> pendingReactorActionReasons =
        EnumSet.noneOf(ReactorActionReason.class);
    private long lastPollTimeMs = 0L;

    public ConsumerReactor(LogContext logContext,
                           Time time,
                           BlockingQueue<ApplicationEvent> applicationEventQueue,
                           CompletableEventReaper applicationEventReaper,
                           Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier,
                           Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                           Supplier<RequestManagers> requestManagersSupplier,
                           AsyncConsumerMetrics asyncConsumerMetrics) {
        super(REACTOR_THREAD_NAME, true);
        this.time = time;
        this.log = logContext.logger(getClass());
        this.applicationEventQueue = applicationEventQueue;
        this.applicationEventReaper = applicationEventReaper;
        this.applicationEventProcessorSupplier = applicationEventProcessorSupplier;
        this.networkClientDelegateSupplier = networkClientDelegateSupplier;
        this.requestManagersSupplier = requestManagersSupplier;
        this.running = true;
        this.asyncConsumerMetrics = asyncConsumerMetrics;
        this.reactorSchedule = ReactorSchedule.initial(
            MAX_POLL_TIMEOUT_MS,
            time.milliseconds()
        );
    }

    /**
     * Start the reactor thread and let it complete its initialization before proceeding. The
     * {@link ClassicKafkaConsumer} constructor blocks during creation of its {@link NetworkClient}, providing
     * precedent for waiting here.
     *
     * In certain cases (e.g. an invalid {@link LoginModule} in {@link SaslConfigs#SASL_JAAS_CONFIG}), an error
     * could be thrown during {@link #initializeResources()}. This would result in the {@link #run()} method
     * exiting, no longer able to process events, which means that the consumer effectively hangs.
     *
     * @param timeoutMs Length of time, in milliseconds, to wait for the thread to start and complete initialization
     */
    public void start(int timeoutMs) {
        // start() is invoked internally instead of by the caller to avoid SpotBugs errors about starting a thread
        // in a constructor.
        start();

        try {
            if (!initializationLatch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                maybeSetInitializationError(
                    new TimeoutException("Consumer reactor resource initialization timed out after " + timeoutMs + " ms")
                );
            }
        } catch (InterruptedException e) {
            maybeSetInitializationError(
                new InterruptException("Consumer reactor resource initialization was interrupted", e)
            );
        }

        KafkaException e = initializationError.get();

        if (e != null)
            throw e;
    }

    @Override
    public void run() {
        try {
            log.debug("Consumer reactor started");

            // Wait until we're securely in the reactor thread to initialize these objects...
            try {
                initializeResources();
            } catch (Throwable t) {
                KafkaException e = ConsumerUtils.maybeWrapAsKafkaException(t);
                maybeSetInitializationError(e);

                // This will still call cleanup() via the `finally` section below.
                return;
            } finally {
                initializationLatch.countDown();
            }

            while (running) {
                try {
                    runOnce();
                } catch (final Throwable e) {
                    // Swallow the exception and continue
                    log.error("Unexpected error caught in consumer reactor", e);
                }
            }
        } catch (Throwable t) {
            log.error("Unexpected failure in consumer reactor", t);
        } finally {
            cleanup();
        }
    }

    private void maybeSetInitializationError(KafkaException error) {
        if (initializationError.compareAndSet(null, error))
            return;

        log.error("Consumer reactor resource initialization error ({}) will be suppressed as an error was already set", error.getMessage(), error);
    }

    void initializeResources() {
        applicationEventProcessor = applicationEventProcessorSupplier.get();
        networkClientDelegate = networkClientDelegateSupplier.get();
        requestManagers = requestManagersSupplier.get();
    }

    /**
     * Poll and process the {@link ApplicationEvent application events}. It performs the following tasks:
     *
     * <ol>
     *     <li>
     *         Drains and processes all the events from the application thread's application event queue via
     *         {@link ApplicationEventProcessor}
     *     </li>
     *     <li>
     *         Poll each {@link RequestManager} and collect its proposed network work, state transitions, and next
     *         poll delay
     *     </li>
     *     <li>
     *         Stage each {@link AbstractRequest.Builder request} to be sent via
     *         {@link NetworkClientDelegate#addAll(List)}
     *     </li>
     *     <li>
     *         Poll the client via {@link KafkaClient#poll(long, long)} to send the requests, as well as
     *         retrieve any available responses
     *     </li>
     * </ol>
     */
    void runOnce() {
        // The following code avoids use of the Java Collections Streams API to reduce overhead in this loop.
        processApplicationEvents();

        final long currentTimeMs = time.milliseconds();
        if (lastPollTimeMs != 0L) {
            asyncConsumerMetrics.recordTimeBetweenNetworkThreadPoll(currentTimeMs - lastPollTimeMs);
        }
        lastPollTimeMs = currentTimeMs;

        long pollWaitTimeMs = MAX_POLL_TIMEOUT_MS;

        // A full pre-I/O pass remains the correctness fallback while metadata, disconnect, capacity, and
        // cross-manager dependencies are migrated to typed input events. It supersedes and discards any completion
        // marks deferred from a previous post-I/O pass because every manager is polled below.
        affectedManagers.clear();
        List<NetworkClientDelegate.PollResult> pollResults = new ArrayList<>();
        List<RequestManager> managers = new ArrayList<>();
        for (RequestManager rm : requestManagers.entries()) {
            NetworkClientDelegate.PollResult result = rm.poll(currentTimeMs);
            managers.add(rm);
            pollResults.add(result);
            stagePollResult(rm, result, currentTimeMs);
            networkClientDelegate.addAll(result.unsentRequests);
        }
        managerPollCache.retainManagers(managers);

        ReactorSchedule proposedSchedule = ReactorSchedule.from(
            managerPollCache.states(),
            currentTimeMs
        );
        proposedSchedule = publishReactorSchedule(proposedSchedule, currentTimeMs);
        pollWaitTimeMs = Math.min(
            pollWaitTimeMs,
            proposedSchedule.networkPollTimeoutMs(currentTimeMs)
        );
        collectApplicationEventActions();
        collectStateTransitions(pollResults);
        executeReactorActions();

        networkClientDelegate.poll(pollWaitTimeMs, currentTimeMs);

        // Deliver an expired legacy application deadline before asking managers for a fresh one. The manager poll
        // deadline itself is consumed by polling the manager again and never directly wakes the application thread.
        long afterNetworkPollMs = time.milliseconds();
        deliverExpiredApplicationDeadline(afterNetworkPollMs);

        // Request completion callbacks mark their owning manager. Poll only that stable snapshot here; marks produced
        // by this pass are intentionally deferred to the next full pre-I/O pass.
        List<NetworkClientDelegate.PollResult> postIoResults = pollAffectedManagers(afterNetworkPollMs);
        if (!postIoResults.isEmpty()) {
            publishReactorSchedule(
                ReactorSchedule.from(managerPollCache.states(), afterNetworkPollMs),
                afterNetworkPollMs
            );
            deliverExpiredApplicationDeadline(afterNetworkPollMs);
            collectStateTransitions(postIoResults);
        }
        collectApplicationEventActions();
        executeReactorActions();

        reapExpiredApplicationEvents(currentTimeMs);
        List<CompletableEvent<?>> uncompletedEvents = applicationEventReaper.uncompletedEvents();
        if (stageMetadataErrorActions(uncompletedEvents))
            executeReactorActions();
    }

    /**
     * Process the events-if any-that were produced by the application thread.
     */
    private void processApplicationEvents() {
        LinkedList<ApplicationEvent> events = new LinkedList<>();
        applicationEventQueue.drainTo(events);
        if (events.isEmpty())
            return;

        asyncConsumerMetrics.recordApplicationEventQueueSize(0);
        long startMs = time.milliseconds();
        for (ApplicationEvent event : events) {
            asyncConsumerMetrics.recordApplicationEventQueueTime(time.milliseconds() - event.enqueuedMs());
            try {
                if (event instanceof CompletableEvent) {
                    applicationEventReaper.add((CompletableEvent<?>) event);
                }
                // Check if there are any metadata errors and fail the event if an error is present.
                // This call is meant to handle "immediately completed events" which may not enter the
                // awaiting state, so metadata errors need to be checked and handled right away.
                if (event instanceof MetadataErrorNotifiableEvent) {
                    if (stageMetadataErrorActions(List.of(event)))
                        continue;
                }
                applicationEventProcessor.process(event);
            } catch (Throwable t) {
                log.error("Error processing event {}", t.getMessage(), t);
                if (event instanceof CompletableEvent) {
                    ((CompletableEvent<?>) event).future().completeExceptionally(t);
                }
            }
        }
        asyncConsumerMetrics.recordApplicationEventQueueProcessingTime(time.milliseconds() - startMs);
    }

    /**
     * "Complete" any events that have expired. This cleanup step should only be called after the network I/O
     * thread has made at least one call to {@link NetworkClientDelegate#poll(long, long) poll} so that each event
     * is given least one attempt to satisfy any network requests <em>before</em> checking if a timeout has expired.
     */
    private void reapExpiredApplicationEvents(long currentTimeMs) {
        asyncConsumerMetrics.recordApplicationEventExpiredSize(applicationEventReaper.reap(currentTimeMs));
    }

    /**
     * Performs any network I/O that is needed at the time of close for the consumer:
     *
     * <ol>
     *     <li>
     *         Iterate through the {@link RequestManager} list and invoke {@link RequestManager#pollOnClose(long)}
     *         to get the {@link NetworkClientDelegate.UnsentRequest} list and the poll time for the network poll
     *     </li>
     *     <li>
     *         Stage each {@link AbstractRequest.Builder request} to be sent via
     *         {@link NetworkClientDelegate#addAll(List)}
     *     </li>
     *     <li>
     *         {@link KafkaClient#poll(long, long) Poll the client} to send the requests, as well as
     *         retrieve any available responses
     *     </li>
     *     <li>
     *         Continuously {@link KafkaClient#poll(long, long) poll the client} as long as the
     *         {@link Timer#notExpired() timer hasn't expired} to retrieve the responses
     *     </li>
     * </ol>
     */
    // Visible for testing
    static void runAtClose(final Collection<RequestManager> requestManagers,
                           final NetworkClientDelegate networkClientDelegate,
                           final long currentTimeMs) {
        // These are the optional outgoing requests at the time of closing the consumer
        for (RequestManager rm : requestManagers) {
            NetworkClientDelegate.PollResult pollResult = rm.pollOnClose(currentTimeMs);
            networkClientDelegate.addAll(pollResult);
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void wakeup() {
        // The network client can be null if the initializeResources method has not yet been called.
        if (networkClientDelegate != null)
            networkClientDelegate.wakeup();
    }

    /**
     * Returns the delay for which the application thread can safely wait before it should be responsive
     * to results from the request managers. For example, the subscription state can change when heartbeats
     * are sent, so blocking for longer than the heartbeat interval might mean the application thread is not
     * responsive to changes.
     *
     * Because this method is called by the application thread, it's not allowed to access the request managers
     * that actually provide the information. As a result, the consumer reactor periodically caches the
     * information from the request managers and this can then be read safely using this method.
     *
     * @return The maximum delay in milliseconds
     */
    public long maximumTimeToWait() {
        return reactorSchedule.remainingMsForApplication(time.milliseconds());
    }

    private void stagePollResult(final RequestManager manager,
                                 final NetworkClientDelegate.PollResult result,
                                 final long currentTimeMs) {
        managerPollCache.update(manager, result, manager.maximumTimeToWait(currentTimeMs), currentTimeMs);
        for (NetworkClientDelegate.UnsentRequest request : result.unsentRequests) {
            request.whenComplete((response, error) -> affectedManagers.add(manager));
        }
    }

    private List<NetworkClientDelegate.PollResult> pollAffectedManagers(final long currentTimeMs) {
        if (affectedManagers.isEmpty())
            return List.of();

        Set<RequestManager> managers = Collections.newSetFromMap(new IdentityHashMap<>());
        managers.addAll(affectedManagers);
        affectedManagers.clear();

        List<NetworkClientDelegate.PollResult> results = new ArrayList<>(managers.size());
        for (RequestManager manager : managers) {
            NetworkClientDelegate.PollResult result = manager.poll(currentTimeMs);
            results.add(result);
            stagePollResult(manager, result, currentTimeMs);
            networkClientDelegate.addAll(result.unsentRequests);
        }
        return results;
    }

    private ReactorSchedule publishReactorSchedule(
        final ReactorSchedule decision,
        final long currentTimeMs
    ) {
        ReactorSchedule previous = reactorSchedule;
        ReactorSchedule next = decision.withGeneration(previous.generation() + 1L);

        // Publish before signaling. Fetch buffers latch wakeups, so the application either observes the new
        // snapshot before waiting or is released after it has started waiting on the old snapshot.
        reactorSchedule = next;

        if (log.isTraceEnabled() && !next.sameSchedule(previous)) {
            log.trace(
                "Reactor schedule changed: source={}, previousDeadline={}, deadline={}, networkPollTimeout={}",
                next.deadlineSource().orElse("none"),
                previous.pollDeadlineMs(),
                next.pollDeadlineMs(),
                next.networkPollTimeoutMs(currentTimeMs)
            );
        }

        // This compatibility action only applies to the legacy application wait projection. Manager poll deadlines
        // such as fetch reconnect backoff are reactor-only and cannot enter this branch.
        if (next.shortensApplicationWait(previous) && next.applicationRemainingMs(currentTimeMs) > 0L) {
            stageWakeApplication();
            pendingReactorActionReasons.add(
                ReactorActionReason.SCHEDULE_SHORTENED
            );
        }

        return next;
    }

    private void deliverExpiredApplicationDeadline(final long currentTimeMs) {
        ReactorSchedule current = reactorSchedule;
        if (current.applicationDeadlineMs() == Long.MAX_VALUE
            || current.applicationRemainingMs(currentTimeMs) > 0L
            || current.applicationDeadlineDelivered()) {
            return;
        }

        // Mark delivery in the published snapshot before signaling. This prevents the released application thread
        // from observing a stale 0ms timeout and spinning while the manager processes the resulting event.
        managerPollCache.markApplicationDeadlineDelivered(current);
        reactorSchedule = current.withApplicationDeadlineDelivered();
        stageWakeApplication();
        pendingReactorActionReasons.add(
            ReactorActionReason.WAIT_DEADLINE_EXPIRED
        );
    }

    private void collectStateTransitions(final Collection<NetworkClientDelegate.PollResult> results) {
        for (NetworkClientDelegate.PollResult result : results)
            pendingStateTransitions.addAll(result.stateTransitions());
        if (!pendingStateTransitions.isEmpty()) {
            stageWakeApplication();
            pendingReactorActionReasons.add(
                ReactorActionReason.STATE_TRANSITION
            );
        }
    }

    private void executeReactorActions() {
        if (pendingReactorActions.isEmpty())
            return;

        if (log.isTraceEnabled()) {
            log.trace(
                "Executing reactor action: actions={}, reasons={}, stateTransitions={}",
                pendingReactorActions,
                pendingReactorActionReasons,
                pendingStateTransitions
            );
        }
        // Complete or publish event state before releasing the application thread. A wakeup is retained and
        // coalesced, so executing it last cannot lose the notification.
        for (ReactorAction action : pendingReactorActions) {
            if (action.type() != ReactorAction.Type.WAKE_APPLICATION)
                action.execute(requestManagers);
        }
        if (pendingReactorActions.contains(ReactorAction.wakeApplication()))
            ReactorAction.wakeApplication().execute(requestManagers);
        pendingReactorActions.clear();
        pendingReactorActionReasons.clear();
        pendingStateTransitions.clear();
    }

    private void collectApplicationEventActions() {
        List<ReactorAction> actions = applicationEventProcessor.drainReactorActions();
        pendingReactorActions.addAll(actions);
        if (!actions.isEmpty())
            pendingReactorActionReasons.add(ReactorActionReason.APPLICATION_EVENT_PROGRESS);
    }

    private void stageWakeApplication() {
        ReactorAction wakeApplication = ReactorAction.wakeApplication();
        if (!pendingReactorActions.contains(wakeApplication))
            pendingReactorActions.add(wakeApplication);
    }

    // Visible for testing. The immutable snapshot is safe to inspect without exposing request-manager state.
    ReactorSchedule reactorSchedule() {
        return reactorSchedule;
    }

    public long reactorScheduleGeneration() {
        return reactorSchedule.generation();
    }

    @Override
    public void close() {
        close(closeTimeout);
    }

    public void close(final Duration timeout) {
        Objects.requireNonNull(timeout, "Close timeout for consumer reactor must be non-null");

        closer.close(
                () -> closeInternal(timeout),
                () -> log.warn("The consumer reactor was already closed")
        );
    }

    /**
     * Starts the closing process.
     *
     * <p/>
     *
     * This method is called from the application thread, but our resources are owned by the reactor. As such,
     * we don't actually close any of those resources here, immediately, on the application thread. Instead, we just
     * update our internal state on the application thread. When the reactor next
     * {@link #run() executes its loop}, it will notice that state, cease processing any further events, and begin
     * {@link #cleanup() closing its resources}.
     *
     * <p/>
     *
     * This method will wait (i.e. block the application thread) for up to the duration of the given timeout to give
     * the reactor the time to close down cleanly.
     *
     * @param timeout Upper bound of time to wait for the reactor to close its resources
     */
    private void closeInternal(final Duration timeout) {
        long timeoutMs = timeout.toMillis();
        log.trace("Signaling the consumer reactor to close in {}ms", timeoutMs);
        running = false;
        closeTimeout = timeout;
        wakeup();

        try {
            join();
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for consumer reactor to complete", e);
        }
    }

    /**
     * Check the unsent queue one last time and poll until all requests are sent or the timer runs out.
     */
    private void sendUnsentRequests(final Timer timer) {
        if (!networkClientDelegate.hasAnyPendingRequests())
            return;

        do {
            networkClientDelegate.poll(timer.remainingMs(), timer.currentTimeMs(), true);
            timer.update();
        } while (timer.notExpired() && networkClientDelegate.hasAnyPendingRequests());

        if (networkClientDelegate.hasAnyPendingRequests()) {
            log.warn("Close timeout of {} ms expired before the consumer reactor was able " +
                "to complete pending requests. Inflight request count: {}, Unsent request count: {}",
                timer.timeoutMs(), networkClientDelegate.inflightRequestCount(), networkClientDelegate.unsentRequests().size());
        }
    }

    void cleanup() {
        log.trace("Closing the consumer reactor");
        Timer timer = time.timer(closeTimeout);
        try {
            // If an error was thrown from initializeResources(), it's possible that the list of request managers
            // is null, so check before using. If the request manager list is null, there wasn't any real work
            // performed, so not being able to close the request managers isn't so bad.
            if (requestManagers != null && networkClientDelegate != null)
                runAtClose(requestManagers.entries(), networkClientDelegate, time.milliseconds());
        } catch (Exception e) {
            log.error("Unexpected error during shutdown. Proceed with closing.", e);
        } finally {
            // Likewise, if an error was thrown from initializeResources(), it's possible for the network client
            // to be null, so check before using. If the network client is null, things have failed catastrophically
            // enough that there aren't any outstanding requests to be sent anyway.
            if (networkClientDelegate != null)
                sendUnsentRequests(timer);

            asyncConsumerMetrics.recordApplicationEventExpiredSize(applicationEventReaper.reap(applicationEventQueue));

            closeQuietly(requestManagers, "request managers");
            closeQuietly(networkClientDelegate, "network client delegate");
            log.debug("Closed the consumer reactor");
        }
    }

    /**
     * If there is a metadata error, stage completion of all uncompleted events that require subscription metadata.
     * The caller publishes the current schedule before these actions complete the events and wake the application.
     */
    private boolean stageMetadataErrorActions(List<?> events) {
        List<MetadataErrorNotifiableEvent> filteredEvents = new ArrayList<>();

        for (Object obj : events) {
            if (obj instanceof MetadataErrorNotifiableEvent) {
                filteredEvents.add((MetadataErrorNotifiableEvent) obj);
            }
        }

        // Don't get-and-clear the metadata error if there are no events that will be notified.
        if (filteredEvents.isEmpty())
            return false;

        Optional<Exception> metadataError = networkClientDelegate.getAndClearMetadataError();

        if (metadataError.isPresent()) {
            filteredEvents.forEach(e -> pendingReactorActions.add(
                ReactorAction.notifyMetadataError(e, metadataError.get())
            ));
            stageWakeApplication();
            pendingReactorActionReasons.add(ReactorActionReason.METADATA_ERROR);
            return true;
        } else {
            return false;
        }
    }
}
