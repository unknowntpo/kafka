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

import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.FetchPositionsErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.MetadataErrorNotifiableEvent;
import org.apache.kafka.clients.consumer.internals.events.UpdatePatternSubscriptionEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.pipeline.LoopSignal;
import org.apache.kafka.clients.consumer.internals.pipeline.LoopTimer;
import org.apache.kafka.clients.consumer.internals.pipeline.PassDecision;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.KafkaThread;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * The background thread of the pipelined consumer: an event loop with three sources of work and no global wait
 * time.
 *
 * <ul>
 *   <li><b>I/O</b>: {@link NetworkClientDelegate#poll} dispatches responses to their owners (the
 *       {@link ManagerTask}s, including the unchanged {@link FetchRequestManager}) as they arrive.</li>
 *   <li><b>Timers</b>: every {@link ManagerTask} and the event reaper own their own deadline in a {@link LoopTimer};
 *       the loop blocks until the earliest one.</li>
 *   <li><b>Commands</b>: {@link ApplicationEvent}s and internal {@link Runnable}s from the application thread, drained
 *       from a lock-free queue; enqueuing wakes the loop at most once per park cycle ({@link LoopSignal}).</li>
 * </ul>
 *
 * <p>Steady-state {@code poll()} calls on the application thread produce no commands: the loop learns that the
 * application polled through {@link #onApplicationPoll(long)} (a volatile write) and does the per-poll bookkeeping
 * (heartbeat poll timer, auto-commit, reconciliation, fetch request creation) on its own schedule.
 *
 * <p>This is the "event loop only" variant: the fetch path ({@link FetchRequestManager}, {@link FetchBuffer},
 * {@link FetchCollector}) is the previous implementation's, unchanged, so that the effect of the loop can be
 * measured on its own. The next fetch is created once per application poll, exactly as the previous
 * implementation's per-poll event did.
 */
public class ConsumerEventLoop extends KafkaThread implements Closeable {

    /** Same thread name as the previous implementation so dashboards and profiles stay comparable. */
    public static final String BACKGROUND_THREAD_NAME = "consumer_background_thread";

    static final long MAX_BLOCK_MS = 5_000;
    static final long REAPER_INTERVAL_MS = 100;
    /** Minimum wait after an unexpected error in a loop pass, so a persistent failure cannot spin the thread. */
    static final long ERROR_BACKOFF_MS = 100;
    static final long ERROR_LOG_INTERVAL_MS = 1_000;

    private final Logger log;
    private final Time time;
    private final long defaultApiTimeoutMs;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier;
    private final Supplier<NetworkClientDelegate> networkClientDelegateSupplier;
    private final Supplier<RequestManagers> requestManagersSupplier;
    private final BackgroundEventHandler backgroundEventHandler;
    private final CompletableEventReaper applicationEventReaper;
    private final AsyncConsumerMetrics asyncConsumerMetrics;

    private final ConcurrentLinkedQueue<Object> commands = new ConcurrentLinkedQueue<>();
    private final LoopSignal signal;
    private final LoopTimer timer = new LoopTimer();
    private final CountDownLatch initializationLatch = new CountDownLatch(1);
    private final List<ManagerTask> managerTasks = new ArrayList<>();
    private LifecycleSequencer lifecycle;

    private volatile NetworkClientDelegate networkClientDelegate;
    private ApplicationEventProcessor applicationEventProcessor;
    private RequestManagers requestManagers;
    private volatile Throwable initializationError;
    private volatile boolean running;
    private volatile Duration closeTimeout = Duration.ofMillis(ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS);

    /**
     * Every manager must run: set by commands, per-poll bookkeeping, metadata changes and request completions
     * (from callbacks on the loop thread, inside the network poll). Volatile for SpotBugs and clarity.
     */
    private volatile boolean managersDirty = true;
    /** Loop pass counter; a manager runs at most once per pass whatever combination of triggers fired. */
    private long pass;
    /** A command from the application thread was processed in the current pass (commands re-run every manager). */
    private boolean commandProcessedThisPass;
    /**
     * The application asked for the next fetch (a poll, or records returned): set from the application thread, so
     * the loop calls {@link FetchRequestManager#createFetchRequests()} on its next pass, as the previous
     * implementation's per-poll event did.
     */
    private volatile boolean fetchRequested;
    /** Fetching starts with the first application poll(), as before; until then only the group protocol runs. */
    private volatile boolean fetchingEnabled;
    private volatile boolean lastPassFailed;
    private long lastErrorLogMs;
    private long suppressedErrors;
    private long lastLoopTimeMs;
    private int lastMetadataVersion;
    /** Written by the application thread on every poll(); read by the loop. */
    private volatile long lastApplicationPollMs;
    /** True while the application thread is inside poll(); time spent there must not count against max.poll.interval.ms. */
    private volatile boolean applicationInPoll;
    private long lastPollTimerRefreshMs;
    /** Poll counter, so repeated polls are distinguishable even when the clock does not advance. */
    private volatile long applicationPollSequence;
    private long lastHousekeptPollSequence;
    /** The poll sequence up to which the reconciliation check has run; read by the application thread. */
    private volatile long reconciliationCheckedPollSequence;
    private CompletableFuture<Void> positionsUpdate;
    /** Called when a published {@link PassDecision} changes something the application thread may wait for. */
    private final Runnable onApplicationVisibleChange;
    /** Inputs with identity consumed so far: request completions, commands, metadata changes. Loop thread only. */
    private long stateVersion;
    /** Published at the end of every pass; read by the application thread. */
    private volatile PassDecision latestDecision = PassDecision.NONE;
    /** Whether positions may have changed in this pass (managers ran, a command or metadata change was processed). */
    private boolean positionsMayHaveChanged = true;

    public ConsumerEventLoop(LogContext logContext,
                             Time time,
                             long defaultApiTimeoutMs,
                             SubscriptionState subscriptions,
                             ConsumerMetadata metadata,
                             Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier,
                             Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                             Supplier<RequestManagers> requestManagersSupplier,
                             BackgroundEventHandler backgroundEventHandler,
                             AsyncConsumerMetrics asyncConsumerMetrics,
                             Runnable onApplicationVisibleChange) {
        super(BACKGROUND_THREAD_NAME, true);
        this.log = logContext.logger(ConsumerEventLoop.class);
        this.time = time;
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.applicationEventProcessorSupplier = applicationEventProcessorSupplier;
        this.networkClientDelegateSupplier = networkClientDelegateSupplier;
        this.requestManagersSupplier = requestManagersSupplier;
        this.backgroundEventHandler = backgroundEventHandler;
        this.applicationEventReaper = new CompletableEventReaper(logContext);
        this.asyncConsumerMetrics = asyncConsumerMetrics;
        this.onApplicationVisibleChange = onApplicationVisibleChange;
        this.signal = new LoopSignal(() -> {
            NetworkClientDelegate delegate = networkClientDelegate;
            if (delegate != null)
                delegate.wakeup();
        });
    }

    // ---- application thread API ---------------------------------------------------------------------------------

    /** @return the signal other threads use to wake the loop */
    public LoopSignal signal() {
        return signal;
    }

    /** Loop thread only: the network client delegate, available once the loop has initialized. */
    public NetworkClientDelegate networkClientDelegate() {
        return networkClientDelegate;
    }

    /** Enqueues an event for the loop and wakes it if it is parked. */
    public void add(ApplicationEvent event) {
        ensureAlive();
        event.setEnqueuedMs(time.milliseconds());
        commands.add(event);
        signal.wakeupIfParked();
    }

    /** Enqueues an event and blocks until it completes. */
    public <T> T addAndGet(CompletableApplicationEvent<T> event) {
        add(event);
        if (Thread.interrupted())
            throw new InterruptException("Interrupted while waiting for " + event);
        return ConsumerUtils.getResult(event.future());
    }

    /** Runs {@code task} on the loop thread. */
    public void execute(Runnable task) {
        ensureAlive();
        commands.add(task);
        signal.wakeupIfParked();
    }

    /**
     * Application thread: records that {@code poll()} was called; no event, no wake-up.
     *
     * @return the sequence number of this poll, for {@link #reconciliationCheckedPollSequence()}
     */
    public long onApplicationPoll(long pollTimeMs) {
        lastApplicationPollMs = pollTimeMs;
        applicationInPoll = true;
        return ++applicationPollSequence;
    }

    /** Application thread: poll() is returning to the application. */
    public void onApplicationPollReturn() {
        applicationInPoll = false;
    }

    /** Application thread: wakes the loop so the per-poll bookkeeping runs now (used only when it matters). */
    public void wakeup() {
        signal.wakeupIfParked();
    }

    /**
     * Application thread: asks for the next round of fetch requests (the previous implementation's
     * {@code CreateFetchRequestsEvent}, as a flag). Wakes the loop only if it is parked: under load the loop is
     * awake handling responses and the flag is picked up on its next pass at no cost.
     */
    public void requestFetch() {
        fetchRequested = true;
        signal.wakeupIfParked();
    }

    /** @return the latest application poll sequence for which the reconciliation check has completed */
    public long reconciliationCheckedPollSequence() {
        return reconciliationCheckedPollSequence;
    }

    /** Any thread: the loop's conclusion at the end of its most recent pass. */
    public PassDecision latestDecision() {
        return latestDecision;
    }

    // Visible for testing
    LifecycleSequencer lifecycle() {
        return lifecycle;
    }

    private void ensureAlive() {
        if (initializationError != null)
            throw ConsumerUtils.maybeWrapAsKafkaException(initializationError);
        if (!running && initializationLatch.getCount() == 0)
            throw new IllegalStateException("Consumer event loop is not running");
    }

    // ---- lifecycle ------------------------------------------------------------------------------------------------

    /** Starts the thread and waits for its resources to be initialized. */
    public void start(long timeoutMs) {
        running = true;
        super.start();
        try {
            if (!initializationLatch.await(timeoutMs, TimeUnit.MILLISECONDS))
                throw new KafkaException("Timed out waiting for the consumer event loop to initialize");
        } catch (InterruptedException e) {
            throw new InterruptException("Interrupted while waiting for the consumer event loop to initialize", e);
        }
        if (initializationError != null)
            throw ConsumerUtils.maybeWrapAsKafkaException(initializationError);
    }

    @Override
    public void run() {
        try {
            initializeResources();
        } catch (Throwable t) {
            initializationError = t;
            running = false;
        } finally {
            initializationLatch.countDown();
        }
        try {
            while (running) {
                try {
                    runOnce();
                    lastPassFailed = false;
                } catch (Throwable t) {
                    lastPassFailed = true;
                    logUnexpectedError(t);
                }
            }
        } finally {
            cleanup();
        }
    }

    // Visible for testing: lets tests drive runOnce() on the calling thread without starting the loop thread.
    void initializeResources() {
        applicationEventProcessor = applicationEventProcessorSupplier.get();
        networkClientDelegate = networkClientDelegateSupplier.get();
        requestManagers = requestManagersSupplier.get();
        lifecycle = new LifecycleSequencer(new LogContext(log.getName() + " "), requestManagers);
        for (RequestManager rm : requestManagers.entries()) {
            managerTasks.add(new ManagerTask(rm, networkClientDelegate, timer, () -> pass, () -> stateVersion,
                    this::markManagersDirty, this::markManagerDue));
        }
        long now = time.milliseconds();
        timer.schedule(now, REAPER_INTERVAL_MS, this::reap);
    }

    private void logUnexpectedError(Throwable t) {
        long now = time.milliseconds();
        if (now - lastErrorLogMs >= ERROR_LOG_INTERVAL_MS) {
            if (suppressedErrors > 0)
                log.error("Unexpected error in the consumer event loop ({} similar errors suppressed)", suppressedErrors, t);
            else
                log.error("Unexpected error in the consumer event loop", t);
            lastErrorLogMs = now;
            suppressedErrors = 0;
        } else {
            suppressedErrors++;
            log.debug("Unexpected error in the consumer event loop", t);
        }
    }

    private void markManagersDirty() {
        // A request completed: an input with identity (the request's owner) reached the loop.
        stateVersion++;
        managersDirty = true;
    }

    /** A manager's timer expired: run the due managers, in registration order, in this pass (semantics S6). */
    private void markManagerDue() {
        managersDirty = true;
    }

    void runOnce() {
        signal.markRunning();
        pass++;
        commandProcessedThisPass = false;
        final long now = time.milliseconds();
        if (lastLoopTimeMs != 0L)
            asyncConsumerMetrics.recordTimeBetweenNetworkThreadPoll(now - lastLoopTimeMs);
        lastLoopTimeMs = now;
        asyncConsumerMetrics.recordBackgroundPass();

        processCommands(now);
        housekeepApplicationPoll(now);
        maybeCreateFetchRequests();
        keepPollTimerFreshWhileInPoll(now);
        checkMetadataVersion();
        timer.runExpired(now);
        runDirtyWork(now);
        // Expire tracked events whose deadline has passed. Doing this every pass (not only on the reaper timer)
        // matters for events enqueued with an already-expired deadline, e.g. close(Duration.ZERO): they must fail
        // fast even if the clock does not advance (MockTime in tests).
        if (applicationEventReaper.size() > 0)
            reapExpiredEvents(now);
        propagateMetadataError();
        publishDecision();

        long blockMs = Math.min(timer.timeToNextMs(now), MAX_BLOCK_MS);
        if (!signal.prepareToPark(this::hasPendingWork))
            blockMs = 0;
        if (lastPassFailed)
            blockMs = Math.max(blockMs, ERROR_BACKOFF_MS);
        networkClientDelegate.poll(blockMs, now);
    }

    /**
     * One immutable conclusion per pass, published before the loop blocks. The application thread is woken only
     * when a field it may be waiting for changed, so it never waits on a state it cannot observe (and never spins
     * on one that did not change).
     */
    private void publishDecision() {
        PassDecision previous = latestDecision;
        // SubscriptionState is a monitor shared with the application thread: only take it when this pass could
        // have changed positions, not on the (frequent) passes that merely handled a fetch response.
        boolean allPositionsKnown = positionsMayHaveChanged ? subscriptions.hasAllFetchPositions() : previous.allPositionsKnown;
        positionsMayHaveChanged = false;
        PassDecision decision = new PassDecision(
                pass,
                stateVersion,
                timer.nextDeadlineMs(),
                allPositionsKnown,
                positionsUpdate != null && !positionsUpdate.isDone(),
                reconciliationCheckedPollSequence,
                backgroundEventHandler.size() > 0);
        latestDecision = decision;
        if (decision.applicationVisibleChangeSince(previous))
            onApplicationVisibleChange.run();
    }

    /**
     * While the application thread is blocked inside poll() it is, by definition, polling: the previous
     * implementation re-armed the heartbeat poll timer roughly every 100 ms during a long poll, so a poll timeout
     * longer than max.poll.interval.ms did not make the member leave the group. Same here, throttled to the
     * reaper interval.
     */
    private void keepPollTimerFreshWhileInPoll(long now) {
        if (!applicationInPoll || now - lastPollTimerRefreshMs < REAPER_INTERVAL_MS)
            return;
        lastPollTimerRefreshMs = now;
        requestManagers.consumerHeartbeatRequestManager.ifPresent(hrm -> hrm.resetPollTimer(now));
        requestManagers.streamsGroupHeartbeatRequestManager.ifPresent(hrm -> hrm.resetPollTimer(now));
    }

    /**
     * Metadata arrives through the network client itself, not through a request future, so it cannot mark the
     * managers dirty by itself; managers that were waiting for a leader or coordinator address need a re-run.
     */
    private void checkMetadataVersion() {
        int metadataVersion = metadata.updateVersion();
        if (metadataVersion != lastMetadataVersion) {
            lastMetadataVersion = metadataVersion;
            stateVersion++;
            managersDirty = true;
            positionsMayHaveChanged = true;
            maybeUpdateFetchPositions(time.milliseconds(), true);
        }
    }

    private void runDirtyWork(long now) {
        boolean ran = false;
        if (managersDirty) {
            managersDirty = false;
            ran = runManagers(now);
        }
        if (ran) {
            positionsMayHaveChanged = true;
            // Managers may have changed the assignment or positions (reconciliation, offset reset/validation);
            // newly assigned partitions need positions before they can be fetched, without waiting for a poll.
            maybeUpdateFetchPositions(now, false);
        }
    }

    /**
     * Creates the next round of fetch requests when the application asked for one. The unchanged
     * {@link FetchRequestManager} only builds requests while such a request is pending, and sends them from its
     * {@code poll()}, hence the manager pass.
     */
    private void maybeCreateFetchRequests() {
        if (!fetchRequested || !fetchingEnabled)
            return;
        fetchRequested = false;
        requestManagers.fetchRequestManager.createFetchRequests();
        // The application's request for the next fetch is an input with identity: the fetch manager (which
        // declares ANY_INPUT) must see a newer version or the verification in ManagerTask would hold it back.
        stateVersion++;
        managersDirty = true;
    }

    /**
     * Runs every manager whose declared trigger fired (semantics S1; each at most once per pass). A manager that
     * throws does not stop the others from getting their turn; the first failure is rethrown afterwards so the
     * loop's error handling (rate-limited log, back-off) still applies.
     */
    private boolean runManagers(long now) {
        boolean ran = false;
        int runsWithoutRequests = 0;
        RuntimeException failure = null;
        for (ManagerTask task : managerTasks) {
            if (!task.wantsRun(commandProcessedThisPass))
                continue;
            try {
                if (task.run(now)) {
                    ran = true;
                    if (task.lastRunSentNothing())
                        runsWithoutRequests++;
                }
            } catch (RuntimeException e) {
                if (failure == null)
                    failure = e;
                else
                    log.debug("Additional request manager failure in the same pass", e);
            }
        }
        asyncConsumerMetrics.recordManagerRunsWithoutRequests(runsWithoutRequests);
        if (failure != null)
            throw failure;
        return ran;
    }

    /**
     * A new application poll is deliberately not "pending work": its bookkeeping rides on the next pass the loop
     * makes anyway (a response under load, a manager timer when idle). The application thread wakes the loop
     * explicitly in the cases where waiting would matter (missing positions, pending reconciliation).
     */
    private boolean hasPendingWork() {
        return !commands.isEmpty() || managersDirty;
    }

    private void processCommands(long now) {
        Object command;
        while ((command = commands.poll()) != null) {
            try {
                if (command instanceof ApplicationEvent) {
                    ApplicationEvent event = (ApplicationEvent) command;
                    asyncConsumerMetrics.recordApplicationEventQueueTime(now - event.enqueuedMs());
                    if (event instanceof CompletableEvent)
                        applicationEventReaper.add((CompletableEvent<?>) event);
                    if (event instanceof MetadataErrorNotifiableEvent && maybeFailOnMetadataError(List.of(event)))
                        continue;
                    if (!lifecycle.apply(event))
                        applicationEventProcessor.process(event);
                } else {
                    ((Runnable) command).run();
                }
                stateVersion++;
                commandProcessedThisPass = true;
                positionsMayHaveChanged = true;
            } catch (Throwable t) {
                log.warn("Error processing command {}", command, t);
            }
        }
        if (commandProcessedThisPass)
            managersDirty = true;
    }

    /**
     * The bookkeeping the previous implementation did for every {@code AsyncPollEvent}, done once per loop pass
     * when the application has polled since the last pass: reconciliation check, auto-commit timer, heartbeat poll
     * timer, pattern-subscription refresh, and fetch-position initialization.
     */
    private void housekeepApplicationPoll(long now) {
        long pollSequence = applicationPollSequence;
        if (pollSequence == lastHousekeptPollSequence)
            return;
        lastHousekeptPollSequence = pollSequence;
        long pollMs = lastApplicationPollMs;
        fetchingEnabled = true;
        // Every application poll asks for the next fetch, as the previous implementation's per-poll event did.
        fetchRequested = true;

        requestManagers.consumerMembershipManager.ifPresent(mm -> mm.maybeReconcile(true));
        reconciliationCheckedPollSequence = pollSequence;

        if (requestManagers.commitRequestManager.isPresent()) {
            requestManagers.commitRequestManager.get().updateTimerAndMaybeCommit(pollMs);
            requestManagers.consumerHeartbeatRequestManager.ifPresent(hrm -> {
                if (subscriptions.hasPatternSubscription())
                    applicationEventProcessor.process(new UpdatePatternSubscriptionEvent(now + defaultApiTimeoutMs));
                hrm.membershipManager().onConsumerPoll();
                hrm.resetPollTimer(pollMs);
            });
            requestManagers.streamsGroupHeartbeatRequestManager.ifPresent(hrm -> {
                hrm.membershipManager().onConsumerPoll();
                hrm.resetPollTimer(pollMs);
            });
        }

        maybeUpdateFetchPositions(now, false);
        // An application poll is an input with identity: the per-poll bookkeeping above changed manager state.
        stateVersion++;
        managersDirty = true;
    }

    /**
     * Starts resolving fetch positions (committed offsets, offset reset, validation) when some assigned partition
     * lacks one. One attempt may complete without resolving everything, for example while a leader is unknown; the
     * previous implementation simply retried on every poll event. Here an attempt is made when the application
     * polls, whenever metadata changes, and after a manager pass, which is when a new attempt can make progress.
     *
     * <p>An attempt that completes at once made no request and changed nothing, so it must not trigger another
     * pass by itself: that would spin the loop for as long as a leader is unknown or a reset is backing off. Only
     * an attempt with requests outstanding triggers a manager pass (to send them); completion marks the fetch
     * pipeline dirty, whose positions may now be valid.
     *
     * @param metadataChanged {@code true} to run even when every partition has a position: positions then still
     *                        need re-validation against the new leader epochs (the manager decides what to do)
     */
    private void maybeUpdateFetchPositions(long now, boolean metadataChanged) {
        if (!fetchingEnabled || (positionsUpdate != null && !positionsUpdate.isDone()))
            return;
        if (!metadataChanged && subscriptions.hasAllFetchPositions())
            return;
        positionsUpdate = requestManagers.offsetsRequestManager.updateFetchPositions(now + defaultApiTimeoutMs);
        final boolean[] asynchronous = {false};
        if (!positionsUpdate.isDone()) {
            // Requests were queued inside the offsets / commit managers; they leave on the next manager pass.
            // The attempt is an input with identity for the managers that must send them.
            stateVersion++;
            managersDirty = true;
        }
        positionsUpdate.whenComplete((ignored, error) -> {
            // Positions resolved by a response: create fetch requests for them without waiting for the next poll,
            // as the previous implementation chained them after the positions update. An attempt that completed
            // at once made no progress and must not trigger anything (that would spin, see above).
            positionsMayHaveChanged = true;
            if (asynchronous[0])
                fetchRequested = true;
            Throwable cause = error instanceof CompletionException ? error.getCause() : error;
            if (cause == null || cause instanceof org.apache.kafka.common.errors.TimeoutException
                    || cause instanceof java.util.concurrent.TimeoutException)
                return; // positions are retried on the next poll or metadata change; timeouts are not surfaced
            // Not a plain ErrorEvent: this attempt was not requested by a poll(), so the application thread checks
            // that positions are still missing before surfacing it (a seek may have happened in between).
            backgroundEventHandler.add(new FetchPositionsErrorEvent(ConsumerUtils.maybeWrapAsKafkaException(cause),
                    () -> !subscriptions.hasAllFetchPositions()));
        });
        asynchronous[0] = true;
    }

    private void reap() {
        long now = timer.lastRunTimeMs();
        try {
            reapExpiredEvents(now);
        } finally {
            timer.schedule(now, REAPER_INTERVAL_MS, this::reap);
        }
    }

    private void reapExpiredEvents(long now) {
        asyncConsumerMetrics.recordApplicationEventExpiredSize(applicationEventReaper.reap(now));
    }

    /**
     * A metadata error (invalid topic, unauthorized topic, bootstrap failure) is stored by the network client
     * delegate; hand it to every pending event that wants it and surface it to the application thread through the
     * background queue, which is where the previous implementation's per-poll event would have thrown it.
     */
    private void propagateMetadataError() {
        Optional<Exception> metadataError = networkClientDelegate.getAndClearMetadataError();
        if (metadataError.isEmpty())
            return;
        for (CompletableEvent<?> event : applicationEventReaper.uncompletedEvents()) {
            if (event instanceof MetadataErrorNotifiableEvent)
                ((MetadataErrorNotifiableEvent) event).onMetadataError(metadataError.get());
        }
        backgroundEventHandler.add(new ErrorEvent(metadataError.get()));
    }

    private boolean maybeFailOnMetadataError(List<?> events) {
        List<MetadataErrorNotifiableEvent> notifiable = new ArrayList<>();
        for (Object obj : events) {
            if (obj instanceof MetadataErrorNotifiableEvent)
                notifiable.add((MetadataErrorNotifiableEvent) obj);
        }
        if (notifiable.isEmpty())
            return false;
        Optional<Exception> metadataError = networkClientDelegate.getAndClearMetadataError();
        if (metadataError.isEmpty())
            return false;
        notifiable.forEach(e -> e.onMetadataError(metadataError.get()));
        return true;
    }

    @Override
    public void close() {
        close(Duration.ofMillis(ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS));
    }

    public void close(Duration timeout) {
        if (!running && initializationLatch.getCount() > 0)
            return;
        log.trace("Signaling the consumer event loop to close in {} ms", timeout.toMillis());
        running = false;
        closeTimeout = timeout;
        signal.wakeupIfParked();
        NetworkClientDelegate delegate = networkClientDelegate;
        if (delegate != null)
            delegate.wakeup();
        try {
            join();
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for the consumer event loop to complete", e);
        }
    }

    private void cleanup() {
        log.trace("Closing the consumer event loop");
        Timer closeTimer = time.timer(closeTimeout);
        try {
            if (requestManagers != null && networkClientDelegate != null)
                lifecycle.shutdown(time.milliseconds(), networkClientDelegate);
        } catch (Exception e) {
            log.error("Unexpected error during shutdown. Proceed with closing.", e);
        } finally {
            if (networkClientDelegate != null)
                sendUnsentRequests(closeTimer);
            List<CompletableEvent<?>> pending = new ArrayList<>();
            Object command;
            while ((command = commands.poll()) != null) {
                if (command instanceof CompletableEvent)
                    pending.add((CompletableEvent<?>) command);
            }
            asyncConsumerMetrics.recordApplicationEventExpiredSize(applicationEventReaper.reap(pending));
            closeQuietly(requestManagers, "request managers");
            closeQuietly(networkClientDelegate, "network client delegate");
            log.debug("Closed the consumer event loop");
        }
    }

    private void sendUnsentRequests(Timer closeTimer) {
        if (!networkClientDelegate.hasAnyPendingRequests())
            return;
        do {
            networkClientDelegate.poll(closeTimer.remainingMs(), closeTimer.currentTimeMs(), true);
            closeTimer.update();
        } while (closeTimer.notExpired() && networkClientDelegate.hasAnyPendingRequests());
        if (networkClientDelegate.hasAnyPendingRequests()) {
            log.warn("Close timeout of {} ms expired before the consumer event loop was able to complete pending requests. " +
                    "Inflight request count: {}, Unsent request count: {}", closeTimer.timeoutMs(),
                    networkClientDelegate.inflightRequestCount(), networkClientDelegate.unsentRequests().size());
        }
    }
}
