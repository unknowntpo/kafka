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
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerHeartbeatRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager;
import org.apache.kafka.clients.consumer.internals.FetchMetricsManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager;
import org.apache.kafka.clients.consumer.internals.MemberState;
import org.apache.kafka.clients.consumer.internals.MemberStateListener;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.OffsetCommitCallbackInvoker;
import org.apache.kafka.clients.consumer.internals.OffsetsRequestManager;
import org.apache.kafka.clients.consumer.internals.PositionsValidator;
import org.apache.kafka.clients.consumer.internals.RequestManager;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.clients.consumer.ng.loop.LoopSignal;
import org.apache.kafka.clients.consumer.ng.loop.LoopTimer;
import org.apache.kafka.clients.consumer.ng.loop.ManagerTask;
import org.apache.kafka.clients.consumer.ng.loop.PassDecision;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.KafkaThread;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * The I/O thread of the next-generation consumer (CONSUMER-NG-04 §2). It owns the network client, the protocol
 * state machines reused from the clients module (coordinator, commit, membership, heartbeat, offsets), the
 * {@link FetchPipeline}, and the input-driven scheduler ported from the previous line: a manager runs only when
 * one of its declared inputs arrived or its own timer expired, never on a fixed cadence.
 *
 * <p>The application thread talks to the engine through {@link #submit commands} (subscribe, commit, seek,
 * close: things that are genuinely requests) and through the poll bookkeeping ({@link #onApplicationPoll},
 * {@link #onApplicationPollIteration}), which are volatile writes rather than events. The engine talks back
 * through the {@link BackgroundEventHandler} (assignment and error events) and through one immutable
 * {@link PassDecision} per pass, waking the application thread only when a field it may be waiting for changed.
 */
public final class ConsumerEngine implements AutoCloseable {

    private static final long REAPER_INTERVAL_MS = 100;
    private static final long MAX_BLOCK_MS = 60_000;
    private static final long DEFAULT_CLOSE_TIMEOUT_MS = 30_000L;
    private static final long ERROR_BACKOFF_MS = 100;

    private final Logger log;
    private final Time time;
    private final LogContext logContext;
    final SubscriptionState subscriptions;
    final ConsumerMetadata metadata;
    final Metrics metrics;
    private final NetworkClient client;
    private final Selector selector;
    private final NetworkClientDelegate network;
    private final DirectBufferPool pool;
    final FetchPipeline fetch;
    final BlockingQueue<BackgroundEvent> backgroundQueue;
    final BackgroundEventHandler backgroundEventHandler;
    final Optional<CoordinatorRequestManager> coordinatorManager;
    final Optional<CommitRequestManager> commitManager;
    final Optional<ConsumerMembershipManager> membershipManager;
    final Optional<ConsumerHeartbeatRequestManager> heartbeatManager;
    final OffsetsRequestManager offsetsManager;
    private final List<ManagerTask> tasks = new ArrayList<>();
    private final LoopTimer timer;
    private final LoopSignal signal;
    private final KafkaThread thread;
    private final long defaultApiTimeoutMs;

    private final ConcurrentLinkedQueue<Runnable> commands = new ConcurrentLinkedQueue<>();
    private final AtomicLong pass = new AtomicLong();
    private final AtomicLong stateVersion = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Runnable onApplicationVisibleChange;
    private volatile long applicationPollSequence;
    private volatile long lastApplicationPollMs;
    private volatile boolean applicationInPoll;
    /** Application thread is in {@code position()} and needs the positions machinery to run. */
    private volatile boolean applicationWantsPositions;
    /** Counts poll() and position() calls, so a positions error is raised at most once per application call. */
    private final AtomicLong applicationCallSequence = new AtomicLong();
    private long positionsErrorRaisedForCall = -1;
    private volatile PassDecision latestDecision = PassDecision.NONE;
    private volatile boolean reconciling;

    private long lastHousekeptPollSequence = -1;
    private long lastPollTimerRefreshMs;
    private int lastMetadataVersion;
    private int lastAssignmentId = -1;
    private boolean managersDirty;
    private boolean commandProcessedThisPass;
    private boolean lastPassFailed;
    private volatile boolean positionsMayHaveChanged = true;
    private CompletableFuture<Void> positionsUpdate;
    private volatile long closeTimeoutMs = DEFAULT_CLOSE_TIMEOUT_MS;
    private boolean closing;

    public ConsumerEngine(ConsumerConfig config, LogContext logContext, Time time, Metrics metrics,
                          SubscriptionState subscriptions, OffsetCommitCallbackInvoker commitCallbackInvoker,
                          MemberStateListener applicationMemberStateListener, FetchMetricsManager fetchMetricsManager, long creditBytes,
                          Runnable onApplicationVisibleChange) {
        this.log = logContext.logger(ConsumerEngine.class);
        this.logContext = logContext;
        this.time = time;
        this.metrics = metrics;
        this.subscriptions = subscriptions;
        this.onApplicationVisibleChange = onApplicationVisibleChange;
        this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config, GroupRebalanceConfig.ProtocolType.CONSUMER);

        this.metadata = new ConsumerMetadata(config, subscriptions, logContext, new ClusterResourceListeners());
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG));
        metadata.bootstrap(addresses);

        this.pool = new DirectBufferPool(creditBytes + 2L * config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG), this::memoryReleased);
        // One ApiVersions instance shared by the network client (which fills it) and the managers (which read it).
        ApiVersions apiVersions = new ApiVersions();
        EngineNetwork.Created created = EngineNetwork.createNetworkClient(config, logContext, time, metrics, metadata, pool, apiVersions,
                fetchMetricsManager.throttleTimeSensor());
        this.client = created.client();
        this.selector = created.selector();

        this.backgroundQueue = new LinkedBlockingQueue<>();
        AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, ConsumerUtils.CONSUMER_METRIC_GROUP);
        this.backgroundEventHandler = new BackgroundEventHandler(backgroundQueue, time, asyncConsumerMetrics);
        this.network = new NetworkClientDelegate(time, config, logContext, client, metadata, backgroundEventHandler, false, asyncConsumerMetrics);

        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        int requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        IsolationLevel isolationLevel = IsolationLevel.valueOf(config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT));
        CoordinatorRequestManager coordinator = null;
        CommitRequestManager commit = null;
        ConsumerMembershipManager membership = null;
        ConsumerHeartbeatRequestManager heartbeat = null;
        if (groupRebalanceConfig.groupId != null) {
            coordinator = new CoordinatorRequestManager(logContext, retryBackoffMs, retryBackoffMaxMs, groupRebalanceConfig.groupId);
            commit = new CommitRequestManager(time, logContext, subscriptions, config, coordinator, commitCallbackInvoker,
                    groupRebalanceConfig.groupId, groupRebalanceConfig.groupInstanceId, metrics, metadata);
            membership = new ConsumerMembershipManager(groupRebalanceConfig.groupId, groupRebalanceConfig.groupInstanceId,
                    groupRebalanceConfig.rackId, groupRebalanceConfig.rebalanceTimeoutMs,
                    Optional.ofNullable(config.getString(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG)),
                    subscriptions, commit, metadata, logContext, backgroundEventHandler, time, metrics,
                    config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
            membership.registerStateListener(commit);
            membership.registerStateListener(applicationMemberStateListener);
            membership.registerStateListener(new MemberStateListener() {
                @Override
                public void onMemberEpochUpdated(Optional<Integer> memberEpoch, String memberId) {
                }

                @Override
                public void onMemberStateChange(MemberState state) {
                    reconciling = state == MemberState.RECONCILING;
                }
            });
            heartbeat = new ConsumerHeartbeatRequestManager(logContext, time, config, coordinator, subscriptions, membership,
                    backgroundEventHandler, metrics);
        }
        this.coordinatorManager = Optional.ofNullable(coordinator);
        this.commitManager = Optional.ofNullable(commit);
        this.membershipManager = Optional.ofNullable(membership);
        this.heartbeatManager = Optional.ofNullable(heartbeat);
        PositionsValidator positionsValidator = new PositionsValidator(logContext, time, subscriptions, metadata);
        this.offsetsManager = new OffsetsRequestManager(subscriptions, metadata, isolationLevel, time, retryBackoffMs, requestTimeoutMs,
                (int) defaultApiTimeoutMs, apiVersions, network, commit, positionsValidator, logContext);

        this.fetch = new FetchPipeline(config, logContext, subscriptions, metadata, client, pool, creditBytes, this::onData,
                e -> backgroundEventHandler.add(new ErrorEvent(e)), fetchMetricsManager);
        this.timer = new LoopTimer();
        this.signal = new LoopSignal(client::wakeup);
        for (RequestManager rm : managers())
            tasks.add(new ManagerTask(rm, network, timer, pass::get, stateVersion::get, this::markManagersDirty, this::markManagersDirty));
        this.thread = new KafkaThread("ng-consumer-" + clientId, this::run, true);
    }

    private List<RequestManager> managers() {
        List<RequestManager> list = new ArrayList<>();
        coordinatorManager.ifPresent(list::add);
        commitManager.ifPresent(list::add);
        heartbeatManager.ifPresent(list::add);
        membershipManager.ifPresent(list::add);
        list.add(offsetsManager);
        return list;
    }

    public void start() {
        thread.start();
    }

    // ---- application thread API --------------------------------------------------------------------------------

    /** Runs {@code command} on the I/O thread; the returned future completes with the future the command returns. */
    public <T> CompletableFuture<T> submit(Function<ConsumerEngine, CompletableFuture<T>> command) {
        CompletableFuture<T> result = new CompletableFuture<>();
        commands.add(() -> {
            try {
                CompletableFuture<T> inner = command.apply(this);
                if (inner == null)
                    result.complete(null);
                else
                    inner.whenComplete((value, error) -> {
                        if (error != null)
                            result.completeExceptionally(error instanceof CompletionException ? error.getCause() : error);
                        else
                            result.complete(value);
                    });
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        signal.wakeupIfParked();
        return result;
    }

    /** Runs {@code command} on the I/O thread without waiting for it. */
    public void execute(Runnable command) {
        commands.add(command);
        signal.wakeupIfParked();
    }

    /** Application thread: a new {@code poll()} call. */
    public long onApplicationPoll(long pollTimeMs) {
        lastApplicationPollMs = pollTimeMs;
        applicationInPoll = true;
        applicationCallSequence.incrementAndGet();
        long sequence = ++applicationPollSequence;
        if (reconciling || !latestDecision.allPositionsKnown)
            signal.wakeupIfParked();
        return sequence;
    }

    /** Application thread: one wait iteration of {@code poll()} ended without records; the next one is a poll input (R11). */
    public long onApplicationPollIteration(long pollTimeMs) {
        lastApplicationPollMs = pollTimeMs;
        long sequence = ++applicationPollSequence;
        signal.wakeupIfParked();
        return sequence;
    }

    public void onApplicationPollReturn() {
        applicationInPoll = false;
    }

    /** Application thread: {@code position()} starts or stops waiting for a fetch position (same rules as in poll). */
    public void applicationWantsPositions(boolean wants) {
        applicationWantsPositions = wants;
        if (wants) {
            applicationCallSequence.incrementAndGet();
            signal.wakeupIfParked();
        }
    }

    public PassDecision latestDecision() {
        return latestDecision;
    }

    public boolean reconciling() {
        return reconciling;
    }

    public void wakeup() {
        signal.wakeupIfParked();
    }

    /** Application thread: a fetch segment was delivered and released. */
    public void released(FetchSegment segment, int deliveredRecords) {
        fetch.released(segment, deliveredRecords);
        if (fetch.starved())
            signal.wakeupIfParked();
    }

    public DirectBufferPool pool() {
        return pool;
    }

    private void memoryReleased() {
        if (fetch.starved() && Thread.currentThread() != thread)
            signal.wakeupIfParked();
    }

    private void onData() {
        onApplicationVisibleChange.run();
    }

    @Override
    public void close() {
        close(DEFAULT_CLOSE_TIMEOUT_MS);
    }

    /** Stops the I/O thread; pending requests (leave group, last commit, in-flight fetch) get up to {@code timeoutMs}. */
    public void close(long timeoutMs) {
        if (running.compareAndSet(true, false)) {
            closeTimeoutMs = Math.max(0, timeoutMs);
            signal.wakeupIfParked();
            try {
                thread.join(30_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ---- I/O thread ----------------------------------------------------------------------------------------------

    private void markManagersDirty() {
        managersDirty = true;
    }

    private void run() {
        try {
            while (running.get()) {
                try {
                    runOnce();
                    lastPassFailed = false;
                } catch (Throwable t) {
                    lastPassFailed = true;
                    log.error("Unexpected error in the consumer engine", t);
                }
            }
            shutdown();
        } catch (Throwable t) {
            log.error("Consumer engine failed", t);
        }
    }

    private void runOnce() {
        signal.markRunning();
        pass.incrementAndGet();
        commandProcessedThisPass = false;
        long now = time.milliseconds();

        processCommands();
        housekeepApplicationPoll(now);
        syncAssignment();
        checkMetadataVersion();
        if (timer.runExpired(now) > 0)
            managersDirty = true;
        keepPollTimerFreshWhileInPoll(now);
        if (applicationWantsPositions && positionsUpdateWanted())
            managersDirty = true;
        if (managersDirty) {
            managersDirty = false;
            runManagers(now);
            positionsMayHaveChanged = true;
            maybeUpdateFetchPositions(now);
        }
        fetch.sendFetches(now);
        publishDecision();

        long blockMs = Math.min(Math.min(timer.timeToNextMs(now), MAX_BLOCK_MS), fetch.fetchMaxWaitMs() + 1_000L);
        if (!signal.prepareToPark(this::hasPendingWork))
            blockMs = 0;
        if (lastPassFailed)
            blockMs = Math.max(blockMs, ERROR_BACKOFF_MS);
        network.poll(blockMs, now);
        releaseUnclaimedReceives();
        checkCoordinatorConnection(time.milliseconds());
        fetch.handleResponses();
    }

    /**
     * Same rule as the classic consumer ({@code AbstractCoordinator.checkAndGetCoordinator}): a coordinator whose
     * connection dropped is unknown, so requests wait for rediscovery instead of failing against a dead node.
     */
    private void checkCoordinatorConnection(long now) {
        if (coordinatorManager.isEmpty())
            return;
        Optional<Node> node = coordinatorManager.get().coordinator();
        if (node.isPresent() && client.connectionFailed(node.get()) && client.connectionDelay(node.get(), now) > 0) {
            coordinatorManager.get().markCoordinatorUnknown("coordinator connection lost", now);
            stateVersion.incrementAndGet();
            managersDirty = true;
        }
    }

    private boolean hasPendingWork() {
        return managersDirty || !commands.isEmpty() || applicationPollSequence != lastHousekeptPollSequence;
    }

    private void processCommands() {
        Runnable cmd;
        while ((cmd = commands.poll()) != null) {
            try {
                cmd.run();
            } catch (Throwable t) {
                log.error("Command failed", t);
            }
            commandProcessedThisPass = true;
        }
        if (commandProcessedThisPass) {
            stateVersion.incrementAndGet();
            managersDirty = true;
        }
    }

    /**
     * The bookkeeping the previous implementation did for every poll event, once per poll iteration: reconciliation
     * steps that only the poll path may run, the auto-commit and heartbeat poll timers, fetch position initialization.
     */
    private void housekeepApplicationPoll(long now) {
        long sequence = applicationPollSequence;
        if (sequence == lastHousekeptPollSequence)
            return;
        lastHousekeptPollSequence = sequence;
        long pollMs = lastApplicationPollMs;
        fetch.resumeAfterErrors();
        boolean membershipChanged = false;
        if (membershipManager.isPresent()) {
            ConsumerMembershipManager mm = membershipManager.get();
            MemberState before = mm.state();
            mm.maybeReconcile(true);
            mm.onConsumerPoll();
            membershipChanged = mm.state() != before;
        }
        if (commitManager.isPresent()) {
            commitManager.get().updateTimerAndMaybeCommit(pollMs);
            taskOf(commitManager.get()).trigger();
        }
        heartbeatManager.ifPresent(hrm -> hrm.resetPollTimer(pollMs));
        maybeUpdateFetchPositions(now);
        if (membershipChanged)
            stateVersion.incrementAndGet();
        managersDirty = true;
    }

    private ManagerTask taskOf(RequestManager manager) {
        for (ManagerTask t : tasks)
            if (t.manager() == manager)
                return t;
        throw new IllegalStateException("no task for " + manager);
    }

    private void keepPollTimerFreshWhileInPoll(long now) {
        if (!applicationInPoll || now - lastPollTimerRefreshMs < REAPER_INTERVAL_MS)
            return;
        lastPollTimerRefreshMs = now;
        heartbeatManager.ifPresent(hrm -> hrm.resetPollTimer(now));
    }

    private void syncAssignment() {
        int id = subscriptions.assignmentId();
        if (id != lastAssignmentId) {
            lastAssignmentId = id;
            fetch.syncAssignment();
            positionsMayHaveChanged = true;
        }
    }

    private void checkMetadataVersion() {
        int version = metadata.updateVersion();
        if (version != lastMetadataVersion) {
            lastMetadataVersion = version;
            stateVersion.incrementAndGet();
            managersDirty = true;
            positionsMayHaveChanged = true;
        }
    }

    private void runManagers(long now) {
        for (ManagerTask task : tasks) {
            if (!task.wantsRun(commandProcessedThisPass)) {
                if (log.isTraceEnabled())
                    log.trace("pass {} skip {} (declared {}, version {})", pass.get(), task.manager().getClass().getSimpleName(), task.declared(), stateVersion.get());
                continue;
            }
            task.run(now);
            if (log.isTraceEnabled())
                log.trace("pass {} ran {} sent={} version {}", pass.get(), task.manager().getClass().getSimpleName(), !task.lastRunSentNothing(), stateVersion.get());
        }
    }

    /**
     * Positions are fetched on behalf of an application call (poll or position), as before: the errors it can raise
     * (no reset policy, authorization) belong to that call, and an idle consumer does not keep retrying in the background.
     */
    private boolean positionsUpdateWanted() {
        if (closing || (positionsUpdate != null && !positionsUpdate.isDone()))
            return false;
        if (!applicationInPoll && !applicationWantsPositions)
            return false;
        return !subscriptions.hasAllFetchPositions();
    }

    private void maybeUpdateFetchPositions(long now) {
        if (!positionsUpdateWanted())
            return;
        positionsUpdate = offsetsManager.updateFetchPositions(now + defaultApiTimeoutMs);
        if (positionsUpdate.isDone()) {
            // Nothing was queued (e.g. a partition is waiting out a validation/reset backoff, or the reset policy is
            // missing): the next input (poll iteration, metadata change, timer) retries; re-running managers now would spin.
            positionsMayHaveChanged = true;
            positionsUpdate.whenComplete((ignored, error) -> onPositionsUpdateDone(error));
            return;
        }
        // Requests were queued on the managers: run them so the requests go out.
        stateVersion.incrementAndGet();
        managersDirty = true;
        positionsUpdate.whenComplete((ignored, error) -> {
            positionsMayHaveChanged = true;
            managersDirty = true;
            onPositionsUpdateDone(error);
        });
    }

    private void onPositionsUpdateDone(Throwable error) {
        if (error == null)
            return;
        Throwable cause = error instanceof CompletionException ? error.getCause() : error;
        if (cause instanceof org.apache.kafka.common.errors.TimeoutException || cause instanceof java.util.concurrent.TimeoutException)
            return;
        raisePositionsError(cause);
    }

    /**
     * One error per application call: the call that triggered the update sees it (or, when it already returned as
     * {@code poll(0)} does, the next call), and later calls start clean.
     */
    private void raisePositionsError(Throwable cause) {
        long call = applicationCallSequence.get();
        if (positionsErrorRaisedForCall == call)
            return;
        positionsErrorRaisedForCall = call;
        backgroundEventHandler.add(new ErrorEvent(cause));
    }

    /**
     * The network layer reads every response into a pooled buffer. Fetch responses keep theirs (records point into
     * them, see {@link FetchSegment.Owner}); every other response was fully parsed during the poll, so its buffer goes
     * back to the pool here.
     */
    private void releaseUnclaimedReceives() {
        for (NetworkReceive receive : selector.completedReceives()) {
            ByteBuffer payload = receive.payload();
            if (!fetch.claimed(payload))
                pool.releaseIfPooled(payload);
        }
    }

    private void publishDecision() {
        PassDecision previous = latestDecision;
        boolean allPositionsKnown = positionsMayHaveChanged ? subscriptions.hasAllFetchPositions() : previous.allPositionsKnown;
        positionsMayHaveChanged = false;
        PassDecision decision = new PassDecision(pass.get(), stateVersion.get(), timer.nextDeadlineMs(), allPositionsKnown,
                positionsUpdate != null && !positionsUpdate.isDone(), lastHousekeptPollSequence,
                !backgroundQueue.isEmpty(), reconciling);
        latestDecision = decision;
        if (decision.applicationVisibleChangeSince(previous))
            onApplicationVisibleChange.run();
    }

    /** Close sequence on the I/O thread: commit on close, leave the group, then flush what the managers still send. */
    private void shutdown() {
        closing = true;
        Timer timer = time.timer(closeTimeoutMs);
        try {
            for (RequestManager rm : managers())
                network.addAll(rm.pollOnClose(timer.currentTimeMs()).unsentRequests);
            // Same as the previous network thread: poll until the pending requests are done or the close timer runs out.
            while (network.hasAnyPendingRequests() && timer.notExpired()) {
                network.poll(timer.remainingMs(), timer.currentTimeMs(), true);
                releaseUnclaimedReceives();
                timer.update();
            }
            if (network.hasAnyPendingRequests())
                log.warn("Close timeout of {} ms expired with {} request(s) still in flight", timer.timeoutMs(), network.inflightRequestCount());
        } finally {
            Utils.closeQuietly(network, "network client delegate");
        }
    }

    // ---- commands used by the application side (run on the I/O thread) --------------------------------------------

    void subscribe(Set<String> topics) {
        if (subscriptions.subscribe(topics))
            metadata.requestUpdateForNewTopics();
        membershipManager.ifPresent(ConsumerMembershipManager::onSubscriptionUpdated);
    }

    void assign(Set<TopicPartition> partitions) {
        if (subscriptions.assignFromUser(partitions))
            metadata.requestUpdateForNewTopics();
    }

    void seekReset(TopicPartition partition) {
        fetch.reset(partition);
        positionsMayHaveChanged = true;
    }

    CompletableFuture<Void> leaveGroupOnClose(org.apache.kafka.clients.consumer.CloseOptions.GroupMembershipOperation operation) {
        if (membershipManager.isEmpty())
            return CompletableFuture.completedFuture(null);
        return membershipManager.get().leaveGroupOnClose(operation);
    }
}
