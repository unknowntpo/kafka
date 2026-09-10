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

package org.apache.kafka.jmh.consumer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.ConsumerHeartbeatRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager;
import org.apache.kafka.clients.consumer.internals.Deserializers;
import org.apache.kafka.clients.consumer.internals.Fetch;
import org.apache.kafka.clients.consumer.internals.FetchBuffer;
import org.apache.kafka.clients.consumer.internals.FetchCollector;
import org.apache.kafka.clients.consumer.internals.FetchConfig;
import org.apache.kafka.clients.consumer.internals.FetchMetricsManager;
import org.apache.kafka.clients.consumer.internals.FetchRequestManager;
import org.apache.kafka.clients.consumer.internals.MemberState;
import org.apache.kafka.clients.consumer.internals.MemberStateListener;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.PositionsValidator;
import org.apache.kafka.clients.consumer.internals.RequestManager;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.MetadataErrorNotifiableEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionsAssignedEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionsRemovedEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Measures the cost of one pass of the async consumer background loop
 * ({@code ConsumerNetworkThread.runOnce()}) with the real request managers and
 * {@link NetworkClientDelegate} wired over a {@link MockClient}, so there is no broker, no socket and no
 * application thread. Time is a {@link MockTime} that advances {@value #TIME_STEP_MS} ms per pass.
 *
 * <p><b>This benchmark replicates {@code ConsumerNetworkThread.runOnce()}</b> in {@link #runOnceReplica}
 * because {@code runOnce()} is package-private. Keep {@link #runOnceReplica} in sync with
 * {@code ConsumerNetworkThread.runOnce()} whenever the loop changes.
 *
 * <p>The benchmark also plays the role of the application thread where the background loop needs it:
 * it resets the heartbeat poll timer, applies the assignment delivered through the background event queue,
 * asks for fetch requests and drains the {@link FetchBuffer} through {@link FetchCollector}.
 *
 * <p>Scenarios (see {@link Scenario}):
 * <ul>
 *     <li>{@code IDLE}: member STABLE with one assigned partition and a known coordinator; no fetch demand.
 *     A pass should find nothing to do. Heartbeats sent every {@value #HEARTBEAT_INTERVAL_MS} ms of mock
 *     time are answered.</li>
 *     <li>{@code BLOCKED}: the FindCoordinator request is never answered. The request times out after
 *     {@code request.timeout.ms} of mock time and is retried, so the scenario cycles between
 *     "request in flight" and "backoff".</li>
 *     <li>{@code BLOCKED_HEARTBEAT_INFLIGHT}: the coordinator is known but the first heartbeat is never
 *     answered (same timeout cycle as {@code BLOCKED}; FindCoordinator is always answered).</li>
 *     <li>{@code CONSUME}: a fetch is requested every pass. The fetch request sent in pass N is answered
 *     after pass N with {@code recordsPerFetch} records, delivered by the network poll of pass N+1 and
 *     drained after pass N+1. Each fetch therefore takes two passes; see the {@code fetchResponses} and
 *     {@code recordsCollected} counters.</li>
 *     <li>{@code RECOVERY}: the coordinator is marked unknown; after the coordinator manager's backoff the
 *     FindCoordinator request is sent and stays unanswered for {@code recoveryPasses} passes, then the
 *     response is enqueued, consumed by the next pass, and the cycle restarts. Only the FindCoordinator
 *     round trip is exercised; no heartbeat is sent during the single pass the coordinator is known.</li>
 * </ul>
 *
 * <p>Auxiliary counters are {@link AuxCounters.Type#EVENTS}: JMH reports the sum over all measurement
 * iterations (and threads). Divide the counts by {@code passes} to get per-pass ratios. Values that are not
 * counts ({@code minNetworkTimeoutMs}, {@code lastMaximumTimeToWaitMs}) are also summed across
 * iterations, so divide them by the number of measurement iterations; {@code -1} means "wait forever".
 */
@State(Scope.Thread)
@Fork(1)
@Threads(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public class ConsumerNetworkThreadPassBenchmark {

    public enum Scenario {
        IDLE,
        BLOCKED,
        BLOCKED_HEARTBEAT_INFLIGHT,
        CONSUME,
        RECOVERY
    }

    private enum RecoveryPhase {
        MARK_UNKNOWN,
        AWAIT_FIND_COORDINATOR,
        BLOCKED,
        DELIVER
    }

    private static final int PASSES_PER_INVOCATION = 1000;
    /** Mirrors the package-private {@code ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS}. */
    private static final long MAX_POLL_TIMEOUT_MS = 5000;
    private static final long TIME_STEP_MS = 1;
    private static final String TOPIC = "jmh-pass-topic";
    private static final String GROUP_ID = "jmh-pass-group";
    private static final int HEARTBEAT_INTERVAL_MS = 5000;
    private static final int MEMBER_EPOCH = 1;
    private static final int RECORD_VALUE_SIZE = 128;
    /** Upper bound on the passes used to drive a scenario into its steady state during setup. */
    private static final int BOOTSTRAP_PASS_LIMIT = 10_000;

    private static final MemberStateListener NO_OP_MEMBER_STATE_LISTENER = (memberEpoch, memberId) -> { };

    @Param({"IDLE", "BLOCKED", "BLOCKED_HEARTBEAT_INFLIGHT", "CONSUME", "RECOVERY"})
    public Scenario scenario;

    /** Records in each fetch response ({@code CONSUME} only). */
    @Param({"100"})
    public int recordsPerFetch;

    /** Passes the FindCoordinator request stays unanswered in each cycle ({@code RECOVERY} only). */
    @Param({"50"})
    public int recoveryPasses;

    private MockTime time;
    private MockClient client;
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private Metrics metrics;
    private AsyncConsumerMetrics asyncConsumerMetrics;
    private FetchBuffer fetchBuffer;
    private FetchCollector<byte[], byte[]> fetchCollector;
    private NetworkClientDelegate networkClientDelegate;
    private RequestManagers requestManagers;
    private RequestManager[] managers;
    private CoordinatorRequestManager coordinatorRequestManager;
    private ConsumerHeartbeatRequestManager heartbeatRequestManager;
    private ConsumerMembershipManager membershipManager;
    private FetchRequestManager fetchRequestManager;
    private BlockingQueue<ApplicationEvent> applicationEventQueue;
    private BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private CompletableEventReaper applicationEventReaper;
    private final List<BackgroundEvent> drainedBackgroundEvents = new ArrayList<>();

    private Node coordinatorNode;
    private Uuid topicId;
    private TopicPartition topicPartition;
    private TopicIdPartition topicIdPartition;
    private byte[] recordValue;

    private long lastPollTimeMs;
    private long lastPollWaitTimeMs;
    private long cachedMaximumTimeToWait;
    private RecoveryPhase recoveryPhase;
    private int recoveryBlockedPasses;
    /** True while setup drives the scenario to its steady state; RECOVERY behaves like IDLE until then. */
    private boolean bootstrapping;

    /**
     * Per-iteration counters. {@link AuxCounters.Type#EVENTS} values are summed over the measurement
     * iterations by JMH; see the class Javadoc.
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class PassCounters {
        /** Passes executed. */
        public long passes;
        /** {@link RequestManager#poll(long)} calls. */
        public long managerPolls;
        /** Poll results with {@code timeUntilNextPollMs == 0} and no request: the busy-loop signal. */
        public long zeroWaitEmptyResults;
        /** Passes whose network poll timeout was 0 ms. */
        public long zeroNetworkTimeoutPasses;
        /** Passes whose application-facing {@code maximumTimeToWait} was 0 ms. */
        public long zeroMaximumTimeToWaitPasses;
        /** Requests handed to the network client. */
        public long requestsSent;
        /** Responses fed back by the benchmark (all API keys). */
        public long responsesFed;
        /** Fetch responses fed back ({@code CONSUME}). */
        public long fetchResponses;
        /** Records drained from the fetch buffer ({@code CONSUME}). */
        public long recordsCollected;

        private long minNetworkTimeoutMs;
        private long lastMaximumTimeToWaitMs;

        @Setup(Level.Iteration)
        public void reset() {
            minNetworkTimeoutMs = Long.MAX_VALUE;
            lastMaximumTimeToWaitMs = Long.MAX_VALUE;
        }

        /** Smallest network poll timeout observed in the iteration; {@code -1} means "wait forever". */
        public long minNetworkTimeoutMs() {
            return minNetworkTimeoutMs == Long.MAX_VALUE ? -1 : minNetworkTimeoutMs;
        }

        /** Application-facing {@code maximumTimeToWait} after the last pass; {@code -1} means "wait forever". */
        public long lastMaximumTimeToWaitMs() {
            return lastMaximumTimeToWaitMs == Long.MAX_VALUE ? -1 : lastMaximumTimeToWaitMs;
        }
    }

    @Setup(Level.Trial)
    public void setup() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "jmh-fake-host:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        // OffsetCommitCallbackInvoker has a package-private constructor, so the commit manager gets no
        // invoker. Auto-commit stays off so the invoker is never used.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Math.max(1, recordsPerFetch));
        ConsumerConfig config = new ConsumerConfig(props);

        LogContext logContext = new LogContext("[jmh-pass] ");
        time = new MockTime();
        subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        metadata = new ConsumerMetadata(config, subscriptions, logContext, new ClusterResourceListeners());
        client = new MockClient(time, metadata);

        // Subscribe before the metadata update: ConsumerMetadata only retains subscribed topics.
        subscriptions.subscribe(Set.of(TOPIC));
        topicId = Uuid.randomUuid();
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, Map.of(TOPIC, 1), Map.of(TOPIC, topicId)));
        coordinatorNode = metadata.fetch().nodes().get(0);
        topicPartition = new TopicPartition(TOPIC, 0);
        topicIdPartition = new TopicIdPartition(topicId, topicPartition);
        recordValue = new byte[RECORD_VALUE_SIZE];

        metrics = ConsumerUtils.createMetrics(config, time);
        asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, ConsumerUtils.CONSUMER_METRIC_GROUP);
        FetchMetricsManager fetchMetricsManager = ConsumerUtils.createFetchMetricsManager(metrics);
        backgroundEventQueue = new LinkedBlockingQueue<>();
        BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(backgroundEventQueue, time, asyncConsumerMetrics);
        fetchBuffer = new FetchBuffer(logContext);
        ApiVersions apiVersions = new ApiVersions();
        PositionsValidator positionsValidator = new PositionsValidator(logContext, time, subscriptions, metadata);
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config, GroupRebalanceConfig.ProtocolType.CONSUMER);

        Supplier<NetworkClientDelegate> networkClientDelegateSupplier = NetworkClientDelegate.supplier(
            time,
            config,
            logContext,
            client,
            metadata,
            backgroundEventHandler,
            false,
            asyncConsumerMetrics
        );
        Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplier(
            time,
            logContext,
            backgroundEventHandler,
            metadata,
            subscriptions,
            fetchBuffer,
            config,
            groupRebalanceConfig,
            apiVersions,
            fetchMetricsManager,
            networkClientDelegateSupplier,
            Optional.empty(),
            metrics,
            null,
            NO_OP_MEMBER_STATE_LISTENER,
            Optional.empty(),
            positionsValidator,
            new AtomicBoolean()
        );
        networkClientDelegate = networkClientDelegateSupplier.get();
        requestManagers = requestManagersSupplier.get();
        managers = requestManagers.entries().toArray(new RequestManager[0]);
        coordinatorRequestManager = requestManagers.coordinatorRequestManager.orElseThrow();
        heartbeatRequestManager = requestManagers.consumerHeartbeatRequestManager.orElseThrow();
        membershipManager = requestManagers.consumerMembershipManager.orElseThrow();
        fetchRequestManager = requestManagers.fetchRequestManager;

        applicationEventQueue = new LinkedBlockingQueue<>();
        applicationEventReaper = new CompletableEventReaper(logContext);
        fetchCollector = new FetchCollector<>(
            logContext,
            metadata,
            subscriptions,
            new FetchConfig(config),
            new Deserializers<>(new ByteArrayDeserializer(), new ByteArrayDeserializer(), metrics),
            fetchMetricsManager,
            time
        );

        // What the application thread does on subscribe() and the first poll(): join the group.
        membershipManager.onSubscriptionUpdated();
        membershipManager.onConsumerPoll();

        cachedMaximumTimeToWait = MAX_POLL_TIMEOUT_MS;
        recoveryPhase = RecoveryPhase.MARK_UNKNOWN;
        recoveryBlockedPasses = 0;
        bootstrapping = true;
        PassCounters bootstrapCounters = new PassCounters();
        bootstrapCounters.reset();

        switch (scenario) {
            case BLOCKED:
                // FindCoordinator is never answered; nothing to bootstrap.
                break;
            case BLOCKED_HEARTBEAT_INFLIGHT:
                driveUntil(bootstrapCounters, () -> coordinatorRequestManager.coordinator().isPresent()
                    && hasInFlight(ApiKeys.CONSUMER_GROUP_HEARTBEAT));
                break;
            case IDLE:
            case CONSUME:
            case RECOVERY:
                driveUntil(bootstrapCounters, () -> membershipManager.state() == MemberState.STABLE
                    && subscriptions.assignedPartitions().equals(Set.of(topicPartition)));
                subscriptions.seekValidated(topicPartition,
                    new SubscriptionState.FetchPosition(0L, Optional.empty(), metadata.currentLeader(topicPartition)));
                break;
            default:
                throw new IllegalStateException("Unknown scenario " + scenario);
        }
        bootstrapping = false;
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        requestManagers.close();
        networkClientDelegate.close();
        fetchBuffer.close();
        metrics.close();
    }

    @Benchmark
    @OperationsPerInvocation(PASSES_PER_INVOCATION)
    public void pass(PassCounters counters) {
        for (int i = 0; i < PASSES_PER_INVOCATION; i++) {
            beforePass();
            runOnceReplica(counters);
            afterPass(counters);
            time.sleep(TIME_STEP_MS);
        }
    }

    /**
     * Replica of {@code ConsumerNetworkThread.runOnce()}. Keep in sync with the original; the only
     * differences are the counters and that the application event queue is always empty because there is
     * no application thread (the drain is kept so the pass pays for the queue check).
     */
    private void runOnceReplica(PassCounters counters) {
        // processApplicationEvents()
        LinkedList<ApplicationEvent> events = new LinkedList<>();
        applicationEventQueue.drainTo(events);
        // With no application thread the queue is empty, so the per-event loop of the original is skipped.

        final long currentTimeMs = time.milliseconds();
        if (lastPollTimeMs != 0L) {
            asyncConsumerMetrics.recordTimeBetweenNetworkThreadPoll(currentTimeMs - lastPollTimeMs);
        }
        lastPollTimeMs = currentTimeMs;

        long pollWaitTimeMs = MAX_POLL_TIMEOUT_MS;

        for (RequestManager rm : managers) {
            NetworkClientDelegate.PollResult pollResult = rm.poll(currentTimeMs);
            counters.managerPolls++;
            if (pollResult.timeUntilNextPollMs == 0 && pollResult.unsentRequests.isEmpty())
                counters.zeroWaitEmptyResults++;
            counters.requestsSent += pollResult.unsentRequests.size();
            long timeoutMs = networkClientDelegate.addAll(pollResult);
            pollWaitTimeMs = Math.min(pollWaitTimeMs, timeoutMs);
        }

        networkClientDelegate.poll(pollWaitTimeMs, currentTimeMs);

        long maxTimeToWaitMs = Long.MAX_VALUE;

        for (RequestManager rm : managers) {
            long waitMs = rm.maximumTimeToWait(currentTimeMs);
            maxTimeToWaitMs = Math.min(maxTimeToWaitMs, waitMs);
        }

        cachedMaximumTimeToWait = maxTimeToWaitMs;

        // reapExpiredApplicationEvents(currentTimeMs) + maybeFailOnMetadataError(uncompletedEvents)
        asyncConsumerMetrics.recordApplicationEventExpiredSize(applicationEventReaper.reap(currentTimeMs));
        List<CompletableEvent<?>> uncompletedEvents = applicationEventReaper.uncompletedEvents();
        maybeFailOnMetadataError(uncompletedEvents);

        lastPollWaitTimeMs = pollWaitTimeMs;
        counters.passes++;
        if (pollWaitTimeMs == 0)
            counters.zeroNetworkTimeoutPasses++;
        if (maxTimeToWaitMs == 0)
            counters.zeroMaximumTimeToWaitPasses++;
        counters.minNetworkTimeoutMs = Math.min(counters.minNetworkTimeoutMs, pollWaitTimeMs);
        counters.lastMaximumTimeToWaitMs = maxTimeToWaitMs;
    }

    /** Replica of the private {@code ConsumerNetworkThread.maybeFailOnMetadataError(List)}. */
    private void maybeFailOnMetadataError(List<?> events) {
        List<MetadataErrorNotifiableEvent> filteredEvents = new ArrayList<>();
        for (Object obj : events) {
            if (obj instanceof MetadataErrorNotifiableEvent)
                filteredEvents.add((MetadataErrorNotifiableEvent) obj);
        }
        if (filteredEvents.isEmpty())
            return;
        Optional<Exception> metadataError = networkClientDelegate.getAndClearMetadataError();
        if (metadataError.isPresent()) {
            filteredEvents.forEach(e -> e.onMetadataError(metadataError.get()));
            networkClientDelegate.wakeupApplication();
        }
    }

    /** The application-thread side of a {@code poll()} call, executed before each pass. */
    private void beforePass() {
        long now = time.milliseconds();
        heartbeatRequestManager.resetPollTimer(now);
        membershipManager.onConsumerPoll();
        switch (scenario) {
            case CONSUME:
                fetchRequestManager.createFetchRequests();
                break;
            case RECOVERY:
                // Wait for the delivered FindCoordinator response before starting the next cycle.
                if (!bootstrapping && recoveryPhase == RecoveryPhase.MARK_UNKNOWN
                    && coordinatorRequestManager.coordinator().isPresent()) {
                    coordinatorRequestManager.markCoordinatorUnknown("jmh recovery cycle", now);
                    recoveryPhase = RecoveryPhase.AWAIT_FIND_COORDINATOR;
                }
                break;
            default:
                break;
        }
    }

    /**
     * The "broker" and the application-thread side after each pass: answer in-flight requests according to
     * the scenario (responses are delivered by the network poll of the next pass), apply assignments and
     * drain the fetch buffer.
     */
    private void afterPass(PassCounters counters) {
        applyBackgroundEvents();
        if (client.hasInFlightRequests())
            feedResponses(counters);
        if (scenario == Scenario.CONSUME)
            counters.recordsCollected += drainFetchBuffer();
    }

    private void feedResponses(PassCounters counters) {
        if (scenario == Scenario.RECOVERY && !bootstrapping) {
            if (recoveryPhase == RecoveryPhase.AWAIT_FIND_COORDINATOR && hasInFlight(ApiKeys.FIND_COORDINATOR)) {
                recoveryPhase = RecoveryPhase.BLOCKED;
                recoveryBlockedPasses = 0;
            }
            if (recoveryPhase == RecoveryPhase.BLOCKED) {
                recoveryBlockedPasses++;
                if (recoveryBlockedPasses >= recoveryPasses)
                    recoveryPhase = RecoveryPhase.DELIVER;
            }
        }

        List<ClientRequest> inFlight = new ArrayList<>(client.requests());
        for (ClientRequest request : inFlight) {
            AbstractResponse response = responseFor(request.apiKey());
            if (response == null)
                continue;
            client.respondToRequest(request, response);
            counters.responsesFed++;
            if (request.apiKey() == ApiKeys.FETCH)
                counters.fetchResponses++;
        }

        if (scenario == Scenario.RECOVERY && recoveryPhase == RecoveryPhase.DELIVER)
            recoveryPhase = RecoveryPhase.MARK_UNKNOWN;
    }

    /** Returns the response the scenario allows for the given API key, or {@code null} to leave it in flight. */
    private AbstractResponse responseFor(ApiKeys apiKey) {
        switch (apiKey) {
            case FIND_COORDINATOR:
                switch (scenario) {
                    case BLOCKED:
                        return null;
                    case RECOVERY:
                        return bootstrapping || recoveryPhase == RecoveryPhase.DELIVER ? findCoordinatorResponse() : null;
                    default:
                        return findCoordinatorResponse();
                }
            case CONSUMER_GROUP_HEARTBEAT:
                return scenario == Scenario.BLOCKED_HEARTBEAT_INFLIGHT ? null : heartbeatResponse();
            case FETCH:
                return scenario == Scenario.CONSUME ? fetchResponse() : null;
            default:
                return null;
        }
    }

    private FindCoordinatorResponse findCoordinatorResponse() {
        return FindCoordinatorResponse.prepareResponse(Errors.NONE, GROUP_ID, coordinatorNode);
    }

    private ConsumerGroupHeartbeatResponse heartbeatResponse() {
        ConsumerGroupHeartbeatResponseData.Assignment assignment = new ConsumerGroupHeartbeatResponseData.Assignment()
            .setTopicPartitions(List.of(new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                .setTopicId(topicId)
                .setPartitions(List.of(0))));
        return new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
            .setErrorCode(Errors.NONE.code())
            .setMemberId(membershipManager.memberId())
            .setMemberEpoch(MEMBER_EPOCH)
            .setHeartbeatIntervalMs(HEARTBEAT_INTERVAL_MS)
            .setAssignment(assignment));
    }

    /** A full fetch response with {@code recordsPerFetch} records starting at the current fetch position. */
    private FetchResponse fetchResponse() {
        long baseOffset = subscriptions.position(topicPartition).offset;
        SimpleRecord[] records = new SimpleRecord[recordsPerFetch];
        for (int i = 0; i < recordsPerFetch; i++) {
            ByteBuffer.wrap(recordValue).putLong(0, baseOffset + i);
            records[i] = new SimpleRecord(recordValue.clone());
        }
        MemoryRecords memoryRecords = MemoryRecords.withRecords(baseOffset, Compression.NONE, records);
        long highWatermark = baseOffset + recordsPerFetch;
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> partitions = new LinkedHashMap<>();
        partitions.put(topicIdPartition, new FetchResponseData.PartitionData()
            .setPartitionIndex(topicPartition.partition())
            .setErrorCode(Errors.NONE.code())
            .setHighWatermark(highWatermark)
            .setLastStableOffset(highWatermark)
            .setLogStartOffset(0)
            .setRecords(memoryRecords));
        return FetchResponse.of(Errors.NONE, 0, FetchMetadata.INVALID_SESSION_ID, partitions, List.of());
    }

    /** Drains the fetch buffer the way the application thread does, so fetch demand continues. */
    private long drainFetchBuffer() {
        long records = 0;
        while (true) {
            Fetch<byte[], byte[]> fetch = fetchCollector.collectFetch(fetchBuffer);
            if (fetch.isEmpty())
                return records;
            records += fetch.numRecords();
        }
    }

    /** The application-thread side of the background events: apply the assignment and complete the events. */
    private void applyBackgroundEvents() {
        if (backgroundEventQueue.isEmpty())
            return;
        drainedBackgroundEvents.clear();
        backgroundEventQueue.drainTo(drainedBackgroundEvents);
        for (BackgroundEvent event : drainedBackgroundEvents) {
            if (event instanceof PartitionsAssignedEvent) {
                PartitionsAssignedEvent assigned = (PartitionsAssignedEvent) event;
                membershipManager.applyAssignment(assigned.assignedPartitions(), assigned.addedPartitions());
                assigned.future().complete(null);
            } else if (event instanceof PartitionsRemovedEvent) {
                ((PartitionsRemovedEvent) event).future().complete(null);
            }
            // Other events (errors, callbacks) are not expected in these scenarios and are ignored.
        }
    }

    private boolean hasInFlight(ApiKeys apiKey) {
        for (ClientRequest request : client.requests()) {
            if (request.apiKey() == apiKey)
                return true;
        }
        return false;
    }

    private void driveUntil(PassCounters counters, BooleanSupplier condition) {
        for (int i = 0; i < BOOTSTRAP_PASS_LIMIT; i++) {
            beforePass();
            runOnceReplica(counters);
            afterPass(counters);
            time.sleep(TIME_STEP_MS);
            if (condition.getAsBoolean())
                return;
        }
        throw new IllegalStateException("Scenario " + scenario + " did not reach its steady state within "
            + BOOTSTRAP_PASS_LIMIT + " passes; member state " + membershipManager.state()
            + ", coordinator " + coordinatorRequestManager.coordinator()
            + ", in-flight " + client.requests()
            + ", last network timeout " + lastPollWaitTimeMs
            + ", cached maximumTimeToWait " + cachedMaximumTimeToWait);
    }
}
