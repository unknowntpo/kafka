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
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Enforces the KIP-1371 <em>wait semantics</em> for <b>every</b> {@link RequestManager} at once, instead of
 * relying on one {@code ...DoesNotSpin} test per manager.
 *
 * <p>The rule under test:
 * <ul>
 *     <li>{@link NetworkClientDelegate.PollResult} may only ask for an immediate re-poll
 *     ({@code timeUntilNextPollMs == 0}) when it staged at least one request. Zero with an empty request list
 *     means the background loop would spin, because nothing that happens in this pass can change the
 *     manager's answer.</li>
 *     <li>{@link RequestManager#maximumTimeToWait(long)} may only return 0 when there is an action the
 *     <em>application thread itself</em> has to start and that is possible right now (refreshing the poll
 *     timer, an auto-commit while the coordinator is known). Waiting for an in-flight request, for the
 *     coordinator or for DNS is not such an action.</li>
 * </ul>
 *
 * <p>The same method was patched four times (KAFKA-20253 &rarr; KAFKA-20970 &rarr; KAFKA-21010 &rarr;
 * KAFKA-21031) because the rule was written down nowhere and enforced nowhere. This test is the
 * enforcement point: it drives the real background loop with the real request managers over a
 * {@link MockClient} through a set of blocked states, and checks the invariant for every manager on every
 * pass. A new manager is covered automatically as soon as it appears in {@link RequestManagers#entries()}.
 *
 * <p><b>The real loop is used.</b> This test lives in the same package as {@link ConsumerNetworkThread}, so
 * the package-private {@link ConsumerNetworkThread#runOnce()} and
 * {@link ConsumerNetworkThread#initializeResources()} are reachable and are called directly - nothing is
 * replicated here, unlike {@code ConsumerNetworkThreadPassBenchmark.runOnceReplica} in the {@code
 * jmh-benchmarks} module, which cannot see them. The per-manager {@link NetworkClientDelegate.PollResult}
 * is observed by overriding {@link NetworkClientDelegate#addAll(NetworkClientDelegate.PollResult)}: {@code
 * runOnce()} calls it exactly once per manager, in {@link RequestManagers#entries()} order, so the recorded
 * results line up with the managers by index. {@link RequestManager#maximumTimeToWait(long)} is a query
 * (the heartbeat manager only calls {@code Timer.update(currentTimeMs)}, which is idempotent for a fixed
 * time), so it is re-evaluated after the pass with the same {@code currentTimeMs} the pass used.
 *
 * <p>The fixture (managers, scenarios, "broker" responses and the application-thread side of a pass) is
 * ported from {@code org.apache.kafka.jmh.consumer.ConsumerNetworkThreadPassBenchmark}; the {@code
 * jmh-benchmarks} module is not on the {@code clients} test classpath.
 *
 * <p><b>This test has teeth.</b> On trunk (74fbd50061) {@code BLOCKED_HEARTBEAT_INFLIGHT} fails:
 * with the heartbeat interval expired and the first heartbeat in flight,
 * {@code AbstractHeartbeatRequestManager.poll()} falls through to
 * {@code new PollResult(heartbeatRequestState.timeToNextHeartbeatMs(currentTimeMs))} with no request, and
 * trunk's {@code HeartbeatRequestState.timeToNextHeartbeatMs()} returns {@code remainingBackoffMs()}, which
 * is 0 before the first response has ever been received. That is a zero wait with an empty request list on
 * every pass until the request times out (KAFKA-21031). This branch returns
 * {@code max(1, retryBackoffMs)} in that branch, so the scenario passes here.
 *
 * <p><b>Known gap, deliberately not asserted.</b> With {@code retry.backoff.ms=0} and the coordinator
 * unknown, {@code AbstractHeartbeatRequestManager.maximumTimeToWait()} returns
 * {@code heartbeatRequestState.retryBackoffMs()} and {@code CommitRequestManager.maximumTimeToWait()}
 * returns {@code retryBackoffMs}, both unclamped, so both return 0 while nothing the application thread can
 * do would help. {@link Scenario#CONSUME_ZERO_RETRY_BACKOFF} therefore uses the {@code CONSUME} shape (the
 * coordinator is known), which covers the clamped {@code FetchRequestManager.maximumTimeToWait()}
 * (KAFKA-21049) without tripping over that gap. Clamping those two branches is a production change and is
 * out of scope for this test.
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public class WaitSemanticsInvariantTest {

    /** The shape of the fixture: which requests are answered and what the application thread does. */
    private enum Shape {
        IDLE,
        BLOCKED,
        BLOCKED_HEARTBEAT_INFLIGHT,
        CONSUME,
        RECOVERY,
        ASSIGN_UNSUBSCRIBED
    }

    public enum Scenario {
        /** Member STABLE with one assigned partition and a known coordinator; no fetch demand. */
        IDLE(Shape.IDLE, DEFAULT_RETRY_BACKOFF_MS),
        /** FindCoordinator is never answered: the coordinator stays unknown for the whole run. */
        BLOCKED(Shape.BLOCKED, DEFAULT_RETRY_BACKOFF_MS),
        /** The coordinator is known but the first heartbeat is never answered (KAFKA-21031). */
        BLOCKED_HEARTBEAT_INFLIGHT(Shape.BLOCKED_HEARTBEAT_INFLIGHT, DEFAULT_RETRY_BACKOFF_MS),
        /** A fetch is requested on every pass and answered after it. */
        CONSUME(Shape.CONSUME, DEFAULT_RETRY_BACKOFF_MS),
        /** The coordinator is repeatedly marked unknown and rediscovered. */
        RECOVERY(Shape.RECOVERY, DEFAULT_RETRY_BACKOFF_MS),
        /** {@code group.id} is set but {@code assign()} was used, so the member stays UNSUBSCRIBED (KAFKA-20426). */
        ASSIGN_UNSUBSCRIBED(Shape.ASSIGN_UNSUBSCRIBED, DEFAULT_RETRY_BACKOFF_MS),
        /** The CONSUME shape with {@code retry.backoff.ms=0}, the KAFKA-21049 configuration. */
        CONSUME_ZERO_RETRY_BACKOFF(Shape.CONSUME, 0);

        private final Shape shape;
        private final int retryBackoffMs;

        Scenario(Shape shape, int retryBackoffMs) {
            this.shape = shape;
            this.retryBackoffMs = retryBackoffMs;
        }
    }

    private enum RecoveryPhase {
        MARK_UNKNOWN,
        AWAIT_FIND_COORDINATOR,
        BLOCKED,
        DELIVER
    }

    private static final int DEFAULT_RETRY_BACKOFF_MS = 100;
    /** Passes per scenario. With {@link #TIME_STEP_MS} this is far below {@code request.timeout.ms}, so no
     *  in-flight request ever times out and every scenario stays in one deterministic state. */
    private static final int PASSES = 300;
    private static final long TIME_STEP_MS = 1;
    private static final String TOPIC = "wait-semantics-topic";
    private static final String GROUP_ID = "wait-semantics-group";
    private static final int HEARTBEAT_INTERVAL_MS = 5000;
    private static final int MEMBER_EPOCH = 1;
    private static final int RECORDS_PER_FETCH = 8;
    private static final int RECORD_VALUE_SIZE = 32;
    /** Passes the FindCoordinator request stays unanswered in each RECOVERY cycle. */
    private static final int RECOVERY_BLOCKED_PASSES = 50;
    private static final int BOOTSTRAP_PASS_LIMIT = 10_000;

    private static final MemberStateListener NO_OP_MEMBER_STATE_LISTENER = (memberEpoch, memberId) -> { };

    private Scenario scenario;
    private MockTime time;
    private MockClient client;
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private Metrics metrics;
    private FetchBuffer fetchBuffer;
    private FetchCollector<byte[], byte[]> fetchCollector;
    private RecordingNetworkClientDelegate networkClientDelegate;
    private RequestManagers requestManagers;
    private List<RequestManager> managers;
    private ConsumerNetworkThread networkThread;
    private CoordinatorRequestManager coordinatorRequestManager;
    private ConsumerHeartbeatRequestManager heartbeatRequestManager;
    private ConsumerMembershipManager membershipManager;
    private FetchRequestManager fetchRequestManager;
    private BlockingQueue<BackgroundEvent> backgroundEventQueue;

    private Node coordinatorNode;
    private Uuid topicId;
    private TopicPartition topicPartition;
    private TopicIdPartition topicIdPartition;
    private byte[] recordValue;

    private RecoveryPhase recoveryPhase;
    private int recoveryBlockedPasses;
    private boolean bootstrapping;

    /**
     * Records the {@link NetworkClientDelegate.PollResult} of each manager. {@code runOnce()} calls
     * {@link #addAll(NetworkClientDelegate.PollResult)} once per manager, in
     * {@link RequestManagers#entries()} order.
     */
    private static class RecordingNetworkClientDelegate extends NetworkClientDelegate {

        private final List<NetworkClientDelegate.PollResult> pollResults = new ArrayList<>();

        RecordingNetworkClientDelegate(MockTime time,
                                       ConsumerConfig config,
                                       LogContext logContext,
                                       MockClient client,
                                       ConsumerMetadata metadata,
                                       BackgroundEventHandler backgroundEventHandler,
                                       AsyncConsumerMetrics asyncConsumerMetrics) {
            super(time, config, logContext, client, metadata, backgroundEventHandler, false, asyncConsumerMetrics);
        }

        @Override
        public long addAll(NetworkClientDelegate.PollResult pollResult) {
            pollResults.add(pollResult);
            return super.addAll(pollResult);
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (requestManagers != null)
            requestManagers.close();
        if (networkClientDelegate != null)
            networkClientDelegate.close();
        if (fetchBuffer != null)
            fetchBuffer.close();
        if (metrics != null)
            metrics.close();
    }

    @ParameterizedTest
    @EnumSource(Scenario.class)
    public void testNoManagerAsksForAnImmediateRePollWithoutStagingARequest(Scenario testScenario) {
        setUpScenario(testScenario);

        for (int pass = 1; pass <= PASSES; pass++) {
            long currentTimeMs = time.milliseconds();
            beforePass();
            networkClientDelegate.pollResults.clear();
            networkThread.runOnce();
            List<NetworkClientDelegate.PollResult> pollResults = List.copyOf(networkClientDelegate.pollResults);

            assertEquals(managers.size(), pollResults.size(),
                "runOnce() polled " + pollResults.size() + " managers but RequestManagers.entries() has "
                    + managers.size() + "; the poll results can no longer be matched to their managers");

            for (int i = 0; i < managers.size(); i++) {
                NetworkClientDelegate.PollResult pollResult = pollResults.get(i);
                if (pollResult.timeUntilNextPollMs != 0)
                    continue;
                assertFalse(pollResult.unsentRequests.isEmpty(),
                    describe(i, pass) + " returned timeUntilNextPollMs == 0 with no staged request, so the "
                        + "background loop would poll it again immediately and spin. Return "
                        + "PollResult.retryAfter(delayMs) or PollResult.awaitInput() instead.");
            }

            if (blockedWithNothingForTheApplicationThreadToDo()) {
                for (int i = 0; i < managers.size(); i++) {
                    long maxTimeToWaitMs = managers.get(i).maximumTimeToWait(currentTimeMs);
                    assertNotEquals(0L, maxTimeToWaitMs,
                        describe(i, pass) + " returned maximumTimeToWait == 0, but no action the application "
                            + "thread has to start is possible in this state; the application thread would "
                            + "spin in poll(). Bound it by a positive delay or Long.MAX_VALUE instead.");
                }
            }

            afterPass();
            time.sleep(TIME_STEP_MS);
        }
    }

    /**
     * True for the scenarios in which the consumer is blocked on a background round trip: the coordinator is
     * unknown, or the first heartbeat is in flight. Nothing the application thread does can unblock either,
     * so no manager may pull the application thread's wait down to zero.
     */
    private boolean blockedWithNothingForTheApplicationThreadToDo() {
        return scenario.shape == Shape.BLOCKED || scenario.shape == Shape.BLOCKED_HEARTBEAT_INFLIGHT;
    }

    private String describe(int managerIndex, int pass) {
        return "[" + scenario + " pass " + pass + "] " + managers.get(managerIndex).getClass().getSimpleName();
    }

    private void setUpScenario(Scenario testScenario) {
        this.scenario = testScenario;

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "wait-semantics-fake-host:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        // OffsetCommitCallbackInvoker is not wired up, so auto-commit stays off and the invoker is never used.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, RECORDS_PER_FETCH);
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, (long) scenario.retryBackoffMs);
        ConsumerConfig config = new ConsumerConfig(props);

        LogContext logContext = new LogContext("[wait-semantics] ");
        time = new MockTime();
        subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        metadata = new ConsumerMetadata(config, subscriptions, logContext, new ClusterResourceListeners());
        client = new MockClient(time, metadata);

        topicPartition = new TopicPartition(TOPIC, 0);
        // Register the topic before the metadata update: ConsumerMetadata only retains topics the consumer wants.
        if (scenario.shape == Shape.ASSIGN_UNSUBSCRIBED)
            subscriptions.assignFromUser(Set.of(topicPartition));
        else
            subscriptions.subscribe(Set.of(TOPIC));
        topicId = Uuid.randomUuid();
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, Map.of(TOPIC, 1), Map.of(TOPIC, topicId)));
        coordinatorNode = metadata.fetch().nodes().get(0);
        topicIdPartition = new TopicIdPartition(topicId, topicPartition);
        recordValue = new byte[RECORD_VALUE_SIZE];

        metrics = ConsumerUtils.createMetrics(config, time);
        AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, ConsumerUtils.CONSUMER_METRIC_GROUP);
        FetchMetricsManager fetchMetricsManager = ConsumerUtils.createFetchMetricsManager(metrics);
        backgroundEventQueue = new LinkedBlockingQueue<>();
        BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(backgroundEventQueue, time, asyncConsumerMetrics);
        fetchBuffer = new FetchBuffer(logContext);
        PositionsValidator positionsValidator = new PositionsValidator(logContext, time, subscriptions, metadata);
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config, GroupRebalanceConfig.ProtocolType.CONSUMER);

        networkClientDelegate = new RecordingNetworkClientDelegate(
            time, config, logContext, client, metadata, backgroundEventHandler, asyncConsumerMetrics);
        Supplier<NetworkClientDelegate> networkClientDelegateSupplier = () -> networkClientDelegate;
        requestManagers = RequestManagers.supplier(
            time,
            logContext,
            backgroundEventHandler,
            metadata,
            subscriptions,
            fetchBuffer,
            config,
            groupRebalanceConfig,
            new ApiVersions(),
            fetchMetricsManager,
            networkClientDelegateSupplier,
            Optional.empty(),
            metrics,
            null,
            NO_OP_MEMBER_STATE_LISTENER,
            Optional.empty(),
            positionsValidator,
            new AtomicBoolean()
        ).get();
        managers = requestManagers.entries();
        coordinatorRequestManager = requestManagers.coordinatorRequestManager.orElseThrow();
        heartbeatRequestManager = requestManagers.consumerHeartbeatRequestManager.orElseThrow();
        membershipManager = requestManagers.consumerMembershipManager.orElseThrow();
        fetchRequestManager = requestManagers.fetchRequestManager;
        fetchCollector = new FetchCollector<>(
            logContext,
            metadata,
            subscriptions,
            new FetchConfig(config),
            new Deserializers<>(new ByteArrayDeserializer(), new ByteArrayDeserializer(), metrics),
            fetchMetricsManager,
            time
        );

        BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
        // The queue is always empty (there is no application thread), so runOnce() returns from
        // processApplicationEvents() before it ever touches the processor; a null supplier value is enough.
        Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = () -> null;
        networkThread = new ConsumerNetworkThread(
            logContext,
            time,
            applicationEventQueue,
            new CompletableEventReaper(logContext),
            applicationEventProcessorSupplier,
            networkClientDelegateSupplier,
            () -> requestManagers,
            asyncConsumerMetrics
        );
        // Initialize the thread's resources without ever start()ing it: the test owns the loop.
        networkThread.initializeResources();

        recoveryPhase = RecoveryPhase.MARK_UNKNOWN;
        recoveryBlockedPasses = 0;
        bootstrapping = true;

        if (scenario.shape != Shape.ASSIGN_UNSUBSCRIBED) {
            // What the application thread does on subscribe() and the first poll(): join the group.
            membershipManager.onSubscriptionUpdated();
            membershipManager.onConsumerPoll();
        }

        // Drive the scenario into its steady state. The invariant is not asserted here: the assertions
        // belong to the steady state, and bootstrap passes through short-lived transitions.
        switch (scenario.shape) {
            case BLOCKED:
                // FindCoordinator is never answered; there is nothing to bootstrap.
                break;
            case ASSIGN_UNSUBSCRIBED:
                // The member never joins; only the coordinator has to be discovered.
                driveUntil(() -> coordinatorRequestManager.coordinator().isPresent());
                break;
            case BLOCKED_HEARTBEAT_INFLIGHT:
                driveUntil(() -> coordinatorRequestManager.coordinator().isPresent()
                    && hasInFlight(ApiKeys.CONSUMER_GROUP_HEARTBEAT));
                break;
            default:
                driveUntil(() -> membershipManager.state() == MemberState.STABLE
                    && subscriptions.assignedPartitions().equals(Set.of(topicPartition)));
                subscriptions.seekValidated(topicPartition,
                    new SubscriptionState.FetchPosition(0L, Optional.empty(), metadata.currentLeader(topicPartition)));
                break;
        }
        bootstrapping = false;
    }

    private void driveUntil(BooleanSupplier condition) {
        for (int i = 0; i < BOOTSTRAP_PASS_LIMIT; i++) {
            beforePass();
            networkThread.runOnce();
            afterPass();
            time.sleep(TIME_STEP_MS);
            if (condition.getAsBoolean())
                return;
        }
        throw new IllegalStateException("Scenario " + scenario + " did not reach its steady state within "
            + BOOTSTRAP_PASS_LIMIT + " passes; member state " + membershipManager.state()
            + ", coordinator " + coordinatorRequestManager.coordinator()
            + ", in-flight " + client.requests());
    }

    /** The application-thread side of a {@code poll()} call, executed before each pass. */
    private void beforePass() {
        long now = time.milliseconds();
        heartbeatRequestManager.resetPollTimer(now);
        // A no-op while the member is UNSUBSCRIBED and onSubscriptionUpdated() was never called.
        membershipManager.onConsumerPoll();
        switch (scenario.shape) {
            case CONSUME:
                fetchRequestManager.createFetchRequests();
                break;
            case RECOVERY:
                // Wait for the delivered FindCoordinator response before starting the next cycle.
                if (!bootstrapping && recoveryPhase == RecoveryPhase.MARK_UNKNOWN
                    && coordinatorRequestManager.coordinator().isPresent()) {
                    coordinatorRequestManager.markCoordinatorUnknown("wait-semantics recovery cycle", now);
                    recoveryPhase = RecoveryPhase.AWAIT_FIND_COORDINATOR;
                }
                break;
            default:
                break;
        }
    }

    /** The "broker" and the application thread after each pass: answer in-flight requests and apply assignments. */
    private void afterPass() {
        applyBackgroundEvents();
        if (client.hasInFlightRequests())
            feedResponses();
        if (scenario.shape == Shape.CONSUME)
            drainFetchBuffer();
    }

    /** Drains the fetch buffer the way the application thread does, so fetch demand continues. */
    private void drainFetchBuffer() {
        while (!fetchCollector.collectFetch(fetchBuffer).isEmpty()) {
            // Keep draining until the buffer is empty.
        }
    }

    private void feedResponses() {
        if (scenario.shape == Shape.RECOVERY && !bootstrapping) {
            if (recoveryPhase == RecoveryPhase.AWAIT_FIND_COORDINATOR && hasInFlight(ApiKeys.FIND_COORDINATOR)) {
                recoveryPhase = RecoveryPhase.BLOCKED;
                recoveryBlockedPasses = 0;
            }
            if (recoveryPhase == RecoveryPhase.BLOCKED) {
                recoveryBlockedPasses++;
                if (recoveryBlockedPasses >= RECOVERY_BLOCKED_PASSES)
                    recoveryPhase = RecoveryPhase.DELIVER;
            }
        }

        for (ClientRequest request : new ArrayList<>(client.requests())) {
            AbstractResponse response = responseFor(request.apiKey());
            if (response != null)
                client.respondToRequest(request, response);
        }

        if (scenario.shape == Shape.RECOVERY && recoveryPhase == RecoveryPhase.DELIVER)
            recoveryPhase = RecoveryPhase.MARK_UNKNOWN;
    }

    /** The response the scenario allows for the given API key, or {@code null} to leave the request in flight. */
    private AbstractResponse responseFor(ApiKeys apiKey) {
        switch (apiKey) {
            case FIND_COORDINATOR:
                switch (scenario.shape) {
                    case BLOCKED:
                        return null;
                    case RECOVERY:
                        return bootstrapping || recoveryPhase == RecoveryPhase.DELIVER ? findCoordinatorResponse() : null;
                    default:
                        return findCoordinatorResponse();
                }
            case CONSUMER_GROUP_HEARTBEAT:
                return scenario.shape == Shape.BLOCKED_HEARTBEAT_INFLIGHT ? null : heartbeatResponse();
            case FETCH:
                return scenario.shape == Shape.CONSUME ? fetchResponse() : null;
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

    private FetchResponse fetchResponse() {
        long baseOffset = subscriptions.position(topicPartition).offset;
        SimpleRecord[] records = new SimpleRecord[RECORDS_PER_FETCH];
        for (int i = 0; i < RECORDS_PER_FETCH; i++) {
            ByteBuffer.wrap(recordValue).putLong(0, baseOffset + i);
            records[i] = new SimpleRecord(recordValue.clone());
        }
        MemoryRecords memoryRecords = MemoryRecords.withRecords(baseOffset, Compression.NONE, records);
        long highWatermark = baseOffset + RECORDS_PER_FETCH;
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

    /** The application-thread side of the background events: apply the assignment and complete the events. */
    private void applyBackgroundEvents() {
        if (backgroundEventQueue.isEmpty())
            return;
        List<BackgroundEvent> events = new ArrayList<>();
        backgroundEventQueue.drainTo(events);
        for (BackgroundEvent event : events) {
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
}
