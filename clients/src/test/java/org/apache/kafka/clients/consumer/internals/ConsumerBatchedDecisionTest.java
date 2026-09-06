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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AsyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.SyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Component tests: real managers and transport delegate, no broker or real clock. */
// This fixture deliberately includes the production RequestManagers wiring, not just isolated managers.
@SuppressWarnings("ClassDataAbstractionCoupling")
class ConsumerBatchedDecisionTest {
    private static final String GROUP_ID = "batch-group";
    private static final TopicPartition PARTITION = new TopicPartition("topic", 0);
    private static final Node NODE = new Node(1, "localhost", 9092);
    private final LogContext logContext = new LogContext();
    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(time);
    private final ConsumerMembershipManager membership = mock(ConsumerMembershipManager.class);
    private CoordinatorRequestManager coordinator;
    private CommitRequestManager commits;
    private ConsumerHeartbeatRequestManager heartbeat;
    private MockClient client;
    private NetworkClientDelegate delegate;
    private ConsumerNetworkThread thread;
    private ConsumerConfig config;
    private ConsumerMetadata metadata;
    private BackgroundEventHandler background;
    private SubscriptionState subscriptions;
    private final LinkedBlockingQueue<ApplicationEvent> applicationEvents = new LinkedBlockingQueue<>();

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config = new ConsumerConfig(props);
        metadata = mock(ConsumerMetadata.class);
        AsyncConsumerMetrics asyncMetrics = mock(AsyncConsumerMetrics.class);
        background = mock(BackgroundEventHandler.class);
        coordinator = new CoordinatorRequestManager(logContext, 100, 1000, GROUP_ID);
        discoverCoordinator();
        // The real discovery retry delay is already elapsed, not bypassed by the batch mechanism.
        time.sleep(1000);
        subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        commits = new CommitRequestManager(time, logContext,
            subscriptions, config, coordinator,
            mock(OffsetCommitCallbackInvoker.class), GROUP_ID, Optional.empty(), 100, 1000,
            OptionalDouble.of(0), metrics, metadata);
        when(membership.groupInstanceId()).thenReturn(Optional.empty());
        when(membership.state()).thenReturn(MemberState.STABLE);
        ConsumerHeartbeatRequestManager.HeartbeatState heartbeatState = mock(ConsumerHeartbeatRequestManager.HeartbeatState.class);
        when(heartbeatState.buildRequestData()).thenAnswer(ignored -> new ConsumerGroupHeartbeatRequestData()
            .setGroupId(GROUP_ID).setMemberId("member").setMemberEpoch(1));
        heartbeat = new ConsumerHeartbeatRequestManager(logContext,
            time.timer(300_000), config, coordinator, membership, heartbeatState,
            new HeartbeatRequestState(logContext, time, 0, 100, 1000, 0), background, metrics);
        client = new MockClient(time, List.of(coordinator.coordinator().orElseThrow()));
        delegate = new NetworkClientDelegate(time, config, logContext, client, metadata, background, false, asyncMetrics);
        when(membership.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(membership.pollOnClose(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(membership.maximumTimeToWait(anyLong())).thenReturn(Long.MAX_VALUE);
        // Use production configuration, not a test-authored list that could hide an order regression.
        RequestManagers managers = new RequestManagers(logContext,
            idleManager(OffsetsRequestManager.class), idleManager(TopicMetadataRequestManager.class),
            idleManager(FetchRequestManager.class), Optional.of(coordinator), Optional.of(commits),
            Optional.of(heartbeat), Optional.of(membership), Optional.empty(), Optional.empty(), Optional.empty());
        thread = new ConsumerNetworkThread(logContext, time, applicationEvents,
            new CompletableEventReaper(logContext), () -> new ApplicationEventProcessor(logContext, managers, metadata, subscriptions),
            () -> delegate, () -> managers, asyncMetrics);
        thread.initializeResources();
    }

    private <T extends RequestManager> T idleManager(Class<T> type) {
        T manager = mock(type);
        when(manager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(manager.pollOnClose(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(manager.maximumTimeToWait(anyLong())).thenReturn(Long.MAX_VALUE);
        return manager;
    }

    @AfterEach
    void tearDown() {
        if (thread != null)
            thread.close(Duration.ZERO);
        metrics.close();
    }

    private void discoverCoordinator() {
        NetworkClientDelegate.UnsentRequest request = coordinator.poll(time.milliseconds()).unsentRequests.get(0);
        request.handler().onComplete(new ClientResponse(
            new RequestHeader(ApiKeys.FIND_COORDINATOR, request.requestBuilder().build().version(), "test", 1),
            request.handler(), NODE.idString(), time.milliseconds(), time.milliseconds(), false, null, null,
            FindCoordinatorResponse.prepareResponse(Errors.NONE, GROUP_ID, NODE)));
    }

    private ClientRequest request(ApiKeys apiKey) {
        return client.requests().stream().filter(r -> r.requestBuilder().apiKey() == apiKey).findFirst().orElseThrow();
    }

    @Test
    void testFailureContinuationQueuesAnInputAfterTheCurrentErrorConsumption() {
        var operationA = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(1)));
        AsyncCommitEvent operationB = new AsyncCommitEvent(Optional.of(Map.of(PARTITION, new OffsetAndMetadata(2))));
        var continuation = operationA.whenComplete((result, error) -> applicationEvents.add(operationB));
        coordinator.markCoordinatorUnknown("force discovery", time.milliseconds());
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.GROUP_AUTHORIZATION_FAILED, GROUP_ID, NODE));
        thread.runOnce();
        assertTrue(operationA.isCompletedExceptionally());
        assertTrue(continuation.isCompletedExceptionally());
        assertEquals(1, applicationEvents.size());
        assertTrue(coordinator.fatalError().isEmpty());
        thread.runOnce();
        assertTrue(operationB.offsetsReady().isDone());
        assertFalse(operationB.future().isDone());
        verify(background).add(any(ErrorEvent.class));
    }

    @Test
    void testPostIoFatalDeliveryPrecedesEndOfRoundApplicationTimeout() {
        SyncCommitEvent operation = new SyncCommitEvent(
            Optional.of(Map.of(PARTITION, new OffsetAndMetadata(1))), time.milliseconds());
        applicationEvents.add(operation);
        coordinator.markCoordinatorUnknown("force discovery", time.milliseconds());
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.GROUP_AUTHORIZATION_FAILED, GROUP_ID, NODE));
        thread.runOnce();
        assertTrue(operation.future().isCompletedExceptionally());
        Throwable result = assertThrows(CompletionException.class, operation.future()::join).getCause();
        assertInstanceOf(GroupAuthorizationException.class, result);
        ArgumentCaptor<ErrorEvent> delivered = ArgumentCaptor.forClass(ErrorEvent.class);
        verify(background).add(delivered.capture());
        assertSame(result, delivered.getValue().error());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testQueuedInputCutoffDeterminesWhichCommitsSeeDiscoveryFailure(boolean queuedDuringIo) {
        var operationA = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(1)));
        AsyncCommitEvent operationB = new AsyncCommitEvent(Optional.of(Map.of(PARTITION, new OffsetAndMetadata(2))));
        coordinator.markCoordinatorUnknown("force discovery", time.milliseconds());
        client.prepareResponse(request -> {
            if (queuedDuringIo)
                applicationEvents.add(operationB);
            return true;
        }, FindCoordinatorResponse.prepareResponse(Errors.GROUP_AUTHORIZATION_FAILED, GROUP_ID, NODE));

        thread.runOnce();
        assertFalse(operationB.future().isDone());
        if (queuedDuringIo) {
            // A queued input suppresses the post-I/O pass. The error and operation A remain pending.
            assertFalse(operationA.isDone());
            assertTrue(coordinator.fatalError().isPresent());
        } else {
            // With no queued input, the post-I/O pass handles and clears the error before B arrives.
            assertTrue(operationA.isCompletedExceptionally());
            assertTrue(coordinator.fatalError().isEmpty());
            applicationEvents.add(operationB);
        }

        thread.runOnce();
        assertTrue(operationB.offsetsReady().isDone());
        assertTrue(operationA.isCompletedExceptionally());
        ArgumentCaptor<ErrorEvent> delivered = ArgumentCaptor.forClass(ErrorEvent.class);
        verify(background).add(delivered.capture());
        Throwable failure = delivered.getValue().error();
        assertSame(failure, assertThrows(CompletionException.class, operationA::join).getCause());
        assertTrue(coordinator.fatalError().isEmpty());
        if (queuedDuringIo) {
            // Existing next-round order is application input, then the full manager pass.
            assertTrue(operationB.future().isCompletedExceptionally());
            assertSame(failure, assertThrows(CompletionException.class, operationB.future()::join).getCause());
        } else {
            assertFalse(operationB.future().isDone());
            assertEquals(1, commits.unsentOffsetCommitRequests().size());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConfiguredLoopPreservesFatalErrorReadBeforeClear(boolean failureDuringIo) {
        var operation = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(1)));
        var offsets = commits.fetchOffsets(Set.of(PARTITION), time.milliseconds() + 60_000);
        coordinator.markCoordinatorUnknown("force discovery", time.milliseconds());
        var failure = FindCoordinatorResponse.prepareResponse(Errors.GROUP_AUTHORIZATION_FAILED, GROUP_ID, NODE);
        if (failureDuringIo) {
            // The discovery callback runs inside delegate.poll; the full post-I/O pass must deliver it.
            client.prepareResponse(failure);
        } else {
            // The fact is already present when the next pre-I/O pass starts.
            var discovery = coordinator.poll(time.milliseconds()).unsentRequests.get(0);
            discovery.handler().onComplete(new ClientResponse(
                new RequestHeader(ApiKeys.FIND_COORDINATOR, discovery.requestBuilder().build().version(), "test", 2),
                discovery.handler(), NODE.idString(), time.milliseconds(), time.milliseconds(), false, null, null, failure));
        }
        doAnswer(invocation -> {
            // The application error must not overtake the already pending operations' error delivery.
            assertTrue(operation.isCompletedExceptionally());
            assertTrue(offsets.isCompletedExceptionally());
            assertTrue(coordinator.fatalError().isEmpty());
            return null;
        }).when(background).add(any(ErrorEvent.class));

        thread.runOnce();
        assertTrue(operation.isCompletedExceptionally());
        assertTrue(offsets.isCompletedExceptionally());
        ArgumentCaptor<ErrorEvent> delivered = ArgumentCaptor.forClass(ErrorEvent.class);
        verify(background).add(delivered.capture());
        Throwable fatal = delivered.getValue().error();
        assertSame(fatal, assertThrows(CompletionException.class, operation::join).getCause());
        assertSame(fatal, assertThrows(CompletionException.class, offsets::join).getCause());
        assertTrue(coordinator.fatalError().isEmpty());

        // Preserve the original lifecycle: a later operation does not inherit the consumed error.
        var later = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(2)));
        thread.runOnce();
        assertFalse(later.isDone());
        verify(background).add(any(ErrorEvent.class));

        time.sleep(1000);
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, GROUP_ID, NODE));
        thread.runOnce();
        assertFalse(later.isDone());
        thread.runOnce();
        client.respondToRequest(request(ApiKeys.OFFSET_COMMIT), new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE)));
        thread.runOnce();
        assertTrue(later.isDone());
        assertFalse(later.isCompletedExceptionally());
        assertSame(fatal, assertThrows(CompletionException.class, operation::join).getCause());
        verify(background).add(any(ErrorEvent.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCoordinatorFatalErrorDeliveryDependsOnReadBeforeClear(boolean heartbeatFirst) {
        var operation = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(1)));
        var offsets = commits.fetchOffsets(Set.of(PARTITION), time.milliseconds() + 60_000);
        coordinator.markCoordinatorUnknown("force discovery", time.milliseconds());
        var discovery = coordinator.poll(time.milliseconds()).unsentRequests.get(0);
        discovery.handler().onComplete(new ClientResponse(
            new RequestHeader(ApiKeys.FIND_COORDINATOR, discovery.requestBuilder().build().version(), "test", 2),
            discovery.handler(), NODE.idString(), time.milliseconds(), time.milliseconds(), false, null, null,
            FindCoordinatorResponse.prepareResponse(Errors.GROUP_AUTHORIZATION_FAILED, GROUP_ID, NODE)));
        Throwable fatal = coordinator.fatalError().orElseThrow();

        // Existing order is commit first. The reverse is an extension probe, not production scheduling.
        if (heartbeatFirst) {
            heartbeat.poll(time.milliseconds());
            commits.poll(time.milliseconds());
        } else {
            commits.poll(time.milliseconds());
            heartbeat.poll(time.milliseconds());
        }
        // A later pass without another discovery response cannot recover the consumed failure.
        assertTrue(commits.poll(time.milliseconds()).unsentRequests.isEmpty());
        heartbeat.poll(time.milliseconds());
        ArgumentCaptor<ErrorEvent> delivered = ArgumentCaptor.forClass(ErrorEvent.class);
        verify(background).add(delivered.capture());
        assertSame(fatal, delivered.getValue().error());
        assertTrue(coordinator.fatalError().isEmpty());
        if (heartbeatFirst) {
            // Characterization, not an endorsed contract: the alternative order loses operation error delivery.
            assertFalse(operation.isDone());
            assertFalse(offsets.isDone());
            assertEquals(1, commits.unsentOffsetCommitRequests().size());
        } else {
            assertTrue(operation.isCompletedExceptionally());
            assertTrue(offsets.isCompletedExceptionally());
            assertSame(fatal, assertThrows(CompletionException.class, operation::join).getCause());
            assertSame(fatal, assertThrows(CompletionException.class, offsets::join).getCause());
            assertTrue(commits.unsentOffsetCommitRequests().isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testLeaveTransitionDoesNotRewriteAnAdmittedCommit(boolean leaveBeforeCommitAdmission) {
        try (Metrics transitionMetrics = new Metrics(time)) {
            ConsumerMembershipManager realMembership = new ConsumerMembershipManager(GROUP_ID,
                    Optional.empty(), Optional.empty(), 30_000, Optional.empty(), subscriptions,
                    commits, metadata, logContext, background, time, transitionMetrics, false);
            realMembership.registerStateListener(commits);
            realMembership.transitionToJoining();
            realMembership.updateMemberEpoch(7);
            ConsumerHeartbeatRequestManager realHeartbeat = new ConsumerHeartbeatRequestManager(
                    logContext, time, config, coordinator, subscriptions, realMembership, background, transitionMetrics);
            time.sleep(config.getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG) + 1L);

            var operation = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(1)));
            NetworkClientDelegate.PollResult commitResult;
            NetworkClientDelegate.PollResult leaveResult;
            if (leaveBeforeCommitAdmission) {
                // Contrast: the owner transition is already known when commit admission starts.
                leaveResult = realHeartbeat.poll(time.milliseconds());
                commitResult = commits.poll(time.milliseconds());
            } else {
                // Existing RequestManagers order: commit builds before heartbeat observes poll expiry.
                commitResult = commits.poll(time.milliseconds());
                leaveResult = realHeartbeat.poll(time.milliseconds());
            }

            assertEquals(MemberState.STALE, realMembership.state());
            assertEquals(1, commitResult.unsentRequests.size());
            assertEquals(1, leaveResult.unsentRequests.size());
            OffsetCommitRequest commitRequest = (OffsetCommitRequest) commitResult.unsentRequests.get(0).requestBuilder().build();
            ConsumerGroupHeartbeatRequest leaveRequest = (ConsumerGroupHeartbeatRequest) leaveResult.unsentRequests.get(0).requestBuilder().build();
            assertEquals(leaveBeforeCommitAdmission ? -1 : 7, commitRequest.data().generationIdOrMemberEpoch());
            assertEquals(-1, leaveRequest.data().memberEpoch());

            if (leaveBeforeCommitAdmission) {
                delegate.addAll(leaveResult);
                delegate.addAll(commitResult);
            } else {
                delegate.addAll(commitResult);
                delegate.addAll(leaveResult);
            }
            delegate.poll(0, time.milliseconds());
            assertEquals(leaveBeforeCommitAdmission
                            ? List.of(ApiKeys.CONSUMER_GROUP_HEARTBEAT, ApiKeys.OFFSET_COMMIT)
                            : List.of(ApiKeys.OFFSET_COMMIT, ApiKeys.CONSUMER_GROUP_HEARTBEAT),
                    client.requests().stream().map(r -> r.requestBuilder().apiKey()).collect(Collectors.toList()));
            assertEquals(leaveBeforeCommitAdmission ? -1 : 7,
                    ((OffsetCommitRequest) request(ApiKeys.OFFSET_COMMIT).requestBuilder().build()).data().generationIdOrMemberEpoch());

            // A broker success belongs to that admitted attempt; membership changes do not erase it.
            client.respondToRequest(request(ApiKeys.OFFSET_COMMIT), new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE)));
            delegate.poll(0, time.milliseconds());
            assertTrue(operation.isDone());
            assertFalse(operation.isCompletedExceptionally());
        }
    }

    private enum CommitMode { ASYNC, SYNC, PERIODIC, BEFORE_REBALANCE }

    @Test
    void testCommitAdmissionReservesOnceAndRechecksRetryState() {
        commits.onMemberEpochUpdated(Optional.of(7), "member");
        var operation = commits.commitSync(Map.of(PARTITION, new OffsetAndMetadata(10)), time.milliseconds() + 10_000);
        var attempt = commits.unsentOffsetCommitRequests().element();
        var initial = commits.poll(time.milliseconds());
        assertEquals(1, initial.unsentRequests.size());
        assertTrue(attempt.requestInFlight());
        assertTrue(attempt.tryAdmit(time.milliseconds()).isEmpty(), "An in-flight attempt cannot be admitted twice");
        delegate.addAll(initial);
        delegate.poll(0, time.milliseconds());
        OffsetCommitRequest first = (OffsetCommitRequest) request(ApiKeys.OFFSET_COMMIT).requestBuilder().build();
        assertEquals(7, first.data().generationIdOrMemberEpoch());

        client.respondToRequest(request(ApiKeys.OFFSET_COMMIT),
                new OffsetCommitResponse(0, Map.of(PARTITION, Errors.COORDINATOR_LOAD_IN_PROGRESS)));
        delegate.poll(0, time.milliseconds());
        assertFalse(operation.isDone());
        assertFalse(attempt.requestInFlight());
        assertTrue(attempt.tryAdmit(time.milliseconds()).isEmpty(), "Retry admission must respect backoff");
        assertFalse(attempt.requestInFlight(), "Rejected admission must not reserve an attempt");
        assertTrue(commits.poll(time.milliseconds()).unsentRequests.isEmpty());

        commits.onMemberEpochUpdated(Optional.of(8), "member");
        time.sleep(100);
        var retry = commits.poll(time.milliseconds());
        assertEquals(1, retry.unsentRequests.size());
        assertTrue(attempt.requestInFlight());
        assertTrue(attempt.tryAdmit(time.milliseconds()).isEmpty());
        OffsetCommitRequest second = (OffsetCommitRequest) retry.unsentRequests.get(0).requestBuilder().build();
        assertEquals(8, second.data().generationIdOrMemberEpoch());
        assertEquals(7, first.data().generationIdOrMemberEpoch());
        delegate.addAll(retry);
        delegate.poll(0, time.milliseconds());
        client.respondToRequest(request(ApiKeys.OFFSET_COMMIT), new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE)));
        delegate.poll(0, time.milliseconds());
        assertTrue(operation.isDone());
        assertFalse(operation.isCompletedExceptionally());
        assertTrue(commits.unsentOffsetCommitRequests().isEmpty());
    }

    @ParameterizedTest
    @CsvSource({
        "ASYNC, STALE_MEMBER_EPOCH", "ASYNC, UNKNOWN_MEMBER_ID", "ASYNC, COORDINATOR_LOAD_IN_PROGRESS",
        "SYNC, STALE_MEMBER_EPOCH", "SYNC, UNKNOWN_MEMBER_ID", "SYNC, COORDINATOR_LOAD_IN_PROGRESS",
        "PERIODIC, STALE_MEMBER_EPOCH", "PERIODIC, UNKNOWN_MEMBER_ID", "PERIODIC, COORDINATOR_LOAD_IN_PROGRESS",
        "BEFORE_REBALANCE, STALE_MEMBER_EPOCH", "BEFORE_REBALANCE, UNKNOWN_MEMBER_ID", "BEFORE_REBALANCE, COORDINATOR_LOAD_IN_PROGRESS"
    })
    void testCommitFailureAfterPollTimeout(CommitMode mode, Errors error) {
        try (Metrics transitionMetrics = new Metrics(time)) {
            Map<String, Object> properties = new java.util.HashMap<>(config.originals());
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
            ConsumerConfig autoConfig = new ConsumerConfig(properties);
            CommitRequestManager localCommits = new CommitRequestManager(time, logContext,
                    subscriptions, autoConfig, coordinator, mock(OffsetCommitCallbackInvoker.class),
                    GROUP_ID, Optional.empty(), 100, 1000, OptionalDouble.of(0), transitionMetrics, metadata);
            ConsumerMembershipManager realMembership = new ConsumerMembershipManager(GROUP_ID,
                    Optional.empty(), Optional.empty(), 30_000, Optional.empty(), subscriptions,
                    localCommits, metadata, logContext, background, time, transitionMetrics, true);
            realMembership.registerStateListener(localCommits);
            realMembership.transitionToJoining();
            realMembership.updateMemberEpoch(7);
            // Controlled assignment with no user rebalance listener: loss releases it synchronously.
            subscriptions.subscribe(Set.of(PARTITION.topic()));
            subscriptions.assignFromSubscribed(Set.of(PARTITION));
            subscriptions.seek(PARTITION, 10);
            ConsumerHeartbeatRequestManager realHeartbeat = new ConsumerHeartbeatRequestManager(
                    logContext, time, autoConfig, coordinator, subscriptions, realMembership, background, transitionMetrics);
            time.sleep(autoConfig.getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG) + 1L);

            CompletableFuture<?> operation;
            switch (mode) {
                case ASYNC:
                    operation = localCommits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(10)));
                    break;
                case SYNC:
                    operation = localCommits.commitSync(Map.of(PARTITION, new OffsetAndMetadata(10)), time.milliseconds() + 10_000);
                    break;
                case BEFORE_REBALANCE:
                    operation = localCommits.maybeAutoCommitSyncBeforeRebalance(time.milliseconds() + 10_000);
                    break;
                case PERIODIC:
                    localCommits.updateTimerAndMaybeCommit(time.milliseconds());
                    operation = localCommits.unsentOffsetCommitRequests().element().future();
                    break;
                default:
                    throw new AssertionError(mode);
            }
            var admitted = localCommits.poll(time.milliseconds());
            assertEquals(1, admitted.unsentRequests.size());
            delegate.addAll(admitted);
            delegate.addAll(realHeartbeat.poll(time.milliseconds()));
            assertEquals(MemberState.STALE, realMembership.state());
            assertTrue(subscriptions.allConsumed().isEmpty());
            delegate.poll(0, time.milliseconds());
            OffsetCommitRequest first = (OffsetCommitRequest) request(ApiKeys.OFFSET_COMMIT).requestBuilder().build();
            assertEquals(7, first.data().generationIdOrMemberEpoch());
            client.respondToRequest(request(ApiKeys.OFFSET_COMMIT), new OffsetCommitResponse(0, Map.of(PARTITION, error)));
            delegate.poll(0, time.milliseconds());

            if (mode == CommitMode.SYNC && error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                assertFalse(operation.isDone());
                assertTrue(localCommits.poll(time.milliseconds()).unsentRequests.isEmpty());
                time.sleep(100);
                var retry = localCommits.poll(time.milliseconds());
                assertEquals(1, retry.unsentRequests.size());
                OffsetCommitRequest retried = (OffsetCommitRequest) retry.unsentRequests.get(0).requestBuilder().build();
                assertEquals(-1, retried.data().generationIdOrMemberEpoch());
                assertEquals(10, retried.data().topics().get(0).partitions().get(0).committedOffset());
                assertEquals(7, first.data().generationIdOrMemberEpoch());
                delegate.addAll(retry);
                delegate.poll(0, time.milliseconds());
                client.respondToRequest(request(ApiKeys.OFFSET_COMMIT), new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE)));
                delegate.poll(0, time.milliseconds());
                assertTrue(operation.isDone());
                assertFalse(operation.isCompletedExceptionally());
            } else if (mode == CommitMode.BEFORE_REBALANCE && error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                // Retry refreshes offsets after assignment loss; empty work completes without another send.
                assertTrue(operation.isDone());
                assertFalse(operation.isCompletedExceptionally());
            } else {
                assertTrue(operation.isCompletedExceptionally());
                Throwable cause = assertThrows(CompletionException.class, operation::join).getCause();
                if (error == Errors.UNKNOWN_MEMBER_ID || (mode == CommitMode.SYNC && error == Errors.STALE_MEMBER_EPOCH))
                    assertInstanceOf(CommitFailedException.class, cause);
                else if (mode == CommitMode.ASYNC && error == Errors.COORDINATOR_LOAD_IN_PROGRESS)
                    assertInstanceOf(RetriableCommitFailedException.class, cause);
                else
                    assertInstanceOf(error.exception().getClass(), cause);
            }
            assertTrue(localCommits.poll(time.milliseconds()).unsentRequests.isEmpty());
            assertTrue(localCommits.unsentOffsetCommitRequests().isEmpty());
            if (mode == CommitMode.PERIODIC) {
                time.sleep(1000);
                localCommits.updateTimerAndMaybeCommit(time.milliseconds());
                assertTrue(localCommits.poll(time.milliseconds()).unsentRequests.isEmpty());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCompletionObserverIsNotABatchSnapshot(boolean heartbeatFirst) {
        var commit = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(1)));
        AtomicReference<Boolean> coordinatorKnownAtCompletion = new AtomicReference<>();
        var observer = commit.thenRun(() -> coordinatorKnownAtCompletion.set(coordinator.coordinator().isPresent()));
        thread.runOnce();
        ClientRequest commitRequest = request(ApiKeys.OFFSET_COMMIT);
        ClientRequest heartbeatRequest = request(ApiKeys.CONSUMER_GROUP_HEARTBEAT);
        var commitResponse = new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE));
        var heartbeatResponse = new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NOT_COORDINATOR.code()));
        if (heartbeatFirst) {
            client.respondToRequest(heartbeatRequest, heartbeatResponse);
            client.respondToRequest(commitRequest, commitResponse);
        } else {
            client.respondToRequest(commitRequest, commitResponse);
            client.respondToRequest(heartbeatRequest, heartbeatResponse);
        }
        thread.runOnce();
        observer.join();
        assertEquals(!heartbeatFirst, coordinatorKnownAtCompletion.get());
        assertTrue(coordinator.coordinator().isEmpty());
        assertFalse(commit.isCompletedExceptionally());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testHeartbeatInvalidationPrecedesFollowupAdmissionInEitherResponseOrder(boolean heartbeatFirst) {
        var first = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(1)));
        var followup = first.thenCompose(ignored -> commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(2))));
        thread.runOnce();
        ClientRequest commit = request(ApiKeys.OFFSET_COMMIT);
        ClientRequest heartbeat = request(ApiKeys.CONSUMER_GROUP_HEARTBEAT);
        var commitResponse = new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE));
        var heartbeatResponse = new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
            .setErrorCode(Errors.NOT_COORDINATOR.code()));
        if (heartbeatFirst) {
            client.respondToRequest(heartbeat, heartbeatResponse);
            client.respondToRequest(commit, commitResponse);
        } else {
            client.respondToRequest(commit, commitResponse);
            client.respondToRequest(heartbeat, heartbeatResponse);
        }

        long batchTime = time.milliseconds();
        thread.runOnce();
        assertTrue(first.isDone());
        assertFalse(first.isCompletedExceptionally(), "a valid success remains valid despite owner invalidation");
        assertFalse(followup.isDone());
        assertTrue(coordinator.coordinator().isEmpty());
        assertEquals(0, client.inFlightRequestCount(), "no transport send is interleaved with callbacks");
        assertEquals(1, delegate.unsentRequests().size(), "the post-batch pass admits discovery in this runOnce");
        assertEquals(ApiKeys.FIND_COORDINATOR, delegate.unsentRequests().peek().requestBuilder().apiKey());

        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, GROUP_ID, NODE));
        thread.runOnce();
        assertTrue(coordinator.coordinator().isPresent());
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(2, delegate.unsentRequests().size(), "recovery admits commit and heartbeat before returning");
        var nextCommit = delegate.unsentRequests().stream()
            .filter(r -> r.requestBuilder().apiKey() == ApiKeys.OFFSET_COMMIT).findFirst().orElseThrow();
        var builder = assertInstanceOf(OffsetCommitRequest.Builder.class, nextCommit.requestBuilder());
        assertEquals(2, builder.build().data().topics().get(0).partitions().get(0).committedOffset());
        assertEquals(coordinator.coordinator(), nextCommit.node());
        assertEquals(batchTime, time.milliseconds(), "no periodic timer tick is needed for this recovery");

        thread.runOnce();
        assertEquals(2, client.inFlightRequestCount(), "the next transport poll sends each admitted attempt once");
        thread.runOnce();
        assertEquals(2, client.inFlightRequestCount(), "an unchanged in-flight attempt cannot be duplicated");
    }

    @Test
    void testReadyFollowupIsAdmittedWithoutWaitingForUnrelatedHeartbeat() {
        var first = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(1)));
        var followup = first.thenCompose(ignored -> commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(2))));
        thread.runOnce();
        client.respondToRequest(request(ApiKeys.OFFSET_COMMIT), new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE)));
        thread.runOnce();
        assertTrue(first.isDone());
        assertFalse(followup.isDone());
        assertEquals(1, client.inFlightRequestCount(), "unrelated heartbeat is still in flight");
        assertEquals(1, delegate.unsentRequests().size(), "ready followup is admitted in the completion iteration");
        assertEquals(ApiKeys.OFFSET_COMMIT, delegate.unsentRequests().peek().requestBuilder().apiKey());
    }

    @Test
    void testImmediateFollowupResponseDoesNotRecursivelyDrainAnotherBatch() {
        var first = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(1)));
        var second = first.thenCompose(ignored -> commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(2))));
        var third = second.thenCompose(ignored -> commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(3))));
        thread.runOnce();
        client.respondToRequest(request(ApiKeys.OFFSET_COMMIT), new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE)));
        client.prepareResponse(new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE)));
        thread.runOnce();
        assertTrue(first.isDone());
        assertFalse(second.isDone(), "even an immediately available next response belongs to another I/O batch");
        assertFalse(third.isDone());
        assertEquals(1, delegate.unsentRequests().size());
        assertEquals(1, client.numAwaitingResponses());

        thread.runOnce();
        assertTrue(second.isDone());
        assertFalse(third.isDone());
        assertEquals(1, delegate.unsentRequests().size(), "only the next attempt is staged, not recursively executed");
    }

    @Test
    void testLateHeartbeatInvalidationCannotClearRediscoveredOwner() {
        var first = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(1)));
        var followup = first.thenCompose(ignored -> commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(2))));
        thread.runOnce();
        ClientRequest oldHeartbeat = request(ApiKeys.CONSUMER_GROUP_HEARTBEAT);
        long capturedVersion = coordinator.coordinatorVersion();
        coordinator.markCoordinatorUnknown("test rediscovery", time.milliseconds());
        discoverCoordinator();
        long currentVersion = coordinator.coordinatorVersion();
        assertTrue(currentVersion > capturedVersion);
        client.respondToRequest(request(ApiKeys.OFFSET_COMMIT), new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE)));
        client.respondToRequest(oldHeartbeat, new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
            .setErrorCode(Errors.NOT_COORDINATOR.code())));

        thread.runOnce();
        assertFalse(first.isCompletedExceptionally());
        assertFalse(followup.isDone());
        assertTrue(coordinator.coordinator().isPresent());
        assertEquals(currentVersion, coordinator.coordinatorVersion());
        assertTrue(delegate.unsentRequests().stream().anyMatch(r -> r.requestBuilder().apiKey() == ApiKeys.OFFSET_COMMIT),
            "owner-side version fencing remains necessary even with an ordered completion batch");
    }
}
