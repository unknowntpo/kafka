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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerHeartbeatRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager;
import org.apache.kafka.clients.consumer.internals.FetchRequestManager;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.OffsetsRequestManager;
import org.apache.kafka.clients.consumer.internals.PositionsValidator;
import org.apache.kafka.clients.consumer.internals.RequestManagerScheduler;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager;
import org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager;
import org.apache.kafka.clients.consumer.internals.ShareMembershipManager;
import org.apache.kafka.clients.consumer.internals.StreamsGroupHeartbeatRequestManager;
import org.apache.kafka.clients.consumer.internals.StreamsGroupTopologyDescriptionRequestManager;
import org.apache.kafka.clients.consumer.internals.StreamsMembershipManager;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.TopicMetadataRequestManager;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.verification.VerificationMode;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class ApplicationEventProcessorTest {
    private final Time time = new MockTime();
    private final CommitRequestManager commitRequestManager = mock(CommitRequestManager.class);
    private final ConsumerHeartbeatRequestManager heartbeatRequestManager = mock(ConsumerHeartbeatRequestManager.class);
    private final ConsumerMembershipManager membershipManager = mock(ConsumerMembershipManager.class);
    private final OffsetsRequestManager offsetsRequestManager = mock(OffsetsRequestManager.class);
    private final FetchRequestManager fetchRequestManager = mock(FetchRequestManager.class);
    private SubscriptionState subscriptionState = mock(SubscriptionState.class);
    private final ConsumerMetadata metadata = mock(ConsumerMetadata.class);
    private final StreamsGroupHeartbeatRequestManager streamsGroupHeartbeatRequestManager = mock(StreamsGroupHeartbeatRequestManager.class);
    private final StreamsGroupTopologyDescriptionRequestManager streamsGroupTopologyDescriptionRequestManager = mock(StreamsGroupTopologyDescriptionRequestManager.class);
    private final StreamsMembershipManager streamsMembershipManager = mock(StreamsMembershipManager.class);
    private final ShareHeartbeatRequestManager shareHeartbeatRequestManager = mock(ShareHeartbeatRequestManager.class);
    private final ShareMembershipManager shareMembershipManager = mock(ShareMembershipManager.class);
    private ApplicationEventProcessor processor;
    private final ArrayDeque<FetchRequestManager.FetchContinuation> fetchContinuations = new ArrayDeque<>();
    private boolean fetchContinuationsClosed;

    private void processFetchContinuations(int count) {
        for (int i = 0; i < count && !fetchContinuations.isEmpty(); i++)
            fetchContinuations.removeFirst().advance();
    }

    private void setupProcessor(boolean withGroupId) {
        lenient().doAnswer(invocation -> {
            FetchRequestManager.FetchContinuation continuation = invocation.getArgument(0);
            if (fetchContinuationsClosed)
                continuation.onClose();
            else
                fetchContinuations.addLast(continuation);
            return null;
        }).when(fetchRequestManager).enqueueFetchContinuation(any());
        lenient().doAnswer(invocation -> {
            fetchContinuationsClosed = true;
            while (!fetchContinuations.isEmpty())
                fetchContinuations.removeFirst().onClose();
            return null;
        }).when(fetchRequestManager).closeFetchContinuations();
        RequestManagers requestManagers = new RequestManagers(
                new LogContext(),
                offsetsRequestManager,
                mock(TopicMetadataRequestManager.class),
                fetchRequestManager,
                withGroupId ? Optional.of(mock(CoordinatorRequestManager.class)) : Optional.empty(),
                withGroupId ? Optional.of(commitRequestManager) : Optional.empty(),
                withGroupId ? Optional.of(heartbeatRequestManager) : Optional.empty(),
                withGroupId ? Optional.of(membershipManager) : Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()
        );
        processor = new ApplicationEventProcessor(
                new LogContext(),
                requestManagers,
                metadata,
                subscriptionState
        );
    }

    private void setupStreamProcessor(boolean withGroupId) {
        RequestManagers requestManagers = new RequestManagers(
            new LogContext(),
            offsetsRequestManager,
            mock(TopicMetadataRequestManager.class),
            mock(FetchRequestManager.class),
            withGroupId ? Optional.of(mock(CoordinatorRequestManager.class)) : Optional.empty(),
            withGroupId ? Optional.of(commitRequestManager) : Optional.empty(),
            withGroupId ? Optional.of(heartbeatRequestManager) : Optional.empty(),
            Optional.empty(),
            withGroupId ? Optional.of(streamsGroupHeartbeatRequestManager) : Optional.empty(),
            withGroupId ? Optional.of(streamsGroupTopologyDescriptionRequestManager) : Optional.empty(),
            withGroupId ? Optional.of(streamsMembershipManager) : Optional.empty()
        );
        processor = new ApplicationEventProcessor(
            new LogContext(),
            requestManagers,
            metadata,
            subscriptionState
        );
    }

    private void setupShareProcessor() {
        RequestManagers requestManagers = new RequestManagers(
            new LogContext(),
            mock(ShareConsumeRequestManager.class),
            Optional.of(mock(CoordinatorRequestManager.class)),
            Optional.of(shareHeartbeatRequestManager),
            Optional.of(shareMembershipManager)
        );
        processor = new ApplicationEventProcessor(
            new LogContext(),
            requestManagers,
            metadata,
            subscriptionState
        );
    }

    @Test
    public void testPrepClosingCommitEvents() {
        setupProcessor(true);
        List<NetworkClientDelegate.UnsentRequest> results = mockCommitResults();
        doReturn(new NetworkClientDelegate.PollResult(100, results)).when(commitRequestManager).pollOnClose(anyLong());
        processor.process(new CommitOnCloseEvent());
        verify(commitRequestManager).signalClose();
    }

    @Test
    public void testProcessUnsubscribeEventWithGroupId() {
        setupProcessor(true);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        when(membershipManager.leaveGroup()).thenReturn(CompletableFuture.completedFuture(null));
        processor.process(new UnsubscribeEvent(0));
        verify(membershipManager).leaveGroup();
    }

    @Test
    public void testProcessUnsubscribeEventWithoutGroupId() {
        setupProcessor(false);
        UnsubscribeEvent event = new UnsubscribeEvent(0);
        expectPositionNotificationBeforeCompletion(event, () -> verify(subscriptionState).unsubscribe());
        processor.process(event);
        verify(subscriptionState).unsubscribe();
        verify(offsetsRequestManager).onPositionStateChanged();
    }

    @ParameterizedTest
    @MethodSource("applicationEvents")
    public void testApplicationEventIsProcessed(ApplicationEvent e) {
        ApplicationEventProcessor applicationEventProcessor = mock(ApplicationEventProcessor.class);
        applicationEventProcessor.process(e);
        verify(applicationEventProcessor).process(any(e.getClass()));
    }

    private static Stream<Arguments> applicationEvents() {
        return Stream.of(
                Arguments.of(new AsyncPollEvent(calculateDeadlineMs(12345, 100), 100)),
                Arguments.of(new CreateFetchRequestsEvent(calculateDeadlineMs(12345, 100))),
                Arguments.of(new CheckAndUpdatePositionsEvent(500)),
                Arguments.of(new TopicMetadataEvent("topic", Long.MAX_VALUE)),
                Arguments.of(new AssignmentChangeEvent(12345, 12345, Collections.emptyList())));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testListOffsetsEventIsProcessed(boolean requireTimestamp) {
        ApplicationEventProcessor applicationEventProcessor = mock(ApplicationEventProcessor.class);
        Map<TopicPartition, Long> timestamps = Collections.singletonMap(new TopicPartition("topic1", 1), 5L);
        ApplicationEvent e = new ListOffsetsEvent(timestamps, calculateDeadlineMs(time, 100), requireTimestamp);
        applicationEventProcessor.process(e);
        verify(applicationEventProcessor).process(any(ListOffsetsEvent.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testAssignmentChangeEvent(boolean withGroupId) {
        final long currentTimeMs = 12345;
        TopicPartition tp = new TopicPartition("topic", 0);
        AssignmentChangeEvent event = new AssignmentChangeEvent(currentTimeMs, 12345, Collections.singleton(tp));

        setupProcessor(withGroupId);
        doReturn(true).when(subscriptionState).assignFromUser(Collections.singleton(tp));
        expectPositionNotificationBeforeCompletion(event,
                () -> verify(subscriptionState).assignFromUser(Collections.singleton(tp)));
        processor.process(event);
        if (withGroupId) {
            verify(commitRequestManager).updateTimerAndMaybeCommit(currentTimeMs);
        } else {
            verify(commitRequestManager, never()).updateTimerAndMaybeCommit(currentTimeMs);
        }
        verify(metadata).requestUpdateForNewTopics();
        verify(subscriptionState).assignFromUser(Collections.singleton(tp));
        verify(offsetsRequestManager).onPositionStateChanged();
        assertDoesNotThrow(() -> event.future().get());
    }

    @Test
    public void testAssignmentChangeEventWithException() {
        AssignmentChangeEvent event = new AssignmentChangeEvent(12345, 12345, Collections.emptyList());

        setupProcessor(false);
        doThrow(new IllegalStateException()).when(subscriptionState).assignFromUser(any());
        processor.process(event);
        verify(offsetsRequestManager, never()).onPositionStateChanged();

        ExecutionException e = assertThrows(ExecutionException.class, () -> event.future().get());
        assertInstanceOf(IllegalStateException.class, e.getCause());
    }

    @Test
    public void testResetOffsetEvent() {
        Collection<TopicPartition> tp = Collections.singleton(new TopicPartition("topic", 0));
        AutoOffsetResetStrategy strategy = AutoOffsetResetStrategy.LATEST;
        ResetOffsetEvent event = new ResetOffsetEvent(tp, strategy, 12345);

        setupProcessor(false);
        expectPositionNotificationBeforeCompletion(event,
                () -> verify(subscriptionState).requestOffsetReset(event.topicPartitions(), event.offsetResetStrategy()));
        processor.process(event);
        verify(subscriptionState).requestOffsetReset(event.topicPartitions(), event.offsetResetStrategy());
        verify(offsetsRequestManager).onPositionStateChanged();
    }

    @Test
    public void testSeekUnvalidatedEvent() {
        TopicPartition tp = new TopicPartition("topic", 0);
        Optional<Integer> offsetEpoch = Optional.of(1);
        SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
                0, offsetEpoch, Metadata.LeaderAndEpoch.noLeaderOrEpoch());
        SeekUnvalidatedEvent event = new SeekUnvalidatedEvent(12345, tp, 0, offsetEpoch);

        setupProcessor(false);
        doReturn(Metadata.LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(tp);
        doNothing().when(subscriptionState).seekUnvalidated(eq(tp), any());
        expectPositionNotificationBeforeCompletion(event,
                () -> verify(subscriptionState).seekUnvalidated(tp, position));
        processor.process(event);
        verify(metadata).updateLastSeenEpochIfNewer(tp, offsetEpoch.get());
        verify(metadata).currentLeader(tp);
        verify(subscriptionState).seekUnvalidated(tp, position);
        verify(offsetsRequestManager).onPositionStateChanged();
        assertDoesNotThrow(() -> event.future().get());
    }

    @Test
    public void testSeekUnvalidatedEventWithException() {
        TopicPartition tp = new TopicPartition("topic", 0);
        SeekUnvalidatedEvent event = new SeekUnvalidatedEvent(12345, tp, 0, Optional.empty());

        setupProcessor(false);
        doReturn(Metadata.LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(tp);
        doThrow(new IllegalStateException()).when(subscriptionState).seekUnvalidated(eq(tp), any());
        processor.process(event);
        verify(offsetsRequestManager, never()).onPositionStateChanged();

        ExecutionException e = assertThrows(ExecutionException.class, () -> event.future().get());
        assertInstanceOf(IllegalStateException.class, e.getCause());
    }

    private void expectPositionNotificationBeforeCompletion(CompletableApplicationEvent<?> event, Runnable verifyMutation) {
        doAnswer(invocation -> {
            verifyMutation.run();
            assertFalse(event.future().isDone(), "publish the state change before completing the event");
            return null;
        }).when(offsetsRequestManager).onPositionStateChanged();
    }

    @Test
    public void testAssignmentChangeWithoutNewTopicsNotifiesPositions() {
        setupProcessor(false);
        TopicPartition tp = new TopicPartition("topic", 1);
        AssignmentChangeEvent event = new AssignmentChangeEvent(12345, 12345, Set.of(tp));
        when(subscriptionState.assignFromUser(Set.of(tp))).thenReturn(false);
        expectPositionNotificationBeforeCompletion(event,
                () -> verify(subscriptionState).assignFromUser(Set.of(tp)));
        processor.process(event);
        verify(metadata, never()).requestUpdateForNewTopics();
        verify(offsetsRequestManager).onPositionStateChanged();
        assertDoesNotThrow(() -> event.future().get());
    }

    @Test
    public void testFailedResetDoesNotNotifyPositions() {
        setupProcessor(false);
        ResetOffsetEvent event = new ResetOffsetEvent(Set.of(new TopicPartition("topic", 0)),
                AutoOffsetResetStrategy.EARLIEST, 12345);
        doThrow(new IllegalStateException()).when(subscriptionState).requestOffsetReset(eq(event.topicPartitions()), eq(event.offsetResetStrategy()));
        processor.process(event);
        assertFutureThrows(IllegalStateException.class, event.future());
        verify(offsetsRequestManager, never()).onPositionStateChanged();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testApplyAssignmentNotifiesPositions(boolean streams) {
        if (streams)
            setupStreamProcessor(true);
        else
            setupProcessor(true);
        ApplyAssignmentEvent event = new ApplyAssignmentEvent(Set.of(new TopicPartition("topic", 0)),
                Collections.emptySortedSet());
        expectPositionNotificationBeforeCompletion(event, () -> {
            if (streams)
                verify(streamsMembershipManager).applyAssignment(event.assignedPartitions(), event.addedPartitions());
            else
                verify(membershipManager).applyAssignment(event.assignedPartitions(), event.addedPartitions());
        });
        processor.process(event);
        verify(offsetsRequestManager).onPositionStateChanged();
        assertDoesNotThrow(() -> event.future().get());
    }

    @Test
    public void testFailedApplyAssignmentDoesNotNotifyPositions() {
        setupProcessor(true);
        ApplyAssignmentEvent event = new ApplyAssignmentEvent(Set.of(new TopicPartition("topic", 0)),
                Collections.emptySortedSet());
        doThrow(new IllegalStateException()).when(membershipManager).applyAssignment(any(), any());
        processor.process(event);
        assertFutureThrows(IllegalStateException.class, event.future());
        verify(offsetsRequestManager, never()).onPositionStateChanged();
    }

    @Test
    public void testSeekActivatesRealRetainedValidationOwner() {
        LogContext context = new LogContext();
        Time clock = new MockTime(0, 0, 0);
        TopicPartition tp = new TopicPartition("topic", 0);
        Node leader = new Node(1, "localhost", 9092);
        Metadata.LeaderAndEpoch leaderEpoch = new Metadata.LeaderAndEpoch(Optional.of(leader), Optional.of(3));
        when(metadata.currentLeader(tp)).thenReturn(leaderEpoch);
        SubscriptionState subscriptions = new SubscriptionState(context, AutoOffsetResetStrategy.EARLIEST);
        subscriptions.assignFromUser(Set.of(tp));
        subscriptions.seekUnvalidated(tp, new SubscriptionState.FetchPosition(5, Optional.of(2), leaderEpoch));
        OffsetsRequestManager offsets = new OffsetsRequestManager(subscriptions, metadata,
                IsolationLevel.READ_UNCOMMITTED, clock, 100, 1000, 5000, new ApiVersions(),
                mock(NetworkClientDelegate.class), null,
                new PositionsValidator(context, clock, subscriptions, metadata), context);
        RequestManagers managers = new RequestManagers(context, offsets, mock(TopicMetadataRequestManager.class),
                fetchRequestManager, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty());
        ApplicationEventProcessor eventProcessor = new ApplicationEventProcessor(context, managers, metadata, subscriptions);
        CompletableFuture<Void> positionReady = offsets.updateFetchPositionsAndAwaitValidation(5000, () -> true);
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            assertTrue(scheduler.poll(offsets, 0).unsentRequests.isEmpty());
            assertNull(scheduler.poll(offsets, 1));
            SeekUnvalidatedEvent seek = new SeekUnvalidatedEvent(5000, tp, 20, Optional.empty());
            eventProcessor.process(seek);
            assertDoesNotThrow(() -> seek.future().get());
            assertFalse(positionReady.isDone(), "event notification must defer owner work");
            assertTrue(scheduler.poll(offsets, 1).unsentRequests.isEmpty());
            assertTrue(positionReady.isDone());
            assertFalse(positionReady.isCompletedExceptionally());
            assertEquals(20, subscriptions.position(tp).offset);
        } finally {
            offsets.closePendingPositionResets();
        }
    }

    @Test
    public void testCancelledAsyncPollBeforeProcessingDoesNoPreparation() {
        setupProcessor(false);
        AsyncPollEvent event = new AsyncPollEvent(5000, 0);
        event.requestCancellation();
        processor.process(event);
        verify(offsetsRequestManager, never()).updateFetchPositionsAndAwaitValidation(anyLong(), any());
        verify(fetchRequestManager, never()).createFetchRequestsWithReconnect(anyLong(), any());
        assertFalse(event.isComplete(), "only the cancellation event terminalizes the abandoned poll");
        processor.process(new CancelAsyncPollEvent(event));
        assertTrue(event.isComplete());
        assertTrue(event.error().isEmpty());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    public void testCancellationRevokesPositionAndFetchContinuationBeforeOwnerCompletion(int stage) {
        setupProcessor(false);
        CompletableFuture<Void> positions = new CompletableFuture<>();
        CompletableFuture<Void> fetch = new CompletableFuture<>();
        AtomicReference<BooleanSupplier> positionPermission = new AtomicReference<>();
        AtomicReference<BooleanSupplier> fetchPermission = new AtomicReference<>();
        when(offsetsRequestManager.updateFetchPositionsAndAwaitValidation(anyLong(), any())).thenAnswer(invocation -> {
            positionPermission.set(invocation.getArgument(1));
            return positions;
        });
        when(fetchRequestManager.createFetchRequestsWithReconnect(anyLong(), any())).thenAnswer(invocation -> {
            fetchPermission.set(invocation.getArgument(1));
            return fetch;
        });
        AsyncPollEvent event = new AsyncPollEvent(5000, 0);
        processor.process(event);
        if (stage > 0)
            positions.complete(null);
        if (stage == 2)
            processFetchContinuations(1);

        event.requestCancellation();
        assertFalse(positionPermission.get().getAsBoolean());
        if (stage == 0)
            positions.complete(null);
        if (stage == 1)
            processFetchContinuations(1);
        if (stage == 2) {
            assertFalse(fetchPermission.get().getAsBoolean());
            fetch.completeExceptionally(new KafkaException("late abandoned fetch failure"));
        } else {
            verify(fetchRequestManager, never()).createFetchRequestsWithReconnect(anyLong(), any());
        }
        assertEquals(0, fetchContinuations.size());
        assertFalse(event.isComplete());
        doAnswer(invocation -> {
            assertTrue(event.isComplete(), "terminal result must precede retained-owner notification");
            return null;
        }).when(offsetsRequestManager).onPositionStateChanged();

        processor.process(new CancelAsyncPollEvent(event));

        assertTrue(event.isComplete());
        assertTrue(event.error().isEmpty());
        verify(offsetsRequestManager).onPositionStateChanged();
        verify(fetchRequestManager).onPollDemandChanged();
    }

    @Test
    public void testAsyncPollEvent() {
        AsyncPollEvent event = new AsyncPollEvent(12346, 12345);

        setupProcessor(true);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        when(offsetsRequestManager.updateFetchPositionsAndAwaitValidation(eq(event.deadlineMs()), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(fetchRequestManager.createFetchRequestsWithReconnect(anyLong(), any())).thenReturn(CompletableFuture.completedFuture(null));
        processor.process(event);
        assertFalse(event.isComplete());
        verify(fetchRequestManager, never()).createFetchRequestsWithReconnect(anyLong(), any());
        processFetchContinuations(fetchContinuations.size());
        assertTrue(event.isComplete());
        verify(commitRequestManager).updateTimerAndMaybeCommit(event.pollTimeMs());
        verify(membershipManager).onConsumerPoll();
        verify(heartbeatRequestManager).resetPollTimer(event.pollTimeMs());
        verify(offsetsRequestManager).updateFetchPositionsAndAwaitValidation(eq(event.deadlineMs()), any());
        verify(fetchRequestManager).createFetchRequestsWithReconnect(anyLong(), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCompletedAsyncPollDoesNotStartFetchAfterLatePositions(boolean failedPositions) {
        setupProcessor(true);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        CompletableFuture<Void> positions = new CompletableFuture<>();
        when(offsetsRequestManager.updateFetchPositionsAndAwaitValidation(anyLong(), any())).thenReturn(positions);
        when(fetchRequestManager.createFetchRequestsWithReconnect(anyLong(), any())).thenReturn(CompletableFuture.completedFuture(null));
        AsyncPollEvent event = new AsyncPollEvent(12346, 12345);
        processor.process(event);
        KafkaException original = new KafkaException("metadata error");
        event.onMetadataError(original);
        if (failedPositions)
            positions.completeExceptionally(new KafkaException("late position error"));
        else
            positions.complete(null);
        processFetchContinuations(fetchContinuations.size());
        assertTrue(event.isComplete());
        assertEquals(original, event.error().orElseThrow());
        verify(fetchRequestManager, never()).createFetchRequestsWithReconnect(anyLong(), any());
    }

    @Test
    public void testCompletedAsyncPollKeepsErrorAfterLateFetchFailure() {
        setupProcessor(true);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        when(offsetsRequestManager.updateFetchPositionsAndAwaitValidation(anyLong(), any())).thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<Void> fetch = new CompletableFuture<>();
        when(fetchRequestManager.createFetchRequestsWithReconnect(anyLong(), any())).thenReturn(fetch);
        AsyncPollEvent event = new AsyncPollEvent(12346, 12345);
        processor.process(event);
        processFetchContinuations(fetchContinuations.size());
        verify(fetchRequestManager).createFetchRequestsWithReconnect(anyLong(), any());
        KafkaException original = new KafkaException("metadata error");
        event.onMetadataError(original);
        fetch.completeExceptionally(new KafkaException("late fetch error"));
        assertEquals(original, event.error().orElseThrow());
        assertTrue(event.isComplete());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testAsyncPollResultPublishedBeforeCompletionObserver(boolean failed) {
        AsyncPollEvent event = new AsyncPollEvent(12346, 12345);
        KafkaException error = new KafkaException("test");
        CompletableFuture<Void> observed = event.reconciliationCheckFuture().thenRun(() -> {
            assertTrue(event.isComplete());
            assertEquals(failed ? Optional.of(error) : Optional.empty(), event.error());
        });
        if (failed)
            event.completeExceptionally(error);
        else
            event.completeSuccessfully();
        observed.join();
    }

    @Test
    public void testPositionCompletionOnlyQueuesFetchAndRechecksTerminalState() {
        setupProcessor(true);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        CompletableFuture<Void> positions = new CompletableFuture<>();
        when(offsetsRequestManager.updateFetchPositionsAndAwaitValidation(anyLong(), any())).thenReturn(positions);
        AsyncPollEvent event = new AsyncPollEvent(12346, 12345);
        processor.process(event);
        positions.complete(null);
        assertEquals(1, fetchContinuations.size());
        verify(fetchRequestManager, never()).createFetchRequestsWithReconnect(anyLong(), any());
        KafkaException error = new org.apache.kafka.common.errors.TimeoutException("operation ended");
        event.completeExceptionally(error);
        processFetchContinuations(1);
        verify(fetchRequestManager, never()).createFetchRequestsWithReconnect(anyLong(), any());
        assertEquals(error, event.error().orElseThrow());
        assertEquals(0, fetchContinuations.size());
    }

    @Test
    public void testNewFetchContinuationsWaitForNextBatch() {
        setupProcessor(true);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        when(offsetsRequestManager.updateFetchPositionsAndAwaitValidation(anyLong(), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(fetchRequestManager.createFetchRequestsWithReconnect(anyLong(), any())).thenReturn(CompletableFuture.completedFuture(null));
        AsyncPollEvent first = new AsyncPollEvent(12346, 12345);
        AsyncPollEvent second = new AsyncPollEvent(12347, 12345);
        processor.process(first);
        int batch = fetchContinuations.size();
        processor.process(second);
        processFetchContinuations(batch);
        assertTrue(first.isComplete());
        assertFalse(second.isComplete());
        verify(fetchRequestManager).createFetchRequestsWithReconnect(anyLong(), any());
        assertEquals(1, fetchContinuations.size());
        processFetchContinuations(1);
        assertTrue(second.isComplete());
        verify(fetchRequestManager, times(2)).createFetchRequestsWithReconnect(anyLong(), any());
    }

    @Test
    public void testSynchronousFetchFailureReachesOriginalEvent() {
        setupProcessor(true);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        when(offsetsRequestManager.updateFetchPositionsAndAwaitValidation(anyLong(), any())).thenReturn(CompletableFuture.completedFuture(null));
        KafkaException error = new KafkaException("fetch entry failure");
        when(fetchRequestManager.createFetchRequestsWithReconnect(anyLong(), any())).thenThrow(error);
        AsyncPollEvent event = new AsyncPollEvent(12346, 12345);
        processor.process(event);
        processFetchContinuations(1);
        assertTrue(event.isComplete());
        assertEquals(error, event.error().orElseThrow());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCloseRejectsQueuedAndLateFetchContinuations(boolean completeBeforeClose) {
        setupProcessor(true);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        CompletableFuture<Void> positions = new CompletableFuture<>();
        when(offsetsRequestManager.updateFetchPositionsAndAwaitValidation(anyLong(), any())).thenReturn(positions);
        AsyncPollEvent event = new AsyncPollEvent(12346, 12345);
        processor.process(event);
        if (completeBeforeClose)
            positions.complete(null);
        processor.closeFetchContinuations();
        assertTrue(event.isComplete(), "close must not depend on the position callback arriving");
        positions.complete(null);
        assertTrue(event.isComplete());
        assertTrue(event.error().isPresent());
        assertEquals(0, fetchContinuations.size());
        verify(fetchRequestManager, never()).createFetchRequestsWithReconnect(anyLong(), any());
    }

    @Test
    public void testClosedProcessorRejectsNewAsyncPollWithoutStartingPositions() {
        setupProcessor(false);
        processor.closeFetchContinuations();
        AsyncPollEvent event = new AsyncPollEvent(1000, 0);
        processor.process(event);
        assertTrue(event.isComplete());
        assertTrue(event.error().isPresent());
        verify(offsetsRequestManager, never()).updateFetchPositionsAndAwaitValidation(anyLong(), any());
        verify(fetchRequestManager, never()).enqueueFetchContinuation(any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPositionTimeoutDoesNotStartFetch(boolean wrapped) {
        setupProcessor(false);
        Throwable timeout = new org.apache.kafka.common.errors.TimeoutException("position deadline expired");
        if (wrapped)
            timeout = new java.util.concurrent.CompletionException(timeout);
        when(offsetsRequestManager.updateFetchPositionsAndAwaitValidation(anyLong(), any()))
                .thenReturn(CompletableFuture.failedFuture(timeout));
        AsyncPollEvent event = new AsyncPollEvent(1000, 0);
        processor.process(event);
        processFetchContinuations(fetchContinuations.size());
        assertTrue(event.isComplete());
        assertTrue(event.error().isEmpty());
        verify(fetchRequestManager, never()).createFetchRequestsWithReconnect(anyLong(), any());
    }

    @Test
    public void testSynchronousPositionFailureTerminatesOriginalEvent() {
        setupProcessor(false);
        KafkaException error = new KafkaException("position entry failure");
        when(offsetsRequestManager.updateFetchPositionsAndAwaitValidation(anyLong(), any())).thenThrow(error);
        AsyncPollEvent event = new AsyncPollEvent(1000, 0);
        processor.process(event);
        assertTrue(event.isComplete());
        assertEquals(error, event.error().orElseThrow());
        processor.closeFetchContinuations();
        assertEquals(error, event.error().orElseThrow());
        verify(fetchRequestManager, never()).enqueueFetchContinuation(any());
    }

    @Test
    public void testSharePollEventCallsShareManagers() {
        SharePollEvent event = new SharePollEvent(12346, 12345);

        setupShareProcessor();
        when(shareHeartbeatRequestManager.membershipManager()).thenReturn(shareMembershipManager);
        processor.process(event);
        assertTrue(event.isComplete());
        verify(shareMembershipManager).maybeReconcile(true);
        verify(shareMembershipManager).onConsumerPoll();
        verify(shareHeartbeatRequestManager).resetPollTimer(event.pollTimeMs());
    }

    @Test
    public void testTopicSubscriptionChangeEvent() {
        Set<String> topics = Set.of("topic1", "topic2");
        TopicSubscriptionChangeEvent event = new TopicSubscriptionChangeEvent(topics, 12345);

        setupProcessor(true);
        when(subscriptionState.subscribe(topics)).thenReturn(true);
        when(metadata.requestUpdateForNewTopics()).thenReturn(1);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        processor.process(event);

        verify(subscriptionState).subscribe(eq(topics));
        verify(metadata).requestUpdateForNewTopics();
        assertEquals(1, processor.metadataVersionSnapshot());
        verify(membershipManager).onSubscriptionUpdated();
        // verify member state doesn't transition to JOINING.
        verify(membershipManager, never()).onConsumerPoll();
        assertDoesNotThrow(() -> event.future().get());
    }

    @Test
    public void testFetchCommittedOffsetsEvent() {
        TopicPartition tp0 = new TopicPartition("topic", 0);
        TopicPartition tp1 = new TopicPartition("topic", 1);
        TopicPartition tp2 = new TopicPartition("topic", 2);
        Set<TopicPartition> partitions = Set.of(tp0, tp1, tp2);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = Map.of(
            tp0, new OffsetAndMetadata(10L, Optional.of(2), ""),
            tp1, new OffsetAndMetadata(15L, Optional.empty(), ""),
            tp2, new OffsetAndMetadata(20L, Optional.of(3), "")
        );
        FetchCommittedOffsetsEvent event = new FetchCommittedOffsetsEvent(partitions, 12345);

        setupProcessor(true);
        CommitRequestManager.OffsetFetchResult fetchResult = new CommitRequestManager.OffsetFetchResult(
            topicPartitionOffsets, Collections.emptyMap());
        when(commitRequestManager.fetchOffsets(partitions, 12345)).thenReturn(CompletableFuture.completedFuture(fetchResult));
        processor.process(event);

        verify(commitRequestManager).fetchOffsets(partitions, 12345);
        assertEquals(topicPartitionOffsets, assertDoesNotThrow(() -> event.future().get()));
    }

    @Test
    public void testTopicSubscriptionChangeEventWithIllegalSubscriptionState() {
        subscriptionState = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.EARLIEST);
        TopicSubscriptionChangeEvent event = new TopicSubscriptionChangeEvent(
            Set.of("topic1", "topic2"), 12345);

        subscriptionState.subscribe(Pattern.compile("topic.*"));
        setupProcessor(true);
        when(metadata.requestUpdateForNewTopics()).thenReturn(1);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        processor.process(event);

        ExecutionException e = assertThrows(ExecutionException.class, () -> event.future().get());
        assertInstanceOf(IllegalStateException.class, e.getCause());
        assertEquals("Subscription to topics, partitions and pattern are mutually exclusive", e.getCause().getMessage());
    }

    @Test
    public void testTopicPatternSubscriptionChangeEvent() {
        Pattern pattern = Pattern.compile("topic.*");
        Set<String> topics = Set.of("topic.1", "topic.2");
        TopicPatternSubscriptionChangeEvent event = new TopicPatternSubscriptionChangeEvent(pattern, 12345);

        setupProcessor(true);

        Cluster cluster = mock(Cluster.class);
        when(metadata.fetch()).thenReturn(cluster);
        when(cluster.topics()).thenReturn(topics);
        when(subscriptionState.matchesSubscribedPattern("topic.1")).thenReturn(true);
        when(subscriptionState.matchesSubscribedPattern("topic.2")).thenReturn(true);
        when(subscriptionState.subscribeFromPattern(topics)).thenReturn(true);
        when(metadata.requestUpdateForNewTopics()).thenReturn(1);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        processor.process(event);

        verify(subscriptionState).subscribe(eq(pattern));
        verify(subscriptionState).subscribeFromPattern(eq(topics));
        verify(metadata, times(2)).requestUpdateForNewTopics();
        assertEquals(1, processor.metadataVersionSnapshot());
        verify(membershipManager).onSubscriptionUpdated();
        // verify member state doesn't transition to JOINING.
        verify(membershipManager, never()).onConsumerPoll();
        assertDoesNotThrow(() -> event.future().get());
    }

    @Test
    public void testTopicPatternSubscriptionTriggersJoin() {
        TopicPatternSubscriptionChangeEvent event = new TopicPatternSubscriptionChangeEvent(
            Pattern.compile("topic.*"), 12345);
        setupProcessor(true);
        Cluster cluster = mock(Cluster.class);
        when(metadata.fetch()).thenReturn(cluster);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);

        // Initial subscription where no topics match the pattern. Membership manager
        // should still be notified so it joins if not in the group (with empty subscription).
        when(subscriptionState.subscribeFromPattern(any())).thenReturn(false);
        processor.process(event);
        verify(membershipManager).onSubscriptionUpdated();

        clearInvocations(membershipManager);

        // Subscription where some topics match so subscription is updated. Membership manager
        // should be notified so it joins if not in the group.
        when(subscriptionState.subscribeFromPattern(any())).thenReturn(true);
        processor.process(event);
        verify(membershipManager).onSubscriptionUpdated();
    }

    @Test
    public void testTopicPatternSubscriptionChangeEventWithIllegalSubscriptionState() {
        subscriptionState = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.EARLIEST);
        TopicPatternSubscriptionChangeEvent event = new TopicPatternSubscriptionChangeEvent(
            Pattern.compile("topic.*"), 12345);

        setupProcessor(true);

        subscriptionState.subscribe(Set.of("topic.1", "topic.2"));
        processor.process(event);

        ExecutionException e = assertThrows(ExecutionException.class, () -> event.future().get());
        assertInstanceOf(IllegalStateException.class, e.getCause());
        assertEquals("Subscription to topics, partitions and pattern are mutually exclusive", e.getCause().getMessage());
    }

    @Test
    public void testUpdatePatternSubscriptionEventOnlyTakesEffectWhenMetadataHasNewVersion() {
        UpdatePatternSubscriptionEvent event1 = new UpdatePatternSubscriptionEvent(12345);

        setupProcessor(true);
        when(subscriptionState.hasPatternSubscription()).thenReturn(true);
        when(metadata.updateVersion()).thenReturn(0);

        processor.process(event1);
        assertDoesNotThrow(() -> event1.future().get());

        Cluster cluster = mock(Cluster.class);
        Set<String> topics = Set.of("topic.1", "topic.2");
        when(metadata.updateVersion()).thenReturn(1);
        when(subscriptionState.hasPatternSubscription()).thenReturn(true);
        when(metadata.fetch()).thenReturn(cluster);
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        when(cluster.topics()).thenReturn(topics);
        when(subscriptionState.matchesSubscribedPattern("topic.1")).thenReturn(true);
        when(subscriptionState.matchesSubscribedPattern("topic.2")).thenReturn(true);
        when(subscriptionState.subscribeFromPattern(topics)).thenReturn(true);
        when(metadata.requestUpdateForNewTopics()).thenReturn(1);

        UpdatePatternSubscriptionEvent event2 = new UpdatePatternSubscriptionEvent(12345);
        processor.process(event2);
        verify(metadata).requestUpdateForNewTopics();
        verify(subscriptionState).subscribeFromPattern(topics);
        assertEquals(1, processor.metadataVersionSnapshot());
        verify(membershipManager).onSubscriptionUpdated();
        assertDoesNotThrow(() -> event2.future().get());
    }

    @Test
    public void testR2JPatternSubscriptionEventSuccess() {
        SubscriptionPattern pattern = new SubscriptionPattern("t*");
        TopicRe2JPatternSubscriptionChangeEvent event =
            new TopicRe2JPatternSubscriptionChangeEvent(pattern, 12345);

        setupProcessor(true);
        processor.process(event);

        verify(subscriptionState).subscribe(eq(pattern));
        verify(subscriptionState, never()).subscribeFromPattern(any());
        verify(membershipManager).onSubscriptionUpdated();
        assertDoesNotThrow(() -> event.future().get());
    }

    @Test
    public void testR2JPatternSubscriptionEventFailureWithMixedSubscriptionType() {
        SubscriptionPattern pattern = new SubscriptionPattern("t*");
        TopicRe2JPatternSubscriptionChangeEvent event =
            new TopicRe2JPatternSubscriptionChangeEvent(pattern, 12345);
        Exception mixedSubscriptionError = new IllegalStateException("Subscription to topics, partitions and " +
            "pattern are mutually exclusive");
        doThrow(mixedSubscriptionError).when(subscriptionState).subscribe(eq(pattern));

        setupProcessor(true);
        processor.process(event);

        verify(subscriptionState).subscribe(eq(pattern));
        Exception thrown = assertFutureThrows(IllegalStateException.class, event.future());
        assertEquals(mixedSubscriptionError, thrown);
    }

    @Test
    public void testSyncCommitEventWithEmptyOffsets() {
        Map<TopicPartition, OffsetAndMetadata> allConsumed =
            Map.of(new TopicPartition("topic", 0), new OffsetAndMetadata(10, Optional.of(1), ""));
        SyncCommitEvent event = new SyncCommitEvent(Optional.empty(), 12345);
        setupProcessor(true);
        doReturn(allConsumed).when(subscriptionState).allConsumed();
        doReturn(CompletableFuture.completedFuture(allConsumed)).when(commitRequestManager).commitSync(allConsumed, 12345);

        processor.process(event);
        verify(commitRequestManager).commitSync(allConsumed, 12345);
        assertTrue(event.offsetsReady.isDone());
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = assertDoesNotThrow(() -> event.future().get());
        assertEquals(allConsumed, committedOffsets);
    }

    @Test
    public void testSyncCommitEvent() {
        Map<TopicPartition, OffsetAndMetadata> offsets =
            Map.of(new TopicPartition("topic", 0), new OffsetAndMetadata(10, Optional.of(1), ""));
        SyncCommitEvent event = new SyncCommitEvent(Optional.of(offsets), 12345);
        setupProcessor(true);
        doReturn(CompletableFuture.completedFuture(offsets)).when(commitRequestManager).commitSync(offsets, 12345);

        processor.process(event);
        verify(commitRequestManager).commitSync(offsets, 12345);
        assertTrue(event.offsetsReady.isDone());
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = assertDoesNotThrow(() -> event.future().get());
        assertEquals(offsets, committedOffsets);
    }

    @Test
    public void testSyncCommitEventWithoutCommitRequestManager() {
        SyncCommitEvent event = new SyncCommitEvent(Optional.empty(), 12345);

        setupProcessor(false);
        processor.process(event);
        assertFutureThrows(KafkaException.class, event.future());
    }

    @Test
    public void testSyncCommitEventWithException() {
        SyncCommitEvent event = new SyncCommitEvent(Optional.empty(), 12345);

        setupProcessor(true);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = new CompletableFuture<>();
        future.completeExceptionally(new IllegalStateException());
        doReturn(future).when(commitRequestManager).commitSync(any(), anyLong());
        processor.process(event);

        verify(commitRequestManager).commitSync(Collections.emptyMap(), 12345);
        assertTrue(event.offsetsReady.isDone());
        assertFutureThrows(IllegalStateException.class, event.future());
    }

    @Test
    public void testAsyncCommitEventWithEmptyOffsets() {
        Map<TopicPartition, OffsetAndMetadata> allConsumed =
            Map.of(new TopicPartition("topic", 0), new OffsetAndMetadata(10, Optional.of(1), ""));
        AsyncCommitEvent event = new AsyncCommitEvent(Optional.empty());
        setupProcessor(true);
        doReturn(CompletableFuture.completedFuture(allConsumed)).when(commitRequestManager).commitAsync(allConsumed);
        doReturn(allConsumed).when(subscriptionState).allConsumed();

        processor.process(event);
        verify(commitRequestManager).commitAsync(allConsumed);
        assertTrue(event.offsetsReady.isDone());
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = assertDoesNotThrow(() -> event.future().get());
        assertEquals(allConsumed, committedOffsets);
    }

    @Test
    public void testAsyncCommitEvent() {
        Map<TopicPartition, OffsetAndMetadata> offsets =
            Map.of(new TopicPartition("topic", 0), new OffsetAndMetadata(10, Optional.of(1), ""));
        AsyncCommitEvent event = new AsyncCommitEvent(Optional.of(offsets));
        setupProcessor(true);
        doReturn(CompletableFuture.completedFuture(offsets)).when(commitRequestManager).commitAsync(offsets);

        processor.process(event);
        verify(commitRequestManager).commitAsync(offsets);
        assertTrue(event.offsetsReady.isDone());
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = assertDoesNotThrow(() -> event.future().get());
        assertEquals(offsets, committedOffsets);
    }

    @Test
    public void testAsyncCommitEventWithoutCommitRequestManager() {
        AsyncCommitEvent event = new AsyncCommitEvent(Optional.empty());

        setupProcessor(false);
        processor.process(event);
        assertFutureThrows(KafkaException.class, event.future());
    }

    @Test
    public void testAsyncCommitEventWithException() {
        AsyncCommitEvent event = new AsyncCommitEvent(Optional.empty());

        setupProcessor(true);
        doReturn(Collections.emptyMap()).when(subscriptionState).allConsumed();
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = new CompletableFuture<>();
        future.completeExceptionally(new IllegalStateException());
        doReturn(future).when(commitRequestManager).commitAsync(any());
        processor.process(event);

        verify(commitRequestManager).commitAsync(Collections.emptyMap());
        assertTrue(event.offsetsReady.isDone());
        assertFutureThrows(IllegalStateException.class, event.future());
    }

    @Test
    public void testStreamsOnTasksRevokedCallbackCompletedEvent() {
        setupStreamProcessor(true);
        StreamsOnTasksRevokedCallbackCompletedEvent event =
            new StreamsOnTasksRevokedCallbackCompletedEvent(new CompletableFuture<>(), Optional.empty());
        processor.process(event);
        verify(streamsMembershipManager).onTasksRevokedCallbackCompleted(event);
    }

    @Test
    public void testStreamsOnTasksRevokedCallbackCompletedEventWithoutStreamsMembershipManager() {
        setupStreamProcessor(false);
        StreamsOnTasksRevokedCallbackCompletedEvent event =
            new StreamsOnTasksRevokedCallbackCompletedEvent(new CompletableFuture<>(), Optional.empty());
        try (final LogCaptureAppender logAppender = LogCaptureAppender.createAndRegister()) {
            logAppender.setClassLogger(ApplicationEventProcessor.class, Level.WARN);
            processor.process(event);
            assertTrue(logAppender.getMessages().stream().anyMatch(e ->
                e.contains("An internal error occurred; the Streams membership manager was not present, so the notification " +
                    "of the onTasksRevoked callback execution could not be sent")));
            verify(streamsMembershipManager, never()).onTasksRevokedCallbackCompleted(event);
        }
    }

    @Test
    public void testStreamsOnTasksAssignedCallbackCompletedEvent() {
        setupStreamProcessor(true);
        StreamsOnTasksAssignedCallbackCompletedEvent event =
            new StreamsOnTasksAssignedCallbackCompletedEvent(new CompletableFuture<>(), Optional.empty());
        processor.process(event);
        verify(streamsMembershipManager).onTasksAssignedCallbackCompleted(event);
    }

    @Test
    public void testStreamsOnTasksAssignedCallbackCompletedEventWithoutStreamsMembershipManager() {
        setupStreamProcessor(false);
        StreamsOnTasksAssignedCallbackCompletedEvent event =
            new StreamsOnTasksAssignedCallbackCompletedEvent(new CompletableFuture<>(), Optional.empty());
        try (final LogCaptureAppender logAppender = LogCaptureAppender.createAndRegister()) {
            logAppender.setClassLogger(ApplicationEventProcessor.class, Level.WARN);
            processor.process(event);
            assertTrue(logAppender.getMessages().stream().anyMatch(e ->
                e.contains("An internal error occurred; the Streams membership manager was not present, so the notification " +
                    "of the onTasksAssigned callback execution could not be sent")));
            verify(streamsMembershipManager, never()).onTasksAssignedCallbackCompleted(event);
        }
    }

    @Test
    public void testStreamsOnAllTasksLostCallbackCompletedEvent() {
        setupStreamProcessor(true);
        StreamsOnAllTasksLostCallbackCompletedEvent event =
            new StreamsOnAllTasksLostCallbackCompletedEvent(new CompletableFuture<>(), Optional.empty());
        processor.process(event);
        verify(streamsMembershipManager).onAllTasksLostCallbackCompleted(event);
    }

    @Test
    public void testStreamsOnAllTasksLostCallbackCompletedEventWithoutStreamsMembershipManager() {
        setupStreamProcessor(false);
        StreamsOnAllTasksLostCallbackCompletedEvent event =
            new StreamsOnAllTasksLostCallbackCompletedEvent(new CompletableFuture<>(), Optional.empty());
        try (final LogCaptureAppender logAppender = LogCaptureAppender.createAndRegister()) {
            logAppender.setClassLogger(ApplicationEventProcessor.class, Level.WARN);
            processor.process(event);
            assertTrue(logAppender.getMessages().stream().anyMatch(e ->
                e.contains("An internal error occurred; the Streams membership manager was not present, so the notification " +
                    "of the onAllTasksLost callback execution could not be sent")));
            verify(streamsMembershipManager, never()).onAllTasksLostCallbackCompleted(event);
        }
    }

    @Test
    public void testUpdatePatternSubscriptionInvokedWhenMetadataUpdated() {
        when(subscriptionState.hasPatternSubscription()).thenReturn(true);
        when(subscriptionState.matchesSubscribedPattern(any(String.class))).thenReturn(true);
        when(metadata.updateVersion()).thenReturn(1, 2);
        testUpdatePatternSubscription(times(1));
    }

    @Test
    public void testUpdatePatternSubscriptionNotInvokedWhenNotUsingPatternSubscription() {
        when(subscriptionState.hasPatternSubscription()).thenReturn(false);
        when(metadata.updateVersion()).thenReturn(1, 2);
        testUpdatePatternSubscription(never());
    }

    @Test
    public void testUpdatePatternSubscriptionNotInvokedWhenMetadataNotUpdated() {
        when(subscriptionState.hasPatternSubscription()).thenReturn(true);
        when(subscriptionState.matchesSubscribedPattern(any(String.class))).thenReturn(true);
        when(metadata.updateVersion()).thenReturn(1, 1);
        testUpdatePatternSubscription(never());
    }

    private void testUpdatePatternSubscription(VerificationMode verificationMode) {
        String topic = "test-topic";
        Cluster cluster = mock(Cluster.class);

        when(metadata.fetch()).thenReturn(cluster);
        when(cluster.topics()).thenReturn(Set.of(topic));

        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);
        when(offsetsRequestManager.updateFetchPositionsAndAwaitValidation(anyLong(), any())).thenReturn(CompletableFuture.completedFuture(null));

        setupProcessor(true);
        processor.process(new AsyncPollEvent(110, 100));
        verify(subscriptionState, verificationMode).matchesSubscribedPattern(topic);
        verify(membershipManager, verificationMode).onSubscriptionUpdated();
    }

    @Test
    public void testRefreshCommittedOffsetsShouldNotResetIfFailedWithTimeout() {
        setupProcessor(true);
        testUpdateFetchPositionsWithFetchCommittedOffsetsTimeout();
    }

    @Test
    public void testRefreshCommittedOffsetsNotCalledIfNoGroupId() {
        // Create consumer without group id so committed offsets are not used for updating positions
        setupProcessor(false);
        testUpdateFetchPositionsWithFetchCommittedOffsetsTimeout();
    }

    private void testUpdateFetchPositionsWithFetchCommittedOffsetsTimeout() {
        when(offsetsRequestManager.updateFetchPositionsAndAwaitValidation(anyLong(), any())).thenReturn(
            CompletableFuture.failedFuture(new Throwable("Intentional failure"))
        );
        when(heartbeatRequestManager.membershipManager()).thenReturn(membershipManager);

        // Verify that the poll completes even when the update fetch positions throws an error.
        AsyncPollEvent event = new AsyncPollEvent(110, 100);
        processor.process(event);
        verify(offsetsRequestManager).updateFetchPositionsAndAwaitValidation(anyLong(), any());
        assertFalse(event.isComplete());
        processFetchContinuations(fetchContinuations.size());
        assertTrue(event.isComplete());
        assertFalse(event.error().isEmpty());
    }

    private List<NetworkClientDelegate.UnsentRequest> mockCommitResults() {
        return Collections.singletonList(mock(NetworkClientDelegate.UnsentRequest.class));
    }
}
