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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.ApplyAssignmentEvent;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.AsyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.AsyncPollEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionsAssignedEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionsRemovedEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.internals.AbstractMembershipManager.TOPIC_PARTITION_COMPARATOR;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_LOST;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pins the KIP-1371 termination contract (C4) and the callback half of the thread-ownership contract (C5)
 * for the async consumer. See {@code experiments/kip-1371-fable/termination-table.md} for the full
 * "pending work x terminator" table these tests back.
 *
 * <p>Each test uses only the seams that already exist at HEAD (request-manager {@code poll()}, the
 * {@link CompletableEventReaper}, a mocked {@link ApplicationEventHandler}), so they run without a network
 * thread and without wall-clock waits.
 *
 * <p>Related existing coverage, referenced rather than duplicated:
 * <ul>
 *   <li>{@code CommitRequestManagerTest.testPollWithClosingAndPendingRequests}: unsent {@code commitAsync}
 *       + coordinator unknown + closing gives {@link CommitFailedException}.</li>
 *   <li>{@code AsyncKafkaConsumerTest.testCloseAwaitPendingAsyncCommitIncomplete/Complete}: close waits for
 *       {@code lastPendingAsyncCommit} within the close timer and runs the commit callback.</li>
 *   <li>{@code ConsumerMembershipManagerTest.testListenerCallbacksThrowsErrorOnPartitions*}: the
 *       KAFKA-18160 fix on the network-thread side (a callback error still completes the reconciliation
 *       future).</li>
 * </ul>
 */
@SuppressWarnings({"unchecked", "ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class TerminationContractTest {

    private static final String GROUP_ID = "group-id";
    private static final String TOPIC = "topic";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);
    private static final Node NODE = new Node(0, "host1", 9092);
    private static final long RETRY_BACKOFF_MS = 100;
    private static final long RETRY_BACKOFF_MAX_MS = 1000;
    private static final int REQUEST_TIMEOUT_MS = 30_000;
    private static final int DEFAULT_API_TIMEOUT_MS = 60_000;

    private final LogContext logContext = new LogContext();
    private final MockTime time = new MockTime(0);
    private final Metrics metrics = new Metrics();
    private final ConsumerMetadata metadata = mock(ConsumerMetadata.class);
    private final CoordinatorRequestManager coordinatorRequestManager = mock(CoordinatorRequestManager.class);
    private final ApplicationEventHandler applicationEventHandler = mock(ApplicationEventHandler.class);
    private final FetchCollector<String, String> fetchCollector = mock(FetchCollector.class);
    private final LinkedBlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
    private final CompletableEventReaper backgroundEventReaper = new CompletableEventReaper(logContext);

    private AsyncKafkaConsumer<String, String> consumer;

    @AfterEach
    public void tearDown() {
        // A callback that threw InterruptException leaves the interrupt flag set; never leak it to the next test.
        Thread.interrupted();
        backgroundEventQueue.clear();
        if (consumer != null) {
            try {
                consumer.close(CloseOptions.timeout(Duration.ZERO));
            } catch (Exception swallow) {
                // best effort: the mocked ApplicationEventHandler does not answer close events
            }
        }
        consumer = null;
        metrics.close();
    }

    // ------------------------------------------------------------------------------------------------
    // (a) unsent commit at close
    // ------------------------------------------------------------------------------------------------

    /**
     * Termination row "unsent commitSync, coordinator unknown": the first {@code poll()} after
     * {@code signalClose()} (the {@code CommitOnCloseEvent} of close step 3) fails the request with
     * {@link CommitFailedException}. No timer is involved: it is bounded by one network-thread iteration.
     * The {@code commitAsync} variant is {@code CommitRequestManagerTest.testPollWithClosingAndPendingRequests}.
     */
    @Test
    public void testUnsentCommitSyncWithCoordinatorUnknownFailsWithCommitFailedExceptionAtClose() {
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        CommitRequestManager commitRequestManager = newCommitRequestManager(subscriptions);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());

        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commit = commitRequestManager.commitSync(
            Map.of(TP, new OffsetAndMetadata(10)), time.milliseconds() + DEFAULT_API_TIMEOUT_MS);
        assertEquals(NetworkClientDelegate.PollResult.EMPTY, commitRequestManager.poll(time.milliseconds()));
        assertFalse(commit.isDone(), "an unsent commitSync must wait for the coordinator while not closing");

        commitRequestManager.signalClose();
        assertEquals(NetworkClientDelegate.PollResult.EMPTY, commitRequestManager.poll(time.milliseconds()));

        assertTrue(commit.isCompletedExceptionally());
        Throwable cause = assertThrows(ExecutionException.class, commit::get).getCause();
        assertInstanceOf(CommitFailedException.class, cause);
    }

    /**
     * Termination row "unsent commit, coordinator known": after {@code signalClose()} the next {@code poll()}
     * drains every unsent commit regardless of its backoff, so it reaches the wire inside the close timer.
     */
    @Test
    public void testUnsentCommitWithCoordinatorKnownIsDrainedOnFirstPollAfterSignalClose() {
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        CommitRequestManager commitRequestManager = newCommitRequestManager(subscriptions);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(NODE));

        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commit = commitRequestManager.commitSync(
            Map.of(TP, new OffsetAndMetadata(10)), time.milliseconds() + DEFAULT_API_TIMEOUT_MS);
        commitRequestManager.signalClose();

        NetworkClientDelegate.PollResult result = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size());
        assertFalse(commit.isDone(), "the request is now in flight; only the response or the close timer ends it");
        assertEquals(NetworkClientDelegate.PollResult.EMPTY, commitRequestManager.poll(time.milliseconds()),
            "the drained commit must not be re-sent on the next poll");
    }

    /**
     * Characterisation of the AMBIGUOUS termination row "unsent OffsetFetch at close". After
     * {@code signalClose()}, {@code CommitRequestManager.poll()} never sends an unsent OffsetFetch and never
     * completes its future: with a coordinator the closing branch only drains commits, without one
     * {@code drainPendingCommits()} calls {@code clearAll()}, which also drops the unsent fetches silently.
     * The only terminator is the application-side {@link CompletableEventReaper} on the wrapping event.
     * This test pins the current behaviour so a redesign that adds a manager-side terminator flips it on purpose.
     */
    @Test
    public void testUnsentOffsetFetchIsDroppedWithoutCompletingItsFutureAtClose() {
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);

        // Coordinator known: the closing branch returns only commits, so the fetch is neither sent nor failed.
        CommitRequestManager withCoordinator = newCommitRequestManager(subscriptions);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(NODE));
        CompletableFuture<CommitRequestManager.OffsetFetchResult> fetch1 =
            withCoordinator.fetchOffsets(Set.of(TP), time.milliseconds() + DEFAULT_API_TIMEOUT_MS);
        withCoordinator.signalClose();
        assertEquals(NetworkClientDelegate.PollResult.EMPTY, withCoordinator.poll(time.milliseconds()));
        assertFalse(fetch1.isDone());
        assertEquals(1, withCoordinator.pendingRequests.unsentOffsetFetches.size());

        // Coordinator unknown: drainPendingCommits() -> clearAll() drops the fetch without completing it.
        CommitRequestManager withoutCoordinator = newCommitRequestManager(subscriptions);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());
        CompletableFuture<CommitRequestManager.OffsetFetchResult> fetch2 =
            withoutCoordinator.fetchOffsets(Set.of(TP), time.milliseconds() + DEFAULT_API_TIMEOUT_MS);
        withoutCoordinator.signalClose();
        assertEquals(NetworkClientDelegate.PollResult.EMPTY, withoutCoordinator.poll(time.milliseconds()));
        assertFalse(fetch2.isDone());
        assertTrue(withoutCoordinator.pendingRequests.unsentOffsetFetches.isEmpty());
    }

    // ------------------------------------------------------------------------------------------------
    // (b) late responses after the assignment was released by close
    // ------------------------------------------------------------------------------------------------

    /**
     * Close step 6 ({@code leaveGroup}) runs {@code subscriptions.unsubscribe()} before the network thread is
     * stopped, so a ListOffsets response that arrives during the close window finds no assigned partition.
     * {@code SubscriptionState.maybeSeekUnvalidated} must skip it without throwing and without writing a position.
     */
    @Test
    public void testLateListOffsetsResponseAfterAssignmentReleasedDoesNotThrowOrWritePosition() {
        SubscriptionState subscriptions = spy(new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST));
        OffsetsRequestManager offsetsRequestManager = newOffsetsRequestManager(subscriptions, null);
        mockLeader();

        subscriptions.assignFromUser(Set.of(TP));
        subscriptions.requestOffsetReset(TP);
        CompletableFuture<Void> reset = offsetsRequestManager.resetPositionsIfNeeded();
        NetworkClientDelegate.PollResult poll = offsetsRequestManager.poll(time.milliseconds());
        assertEquals(1, poll.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest request = poll.unsentRequests.get(0);

        // close: leaveGroup releases the assignment while the request is still in flight
        subscriptions.unsubscribe();
        clearInvocations(subscriptions);

        assertDoesNotThrow(() -> listOffsetsResponse(request, 5L).onComplete());

        assertTrue(reset.isDone());
        assertFalse(reset.isCompletedExceptionally());
        verify(subscriptions, never()).seekUnvalidated(any(TopicPartition.class), any(SubscriptionState.FetchPosition.class));
        verify(subscriptions, never()).position(any(TopicPartition.class), any(SubscriptionState.FetchPosition.class));
        assertTrue(subscriptions.assignedPartitions().isEmpty());
    }

    /**
     * Characterisation of the AMBIGUOUS half of the same row: with a manual assignment (no group, so close never
     * calls {@code unsubscribe()}), a late ListOffsets response still writes the position. Nothing reads it after
     * close, but there is no guard on the manager side.
     */
    @Test
    public void testLateListOffsetsResponseWithManualAssignmentStillWritesPosition() {
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        OffsetsRequestManager offsetsRequestManager = newOffsetsRequestManager(subscriptions, null);
        mockLeader();

        subscriptions.assignFromUser(Set.of(TP));
        subscriptions.requestOffsetReset(TP);
        offsetsRequestManager.resetPositionsIfNeeded();
        NetworkClientDelegate.UnsentRequest request = offsetsRequestManager.poll(time.milliseconds()).unsentRequests.get(0);

        assertDoesNotThrow(() -> listOffsetsResponse(request, 5L).onComplete());

        assertTrue(subscriptions.hasValidPosition(TP));
        assertEquals(5L, subscriptions.position(TP).offset);
    }

    /**
     * Same as {@link #testLateListOffsetsResponseAfterAssignmentReleasedDoesNotThrowOrWritePosition()} for the
     * OffsetFetch that initialises positions from committed offsets: {@code refreshOffsets} filters by
     * {@code initializingPartitions()}, which is empty once the assignment is released.
     */
    @Test
    public void testLateOffsetFetchResponseAfterAssignmentReleasedDoesNotThrowOrWritePosition() {
        SubscriptionState subscriptions = spy(new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST));
        CommitRequestManager commitRequestManager = newCommitRequestManager(subscriptions);
        OffsetsRequestManager offsetsRequestManager = newOffsetsRequestManager(subscriptions, commitRequestManager);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(NODE));
        mockLeader();

        subscriptions.assignFromUser(Set.of(TP));
        CompletableFuture<Void> update = offsetsRequestManager.updateFetchPositions(time.milliseconds() + DEFAULT_API_TIMEOUT_MS);
        NetworkClientDelegate.PollResult poll = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, poll.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest request = poll.unsentRequests.get(0);
        assertFalse(update.isDone());

        // close: leaveGroup releases the assignment while the OffsetFetch is still in flight
        subscriptions.unsubscribe();
        clearInvocations(subscriptions);

        assertDoesNotThrow(() -> offsetFetchResponse(request, 100L).onComplete());

        assertTrue(update.isDone());
        assertFalse(update.isCompletedExceptionally());
        verify(subscriptions, never()).seekUnvalidated(any(TopicPartition.class), any(SubscriptionState.FetchPosition.class));
        verify(subscriptions, never()).position(any(TopicPartition.class), any(SubscriptionState.FetchPosition.class));
        assertTrue(subscriptions.assignedPartitions().isEmpty());
    }

    // ------------------------------------------------------------------------------------------------
    // (c) rebalance callback events still pending at close
    // ------------------------------------------------------------------------------------------------

    /**
     * Close step 9 ({@code backgroundEventReaper.reap(backgroundEventQueue)}) fails every rebalance callback
     * event the application never got to. The callback is not invoked and the application thread never enters
     * {@code addAndGet(ApplyAssignmentEvent)}, so close cannot block on it.
     */
    @Test
    public void testRebalanceCallbackEventsLeftInBackgroundQueueAreFailedAtClose() {
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        ThrowingListener listener = new ThrowingListener(null);
        subscriptions.subscribe(Set.of(TOPIC));
        subscriptions.setRebalanceListener(listener, mock(Consumer.class));
        consumer = newConsumer(subscriptions, newInvoker(subscriptions));

        PartitionsAssignedEvent assigned = new PartitionsAssignedEvent(Set.of(TP), sortedSet(TP));
        PartitionsRemovedEvent revoked = new PartitionsRemovedEvent(ON_PARTITIONS_REVOKED, sortedSet(TP));
        backgroundEventQueue.add(assigned);
        backgroundEventQueue.add(revoked);

        assertDoesNotThrow(() -> consumer.close(CloseOptions.timeout(Duration.ZERO)));
        consumer = null;

        assertTimedOutBeforeClose(assigned.future());
        assertTimedOutBeforeClose(revoked.future());
        assertEquals(0, listener.invocations, "a reaped callback event must not run the user callback");
        verify(applicationEventHandler, never()).addAndGet(isA(ApplyAssignmentEvent.class));
    }

    /**
     * The network-thread side of the same row: an {@code ApplyAssignmentEvent} the application thread is blocked
     * on in {@code addAndGet} is failed by {@code ConsumerNetworkThread.cleanup()} (close step 8), so the
     * application thread is released with a {@link TimeoutException}.
     */
    @Test
    public void testApplyAssignmentEventInApplicationQueueIsFailedByNetworkThreadCleanup() {
        LinkedBlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
        CompletableEventReaper applicationEventReaper = new CompletableEventReaper(logContext);
        Supplier<ApplicationEventProcessor> processor = () -> mock(ApplicationEventProcessor.class);
        Supplier<NetworkClientDelegate> networkClientDelegate = () -> mock(NetworkClientDelegate.class);
        Supplier<RequestManagers> requestManagers = () -> mock(RequestManagers.class);
        ConsumerNetworkThread thread = new ConsumerNetworkThread(
            logContext, time, applicationEventQueue, applicationEventReaper, processor,
            networkClientDelegate, requestManagers, mock(AsyncConsumerMetrics.class));
        thread.initializeResources();

        ApplyAssignmentEvent applyEvent = new ApplyAssignmentEvent(Set.of(TP), sortedSet(TP));
        applicationEventQueue.add(applyEvent);

        thread.cleanup();

        assertTimedOutBeforeClose(applyEvent.future());
    }

    /**
     * And the application-thread reaction to that release: {@code applyNewAssignment} turns the failure into a
     * {@link ConsumerRebalanceListenerCallbackCompletedEvent} carrying the error (so the network thread, if still
     * alive, completes the reconciliation future) and rethrows to the caller instead of hanging.
     */
    @Test
    public void testApplicationThreadReleasedFromApplyAssignmentReportsErrorAndDoesNotHang() {
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        consumer = newConsumer(subscriptions, newInvoker(subscriptions));
        TimeoutException closed = new TimeoutException("ApplyAssignmentEvent could not be completed before the consumer closed");
        doThrow(closed).when(applicationEventHandler).addAndGet(isA(ApplyAssignmentEvent.class));

        PartitionsAssignedEvent assigned = new PartitionsAssignedEvent(Set.of(TP), sortedSet(TP));
        backgroundEventQueue.add(assigned);

        KafkaException thrown = assertThrows(KafkaException.class, () -> consumer.processBackgroundEvents());
        assertSame(closed, thrown);

        ConsumerRebalanceListenerCallbackCompletedEvent completed = captureCallbackCompletedEvent();
        assertEquals(ON_PARTITIONS_ASSIGNED, completed.methodName());
        assertSame(assigned.future(), completed.future());
        assertTrue(completed.error().isPresent());
        assertSame(closed, completed.error().get());
    }

    // ------------------------------------------------------------------------------------------------
    // (d) WakeupException / InterruptException thrown from user callbacks (KAFKA-18160 for all three kinds)
    // ------------------------------------------------------------------------------------------------

    private static Stream<Arguments> rebalanceCallbackErrors() {
        return Stream.of(
            Arguments.of(ON_PARTITIONS_REVOKED, WakeupException.class),
            Arguments.of(ON_PARTITIONS_REVOKED, InterruptException.class),
            Arguments.of(ON_PARTITIONS_ASSIGNED, WakeupException.class),
            Arguments.of(ON_PARTITIONS_ASSIGNED, InterruptException.class),
            Arguments.of(ON_PARTITIONS_LOST, WakeupException.class),
            Arguments.of(ON_PARTITIONS_LOST, InterruptException.class));
    }

    /**
     * KAFKA-18160 guarantee, application-thread side: {@code ConsumerRebalanceListenerInvoker} rethrows
     * {@link WakeupException}/{@link InterruptException}, and {@code AsyncKafkaConsumer.invokeRebalanceCallbacks}
     * catches them and still produces the {@link ConsumerRebalanceListenerCallbackCompletedEvent} for the
     * original future. Before the fix the event was skipped and the network thread waited forever.
     */
    @ParameterizedTest
    @MethodSource("rebalanceCallbackErrors")
    public void testCallbackWakeupOrInterruptStillProducesCallbackCompletedEvent(
        ConsumerRebalanceListenerMethodName methodName,
        Class<? extends KafkaException> errorClass) {
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        ThrowingListener listener = new ThrowingListener(errorClass);
        subscriptions.subscribe(Set.of(TOPIC));
        subscriptions.setRebalanceListener(listener, mock(Consumer.class));
        subscriptions.assignFromSubscribed(Set.of(TP));
        CompletableFuture<Void> future = new CompletableFuture<>();

        ConsumerRebalanceListenerCallbackCompletedEvent completed = AsyncKafkaConsumer.invokeRebalanceCallbacks(
            newInvoker(subscriptions), methodName, sortedSet(TP), future);

        assertEquals(1, listener.invocations);
        assertEquals(methodName, completed.methodName());
        assertSame(future, completed.future());
        assertTrue(completed.error().isPresent());
        assertInstanceOf(errorClass, completed.error().get());
        assertFalse(future.isDone(), "only the network thread completes the reconciliation future");

        // network-thread side: the completed event ends the wait, exceptionally
        newMembershipManager().consumerRebalanceListenerCallbackCompleted(completed);
        assertTrue(future.isCompletedExceptionally());
    }

    /**
     * The same guarantee through {@code processBackgroundEvents}: the error is reported to the network thread
     * via the application queue first, then propagated to the caller of {@code poll()}.
     */
    @ParameterizedTest
    @MethodSource("rebalanceCallbackErrors")
    public void testCallbackWakeupOrInterruptDuringProcessBackgroundEventsReportsThenPropagates(
        ConsumerRebalanceListenerMethodName methodName,
        Class<? extends KafkaException> errorClass) {
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        ThrowingListener listener = new ThrowingListener(errorClass);
        subscriptions.subscribe(Set.of(TOPIC));
        subscriptions.setRebalanceListener(listener, mock(Consumer.class));
        consumer = newConsumer(subscriptions, newInvoker(subscriptions));
        doAnswer(invocation -> {
            ApplyAssignmentEvent event = invocation.getArgument(0);
            subscriptions.assignFromSubscribedAwaitingCallback(event.assignedPartitions(), event.addedPartitions());
            return null;
        }).when(applicationEventHandler).addAndGet(isA(ApplyAssignmentEvent.class));

        CompletableFuture<Void> future;
        if (methodName == ON_PARTITIONS_ASSIGNED) {
            PartitionsAssignedEvent event = new PartitionsAssignedEvent(Set.of(TP), sortedSet(TP));
            future = event.future();
            backgroundEventQueue.add(event);
        } else {
            PartitionsRemovedEvent event = new PartitionsRemovedEvent(methodName, sortedSet(TP));
            future = event.future();
            backgroundEventQueue.add(event);
        }

        KafkaException thrown = assertThrows(KafkaException.class, () -> consumer.processBackgroundEvents());
        assertInstanceOf(errorClass, thrown);

        ConsumerRebalanceListenerCallbackCompletedEvent completed = captureCallbackCompletedEvent();
        assertEquals(methodName, completed.methodName());
        assertSame(future, completed.future());
        assertTrue(completed.error().isPresent());
        assertSame(thrown, completed.error().get());
        assertEquals(1, listener.invocations);
    }

    private static Stream<Arguments> commitCallbackErrors() {
        return Stream.of(
            Arguments.of(WakeupException.class),
            Arguments.of(InterruptException.class));
    }

    /**
     * {@link OffsetCommitCallback} is the third callback kind. Its contract is different: the network thread
     * completes the commit future <em>before</em> the callback is queued, so it never waits on the callback.
     * A {@link WakeupException}/{@link InterruptException} thrown by the callback therefore only has to reach
     * the caller of {@code poll()}, and must not be replayed by the next {@code poll()}.
     */
    @ParameterizedTest
    @MethodSource("commitCallbackErrors")
    public void testCommitCallbackWakeupOrInterruptPropagatesFromPollWithoutBlockingNetworkThread(
        Class<? extends KafkaException> errorClass) {
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        consumer = newConsumer(subscriptions, newInvoker(subscriptions));
        doReturn(Fetch.<String, String>empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        doReturn(Metadata.LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        doAnswer(invocation -> {
            AssignmentChangeEvent event = invocation.getArgument(0);
            subscriptions.assignFromUser(Set.copyOf(event.partitions()));
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).addAndGet(isA(AssignmentChangeEvent.class));
        AtomicReference<AsyncCommitEvent> commitEvent = new AtomicReference<>();
        doAnswer(invocation -> {
            AsyncCommitEvent event = invocation.getArgument(0);
            commitEvent.set(event);
            event.markOffsetsReady();
            event.future().complete(event.offsets().orElse(Map.of()));
            return null;
        }).when(applicationEventHandler).add(isA(AsyncCommitEvent.class));
        doAnswer(invocation -> {
            AsyncPollEvent event = invocation.getArgument(0);
            event.completeSuccessfully();
            return null;
        }).when(applicationEventHandler).add(isA(AsyncPollEvent.class));

        consumer.assign(Set.of(TP));
        AtomicBoolean futureDoneWhenCallbackRan = new AtomicBoolean();
        AtomicBoolean callbackInvoked = new AtomicBoolean();
        OffsetCommitCallback callback = (offsets, exception) -> {
            callbackInvoked.set(true);
            futureDoneWhenCallbackRan.set(commitEvent.get().future().isDone());
            throw newError(errorClass);
        };
        consumer.commitAsync(Map.of(TP, new OffsetAndMetadata(10)), callback);

        KafkaException thrown = assertThrows(KafkaException.class, () -> consumer.poll(Duration.ZERO));
        assertInstanceOf(errorClass, thrown);
        assertTrue(callbackInvoked.get());
        assertTrue(futureDoneWhenCallbackRan.get(), "the commit future must be complete before the callback runs");

        Thread.interrupted();
        callbackInvoked.set(false);
        assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
        assertFalse(callbackInvoked.get(), "a callback that threw must not be replayed by the next poll");
    }

    // ------------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------------

    private static final class ThrowingListener implements ConsumerRebalanceListener {
        private final Class<? extends KafkaException> errorClass;
        private int invocations;

        private ThrowingListener(Class<? extends KafkaException> errorClass) {
            this.errorClass = errorClass;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            invoke();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            invoke();
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            invoke();
        }

        private void invoke() {
            invocations++;
            if (errorClass != null)
                throw newError(errorClass);
        }
    }

    private static KafkaException newError(Class<? extends KafkaException> errorClass) {
        if (errorClass == WakeupException.class)
            return new WakeupException();
        if (errorClass == InterruptException.class)
            return new InterruptException("Intentional interrupt from user callback");
        throw new IllegalArgumentException("Unexpected error class " + errorClass);
    }

    private static SortedSet<TopicPartition> sortedSet(TopicPartition... partitions) {
        SortedSet<TopicPartition> set = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        Collections.addAll(set, partitions);
        return set;
    }

    private static void assertTimedOutBeforeClose(CompletableFuture<?> future) {
        assertTrue(future.isCompletedExceptionally());
        Throwable cause = assertThrows(ExecutionException.class, future::get).getCause();
        assertInstanceOf(TimeoutException.class, cause);
        assertNotNull(cause.getMessage());
        assertTrue(cause.getMessage().contains("could not be completed before the consumer closed"), cause.getMessage());
    }

    private ConsumerRebalanceListenerCallbackCompletedEvent captureCallbackCompletedEvent() {
        ArgumentCaptor<ConsumerRebalanceListenerCallbackCompletedEvent> captor =
            ArgumentCaptor.forClass(ConsumerRebalanceListenerCallbackCompletedEvent.class);
        verify(applicationEventHandler).add(captor.capture());
        return captor.getValue();
    }

    private void mockLeader() {
        when(metadata.currentLeader(TP)).thenReturn(new Metadata.LeaderAndEpoch(Optional.of(NODE), Optional.empty()));
        when(metadata.fetch()).thenReturn(new Cluster("clusterId", List.of(NODE),
            List.of(new PartitionInfo(TOPIC, TP.partition(), NODE, null, null)),
            Collections.emptySet(), Collections.emptySet()));
    }

    private ConsumerRebalanceListenerInvoker newInvoker(SubscriptionState subscriptions) {
        return new ConsumerRebalanceListenerInvoker(logContext, subscriptions, time, mock(RebalanceCallbackMetricsManager.class));
    }

    private ConsumerMembershipManager newMembershipManager() {
        return new ConsumerMembershipManager(GROUP_ID, Optional.empty(), Optional.empty(), 30_000, Optional.empty(),
            mock(SubscriptionState.class), mock(CommitRequestManager.class), metadata, logContext,
            mock(BackgroundEventHandler.class), time, new Metrics(), false);
    }

    private CommitRequestManager newCommitRequestManager(SubscriptionState subscriptions) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new CommitRequestManager(
            time,
            logContext,
            subscriptions,
            new ConsumerConfig(props),
            coordinatorRequestManager,
            mock(OffsetCommitCallbackInvoker.class),
            GROUP_ID,
            Optional.empty(),
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MAX_MS,
            OptionalDouble.of(0),
            new Metrics(),
            metadata,
            new AtomicBoolean());
    }

    private OffsetsRequestManager newOffsetsRequestManager(SubscriptionState subscriptions,
                                                           CommitRequestManager commitRequestManager) {
        return new OffsetsRequestManager(
            subscriptions,
            metadata,
            IsolationLevel.READ_UNCOMMITTED,
            time,
            RETRY_BACKOFF_MS,
            REQUEST_TIMEOUT_MS,
            DEFAULT_API_TIMEOUT_MS,
            mock(ApiVersions.class),
            mock(NetworkClientDelegate.class),
            commitRequestManager,
            new PositionsValidator(logContext, time, subscriptions, metadata),
            logContext);
    }

    private AsyncKafkaConsumer<String, String> newConsumer(SubscriptionState subscriptions,
                                                           ConsumerRebalanceListenerInvoker invoker) {
        return new AsyncKafkaConsumer<>(
            logContext,
            "client-id",
            new Deserializers<>(new StringDeserializer(), new StringDeserializer(), metrics),
            mock(FetchBuffer.class),
            fetchCollector,
            mock(FetchMetricsManager.class),
            mock(RebalanceCallbackMetricsManager.class),
            new ConsumerInterceptors<>(Collections.emptyList(), metrics),
            time,
            applicationEventHandler,
            backgroundEventQueue,
            backgroundEventReaper,
            invoker,
            metrics,
            subscriptions,
            metadata,
            RETRY_BACKOFF_MS,
            REQUEST_TIMEOUT_MS,
            DEFAULT_API_TIMEOUT_MS,
            GROUP_ID,
            false,
            new PositionsValidator(logContext, time, subscriptions, metadata));
    }

    private ClientResponse listOffsetsResponse(NetworkClientDelegate.UnsentRequest request, long offset) {
        AbstractRequest built = request.requestBuilder().build();
        ListOffsetsResponseData.ListOffsetsTopicResponse topicResponse = ListOffsetsResponse.singletonListOffsetsTopicResponse(
            TP, Errors.NONE, ListOffsetsResponse.UNKNOWN_TIMESTAMP, offset, ListOffsetsResponse.UNKNOWN_EPOCH);
        ListOffsetsResponse response = new ListOffsetsResponse(new ListOffsetsResponseData()
            .setThrottleTimeMs(0)
            .setTopics(List.of(topicResponse)));
        return new ClientResponse(
            new RequestHeader(ApiKeys.LIST_OFFSETS, built.version(), "", 1),
            request.handler(),
            "-1",
            time.milliseconds(),
            time.milliseconds(),
            false,
            null,
            null,
            response);
    }

    private ClientResponse offsetFetchResponse(NetworkClientDelegate.UnsentRequest request, long committedOffset) {
        AbstractRequest built = request.requestBuilder().build();
        OffsetFetchResponseData.OffsetFetchResponseGroup group = new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId(GROUP_ID)
            .setErrorCode(Errors.NONE.code())
            .setTopics(List.of(new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName(TOPIC)
                .setTopicId(Uuid.ZERO_UUID)
                .setPartitions(List.of(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(TP.partition())
                    .setCommittedOffset(committedOffset)
                    .setCommittedLeaderEpoch(1)
                    .setMetadata("")))));
        OffsetFetchResponse response = new OffsetFetchResponse.Builder(group).build(ApiKeys.OFFSET_FETCH.latestVersion());
        return new ClientResponse(
            new RequestHeader(ApiKeys.OFFSET_FETCH, built.version(), "", 1),
            request.handler(),
            "-1",
            time.milliseconds(),
            time.milliseconds(),
            false,
            null,
            null,
            response);
    }
}
