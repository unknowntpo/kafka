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
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AsyncPollEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CheckAndUpdatePositionsEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.PausePartitionsEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.requiredConsumerConfig;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerReactorTest {
    private final Time time;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final ApplicationEventProcessor applicationEventProcessor;
    private final OffsetsRequestManager offsetsRequestManager;
    private final ConsumerHeartbeatRequestManager heartbeatRequestManager;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final ConsumerReactor consumerReactor;
    private final NetworkClientDelegate networkClientDelegate;
    private final RequestManagers requestManagers;
    private final CompletableEventReaper applicationEventReaper;
    private final AsyncConsumerMetrics asyncConsumerMetrics;

    ConsumerReactorTest() {
        this.networkClientDelegate = mock(NetworkClientDelegate.class);
        this.requestManagers = mock(RequestManagers.class);
        this.offsetsRequestManager = mock(OffsetsRequestManager.class);
        this.heartbeatRequestManager = mock(ConsumerHeartbeatRequestManager.class);
        this.coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        this.applicationEventProcessor = mock(ApplicationEventProcessor.class);
        this.applicationEventReaper = mock(CompletableEventReaper.class);
        this.time = new MockTime();
        this.applicationEventQueue = new LinkedBlockingQueue<>();
        this.asyncConsumerMetrics = mock(AsyncConsumerMetrics.class);
        LogContext logContext = new LogContext();

        this.consumerReactor = new ConsumerReactor(
                logContext,
                time,
                applicationEventQueue,
                applicationEventReaper,
                () -> applicationEventProcessor,
                () -> networkClientDelegate,
                () -> requestManagers,
                asyncConsumerMetrics
        );
    }

    @BeforeEach
    public void setup() {
        when(offsetsRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(offsetsRequestManager.maximumTimeToWait(anyLong())).thenReturn(Long.MAX_VALUE);
        when(heartbeatRequestManager.maximumTimeToWait(anyLong())).thenReturn(Long.MAX_VALUE);
        when(coordinatorRequestManager.maximumTimeToWait(anyLong())).thenReturn(Long.MAX_VALUE);
        when(heartbeatRequestManager.usesLegacyApplicationWait()).thenReturn(true);
        consumerReactor.initializeResources();
    }

    @AfterEach
    public void tearDown() {
        if (consumerReactor != null)
            consumerReactor.close();
    }

    @Test
    public void testEnsureCloseStopsRunningThread() {
        assertTrue(consumerReactor.isRunning(),
            "ConsumerReactor should start running when created");

        consumerReactor.close();
        assertFalse(consumerReactor.isRunning(),
            "close() should make consumerReactor.running false by calling closeInternal(Duration timeout)");
    }

    @ParameterizedTest
    @ValueSource(longs = {ConsumerReactor.MAX_POLL_TIMEOUT_MS - 1, ConsumerReactor.MAX_POLL_TIMEOUT_MS, ConsumerReactor.MAX_POLL_TIMEOUT_MS + 1})
    public void testConsumerReactorPollTimeComputations(long exampleTime) {
        List<RequestManager> list = List.of(coordinatorRequestManager, heartbeatRequestManager);
        when(requestManagers.entries()).thenReturn(list);

        NetworkClientDelegate.PollResult pollResult = new NetworkClientDelegate.PollResult(exampleTime);
        NetworkClientDelegate.PollResult pollResult1 = new NetworkClientDelegate.PollResult(exampleTime + 100);

        long t = time.milliseconds();
        when(coordinatorRequestManager.poll(t)).thenReturn(pollResult);
        when(coordinatorRequestManager.maximumTimeToWait(t)).thenReturn(exampleTime);
        when(heartbeatRequestManager.poll(t)).thenReturn(pollResult1);
        when(heartbeatRequestManager.maximumTimeToWait(t)).thenReturn(exampleTime + 100);
        consumerReactor.runOnce();

        verify(networkClientDelegate).poll(Math.min(exampleTime, ConsumerReactor.MAX_POLL_TIMEOUT_MS), time.milliseconds());
        assertEquals(exampleTime + 100, consumerReactor.maximumTimeToWait(),
            "only managers explicitly using the legacy application wait may constrain the application");
    }

    @Test
    public void testStartupAndTearDown() throws InterruptedException {
        consumerReactor.start();
        TestCondition isStarted = consumerReactor::isRunning;
        TestCondition isClosed = () -> !(consumerReactor.isRunning() || consumerReactor.isAlive());

        // There's a nonzero amount of time between starting the thread and having it
        // begin to execute our code. Wait for a bit before checking...
        TestUtils.waitForCondition(isStarted,
                "The consumer network thread did not start within " + DEFAULT_MAX_WAIT_MS + " ms");

        consumerReactor.close(Duration.ofMillis(DEFAULT_MAX_WAIT_MS));

        TestUtils.waitForCondition(isClosed,
                "The consumer network thread did not stop within " + DEFAULT_MAX_WAIT_MS + " ms");
    }

    @Test
    public void testRequestsTransferFromManagersToClientOnThreadRun() {
        List<RequestManager> list = List.of(coordinatorRequestManager, heartbeatRequestManager, offsetsRequestManager);

        when(requestManagers.entries()).thenReturn(list);
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        consumerReactor.runOnce();
        requestManagers.entries().forEach(rm -> verify(rm).poll(anyLong()));
        verify(heartbeatRequestManager).maximumTimeToWait(anyLong());
        verify(coordinatorRequestManager, never()).maximumTimeToWait(anyLong());
        verify(offsetsRequestManager, never()).maximumTimeToWait(anyLong());
        verify(networkClientDelegate, times(list.size())).addAll(anyList());
        verify(networkClientDelegate).poll(anyLong(), anyLong());
    }

    @Test
    public void testPersistentEmptyImmediatePollResultIdentifiesManager() {
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager));
        when(coordinatorRequestManager.poll(anyLong()))
            .thenReturn(new NetworkClientDelegate.PollResult(0L));

        consumerReactor.runOnce();
        consumerReactor.runOnce();

        verify(coordinatorRequestManager, times(2)).poll(anyLong());
        verify(networkClientDelegate, times(2)).poll(ConsumerReactor.MAX_POLL_TIMEOUT_MS, time.milliseconds());
        verify(asyncConsumerMetrics, times(2)).recordPollResultContractViolation();
    }

    @Test
    public void testManagerPollFailureIsIsolatedWithoutSkippingNetworkIo() {
        RuntimeException failure = new RuntimeException("manager poll failed");
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager));
        when(coordinatorRequestManager.poll(anyLong())).thenThrow(failure);

        consumerReactor.runOnce();

        verify(asyncConsumerMetrics).recordManagerPollFailure();
        verify(networkClientDelegate).poll(ConsumerReactor.MAX_POLL_TIMEOUT_MS, time.milliseconds());
    }

    @Test
    public void testMaximumTimeToWait() {
        final int defaultHeartbeatIntervalMs = 1000;
        // Initial value before runOnce has been called
        assertEquals(ConsumerReactor.MAX_POLL_TIMEOUT_MS, consumerReactor.maximumTimeToWait());

        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.maximumTimeToWait(time.milliseconds())).thenReturn((long) defaultHeartbeatIntervalMs);

        consumerReactor.runOnce();
        // After runOnce has been called, it takes the default heartbeat interval from the heartbeat request manager
        assertEquals(defaultHeartbeatIntervalMs, consumerReactor.maximumTimeToWait());
    }

    @Test
    public void testReactorOnlyDeadlineDoesNotWakeApplication() {
        long startMs = time.milliseconds();
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager));
        when(coordinatorRequestManager.poll(startMs))
            .thenReturn(new NetworkClientDelegate.PollResult(100L));

        consumerReactor.runOnce();

        verify(networkClientDelegate).poll(100L, startMs);
        verify(requestManagers, never()).wakeupApplicationThread();
        assertEquals(startMs + 100L, consumerReactor.reactorSchedule().reactorDeadlineMs());
        assertEquals(Long.MAX_VALUE, consumerReactor.reactorSchedule().applicationDeadlineMs());
    }

    @Test
    public void testPostIoBackgroundEventIsPublishedAfterScheduleAndWakesApplication() {
        AtomicReference<Boolean> backgroundEventPending = new AtomicReference<>(false);
        when(requestManagers.entries()).thenReturn(List.of());
        when(requestManagers.hasPendingBackgroundEvents()).thenAnswer(ignored -> backgroundEventPending.get());
        when(requestManagers.publishPendingBackgroundEvents()).thenAnswer(ignored -> {
            if (!backgroundEventPending.compareAndSet(true, false))
                return 0;
            assertEquals(2L, consumerReactor.reactorSchedule().generation(),
                "the post-I/O schedule must be published before the background event");
            return 1;
        });
        doAnswer(ignored -> {
            backgroundEventPending.set(true);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());

        consumerReactor.runOnce();

        InOrder order = inOrder(networkClientDelegate, requestManagers);
        order.verify(networkClientDelegate).poll(anyLong(), anyLong());
        order.verify(requestManagers).publishPendingBackgroundEvents();
        order.verify(requestManagers).wakeupApplicationThread();
        assertFalse(backgroundEventPending.get());
    }

    @Test
    public void testManagerEventActionIsExecutedAfterSchedulePublication() {
        long currentTimeMs = time.milliseconds();
        AtomicLong deadlineObservedByWakeup = new AtomicLong(-1L);
        NetworkClientDelegate.PollResult result = NetworkClientDelegate.PollResult.progress(
            List.of(),
            List.of(ManagerEvent.FetchBufferHasData.INSTANCE),
            NextPollCondition.retryAfter(100L)
        );
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager));
        when(coordinatorRequestManager.poll(currentTimeMs)).thenReturn(result);
        when(requestManagers.planManagerEvents(any())).thenReturn(
            CoordinationPlan.action(ReactorAction.wakeApplication())
        );
        doAnswer(invocation -> {
            deadlineObservedByWakeup.set(consumerReactor.reactorSchedule().reactorDeadlineMs());
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        consumerReactor.runOnce();

        assertEquals(currentTimeMs + 100L, deadlineObservedByWakeup.get());
        verify(requestManagers).wakeupApplicationThread();
    }

    @Test
    public void testManagerEventsFromOnePhaseAreEvaluatedAsOneBatchAndWakeOnce() {
        long currentTimeMs = time.milliseconds();
        NetworkClientDelegate.PollResult fetchResult = NetworkClientDelegate.PollResult.progress(
            List.of(),
            List.of(ManagerEvent.FetchBufferHasData.INSTANCE),
            NextPollCondition.awaitInput()
        );
        NetworkClientDelegate.PollResult offsetsResult = NetworkClientDelegate.PollResult.progress(
            List.of(),
            List.of(ManagerEvent.LocalProgress.FETCH_POSITIONS_UPDATE_FAILED),
            NextPollCondition.awaitInput()
        );
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager, offsetsRequestManager));
        when(coordinatorRequestManager.poll(currentTimeMs)).thenReturn(fetchResult);
        when(offsetsRequestManager.poll(currentTimeMs)).thenReturn(offsetsResult);
        when(requestManagers.planManagerEvents(any())).thenAnswer(invocation ->
            ManagerCoordinationPolicy.standard().evaluate(invocation.getArgument(0)));

        consumerReactor.runOnce();

        verify(requestManagers).planManagerEvents(argThat(events ->
            events.equals(List.of(
                ManagerEvent.FetchBufferHasData.INSTANCE,
                ManagerEvent.LocalProgress.FETCH_POSITIONS_UPDATE_FAILED
            ))));
        verify(requestManagers, times(1)).wakeupApplicationThread();
    }

    @Test
    public void testUnexpectedPreIoCrossOwnerCommandIsRetainedAndEventuallyApplied() {
        long currentTimeMs = time.milliseconds();
        ManagerEvent.CoordinatorUnavailableObserved observation =
            new ManagerEvent.CoordinatorUnavailableObserved("heartbeat", "not coordinator", currentTimeMs, 7L);
        NetworkClientDelegate.PollResult result = NetworkClientDelegate.PollResult.progress(
            List.of(),
            List.of(observation),
            NextPollCondition.awaitInput()
        );
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.poll(currentTimeMs))
            .thenReturn(result, NetworkClientDelegate.PollResult.awaitInput());
        when(requestManagers.planManagerEvents(any())).thenReturn(CoordinationPlan.command(
            new ManagerCommand.InvalidateCoordinatorIfCurrent(observation)
        ));

        consumerReactor.runOnce();

        verify(networkClientDelegate).poll(anyLong(), anyLong());
        verify(requestManagers, never()).applyManagerCommands(any());

        consumerReactor.runOnce();

        verify(requestManagers).applyManagerCommands(argThat(commands -> commands.size() == 1));
        verify(networkClientDelegate, times(2)).poll(anyLong(), anyLong());
    }

    @Test
    public void testPendingEventSurvivesPostIoPollFailureAndAppliesBeforeNextFullPass() {
        long currentTimeMs = time.milliseconds();
        ManagerEvent.CoordinatorUnavailableObserved observation =
            new ManagerEvent.CoordinatorUnavailableObserved("heartbeat", "not coordinator", currentTimeMs, 7L);
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            mock(AbstractRequest.Builder.class),
            Optional.empty()
        );
        AtomicLong pollCount = new AtomicLong();
        AtomicLong ownerCommandApplied = new AtomicLong();

        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(requestManagers.drainPendingManagerEvents()).thenReturn(List.of(), List.of(observation));
        when(heartbeatRequestManager.poll(currentTimeMs)).thenAnswer(invocation -> {
            long call = pollCount.getAndIncrement();
            if (call == 0L)
                return new NetworkClientDelegate.PollResult(request);
            if (call == 1L)
                throw new KafkaException("post-I/O poll failed before publishing pending facts");
            assertEquals(1L, ownerCommandApplied.get(),
                "owner command must apply before the next full manager pass");
            return NetworkClientDelegate.PollResult.awaitInput();
        });
        when(requestManagers.planManagerEvents(any())).thenReturn(CoordinationPlan.command(
            new ManagerCommand.InvalidateCoordinatorIfCurrent(observation)
        ));
        doAnswer(invocation -> {
            ownerCommandApplied.incrementAndGet();
            return null;
        }).when(requestManagers).applyManagerCommands(any());
        doAnswer(invocation -> {
            request.future().complete(null);
            return null;
        }).doAnswer(invocation -> {
            assertEquals(1L, ownerCommandApplied.get(), "owner command must apply before the next network poll");
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());

        consumerReactor.runOnce();

        assertEquals(0L, ownerCommandApplied.get());
        assertEquals(2L, pollCount.get(), "post-I/O manager poll must reach the injected failure");
        verify(asyncConsumerMetrics).recordManagerPollFailure();

        consumerReactor.runOnce();

        assertEquals(1L, ownerCommandApplied.get());
        assertEquals(3L, pollCount.get());
        verify(requestManagers).applyManagerCommands(argThat(commands -> commands.size() == 1));
        verify(networkClientDelegate, times(2)).poll(anyLong(), anyLong());
    }

    @Test
    public void testApplicationEventCompletionExecutesAfterSchedulePublicationAndBeforeWakeup() {
        long currentTimeMs = time.milliseconds();
        AtomicLong completionGeneration = new AtomicLong(-1L);
        AtomicLong generationObservedByWakeup = new AtomicLong(-1L);
        AsyncPollEvent event = new AsyncPollEvent(currentTimeMs + 1_000L, currentTimeMs) {
            @Override
            public void completeSuccessfully() {
                completionGeneration.set(consumerReactor.reactorScheduleGeneration());
                super.completeSuccessfully();
            }
        };
        NetworkClientDelegate.PollResult result = NetworkClientDelegate.PollResult.progress(
            List.of(),
            List.of(ManagerEvent.LocalProgress.FETCH_REQUEST_TERMINATED),
            NextPollCondition.retryAfter(100L)
        );
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager));
        when(coordinatorRequestManager.poll(currentTimeMs)).thenReturn(result);
        when(requestManagers.planManagerEvents(any())).thenAnswer(invocation ->
            ManagerCoordinationPolicy.standard().evaluate(invocation.getArgument(0)));
        when(applicationEventProcessor.drainReactorActions()).thenReturn(
            List.of(ReactorAction.completeAsyncPoll(event, null)),
            List.of()
        );
        doAnswer(invocation -> {
            assertTrue(event.isComplete(), "Event completion must be visible before wakeup");
            generationObservedByWakeup.set(consumerReactor.reactorScheduleGeneration());
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        consumerReactor.runOnce();

        assertTrue(completionGeneration.get() > 0L);
        assertEquals(completionGeneration.get(), generationObservedByWakeup.get());
        verify(requestManagers).wakeupApplicationThread();
    }

    @Test
    public void testFailingReactorActionDoesNotSuppressLaterActionOrWakeup() {
        long currentTimeMs = time.milliseconds();
        AsyncPollEvent failingEvent = new AsyncPollEvent(currentTimeMs + 1_000L, currentTimeMs) {
            @Override
            public void completeSuccessfully() {
                throw new KafkaException("action failed");
            }
        };
        AsyncPollEvent succeedingEvent = new AsyncPollEvent(currentTimeMs + 1_000L, currentTimeMs);
        NetworkClientDelegate.PollResult progress = NetworkClientDelegate.PollResult.progress(
            List.of(),
            List.of(ManagerEvent.LocalProgress.FETCH_REQUEST_TERMINATED),
            NextPollCondition.retryAfter(100L)
        );
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager));
        when(coordinatorRequestManager.poll(currentTimeMs)).thenReturn(progress);
        when(requestManagers.planManagerEvents(any())).thenAnswer(invocation ->
            ManagerCoordinationPolicy.standard().evaluate(invocation.getArgument(0)));
        when(applicationEventProcessor.drainReactorActions()).thenReturn(
            List.of(
                ReactorAction.completeAsyncPoll(failingEvent, null),
                ReactorAction.completeAsyncPoll(succeedingEvent, null)
            ),
            List.of()
        );

        consumerReactor.runOnce();

        assertTrue(succeedingEvent.isComplete());
        verify(asyncConsumerMetrics).recordReactorActionFailure();
        verify(asyncConsumerMetrics).recordApplicationWakeup();
        verify(requestManagers).wakeupApplicationThread();
        verify(networkClientDelegate).poll(anyLong(), anyLong());
    }

    @Test
    public void testMetadataErrorExecutesAfterSchedulePublicationAndBeforeWakeup() {
        long currentTimeMs = time.milliseconds();
        KafkaException metadataError = new KafkaException("metadata error");
        AtomicLong completionGeneration = new AtomicLong(-1L);
        AsyncPollEvent event = new AsyncPollEvent(currentTimeMs + 1_000L, currentTimeMs) {
            @Override
            public void onMetadataError(final Exception error) {
                super.onMetadataError(error);
                completionGeneration.set(consumerReactor.reactorScheduleGeneration());
            }
        };
        applicationEventQueue.add(event);
        when(networkClientDelegate.getAndClearMetadataError()).thenReturn(Optional.of(metadataError));
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager));
        doAnswer(invocation -> {
            assertTrue(event.isComplete(), "Metadata error must complete the event before wakeup");
            assertEquals(completionGeneration.get(), consumerReactor.reactorScheduleGeneration());
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        consumerReactor.runOnce();

        assertEquals(metadataError, event.error().orElseThrow());
        assertTrue(completionGeneration.get() > 0L);
        verify(applicationEventProcessor, never()).process(event);
        verify(requestManagers).wakeupApplicationThread();
    }

    @Test
    public void testWakeupDeduplicationIsPerReactorPhase() {
        long currentTimeMs = time.milliseconds();
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            mock(AbstractRequest.Builder.class),
            Optional.empty()
        );
        NetworkClientDelegate.PollResult preIo = NetworkClientDelegate.PollResult.progress(
            List.of(request),
            List.of(ManagerEvent.FetchBufferHasData.INSTANCE),
            NextPollCondition.awaitInput()
        );
        NetworkClientDelegate.PollResult postIo = NetworkClientDelegate.PollResult.progress(
            List.of(),
            List.of(ManagerEvent.FetchBufferHasData.INSTANCE),
            NextPollCondition.awaitInput()
        );
        CheckAndUpdatePositionsEvent metadataEvent =
            new CheckAndUpdatePositionsEvent(currentTimeMs + 1_000L);

        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(requestManagers.planManagerEvents(any())).thenReturn(
            CoordinationPlan.action(ReactorAction.wakeApplication())
        );
        doReturn(preIo, postIo).when(heartbeatRequestManager).poll(currentTimeMs);
        doAnswer(invocation -> {
            request.future().complete(null);
            return null;
        }).when(networkClientDelegate).poll(ConsumerReactor.MAX_POLL_TIMEOUT_MS, currentTimeMs);
        when(applicationEventReaper.uncompletedEvents()).thenReturn(List.of(metadataEvent));
        when(networkClientDelegate.getAndClearMetadataError()).thenReturn(
            Optional.of(new KafkaException("metadata error")));

        consumerReactor.runOnce();

        verify(requestManagers, times(3)).wakeupApplicationThread();
    }

    @Test
    public void testRequestCompletionPollsOnlyAffectedManagerAndPublishesBeforeWakeup() {
        long currentTimeMs = time.milliseconds();
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            mock(AbstractRequest.Builder.class),
            Optional.empty()
        );
        NetworkClientDelegate.PollResult beforeCompletion =
            new NetworkClientDelegate.PollResult(request);
        NetworkClientDelegate.PollResult afterCompletion = NetworkClientDelegate.PollResult.progress(
            List.of(),
            List.of(ManagerEvent.LocalProgress.FETCH_REQUEST_TERMINATED),
            NextPollCondition.retryAfter(7_000L)
        );
        NetworkClientDelegate.PollResult unaffected =
            new NetworkClientDelegate.PollResult(6_000L);
        AtomicLong deadlineObservedByWakeup = new AtomicLong(-1L);

        when(requestManagers.entries()).thenReturn(
            List.of(heartbeatRequestManager, coordinatorRequestManager)
        );
        doReturn(beforeCompletion, afterCompletion)
            .when(heartbeatRequestManager).poll(currentTimeMs);
        doReturn(unaffected).when(coordinatorRequestManager).poll(currentTimeMs);
        when(requestManagers.planManagerEvents(any())).thenAnswer(invocation ->
            ManagerCoordinationPolicy.standard().evaluate(invocation.getArgument(0)));
        doAnswer(invocation -> {
            request.future().complete(null);
            return null;
        }).when(networkClientDelegate).poll(ConsumerReactor.MAX_POLL_TIMEOUT_MS, currentTimeMs);
        doAnswer(invocation -> {
            deadlineObservedByWakeup.set(consumerReactor.reactorSchedule().reactorDeadlineMs());
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        consumerReactor.runOnce();

        verify(heartbeatRequestManager, times(2)).poll(currentTimeMs);
        verify(coordinatorRequestManager).poll(currentTimeMs);
        verify(requestManagers).wakeupApplicationThread();
        assertEquals(currentTimeMs + 6_000L, deadlineObservedByWakeup.get());
    }

    @Test
    public void testCoordinatorDiscoveryIsObservedByHeartbeatAndCommitOnNextFullPass() {
        long initialTimeMs = time.milliseconds();
        String groupId = "group-id";
        Node coordinatorNode = new Node(1, "localhost", 9092);
        CommitRequestManager commitRequestManager = mock(CommitRequestManager.class);
        CoordinatorRequestManager realCoordinatorRequestManager = new CoordinatorRequestManager(
            new LogContext(),
            100L,
            1_000L,
            groupId
        );
        NetworkClientDelegate.UnsentRequest initialDiscovery =
            realCoordinatorRequestManager.poll(initialTimeMs).unsentRequests.get(0);
        initialDiscovery.handler().onComplete(
            buildCoordinatorResponse(initialDiscovery, coordinatorNode, groupId)
        );
        realCoordinatorRequestManager.poll(initialTimeMs);
        time.sleep(1_000L);
        long currentTimeMs = time.milliseconds();
        realCoordinatorRequestManager.markCoordinatorUnknown("test coordinator loss", currentTimeMs);
        assertTrue(realCoordinatorRequestManager.coordinator().isEmpty());

        NetworkClientDelegate.UnsentRequest heartbeatRequest = new NetworkClientDelegate.UnsentRequest(
            mock(AbstractRequest.Builder.class),
            Optional.empty()
        );
        NetworkClientDelegate.PollResult heartbeatReady = new NetworkClientDelegate.PollResult(
            1_000L,
            List.of(heartbeatRequest)
        );
        NetworkClientDelegate.UnsentRequest commitRequest = new NetworkClientDelegate.UnsentRequest(
            mock(AbstractRequest.Builder.class),
            Optional.empty()
        );
        NetworkClientDelegate.PollResult commitReady =
            NetworkClientDelegate.PollResult.progress(
                List.of(commitRequest), List.of(), NextPollCondition.retryAfter(1_000L));
        AtomicReference<NetworkClientDelegate.UnsentRequest> findCoordinatorRequest = new AtomicReference<>();

        when(requestManagers.entries()).thenReturn(
            List.of(realCoordinatorRequestManager, commitRequestManager, heartbeatRequestManager)
        );
        when(commitRequestManager.poll(currentTimeMs)).thenAnswer(invocation ->
            realCoordinatorRequestManager.coordinator().isEmpty()
                ? NetworkClientDelegate.PollResult.awaitInput()
                : commitReady
        );
        when(heartbeatRequestManager.poll(currentTimeMs)).thenAnswer(invocation ->
            realCoordinatorRequestManager.coordinator().isEmpty()
                ? NetworkClientDelegate.PollResult.EMPTY
                : heartbeatReady
        );
        doAnswer(invocation -> {
            List<NetworkClientDelegate.UnsentRequest> requests = invocation.getArgument(0);
            for (NetworkClientDelegate.UnsentRequest request : requests) {
                if (request.requestBuilder() instanceof FindCoordinatorRequest.Builder)
                    findCoordinatorRequest.set(request);
            }
            return null;
        }).when(networkClientDelegate).addAll(anyList());
        doAnswer(invocation -> {
            NetworkClientDelegate.UnsentRequest request = findCoordinatorRequest.get();
            assertTrue(request != null, "FindCoordinator request must be staged before network poll");
            request.handler().onComplete(buildCoordinatorResponse(request, coordinatorNode, groupId));
            return null;
        }).doNothing().when(networkClientDelegate).poll(ConsumerReactor.MAX_POLL_TIMEOUT_MS, currentTimeMs);

        consumerReactor.runOnce();

        verify(networkClientDelegate, never()).addAll(heartbeatReady.unsentRequests);
        verify(networkClientDelegate, never()).addAll(commitReady.unsentRequests);
        verify(heartbeatRequestManager).poll(currentTimeMs);
        verify(commitRequestManager).poll(currentTimeMs);

        consumerReactor.runOnce();

        InOrder networkOrder = inOrder(networkClientDelegate);
        networkOrder.verify(networkClientDelegate).poll(ConsumerReactor.MAX_POLL_TIMEOUT_MS, currentTimeMs);
        networkOrder.verify(networkClientDelegate).addAll(commitReady.unsentRequests);
        networkOrder.verify(networkClientDelegate).addAll(heartbeatReady.unsentRequests);
        networkOrder.verify(networkClientDelegate).poll(1_000L, currentTimeMs);
        verify(networkClientDelegate).poll(ConsumerReactor.MAX_POLL_TIMEOUT_MS, currentTimeMs);
        verify(networkClientDelegate).poll(1_000L, currentTimeMs);
        verify(heartbeatRequestManager, times(2)).poll(currentTimeMs);
        verify(commitRequestManager, times(2)).poll(currentTimeMs);
        verify(networkClientDelegate).addAll(commitReady.unsentRequests);
        verify(networkClientDelegate).addAll(heartbeatReady.unsentRequests);
        verify(requestManagers, never()).wakeupApplicationThread();
        assertTrue(realCoordinatorRequestManager.coordinator().isPresent());
        assertEquals(currentTimeMs + 1_000L, consumerReactor.reactorSchedule().reactorDeadlineMs());
    }

    /**
     * Component-layer proof of the exact response-to-event-to-owner ordering with real request managers.
     */
    @Test
    public void testRealHeartbeatInvalidationIsRoutedBeforeNextNetworkPoll() {
        long currentTimeMs = time.milliseconds();
        String groupId = "group-id";
        Node coordinatorNode = new Node(1, "localhost", 9092);
        LogContext localLogContext = new LogContext();
        NetworkClientDelegate localNetworkClientDelegate = mock(NetworkClientDelegate.class);
        ConsumerMetadata localMetadata = mock(ConsumerMetadata.class);
        SubscriptionState localSubscriptions = mock(SubscriptionState.class);
        when(localSubscriptions.subscription()).thenReturn(Set.of("topic"));
        when(localSubscriptions.assignedPartitions()).thenReturn(Set.of());
        when(localSubscriptions.hasAutoAssignedPartitions()).thenReturn(true);

        Properties properties = requiredConsumerConfig();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");
        properties.setProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
        properties.setProperty(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, "1000");
        ConsumerConfig config = new ConsumerConfig(properties);
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
            config,
            GroupRebalanceConfig.ProtocolType.CONSUMER
        );
        Metrics localMetrics = new Metrics(time);
        RequestManagers realRequestManagers = RequestManagers.supplier(
            time,
            localLogContext,
            mock(BackgroundEventHandler.class),
            localMetadata,
            localSubscriptions,
            mock(FetchBuffer.class),
            config,
            groupRebalanceConfig,
            mock(ApiVersions.class),
            mock(FetchMetricsManager.class),
            () -> localNetworkClientDelegate,
            Optional.empty(),
            localMetrics,
            mock(OffsetCommitCallbackInvoker.class),
            (memberEpoch, memberId) -> { },
            Optional.empty(),
            new PositionsValidator(localLogContext, time, localSubscriptions, localMetadata)
        ).get();
        RecordingRequestManagers recordingRequestManagers = new RecordingRequestManagers(
            localLogContext,
            realRequestManagers
        );
        RecordingRequestManagers observedRequestManagers = spy(recordingRequestManagers);
        CoordinatorRequestManager realCoordinatorRequestManager =
            observedRequestManagers.coordinatorRequestManager.orElseThrow();
        ConsumerHeartbeatRequestManager realHeartbeatRequestManager =
            observedRequestManagers.consumerHeartbeatRequestManager.orElseThrow();
        ConsumerMembershipManager realMembershipManager =
            observedRequestManagers.consumerMembershipManager.orElseThrow();
        assertTrue(observedRequestManagers.entries().contains(realHeartbeatRequestManager));

        NetworkClientDelegate.UnsentRequest initialDiscovery =
            realCoordinatorRequestManager.poll(currentTimeMs).unsentRequests.get(0);
        initialDiscovery.handler().onComplete(
            buildCoordinatorResponse(initialDiscovery, coordinatorNode, groupId));
        realCoordinatorRequestManager.poll(currentTimeMs);
        assertTrue(realCoordinatorRequestManager.coordinator().isPresent());
        realMembershipManager.transitionToJoining();

        AtomicReference<NetworkClientDelegate.UnsentRequest> heartbeatRequest = new AtomicReference<>();
        AtomicLong heartbeatRequestCount = new AtomicLong();
        AtomicReference<NetworkClientDelegate.UnsentRequest> findCoordinatorRequest = new AtomicReference<>();
        doAnswer(invocation -> {
            List<NetworkClientDelegate.UnsentRequest> requests = invocation.getArgument(0);
            for (NetworkClientDelegate.UnsentRequest request : requests) {
                if (request.requestBuilder() instanceof ConsumerGroupHeartbeatRequest.Builder) {
                    heartbeatRequestCount.incrementAndGet();
                    heartbeatRequest.compareAndSet(null, request);
                }
                if (request.requestBuilder() instanceof FindCoordinatorRequest.Builder)
                    findCoordinatorRequest.set(request);
            }
            return null;
        }).when(localNetworkClientDelegate).addAll(anyList());
        doAnswer(invocation -> {
            assertTrue(heartbeatRequest.get() != null,
                "real heartbeat request must be staged before the first network poll");
            heartbeatRequest.get().handler().onComplete(
                buildHeartbeatResponse(heartbeatRequest.get(), Errors.NOT_COORDINATOR));
            return null;
        }).doAnswer(invocation -> {
            assertTrue(findCoordinatorRequest.get() != null,
                "rediscovery must be staged before the next network poll");
            return null;
        }).when(localNetworkClientDelegate).poll(anyLong(), anyLong());

        ConsumerReactor localReactor = new ConsumerReactor(
            new LogContext(),
            time,
            new LinkedBlockingQueue<>(),
            mock(CompletableEventReaper.class),
            () -> mock(ApplicationEventProcessor.class),
            () -> localNetworkClientDelegate,
            () -> observedRequestManagers,
            asyncConsumerMetrics
        );
        localReactor.initializeResources();
        try {
            localReactor.runOnce();
            assertTrue(realCoordinatorRequestManager.coordinator().isPresent(),
                "the post-I/O manager command is deferred to the next full pass");
            assertTrue(findCoordinatorRequest.get() == null);
            verify(observedRequestManagers).planManagerEvents(any());
            verify(observedRequestManagers, never()).applyManagerCommands(any());

            clearInvocations(observedRequestManagers, localNetworkClientDelegate);

            // The coordinator owner retains its normal retry policy; wait beyond the jittered 100 ms backoff.
            time.sleep(200L);
            localReactor.runOnce();

            assertTrue(realCoordinatorRequestManager.coordinator().isEmpty());
            assertEquals(1L, heartbeatRequestCount.get(),
                "the post-I/O invalidation result must not admit a heartbeat using the stale coordinator");
            verify(observedRequestManagers).applyManagerCommands(any());
            assertEquals(1, observedRequestManagers.plannedEvents().size());
            ManagerEvent event = observedRequestManagers.plannedEvents().get(0);
            assertTrue(event instanceof ManagerEvent.CoordinatorUnavailableObserved);
            assertEquals(ConsumerHeartbeatRequestManager.class.getSimpleName(), event.source());

            InOrder secondRunOrder = inOrder(observedRequestManagers, localNetworkClientDelegate);
            secondRunOrder.verify(observedRequestManagers).applyManagerCommands(any());
            secondRunOrder.verify(localNetworkClientDelegate).addAll(
                argThat((List<NetworkClientDelegate.UnsentRequest> requests) -> requests.stream()
                    .anyMatch(request -> request.requestBuilder() instanceof FindCoordinatorRequest.Builder))
            );
            secondRunOrder.verify(localNetworkClientDelegate).poll(anyLong(), anyLong());
        } finally {
            localReactor.close();
            localMetrics.close();
        }
    }

    private ClientResponse buildCoordinatorResponse(final NetworkClientDelegate.UnsentRequest request,
                                                    final Node coordinatorNode,
                                                    final String groupId) {
        FindCoordinatorRequest findCoordinatorRequest = (FindCoordinatorRequest) request.requestBuilder().build();
        FindCoordinatorResponse response = FindCoordinatorResponse.prepareResponse(
            Errors.NONE,
            groupId,
            coordinatorNode
        );
        return new ClientResponse(
            new RequestHeader(ApiKeys.FIND_COORDINATOR, findCoordinatorRequest.version(), "", 1),
            request.handler(),
            coordinatorNode.idString(),
            time.milliseconds(),
            time.milliseconds(),
            false,
            null,
            null,
            response
        );
    }

    private ClientResponse buildHeartbeatResponse(final NetworkClientDelegate.UnsentRequest request,
                                                  final Errors error) {
        ConsumerGroupHeartbeatResponse response = new ConsumerGroupHeartbeatResponse(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(error.code())
                .setErrorMessage("heartbeat rejected coordinator")
                .setHeartbeatIntervalMs(1_000)
        );
        return new ClientResponse(
            new RequestHeader(
                ApiKeys.CONSUMER_GROUP_HEARTBEAT,
                ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion(),
                "client-id",
                1
            ),
            request.handler(),
            "0",
            time.milliseconds(),
            time.milliseconds(),
            false,
            null,
            null,
            response
        );
    }

    /** Captures the event batch before the coordination policy derives commands and actions. */
    private static final class RecordingRequestManagers extends RequestManagers {
        private List<ManagerEvent> plannedEvents = List.of();

        private RecordingRequestManagers(final LogContext logContext,
                                         final RequestManagers delegate) {
            super(
                logContext,
                delegate.offsetsRequestManager,
                delegate.topicMetadataRequestManager,
                delegate.fetchRequestManager,
                delegate.coordinatorRequestManager,
                delegate.commitRequestManager,
                delegate.consumerHeartbeatRequestManager,
                delegate.consumerMembershipManager,
                delegate.streamsGroupHeartbeatRequestManager,
                delegate.streamsGroupTopologyDescriptionRequestManager,
                delegate.streamsMembershipManager
            );
        }

        @Override
        CoordinationPlan planManagerEvents(final Collection<ManagerEvent> events) {
            plannedEvents = List.copyOf(events);
            return super.planManagerEvents(events);
        }

        private List<ManagerEvent> plannedEvents() {
            return plannedEvents;
        }
    }

    @Test
    public void testEarlierManagerDeadlineDoesNotEraseLaterManagerDeadline() {
        long startMs = time.milliseconds();
        when(requestManagers.entries()).thenReturn(
            List.of(coordinatorRequestManager, heartbeatRequestManager)
        );
        when(coordinatorRequestManager.poll(anyLong()))
            .thenReturn(new NetworkClientDelegate.PollResult(100L));
        when(heartbeatRequestManager.poll(anyLong()))
            .thenReturn(new NetworkClientDelegate.PollResult(30L))
            .thenReturn(new NetworkClientDelegate.PollResult(30L))
            .thenReturn(new NetworkClientDelegate.PollResult(100L));
        doAnswer(invocation -> {
            time.sleep(invocation.getArgument(0, Long.class));
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());

        consumerReactor.runOnce();
        consumerReactor.runOnce();
        consumerReactor.runOnce();

        verify(networkClientDelegate).poll(30L, startMs);
        verify(networkClientDelegate).poll(30L, startMs + 30L);
        verify(networkClientDelegate).poll(40L, startMs + 60L);
        verify(requestManagers, never()).wakeupApplicationThread();
        assertEquals(startMs + 100L, consumerReactor.reactorSchedule().reactorDeadlineMs());
    }

    @Test
    public void testCompatibilityApplicationDeadlineBoundsNetworkPoll() {
        long startMs = time.milliseconds();
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.maximumTimeToWait(startMs)).thenReturn(100L);

        consumerReactor.runOnce();

        verify(networkClientDelegate).poll(100L, startMs);
    }

    @Test
    public void testShorterCompatibilityScheduleIsPublishedBeforeWakeup() {
        AtomicLong waitObservedByWakeup = new AtomicLong(-1L);
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.maximumTimeToWait(anyLong())).thenReturn(100L);
        doAnswer(invocation -> {
            waitObservedByWakeup.set(consumerReactor.maximumTimeToWait());
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        consumerReactor.runOnce();

        assertEquals(100L, waitObservedByWakeup.get());
    }

    @Test
    public void testDeliveredCompatibilityDeadlineRearmsAndBoundsNextPoll() {
        long startMs = time.milliseconds();
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.maximumTimeToWait(anyLong())).thenReturn(100L);
        doAnswer(invocation -> {
            time.sleep(invocation.getArgument(0, Long.class));
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());

        consumerReactor.runOnce();
        assertEquals(Long.MAX_VALUE, consumerReactor.maximumTimeToWait());
        consumerReactor.runOnce();

        verify(networkClientDelegate).poll(100L, startMs);
        verify(networkClientDelegate).poll(100L, startMs + 100L);
        verify(requestManagers, times(3)).wakeupApplicationThread();
    }

    @Test
    public void testPersistentZeroCompatibilityWaitDoesNotBusyLoop() {
        long startMs = time.milliseconds();
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.maximumTimeToWait(anyLong())).thenReturn(0L);

        consumerReactor.runOnce();
        consumerReactor.runOnce();

        verify(networkClientDelegate).poll(0L, startMs);
        verify(networkClientDelegate).poll(ConsumerReactor.MAX_POLL_TIMEOUT_MS, startMs);
        verify(requestManagers).wakeupApplicationThread();
    }

    @Test
    public void testCleanupInvokesReaper() {
        LinkedList<NetworkClientDelegate.UnsentRequest> queue = new LinkedList<>();
        when(networkClientDelegate.unsentRequests()).thenReturn(queue);
        when(applicationEventReaper.reap(applicationEventQueue)).thenReturn(1L);
        consumerReactor.cleanup();
        verify(applicationEventReaper).reap(applicationEventQueue);
        verify(asyncConsumerMetrics).recordApplicationEventExpiredSize(1L);
    }

    @Test
    public void testCleanupExecutesStagedAsyncPollCompletion() {
        AsyncPollEvent event = new AsyncPollEvent(time.milliseconds() + 1_000L, time.milliseconds());
        when(applicationEventProcessor.drainReactorActions()).thenReturn(
            List.of(),
            List.of(),
            List.of(ReactorAction.completeAsyncPoll(event, null)));

        consumerReactor.runOnce();
        assertFalse(event.isComplete());
        assertTrue(consumerReactor.reactorScheduleGeneration() > 0L);

        consumerReactor.cleanup();

        assertTrue(event.isComplete(), "cleanup must not drop an async-poll completion staged after the final loop drain");
    }

    @Test
    public void testRunOnceInvokesReaper() {
        when(applicationEventReaper.reap(any(Long.class))).thenReturn(1L);
        consumerReactor.runOnce();
        verify(applicationEventReaper).reap(any(Long.class));
        verify(asyncConsumerMetrics).recordApplicationEventExpiredSize(1L);
    }

    @Test
    public void testSendUnsentRequests() {
        when(networkClientDelegate.hasAnyPendingRequests()).thenReturn(true).thenReturn(true).thenReturn(false);
        consumerReactor.cleanup();
        verify(networkClientDelegate, times(2)).poll(anyLong(), anyLong(), eq(true));
    }

    @ParameterizedTest
    @MethodSource("org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetricsTest#groupNameProvider")
    public void testRunOnceRecordTimeBetweenNetworkThreadPoll(String groupName) {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, groupName);
             ConsumerReactor consumerReactor = new ConsumerReactor(
                     new LogContext(),
                     time,
                     applicationEventQueue,
                     applicationEventReaper,
                     () -> applicationEventProcessor,
                     () -> networkClientDelegate,
                     () -> requestManagers,
                     asyncConsumerMetrics
             )) {
            consumerReactor.initializeResources();

            consumerReactor.runOnce();
            time.sleep(10);
            consumerReactor.runOnce();
            assertEquals(
                10,
                (double) metrics.metric(
                    metrics.metricName("time-between-network-thread-poll-avg", groupName)
                ).metricValue()
            );
            assertEquals(
                10,
                (double) metrics.metric(
                    metrics.metricName("time-between-network-thread-poll-max", groupName)
                ).metricValue()
            );
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetricsTest#groupNameProvider")
    public void testRunOnceRecordApplicationEventQueueSizeAndApplicationEventQueueTime(String groupName) {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, groupName);
             ConsumerReactor consumerReactor = new ConsumerReactor(
                     new LogContext(),
                     time,
                     applicationEventQueue,
                     applicationEventReaper,
                     () -> applicationEventProcessor,
                     () -> networkClientDelegate,
                     () -> requestManagers,
                     asyncConsumerMetrics
             )) {
            consumerReactor.initializeResources();

            AsyncPollEvent event = new AsyncPollEvent(10, 0);
            event.setEnqueuedMs(time.milliseconds());
            applicationEventQueue.add(event);
            asyncConsumerMetrics.recordApplicationEventQueueSize(1);

            time.sleep(10);
            consumerReactor.runOnce();
            assertEquals(
                0,
                (double) metrics.metric(
                    metrics.metricName("application-event-queue-size", groupName)
                ).metricValue()
            );
            assertEquals(
                10,
                (double) metrics.metric(
                    metrics.metricName("application-event-queue-time-avg", groupName)
                ).metricValue()
            );
            assertEquals(
                10,
                (double) metrics.metric(
                    metrics.metricName("application-event-queue-time-max", groupName)
                ).metricValue()
            );
        }
    }

    @Test
    public void testNetworkClientDelegateInitializeResourcesError() {
        Supplier<NetworkClientDelegate> networkClientDelegateSupplier = () -> {
            throw new KafkaException("Injecting NetworkClientDelegate initialization failure");
        };
        Supplier<RequestManagers> requestManagersSupplier = () -> requestManagers;
        testInitializeResourcesError(networkClientDelegateSupplier, requestManagersSupplier);
    }

    @Test
    public void testRequestManagersInitializeResourcesError() {
        Supplier<NetworkClientDelegate> networkClientDelegateSupplier = () -> networkClientDelegate;
        Supplier<RequestManagers> requestManagersSupplier = () -> {
            throw new KafkaException("Injecting RequestManagers initialization failure");
        };
        testInitializeResourcesError(networkClientDelegateSupplier, requestManagersSupplier);
    }

    @Test
    public void testNetworkClientDelegateAndRequestManagersInitializeResourcesError() {
        Supplier<NetworkClientDelegate> networkClientDelegateSupplier = () -> {
            throw new KafkaException("Injecting NetworkClientDelegate initialization failure");
        };
        Supplier<RequestManagers> requestManagersSupplier = () -> {
            throw new KafkaException("Injecting RequestManagers initialization failure");
        };
        testInitializeResourcesError(networkClientDelegateSupplier, requestManagersSupplier);
    }

    @Test
    public void testProcessEventFailureCompletesFutureExceptionally() {
        RuntimeException processingError = new RuntimeException("Simulated processing failure");
        doThrow(processingError).when(applicationEventProcessor).process(any(ApplicationEvent.class));

        PausePartitionsEvent event = new PausePartitionsEvent(Collections.emptyList(), time.milliseconds() + 1000);
        event.setEnqueuedMs(time.milliseconds());
        applicationEventQueue.add(event);

        consumerReactor.runOnce();

        assertTrue(event.future().isDone(), "Event future should be completed after processing failure");
        assertTrue(event.future().isCompletedExceptionally(), "Event future should be completed exceptionally");

        KafkaException thrown = assertThrows(KafkaException.class, () -> ConsumerUtils.getResult(event.future()));
        assertEquals(processingError, thrown.getCause());
    }

    /**
     * Tests that when an error occurs during {@link ConsumerReactor#initializeResources()} that the
     * logic in {@link ConsumerReactor#cleanup()} will not throw errors when closing.
     */
    private void testInitializeResourcesError(Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                                              Supplier<RequestManagers> requestManagersSupplier) {
        // A new ConsumerReactor is created because the shared one doesn't have any issues initializing its
        // resources. However, most of the mocks can be reused, so this is mostly boilerplate except for the error
        // when a supplier is invoked.
        try (ConsumerReactor thread = new ConsumerReactor(
            new LogContext(),
            time,
            applicationEventQueue,
            applicationEventReaper,
            () -> applicationEventProcessor,
            networkClientDelegateSupplier,
            requestManagersSupplier,
            asyncConsumerMetrics
        )) {
            assertThrows(KafkaException.class, thread::initializeResources, "initializeResources should fail because one or more Supplier throws an error on get()");
            assertDoesNotThrow(thread::cleanup, "cleanup() should not cause an error because all references are checked before use");
        }
    }
}
