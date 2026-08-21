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
import org.apache.kafka.clients.consumer.internals.events.AsyncPollEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.PausePartitionsEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.AbstractRequest;
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

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
        assertEquals(consumerReactor.maximumTimeToWait(), exampleTime);
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
        requestManagers.entries().forEach(rm -> verify(rm).maximumTimeToWait(anyLong()));
        verify(networkClientDelegate, times(list.size())).addAll(anyList());
        verify(networkClientDelegate).poll(anyLong(), anyLong());
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
        assertEquals(startMs + 100L, consumerReactor.reactorSchedule().networkDeadlineMs());
        assertEquals(Long.MAX_VALUE, consumerReactor.reactorSchedule().applicationDeadlineMs());
    }

    @Test
    public void testStateTransitionIsExecutedAfterSchedulePublication() {
        long currentTimeMs = time.milliseconds();
        AtomicLong deadlineObservedByWakeup = new AtomicLong(-1L);
        NetworkClientDelegate.PollResult result = new NetworkClientDelegate.PollResult(
            100L,
            List.of(),
            Set.of(StateTransition.FETCH_BUFFER_HAS_DATA)
        );
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager));
        when(coordinatorRequestManager.poll(currentTimeMs)).thenReturn(result);
        doAnswer(invocation -> {
            deadlineObservedByWakeup.set(consumerReactor.reactorSchedule().networkDeadlineMs());
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        consumerReactor.runOnce();

        assertEquals(currentTimeMs + 100L, deadlineObservedByWakeup.get());
        verify(requestManagers).wakeupApplicationThread();
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
        NetworkClientDelegate.PollResult afterCompletion = new NetworkClientDelegate.PollResult(
            7_000L,
            List.of(),
            Set.of(StateTransition.FETCH_REQUEST_TERMINATED)
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
        doAnswer(invocation -> {
            request.future().complete(null);
            return null;
        }).when(networkClientDelegate).poll(ConsumerReactor.MAX_POLL_TIMEOUT_MS, currentTimeMs);
        doAnswer(invocation -> {
            deadlineObservedByWakeup.set(consumerReactor.reactorSchedule().networkDeadlineMs());
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        consumerReactor.runOnce();

        verify(heartbeatRequestManager, times(2)).poll(currentTimeMs);
        verify(coordinatorRequestManager).poll(currentTimeMs);
        verify(requestManagers).wakeupApplicationThread();
        assertEquals(currentTimeMs + 6_000L, deadlineObservedByWakeup.get());
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
        assertEquals(startMs + 100L, consumerReactor.reactorSchedule().networkDeadlineMs());
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
