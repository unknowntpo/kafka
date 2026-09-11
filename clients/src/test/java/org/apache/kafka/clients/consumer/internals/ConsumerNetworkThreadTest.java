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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerNetworkThreadTest {
    private final Time time;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final ApplicationEventProcessor applicationEventProcessor;
    private final OffsetsRequestManager offsetsRequestManager;
    private final ConsumerHeartbeatRequestManager heartbeatRequestManager;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final ConsumerNetworkThread consumerNetworkThread;
    private final NetworkClientDelegate networkClientDelegate;
    private final RequestManagers requestManagers;
    private final CompletableEventReaper applicationEventReaper;
    private final AsyncConsumerMetrics asyncConsumerMetrics;

    ConsumerNetworkThreadTest() {
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

        this.consumerNetworkThread = new ConsumerNetworkThread(
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
        consumerNetworkThread.initializeResources();
        when(coordinatorRequestManager.nextPollCondition(anyLong())).thenReturn(NextPollCondition.ready());
        when(heartbeatRequestManager.nextPollCondition(anyLong())).thenReturn(NextPollCondition.ready());
        when(offsetsRequestManager.nextPollCondition(anyLong())).thenReturn(NextPollCondition.ready());
        when(coordinatorRequestManager.applicationPollCondition(anyLong())).thenReturn(NextPollCondition.idle());
        when(heartbeatRequestManager.applicationPollCondition(anyLong())).thenReturn(NextPollCondition.idle());
        when(offsetsRequestManager.applicationPollCondition(anyLong())).thenReturn(NextPollCondition.idle());
    }

    @AfterEach
    public void tearDown() {
        if (consumerNetworkThread != null)
            consumerNetworkThread.close();
    }

    @Test
    public void testEnsureCloseStopsRunningThread() {
        assertTrue(consumerNetworkThread.isRunning(),
            "ConsumerNetworkThread should start running when created");

        consumerNetworkThread.close();
        assertFalse(consumerNetworkThread.isRunning(),
            "close() should make consumerNetworkThread.running false by calling closeInternal(Duration timeout)");
    }

    @ParameterizedTest
    @ValueSource(longs = {ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS - 1, ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS, ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS + 1})
    public void testConsumerNetworkThreadPollTimeComputations(long exampleTime) {
        List<RequestManager> list = List.of(coordinatorRequestManager, heartbeatRequestManager);
        when(requestManagers.entries()).thenReturn(list);

        NetworkClientDelegate.PollResult pollResult = NetworkClientDelegate.PollResult.EMPTY;
        NetworkClientDelegate.PollResult pollResult1 = NetworkClientDelegate.PollResult.EMPTY;

        long t = time.milliseconds();
        when(coordinatorRequestManager.poll(t)).thenReturn(pollResult);
        when(coordinatorRequestManager.applicationPollCondition(t))
            .thenReturn(NextPollCondition.after(t, exampleTime));
        when(heartbeatRequestManager.poll(t)).thenReturn(pollResult1);
        when(heartbeatRequestManager.applicationPollCondition(t))
            .thenReturn(NextPollCondition.after(t, exampleTime + 100));
        when(coordinatorRequestManager.nextPollCondition(anyLong()))
            .thenReturn(NextPollCondition.ready(), NextPollCondition.after(t, exampleTime));
        when(heartbeatRequestManager.nextPollCondition(anyLong()))
            .thenReturn(NextPollCondition.ready(), NextPollCondition.after(t, exampleTime + 100));
        consumerNetworkThread.runOnce();

        verify(networkClientDelegate).poll(Math.min(exampleTime, ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS), time.milliseconds());
        assertEquals(applicationWaitMs(), exampleTime);
    }

    @ParameterizedTest
    @ValueSource(longs = {40, 140})
    public void testManagerWorkConsumesNetworkWaitBudget(long processingMs) {
        long startMs = time.milliseconds();
        NetworkClientDelegate.PollResult result = NetworkClientDelegate.PollResult.EMPTY;
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager, heartbeatRequestManager));
        when(coordinatorRequestManager.poll(startMs)).thenAnswer(invocation -> {
            time.sleep(processingMs);
            return result;
        });
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(coordinatorRequestManager.nextPollCondition(anyLong()))
            .thenReturn(NextPollCondition.ready(), NextPollCondition.after(startMs, 100));
        when(heartbeatRequestManager.nextPollCondition(anyLong()))
            .thenReturn(NextPollCondition.ready(), NextPollCondition.idle());

        consumerNetworkThread.runOnce();

        verify(heartbeatRequestManager).poll(startMs + processingMs);
        verify(networkClientDelegate).poll(Math.max(0, 100 - processingMs), startMs + processingMs);
    }

    @Test
    public void testLaterManagerEnablesEarlierManagerBeforeNetworkWait() {
        AtomicBoolean earlierReady = new AtomicBoolean();
        AtomicBoolean laterReady = new AtomicBoolean(true);
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager, heartbeatRequestManager));
        when(coordinatorRequestManager.nextPollCondition(anyLong())).thenAnswer(invocation ->
            earlierReady.get() ? NextPollCondition.ready() : NextPollCondition.idle());
        when(heartbeatRequestManager.nextPollCondition(anyLong())).thenAnswer(invocation ->
            laterReady.get() ? NextPollCondition.ready() : NextPollCondition.idle());
        when(heartbeatRequestManager.poll(anyLong())).thenAnswer(invocation -> {
            laterReady.set(false);
            earlierReady.set(true);
            return NetworkClientDelegate.PollResult.EMPTY;
        });
        when(coordinatorRequestManager.poll(anyLong())).thenAnswer(invocation -> {
            earlierReady.set(false);
            return NetworkClientDelegate.PollResult.EMPTY;
        });

        consumerNetworkThread.runOnce();
        verify(coordinatorRequestManager, never()).poll(anyLong());
        verify(networkClientDelegate).poll(0, time.milliseconds());

        consumerNetworkThread.runOnce();
        verify(coordinatorRequestManager).poll(anyLong());
        verify(heartbeatRequestManager).poll(anyLong());
        verify(networkClientDelegate).poll(ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS, time.milliseconds());
    }

    @Test
    public void testApplicationInputEnablesQuiescentManager() {
        AtomicBoolean ready = new AtomicBoolean();
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager));
        when(coordinatorRequestManager.nextPollCondition(anyLong())).thenAnswer(invocation ->
            ready.get() ? NextPollCondition.ready() : NextPollCondition.idle());
        when(coordinatorRequestManager.poll(anyLong())).thenAnswer(invocation -> {
            ready.set(false);
            return NetworkClientDelegate.PollResult.EMPTY;
        });
        consumerNetworkThread.runOnce();
        verify(coordinatorRequestManager, never()).poll(anyLong());

        AsyncPollEvent event = new AsyncPollEvent(time.milliseconds() + 100, time.milliseconds());
        doAnswer(invocation -> {
            ready.set(true);
            event.completeSuccessfully();
            return null;
        }).when(applicationEventProcessor).process(event);
        applicationEventQueue.add(event);
        consumerNetworkThread.runOnce();

        verify(coordinatorRequestManager).poll(anyLong());
        assertTrue(event.isComplete());
        assertFalse(ready.get());
    }

    @Test
    public void testApplicationWaitAndEventExpiryUsePostNetworkTime() {
        long startMs = time.milliseconds();
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        doAnswer(invocation -> {
            time.sleep(30);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());
        when(heartbeatRequestManager.applicationPollCondition(startMs + 30)).thenAnswer(invocation -> {
            time.sleep(40);
            return NextPollCondition.after(startMs + 30, 100L);
        });

        consumerNetworkThread.runOnce();

        verify(heartbeatRequestManager).applicationPollCondition(startMs + 30);
        assertEquals(100, applicationWaitMs());
        verify(applicationEventReaper).reap(startMs + 70);
        time.sleep(20);
        assertEquals(100, applicationWaitMs());
        time.sleep(60);
        assertEquals(100, applicationWaitMs());
    }

    @Test
    public void testNoApplicationDeadlineRemainsUnbounded() {
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.applicationPollCondition(anyLong())).thenReturn(NextPollCondition.idle());

        consumerNetworkThread.runOnce();

        assertEquals(Long.MAX_VALUE, applicationWaitMs());
    }

    @Test
    public void testStartupAndTearDown() throws InterruptedException {
        consumerNetworkThread.start();
        TestCondition isStarted = consumerNetworkThread::isRunning;
        TestCondition isClosed = () -> !(consumerNetworkThread.isRunning() || consumerNetworkThread.isAlive());

        // There's a nonzero amount of time between starting the thread and having it
        // begin to execute our code. Wait for a bit before checking...
        TestUtils.waitForCondition(isStarted,
                "The consumer network thread did not start within " + DEFAULT_MAX_WAIT_MS + " ms");

        consumerNetworkThread.close(Duration.ofMillis(DEFAULT_MAX_WAIT_MS));

        TestUtils.waitForCondition(isClosed,
                "The consumer network thread did not stop within " + DEFAULT_MAX_WAIT_MS + " ms");
    }

    @Test
    public void testRequestsTransferFromManagersToClientOnThreadRun() {
        List<RequestManager> list = List.of(coordinatorRequestManager, heartbeatRequestManager, offsetsRequestManager);

        when(requestManagers.entries()).thenReturn(list);
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(mock(NetworkClientDelegate.PollResult.class));
        consumerNetworkThread.runOnce();
        requestManagers.entries().forEach(rm -> verify(rm).poll(anyLong()));
        requestManagers.entries().forEach(rm -> verify(rm).applicationPollCondition(anyLong()));
        verify(networkClientDelegate).addAll(any(NetworkClientDelegate.PollResult.class));
        verify(networkClientDelegate).poll(anyLong(), anyLong());
    }

    @Test
    public void testMaximumTimeToWait() {
        final int defaultHeartbeatIntervalMs = 1000;
        // Initial value before runOnce has been called
        assertEquals(ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS, applicationWaitMs());

        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.applicationPollCondition(time.milliseconds()))
            .thenReturn(NextPollCondition.after(time.milliseconds(), defaultHeartbeatIntervalMs));

        consumerNetworkThread.runOnce();
        // After runOnce has been called, it takes the default heartbeat interval from the heartbeat request manager
        assertEquals(defaultHeartbeatIntervalMs, applicationWaitMs());
    }

    private long applicationWaitMs() {
        long currentTimeMs = time.milliseconds();
        return consumerNetworkThread.applicationPollCondition().remainingMs(currentTimeMs);
    }

    @Test
    public void testCleanupInvokesReaper() {
        LinkedList<NetworkClientDelegate.UnsentRequest> queue = new LinkedList<>();
        when(networkClientDelegate.unsentRequests()).thenReturn(queue);
        when(applicationEventReaper.reap(applicationEventQueue)).thenReturn(1L);
        consumerNetworkThread.cleanup();
        verify(applicationEventReaper).reap(applicationEventQueue);
        verify(asyncConsumerMetrics).recordApplicationEventExpiredSize(1L);
    }

    @Test
    public void testRunOnceInvokesReaper() {
        when(applicationEventReaper.reap(any(Long.class))).thenReturn(1L);
        consumerNetworkThread.runOnce();
        verify(applicationEventReaper).reap(any(Long.class));
        verify(asyncConsumerMetrics).recordApplicationEventExpiredSize(1L);
    }

    @Test
    public void testSendUnsentRequests() {
        when(networkClientDelegate.hasAnyPendingRequests()).thenReturn(true).thenReturn(true).thenReturn(false);
        consumerNetworkThread.cleanup();
        verify(networkClientDelegate, times(2)).poll(anyLong(), anyLong(), eq(true));
    }

    @ParameterizedTest
    @MethodSource("org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetricsTest#groupNameProvider")
    public void testRunOnceRecordTimeBetweenNetworkThreadPoll(String groupName) {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, groupName);
             ConsumerNetworkThread consumerNetworkThread = new ConsumerNetworkThread(
                     new LogContext(),
                     time,
                     applicationEventQueue,
                     applicationEventReaper,
                     () -> applicationEventProcessor,
                     () -> networkClientDelegate,
                     () -> requestManagers,
                     asyncConsumerMetrics
             )) {
            consumerNetworkThread.initializeResources();

            consumerNetworkThread.runOnce();
            time.sleep(10);
            consumerNetworkThread.runOnce();
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
             ConsumerNetworkThread consumerNetworkThread = new ConsumerNetworkThread(
                     new LogContext(),
                     time,
                     applicationEventQueue,
                     applicationEventReaper,
                     () -> applicationEventProcessor,
                     () -> networkClientDelegate,
                     () -> requestManagers,
                     asyncConsumerMetrics
             )) {
            consumerNetworkThread.initializeResources();

            AsyncPollEvent event = new AsyncPollEvent(10, 0);
            event.setEnqueuedMs(time.milliseconds());
            applicationEventQueue.add(event);
            asyncConsumerMetrics.recordApplicationEventQueueSize(1);

            time.sleep(10);
            consumerNetworkThread.runOnce();
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

        consumerNetworkThread.runOnce();

        assertTrue(event.future().isDone(), "Event future should be completed after processing failure");
        assertTrue(event.future().isCompletedExceptionally(), "Event future should be completed exceptionally");

        KafkaException thrown = assertThrows(KafkaException.class, () -> ConsumerUtils.getResult(event.future()));
        assertEquals(processingError, thrown.getCause());
    }

    /**
     * Tests that when an error occurs during {@link ConsumerNetworkThread#initializeResources()} that the
     * logic in {@link ConsumerNetworkThread#cleanup()} will not throw errors when closing.
     */
    private void testInitializeResourcesError(Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                                              Supplier<RequestManagers> requestManagersSupplier) {
        // A new ConsumerNetworkThread is created because the shared one doesn't have any issues initializing its
        // resources. However, most of the mocks can be reused, so this is mostly boilerplate except for the error
        // when a supplier is invoked.
        try (ConsumerNetworkThread thread = new ConsumerNetworkThread(
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
