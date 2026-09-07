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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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

    @Test
    public void testEarlierInputDrivenScheduleIsPublishedBeforeLatchedWake() throws InterruptedException {
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager));
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(networkClientDelegate.addAll(any(NetworkClientDelegate.PollResult.class))).thenReturn(Long.MAX_VALUE);
        when(coordinatorRequestManager.maximumTimeToWait(anyLong())).thenReturn(Long.MAX_VALUE, 0L, 0L);
        try (FetchBuffer buffer = new FetchBuffer(new LogContext())) {
            java.util.concurrent.atomic.AtomicInteger wakes = new java.util.concurrent.atomic.AtomicInteger();
            consumerNetworkThread.setScheduleWakeup(() -> {
                assertEquals(0, consumerNetworkThread.maximumTimeToWait());
                wakes.incrementAndGet();
                buffer.wakeup();
            });
            consumerNetworkThread.runOnce();
            assertEquals(0, wakes.get());
            consumerNetworkThread.runOnce();
            assertEquals(1, wakes.get());
            long before = time.milliseconds();
            buffer.awaitWakeup(time.timer(60_000));
            assertEquals(before, time.milliseconds(), "wake must survive arrival before wait entry");
            consumerNetworkThread.runOnce();
            assertEquals(1, wakes.get(), "an unchanged expired obligation must not produce wakeup ping-pong");
        }
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    public void testEarlyFetchWakeAndAggregateSchedule(boolean notifySchedule, boolean enterWaitBeforePublication) throws Exception {
        AtomicLong heartbeatWait = new AtomicLong(30_000);
        when(requestManagers.entries()).thenReturn(List.of(offsetsRequestManager, heartbeatRequestManager));
        when(offsetsRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(offsetsRequestManager.maximumTimeToWait(anyLong())).thenReturn(60_000L);
        when(heartbeatRequestManager.maximumTimeToWait(anyLong())).thenAnswer(ignored -> heartbeatWait.get());
        when(networkClientDelegate.addAll(any(NetworkClientDelegate.PollResult.class))).thenReturn(Long.MAX_VALUE);
        consumerNetworkThread.runOnce();
        assertEquals(30_000L, consumerNetworkThread.maximumTimeToWait());

        try (FetchBuffer buffer = new FetchBuffer(new LogContext())) {
            FetchBufferProducer producer = new FetchBufferProducer(buffer);
            producer.requestStarted(1);
            CountDownLatch earlyWakeConsumed = new CountDownLatch(1);
            CountDownLatch enterSecondWait = new CountDownLatch(enterWaitBeforePublication ? 0 : 1);
            AtomicBoolean returnedFromSecondWait = new AtomicBoolean();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicInteger scheduleWakes = new AtomicInteger();
            Thread application = new Thread(() -> {
                try {
                    buffer.awaitWakeup(time.timer(60_000));
                    assertFalse(producer.hasPendingRequests(), "local completion state must precede the first wake");
                    long oldWait = consumerNetworkThread.maximumTimeToWait();
                    assertEquals(30_000L, oldWait, "the I/O batch has not published its aggregate yet");
                    earlyWakeConsumed.countDown();
                    assertTrue(enterSecondWait.await(2, TimeUnit.SECONDS));
                    buffer.awaitWakeup(time.timer(oldWait));
                    assertEquals(0L, consumerNetworkThread.maximumTimeToWait());
                    returnedFromSecondWait.set(true);
                } catch (Throwable t) {
                    failure.set(t);
                } finally {
                    earlyWakeConsumed.countDown();
                }
            }, "early-fetch-wake-application");

            consumerNetworkThread.setScheduleWakeup(() -> {
                assertEquals(0L, consumerNetworkThread.maximumTimeToWait(), "publish aggregate before notifying");
                scheduleWakes.incrementAndGet();
                if (notifySchedule)
                    buffer.wakeup();
            });
            when(networkClientDelegate.completedRequestsInLastPoll()).thenReturn(true);
            doAnswer(ignored -> {
                producer.requestCompleted(1);
                assertTrue(earlyWakeConsumed.await(2, TimeUnit.SECONDS));
                assertNull(failure.get());
                if (enterWaitBeforePublication)
                    TestUtils.waitForCondition(() -> application.getState() == Thread.State.TIMED_WAITING,
                            2_000, "application did not re-enter its old wait");
                // Model a later completion in the same I/O batch changing another manager's contribution.
                heartbeatWait.set(0);
                return null;
            }).when(networkClientDelegate).poll(anyLong(), anyLong());

            try {
                application.start();
                consumerNetworkThread.runOnce();
                assertEquals(1, scheduleWakes.get());
                enterSecondWait.countDown();
                if (notifySchedule) {
                    application.join(2_000);
                    assertFalse(application.isAlive());
                    assertTrue(returnedFromSecondWait.get());
                } else {
                    // Negative control: publishing the bound alone cannot release an existing/stale wait.
                    TestUtils.waitForCondition(() -> application.getState() == Thread.State.TIMED_WAITING,
                            2_000, "negative control did not remain in the old wait");
                    assertFalse(returnedFromSecondWait.get());
                }
                assertNull(failure.get());
                if (!notifySchedule) {
                    // Release the negative-control waiter explicitly before checking a later deadline.
                    buffer.wakeup();
                    application.join(2_000);
                    assertFalse(application.isAlive());
                    assertTrue(returnedFromSecondWait.get());
                    assertNull(failure.get());
                }
                heartbeatWait.set(Long.MAX_VALUE);
                doAnswer(ignored -> null).when(networkClientDelegate).poll(anyLong(), anyLong());
                consumerNetworkThread.runOnce();
                assertEquals(60_000L, consumerNetworkThread.maximumTimeToWait());
                assertEquals(1, scheduleWakes.get(), "a later deadline does not require a schedule wake");
            } finally {
                enterSecondWait.countDown();
                buffer.wakeup();
                application.join(2_000);
                if (application.isAlive()) {
                    application.interrupt();
                    application.join(2_000);
                }
                assertFalse(application.isAlive(), "test must not leak its application waiter");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS - 1, ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS, ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS + 1})
    public void testConsumerNetworkThreadPollTimeComputations(long exampleTime) {
        List<RequestManager> list = List.of(coordinatorRequestManager, heartbeatRequestManager);
        when(requestManagers.entries()).thenReturn(list);

        NetworkClientDelegate.PollResult pollResult = new NetworkClientDelegate.PollResult(exampleTime);
        NetworkClientDelegate.PollResult pollResult1 = new NetworkClientDelegate.PollResult(exampleTime + 100);

        long t = time.milliseconds();
        when(coordinatorRequestManager.poll(t)).thenReturn(pollResult);
        when(coordinatorRequestManager.maximumTimeToWait(t)).thenReturn(exampleTime);
        when(heartbeatRequestManager.poll(t)).thenReturn(pollResult1);
        when(heartbeatRequestManager.maximumTimeToWait(t)).thenReturn(exampleTime + 100);
        when(networkClientDelegate.addAll(pollResult)).thenReturn(pollResult.timeUntilNextPollMs);
        when(networkClientDelegate.addAll(pollResult1)).thenReturn(pollResult1.timeUntilNextPollMs);
        consumerNetworkThread.runOnce();

        verify(networkClientDelegate).poll(Math.min(exampleTime, ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS), time.milliseconds());
        assertEquals(consumerNetworkThread.maximumTimeToWait(), exampleTime);
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
        requestManagers.entries().forEach(rm -> verify(rm).maximumTimeToWait(anyLong()));
        verify(networkClientDelegate).addAll(any(NetworkClientDelegate.PollResult.class));
        verify(networkClientDelegate).poll(anyLong(), anyLong());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testOnePostIoDecisionPassOnlyForACompletedBatch(boolean completedBatch) {
        List<String> phases = new ArrayList<>();
        long beforeIo = time.milliseconds();
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager));
        when(coordinatorRequestManager.poll(anyLong())).thenAnswer(invocation -> {
            phases.add("decide:" + invocation.getArgument(0));
            return NetworkClientDelegate.PollResult.EMPTY;
        });
        when(networkClientDelegate.addAll(any(NetworkClientDelegate.PollResult.class))).thenReturn(Long.MAX_VALUE);
        doAnswer(ignored -> {
            phases.add("io");
            time.sleep(25);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());
        when(networkClientDelegate.completedRequestsInLastPoll()).thenReturn(completedBatch);
        when(coordinatorRequestManager.maximumTimeToWait(anyLong())).thenAnswer(ignored -> {
            phases.add("wait");
            return 10L;
        });
        consumerNetworkThread.setScheduleWakeup(() -> {
            assertEquals(10L, consumerNetworkThread.maximumTimeToWait());
            phases.add("wake");
        });

        consumerNetworkThread.runOnce();
        List<String> expected = new ArrayList<>(List.of("decide:" + beforeIo, "io"));
        if (completedBatch)
            expected.add("decide:" + (beforeIo + 25));
        expected.addAll(List.of("wait", "wake"));
        assertEquals(expected, phases);
        verify(networkClientDelegate, times(1)).poll(anyLong(), anyLong());
        verify(coordinatorRequestManager, times(completedBatch ? 2 : 1)).poll(anyLong());
    }

    @Test
    public void testCloseObservedDuringIoSkipsPostBatchAdmission() {
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager));
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(networkClientDelegate.completedRequestsInLastPoll()).thenReturn(true);
        doAnswer(ignored -> {
            // The thread is not started: simulate the close flag becoming visible while I/O runs.
            consumerNetworkThread.close(Duration.ZERO);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());

        consumerNetworkThread.runOnce();
        assertFalse(consumerNetworkThread.isRunning());
        verify(coordinatorRequestManager, times(1)).poll(anyLong());
        verify(networkClientDelegate, times(1)).poll(anyLong(), anyLong());
    }

    @Test
    public void testApplicationInputQueuedDuringIoPrecedesAnotherDecisionPass() {
        PausePartitionsEvent event = new PausePartitionsEvent(List.of(), Long.MAX_VALUE);
        when(requestManagers.entries()).thenReturn(List.of(coordinatorRequestManager));
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(networkClientDelegate.completedRequestsInLastPoll()).thenReturn(true);
        doAnswer(ignored -> {
            applicationEventQueue.add(event);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());

        consumerNetworkThread.runOnce();
        verify(coordinatorRequestManager, times(1)).poll(anyLong());
        assertEquals(List.of(event), List.copyOf(applicationEventQueue));

        doAnswer(ignored -> null).when(networkClientDelegate).poll(anyLong(), anyLong());
        when(networkClientDelegate.completedRequestsInLastPoll()).thenReturn(false);
        List<String> order = new ArrayList<>();
        doAnswer(ignored -> {
            order.add("input");
            return null;
        }).when(applicationEventProcessor).process(event);
        when(coordinatorRequestManager.poll(anyLong())).thenAnswer(ignored -> {
            order.add("decision");
            return NetworkClientDelegate.PollResult.EMPTY;
        });
        consumerNetworkThread.runOnce();
        assertEquals(List.of("input", "decision"), order);
        assertTrue(applicationEventQueue.isEmpty());
    }

    @Test
    public void testMaximumTimeToWait() {
        final int defaultHeartbeatIntervalMs = 1000;
        // Initial value before runOnce has been called
        assertEquals(ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS, consumerNetworkThread.maximumTimeToWait());

        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.maximumTimeToWait(time.milliseconds())).thenReturn((long) defaultHeartbeatIntervalMs);

        consumerNetworkThread.runOnce();
        // After runOnce has been called, it takes the default heartbeat interval from the heartbeat request manager
        assertEquals(defaultHeartbeatIntervalMs, consumerNetworkThread.maximumTimeToWait());
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
