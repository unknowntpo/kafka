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
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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
        when(offsetsRequestManager.progressIntent(anyLong())).thenCallRealMethod();
        when(heartbeatRequestManager.progressIntent(anyLong())).thenCallRealMethod();
        when(coordinatorRequestManager.progressIntent(anyLong())).thenCallRealMethod();
        when(requestManagers.drainApplicationProgressEffects()).thenReturn(Set.of());
        consumerReactor.initializeResources();
    }

    @AfterEach
    public void tearDown() {
        if (consumerReactor != null)
            consumerReactor.close();
    }

    @Test
    public void testEnsureCloseStopsReactor() {
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
        when(networkClientDelegate.addAll(pollResult)).thenReturn(pollResult.timeUntilNextPollMs);
        when(networkClientDelegate.addAll(pollResult1)).thenReturn(pollResult1.timeUntilNextPollMs);
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
                "The consumer reactor did not start within " + DEFAULT_MAX_WAIT_MS + " ms");

        consumerReactor.close(Duration.ofMillis(DEFAULT_MAX_WAIT_MS));

        TestUtils.waitForCondition(isClosed,
                "The consumer reactor did not stop within " + DEFAULT_MAX_WAIT_MS + " ms");
    }

    @Test
    public void testRequestsTransferFromManagersToClientOnReactorRun() {
        List<RequestManager> list = List.of(coordinatorRequestManager, heartbeatRequestManager, offsetsRequestManager);

        when(requestManagers.entries()).thenReturn(list);
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(mock(NetworkClientDelegate.PollResult.class));
        consumerReactor.runOnce();
        requestManagers.entries().forEach(rm -> verify(rm).poll(anyLong()));
        requestManagers.entries().forEach(rm -> verify(rm, times(2)).maximumTimeToWait(anyLong()));
        verify(networkClientDelegate).addAll(any(NetworkClientDelegate.PollResult.class));
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
    public void testApplicationWaitDecisionUsesTimeAfterNetworkPoll() {
        long timeBeforePollMs = time.milliseconds();
        AtomicLong managerDecisionTimeMs = new AtomicLong(-1L);
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.poll(timeBeforePollMs)).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(networkClientDelegate.addAll(NetworkClientDelegate.PollResult.EMPTY)).thenReturn(Long.MAX_VALUE);
        doAnswer(invocation -> {
            time.sleep(250L);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), eq(timeBeforePollMs));
        when(heartbeatRequestManager.maximumTimeToWait(anyLong())).thenAnswer(invocation -> {
            managerDecisionTimeMs.set(invocation.getArgument(0, Long.class));
            return 100L;
        });

        consumerReactor.runOnce();

        assertEquals(100L, consumerReactor.maximumTimeToWait());
        assertEquals(timeBeforePollMs + 250L, managerDecisionTimeMs.get());
        assertEquals(managerDecisionTimeMs.get(), consumerReactor.applicationWait().decidedAtMs());
    }

    @Test
    public void testShorterDecisionIsPublishedBeforeApplicationWakeup() {
        AtomicLong timeoutObservedByWakeup = new AtomicLong(-1L);
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.maximumTimeToWait(anyLong())).thenReturn(100L);
        doAnswer(invocation -> {
            timeoutObservedByWakeup.set(consumerReactor.maximumTimeToWait());
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        consumerReactor.runOnce();

        assertEquals(100L, timeoutObservedByWakeup.get());
        verify(requestManagers).wakeupApplicationThread();
    }

    @Test
    public void testReactorCoalescesProgressEffectWithShorterDecisionWakeup() {
        AtomicLong timeoutObservedByWakeup = new AtomicLong(-1L);
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.maximumTimeToWait(anyLong())).thenReturn(100L);
        doReturn(Set.of(ConsumerReactorProgress.ApplicationProgressEffect.FETCH_REQUEST_TERMINATED))
            .doReturn(Set.of())
            .when(requestManagers).drainApplicationProgressEffects();
        doAnswer(invocation -> {
            timeoutObservedByWakeup.set(consumerReactor.maximumTimeToWait());
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        consumerReactor.runOnce();

        assertEquals(100L, timeoutObservedByWakeup.get());
        verify(requestManagers).wakeupApplicationThread();
    }

    @Test
    public void testReactorAppliesProgressEffectWithoutChangingPublishedWait() {
        when(requestManagers.entries()).thenReturn(List.of());
        doReturn(Set.of(ConsumerReactorProgress.ApplicationProgressEffect.FETCH_REQUEST_TERMINATED))
            .doReturn(Set.of())
            .when(requestManagers).drainApplicationProgressEffects();

        consumerReactor.runOnce();

        assertEquals(Long.MAX_VALUE, consumerReactor.maximumTimeToWait());
        verify(requestManagers).wakeupApplicationThread();
    }

    @Test
    public void testReactorPublishesPostPollDecisionBeforeApplyingNetworkProgressEffect() {
        long startMs = time.milliseconds();
        AtomicBoolean networkPollCompleted = new AtomicBoolean(false);
        AtomicBoolean effectDelivered = new AtomicBoolean(false);
        AtomicLong decisionTimeObservedByWakeup = new AtomicLong(-1L);
        when(requestManagers.entries()).thenReturn(List.of());
        when(requestManagers.drainApplicationProgressEffects()).thenAnswer(invocation -> {
            if (networkPollCompleted.get() && effectDelivered.compareAndSet(false, true)) {
                return Set.of(ConsumerReactorProgress.ApplicationProgressEffect.FETCH_REQUEST_TERMINATED);
            }
            return Set.of();
        });
        doAnswer(invocation -> {
            time.sleep(10L);
            networkPollCompleted.set(true);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());
        doAnswer(invocation -> {
            decisionTimeObservedByWakeup.set(consumerReactor.applicationWait().decidedAtMs());
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        consumerReactor.runOnce();

        assertEquals(startMs + 10L, decisionTimeObservedByWakeup.get());
        verify(requestManagers).wakeupApplicationThread();
    }

    @Test
    public void testEachExpiredDeadlineWakesOnceEvenWhenLaterThanPreviousDeadline() {
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.progressIntent(anyLong())).thenAnswer(invocation ->
            ConsumerReactorProgress.ProgressIntent.awaitDeadlineAfter(
                invocation.getArgument(0, Long.class),
                0L
            )
        );

        consumerReactor.runOnce();
        consumerReactor.runOnce();
        time.sleep(100L);
        consumerReactor.runOnce();

        verify(requestManagers, times(2)).wakeupApplicationThread();
    }

    @Test
    public void testExpiredDeadlineDoesNotLeaveZeroApplicationWait() {
        long startMs = time.milliseconds();
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        doReturn(ConsumerReactorProgress.ProgressIntent.awaitDeadlineAfter(startMs, 0L))
            .when(heartbeatRequestManager).progressIntent(anyLong());

        consumerReactor.runOnce();

        assertEquals(Long.MAX_VALUE, consumerReactor.maximumTimeToWait());
        assertTrue(consumerReactor.applicationWait().deadlineNotificationDelivered());

        consumerReactor.runOnce();

        assertEquals(Long.MAX_VALUE, consumerReactor.maximumTimeToWait());
        verify(requestManagers).wakeupApplicationThread();
    }

    @Test
    public void testLegacyRelativeWaitExpiresBeforeFreshDecisionMovesDeadlineForward() {
        long startMs = time.milliseconds();
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.poll(startMs)).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(heartbeatRequestManager.maximumTimeToWait(anyLong())).thenReturn(100L);
        when(networkClientDelegate.addAll(NetworkClientDelegate.PollResult.EMPTY)).thenReturn(Long.MAX_VALUE);
        doAnswer(invocation -> {
            time.sleep(invocation.getArgument(0, Long.class));
            return null;
        }).when(networkClientDelegate).poll(anyLong(), eq(startMs));

        consumerReactor.runOnce();

        verify(networkClientDelegate).poll(100L, startMs);
        // One wake publishes the shorter deadline; the second delivers its expiry before the legacy adapter
        // computes a fresh relative wait from the later time.
        verify(requestManagers, times(2)).wakeupApplicationThread();
        assertEquals(100L, consumerReactor.maximumTimeToWait());
        assertEquals(startMs + 200L, consumerReactor.applicationWait().deadlineAtMs());
    }

    @Test
    public void testLegacyRelativeWaitDoesNotDriftAcrossEarlyNetworkReturns() {
        long startMs = time.milliseconds();
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(heartbeatRequestManager.maximumTimeToWait(anyLong())).thenReturn(100L);
        when(networkClientDelegate.addAll(NetworkClientDelegate.PollResult.EMPTY)).thenReturn(Long.MAX_VALUE);
        doAnswer(invocation -> {
            time.sleep(10L);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());

        consumerReactor.runOnce();
        consumerReactor.runOnce();

        assertEquals(startMs + 100L, consumerReactor.applicationWait().deadlineAtMs());
        assertEquals(80L, consumerReactor.maximumTimeToWait());
    }

    @Test
    public void testSameDeadlineFromDifferentSourceIsANewTransition() {
        long startMs = time.milliseconds();
        ConsumerReactorProgress.ProgressIntent expired =
            ConsumerReactorProgress.ProgressIntent.awaitDeadlineAfter(startMs, 0L);
        AtomicReference<List<RequestManager>> activeManagers =
            new AtomicReference<>(List.of(heartbeatRequestManager));
        when(requestManagers.entries()).thenAnswer(invocation -> activeManagers.get());
        doReturn(expired).when(heartbeatRequestManager).progressIntent(anyLong());
        doReturn(expired).when(coordinatorRequestManager).progressIntent(anyLong());

        consumerReactor.runOnce();
        activeManagers.set(List.of(coordinatorRequestManager));
        consumerReactor.runOnce();

        verify(requestManagers, times(2)).wakeupApplicationThread();
        assertEquals(
            CoordinatorRequestManager.class.getSimpleName(),
            consumerReactor.applicationWait().source().orElseThrow()
        );
    }

    @Test
    public void testSameSourceAndDeadlineWithNewGenerationIsANewTransition() {
        long startMs = time.milliseconds();
        ConsumerReactorProgress.ProgressIntent first =
            ConsumerReactorProgress.ProgressIntent.awaitDeadlineAfter(startMs, 0L)
                .withSemanticGeneration(1L);
        ConsumerReactorProgress.ProgressIntent second =
            ConsumerReactorProgress.ProgressIntent.awaitDeadlineAfter(startMs, 0L)
                .withSemanticGeneration(2L);
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        doReturn(first).doReturn(first).doReturn(second).doReturn(second)
            .when(heartbeatRequestManager).progressIntent(anyLong());

        consumerReactor.runOnce();
        consumerReactor.runOnce();

        verify(requestManagers, times(2)).wakeupApplicationThread();
        assertEquals(2L, consumerReactor.applicationWait().semanticGeneration());
    }

    @Test
    public void testProgressDeadlineLimitsEveryNetworkPollUntilItIsPublishedAsExpired() {
        long startMs = time.milliseconds();
        ConsumerReactorProgress.ProgressIntent deadline =
            ConsumerReactorProgress.ProgressIntent.awaitDeadlineAfter(startMs, 100L);
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        doReturn(deadline).when(heartbeatRequestManager).progressIntent(anyLong());
        when(networkClientDelegate.addAll(NetworkClientDelegate.PollResult.EMPTY)).thenReturn(Long.MAX_VALUE);

        consumerReactor.runOnce();
        consumerReactor.runOnce();
        time.sleep(100L);
        consumerReactor.runOnce();
        consumerReactor.runOnce();

        verify(networkClientDelegate, times(2)).poll(100L, startMs);
        verify(networkClientDelegate).poll(0L, startMs + 100L);
        verify(networkClientDelegate).poll(ConsumerReactor.MAX_POLL_TIMEOUT_MS, startMs + 100L);
    }

    @Test
    public void testDeadlineWakeupReleasesApplicationWaitUsingOlderSnapshot() throws Exception {
        long startMs = time.milliseconds();
        FetchBuffer fetchBuffer = new FetchBuffer(new LogContext());
        CountDownLatch firstWaitReturned = new CountDownLatch(1);
        CountDownLatch secondWaitStarted = new CountDownLatch(1);
        ExecutorService applicationExecutor = Executors.newSingleThreadExecutor();
        AtomicInteger networkPolls = new AtomicInteger();

        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        doReturn(ConsumerReactorProgress.ProgressIntent.awaitDeadlineAfter(startMs, 100L))
            .when(heartbeatRequestManager).progressIntent(anyLong());
        when(networkClientDelegate.addAll(NetworkClientDelegate.PollResult.EMPTY)).thenReturn(Long.MAX_VALUE);
        doAnswer(invocation -> {
            if (networkPolls.incrementAndGet() == 2)
                time.sleep(100L);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());
        doAnswer(invocation -> {
            fetchBuffer.wakeup();
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        Future<?> applicationWait = applicationExecutor.submit(() -> {
            // The caller may already be blocked with an older snapshot and cannot read the newly published
            // maximumTimeToWait() until it is released. Both publication of a shorter deadline and expiry of that
            // deadline must therefore produce retained fetch-buffer wakeups.
            fetchBuffer.awaitWakeup(Time.SYSTEM.timer(DEFAULT_MAX_WAIT_MS));
            firstWaitReturned.countDown();
            secondWaitStarted.countDown();
            fetchBuffer.awaitWakeup(Time.SYSTEM.timer(DEFAULT_MAX_WAIT_MS));
        });

        try {
            consumerReactor.runOnce();
            assertTrue(firstWaitReturned.await(DEFAULT_MAX_WAIT_MS, TimeUnit.MILLISECONDS));
            assertTrue(secondWaitStarted.await(DEFAULT_MAX_WAIT_MS, TimeUnit.MILLISECONDS));

            consumerReactor.runOnce();

            applicationWait.get(DEFAULT_MAX_WAIT_MS, TimeUnit.MILLISECONDS);
            verify(requestManagers, times(2)).wakeupApplicationThread();
            assertEquals(2, networkPolls.get());
        } finally {
            fetchBuffer.wakeup();
            applicationExecutor.shutdownNow();
        }
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
