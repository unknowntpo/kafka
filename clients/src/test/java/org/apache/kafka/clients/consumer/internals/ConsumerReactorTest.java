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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
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
        when(offsetsRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(offsetsRequestManager.reconcile(anyLong())).thenAnswer(invocation -> {
            long currentTimeMs = invocation.getArgument(0);
            return ManagerReconcileResult.of(
                offsetsRequestManager,
                offsetsRequestManager.poll(currentTimeMs),
                offsetsRequestManager.nextReconcile(currentTimeMs)
            );
        });
        when(heartbeatRequestManager.reconcile(anyLong())).thenCallRealMethod();
        when(coordinatorRequestManager.reconcile(anyLong())).thenCallRealMethod();
        when(offsetsRequestManager.nextReconcile(anyLong())).thenCallRealMethod();
        when(heartbeatRequestManager.nextReconcile(anyLong())).thenCallRealMethod();
        when(coordinatorRequestManager.nextReconcile(anyLong())).thenCallRealMethod();
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
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        consumerReactor.runOnce();
        requestManagers.entries().forEach(rm -> verify(rm).reconcile(anyLong()));
        requestManagers.entries().forEach(rm -> verify(rm).poll(anyLong()));
        requestManagers.entries().forEach(rm -> verify(rm).maximumTimeToWait(anyLong()));
        verify(networkClientDelegate, times(list.size())).addAll(any(NetworkClientDelegate.PollResult.class));
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
    public void testExpiredScheduleIsNotMovedForwardByNetworkPoll() {
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

        assertEquals(Long.MAX_VALUE, consumerReactor.maximumTimeToWait());
        assertEquals(timeBeforePollMs, managerDecisionTimeMs.get());

        consumerReactor.runOnce();

        assertEquals(100L, consumerReactor.maximumTimeToWait());
        assertEquals(timeBeforePollMs + 250L, managerDecisionTimeMs.get());
        assertEquals(managerDecisionTimeMs.get(), consumerReactor.reactorSchedule().decidedAtMs());
    }

    @Test
    public void testNextFullReconciliationRecomputesCrossManagerSchedule() {
        long startMs = time.milliseconds();
        AtomicBoolean coordinatorReady = new AtomicBoolean(false);
        List<String> wakeupSources = new ArrayList<>();
        NextReconcile coordinatorDeadline =
            NextReconcile.atDeadlineAfter(startMs, 100L);
        NextReconcile heartbeatDeadline =
            NextReconcile.atDeadlineAfter(startMs, 30L);

        when(requestManagers.entries()).thenReturn(
            List.of(coordinatorRequestManager, heartbeatRequestManager, offsetsRequestManager)
        );
        when(coordinatorRequestManager.poll(startMs)).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(heartbeatRequestManager.poll(startMs)).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(offsetsRequestManager.poll(startMs)).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(networkClientDelegate.addAll(NetworkClientDelegate.PollResult.EMPTY)).thenReturn(Long.MAX_VALUE);
        doAnswer(invocation ->
            coordinatorReady.get()
                ? NextReconcile.onEvent()
                : coordinatorDeadline
        ).when(coordinatorRequestManager).nextReconcile(anyLong());
        doAnswer(invocation ->
            coordinatorReady.get()
                ? heartbeatDeadline
                : NextReconcile.onEvent()
        ).when(heartbeatRequestManager).nextReconcile(anyLong());
        doReturn(NextReconcile.onEvent())
            .when(offsetsRequestManager).nextReconcile(anyLong());
        doAnswer(invocation -> {
            time.sleep(10L);
            coordinatorReady.set(true);
            return null;
        }).when(networkClientDelegate).poll(100L, startMs);
        doAnswer(invocation -> {
            wakeupSources.add(consumerReactor.reactorSchedule().source().orElseThrow());
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        // The first poll changes a cross-manager dependency which is not typed yet. The next full pre-I/O pass is
        // the correctness fallback and publishes the heartbeat deadline without managers calling each other.
        consumerReactor.runOnce();
        consumerReactor.runOnce();

        verify(networkClientDelegate).poll(100L, startMs);
        verify(coordinatorRequestManager, times(2)).nextReconcile(anyLong());
        verify(heartbeatRequestManager, times(2)).nextReconcile(anyLong());
        verify(offsetsRequestManager, times(2)).nextReconcile(anyLong());
        assertEquals(
            List.of(
                CoordinatorRequestManager.class.getSimpleName(),
                ConsumerHeartbeatRequestManager.class.getSimpleName()
            ),
            wakeupSources
        );
        assertEquals(startMs + 30L, consumerReactor.reactorSchedule().deadlineAtMs());
        assertEquals(20L, consumerReactor.maximumTimeToWait());
    }

    @Test
    public void testShorterScheduleIsPublishedBeforeWakeAction() {
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
    public void testReactorCoalescesStateTransitionAndShorterScheduleIntoOneAction() {
        long currentTimeMs = time.milliseconds();
        AtomicLong timeoutObservedByWakeup = new AtomicLong(-1L);
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.maximumTimeToWait(anyLong())).thenReturn(100L);
        doReturn(ManagerReconcileResult.of(
            heartbeatRequestManager,
            NetworkClientDelegate.PollResult.EMPTY,
            Set.of(StateTransition.FETCH_REQUEST_TERMINATED),
            NextReconcile.atDeadlineAfter(currentTimeMs, 100L)
        )).when(heartbeatRequestManager).reconcile(currentTimeMs);
        doAnswer(invocation -> {
            timeoutObservedByWakeup.set(consumerReactor.maximumTimeToWait());
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        consumerReactor.runOnce();

        assertEquals(100L, timeoutObservedByWakeup.get());
        verify(requestManagers).wakeupApplicationThread();
    }

    @Test
    public void testRequestCompletionReconcilesOnlyAffectedManagerAndRetainsOtherDeadline() {
        long currentTimeMs = time.milliseconds();
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            mock(AbstractRequest.Builder.class),
            Optional.empty()
        );
        ManagerReconcileResult beforeCompletion = ManagerReconcileResult.of(
            heartbeatRequestManager,
            new NetworkClientDelegate.PollResult(request),
            NextReconcile.onEvent()
        );
        ManagerReconcileResult afterCompletion = ManagerReconcileResult.of(
            heartbeatRequestManager,
            NetworkClientDelegate.PollResult.EMPTY,
            Set.of(StateTransition.FETCH_REQUEST_TERMINATED),
            NextReconcile.atDeadlineAfter(currentTimeMs, 7_000L)
        );
        ManagerReconcileResult unaffected = ManagerReconcileResult.of(
            coordinatorRequestManager,
            NetworkClientDelegate.PollResult.EMPTY,
            NextReconcile.atDeadlineAfter(currentTimeMs, 6_000L)
        );
        AtomicLong deadlineObservedByWakeup = new AtomicLong(-1L);

        when(requestManagers.entries()).thenReturn(
            List.of(heartbeatRequestManager, coordinatorRequestManager)
        );
        doReturn(beforeCompletion, afterCompletion)
            .when(heartbeatRequestManager).reconcile(currentTimeMs);
        doReturn(unaffected).when(coordinatorRequestManager).reconcile(currentTimeMs);
        when(networkClientDelegate.addAll(any(NetworkClientDelegate.PollResult.class)))
            .thenReturn(Long.MAX_VALUE);
        doAnswer(invocation -> {
            request.future().complete(null);
            return null;
        }).when(networkClientDelegate).poll(ConsumerReactor.MAX_POLL_TIMEOUT_MS, currentTimeMs);
        doAnswer(invocation -> {
            deadlineObservedByWakeup.set(consumerReactor.reactorSchedule().deadlineAtMs());
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        consumerReactor.runOnce();

        verify(heartbeatRequestManager, times(2)).reconcile(currentTimeMs);
        verify(coordinatorRequestManager).reconcile(currentTimeMs);
        verify(requestManagers).wakeupApplicationThread();
        assertEquals(currentTimeMs + 6_000L, deadlineObservedByWakeup.get());
        assertEquals(
            CoordinatorRequestManager.class.getSimpleName(),
            consumerReactor.reactorSchedule().source().orElseThrow()
        );
    }

    @Test
    public void testReactorAppliesStateTransitionWithoutChangingPublishedSchedule() {
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        doReturn(ManagerReconcileResult.of(
            heartbeatRequestManager,
            NetworkClientDelegate.PollResult.EMPTY,
            Set.of(StateTransition.FETCH_REQUEST_TERMINATED),
            NextReconcile.onEvent()
        )).when(heartbeatRequestManager).reconcile(anyLong());
        doReturn(NextReconcile.onEvent()).when(heartbeatRequestManager).nextReconcile(anyLong());

        consumerReactor.runOnce();

        assertEquals(Long.MAX_VALUE, consumerReactor.maximumTimeToWait());
        verify(requestManagers).wakeupApplicationThread();
    }

    @Test
    public void testReactorPublishesScheduleBeforeExecutingTransitionFromNextReconciliation() {
        long startMs = time.milliseconds();
        AtomicBoolean networkPollCompleted = new AtomicBoolean(false);
        AtomicBoolean transitionReturned = new AtomicBoolean(false);
        AtomicLong decisionTimeObservedByWakeup = new AtomicLong(-1L);
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        doAnswer(invocation ->
            ManagerReconcileResult.of(
                heartbeatRequestManager,
                NetworkClientDelegate.PollResult.EMPTY,
                networkPollCompleted.get() && transitionReturned.compareAndSet(false, true)
                    ? Set.of(StateTransition.FETCH_REQUEST_TERMINATED)
                    : Set.of(),
                NextReconcile.onEvent()
            )
        ).when(heartbeatRequestManager).reconcile(anyLong());
        doReturn(NextReconcile.onEvent()).when(heartbeatRequestManager).nextReconcile(anyLong());
        doAnswer(invocation -> {
            time.sleep(10L);
            networkPollCompleted.set(true);
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());
        doAnswer(invocation -> {
            decisionTimeObservedByWakeup.set(consumerReactor.reactorSchedule().decidedAtMs());
            return null;
        }).when(requestManagers).wakeupApplicationThread();

        consumerReactor.runOnce();
        assertEquals(-1L, decisionTimeObservedByWakeup.get());

        consumerReactor.runOnce();

        assertEquals(startMs + 10L, decisionTimeObservedByWakeup.get());
        verify(requestManagers).wakeupApplicationThread();
    }

    @Test
    public void testSameExpiredDeadlineDoesNotCauseRepeatedWakeups() {
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.nextReconcile(anyLong())).thenAnswer(invocation ->
            NextReconcile.atDeadlineAfter(
                invocation.getArgument(0, Long.class),
                0L
            )
        );

        consumerReactor.runOnce();
        consumerReactor.runOnce();

        verify(requestManagers).wakeupApplicationThread();
    }

    @Test
    public void testExpiredDeadlineDoesNotLeaveZeroReactorSchedule() {
        long startMs = time.milliseconds();
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        doReturn(NextReconcile.atDeadlineAfter(startMs, 0L))
            .when(heartbeatRequestManager).nextReconcile(anyLong());

        consumerReactor.runOnce();

        assertEquals(Long.MAX_VALUE, consumerReactor.maximumTimeToWait());
        assertTrue(consumerReactor.reactorSchedule().deadlineNotificationDelivered());

        consumerReactor.runOnce();

        assertEquals(Long.MAX_VALUE, consumerReactor.maximumTimeToWait());
        verify(requestManagers).wakeupApplicationThread();
    }

    @Test
    public void testLegacyRelativeWaitExpiresBeforeFreshScheduleMovesDeadlineForward() {
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

        assertEquals(Long.MAX_VALUE, consumerReactor.maximumTimeToWait());

        consumerReactor.runOnce();

        verify(networkClientDelegate).poll(100L, startMs);
        // One wake publishes the shorter deadline; the second delivers its expiry before the legacy adapter
        // computes a fresh relative wait during the next reactor iteration.
        verify(requestManagers, times(2)).wakeupApplicationThread();
        assertEquals(100L, consumerReactor.maximumTimeToWait());
        assertEquals(startMs + 200L, consumerReactor.reactorSchedule().deadlineAtMs());
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

        assertEquals(startMs + 100L, consumerReactor.reactorSchedule().deadlineAtMs());
        assertEquals(80L, consumerReactor.maximumTimeToWait());
    }

    @Test
    public void testSameDeadlineFromDifferentSourceIsANewTransition() {
        long startMs = time.milliseconds();
        NextReconcile expired =
            NextReconcile.atDeadlineAfter(startMs, 0L);
        AtomicReference<List<RequestManager>> activeManagers =
            new AtomicReference<>(List.of(heartbeatRequestManager));
        when(requestManagers.entries()).thenAnswer(invocation -> activeManagers.get());
        doReturn(expired).when(heartbeatRequestManager).nextReconcile(anyLong());
        doReturn(expired).when(coordinatorRequestManager).nextReconcile(anyLong());

        consumerReactor.runOnce();
        activeManagers.set(List.of(coordinatorRequestManager));
        consumerReactor.runOnce();

        verify(requestManagers, times(2)).wakeupApplicationThread();
        assertEquals(
            CoordinatorRequestManager.class.getSimpleName(),
            consumerReactor.reactorSchedule().source().orElseThrow()
        );
    }

    @Test
    public void testSameSourceAndDeadlineWithNewGenerationIsANewTransition() {
        long startMs = time.milliseconds();
        NextReconcile first =
            NextReconcile.atDeadlineAfter(startMs, 0L)
                .withSemanticGeneration(1L);
        NextReconcile second =
            NextReconcile.atDeadlineAfter(startMs, 0L)
                .withSemanticGeneration(2L);
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        doReturn(first).doReturn(second)
            .when(heartbeatRequestManager).nextReconcile(anyLong());

        consumerReactor.runOnce();
        consumerReactor.runOnce();

        verify(requestManagers, times(2)).wakeupApplicationThread();
        assertEquals(2L, consumerReactor.reactorSchedule().semanticGeneration());
    }

    @Test
    public void testPersistentZeroCompatibilityWaitDoesNotBusyLoop() {
        long startMs = time.milliseconds();
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(heartbeatRequestManager.maximumTimeToWait(anyLong())).thenReturn(0L);
        when(networkClientDelegate.addAll(NetworkClientDelegate.PollResult.EMPTY)).thenReturn(Long.MAX_VALUE);

        consumerReactor.runOnce();
        time.sleep(1L);
        consumerReactor.runOnce();

        verify(networkClientDelegate).poll(0L, startMs);
        verify(networkClientDelegate).poll(ConsumerReactor.MAX_POLL_TIMEOUT_MS, startMs + 1L);
        verify(requestManagers).wakeupApplicationThread();
        assertEquals(Long.MAX_VALUE, consumerReactor.maximumTimeToWait());
    }

    @Test
    public void testScheduleDeadlineLimitsEveryNetworkPollUntilItIsPublishedAsExpired() {
        long startMs = time.milliseconds();
        NextReconcile deadline =
            NextReconcile.atDeadlineAfter(startMs, 100L);
        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        doReturn(deadline).when(heartbeatRequestManager).nextReconcile(anyLong());
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
    public void testDeadlineWakeupReleasesReactorScheduleUsingOlderSnapshot() throws Exception {
        long startMs = time.milliseconds();
        FetchBuffer fetchBuffer = new FetchBuffer(new LogContext());
        CountDownLatch firstWaitReturned = new CountDownLatch(1);
        CountDownLatch secondWaitStarted = new CountDownLatch(1);
        ExecutorService applicationExecutor = Executors.newSingleThreadExecutor();
        AtomicInteger networkPolls = new AtomicInteger();

        when(requestManagers.entries()).thenReturn(List.of(heartbeatRequestManager));
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        doReturn(NextReconcile.atDeadlineAfter(startMs, 100L))
            .when(heartbeatRequestManager).nextReconcile(anyLong());
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

        Future<?> reactorSchedule = applicationExecutor.submit(() -> {
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

            reactorSchedule.get(DEFAULT_MAX_WAIT_MS, TimeUnit.MILLISECONDS);
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
