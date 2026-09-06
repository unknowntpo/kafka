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

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AsyncPollEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Real admission, event processing, loop and delegate; metadata/manager work are controlled seams. */
@Timeout(10)
class ConsumerAsyncPollMetadataTest {
    private final LogContext logContext = new LogContext();
    private final MockTime time = new MockTime();
    private final ConsumerMetadata metadata = mock(ConsumerMetadata.class);
    private final OffsetsRequestManager offsets = mock(OffsetsRequestManager.class);
    private final FetchRequestManager fetch = mock(FetchRequestManager.class);
    private final TopicMetadataRequestManager topics = mock(TopicMetadataRequestManager.class);
    private final BlockingQueue<ApplicationEvent> inputs = new LinkedBlockingQueue<>();
    private final CompletableFuture<Void> positions = new CompletableFuture<>();
    private final AtomicInteger notifications = new AtomicInteger();
    private final TopicAuthorizationException error = new TopicAuthorizationException(Set.of("topic"));
    private ConsumerNetworkThread thread;
    private NetworkClientDelegate delegate;

    @BeforeEach
    void setup() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        ConsumerConfig config = new ConsumerConfig(properties);
        AsyncConsumerMetrics metrics = mock(AsyncConsumerMetrics.class);
        RequestManagers managers = new RequestManagers(logContext, offsets, topics, fetch,
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty());
        for (RequestManager manager : List.of(offsets, topics, fetch)) {
            when(manager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
            when(manager.pollOnClose(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
            when(manager.maximumTimeToWait(anyLong())).thenReturn(Long.MAX_VALUE);
        }
        when(offsets.updateFetchPositions(anyLong())).thenReturn(positions);
        when(fetch.createFetchRequests()).thenReturn(CompletableFuture.completedFuture(null));
        ApplicationEventProcessor processor = new ApplicationEventProcessor(logContext, managers, metadata,
                new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST));
        delegate = new NetworkClientDelegate(time, config, logContext, new MockClient(time), metadata,
                mock(BackgroundEventHandler.class), false, metrics);
        thread = new ConsumerNetworkThread(logContext, time, inputs, new CompletableEventReaper(logContext),
                () -> processor, () -> delegate, () -> managers, metrics);
        thread.initializeResources();
    }

    @AfterEach
    void close() {
        if (thread != null) {
            thread.close(Duration.ZERO);
            // Tests drive runOnce directly, so its normal run() finally block does not run.
            thread.cleanup();
        }
    }

    private AsyncPollEvent admit(long deadlineMs) {
        AsyncPollEvent event = new AsyncPollEvent(deadlineMs, time.milliseconds(), notifications::incrementAndGet);
        inputs.add(event);
        thread.runOnce();
        return event;
    }

    private void metadataFailureInNextPoll() {
        doThrow(error).doNothing().when(metadata).maybeThrowAnyException();
        thread.runOnce();
    }

    @Test
    void testMetadataErrorAfterAdmissionReachesPendingPoll() {
        AsyncPollEvent event = admit(time.milliseconds() + 60_000);
        assertTrue(event.isValidatePositionsComplete());
        assertFalse(event.isComplete());
        metadataFailureInNextPoll();
        assertTrue(event.isComplete(), "A pending metadata-dependent poll must receive a later I/O error");
        assertSame(error, event.error().orElseThrow());
        assertEquals(1, notifications.get());
        assertTrue(delegate.getAndClearMetadataError().isEmpty());
    }

    @Test
    void testErrorBeforeAdmissionIsStillDelivered() {
        metadataFailureInNextPoll();
        AsyncPollEvent event = admit(time.milliseconds() + 60_000);
        assertSame(error, event.error().orElseThrow());
        assertEquals(1, notifications.get());
        verify(offsets, never()).updateFetchPositions(anyLong());
    }

    @Test
    void testCompletedPollIsNotRetargetedByLaterMetadataError() {
        positions.complete(null);
        AsyncPollEvent completed = admit(time.milliseconds() + 60_000);
        assertTrue(completed.isComplete());
        metadataFailureInNextPoll();
        assertTrue(completed.error().isEmpty());
        assertEquals(0, notifications.get());
        AsyncPollEvent next = admit(time.milliseconds() + 60_000);
        assertSame(error, next.error().orElseThrow());
        assertEquals(1, notifications.get());
    }

    @Test
    void testExpiredPollDoesNotConsumeErrorForNextOperation() {
        AsyncPollEvent expired = admit(time.milliseconds() + 100);
        time.sleep(100);
        metadataFailureInNextPoll();
        assertTrue(expired.error().isEmpty());
        assertEquals(0, notifications.get());
        AsyncPollEvent next = admit(time.milliseconds() + 60_000);
        assertSame(error, next.error().orElseThrow());
        assertEquals(1, notifications.get());
    }

    @Test
    void testExpiredAndLivePollsDoNotShareErrorLifetime() {
        AsyncPollEvent expired = admit(time.milliseconds() + 100);
        AsyncPollEvent live = admit(time.milliseconds() + 60_000);
        time.sleep(100);
        metadataFailureInNextPoll();
        assertTrue(expired.error().isEmpty());
        assertSame(error, live.error().orElseThrow());
        assertEquals(1, notifications.get());
        assertTrue(delegate.getAndClearMetadataError().isEmpty());
    }

    @Test
    void testFreshPollCanSucceedWithoutRevivingFailedPoll() {
        AsyncPollEvent failed = admit(time.milliseconds() + 60_000);
        metadataFailureInNextPoll();
        CompletableFuture<Void> nextPositions = new CompletableFuture<>();
        when(offsets.updateFetchPositions(anyLong())).thenReturn(nextPositions);
        AsyncPollEvent next = admit(time.milliseconds() + 60_000);

        positions.complete(null);
        verify(fetch, never()).createFetchRequests();
        assertFalse(next.isComplete(), "the old operation must not complete its replacement");
        nextPositions.complete(null);
        assertTrue(next.isComplete());
        assertTrue(next.error().isEmpty());
        assertSame(error, failed.error().orElseThrow());
        verify(fetch).createFetchRequests();
        assertEquals(1, notifications.get(), "successful preparation alone is not another error notification");
    }

    @Test
    void testFailedPollDoesNotStartAnotherStageWhenPositionsFinishLater() {
        AsyncPollEvent event = admit(time.milliseconds() + 60_000);
        metadataFailureInNextPoll();
        positions.complete(null);
        verify(fetch, never()).createFetchRequests();
        assertSame(error, event.error().orElseThrow());
        assertEquals(1, notifications.get());
    }

    @Test
    void testPendingMetadataDependentsAreNotReplacedByLatestPoll() {
        AsyncPollEvent first = admit(time.milliseconds() + 60_000);
        AsyncPollEvent second = admit(time.milliseconds() + 60_000);
        metadataFailureInNextPoll();
        assertSame(error, first.error().orElseThrow());
        assertSame(error, second.error().orElseThrow());
        assertEquals(2, notifications.get());
        doNothing().when(metadata).maybeThrowAnyException();
        thread.runOnce();
        assertEquals(2, notifications.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRoutedMetadataErrorPublishesBeforeLatchedWaitRelease(boolean alreadyWaiting) throws Exception {
        try (FetchBuffer buffer = new FetchBuffer(logContext)) {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AsyncPollEvent event = new AsyncPollEvent(time.milliseconds() + 60_000, time.milliseconds(), buffer::wakeup);
            inputs.add(event);
            thread.runOnce();
            Thread waiter = new Thread(() -> {
                try {
                    buffer.awaitWakeup(Time.SYSTEM.timer(30_000));
                    assertTrue(event.isComplete());
                    assertSame(error, event.error().orElseThrow());
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "async-poll-metadata-waiter");
            try {
                if (alreadyWaiting) {
                    waiter.start();
                    TestUtils.waitForCondition(() -> waiter.getState() == Thread.State.TIMED_WAITING,
                            2_000, "application did not enter buffer wait");
                }
                metadataFailureInNextPoll();
                if (!alreadyWaiting)
                    waiter.start();
                waiter.join(2_000);
                assertFalse(waiter.isAlive(), "the error must release the wait, not rely on its timeout");
                assertNull(failure.get());
            } finally {
                buffer.wakeup();
                waiter.interrupt();
                waiter.join(2_000);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAdmissionErrorCanBeObservedBeforeAggregatePublication(boolean alreadyWaiting) throws Exception {
        // With no eligible event, the real delegate retains the error for the next input boundary.
        metadataFailureInNextPoll();
        assertEquals(Long.MAX_VALUE, thread.maximumTimeToWait());
        when(fetch.maximumTimeToWait(anyLong())).thenReturn(42L);
        try (FetchBuffer buffer = new FetchBuffer(logContext)) {
            CountDownLatch observedError = new CountDownLatch(1);
            CountDownLatch enterWait = new CountDownLatch(alreadyWaiting ? 0 : 1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AsyncPollEvent event = new AsyncPollEvent(time.milliseconds() + 60_000, time.milliseconds(), () -> {
                buffer.wakeup();
                enterWait.countDown();
                try {
                    assertTrue(observedError.await(2, TimeUnit.SECONDS), "error observation waited for the aggregate");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });
            Thread waiter = new Thread(() -> {
                try {
                    assertTrue(enterWait.await(2, TimeUnit.SECONDS));
                    buffer.awaitWakeup(Time.SYSTEM.timer(30_000));
                    assertTrue(event.isComplete());
                    assertSame(error, event.error().orElseThrow());
                    // This error outcome needs its event state, not the later manager-wait projection.
                    assertEquals(Long.MAX_VALUE, thread.maximumTimeToWait());
                } catch (Throwable t) {
                    failure.set(t);
                } finally {
                    observedError.countDown();
                }
            }, "pre-publication-error-observer");
            try {
                waiter.start();
                if (alreadyWaiting)
                    TestUtils.waitForCondition(() -> waiter.getState() == Thread.State.TIMED_WAITING,
                            2_000, "application did not enter buffer wait");
                inputs.add(event);
                thread.runOnce();
                waiter.join(2_000);
                assertFalse(waiter.isAlive());
                assertNull(failure.get());
                assertEquals(42L, thread.maximumTimeToWait());
                assertSame(error, event.error().orElseThrow());
                verify(offsets, never()).updateFetchPositions(anyLong());
            } finally {
                observedError.countDown();
                enterWait.countDown();
                buffer.wakeup();
                waiter.interrupt();
                waiter.join(2_000);
            }
        }
    }

    @Test
    void testLaterFetchFailureCannotOverwriteDeliveredMetadataError() {
        CompletableFuture<Void> preparation = new CompletableFuture<>();
        when(fetch.createFetchRequests()).thenReturn(preparation);
        positions.complete(null);
        AsyncPollEvent event = admit(time.milliseconds() + 60_000);
        assertFalse(event.isComplete());
        metadataFailureInNextPoll();
        preparation.completeExceptionally(new IllegalStateException("later preparation failure"));
        assertSame(error, event.error().orElseThrow());
        assertEquals(1, notifications.get());
    }

    @Test
    void testSynchronousProcessingFailureCompletesAsyncPoll() {
        when(offsets.updateFetchPositions(anyLong())).thenThrow(error);
        AsyncPollEvent event = admit(time.milliseconds() + 60_000);
        assertTrue(event.isComplete());
        assertSame(error, event.error().orElseThrow());
        assertEquals(1, notifications.get());
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void testNewFetchValidationFailureDoesNotDependOnFutureTiming(boolean throwsBeforeReturning, boolean positionsAlreadyReady) {
        // Extension probe: a manager may reject new work before returning its future, or complete
        // the returned future exceptionally. Application delivery must not depend on that choice.
        if (throwsBeforeReturning)
            when(fetch.createFetchRequests()).thenThrow(error);
        else
            when(fetch.createFetchRequests()).thenReturn(CompletableFuture.failedFuture(error));
        if (positionsAlreadyReady)
            positions.complete(null);

        AsyncPollEvent event = admit(time.milliseconds() + 60_000);
        if (!positionsAlreadyReady) {
            assertFalse(event.isComplete());
            positions.complete(null);
        }
        assertTrue(event.isComplete(), "a validation error must not disappear into an unobserved dependent future");
        assertSame(error, event.error().orElseThrow());
        assertEquals(1, notifications.get());
        verify(fetch).createFetchRequests();
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void testFetchRejectionPreservesExistingTimeoutPolicy(boolean throwsBeforeReturning, boolean kafkaTimeout) {
        Throwable timeout = kafkaTimeout ? new org.apache.kafka.common.errors.TimeoutException("preparation expired")
                : new java.util.concurrent.TimeoutException("preparation expired");
        if (throwsBeforeReturning) {
            // Answer permits exercising either timeout family without declaring a checked throws clause.
            when(fetch.createFetchRequests()).thenAnswer(ignored -> {
                throw timeout;
            });
        } else {
            when(fetch.createFetchRequests()).thenReturn(CompletableFuture.failedFuture(timeout));
        }
        AsyncPollEvent event = admit(time.milliseconds() + 60_000);
        positions.complete(null);
        assertTrue(event.isComplete());
        assertTrue(event.error().isEmpty(), "the adapter must not wrap the cause and change timeout classification");
        assertEquals(0, notifications.get());
        verify(fetch).createFetchRequests();
    }
}
