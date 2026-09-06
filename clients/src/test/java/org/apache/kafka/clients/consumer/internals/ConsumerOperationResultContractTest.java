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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AsyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.CreateFetchRequestsEvent;
import org.apache.kafka.clients.consumer.internals.events.SyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Real input loop, event processor, delegate and reaper; manager outcomes are controlled extension seams. */
@Timeout(10)
class ConsumerOperationResultContractTest {
    enum Route { FETCH, ASYNC_COMMIT, SYNC_COMMIT }
    enum Outcome { THROW, FAILED_READY, FAILED_LATER, SUCCESS_READY, SUCCESS_LATER }

    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    private final Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(new TopicPartition("result-contract", 0), new OffsetAndMetadata(7));
    private final KafkaException rejection = new KafkaException("new manager validation rejected this operation");
    private final BlockingQueue<ApplicationEvent> inputs = new LinkedBlockingQueue<>();
    private final AtomicInteger invocations = new AtomicInteger();
    private final CompletableEventReaper reaper = new CompletableEventReaper(logContext);
    private CompletableFuture<Object> ownerResult = new CompletableFuture<>();
    private boolean rejectByThrow;
    private ConsumerNetworkThread loop;

    @BeforeEach
    void setup() {
        ConsumerMetadata metadata = mock(ConsumerMetadata.class);
        OffsetsRequestManager positions = mock(OffsetsRequestManager.class);
        TopicMetadataRequestManager topics = mock(TopicMetadataRequestManager.class);
        FetchRequestManager fetch = mock(FetchRequestManager.class);
        CommitRequestManager commit = mock(CommitRequestManager.class);
        RequestManagers managers = new RequestManagers(logContext, positions, topics, fetch,
                Optional.empty(), Optional.of(commit), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty());
        for (RequestManager manager : managers.entries()) {
            when(manager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
            when(manager.pollOnClose(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
            when(manager.maximumTimeToWait(anyLong())).thenReturn(Long.MAX_VALUE);
        }
        when(fetch.createFetchRequests()).thenAnswer(ignored -> invoke());
        when(commit.commitAsync(eq(offsets))).thenAnswer(ignored -> invoke());
        when(commit.commitSync(eq(offsets), anyLong())).thenAnswer(ignored -> invoke());
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        AsyncConsumerMetrics metrics = mock(AsyncConsumerMetrics.class);
        NetworkClientDelegate delegate = new NetworkClientDelegate(time, new ConsumerConfig(properties), logContext,
                new MockClient(time), metadata, mock(BackgroundEventHandler.class), false, metrics);
        ApplicationEventProcessor processor = new ApplicationEventProcessor(logContext, managers, metadata,
                new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST));
        loop = new ConsumerNetworkThread(logContext, time, inputs, reaper,
                () -> processor, () -> delegate, () -> managers, metrics);
        loop.initializeResources();
    }

    private CompletableFuture<Object> invoke() {
        invocations.incrementAndGet();
        if (rejectByThrow)
            throw rejection;
        return ownerResult;
    }

    private CompletableApplicationEvent<?> event(Route route) {
        switch (route) {
            case FETCH:
                return new CreateFetchRequestsEvent(time.milliseconds() + 100);
            case ASYNC_COMMIT:
                return new AsyncCommitEvent(Optional.of(offsets));
            case SYNC_COMMIT:
                return new SyncCommitEvent(Optional.of(offsets), time.milliseconds() + 100);
            default:
                throw new AssertionError(route);
        }
    }

    private Object result(Route route) {
        return route == Route.FETCH ? null : offsets;
    }

    private void admit(CompletableApplicationEvent<?> event) {
        inputs.add(event);
        loop.runOnce();
    }

    private Throwable failure(CompletableApplicationEvent<?> event) {
        assertTrue(event.future().isDone());
        return event.future().handle((value, error) -> error).join();
    }

    @AfterEach
    void cleanup() {
        if (loop != null) {
            loop.close(Duration.ZERO);
            // Direct runOnce tests do not enter the background thread's run/finally block.
            loop.cleanup();
            loop = null;
        }
    }

    static Stream<Arguments> outcomes() {
        return Arrays.stream(Route.values()).flatMap(route -> Arrays.stream(Outcome.values())
                .map(outcome -> Arguments.of(route, outcome)));
    }

    @ParameterizedTest
    @MethodSource("outcomes")
    void testExistingResultPathDoesNotNeedAnEffectTimingChoice(Route route, Outcome outcome) {
        CompletableApplicationEvent<?> event = event(route);
        AtomicInteger observed = new AtomicInteger();
        CompletableFuture<?> observer = event.future().whenComplete((value, error) -> {
            assertTrue(event.future().isDone());
            observed.incrementAndGet();
        });
        rejectByThrow = outcome == Outcome.THROW;
        if (outcome == Outcome.FAILED_READY)
            ownerResult.completeExceptionally(rejection);
        if (outcome == Outcome.SUCCESS_READY)
            ownerResult.complete(result(route));
        admit(event);
        if (outcome == Outcome.FAILED_LATER || outcome == Outcome.SUCCESS_LATER) {
            assertFalse(event.future().isDone());
            if (outcome == Outcome.FAILED_LATER)
                ownerResult.completeExceptionally(rejection);
            else
                ownerResult.complete(result(route));
        }
        assertEquals(1, invocations.get());
        if (outcome == Outcome.SUCCESS_READY || outcome == Outcome.SUCCESS_LATER)
            assertSame(result(route), event.future().join());
        else
            assertSame(rejection, failure(event));
        assertTrue(observer.isDone());
        assertEquals(1, observed.get());
        loop.runOnce();
        assertEquals(0, reaper.size());
    }

    static Stream<Arguments> lateOutcomes() {
        return Arrays.stream(Route.values()).flatMap(route -> Stream.of(false, true)
                .map(failed -> Arguments.of(route, failed)));
    }

    private void finishOwner(Route route, boolean failed) {
        if (failed)
            assertTrue(ownerResult.completeExceptionally(rejection));
        else
            assertTrue(ownerResult.complete(result(route)));
    }

    @ParameterizedTest
    @MethodSource("lateOutcomes")
    void testCancellingEventWaitDoesNotCancelOwnerOrAcceptLateResult(Route route, boolean lateFailure) {
        CompletableApplicationEvent<?> event = event(route);
        admit(event);
        assertTrue(event.future().cancel(false));
        Throwable cancellation = failure(event);
        assertInstanceOf(CancellationException.class, cancellation);
        assertFalse(ownerResult.isDone());
        finishOwner(route, lateFailure);
        assertSame(cancellation, failure(event));
        loop.runOnce();
        assertEquals(0, reaper.size());
    }

    @ParameterizedTest
    @CsvSource({"FETCH,false", "FETCH,true", "SYNC_COMMIT,false", "SYNC_COMMIT,true"})
    void testDeadlineRetiresObserverWithoutRewritingOwnerOutcome(Route route, boolean lateFailure) {
        CompletableApplicationEvent<?> event = event(route);
        admit(event);
        time.sleep(100);
        loop.runOnce();
        Throwable timeout = failure(event);
        assertInstanceOf(TimeoutException.class, timeout);
        assertEquals(0, reaper.size());
        assertFalse(ownerResult.isDone());
        finishOwner(route, lateFailure);
        assertSame(timeout, failure(event));
    }

    @ParameterizedTest
    @MethodSource("lateOutcomes")
    void testCleanupRetiresAdmittedObserverBeforeLateOwnerResult(Route route, boolean lateFailure) {
        CompletableApplicationEvent<?> event = event(route);
        admit(event);
        cleanup();
        Throwable closed = failure(event);
        assertInstanceOf(TimeoutException.class, closed);
        assertFalse(ownerResult.isDone());
        finishOwner(route, lateFailure);
        assertSame(closed, failure(event));
        assertEquals(0, reaper.size());
    }

    @ParameterizedTest
    @EnumSource(Route.class)
    void testCleanupDoesNotInvokeQueuedOperation(Route route) {
        CompletableApplicationEvent<?> event = event(route);
        inputs.add(event);
        cleanup();
        assertInstanceOf(TimeoutException.class, failure(event));
        assertEquals(0, invocations.get());
        assertTrue(inputs.isEmpty());
        assertEquals(0, reaper.size());
    }

    @Test
    void testAsyncCommitHasNoInventedFiniteApplicationDeadline() {
        CompletableApplicationEvent<?> event = event(Route.ASYNC_COMMIT);
        admit(event);
        time.sleep(10_000);
        loop.runOnce();
        assertFalse(event.future().isDone());
        assertEquals(Long.MAX_VALUE, event.deadlineMs());
        ownerResult.complete(offsets);
        assertSame(offsets, event.future().join());
    }

    @ParameterizedTest
    @EnumSource(Route.class)
    void testCompletionCanQueueIndependentFollowupWithoutRecursiveAdmission(Route route) {
        CompletableApplicationEvent<?> first = event(route);
        CompletableApplicationEvent<?> next = event(route);
        CompletableFuture<?> queued = first.future().thenRun(() -> inputs.add(next));
        admit(first);
        ownerResult.complete(result(route));
        queued.join();
        assertEquals(1, invocations.get());
        assertFalse(next.future().isDone());
        ownerResult = new CompletableFuture<>();
        loop.runOnce();
        assertEquals(2, invocations.get());
        assertFalse(next.future().isDone());
        ownerResult.complete(result(route));
        assertSame(result(route), next.future().join());
    }
}
