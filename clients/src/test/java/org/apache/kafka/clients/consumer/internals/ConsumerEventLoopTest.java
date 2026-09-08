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
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.FetchPositionsErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.pipeline.PassDecision;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Drives {@link ConsumerEventLoop#runOnce()} on the test thread. These cases pin down the triggers the previous
 * implementation covered implicitly by polling every request manager on every iteration, which the event-driven
 * loop has to track explicitly.
 */
public class ConsumerEventLoopTest {

    private static final TopicPartition TP = new TopicPartition("topic", 0);

    private final MockTime time = new MockTime(0);
    private final LogContext logContext = new LogContext();
    private final AtomicInteger metadataVersion = new AtomicInteger(1);
    private ConsumerMetadata metadata;
    private SubscriptionState subscriptions;
    private ApplicationEventProcessor processor;
    private OffsetsRequestManager offsetsRequestManager;
    private TopicMetadataRequestManager topicMetadataRequestManager;
    private FetchRequestManager fetchRequestManager;
    private RequestManagers requestManagers;
    private final AtomicInteger applicationWakeups = new AtomicInteger();
    private LinkedBlockingQueue<BackgroundEvent> backgroundQueue;
    private NetworkClientDelegate networkClientDelegate;
    private ConsumerEventLoop loop;
    private CompletableFuture<Void> positionsFuture;

    @BeforeEach
    public void setup() {
        metadata = mock(ConsumerMetadata.class);
        when(metadata.updateVersion()).thenAnswer(invocation -> metadataVersion.get());
        subscriptions = mock(SubscriptionState.class);
        when(subscriptions.hasAllFetchPositions()).thenReturn(true);
        processor = mock(ApplicationEventProcessor.class);
        offsetsRequestManager = mock(OffsetsRequestManager.class);
        when(offsetsRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        positionsFuture = CompletableFuture.completedFuture(null);
        when(offsetsRequestManager.updateFetchPositions(anyLong())).thenAnswer(invocation -> positionsFuture);
        topicMetadataRequestManager = mock(TopicMetadataRequestManager.class);
        when(topicMetadataRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        fetchRequestManager = mock(FetchRequestManager.class);
        when(fetchRequestManager.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(fetchRequestManager.createFetchRequests()).thenReturn(CompletableFuture.completedFuture(null));
        backgroundQueue = new LinkedBlockingQueue<>();
        AsyncConsumerMetrics asyncConsumerMetrics = mock(AsyncConsumerMetrics.class);
        BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(backgroundQueue, time, asyncConsumerMetrics);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Node node = new Node(0, "localhost", 9092);
        MockClient client = new MockClient(time, Collections.singletonList(node));
        networkClientDelegate = new NetworkClientDelegate(time, new ConsumerConfig(properties), logContext, client, metadata,
                backgroundEventHandler, false, asyncConsumerMetrics);

        requestManagers = new RequestManagers(logContext, offsetsRequestManager, topicMetadataRequestManager,
                fetchRequestManager, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        loop = new ConsumerEventLoop(logContext, time, 1_000, subscriptions, metadata, () -> processor,
                () -> networkClientDelegate, () -> requestManagers, backgroundEventHandler,
                asyncConsumerMetrics, applicationWakeups::incrementAndGet);
        loop.initializeResources();
    }

    @AfterEach
    public void tearDown() throws Exception {
        networkClientDelegate.close();
    }

    private static CompletableApplicationEvent<Void> eventWithDeadline(long deadlineMs) {
        return new CompletableApplicationEvent<>(ApplicationEvent.Type.CHECK_AND_UPDATE_POSITIONS, deadlineMs) {
            @Override
            protected String toStringBase() {
                return super.toStringBase();
            }
        };
    }

    @Test
    public void commandIsProcessedAndManagersRerun() {
        clearInvocations(topicMetadataRequestManager);
        CompletableApplicationEvent<Void> event = eventWithDeadline(time.milliseconds() + 10_000);
        loop.add(event);
        loop.runOnce();
        verify(processor).process(event);
        verify(topicMetadataRequestManager, atLeastOnce()).poll(anyLong());
    }

    @Test
    public void eventWithExpiredDeadlineFailsInTheSamePassEvenIfTheClockDoesNotAdvance() {
        CompletableApplicationEvent<Void> event = eventWithDeadline(time.milliseconds());
        loop.add(event);
        loop.runOnce();
        assertTrue(event.future().isCompletedExceptionally(), "expired at enqueue time: must not wait for the reaper timer");
        ExecutionException e = assertThrows(ExecutionException.class, () -> event.future().get());
        assertInstanceOf(TimeoutException.class, e.getCause());
    }

    @Test
    public void idlePassDoesNotRerunManagers() {
        loop.runOnce();
        clearInvocations(topicMetadataRequestManager, offsetsRequestManager);
        loop.runOnce();
        loop.runOnce();
        verify(topicMetadataRequestManager, never()).poll(anyLong());
        verify(offsetsRequestManager, never()).poll(anyLong());
    }

    @Test
    public void metadataChangeRerunsManagersAndRevalidatesPositions() {
        loop.onApplicationPoll(time.milliseconds());
        loop.runOnce();
        clearInvocations(topicMetadataRequestManager, offsetsRequestManager);

        metadataVersion.incrementAndGet();
        loop.runOnce();
        verify(topicMetadataRequestManager, atLeastOnce()).poll(anyLong());
        verify(offsetsRequestManager).updateFetchPositions(anyLong());
    }

    @Test
    public void positionsAreRetriedOnEachPollWhileMissing() {
        when(subscriptions.hasAllFetchPositions()).thenReturn(false);
        loop.onApplicationPoll(time.milliseconds());
        loop.runOnce();
        loop.onApplicationPoll(time.milliseconds()); // same clock value: must still count as a new poll
        loop.runOnce();
        // At least one attempt per poll (a manager pass may add another); never zero while positions are missing.
        verify(offsetsRequestManager, org.mockito.Mockito.atLeast(2)).updateFetchPositions(anyLong());
    }

    @Test
    public void positionUpdateFailureReachesTheApplicationAsAFetchPositionsErrorEvent() {
        when(subscriptions.hasAllFetchPositions()).thenReturn(false);
        positionsFuture = new CompletableFuture<>();
        loop.onApplicationPoll(time.milliseconds());
        loop.runOnce();
        positionsFuture.completeExceptionally(new java.util.concurrent.CompletionException(new InvalidTopicException("bad")));
        BackgroundEvent event = backgroundQueue.poll();
        assertInstanceOf(FetchPositionsErrorEvent.class, event, "dropped by the application thread if positions are complete by then");
        assertInstanceOf(InvalidTopicException.class, ((ErrorEvent) event).error());
    }

    @Test
    public void storedMetadataErrorIsSurfacedToTheApplication() {
        doThrow(new InvalidTopicException("bad topic")).when(metadata).maybeThrowAnyException();
        loop.runOnce(); // the delegate stores the error during its poll
        loop.runOnce(); // the next pass propagates it
        BackgroundEvent event = backgroundQueue.poll();
        assertInstanceOf(ErrorEvent.class, event);
        assertInstanceOf(InvalidTopicException.class, ((ErrorEvent) event).error());
    }

    @Test
    public void closeWithZeroTimeoutReturnsPromptly() {
        ConsumerEventLoop threaded = new ConsumerEventLoop(logContext, time, 1_000, subscriptions, metadata, () -> processor,
                () -> networkClientDelegate, () -> requestManagers,
                new BackgroundEventHandler(backgroundQueue, time, mock(AsyncConsumerMetrics.class)),
                mock(AsyncConsumerMetrics.class), () -> { });
        threaded.start(5_000);
        long start = System.nanoTime();
        threaded.close(Duration.ZERO);
        assertFalse(threaded.isAlive());
        assertTrue(System.nanoTime() - start < Duration.ofSeconds(5).toNanos());
        assertEquals(0, backgroundQueue.size());
    }
    @Test
    public void positionsAttemptThatCompletesWithoutProgressDoesNotRerunManagersOnItsOwn() {
        // A partition lacks a position and every attempt completes at once (unknown leader, reset back-off): the
        // loop must wait for a poll, a metadata change or a manager trigger, not spin through manager passes.
        when(subscriptions.hasAllFetchPositions()).thenReturn(false);
        loop.onApplicationPoll(time.milliseconds());
        loop.runOnce();
        clearInvocations(topicMetadataRequestManager, offsetsRequestManager);
        loop.runOnce();
        loop.runOnce();
        verify(topicMetadataRequestManager, never()).poll(anyLong());
        verify(offsetsRequestManager, never()).updateFetchPositions(anyLong());
    }

    @Test
    public void positionsAttemptWithRequestsOutstandingTriggersAManagerPass() {
        when(subscriptions.hasAllFetchPositions()).thenReturn(false);
        loop.onApplicationPoll(time.milliseconds());
        loop.runOnce();
        // A manager pass (here: triggered by a command) makes a new attempt, which this time queues requests.
        positionsFuture = new CompletableFuture<>();
        loop.add(eventWithDeadline(time.milliseconds() + 1_000));
        loop.runOnce();
        clearInvocations(topicMetadataRequestManager, offsetsRequestManager);
        loop.runOnce();
        verify(offsetsRequestManager, times(1)).poll(anyLong());
    }

    @Test
    public void managerTimerIsRearmedAfterPollThrows() {
        loop.runOnce();
        when(topicMetadataRequestManager.poll(anyLong()))
            .thenThrow(new IllegalStateException("transient failure"))
            .thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        time.sleep(ManagerTask.MAX_INTERVAL_MS);
        assertThrows(IllegalStateException.class, () -> loop.runOnce());
        clearInvocations(topicMetadataRequestManager);
        loop.runOnce();
        verify(topicMetadataRequestManager, never()).poll(anyLong());
        time.sleep(ManagerTask.FAILURE_RETRY_MS);
        loop.runOnce();
        verify(topicMetadataRequestManager, times(1)).poll(anyLong());
    }

    @Test
    public void aManagerThatThrowsDoesNotStopTheOthersInTheSamePass() {
        loop.runOnce();
        when(offsetsRequestManager.poll(anyLong())).thenThrow(new IllegalStateException("boom"));
        clearInvocations(topicMetadataRequestManager);
        loop.add(eventWithDeadline(time.milliseconds() + 1_000));
        assertThrows(IllegalStateException.class, () -> loop.runOnce());
        verify(topicMetadataRequestManager, times(1)).poll(anyLong());
    }

    @Test
    public void dueTimerAndDirtyPassRunAManagerOnceInTheSamePass() {
        loop.runOnce();
        clearInvocations(offsetsRequestManager, topicMetadataRequestManager);
        time.sleep(ManagerTask.MAX_INTERVAL_MS);
        loop.onApplicationPoll(time.milliseconds());
        loop.runOnce();
        verify(topicMetadataRequestManager, times(1)).poll(anyLong());
        verify(offsetsRequestManager, times(1)).poll(anyLong());
    }

    /**
     * A completion re-runs every manager, not only the sender: managers depend on each other through undeclared
     * future chains (an OffsetFetch completing in the commit manager queues a ListOffsets in the offsets manager).
     */
    @Test
    public void aCompletedRequestRerunsEveryManager() {
        NetworkClientDelegate.UnsentRequest unsent = new NetworkClientDelegate.UnsentRequest(
            new MetadataRequest.Builder(List.of("topic"), false), Optional.of(new Node(0, "localhost", 9092)));
        when(offsetsRequestManager.poll(anyLong()))
            .thenReturn(new NetworkClientDelegate.PollResult(Long.MAX_VALUE, List.of(unsent)))
            .thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        loop.runOnce();
        clearInvocations(offsetsRequestManager, topicMetadataRequestManager);
        loop.runOnce();
        verify(offsetsRequestManager, never()).poll(anyLong());

        unsent.future().completeExceptionally(new RuntimeException("simulated completion"));
        loop.runOnce();
        verify(offsetsRequestManager, times(1)).poll(anyLong());
        verify(topicMetadataRequestManager, times(1)).poll(anyLong());
    }
    @Test
    public void fetchRequestsAreCreatedOncePerApplicationPollAndNotBeforeTheFirstPoll() {
        loop.runOnce();
        verify(fetchRequestManager, never()).createFetchRequests();
        loop.onApplicationPoll(time.milliseconds());
        loop.runOnce();
        verify(fetchRequestManager, times(1)).createFetchRequests();
        verify(fetchRequestManager, atLeastOnce()).poll(anyLong());
        loop.runOnce();
        verify(fetchRequestManager, times(1)).createFetchRequests();
        loop.requestFetch();
        loop.runOnce();
        verify(fetchRequestManager, times(2)).createFetchRequests();
    }

    @Test
    public void decisionIsPublishedEveryPassAndItsVersionAdvancesOnlyOnInputsWithIdentity() {
        loop.runOnce();
        PassDecision first = loop.latestDecision();
        assertEquals(1, first.pass);
        loop.runOnce();
        PassDecision idle = loop.latestDecision();
        assertEquals(2, idle.pass);
        assertEquals(first.stateVersion, idle.stateVersion, "an idle pass consumed no input");

        loop.add(eventWithDeadline(time.milliseconds() + 1_000));
        loop.runOnce();
        long afterCommand = loop.latestDecision().stateVersion;
        assertTrue(afterCommand > idle.stateVersion, "a command is an input with identity");

        metadataVersion.incrementAndGet();
        loop.runOnce();
        long afterMetadata = loop.latestDecision().stateVersion;
        assertTrue(afterMetadata > afterCommand, "a metadata change is an input with identity");

        NetworkClientDelegate.UnsentRequest unsent = new NetworkClientDelegate.UnsentRequest(
            new MetadataRequest.Builder(List.of("topic"), false), Optional.of(new Node(0, "localhost", 9092)));
        when(offsetsRequestManager.poll(anyLong()))
            .thenReturn(new NetworkClientDelegate.PollResult(Long.MAX_VALUE, List.of(unsent)))
            .thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        loop.add(eventWithDeadline(time.milliseconds() + 1_000));
        loop.runOnce();
        long beforeCompletion = loop.latestDecision().stateVersion;
        unsent.future().completeExceptionally(new RuntimeException("simulated completion"));
        loop.runOnce();
        assertTrue(loop.latestDecision().stateVersion > beforeCompletion, "a request completion is an input with identity");
    }

    @Test
    public void applicationIsWokenOnlyWhenADecisionFieldItMayWaitForChanges() {
        loop.runOnce();
        int afterFirst = applicationWakeups.get();
        loop.runOnce();
        loop.runOnce();
        assertEquals(afterFirst, applicationWakeups.get(), "idle passes do not wake the application");

        when(subscriptions.hasAllFetchPositions()).thenReturn(false);
        loop.runOnce();
        assertEquals(afterFirst + 1, applicationWakeups.get(), "positions no longer all known");
        assertFalse(loop.latestDecision().allPositionsKnown);

        loop.onApplicationPoll(time.milliseconds());
        loop.runOnce();
        assertEquals(afterFirst + 2, applicationWakeups.get(), "reconciliation check advanced for the new poll");

        positionsFuture = new CompletableFuture<>();
        loop.onApplicationPoll(time.milliseconds());
        loop.runOnce();
        assertTrue(loop.latestDecision().positionsAttemptInFlight);
        int beforeError = applicationWakeups.get();
        positionsFuture.completeExceptionally(new java.util.concurrent.CompletionException(new InvalidTopicException("bad")));
        loop.runOnce();
        assertTrue(applicationWakeups.get() > beforeError, "a background event was queued for the application");
        assertFalse(loop.latestDecision().positionsAttemptInFlight);
    }
}
