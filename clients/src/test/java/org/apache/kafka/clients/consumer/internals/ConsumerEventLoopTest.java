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
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CommitOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.FetchPositionsErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.LeaveGroupOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.StopFindCoordinatorOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.pipeline.PassDecision;
import org.apache.kafka.clients.consumer.internals.pipeline.WaitCondition;
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
import org.mockito.InOrder;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
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
    private AsyncConsumerMetrics asyncConsumerMetrics;
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
        asyncConsumerMetrics = mock(AsyncConsumerMetrics.class);
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
        clearInvocations(fetchRequestManager);
        loop.requestFetch();
        loop.runOnce();
        verify(fetchRequestManager, times(1)).createFetchRequests();
        verify(fetchRequestManager, times(1)).poll(anyLong());
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

        // Positions only change through an input with identity (a command such as assign/seek, a manager pass,
        // a metadata change); the decision re-reads them on such passes only.
        when(subscriptions.hasAllFetchPositions()).thenReturn(false);
        loop.add(eventWithDeadline(time.milliseconds() + 1_000));
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

    @Test
    public void managerDeclaringOwnCompletionIsNotRerunByOtherManagersInputs() {
        when(topicMetadataRequestManager.waitCondition()).thenReturn(WaitCondition.OWN_COMPLETION);
        NetworkClientDelegate.UnsentRequest others = new NetworkClientDelegate.UnsentRequest(
            new MetadataRequest.Builder(List.of("topic"), false), Optional.of(new Node(0, "localhost", 9092)));
        when(offsetsRequestManager.poll(anyLong()))
            .thenReturn(new NetworkClientDelegate.PollResult(Long.MAX_VALUE, List.of(others)))
            .thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        loop.runOnce();
        clearInvocations(topicMetadataRequestManager);

        // Another manager's request completes and metadata changes: neither is the declared input.
        others.future().completeExceptionally(new RuntimeException("simulated completion"));
        loop.runOnce();
        metadataVersion.incrementAndGet();
        loop.runOnce();
        verify(topicMetadataRequestManager, never()).poll(anyLong());

        // A command always re-runs every manager (its effect on manager state is unknown).
        loop.add(eventWithDeadline(time.milliseconds() + 1_000));
        loop.runOnce();
        verify(topicMetadataRequestManager, times(1)).poll(anyLong());
    }

    @Test
    public void managerDeclaringOwnCompletionIsRerunWhenItsOwnRequestCompletes() {
        when(topicMetadataRequestManager.waitCondition()).thenReturn(WaitCondition.OWN_COMPLETION);
        NetworkClientDelegate.UnsentRequest own = new NetworkClientDelegate.UnsentRequest(
            new MetadataRequest.Builder(List.of("topic"), false), Optional.of(new Node(0, "localhost", 9092)));
        when(topicMetadataRequestManager.poll(anyLong()))
            .thenReturn(new NetworkClientDelegate.PollResult(Long.MAX_VALUE, List.of(own)))
            .thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        loop.runOnce();
        clearInvocations(topicMetadataRequestManager);
        loop.runOnce();
        verify(topicMetadataRequestManager, never()).poll(anyLong());

        own.future().completeExceptionally(new RuntimeException("simulated completion"));
        loop.runOnce();
        verify(topicMetadataRequestManager, times(1)).poll(anyLong());
    }

    @Test
    public void managerDeclaringTimerOnlyRunsOnlyOnItsTimerOrACommand() {
        when(topicMetadataRequestManager.waitCondition()).thenReturn(WaitCondition.TIMER_ONLY);
        when(topicMetadataRequestManager.poll(anyLong())).thenReturn(new NetworkClientDelegate.PollResult(50));
        loop.runOnce();
        clearInvocations(topicMetadataRequestManager);
        loop.onApplicationPoll(time.milliseconds());
        loop.runOnce();
        metadataVersion.incrementAndGet();
        loop.runOnce();
        verify(topicMetadataRequestManager, never()).poll(anyLong());
        time.sleep(50);
        loop.runOnce();
        verify(topicMetadataRequestManager, times(1)).poll(anyLong());
    }

    @Test
    public void aManagerAskingForZeroDelayIsBoundedByTheTimerFloorAndItsDeclaration() {
        when(topicMetadataRequestManager.waitCondition()).thenReturn(WaitCondition.TIMER_ONLY);
        when(topicMetadataRequestManager.poll(anyLong())).thenReturn(new NetworkClientDelegate.PollResult(0));
        loop.runOnce();
        clearInvocations(topicMetadataRequestManager);
        for (int i = 0; i < 100; i++)
            loop.runOnce();
        verify(topicMetadataRequestManager, never()).poll(anyLong());
        time.sleep(1);
        loop.runOnce();
        verify(topicMetadataRequestManager, times(1)).poll(anyLong());
    }

    @Test
    public void loopRecordsPassesAndManagerRunsThatProducedNoRequest() {
        loop.runOnce();
        verify(asyncConsumerMetrics, times(1)).recordBackgroundPass();
        // The first pass runs every manager (all dirty at start); the mocks return no requests.
        verify(asyncConsumerMetrics).recordManagerRunsWithoutRequests(3);
        loop.runOnce();
        verify(asyncConsumerMetrics, times(2)).recordBackgroundPass();
    }

    /**
     * The commit manager reads the coordinator's fatal error and the heartbeat manager clears it; the previous
     * implementation ran them in registration order on every iteration, so the read always came first. A timer that
     * ran the heartbeat manager on the spot would break that contract (semantics S6).
     */
    @Test
    public void managersRunInRegistrationOrderWhateverTriggeredThem() {
        CoordinatorRequestManager coordinator = mock(CoordinatorRequestManager.class);
        AtomicReference<Optional<Throwable>> fatal = new AtomicReference<>(Optional.of(new RuntimeException("fatal")));
        when(coordinator.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(coordinator.fatalError()).thenAnswer(inv -> fatal.get());
        when(coordinator.getAndClearFatalError()).thenAnswer(inv -> fatal.getAndSet(Optional.empty()));
        CommitRequestManager commit = mock(CommitRequestManager.class);
        AtomicBoolean commitSawFatalError = new AtomicBoolean();
        when(commit.poll(anyLong())).thenAnswer(inv -> {
            commitSawFatalError.set(coordinator.fatalError().isPresent());
            return NetworkClientDelegate.PollResult.EMPTY;
        });
        ConsumerHeartbeatRequestManager heartbeat = mock(ConsumerHeartbeatRequestManager.class);
        when(heartbeat.poll(anyLong())).thenAnswer(inv -> {
            coordinator.getAndClearFatalError();
            return new NetworkClientDelegate.PollResult(50);
        });
        RequestManagers managers = new RequestManagers(logContext, offsetsRequestManager, topicMetadataRequestManager,
                fetchRequestManager, Optional.of(coordinator), Optional.of(commit), Optional.of(heartbeat),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        ConsumerEventLoop ordered = new ConsumerEventLoop(logContext, time, 1_000, subscriptions, metadata, () -> processor,
                () -> networkClientDelegate, () -> managers, new BackgroundEventHandler(backgroundQueue, time, asyncConsumerMetrics),
                asyncConsumerMetrics, () -> { });
        ordered.initializeResources();
        // First pass: no fatal error yet, everything runs once and the heartbeat timer is armed for +50 ms.
        fatal.set(Optional.empty());
        ordered.runOnce();
        // The coordinator now has a fatal error; the heartbeat timer expires and a command arrives in the same pass.
        fatal.set(Optional.of(new RuntimeException("fatal")));
        time.sleep(50);
        ordered.add(eventWithDeadline(time.milliseconds() + 1_000));
        ordered.runOnce();
        assertTrue(commitSawFatalError.get(), "the commit manager must read the fatal error before the heartbeat manager clears it");
        assertTrue(fatal.get().isEmpty(), "and the heartbeat manager did clear it afterwards");
    }

    @Test
    public void closeTransitionsPassThroughTheLifecycleSequencerInOrderAndShutdownPollsManagersInRegistrationOrder() {
        CoordinatorRequestManager coordinator = mock(CoordinatorRequestManager.class);
        when(coordinator.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(coordinator.pollOnClose(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        CommitRequestManager commit = mock(CommitRequestManager.class);
        when(commit.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(commit.pollOnClose(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        ConsumerMembershipManager membership = mock(ConsumerMembershipManager.class);
        when(membership.poll(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(membership.pollOnClose(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(membership.leaveGroupOnClose(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(offsetsRequestManager.pollOnClose(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(topicMetadataRequestManager.pollOnClose(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        when(fetchRequestManager.pollOnClose(anyLong())).thenReturn(NetworkClientDelegate.PollResult.EMPTY);
        RequestManagers managers = new RequestManagers(logContext, offsetsRequestManager, topicMetadataRequestManager,
                fetchRequestManager, Optional.of(coordinator), Optional.of(commit), Optional.empty(),
                Optional.of(membership), Optional.empty(), Optional.empty(), Optional.empty());
        ConsumerEventLoop closing = new ConsumerEventLoop(logContext, time, 1_000, subscriptions, metadata, () -> processor,
                () -> networkClientDelegate, () -> managers, new BackgroundEventHandler(backgroundQueue, time, asyncConsumerMetrics),
                asyncConsumerMetrics, () -> { });
        closing.initializeResources();
        assertEquals(LifecycleSequencer.Step.RUNNING, closing.lifecycle().step());

        closing.add(new CommitOnCloseEvent());
        closing.add(new StopFindCoordinatorOnCloseEvent());
        LeaveGroupOnCloseEvent leave = new LeaveGroupOnCloseEvent(time.milliseconds() + 1_000, CloseOptions.GroupMembershipOperation.DEFAULT);
        closing.add(leave);
        closing.runOnce();

        InOrder order = inOrder(commit, coordinator, membership);
        order.verify(commit).signalClose();
        order.verify(coordinator).signalClose();
        order.verify(membership).leaveGroupOnClose(CloseOptions.GroupMembershipOperation.DEFAULT);
        assertTrue(leave.future().isDone());
        assertEquals(LifecycleSequencer.Step.LEFT, closing.lifecycle().step());
        verify(processor, never()).process(any(CommitOnCloseEvent.class));

        closing.lifecycle().shutdown(time.milliseconds(), networkClientDelegate);
        InOrder closeOrder = inOrder(coordinator, commit, membership, offsetsRequestManager, topicMetadataRequestManager, fetchRequestManager);
        closeOrder.verify(coordinator).pollOnClose(anyLong());
        closeOrder.verify(commit).pollOnClose(anyLong());
        closeOrder.verify(membership).pollOnClose(anyLong());
        closeOrder.verify(offsetsRequestManager).pollOnClose(anyLong());
        closeOrder.verify(topicMetadataRequestManager).pollOnClose(anyLong());
        closeOrder.verify(fetchRequestManager).pollOnClose(anyLong());
        assertEquals(LifecycleSequencer.Step.SHUT_DOWN, closing.lifecycle().step());
    }
}
