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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.test.TestUtils.requiredConsumerConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsumerReactorCommitReadinessTest {

    @Test
    public void testCompletionPublishesScheduleBeforeApplicationWakeup() {
        MockTime time = new MockTime();
        LogContext logContext = new LogContext();
        NetworkClientDelegate networkClientDelegate = mock(NetworkClientDelegate.class);
        RequestManagers requestManagers = mock(RequestManagers.class);
        CompletableEventReaper reaper = mock(CompletableEventReaper.class);

        when(reaper.timeUntilNextExpirationMs(anyLong())).thenReturn(Long.MAX_VALUE);

        try (Metrics metrics = new Metrics();
             ConsumerReactor reactor = new ConsumerReactor(
                 logContext,
                 time,
                 new LinkedBlockingQueue<>(),
                 reaper,
                 () -> mock(ApplicationEventProcessor.class),
                 () -> networkClientDelegate,
                 () -> requestManagers,
                 mock(AsyncConsumerMetrics.class)
             )) {
            reactor.initializeResources();
            CommitRequestManager commitRequestManager = newCommitRequestManager(time, logContext, metrics);
            AtomicReference<NetworkClientDelegate.UnsentRequest> autoCommitRequest = new AtomicReference<>();
            AtomicLong networkPollCount = new AtomicLong();
            List<Long> generationsObservedByWakeup = new ArrayList<>();

            when(requestManagers.entries()).thenReturn(List.of(commitRequestManager));
            doAnswer(invocation -> {
                List<NetworkClientDelegate.UnsentRequest> requests = invocation.getArgument(0);
                if (!requests.isEmpty() && autoCommitRequest.get() == null)
                    autoCommitRequest.set(requests.get(0));
                return null;
            }).when(networkClientDelegate).addAll(anyList());
            doAnswer(invocation -> {
                if (networkPollCount.incrementAndGet() == 2L) {
                    long timeoutMs = invocation.getArgument(0);
                    assertEquals(ConsumerReactor.MAX_POLL_TIMEOUT_MS, timeoutMs,
                        "An in-flight commit must not retain a timer deadline");
                    NetworkClientDelegate.UnsentRequest request = autoCommitRequest.get();
                    assertTrue(request != null, "Auto-commit request must be staged before completion");
                    request.handler().onComplete(successfulOffsetCommitResponse(time));
                }
                return null;
            }).when(networkClientDelegate).poll(anyLong(), anyLong());
            doAnswer(invocation -> {
                generationsObservedByWakeup.add(reactor.reactorSchedule().generation());
                return null;
            }).when(requestManagers).wakeupApplicationThread();

            time.sleep(100L);
            commitRequestManager.updateTimerAndMaybeCommit(time.milliseconds());
            reactor.runOnce();

            assertTrue(autoCommitRequest.get() != null);
            long generationWithInflightCommit = reactor.reactorSchedule().generation();
            generationsObservedByWakeup.clear();

            time.sleep(100L);
            reactor.runOnce();

            assertEquals(2L, networkPollCount.get());
            assertEquals(List.of(reactor.reactorSchedule().generation()), generationsObservedByWakeup);
            assertTrue(reactor.reactorSchedule().generation() > generationWithInflightCommit);
            assertTrue(reactor.reactorSchedule().applicationDeadlineDelivered());
        }
    }

    /**
     * KAFKA-20970 vertical proof: an expired auto-commit interval with an unknown coordinator must not collapse
     * into a zero-timeout network poll. The real commit manager contributes an input-driven wait and the reactor
     * publishes that wait before entering the network client.
     */
    @Test
    public void testExpiredAutoCommitWithUnknownCoordinatorDoesNotZeroPoll() {
        MockTime time = new MockTime();
        LogContext logContext = new LogContext();
        NetworkClientDelegate networkClientDelegate = mock(NetworkClientDelegate.class);
        RequestManagers requestManagers = mock(RequestManagers.class);
        CompletableEventReaper reaper = mock(CompletableEventReaper.class);
        AtomicLong observedPollTimeout = new AtomicLong(-1L);

        when(reaper.timeUntilNextExpirationMs(anyLong())).thenReturn(Long.MAX_VALUE);
        doAnswer(invocation -> {
            observedPollTimeout.set(invocation.getArgument(0));
            return null;
        }).when(networkClientDelegate).poll(anyLong(), anyLong());

        try (Metrics metrics = new Metrics();
             ConsumerReactor reactor = new ConsumerReactor(
                 logContext,
                 time,
                 new LinkedBlockingQueue<>(),
                 reaper,
                 () -> mock(ApplicationEventProcessor.class),
                 () -> networkClientDelegate,
                 () -> requestManagers,
                 mock(AsyncConsumerMetrics.class)
             )) {
            reactor.initializeResources();
            CommitRequestManager commitRequestManager = newCommitRequestManager(
                time,
                logContext,
                metrics,
                Optional.empty()
            );
            when(requestManagers.entries()).thenReturn(List.of(commitRequestManager));

            time.sleep(100L);
            reactor.runOnce();

            assertEquals(ConsumerReactor.MAX_POLL_TIMEOUT_MS, observedPollTimeout.get());
            assertEquals(Long.MAX_VALUE, reactor.maximumTimeToWait());
        }
    }

    private static CommitRequestManager newCommitRequestManager(
        final MockTime time,
        final LogContext logContext,
        final Metrics metrics
    ) {
        return newCommitRequestManager(
            time,
            logContext,
            metrics,
            Optional.of(new Node(1, "localhost", 9092))
        );
    }

    private static CommitRequestManager newCommitRequestManager(
        final MockTime time,
        final LogContext logContext,
        final Metrics metrics,
        final Optional<Node> coordinatorNode
    ) {
        TopicPartition partition = new TopicPartition("topic", 0);
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        subscriptions.assignFromUser(Set.of(partition));
        subscriptions.seek(partition, 10L);

        Properties properties = requiredConsumerConfig();
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-id");

        CoordinatorRequestManager coordinator = mock(CoordinatorRequestManager.class);
        when(coordinator.coordinator()).thenReturn(coordinatorNode);
        when(coordinator.coordinatorSnapshot()).thenReturn(
            new CoordinatorSnapshot(coordinatorNode, 1L)
        );

        return new CommitRequestManager(
            time,
            logContext,
            subscriptions,
            new ConsumerConfig(properties),
            coordinator,
            mock(OffsetCommitCallbackInvoker.class),
            "group-id",
            Optional.empty(),
            100L,
            1_000L,
            OptionalDouble.of(0.0),
            metrics,
            mock(ConsumerMetadata.class)
        );
    }

    private static ClientResponse successfulOffsetCommitResponse(final MockTime time) {
        return new ClientResponse(
            new RequestHeader(ApiKeys.OFFSET_COMMIT, (short) 1, "", 1),
            null,
            "-1",
            time.milliseconds(),
            time.milliseconds(),
            false,
            null,
            null,
            new OffsetCommitResponse(0, Collections.emptyMap())
        );
    }
}
