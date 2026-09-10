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
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HeartbeatNextPollConditionTest {
    private static final String DEFAULT_GROUP_ID = "group";
    private static final int DEFAULT_MAX_POLL_INTERVAL_MS = 10000;

    @Test
    public void testRealCoordinatorAndMembershipActivateOnlyHeartbeatOnNextBatch() {
        MockTime time = new MockTime();
        long originalPollDeadline = time.milliseconds() + DEFAULT_MAX_POLL_INTERVAL_MS;
        LogContext logContext = new LogContext();
        SubscriptionState subscriptions = mock(SubscriptionState.class);
        BackgroundEventHandler backgroundEventHandler = mock(BackgroundEventHandler.class);
        ConsumerConfig config = mock(ConsumerConfig.class);
        when(config.getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)).thenReturn(DEFAULT_MAX_POLL_INTERVAL_MS);
        when(config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG)).thenReturn(80L);
        when(config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG)).thenReturn(1000L);
        CoordinatorRequestManager coordinator = new CoordinatorRequestManager(logContext, 0, 0, DEFAULT_GROUP_ID);
        try (Metrics metrics = new Metrics(time); RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            ConsumerMembershipManager member = new ConsumerMembershipManager(DEFAULT_GROUP_ID, Optional.empty(),
                Optional.empty(), DEFAULT_MAX_POLL_INTERVAL_MS, Optional.empty(), subscriptions,
                mock(CommitRequestManager.class), mock(ConsumerMetadata.class), logContext,
                backgroundEventHandler, time, metrics, false);
            ConsumerHeartbeatRequestManager heartbeat = spy(new ConsumerHeartbeatRequestManager(logContext, time,
                config, coordinator, subscriptions, member, backgroundEventHandler, metrics));
            NextPollCondition.Signal unrelatedInput = new NextPollCondition.Signal();
            int[] unrelatedCalls = {0};
            RequestManager unrelated = now -> {
                unrelatedCalls[0]++;
                return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), unrelatedInput.await());
            };
            List<NetworkClientDelegate.UnsentRequest> requests = new ArrayList<>();
            scheduler.registerManagers(Arrays.asList(heartbeat, coordinator, unrelated));
            scheduler.pollReady(time.milliseconds(), result -> {
                requests.addAll(result.unsentRequests);
                return result.timeUntilNextPollMs;
            });
            assertEquals(1, requests.size());
            for (int i = 0; i < 100; i++) {
                time.sleep(1);
                scheduler.pollReady(time.milliseconds(), result -> result.timeUntilNextPollMs);
            }
            verify(heartbeat, times(1)).poll(anyLong());
            member.transitionToJoining();
            verify(heartbeat, times(1)).poll(anyLong());
            scheduler.pollReady(time.milliseconds(), result -> result.timeUntilNextPollMs);
            verify(heartbeat, times(2)).poll(anyLong());

            NetworkClientDelegate.UnsentRequest discovery = requests.get(0);
            Node node = new Node(1, "localhost", 9092);
            discovery.handler().onComplete(new ClientResponse(
                new RequestHeader(ApiKeys.FIND_COORDINATOR, discovery.requestBuilder().build().version(), "", 1),
                discovery.handler(), node.idString(), time.milliseconds(), time.milliseconds(), false, null, null,
                FindCoordinatorResponse.prepareResponse(Errors.NONE, DEFAULT_GROUP_ID, node)));
            assertTrue(coordinator.coordinator().isPresent());
            verify(heartbeat, times(2)).poll(anyLong());
            scheduler.pollReady(time.milliseconds(), result -> {
                requests.addAll(result.unsentRequests);
                return result.timeUntilNextPollMs;
            });
            assertEquals(2, requests.size());
            assertInstanceOf(ConsumerGroupHeartbeatRequest.Builder.class, requests.get(1).requestBuilder());
            verify(heartbeat, times(3)).poll(anyLong());
            for (int i = 0; i < 100; i++) {
                time.sleep(1);
                scheduler.pollReady(time.milliseconds(), result -> result.timeUntilNextPollMs);
            }
            verify(heartbeat, times(3)).poll(anyLong());
            assertEquals(1, unrelatedCalls[0]);

            heartbeat.resetPollTimer(time.milliseconds());
            verify(heartbeat, times(3)).poll(anyLong());
            scheduler.pollReady(time.milliseconds(), result -> result.timeUntilNextPollMs);
            verify(heartbeat, times(4)).poll(anyLong());
            time.sleep(originalPollDeadline - time.milliseconds());
            scheduler.pollReady(time.milliseconds(), result -> result.timeUntilNextPollMs);
            verify(heartbeat, times(4)).poll(anyLong());
            assertEquals(MemberState.JOINING, member.state());

            member.transitionToFatal();
            verify(heartbeat, times(4)).poll(anyLong());
            scheduler.pollReady(time.milliseconds(), result -> result.timeUntilNextPollMs);
            verify(heartbeat, times(5)).poll(anyLong());
            assertTrue(member.shouldSkipHeartbeat());
            assertEquals(1, unrelatedCalls[0]);
        }
    }

}
