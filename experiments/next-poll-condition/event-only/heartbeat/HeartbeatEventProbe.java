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
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.internals.LogContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

public final class HeartbeatEventProbe {
    static final class Rig implements AutoCloseable {
        final NetworkLoopFixture.Clock clock = new NetworkLoopFixture.Clock();
        final CoordinatorRequestManager coordinator = new CoordinatorRequestManager(new LogContext(), 0, 0, "hb");
        final ConsumerMembershipManager membership = mock(ConsumerMembershipManager.class);
        final ConsumerHeartbeatRequestManager.HeartbeatState payload = mock(ConsumerHeartbeatRequestManager.HeartbeatState.class);
        final Metrics metrics = new Metrics();
        final RequestManagerScheduler scheduler = new RequestManagerScheduler();
        final List<NetworkClientDelegate.UnsentRequest> requests = new ArrayList<>();
        final ConsumerHeartbeatRequestManager heartbeat;
        MemberStateListener listener;
        int polls;
        Rig() {
            ConsumerConfig config = mock(ConsumerConfig.class);
            when(config.getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)).thenReturn(10000);
            when(membership.groupInstanceId()).thenReturn(Optional.empty());
            when(membership.state()).thenReturn(MemberState.STABLE);
            when(payload.buildRequestData()).thenAnswer(inv -> new ConsumerGroupHeartbeatRequestData()
                    .setGroupId("hb").setMemberId("member").setMemberEpoch(1));
            doAnswer(inv -> { listener = inv.getArgument(0); return null; }).when(membership).registerStateListener(any());
            heartbeat = new ConsumerHeartbeatRequestManager(new LogContext(), clock.timer(10000), config,
                    coordinator, membership, payload, new HeartbeatRequestState(new LogContext(), clock, 100, 20, 20, 0),
                    mock(BackgroundEventHandler.class), metrics);
            scheduler.registerManagers(Arrays.asList(coordinator, now -> { polls++; return heartbeat.poll(now); }));
        }
        long run(long now) {
            clock.now = now;
            return scheduler.pollReady(now, result -> { requests.addAll(result.unsentRequests); return result.timeUntilNextPollMs; });
        }
        void coordinatorReady(long now) {
            run(now);
            NetworkClientDelegate.UnsentRequest req = requests.get(0);
            clock.now = now;
            req.handler().onComplete(response(req, ApiKeys.FIND_COORDINATOR, now,
                    FindCoordinatorResponse.prepareResponse(Errors.NONE, "hb", new Node(1,"localhost",9092))));
            run(now);
        }
        void success(NetworkClientDelegate.UnsentRequest req, long now) {
            clock.now = now;
            req.handler().onComplete(response(req, ApiKeys.CONSUMER_GROUP_HEARTBEAT, now,
                    new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                            .setErrorCode(Errors.NONE.code()).setHeartbeatIntervalMs(100))));
        }
        public void close() { scheduler.close(); metrics.close(); }
    }
    static ClientResponse response(NetworkClientDelegate.UnsentRequest req, ApiKeys key, long now,
                                    org.apache.kafka.common.requests.AbstractResponse body) {
        return new ClientResponse(new RequestHeader(key, req.requestBuilder().build().version(), "", 1),
                req.handler(), "1", now, now, false, null, null, body);
    }
    public static void main(String[] args) {
        try (Rig r = new Rig()) {
            r.run(1);
            for (int i=2;i<=1001;i++) r.run(i);
            assertEquals(1,r.polls);
            assertEquals(1,r.requests.size());
            NetworkClientDelegate.UnsentRequest req=r.requests.get(0);
            req.handler().onComplete(response(req,ApiKeys.FIND_COORDINATOR,1001,
                    FindCoordinatorResponse.prepareResponse(Errors.NONE,"hb",new Node(1,"localhost",9092))));
            r.run(1001);
            assertEquals(2,r.polls);
            assertEquals(2,r.requests.size());
            assertEquals(ApiKeys.CONSUMER_GROUP_HEARTBEAT,r.requests.get(1).requestBuilder().apiKey());
            r.run(1500);
            assertEquals(2,r.polls,"No re-poll just because heartbeat interval expired while in flight");
            r.success(r.requests.get(1),1500);
            r.run(1500);
            assertEquals(2,r.requests.size(),"Successful request still honors minimum backoff");
            r.run(1520);
            assertEquals(3,r.requests.size(),"Overdue heartbeat proceeds after response and minimum backoff");
        }
        System.out.println("PASS coordinator discovery activates real consumer heartbeat; in-flight wait has no interval spin");
        try (Rig r = new Rig()) {
            r.coordinatorReady(101);
            assertEquals(2,r.requests.size());
            r.success(r.requests.get(1),110);
            r.run(110);
            int calls=r.polls;
            r.run(200);
            assertEquals(calls,r.polls);
            r.run(201);
            assertEquals(3,r.requests.size());
            r.clock.now=202;
            r.requests.get(2).handler().onFailure(202,new TimeoutException("retry"));
            r.run(202);
            calls=r.polls;
            r.run(221);
            assertEquals(calls,r.polls);
            r.run(222);
            assertEquals(4,r.requests.size());
        }
        System.out.println("PASS heartbeat interval and failed-request retry deadlines");
        try (Rig r = new Rig()) {
            r.coordinatorReady(101);
            r.heartbeat.resetPollTimer(9000);
            r.run(9000);
            int calls=r.polls;
            r.run(10001);
            assertEquals(calls,r.polls,"Old max-poll deadline must be cancelled");
            r.run(19000);
            verify(r.membership).transitionToSendingLeaveGroup(true);
            assertEquals(3,r.requests.size(),"Poll expiry generates leave heartbeat even with an in-flight request");
        }
        System.out.println("PASS application poll reset cancels old deadline; max.poll.interval expiry sends leave");
        try (Rig r = new Rig()) {
            when(r.membership.shouldSkipHeartbeat()).thenReturn(true);
            r.coordinatorReady(101);
            assertEquals(1,r.requests.size());
            when(r.membership.shouldSkipHeartbeat()).thenReturn(false);
            r.listener.onMemberStateChange(MemberState.JOINING);
            r.run(102);
            assertEquals(2,r.requests.size());
            when(r.membership.isLeavingGroup()).thenReturn(true);
            assertEquals(1,r.heartbeat.pollOnClose(103).unsentRequests.size());
            r.scheduler.close();
            r.listener.onMemberStateChange(MemberState.LEAVING);
            assertEquals(0,r.run(104)==Long.MAX_VALUE?0:1,"Closed scheduler must not resume manager");
        }
        System.out.println("PASS membership notification and existing close request path");
    }
}
