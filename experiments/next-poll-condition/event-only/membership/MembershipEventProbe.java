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
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.internals.events.*;
import org.apache.kafka.clients.consumer.internals.metrics.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.internals.LogContext;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

public final class MembershipEventProbe {
    static final class Rig implements AutoCloseable {
        final NetworkLoopFixture.Clock clock = new NetworkLoopFixture.Clock();
        final Metrics metrics = new Metrics();
        final AsyncConsumerMetrics asyncMetrics = new AsyncConsumerMetrics(metrics,"membership-probe");
        final LinkedBlockingQueue<ApplicationEvent> events = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<BackgroundEvent> background = new LinkedBlockingQueue<>();
        final BackgroundEventHandler backgroundHandler = new BackgroundEventHandler(background,clock,asyncMetrics);
        final SubscriptionState subscriptions = new SubscriptionState(new LogContext(),AutoOffsetResetStrategy.EARLIEST);
        final ConsumerMetadata metadata = mock(ConsumerMetadata.class);
        final CommitRequestManager commit = mock(CommitRequestManager.class);
        final CoordinatorRequestManager coordinator = new CoordinatorRequestManager(new LogContext(),0,0,"integration");
        final ConsumerMembershipManager membership;
        final ConsumerHeartbeatRequestManager heartbeat;
        final Transport transport;
        final ConsumerNetworkThread thread;
        org.apache.kafka.common.ClusterResourceListener metadataListener;
        int heartbeatPolls;
        Throwable processingFailure;
        Rig() {
            ConsumerConfig config=config();
            doAnswer(inv -> {metadataListener=inv.getArgument(0);return null;}).when(metadata).addClusterUpdateListener(any());
            when(commit.maybeAutoCommitSyncBeforeRebalance(anyLong())).thenReturn(CompletableFuture.completedFuture(null));
            membership=new ConsumerMembershipManager("integration",Optional.empty(),Optional.empty(),10000,
                    Optional.empty(),subscriptions,commit,metadata,new LogContext(),backgroundHandler,clock,
                    mock(RebalanceMetricsManager.class),false);
            heartbeat=new ConsumerHeartbeatRequestManager(new LogContext(),clock,config,coordinator,
                    subscriptions,membership,backgroundHandler,metrics);
            OffsetsRequestManager offsets=mock(OffsetsRequestManager.class);
            FetchRequestManager fetch=mock(FetchRequestManager.class);
            when(offsets.updateFetchPositions(anyLong())).thenReturn(CompletableFuture.completedFuture(null));
            when(fetch.createFetchRequests()).thenReturn(CompletableFuture.completedFuture(null));
            RequestManager counted = new RequestManager() {
                public NetworkClientDelegate.PollResult poll(long now) {heartbeatPolls++;return heartbeat.poll(now);}
                public long maximumTimeToWait(long now) {throw new AssertionError("No maximumTimeToWait scan");}
                public NetworkClientDelegate.PollResult pollOnClose(long now) {return heartbeat.pollOnClose(now);}
            };
            RequestManagers managers = new RequestManagers(new org.apache.kafka.common.utils.LogContext(),offsets,
                    mock(TopicMetadataRequestManager.class),fetch,Optional.of(coordinator),Optional.of(commit),
                    Optional.of(heartbeat),Optional.of(membership),Optional.empty(),Optional.empty()) {
                public List<RequestManager> entries() { return Arrays.asList(coordinator,counted,membership); }
                public void close() { }
            };
            ApplicationEventProcessor processor=new ApplicationEventProcessor(new LogContext(),managers,metadata,subscriptions) {
                public void process(ApplicationEvent event) {
                    try {super.process(event);} catch (RuntimeException | Error failure) {processingFailure=failure;throw failure;}
                }
            };
            transport=new Transport(clock,config,asyncMetrics);
            thread=new ConsumerNetworkThread(new LogContext(),clock,events,new CompletableEventReaper(new LogContext()),
                    () -> processor,() -> transport,() -> managers,asyncMetrics);
            thread.initializeResources();
        }
        void run(long now) {clock.now=now;thread.runOnce();if(processingFailure!=null)throw new AssertionError("Application processor failure",processingFailure);}
        void enqueue(ApplicationEvent event) {event.setEnqueuedMs(clock.now);events.add(event);thread.wakeup();}
        AsyncPollEvent appPoll(long now) {
            AsyncPollEvent e=new AsyncPollEvent(now+10000,now);enqueue(e);run(now);
            assertTrue(e.reconciliationCheckFuture().isDone());assertTrue(e.isComplete());return e;
        }
        void join() {
            enqueue(new TopicSubscriptionChangeEvent(Set.of("topic"),10000));run(1);
            assertEquals(MemberState.UNSUBSCRIBED,membership.state());
            appPoll(2);assertEquals(MemberState.JOINING,membership.state());
            NetworkClientDelegate.UnsentRequest req=transport.requests.get(0);
            transport.completion=() -> req.handler().onComplete(response(req,ApiKeys.FIND_COORDINATOR,3,
                    FindCoordinatorResponse.prepareResponse(Errors.NONE,"integration",new Node(1,"localhost",9092))));
            run(3);run(4);
            assertEquals(2,transport.requests.size());
            assertEquals(0,lastHeartbeat().memberEpoch());
        }
        org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData lastHeartbeat() {
            return ((ConsumerGroupHeartbeatRequest)transport.requests.get(transport.requests.size()-1).requestBuilder().build()).data();
        }
        void heartbeatResponse(Errors error,int epoch,ConsumerGroupHeartbeatResponseData.Assignment assignment,long now) {
            NetworkClientDelegate.UnsentRequest req=transport.requests.get(transport.requests.size()-1);
            ConsumerGroupHeartbeatResponseData data=new ConsumerGroupHeartbeatResponseData().setErrorCode(error.code())
                    .setMemberId(membership.memberId()).setMemberEpoch(epoch).setHeartbeatIntervalMs(1000).setAssignment(assignment);
            transport.completion=() -> req.handler().onComplete(response(req,ApiKeys.CONSUMER_GROUP_HEARTBEAT,now,
                    new ConsumerGroupHeartbeatResponse(data)));
            run(now);run(now+1);
        }
        public void close() {thread.cleanup();asyncMetrics.close();metrics.close();}
    }
    static final class Transport extends NetworkClientDelegate {
        final List<UnsentRequest> requests=new ArrayList<>();Runnable completion;int wakeups;
        Transport(NetworkLoopFixture.Clock clock,ConsumerConfig config,AsyncConsumerMetrics metrics) {
            super(clock,config,new LogContext(),null,null,null,false,metrics);
        }
        public long addAll(PollResult result) {requests.addAll(result.unsentRequests);return result.timeUntilNextPollMs;}
        public void poll(long timeout,long now) {Runnable c=completion;completion=null;if(c!=null)c.run();}
        public boolean hasAnyPendingRequests() {return false;}
        public void wakeup() {wakeups++;}
        public void close() { }
    }
    static ConsumerConfig config() {
        Map<String,Object> config=new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,10000);
        config.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG,0L);
        config.put(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG,0L);
        return new ConsumerConfig(config);
    }
    static ClientResponse response(NetworkClientDelegate.UnsentRequest req,ApiKeys key,long now,AbstractResponse body) {
        return new ClientResponse(new RequestHeader(key,req.requestBuilder().build().version(),"",1),req.handler(),"1",now,now,false,null,null,body);
    }
    static ConsumerGroupHeartbeatResponseData.Assignment emptyAssignment() {
        return new ConsumerGroupHeartbeatResponseData.Assignment().setTopicPartitions(Collections.emptyList());
    }
    public static void main(String[] args) {
        try(Rig r=new Rig()) {
            r.join();r.heartbeatResponse(Errors.NONE,1,emptyAssignment(),5);
            // Empty assignment can require one reconciliation callback on initial join.
            r.appPoll(7);
            completeAssignments(r,8);
            assertEquals(MemberState.STABLE,r.membership.state());
            int calls=r.heartbeatPolls;
            for(int t=20;t<120;t++)r.run(t);
            assertEquals(calls,r.heartbeatPolls);
            r.appPoll(9000);r.run(10002);
            assertNotEquals(MemberState.STALE,r.membership.state());
            assertNotEquals(-1,r.lastHeartbeat().memberEpoch());
        }
        System.out.println("PASS real subscription -> AsyncPollEvent -> JOINING -> coordinator -> heartbeat -> STABLE; no idle poll; reset prevents stale");
        try(Rig r=new Rig()) {
            r.join();
            Uuid id=Uuid.randomUuid();
            when(r.metadata.topicNames()).thenReturn(Map.of(id,"topic"));
            ConsumerGroupHeartbeatResponseData.Assignment assignment=new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(new ConsumerGroupHeartbeatResponseData.TopicPartitions().setTopicId(id).setPartitions(List.of(0))));
            r.heartbeatResponse(Errors.NONE,1,assignment,5);
            assertEquals(MemberState.RECONCILING,r.membership.state());
            r.appPoll(7);
            completeAssignments(r,8);
            assertEquals(MemberState.STABLE,r.membership.state());
            assertEquals(1,r.lastHeartbeat().memberEpoch());
            assertEquals(1,r.lastHeartbeat().topicPartitions().size());
        }
        System.out.println("PASS nonempty assignment -> real reconciliation -> queued callback completion -> acknowledgement heartbeat");
        try(Rig r=new Rig()) {
            r.join();r.heartbeatResponse(Errors.FENCED_MEMBER_EPOCH,1,null,5);
            assertEquals(MemberState.JOINING,r.membership.state());
            assertEquals(0,r.lastHeartbeat().memberEpoch());
            assertEquals(3,r.transport.requests.size());
        }
        System.out.println("PASS fenced response -> real membership rejoin -> targeted heartbeat");
        try(Rig r=new Rig()) {
            r.join();r.heartbeatResponse(Errors.NONE,1,emptyAssignment(),5);r.appPoll(7);completeAssignments(r,8);
            r.run(10007);
            assertEquals(MemberState.STALE,r.membership.state());
            assertEquals(-1,r.lastHeartbeat().memberEpoch());
            r.appPoll(10008);
            assertEquals(MemberState.JOINING,r.membership.state());
            assertEquals(0,r.lastHeartbeat().memberEpoch());
        }
        System.out.println("PASS actual max-poll expiry -> STALE -> AsyncPollEvent -> rejoin");
        try(Rig r=new Rig()) {
            r.join();r.heartbeatResponse(Errors.GROUP_AUTHORIZATION_FAILED,1,null,5);
            assertEquals(MemberState.FATAL,r.membership.state());
            assertTrue(r.background.stream().anyMatch(e -> e instanceof ErrorEvent));
            int polls=r.heartbeatPolls;
            r.run(100);r.run(200);
            assertEquals(polls,r.heartbeatPolls);
        }
        System.out.println("PASS fatal heartbeat response propagates background error and stops heartbeat");
        try(Rig r=new Rig()) {
            r.join();
            Uuid id=Uuid.randomUuid();
            ConsumerGroupHeartbeatResponseData.Assignment assignment=new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(new ConsumerGroupHeartbeatResponseData.TopicPartitions().setTopicId(id).setPartitions(List.of(0))));
            when(r.metadata.topicNames()).thenReturn(Collections.emptyMap());
            r.heartbeatResponse(Errors.NONE,1,assignment,5);r.appPoll(7);completeAssignments(r,8);
            // The first partial assignment can be acknowledged while the topic is unresolved.
            when(r.metadata.topicNames()).thenReturn(Map.of(id,"topic"));
            r.transport.completion=() -> r.metadataListener.onUpdate(new org.apache.kafka.common.ClusterResource("cluster"));
            r.run(10);r.run(11);
            assertTrue(r.background.stream().anyMatch(e -> e instanceof PartitionsAssignedEvent),
                    "Metadata completion must resume reconciliation without another application poll");
            completeAssignments(r,12);
            assertTrue(r.subscriptions.assignedPartitions().contains(new TopicPartition("topic",0)));
        }
        System.out.println("PASS delayed metadata notification resumes real reconciliation without an extra AsyncPollEvent");
        for(boolean close:new boolean[]{false,true})try(Rig r=new Rig()) {
            r.join();r.heartbeatResponse(Errors.NONE,1,emptyAssignment(),5);r.appPoll(7);completeAssignments(r,8);
            CompletableApplicationEvent<Void> event=close?new LeaveGroupOnCloseEvent(20000,CloseOptions.GroupMembershipOperation.LEAVE_GROUP):new UnsubscribeEvent(20000);
            r.enqueue(event);r.run(10);r.run(11);
            assertEquals(-1,r.lastHeartbeat().memberEpoch());
            assertEquals(MemberState.UNSUBSCRIBED,r.membership.state());
            r.heartbeatResponse(Errors.NONE,-1,null,12);
            assertTrue(event.future().isDone());assertFalse(event.future().isCompletedExceptionally());
            int count=r.transport.requests.size();r.run(20);assertEquals(count,r.transport.requests.size());
        }
        System.out.println("PASS real unsubscribe and leave-on-close event paths; leave completes and is not duplicated");
    }
    static void completeAssignments(Rig r,long now) {
        BackgroundEvent event;
        while((event=r.background.poll())!=null) {
            if(event instanceof PartitionsAssignedEvent) {
                PartitionsAssignedEvent assigned=(PartitionsAssignedEvent)event;
                r.subscriptions.assignFromSubscribed(assigned.assignedPartitions());
                r.enqueue(new ConsumerRebalanceListenerCallbackCompletedEvent(ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED,
                        assigned.future(),Optional.empty()));
            } else throw new AssertionError("Unexpected background event "+event);
        }
        r.run(now);r.run(now+1);
    }
}
