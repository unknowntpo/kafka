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

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.internals.events.*;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import java.util.*;
import static org.mockito.Mockito.when;

public final class LifecycleTrace {
    static boolean enabled=true;
    static String scenario;
    static int step;
    static void record(MembershipEventProbe.Rig r,long now) {
        if(!enabled)return;
        List<String> requests=new ArrayList<>();
        for(NetworkClientDelegate.UnsentRequest req:r.transport.requests) {
            AbstractRequest request=req.requestBuilder().build();
            String data=request instanceof ConsumerGroupHeartbeatRequest ? ((ConsumerGroupHeartbeatRequest)request).data().toString():((FindCoordinatorRequest)request).data().toString();
            requests.add(request.apiKey()+":"+data.replace(r.membership.memberId(),"MEMBER"));
        }
        List<String> bg=new ArrayList<>();for(BackgroundEvent e:r.background)bg.add(e.type().toString());
        System.out.println("TRACE|"+scenario+"|"+(step++)+"|"+now+"|"+r.membership.state()+"|"+r.membership.memberEpoch()+"|"+new TreeSet<>(r.subscriptions.assignedPartitions().stream().map(Object::toString).toList())+"|"+bg+"|"+requests);
    }
    public static void main(String[] args) {
        for(String name:List.of("IDLE","TIMER","FENCED","FATAL","STALE","ASSIGNMENT","METADATA","UNSUBSCRIBE","CLOSE")) {
            scenario=name;step=0;
            try(MembershipEventProbe.Rig r=new MembershipEventProbe.Rig()) {
                r.join();
                if(name.equals("FENCED")||name.equals("FATAL")) {
                    r.heartbeatResponse(name.equals("FENCED")?Errors.FENCED_MEMBER_EPOCH:Errors.GROUP_AUTHORIZATION_FAILED,1,null,5);
                    r.run(20);r.run(100);continue;
                }
                if(name.equals("ASSIGNMENT")||name.equals("METADATA")) {
                    Uuid id=new Uuid(1,2);
                    when(r.metadata.topicNames()).thenReturn(name.equals("METADATA")?Collections.emptyMap():Map.of(id,"topic"));
                    ConsumerGroupHeartbeatResponseData.Assignment assignment=new ConsumerGroupHeartbeatResponseData.Assignment()
                            .setTopicPartitions(List.of(new ConsumerGroupHeartbeatResponseData.TopicPartitions().setTopicId(id).setPartitions(List.of(0))));
                    r.heartbeatResponse(Errors.NONE,1,assignment,5);r.appPoll(7);MembershipEventProbe.completeAssignments(r,8);
                    if(name.equals("METADATA")) {
                        r.transport.completion=() -> {
                            when(r.metadata.topicNames()).thenReturn(Map.of(id,"topic"));
                            if(r.metadataListener!=null)r.metadataListener.onUpdate(new org.apache.kafka.common.ClusterResource("cluster"));
                        };
                        r.run(10);r.run(11);MembershipEventProbe.completeAssignments(r,12);
                    }
                    continue;
                }
                r.heartbeatResponse(Errors.NONE,1,MembershipEventProbe.emptyAssignment(),5);r.appPoll(7);MembershipEventProbe.completeAssignments(r,8);
                switch(name) {
                    case "IDLE":for(int t=20;t<120;t++)r.run(t);break;
                    case "TIMER":r.run(1004);r.run(1005);r.heartbeatResponse(Errors.NONE,1,null,1006);r.run(2005);break;
                    case "STALE":r.run(10007);r.appPoll(10008);break;
                    case "UNSUBSCRIBE":r.enqueue(new UnsubscribeEvent(20000));r.run(10);r.run(11);r.heartbeatResponse(Errors.NONE,-1,null,12);r.run(20);break;
                    case "CLOSE":r.enqueue(new LeaveGroupOnCloseEvent(20000,CloseOptions.GroupMembershipOperation.LEAVE_GROUP));r.run(10);r.run(11);r.heartbeatResponse(Errors.NONE,-1,null,12);r.run(20);break;
                    default:throw new AssertionError(name);
                }
            }
        }
    }
}
