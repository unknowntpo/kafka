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

import org.apache.kafka.clients.consumer.internals.events.*;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Same three real managers. Fixed logical clock isolates scheduling and RPC completion cost. */
public final class LifecycleWorkload implements AutoCloseable {
    final MembershipEventProbe.Rig rig;
    final String pattern;
    final Runnable completion;
    long admitted;
    long passes;
    NetworkClientDelegate.UnsentRequest pending;
    LifecycleWorkload(String pattern) {
        LifecycleTrace.enabled=false;
        this.pattern=pattern;
        rig=new MembershipEventProbe.Rig();
        rig.join();
        rig.heartbeatResponse(Errors.NONE,1,MembershipEventProbe.emptyAssignment(),5);
        rig.appPoll(7);MembershipEventProbe.completeAssignments(rig,8);
        pending=rig.transport.requests.get(rig.transport.requests.size()-1);
        ConsumerGroupHeartbeatResponse reply=new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.NONE.code()).setMemberId(rig.membership.memberId()).setMemberEpoch(1)
                .setHeartbeatIntervalMs(pattern.equals("BUSY")?0:1000));
        completion=() -> {
            if(pending!=null) {
                NetworkClientDelegate.UnsentRequest request=pending;
                pending=null;
                request.handler().onComplete(MembershipEventProbe.response(request,ApiKeys.CONSUMER_GROUP_HEARTBEAT,10,reply));
            }
        };
        rig.clock.now=10;
        if(!pattern.equals("INFLIGHT"))completion.run();
        rig.transport.requests.clear();
        rig.thread.runOnce();
        if(!rig.transport.requests.isEmpty())pending=rig.transport.requests.get(rig.transport.requests.size()-1);
        rig.transport.requests.clear();
        if(!pattern.equals("BUSY")) {
            rig.thread.runOnce();rig.thread.runOnce();
            assertTrue(rig.transport.requests.isEmpty());
        }
        assertEquals(MemberState.STABLE,rig.membership.state());
        clearInvocations(rig.metadata,rig.commit,rig.offsetsBoundary,rig.fetchBoundary);
    }
    long pass() {
        rig.transport.requests.clear();
        if(pattern.equals("BUSY"))rig.transport.completion=completion;
        rig.thread.runOnce();
        // Fresh request, if any, is now waiting for the next loop's completion callback.
        if(!rig.transport.requests.isEmpty())pending=rig.transport.requests.get(rig.transport.requests.size()-1);
        admitted+=rig.transport.requests.size();passes++;
        return admitted;
    }
    void validate() {
        verifyNoInteractions(rig.metadata,rig.commit,rig.offsetsBoundary,rig.fetchBoundary);
        assertEquals(MemberState.STABLE,rig.membership.state());
        if(!pattern.equals("BUSY"))assertEquals(0,admitted);
        else assertTrue(admitted>0);
        if(rig.processingFailure!=null)throw new AssertionError(rig.processingFailure);
    }
    public void close() {validate();rig.close();}
    public static void main(String[] args) {
        for(String pattern:new String[]{"DORMANT","INFLIGHT","BUSY"})try(LifecycleWorkload w=new LifecycleWorkload(pattern)) {
            for(int i=0;i<1000;i++) {
                w.pass();
                String request=w.rig.transport.requests.isEmpty()?"-":((ConsumerGroupHeartbeatRequest)w.rig.transport.requests.get(0).requestBuilder().build()).data().toString().replace(w.rig.membership.memberId(),"MEMBER");
                System.out.println("BENCHTRACE|"+pattern+"|"+i+"|"+w.rig.membership.state()+"|"+w.admitted+"|"+request);
            }
        }
    }
}
