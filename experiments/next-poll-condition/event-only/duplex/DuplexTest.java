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
package org.apache.kafka.clients.consumer.internals.duplex;

import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

public final class DuplexTest {
    static final long SECOND=TimeUnit.SECONDS.toNanos(1);
    static void check(boolean value,String message){if(!value)throw new AssertionError(message);}
    static void waitFor(BooleanSupplier condition,String message) throws Exception {
        long until=System.nanoTime()+5*SECOND;
        while(!condition.getAsBoolean()) {
            if(System.nanoTime()>=until)throw new AssertionError(message);
            Thread.sleep(1);
        }
    }
    static void latch(CountDownLatch latch) {
        try{check(latch.await(5,TimeUnit.SECONDS),"Latch timed out");}catch(InterruptedException e){throw new AssertionError(e);}
    }
    static void releaseWithoutParking(CountDownLatch release) {
        // This test hook is between queue recheck and park. A latch.await here would
        // consume the unpark permit under test and manufacture a lost wakeup.
        long until=System.nanoTime()+5*SECOND;
        while(release.getCount()!=0) {
            if(System.nanoTime()>=until)throw new AssertionError("Hook release timeout");
            Thread.yield();
        }
    }
    static DuplexEngine.Completion take(DuplexEngine engine) {
        DuplexEngine.Completion c=engine.awaitCompletion(5*SECOND);
        check(c!=null,"Completion missing");return c;
    }
    public static void main(String[] args) throws Exception {
        ringWrap();
        publicationBeforePark();
        reservedControlAndShutdown();
        completionBackpressure();
        terminalRaces();
        stress();
        System.out.println("PASS all duplex contract tests");
    }
    static void ringWrap() {
        DuplexEngine.Ring<Integer> ring=new DuplexEngine.Ring<>(4);
        for(int turn=0;turn<10000;turn++) {
            for(int i=0;i<4;i++)check(ring.offer(turn*4+i),"Offer failed");
            check(!ring.offer(-1),"Full ring overwritten");
            for(int i=0;i<4;i++)check(ring.poll()==turn*4+i,"Order/publication broken");
            check(ring.poll()==null,"Stale cell retained");
        }
        System.out.println("PASS bounded ring full/empty/wraparound FIFO");
    }
    static void publicationBeforePark() throws Exception {
        for(int round=0;round<100;round++) {
            CountDownLatch armed=new CountDownLatch(1),release=new CountDownLatch(1);
            AtomicBoolean first=new AtomicBoolean(true);
            DuplexEngine engine=new DuplexEngine(8,8,()->{if(first.getAndSet(false)){armed.countDown();releaseWithoutParking(release);}});
            try {
                latch(armed);
                long id=engine.trySubmit(42,0,SECOND);
                check(id>=0,"Submission rejected");
                release.countDown();
                DuplexEngine.Completion c=take(engine);
                check(c.id==id && c.value==42 && c.result==DuplexEngine.Result.OK,"Publication before park lost");
            } finally {release.countDown();engine.close();}
        }
        System.out.println("PASS 100 deterministic enqueue-between-recheck-and-park races");
    }
    static void reservedControlAndShutdown() {
        CountDownLatch armed=new CountDownLatch(1),release=new CountDownLatch(1);
        AtomicBoolean first=new AtomicBoolean(true);
        DuplexEngine engine=new DuplexEngine(4,8,()->{if(first.getAndSet(false)){armed.countDown();releaseWithoutParking(release);}});
        try {
            latch(armed);
            long firstId=engine.trySubmit(1,60*SECOND,120*SECOND);
            check(firstId>=0,"First submission");
            check(engine.trySubmit(2,60*SECOND,120*SECOND)>=0,"Second submission");
            check(engine.trySubmit(3,60*SECOND,120*SECOND)>=0,"Third submission");
            check(engine.trySubmit(4,60*SECOND,120*SECOND)==-1,"Data stole reserved control slot");
            check(engine.tryCancel(firstId),"Reserved cancellation slot unavailable");
            check(!engine.tryCancel(firstId),"Full SQ should return false");
            release.countDown();
            DuplexEngine.Completion c=take(engine);
            check(c.id==firstId && c.result==DuplexEngine.Result.CANCELLED,"Cancel not dispatched");
            engine.requestClose();
            check(take(engine).result==DuplexEngine.Result.CANCELLED,"Shutdown pending work");
            check(take(engine).result==DuplexEngine.Result.CANCELLED,"Shutdown queued work");
            check(engine.trySubmit(5,0,SECOND)==-1,"Accepted after close");
        } finally {release.countDown();engine.close();}
        check(engine.timerCount()==0 && engine.activeCount()==0,"Shutdown retained work");
        System.out.println("PASS reserved SQ control slot, immediate backpressure, cancellation and nonblocking close request");
    }
    static void completionBackpressure() throws Exception {
        try(DuplexEngine engine=new DuplexEngine(8,4)) {
            for(int i=0;i<3;i++)check(engine.trySubmit(i,0,SECOND)>=0,"Submit immediate");
            check(engine.trySubmit(3,60*SECOND,TimeUnit.MILLISECONDS.toNanos(20))>=0,"Submit timer");
            waitFor(()->engine.completed()==4,"Timer failed while application stopped draining CQ");
            check(engine.trySubmit(4,0,SECOND)==-1,"CQ reservation ignored");
            check(engine.outstanding()==4,"Credits lost");
            check(engine.tryCancel(99999),"Control should remain serviceable with full CQ");
            engine.requestClose();
            waitFor(engine::isTerminated,"Full CQ blocked background shutdown");
            BitSet seen=new BitSet();boolean timeout=false;
            for(int i=0;i<4;i++) {
                DuplexEngine.Completion c=take(engine);check(!seen.get((int)c.id),"Duplicate");seen.set((int)c.id);
                timeout |= c.result==DuplexEngine.Result.TIMEOUT;
            }
            check(timeout && engine.outstanding()==0,"CQ full lost completion/credit");
        }
        System.out.println("PASS paused app: near-full CQ timer completion, full-CQ close and lossless drain");
    }
    static void terminalRaces() throws Exception {
        try(DuplexEngine engine=new DuplexEngine(16,8)) {
            long tie=engine.trySubmit(7,0,0);
            check(take(engine).result==DuplexEngine.Result.TIMEOUT,"Exact deadline tie must timeout");
            check(engine.tryCancel(tie),"Late cancellation rejected unnecessarily");
            long success=engine.trySubmit(8,0,SECOND);
            DuplexEngine.Completion done=take(engine);check(done.id==success && done.result==DuplexEngine.Result.OK,"Success failed");
            check(engine.tryCancel(success),"Late cancel enqueue");
            for(int i=0;i<2000;i++) {
                long id=engine.trySubmit(i,TimeUnit.MICROSECONDS.toNanos(20),TimeUnit.MICROSECONDS.toNanos(40));
                check(id>=0,"Single outstanding operation rejected");
                engine.tryCancel(id);
                DuplexEngine.Completion c=take(engine);check(c.id==id && c.value==i,"Wrong race completion");
            }
            waitFor(()->engine.timerCount()==0 && engine.activeCount()==0,"Cancelled timers retained");
            check(engine.pollCompletion()==null,"Duplicate terminal completion");
        }
        System.out.println("PASS timeout tie, late cancel, 2000 completion/cancel races, timer removal");
    }
    static void stress() throws Exception {
        final int total=100000;
        try(DuplexEngine engine=new DuplexEngine(32,64)) {
            BitSet seen=new BitSet(total);int submitted=0,received=0;
            while(received<total) {
                while(submitted<total) {
                    long id=engine.trySubmit(submitted*3L,0,5*SECOND);
                    if(id<0)break;
                    check(id==submitted,"IDs skipped");submitted++;
                }
                DuplexEngine.Completion c=take(engine);
                check(c.id>=0 && c.id<total && !seen.get((int)c.id),"Missing/duplicate ID");
                check(c.value==c.id*3 && c.result==DuplexEngine.Result.OK,"Payload publication/result failed");
                seen.set((int)c.id);received++;
            }
            check(engine.outstanding()==0,"Leaked credits");
            waitFor(()->engine.timerCount()==0 && engine.activeCount()==0,"Retained timer state");
            waitFor(()->engine.parks()>0,"Never parked");
            long dispatched=engine.dispatched(),completed=engine.completed();
            Thread.sleep(25);
            check(engine.dispatched()==dispatched && engine.completed()==completed,"Idle work changed");
            System.out.println("PASS 100000 two-thread operations: exact IDs/payloads, bounded credits, timers empty, idle park; wakeups="+engine.wakeups());
        }
    }
}
