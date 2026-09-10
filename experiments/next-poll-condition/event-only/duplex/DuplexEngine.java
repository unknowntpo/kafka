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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/** Two-thread contract prototype. Fake RPC completion is driven by the owner's timer queue. */
public final class DuplexEngine implements AutoCloseable {
    public enum Result { OK, TIMEOUT, CANCELLED }
    public static final class Completion {
        public final long id;
        public final long value;
        public final Result result;
        Completion(long id, long value, Result result) { this.id=id; this.value=value; this.result=result; }
    }
    static final class Command {
        final long id, value, responseAt, expiresAt;
        final boolean cancel;
        Command(long id, long value, long responseAt, long expiresAt, boolean cancel) {
            this.id=id;this.value=value;this.responseAt=responseAt;this.expiresAt=expiresAt;this.cancel=cancel;
        }
    }
    /** Exactly one producer and one consumer. Volatile indices publish/read array elements. */
    static final class Ring<T> {
        private final Object[] cells;
        private final int mask;
        private volatile long head;
        private volatile long tail;
        Ring(int capacity) {
            if(capacity<2 || Integer.bitCount(capacity)!=1)throw new IllegalArgumentException("Power-of-two capacity >= 2 required");
            cells=new Object[capacity];mask=capacity-1;
        }
        int capacity(){return cells.length;}
        long size(){return tail-head;}
        boolean isEmpty(){return head==tail;}
        boolean offer(T value) {
            long t=tail;
            if(t-head==cells.length)return false;
            cells[(int)t&mask]=value;
            tail=t+1;
            return true;
        }
        @SuppressWarnings("unchecked")
        T poll() {
            long h=head;
            if(h==tail)return null;
            T value=(T)cells[(int)h&mask];cells[(int)h&mask]=null;
            head=h+1;
            return value;
        }
    }
    static final class Operation {
        final Command command;
        final Deadline response, timeout;
        Operation(Command command) {
            this.command=command;
            response=new Deadline(command.responseAt,command.id,false);
            timeout=new Deadline(command.expiresAt,command.id,true);
        }
    }
    static final class Deadline implements Comparable<Deadline> {
        final long at,id;final boolean timeout;
        Deadline(long at,long id,boolean timeout){this.at=at;this.id=id;this.timeout=timeout;}
        public int compareTo(Deadline other) {
            int c=Long.compare(at,other.at);
            if(c==0)c=Boolean.compare(other.timeout,timeout); // Timeout wins an exact tie.
            if(c==0)c=Long.compare(id,other.id);
            return c;
        }
    }
    interface IdleHook { void beforePark(); }
    private final Thread app=Thread.currentThread();
    private final Thread background;
    private final Ring<Command> submissions;
    private final Ring<Completion> completions;
    private final AtomicBoolean backgroundSleeping=new AtomicBoolean();
    private final AtomicBoolean appSleeping=new AtomicBoolean();
    private final AtomicBoolean closing=new AtomicBoolean();
    private final Map<Long,Operation> active=new HashMap<>();
    private final TreeSet<Deadline> deadlines=new TreeSet<>();
    private final long origin=System.nanoTime();
    private final IdleHook idleHook;
    private long nextId;
    private int outstanding; // App-thread-owned admission credits.
    private volatile Throwable failure;
    private volatile boolean terminated;
    private volatile long dispatched, completed, timerEvents, parks, wakeups;
    private volatile int activeCount, timerCount;

    public DuplexEngine(int submissionCapacity,int completionCapacity) {this(submissionCapacity,completionCapacity,()->{});}
    DuplexEngine(int submissionCapacity,int completionCapacity,IdleHook idleHook) {
        submissions=new Ring<>(submissionCapacity);completions=new Ring<>(completionCapacity);this.idleHook=idleHook;
        background=new Thread(this::run,"duplex-background");background.setDaemon(true);background.start();
    }
    private void checkApp() {
        if(Thread.currentThread()!=app)throw new IllegalStateException("Single app-thread owner required");
        if(failure!=null)throw new IllegalStateException("Background failed",failure);
    }
    long now(){return System.nanoTime()-origin;}
    private long deadline(long delay) {
        if(delay<0 || delay>3_600_000_000_000L)throw new IllegalArgumentException("Delay outside [0, 1 hour]");
        return now()+delay;
    }
    /** Returns -1 immediately on backpressure/close. A successful submission reserves one CQ slot. */
    public long trySubmit(long value,long responseDelayNanos,long timeoutNanos) {
        checkApp();
        if(responseDelayNanos<0 || responseDelayNanos>3_600_000_000_000L || timeoutNanos<0 || timeoutNanos>3_600_000_000_000L)
            throw new IllegalArgumentException("Delay outside [0, 1 hour]");
        long submittedAt=now();
        long response=submittedAt+responseDelayNanos,timeout=submittedAt+timeoutNanos;
        if(closing.get() || outstanding==completions.capacity() || submissions.size()>=submissions.capacity()-1)return -1;
        long id=nextId++;
        if(!submissions.offer(new Command(id,value,response,timeout,false)))throw new AssertionError("SPSC admission invariant");
        outstanding++;
        signalBackground();return id;
    }
    /** Acceptance is not cancellation success; the original completion reports the terminal winner. */
    public boolean tryCancel(long id) {
        checkApp();
        if(closing.get() || !submissions.offer(new Command(id,0,0,0,true)))return false;
        signalBackground();return true;
    }
    public Completion pollCompletion() {
        checkApp();Completion result=completions.poll();
        if(result!=null)outstanding--;
        return result;
    }
    /** Optional app-side wait. Submission and completion polling remain nonblocking APIs. */
    public Completion awaitCompletion(long timeoutNanos) {
        checkApp();long until=deadline(timeoutNanos);
        for(;;) {
            Completion result=pollCompletion();if(result!=null)return result;
            long remaining=until-now();if(remaining<=0 || terminated)return null;
            appSleeping.set(true);
            result=pollCompletion();
            if(result!=null){appSleeping.set(false);return result;}
            if(terminated){appSleeping.set(false);return null;}
            LockSupport.parkNanos(this,remaining);
            appSleeping.set(false);
            if(Thread.currentThread().isInterrupted())throw new IllegalStateException("App wait interrupted");
        }
    }
    private void signalBackground() {
        if(backgroundSleeping.getAndSet(false)){wakeups++;LockSupport.unpark(background);}
    }
    private void finish(long id,Result result) {
        Operation operation=active.remove(id);
        if(operation==null)return; // Late response/cancel/timeout cannot complete twice.
        deadlines.remove(operation.response);deadlines.remove(operation.timeout);
        if(!completions.offer(new Completion(id,operation.command.value,result)))throw new AssertionError("Reserved completion credit missing");
        completed++;
        if(appSleeping.getAndSet(false))LockSupport.unpark(app);
    }
    private void run() {
        try {
            for(;;) {
                // Bound each batch so submission load cannot indefinitely starve timers.
                for(int i=0;i<64;i++) {
                    Command command=submissions.poll();if(command==null)break;
                    if(command.cancel){finish(command.id,Result.CANCELLED);continue;}
                    Operation operation=new Operation(command);active.put(command.id,operation);dispatched++;
                    deadlines.add(operation.response);deadlines.add(operation.timeout);
                    if(closing.get())finish(command.id,Result.CANCELLED);
                }
                if(closing.get())for(Long id:active.keySet().toArray(new Long[0]))finish(id,Result.CANCELLED);
                for(int i=0;i<64 && !deadlines.isEmpty();i++) {
                    Deadline due=deadlines.first();if(due.at>now())break;
                    timerEvents++;finish(due.id,due.timeout?Result.TIMEOUT:Result.OK);
                }
                activeCount=active.size();timerCount=deadlines.size();
                if(closing.get() && submissions.isEmpty() && active.isEmpty())return;
                backgroundSleeping.set(true);
                if(!submissions.isEmpty() || closing.get()) {backgroundSleeping.set(false);continue;}
                long wait=deadlines.isEmpty()?Long.MAX_VALUE:deadlines.first().at-now();
                if(wait<=0){backgroundSleeping.set(false);continue;}
                // Publication before arming is caught by the queue recheck; publication after
                // recheck supplies an unpark permit, including before park actually executes.
                idleHook.beforePark();parks++;
                if(wait==Long.MAX_VALUE)LockSupport.park(this);else LockSupport.parkNanos(this,wait);
                backgroundSleeping.set(false);
            }
        } catch(Throwable error) {
            failure=error;
        } finally {
            terminated=true;LockSupport.unpark(app);
        }
    }
    /** Nonblocking request; accepted operations receive cancellation completions during shutdown. */
    public void requestClose(){checkApp();closing.set(true);signalBackground();}
    public boolean isTerminated(){return terminated;}
    public int outstanding(){checkApp();return outstanding;}
    public long completed(){return completed;}
    public long dispatched(){return dispatched;}
    public long parks(){return parks;}
    public long wakeups(){return wakeups;}
    public long timerEvents(){return timerEvents;}
    public int activeCount(){return activeCount;}
    public int timerCount(){return timerCount;}
    public void close() {
        requestClose();
        try{background.join(5000);}catch(InterruptedException e){Thread.currentThread().interrupt();throw new IllegalStateException(e);}
        if(background.isAlive())throw new AssertionError("Background close stalled");
        checkApp();
    }
}
