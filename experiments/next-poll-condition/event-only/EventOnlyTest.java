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

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Bounded executable checks, using actual generated network loop and deterministic boundaries. */
public class EventOnlyTest {
    private static void check(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }
    private static NetworkClientDelegate.PollResult await(NextPollCondition condition) {
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), condition);
    }
    public static void main(String[] args) throws Exception {
        NextPollCondition.Signal signal = new NextPollCondition.Signal();
        int[] polls = {0};
        try (NetworkLoopFixture f = new NetworkLoopFixture(Collections.singletonList(now -> {
            polls[0]++;
            return await(NextPollCondition.anyOf(signal.await(), NextPollCondition.after(now, 10)));
        }))) {
            f.runOnce();
            check(f.transport.waitMs == 10, "timer wait");
            f.clock.now += 8;
            f.runOnce();
            check(polls[0] == 1 && f.transport.waitMs == 1, "no early timer");
            f.runOnce();
            check(polls[0] == 2, "timer expiry wakes only owner");
            signal.publish();
            f.runOnce();
            check(polls[0] == 3, "signal wins over timer");
        }
        check(signal.subscriberCount() == 0, "close detaches");
        System.out.println("PASS deadline, early signal, cancellation, close");

        NextPollCondition.Signal a = new NextPollCondition.Signal();
        NextPollCondition.Signal b = new NextPollCondition.Signal();
        int[] calls = {0, 0};
        try (NetworkLoopFixture f = new NetworkLoopFixture(Arrays.asList(
            now -> { if (++calls[0] == 2) b.publish(); return await(a.await()); },
            now -> { calls[1]++; return await(b.await()); }))) {
            f.runOnce(); a.publish(); f.runOnce();
            check(calls[1] == 1 && f.transport.waitMs == 0, "deferred work prevents sleep");
            f.runOnce(); check(calls[1] == 2, "next snapshot");
        }
        System.out.println("PASS cross-manager next-loop notification");

        NextPollCondition.Signal before = new NextPollCondition.Signal();
        NextPollCondition captured = before.await(); before.publish();
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        scheduler.registerManagers(Collections.singletonList(now -> await(captured)));
        scheduler.pollReady(1, result -> Long.MAX_VALUE);
        check(scheduler.remainingMs(1) == 0, "publish before subscribe is retained");
        scheduler.close();
        System.out.println("PASS publication before condition registration");

        RequestManagerScheduler strict = new RequestManagerScheduler();
        strict.registerManagers(Collections.singletonList(now -> NetworkClientDelegate.PollResult.EMPTY));
        try { strict.pollReady(1, result -> Long.MAX_VALUE); throw new AssertionError("legacy accepted"); }
        catch (IllegalStateException expected) { }
        strict.close();
        System.out.println("PASS missing condition rejected");

        int[] attempts = {0, 0};
        NextPollCondition.Signal retry = new NextPollCondition.Signal();
        try (NetworkLoopFixture f = new NetworkLoopFixture(Arrays.asList(
            now -> { if (attempts[0]++ == 0) throw new IllegalStateException(); return await(retry.await()); },
            now -> { attempts[1]++; return await(retry.await()); }))) {
            try { f.runOnce(); throw new AssertionError("expected exception"); }
            catch (IllegalStateException expected) { }
            f.runOnce(); check(attempts[0] == 2 && attempts[1] == 1, "exception preserves suffix");
        }
        System.out.println("PASS exception preserves remaining ready work");

        // Crossing word boundaries checks that only the unfinished suffix is restored.
        int[] manyCalls = new int[130];
        java.util.List<RequestManager> many = new java.util.ArrayList<>();
        for (int i = 0; i < manyCalls.length; i++) {
            final int index = i;
            NextPollCondition.Signal idle = new NextPollCondition.Signal();
            many.add(now -> {
                if (manyCalls[index]++ == 0 && index == 65) throw new IllegalStateException("middle");
                return await(idle.await());
            });
        }
        try (NetworkLoopFixture f = new NetworkLoopFixture(many)) {
            try { f.runOnce(); throw new AssertionError("expected middle failure"); }
            catch (IllegalStateException expected) { }
            f.runOnce();
            for (int i = 0; i < manyCalls.length; i++)
                check(manyCalls[i] == (i == 65 ? 2 : 1), "suffix restoration at " + i);
        }
        System.out.println("PASS completed prefix not replayed; unfinished suffix preserved across bitset words");

        ApplicationProgress gate = new ApplicationProgress();
        long observed = gate.generation(); gate.publish();
        check(gate.awaitChange(observed, 0), "notification before wait");
        check(!gate.awaitChange(gate.generation(), 1000000), "user timeout retained");
        AtomicBoolean state = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch checked = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            try {
                long version = gate.generation();
                if (!state.get()) {
                    checked.countDown();
                    check(gate.awaitChange(version, TimeUnit.SECONDS.toNanos(2)), "blocked waiter notified");
                }
                check(state.get(), "state published before notification");
            } catch (Throwable t) { failure.set(t); }
        });
        waiter.start();
        check(checked.await(2, TimeUnit.SECONDS), "waiter ready");
        // Taking the monitor after observing TIMED_WAITING proves publication occurs after wait releases it.
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (waiter.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < limit) Thread.yield();
        check(waiter.getState() == Thread.State.TIMED_WAITING, "waiter is blocked");
        state.set(true); gate.publish(); waiter.join(2000);
        check(!waiter.isAlive() && failure.get() == null, "wait completed: " + failure.get());
        System.out.println("PASS application notify-before-wait, blocked wake, timeout");

        try (NetworkLoopFixture f = new NetworkLoopFixture(Collections.singletonList(now -> await(new NextPollCondition.Signal().await())))) {
            ApplicationProgress progress = f.thread.applicationProgress();
            long version = progress.generation(); f.runOnce();
            check(progress.awaitChange(version, 0), "loop publishes progress");
        }
        for (String pattern : new String[] {"IDLE", "SPARSE", "BUSY", "TIMER"}) {
            try (UnifiedLoopWorkload w = new UnifiedLoopWorkload(32, pattern, true, false)) {
                for (int i = 0; i < 10000; i++) w.pass();
                check(w.maximumWaitCalls == 0, "maximumTimeToWait was scanned");
                check(w.published == w.consumed, "unconsumed work");
                System.out.println("WORK," + pattern + "," + w.published + "," + w.consumed + "," + w.managerPolls + "," + w.maximumWaitCalls);
            }
        }
        System.out.println("PASS full-loop progress notification and zero maximumTimeToWait scans");
    }
}
