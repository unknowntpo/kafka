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

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

public class ReuseTest {
    private static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }
    private static Object field(Object owner, String name) throws Exception {
        Field f = owner.getClass().getDeclaredField(name); f.setAccessible(true); return f.get(owner);
    }
    private static NetworkClientDelegate.PollResult waitOn(NextPollCondition condition) {
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), condition);
    }
    public static void main(String[] args) throws Exception {
        EventOnlyTest.main(args);
        NextPollCondition.Signal signal = new NextPollCondition.Signal();
        int[] calls = {0};
        RequestManager manager = now -> { calls[0]++; return waitOn(signal.await()); };
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        scheduler.registerManagers(Collections.singletonList(manager));
        scheduler.pollReady(0, r -> Long.MAX_VALUE);
        Object entry = ((Map<?, ?>) field(scheduler, "entries")).get(manager);
        Object waiter = field(entry, "waiting");
        Object subscription = field(waiter, "firstSubscription");
        for (int i = 1; i <= 10000; i++) {
            signal.publish(); scheduler.pollReady(i, r -> Long.MAX_VALUE);
            check(field(entry, "waiting") == waiter, "waiter was allocated again");
            check(field(waiter, "firstSubscription") == subscription, "subscription was allocated again");
            check(signal.subscriberCount() == 1, "duplicate registration");
        }
        check(calls[0] == 10001, "missed input"); scheduler.close();
        check(signal.subscriberCount() == 0, "close retained subscriber");
        System.out.println("PASS 10,000 re-arms reuse the same waiter and subscription; close detaches");

        NextPollCondition.Signal a = new NextPollCondition.Signal();
        NextPollCondition.Signal b = new NextPollCondition.Signal();
        int[] step = {0};
        try (RequestManagerScheduler s = new RequestManagerScheduler()) {
            RequestManager m = now -> {
                int n = step[0]++;
                return waitOn(n % 3 == 0 ? NextPollCondition.anyOf(a.await(), b.await())
                    : n % 3 == 1 ? b.await() : NextPollCondition.after(now, 1));
            };
            s.registerManagers(Collections.singletonList(m)); s.pollReady(0, r -> Long.MAX_VALUE);
            for (int i = 1; i <= 300; i++) {
                a.publish(); b.publish(); s.pollReady(i, r -> Long.MAX_VALUE);
                check(a.subscriberCount() <= 1 && b.subscriberCount() <= 1, "stale multi-signal slots");
            }
            check(step[0] == 301, "signal/deadline transition missed");
        }
        check(a.subscriberCount() == 0 && b.subscriberCount() == 0, "multi-signal close");
        System.out.println("PASS reusable waiter switches multi-signal/single-signal/deadline modes");

        NextPollCondition.Signal publication = new NextPollCondition.Signal();
        NextPollCondition.Signal.Subscription[] pending = {null};
        NextPollCondition.Signal.Subscription[] replacement = {null};
        int[] invoked = {0, 0, 0};
        publication.subscribe(0, () -> replacement[0] = publication.subscribeReusable(1,
            () -> invoked[1]++, pending[0]));
        pending[0] = publication.subscribeReusable(0, () -> invoked[0]++, null);
        publication.subscribe(0, () -> invoked[2]++);
        publication.publish();
        check(pending[0] != replacement[0], "reused a node still in detached publication");
        check(invoked[0] == 0 && invoked[1] == 0 && invoked[2] == 1, "corrupted publication suffix");
        check(publication.subscriberCount() == 1, "replacement not registered");
        publication.publish(); check(invoked[1] == 1, "replacement not notified later");
        NextPollCondition.Signal.Subscription reused = publication.subscribeReusable(2, () -> invoked[1]++, replacement[0]);
        check(reused == replacement[0], "completed node could not be reused"); reused.run();
        System.out.println("PASS reentrant re-arm preserves detached publication; safe fallback allocation");

        NextPollCondition.Signal once = new NextPollCondition.Signal();
        int[] count = {0};
        Runnable old = once.subscribe(0, () -> count[0]++); once.publish();
        once.subscribe(1, () -> count[0]++); old.run(); once.publish();
        check(count[0] == 2, "stale public cancellation canceled new subscriber");
        System.out.println("PASS public one-shot cancellation handle remains generation-safe");
        NextPollCondition.Signal outer = new NextPollCondition.Signal();
        NextPollCondition.Signal trigger = new NextPollCondition.Signal();
        NextPollCondition.Signal fresh = new NextPollCondition.Signal();
        int[] rearmed = {0};
        try (RequestManagerScheduler nested = new RequestManagerScheduler()) {
            outer.subscribe(0, () -> {
                trigger.publish();
                nested.pollReady(1, r -> Long.MAX_VALUE);
            });
            RequestManager m = now -> {
                int round = ++rearmed[0];
                return waitOn(round == 1 ? NextPollCondition.anyOf(outer.await(), trigger.await())
                    : round == 2 ? NextPollCondition.anyOf(outer.await(), fresh.await())
                    : NextPollCondition.after(now, 10));
            };
            nested.registerManagers(Collections.singletonList(m)); nested.pollReady(0, r -> Long.MAX_VALUE);
            outer.publish();
            check(rearmed[0] == 2 && nested.remainingMs(1) == Long.MAX_VALUE, "old pending callback woke new wait");
            check(outer.subscriberCount() == 1 && fresh.subscriberCount() == 1, "new subscriptions canceled");
            fresh.publish(); nested.pollReady(2, r -> Long.MAX_VALUE);
            outer.publish(); check(nested.remainingMs(2) == 10, "old signal woke timer wait");
            nested.pollReady(11, r -> Long.MAX_VALUE); check(rearmed[0] == 3, "stale deadline");
            nested.pollReady(12, r -> Long.MAX_VALUE); check(rearmed[0] == 4, "new timer lost");
        }
        System.out.println("PASS Waiting re-arm during another publication cannot invoke stale callbacks");

        NextPollCondition.Signal partial = new NextPollCondition.Signal();
        NextPollCondition.Signal valid = new NextPollCondition.Signal();
        int[] tries = {0};
        try (RequestManagerScheduler retry = new RequestManagerScheduler()) {
            RequestManager m = now -> ++tries[0] == 1 ? waitOn(new NextPollCondition() {
                public void register(Registration registration) {
                    registration.onSignal(partial, 0); registration.onDeadline(5);
                    throw new IllegalStateException("partial registration");
                }
                public boolean isReady(long time) { return false; }
                public long remainingMs(long time) { return Long.MAX_VALUE; }
            }) : waitOn(valid.await());
            retry.registerManagers(Collections.singletonList(m));
            try { retry.pollReady(0, r -> Long.MAX_VALUE); throw new AssertionError("expected registration failure"); }
            catch (IllegalStateException expected) { }
            check(partial.subscriberCount() == 0, "partial signal was retained");
            Object e = ((Map<?, ?>) field(retry, "entries")).get(m);
            Object failedWaiter = field(e, "waiting");
            retry.pollReady(1, r -> Long.MAX_VALUE);
            check(field(e, "waiting") == failedWaiter, "failed waiter not reusable");
            partial.publish(); check(retry.remainingMs(10) == Long.MAX_VALUE, "partial timer/signal retained");
            valid.publish(); retry.pollReady(10, r -> Long.MAX_VALUE);
            check(tries[0] == 3, "retry lost valid notification");
        }
        System.out.println("PASS partial registration failure cancels signal/timer before reuse");
    }
}
