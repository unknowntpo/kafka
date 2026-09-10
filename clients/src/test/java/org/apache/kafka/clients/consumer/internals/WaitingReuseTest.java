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

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class WaitingReuseTest {
    @Test
    public void resetsDeadlineAndSignalStateAcrossDifferentWaits() {
        NextPollCondition.Signal first = new NextPollCondition.Signal();
        NextPollCondition.Signal second = new NextPollCondition.Signal();
        int[] polls = {0};
        RequestManager manager = now -> {
            polls[0]++;
            NextPollCondition condition = polls[0] == 1 ? NextPollCondition.anyOf(first.await(), NextPollCondition.after(now, 10)) :
                polls[0] == 2 ? second.await() : NextPollCondition.after(now, 100);
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), condition);
        };
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            scheduler.poll(manager, 0);
            first.publish();
            scheduler.poll(manager, 1);
            assertNull(scheduler.poll(manager, 10));
            assertEquals(Long.MAX_VALUE, scheduler.remainingMs(10));
            first.publish();
            assertNull(scheduler.poll(manager, 11));
            second.publish();
            scheduler.poll(manager, 12);
            assertEquals(100, scheduler.remainingMs(12));
            assertNull(scheduler.poll(manager, 111));
            assertNotNull(scheduler.poll(manager, 112));
        }
    }

    @Test
    public void publicationBeforeRegistrationDoesNotLeaveNextWaitSignalled() {
        NextPollCondition.Signal signal = new NextPollCondition.Signal();
        NextPollCondition captured = signal.await();
        signal.publish();
        int[] polls = {0};
        RequestManager manager = now -> new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(),
            polls[0]++ == 0 ? captured : signal.await());
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            scheduler.poll(manager, 0);
            scheduler.poll(manager, 0);
            assertNull(scheduler.poll(manager, 0));
            assertEquals(1, signal.subscriberCount());
            signal.publish();
            assertNotNull(scheduler.poll(manager, 1));
            assertNull(scheduler.poll(manager, 1));
            assertEquals(3, polls[0]);
        }
    }

    @Test
    public void failedRegistrationDetachesInputsBeforeReusingSlot() {
        NextPollCondition.Signal failedInput = new NextPollCondition.Signal();
        NextPollCondition.Signal nextInput = new NextPollCondition.Signal();
        int[] polls = {0};
        NextPollCondition broken = new NextPollCondition() {
            @Override
            public void register(Registration registration) {
                failedInput.await().register(registration);
                registration.onDeadline(1);
                throw new IllegalStateException("registration failed");
            }
            @Override
            public boolean isReady(long now) {
                throw new AssertionError("not scanned");
            }
            @Override
            public long remainingMs(long now) {
                throw new AssertionError("not scanned");
            }
        };
        RequestManager manager = now -> new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(),
            polls[0]++ == 0 ? broken : nextInput.await());
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            assertThrows(IllegalStateException.class, () -> scheduler.poll(manager, 0));
            assertEquals(0, failedInput.subscriberCount());
            assertNotNull(scheduler.poll(manager, 0));
            failedInput.publish();
            assertNull(scheduler.poll(manager, 1));
            nextInput.publish();
            assertNotNull(scheduler.poll(manager, 2));
            assertEquals(1, nextInput.subscriberCount());
        }
    }

    @Test
    public void returningThroughLegacyModeDoesNotKeepOldDeadline() {
        NextPollCondition.Signal next = new NextPollCondition.Signal();
        int[] polls = {0};
        RequestManager manager = now -> {
            int n = polls[0]++;
            if (n == 1)
                return NetworkClientDelegate.PollResult.EMPTY;
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(),
                n == 0 ? NextPollCondition.after(now, 1) : next.await());
        };
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            scheduler.poll(manager, 0);
            scheduler.poll(manager, 1);
            scheduler.poll(manager, 2);
            assertNull(scheduler.poll(manager, 3));
            assertEquals(Long.MAX_VALUE, scheduler.remainingMs(3));
            next.publish();
            assertNotNull(scheduler.poll(manager, 4));
            assertEquals(4, polls[0]);
        }
    }
}
