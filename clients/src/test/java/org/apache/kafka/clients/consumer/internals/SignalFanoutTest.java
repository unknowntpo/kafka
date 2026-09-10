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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SignalFanoutTest {
    @ParameterizedTest
    @ValueSource(ints = {1, 8, 64})
    public void broadcastAndMultipleInputsCoalesceWithoutLosingLatestState(int managerCount) {
        NextPollCondition.Signal first = new NextPollCondition.Signal();
        NextPollCondition.Signal second = new NextPollCondition.Signal();
        int[] state = {0};
        int[] calls = new int[managerCount];
        int[] observed = new int[managerCount];
        List<RequestManager> managers = new ArrayList<>();
        for (int index = 0; index < managerCount; index++) {
            final int id = index;
            managers.add(now -> {
                calls[id]++;
                observed[id] = state[0];
                return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(),
                    NextPollCondition.anyOf(first.await(), NextPollCondition.anyOf(second.await(),
                        NextPollCondition.after(now, 10))));
            });
        }
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            scheduler.registerManagers(managers);
            scheduler.pollReady(0, result -> result.timeUntilNextPollMs);
            for (int generation = 1; generation <= 5; generation++) {
                assertEquals(managerCount, first.subscriberCount());
                assertEquals(managerCount, second.subscriberCount());
                state[0]++;
                first.publish();
                state[0]++;
                second.publish();
                state[0]++;
                first.publish();
                assertEquals(0, first.subscriberCount());
                assertEquals(0, second.subscriberCount());
                for (int id = 0; id < managerCount; id++)
                    assertEquals(generation, calls[id]); // Publication never runs a manager inline.
                scheduler.pollReady(generation, result -> result.timeUntilNextPollMs);
                for (int id = 0; id < managerCount; id++) {
                    assertEquals(generation + 1, calls[id]);
                    assertEquals(3 * generation, observed[id]);
                }
                scheduler.pollReady(generation, result -> result.timeUntilNextPollMs);
                for (int id = 0; id < managerCount; id++)
                    assertEquals(generation + 1, calls[id]);
            }
            // Old deadlines must have been cancelled along with each completed subscription.
            scheduler.pollReady(10, result -> result.timeUntilNextPollMs);
            for (int id = 0; id < managerCount; id++)
                assertEquals(6, calls[id]);
            assertEquals(5, scheduler.remainingMs(10));
        }
        assertEquals(0, first.subscriberCount());
        assertEquals(0, second.subscriberCount());
    }

    @Test
    public void broadcastAfterCloseCannotActivateReusedManagerSlots() {
        NextPollCondition.Signal oldInput = new NextPollCondition.Signal();
        NextPollCondition.Signal newInput = new NextPollCondition.Signal();
        int[] calls = {0, 0};
        List<RequestManager> oldManagers = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            oldManagers.add(now -> {
                calls[0]++;
                return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), oldInput.await());
            });
        }
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            scheduler.registerManagers(oldManagers);
            scheduler.pollReady(0, result -> result.timeUntilNextPollMs);
            assertEquals(8, oldInput.subscriberCount());
            scheduler.close();
            assertEquals(0, oldInput.subscriberCount());
            RequestManager replacement = now -> {
                calls[1]++;
                return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), newInput.await());
            };
            scheduler.registerManagers(Collections.singletonList(replacement));
            scheduler.pollReady(1, result -> result.timeUntilNextPollMs);
            oldInput.publish();
            scheduler.pollReady(2, result -> result.timeUntilNextPollMs);
            assertEquals(8, calls[0]);
            assertEquals(1, calls[1]);
            assertEquals(Long.MAX_VALUE, scheduler.remainingMs(2));
            newInput.publish();
            scheduler.pollReady(3, result -> result.timeUntilNextPollMs);
            assertEquals(2, calls[1]);
        }
    }
}
