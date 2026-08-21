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

import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class ManagerPollCacheTest {

    @Test
    public void testEarlyNetworkReturnDoesNotMoveExistingDeadlineForward() {
        RequestManager manager = mock(RequestManager.class);
        ManagerPollCache cache = new ManagerPollCache();

        cache.update(manager, new PollResult(100L), Long.MAX_VALUE, 10L);
        cache.update(manager, new PollResult(100L), Long.MAX_VALUE, 20L);

        ManagerPollCache.PollState state = cache.states().iterator().next();
        assertEquals(110L, state.networkDeadlineMs());
    }

    @Test
    public void testExpiredDeadlineCanBeReplacedAfterManagerPoll() {
        RequestManager manager = mock(RequestManager.class);
        ManagerPollCache cache = new ManagerPollCache();

        cache.update(manager, new PollResult(100L), Long.MAX_VALUE, 10L);
        cache.update(manager, new PollResult(100L), Long.MAX_VALUE, 110L);

        ManagerPollCache.PollState state = cache.states().iterator().next();
        assertEquals(210L, state.networkDeadlineMs());
    }

    @Test
    public void testRetainManagersRemovesInactiveManager() {
        RequestManager first = mock(RequestManager.class);
        RequestManager second = mock(RequestManager.class);
        ManagerPollCache cache = new ManagerPollCache();

        cache.update(first, new PollResult(10L), Long.MAX_VALUE, 0L);
        cache.update(second, new PollResult(20L), Long.MAX_VALUE, 0L);
        cache.retainManagers(List.of(second));

        assertEquals(1, cache.states().size());
        assertEquals(second, cache.states().iterator().next().manager());
    }

    @Test
    public void testDeliveredApplicationDeadlineStaysInactiveUntilManagerChangesIt() {
        RequestManager manager = mock(RequestManager.class);
        ManagerPollCache cache = new ManagerPollCache();
        cache.update(manager, PollResult.EMPTY, 0L, 10L);
        ReactorSchedule schedule = ReactorSchedule.from(cache.states(), 10L);

        cache.markApplicationDeadlineDelivered(schedule);
        cache.update(manager, PollResult.EMPTY, 0L, 10L);

        assertEquals(Long.MAX_VALUE, cache.states().iterator().next().activeApplicationDeadlineMs());
    }
}
