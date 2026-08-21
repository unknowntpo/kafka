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

public class ReactorScheduleTest {

    @Test
    public void testNoManagerDeadlinesProducesUnboundedSchedule() {
        ReactorSchedule schedule = ReactorSchedule.from(List.of(), 42L);

        assertEquals(Long.MAX_VALUE, schedule.networkPollTimeoutMs(42L));
        assertEquals(Long.MAX_VALUE, schedule.remainingMsForApplication(42L));
    }

    @Test
    public void testScheduleChoosesEarliestNetworkAndApplicationDeadlinesIndependently() {
        RequestManager fetch = mock(FetchRequestManager.class);
        RequestManager heartbeat = mock(ConsumerHeartbeatRequestManager.class);
        ManagerPollCache cache = new ManagerPollCache();
        cache.update(fetch, new PollResult(100L), Long.MAX_VALUE, 42L);
        cache.update(heartbeat, new PollResult(25L), 50L, 42L);

        ReactorSchedule schedule = ReactorSchedule.from(cache.states(), 42L);

        assertEquals(25L, schedule.networkPollTimeoutMs(42L));
        assertEquals(50L, schedule.remainingMsForApplication(42L));
        assertEquals(ConsumerHeartbeatRequestManager.class.getSimpleName(), schedule.pollSource().orElseThrow());
    }

    @Test
    public void testElapsedTimeIsSubtractedFromAbsoluteDeadline() {
        RequestManager manager = mock(RequestManager.class);
        ManagerPollCache cache = new ManagerPollCache();
        cache.update(manager, new PollResult(100L), Long.MAX_VALUE, 42L);

        ReactorSchedule schedule = ReactorSchedule.from(cache.states(), 52L);

        assertEquals(90L, schedule.networkPollTimeoutMs(52L));
    }

    @Test
    public void testEarlierHeartbeatPollDoesNotEraseLaterFetchDeadline() {
        RequestManager fetch = mock(FetchRequestManager.class);
        RequestManager heartbeat = mock(ConsumerHeartbeatRequestManager.class);
        ManagerPollCache cache = new ManagerPollCache();
        cache.update(fetch, new PollResult(100L), Long.MAX_VALUE, 0L);
        cache.update(heartbeat, new PollResult(30L), Long.MAX_VALUE, 0L);

        ReactorSchedule first = ReactorSchedule.from(cache.states(), 0L);
        assertEquals(30L, first.networkPollTimeoutMs(0L));

        // An early heartbeat poll reports its next relative interval. The fetch manager's absolute deadline remains
        // cached, so the next global decision still reaches fetch at t=100 instead of drifting to t=130.
        cache.update(heartbeat, new PollResult(30L), Long.MAX_VALUE, 30L);
        ReactorSchedule second = ReactorSchedule.from(cache.states(), 30L);
        assertEquals(30L, second.networkPollTimeoutMs(30L));

        cache.update(heartbeat, new PollResult(100L), Long.MAX_VALUE, 60L);
        ReactorSchedule third = ReactorSchedule.from(cache.states(), 60L);
        assertEquals(40L, third.networkPollTimeoutMs(60L));
        assertEquals(FetchRequestManager.class.getSimpleName(), third.pollSource().orElseThrow());
    }
}
