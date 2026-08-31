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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PendingManagerEventsTest {

    @Test
    public void testRepeatedEventTypeIsBoundedAndRetainsLatestDiagnostic() {
        PendingManagerEvents pendingEvents = new PendingManagerEvents();
        pendingEvents.add(new ManagerEvent.CoordinatorUnavailableObserved("heartbeat", "first", 1L, 7L));
        pendingEvents.add(new ManagerEvent.CoordinatorUnavailableObserved("heartbeat", "latest", 2L, 7L));

        NetworkClientDelegate.PollResult result = pendingEvents.publishWith(
            NetworkClientDelegate.PollResult.awaitInput());

        assertEquals(1, result.managerEvents().size());
        ManagerEvent.CoordinatorUnavailableObserved invalidation = assertInstanceOf(
            ManagerEvent.CoordinatorUnavailableObserved.class, result.managerEvents().get(0));
        assertEquals("latest", invalidation.cause());
        assertEquals(2L, invalidation.observedAtMs());
        assertEquals(7L, invalidation.observedCoordinatorVersion());
        assertTrue(pendingEvents.publishWith(NetworkClientDelegate.PollResult.awaitInput())
            .managerEvents().isEmpty());
    }

    @Test
    public void testDelayedOlderVersionCannotReplaceNewerObservation() {
        PendingManagerEvents pendingEvents = new PendingManagerEvents();
        pendingEvents.add(new ManagerEvent.CoordinatorUnavailableObserved("heartbeat", "current", 1L, 9L));
        pendingEvents.add(new ManagerEvent.CoordinatorUnavailableObserved("heartbeat", "delayed", 2L, 7L));

        ManagerEvent.CoordinatorUnavailableObserved observation = assertInstanceOf(
            ManagerEvent.CoordinatorUnavailableObserved.class,
            pendingEvents.publishWith(NetworkClientDelegate.PollResult.awaitInput()).managerEvents().get(0));

        assertEquals("current", observation.cause());
        assertEquals(9L, observation.observedCoordinatorVersion());
    }

    @Test
    public void testInputBoundaryDrainIsLatestOnlyAndClearsBuffer() {
        PendingManagerEvents pendingEvents = new PendingManagerEvents();
        pendingEvents.add(new ManagerEvent.CoordinatorUnavailableObserved("heartbeat", "older", 1L, 7L));
        pendingEvents.add(new ManagerEvent.CoordinatorUnavailableObserved("heartbeat", "newer", 2L, 9L));

        ManagerEvent.CoordinatorUnavailableObserved observation = assertInstanceOf(
            ManagerEvent.CoordinatorUnavailableObserved.class,
            pendingEvents.drain().get(0)
        );

        assertEquals(9L, observation.observedCoordinatorVersion());
        assertTrue(pendingEvents.drain().isEmpty());
    }
}
