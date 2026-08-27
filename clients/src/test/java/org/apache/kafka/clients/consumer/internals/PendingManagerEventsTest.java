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
        pendingEvents.add(new ManagerEvent.CoordinatorInvalidation("heartbeat", "first", 1L));
        pendingEvents.add(new ManagerEvent.CoordinatorInvalidation("heartbeat", "latest", 2L));

        NetworkClientDelegate.PollResult result = pendingEvents.publishWith(
            NetworkClientDelegate.PollResult.awaitEvent());

        assertEquals(1, result.managerEvents().size());
        ManagerEvent.CoordinatorInvalidation invalidation = assertInstanceOf(
            ManagerEvent.CoordinatorInvalidation.class, result.managerEvents().get(0));
        assertEquals("latest", invalidation.cause());
        assertEquals(2L, invalidation.observedAtMs());
        assertTrue(pendingEvents.publishWith(NetworkClientDelegate.PollResult.awaitEvent())
            .managerEvents().isEmpty());
    }
}
