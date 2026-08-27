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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ManagerCoordinationPolicyTest {

    private final ManagerCoordinationPolicy policy = ManagerCoordinationPolicy.standard();

    @Test
    public void testFetchBufferFactProducesApplicationWakeAction() {
        CoordinationPlan plan = policy.evaluate(List.of(ManagerEvent.FetchBufferHasData.INSTANCE));

        assertEquals(List.of(), plan.managerCommands());
        assertEquals(1, plan.reactorActions().size());
        assertSame(ReactorAction.wakeApplication(), plan.reactorActions().get(0));
    }

    @Test
    public void testCoordinatorObservationProducesTargetedOwnerCommand() {
        ManagerEvent.CoordinatorUnavailableObserved observation =
            new ManagerEvent.CoordinatorUnavailableObserved("heartbeat", "not coordinator", 42L, 7L);

        CoordinationPlan plan = policy.evaluate(List.of(observation));

        assertEquals(List.of(), plan.reactorActions());
        ManagerCommand.InvalidateCoordinatorIfCurrent command = assertInstanceOf(
            ManagerCommand.InvalidateCoordinatorIfCurrent.class,
            plan.managerCommands().get(0)
        );
        assertSame(observation, command.observation());
    }

    @Test
    public void testUnknownManagerEventFailsFast() {
        ManagerEvent unknown = new ManagerEvent() {
            @Override
            public Type type() {
                return Type.FETCH_BUFFER_HAS_DATA;
            }

            @Override
            public String source() {
                return "unknown";
            }
        };

        assertThrows(IllegalArgumentException.class, () -> policy.evaluate(List.of(unknown)));
    }
}
