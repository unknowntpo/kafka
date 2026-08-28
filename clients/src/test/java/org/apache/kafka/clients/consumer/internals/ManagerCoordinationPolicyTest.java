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

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

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
    public void testDifferentProgressFactsInOneBatchCoalesceApplicationWakeAction() {
        CoordinationPlan plan = policy.evaluate(List.of(
            ManagerEvent.FetchBufferHasData.INSTANCE,
            ManagerEvent.LocalProgress.FETCH_PREPARATION_FAILED,
            ManagerEvent.LocalProgress.FETCH_POSITIONS_UPDATE_FAILED
        ));

        assertEquals(List.of(), plan.managerCommands());
        assertEquals(List.of(ReactorAction.wakeApplication()), plan.reactorActions());
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
    public void testStandardPolicyCoversEveryDeclaredSemanticType() {
        assertEquals(EnumSet.allOf(ManagerEvent.Type.class), policy.handledTypes());
    }

    @Test
    public void testMissingSemanticTypeFailsAtPolicyConstruction() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> new ManagerCoordinationPolicy(List.of(
                new FetchBufferHasDataHandler(),
                new LocalProgressHandler()
            )));

        assertEquals(
            "Missing manager-event handlers for [COORDINATOR_UNAVAILABLE_OBSERVED]",
            exception.getMessage()
        );
    }

    @Test
    public void testDispatchUsesSemanticTypeRatherThanConcreteEventClass() {
        ManagerEventHandler<ManagerEvent> genericHandler = new ManagerEventHandler<>() {
            @Override
            public Set<ManagerEvent.Type> eventTypes() {
                return EnumSet.allOf(ManagerEvent.Type.class);
            }

            @Override
            public Class<ManagerEvent> eventClass() {
                return ManagerEvent.class;
            }

            @Override
            public CoordinationPlan handle(final ManagerEvent event) {
                return new CoordinationPlan(List.of(), List.of());
            }
        };
        ManagerCoordinationPolicy typeDispatchedPolicy = new ManagerCoordinationPolicy(List.of(genericHandler));
        ManagerEvent eventWithUnregisteredConcreteClass = new ManagerEvent() {
            @Override
            public Type type() {
                return Type.FETCH_BUFFER_HAS_DATA;
            }

            @Override
            public String source() {
                return "unknown";
            }
        };

        CoordinationPlan plan = typeDispatchedPolicy.evaluate(List.of(eventWithUnregisteredConcreteClass));
        assertEquals(List.of(), plan.managerCommands());
        assertEquals(List.of(), plan.reactorActions());
    }
}
