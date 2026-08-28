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

import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Composition-owned policy that evaluates manager facts without owning manager state or reactor phase ordering.
 * Each event type has one typed handler; adding a manager fact does not add a domain switch to ConsumerReactor.
 */
final class ManagerCoordinationPolicy {
    private final Map<ManagerEvent.Type, ManagerEventHandler<? extends ManagerEvent>> handlers;

    ManagerCoordinationPolicy(final Collection<ManagerEventHandler<? extends ManagerEvent>> handlers) {
        Map<ManagerEvent.Type, ManagerEventHandler<? extends ManagerEvent>> byEventType =
            new EnumMap<>(ManagerEvent.Type.class);
        for (ManagerEventHandler<? extends ManagerEvent> handler : handlers) {
            if (handler.eventTypes().isEmpty())
                throw new IllegalArgumentException("Manager-event handler must declare at least one event type");
            for (ManagerEvent.Type eventType : handler.eventTypes()) {
                if (byEventType.put(eventType, handler) != null)
                    throw new IllegalArgumentException("Duplicate handler for " + eventType);
            }
        }

        EnumSet<ManagerEvent.Type> missingTypes = EnumSet.allOf(ManagerEvent.Type.class);
        missingTypes.removeAll(byEventType.keySet());
        if (!missingTypes.isEmpty())
            throw new IllegalArgumentException("Missing manager-event handlers for " + missingTypes);
        this.handlers = Map.copyOf(byEventType);
    }

    static ManagerCoordinationPolicy standard() {
        return new ManagerCoordinationPolicy(List.of(
            new FetchBufferHasDataHandler(),
            new LocalProgressHandler(),
            new CoordinatorUnavailableObservedHandler()
        ));
    }

    CoordinationPlan evaluate(final Collection<ManagerEvent> events) {
        CoordinationPlan.Builder plan = new CoordinationPlan.Builder();
        for (ManagerEvent event : events)
            plan.add(handle(event));
        return plan.build();
    }

    private <E extends ManagerEvent> CoordinationPlan handle(final E event) {
        ManagerEventHandler<? extends ManagerEvent> candidate = handlers.get(event.type());
        return handle(candidate, event);
    }

    private <E extends ManagerEvent> CoordinationPlan handle(final ManagerEventHandler<E> handler,
                                                              final ManagerEvent event) {
        return handler.handle(handler.eventClass().cast(event));
    }

    Set<ManagerEvent.Type> handledTypes() {
        return handlers.keySet();
    }

}
