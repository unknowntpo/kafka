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

import java.util.ArrayList;
import java.util.List;

/** Immutable commands and application-visible actions derived from one ordered manager-event batch. */
final class CoordinationPlan {
    private final List<ManagerCommand> managerCommands;
    private final List<ReactorAction> reactorActions;

    CoordinationPlan(final List<ManagerCommand> managerCommands,
                     final List<ReactorAction> reactorActions) {
        this.managerCommands = List.copyOf(managerCommands);
        this.reactorActions = List.copyOf(reactorActions);
    }

    static CoordinationPlan command(final ManagerCommand command) {
        return new CoordinationPlan(List.of(command), List.of());
    }

    static CoordinationPlan action(final ReactorAction action) {
        return new CoordinationPlan(List.of(), List.of(action));
    }

    List<ManagerCommand> managerCommands() {
        return managerCommands;
    }

    List<ReactorAction> reactorActions() {
        return reactorActions;
    }

    static final class Builder {
        private final List<ManagerCommand> managerCommands = new ArrayList<>();
        private final List<ReactorAction> reactorActions = new ArrayList<>();

        void add(final CoordinationPlan plan) {
            managerCommands.addAll(plan.managerCommands());
            for (ReactorAction action : plan.reactorActions()) {
                if (action != ReactorAction.wakeApplication() || !reactorActions.contains(action))
                    reactorActions.add(action);
            }
        }

        CoordinationPlan build() {
            return new CoordinationPlan(managerCommands, reactorActions);
        }
    }
}
