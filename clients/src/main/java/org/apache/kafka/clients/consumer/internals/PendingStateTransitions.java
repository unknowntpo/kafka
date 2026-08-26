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

import java.util.EnumSet;

/**
 * Accumulates cross-manager state changes produced by request callbacks until the owning
 * request manager can publish them atomically with its next {@link NetworkClientDelegate.PollResult}.
 */
final class PendingStateTransitions {
    private final EnumSet<StateTransition> transitions = EnumSet.noneOf(StateTransition.class);

    void add(final StateTransition transition) {
        transitions.add(transition);
    }

    void addIf(final boolean condition, final StateTransition transition) {
        if (condition)
            add(transition);
    }

    NetworkClientDelegate.PollResult publishWith(final NetworkClientDelegate.PollResult pollResult) {
        if (transitions.isEmpty())
            return pollResult;

        NetworkClientDelegate.PollResult result = pollResult.withStateTransitions(transitions);
        transitions.clear();
        return result;
    }
}
