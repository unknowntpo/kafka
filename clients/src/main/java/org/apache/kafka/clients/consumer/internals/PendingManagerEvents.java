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

import java.util.EnumMap;

/**
 * Producer-local buffer for facts observed by one request manager but not yet published. It retains at most one
 * event of each type until the manager publishes its next poll snapshot, coalescing repeated facts while retaining
 * the latest diagnostic detail.
 */
final class PendingManagerEvents {
    private final EnumMap<ManagerEvent.Type, ManagerEvent> events =
        new EnumMap<>(ManagerEvent.Type.class);

    boolean hasPendingEvents() {
        return !events.isEmpty();
    }

    /**
     * Retains the greatest observed coordinator version for events of the same type, so a delayed response cannot
     * replace a newer pending observation. This only prevents this producer-local buffer from moving backwards;
     * {@link CoordinatorRequestManager} remains the final authority and compares the observation with its current
     * {@link CoordinatorSnapshot} when the event is routed.
     */
    void add(final ManagerEvent event) {
        ManagerEvent previous = events.get(event.type());
        if (previous instanceof ManagerEvent.CoordinatorUnavailableObserved
                && event instanceof ManagerEvent.CoordinatorUnavailableObserved
                && ((ManagerEvent.CoordinatorUnavailableObserved) event).observedCoordinatorVersion()
                    < ((ManagerEvent.CoordinatorUnavailableObserved) previous).observedCoordinatorVersion()) {
            return;
        }
        events.put(event.type(), event);
    }

    /** Attaches the pending facts to the published poll result and clears this producer-local buffer. */
    NetworkClientDelegate.PollResult publishWith(final NetworkClientDelegate.PollResult pollResult) {
        if (events.isEmpty())
            return pollResult;

        NetworkClientDelegate.PollResult result = pollResult.withManagerEvents(events.values());
        events.clear();
        return result;
    }
}
