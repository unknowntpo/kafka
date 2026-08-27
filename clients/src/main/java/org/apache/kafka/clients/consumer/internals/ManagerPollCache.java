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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Retains each request manager's latest reactor deadline. Relative poll delays are converted to absolute deadlines
 * before an early reactor iteration can cause an unrelated manager's deadline to move forward.
 */
final class ManagerPollCache {
    /** Manager identity is the cache key; managers are execution components, not value objects. */
    private final Map<RequestManager, PollState> states = new IdentityHashMap<>();

    /** Stable manager order used when exposing states, including deterministic tie-breaking and diagnostics. */
    private final List<RequestManager> managerOrder = new ArrayList<>();

    void update(final RequestManager manager,
                final PollResult result,
                final long applicationWaitMs,
                final long currentTimeMs) {
        PollState state = states.get(manager);
        if (state == null) {
            state = new PollState(manager);
            states.put(manager, state);
            managerOrder.add(manager);
        }
        state.update(result.timeUntilNextPollMs, applicationWaitMs, currentTimeMs);
    }

    Collection<PollState> states() {
        List<PollState> ordered = new ArrayList<>(managerOrder.size());
        for (RequestManager manager : managerOrder)
            ordered.add(states.get(manager));
        return ordered;
    }

    void retainManagers(final Collection<RequestManager> managers) {
        Set<RequestManager> activeManagers = Collections.newSetFromMap(new IdentityHashMap<>());
        activeManagers.addAll(managers);
        states.keySet().removeIf(manager -> !activeManagers.contains(manager));
        managerOrder.clear();
        managerOrder.addAll(managers);
    }

    void markApplicationDeadlineDelivered(final ReactorSchedule schedule) {
        for (PollState state : states.values())
            state.markApplicationDeadlineDelivered(schedule);
    }

    static final class PollState {
        private final RequestManager manager;

        /** Absolute form of the manager's latest retained {@code timeUntilNextPollMs}. */
        private long reactorDeadlineMs = Long.MAX_VALUE;

        /** Absolute compatibility deadline derived from {@link RequestManager#maximumTimeToWait(long)}. */
        private long applicationDeadlineMs = Long.MAX_VALUE;

        /** One-shot delivery marker for an expired compatibility deadline. */
        private boolean applicationDeadlineDelivered;

        private PollState(final RequestManager manager) {
            this.manager = manager;
        }

        private void update(final long timeUntilNextPollMs,
                            final long applicationWaitMs,
                            final long currentTimeMs) {
            long proposedReactorDeadlineMs = deadlineAfter(currentTimeMs, timeUntilNextPollMs);
            // A finite result polled early cannot postpone a previously published deadline. An explicit event wait,
            // however, means the manager's state changed and time alone can no longer make it runnable, so it
            // withdraws its prior deadline.
            reactorDeadlineMs = proposedReactorDeadlineMs == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : preservePendingDeadline(reactorDeadlineMs, proposedReactorDeadlineMs, currentTimeMs);

            long proposedApplicationDeadlineMs = deadlineAfter(currentTimeMs, applicationWaitMs);
            if (applicationDeadlineDelivered
                && proposedApplicationDeadlineMs <= currentTimeMs
                && applicationDeadlineMs <= currentTimeMs) {
                return;
            }
            applicationDeadlineMs = preservePendingDeadline(
                applicationDeadlineMs,
                proposedApplicationDeadlineMs,
                currentTimeMs
            );
            applicationDeadlineDelivered = false;
        }

        private void markApplicationDeadlineDelivered(final ReactorSchedule schedule) {
            if (applicationDeadlineMs == schedule.applicationDeadlineMs())
                applicationDeadlineDelivered = true;
        }

        RequestManager manager() {
            return manager;
        }

        long reactorDeadlineMs() {
            return reactorDeadlineMs;
        }

        long activeApplicationDeadlineMs() {
            return applicationDeadlineDelivered ? Long.MAX_VALUE : applicationDeadlineMs;
        }

        private static long preservePendingDeadline(final long currentDeadlineMs,
                                                    final long proposedDeadlineMs,
                                                    final long currentTimeMs) {
            if (currentDeadlineMs > currentTimeMs && currentDeadlineMs <= proposedDeadlineMs)
                return currentDeadlineMs;
            return proposedDeadlineMs;
        }

        private static long deadlineAfter(final long currentTimeMs,
                                          final long delayMs) {
            long boundedDelayMs = Math.max(0L, delayMs);
            if (boundedDelayMs == Long.MAX_VALUE || currentTimeMs > Long.MAX_VALUE - boundedDelayMs)
                return Long.MAX_VALUE;
            return currentTimeMs + boundedDelayMs;
        }
    }
}
