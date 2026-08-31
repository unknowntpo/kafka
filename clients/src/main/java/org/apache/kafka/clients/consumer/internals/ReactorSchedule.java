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
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable scheduling decision published by the reactor. The reactor deadline controls when managers are polled
 * again. The application deadline is a migration bridge for managers which still expose
 * {@link RequestManager#maximumTimeToWait(long)}; reactor-only deadlines never wake the application thread.
 */
final class ReactorSchedule {
    /** Earliest retained deadline at which the reactor must poll a manager again. */
    private final long reactorDeadlineMs;

    /** Compatibility deadline which may release the application while managers still expose legacy wait hints. */
    private final long applicationDeadlineMs;

    /** Clock value at which this immutable snapshot was formed, used to project absolute deadlines to delays. */
    private final long decidedAtMs;

    /** Manager contributing {@link #reactorDeadlineMs}; retained for diagnostics rather than policy dispatch. */
    private final RequestManager deadlineSource;

    /** Manager contributing {@link #applicationDeadlineMs}; retained for diagnostics and delivery tracking. */
    private final RequestManager applicationSource;

    /** Prevents one expired compatibility deadline from repeatedly waking the application. */
    private final boolean applicationDeadlineDelivered;

    /** Monotonic publication sequence used to verify that effects observe their phase's published schedule. */
    private final long generation;

    private ReactorSchedule(final long reactorDeadlineMs,
                            final long applicationDeadlineMs,
                            final long decidedAtMs,
                            final RequestManager deadlineSource,
                            final RequestManager applicationSource,
                            final boolean applicationDeadlineDelivered,
                            final long generation) {
        this.reactorDeadlineMs = reactorDeadlineMs;
        this.applicationDeadlineMs = applicationDeadlineMs;
        this.decidedAtMs = decidedAtMs;
        this.deadlineSource = deadlineSource;
        this.applicationSource = applicationSource;
        this.applicationDeadlineDelivered = applicationDeadlineDelivered;
        this.generation = generation;
    }

    static ReactorSchedule initial(final long timeoutMs,
                                   final long currentTimeMs) {
        long deadlineMs = deadlineAfter(currentTimeMs, timeoutMs);
        return new ReactorSchedule(deadlineMs, deadlineMs, currentTimeMs, null, null, false, 0L);
    }

    /** Pure aggregation: the reactor remains the caller and sole owner of the published schedule. */
    static ReactorSchedule from(final Collection<ManagerPollCache.PollState> states,
                                final long currentTimeMs) {
        Objects.requireNonNull(states, "Manager poll states must be non-null");

        long reactorDeadlineMs = Long.MAX_VALUE;
        long applicationDeadlineMs = Long.MAX_VALUE;
        RequestManager deadlineSource = null;
        RequestManager applicationSource = null;
        for (ManagerPollCache.PollState state : states) {
            if (state.reactorDeadlineMs() < reactorDeadlineMs) {
                reactorDeadlineMs = state.reactorDeadlineMs();
                deadlineSource = state.manager();
            }
            if (state.activeApplicationDeadlineMs() < applicationDeadlineMs) {
                applicationDeadlineMs = state.activeApplicationDeadlineMs();
                applicationSource = state.manager();
            }
        }
        return new ReactorSchedule(
            reactorDeadlineMs,
            applicationDeadlineMs,
            currentTimeMs,
            deadlineSource,
            applicationSource,
            false,
            0L
        );
    }

    long timeoutMs() {
        return networkPollTimeoutMs(decidedAtMs);
    }

    long networkPollTimeoutMs(final long currentTimeMs) {
        return remainingMs(pollDeadlineMs(), currentTimeMs);
    }

    long remainingMsForApplication(final long currentTimeMs) {
        return applicationDeadlineDelivered
            ? Long.MAX_VALUE
            : remainingMs(applicationDeadlineMs, currentTimeMs);
    }

    long applicationRemainingMs(final long currentTimeMs) {
        return remainingMs(applicationDeadlineMs, currentTimeMs);
    }

    boolean applicationDeadlineDelivered() {
        return applicationDeadlineDelivered;
    }

    long applicationDeadlineMs() {
        return applicationDeadlineMs;
    }

    long reactorDeadlineMs() {
        return reactorDeadlineMs;
    }

    long pollDeadlineMs() {
        if (!applicationDeadlineDelivered && applicationDeadlineMs < reactorDeadlineMs)
            return applicationDeadlineMs;
        return reactorDeadlineMs;
    }

    long decidedAtMs() {
        return decidedAtMs;
    }

    long generation() {
        return generation;
    }

    boolean shortensApplicationWait(final ReactorSchedule previous) {
        return applicationDeadlineMs < previous.applicationDeadlineMs;
    }

    boolean sameSchedule(final ReactorSchedule other) {
        return reactorDeadlineMs == other.reactorDeadlineMs
            && applicationDeadlineMs == other.applicationDeadlineMs
            && Objects.equals(deadlineSource, other.deadlineSource)
            && Objects.equals(applicationSource, other.applicationSource);
    }

    Optional<String> deadlineSource() {
        if (!applicationDeadlineDelivered && applicationDeadlineMs < reactorDeadlineMs)
            return sourceName(applicationSource);
        return sourceName(deadlineSource);
    }

    Optional<String> applicationSource() {
        return sourceName(applicationSource);
    }

    ReactorSchedule withApplicationDeadlineDelivered() {
        if (applicationDeadlineMs == Long.MAX_VALUE)
            throw new IllegalStateException("An unbounded application wait cannot expire");
        return new ReactorSchedule(
            reactorDeadlineMs,
            applicationDeadlineMs,
            decidedAtMs,
            deadlineSource,
            applicationSource,
            true,
            generation + 1L
        );
    }

    ReactorSchedule withGeneration(final long nextGeneration) {
        return new ReactorSchedule(
            reactorDeadlineMs,
            applicationDeadlineMs,
            decidedAtMs,
            deadlineSource,
            applicationSource,
            applicationDeadlineDelivered,
            nextGeneration
        );
    }

    private static Optional<String> sourceName(final RequestManager manager) {
        if (manager == null)
            return Optional.empty();
        String simpleName = manager.getClass().getSimpleName();
        return Optional.of(simpleName.isEmpty() ? manager.getClass().getName() : simpleName);
    }

    private static long deadlineAfter(final long currentTimeMs,
                                      final long delayMs) {
        long boundedDelayMs = Math.max(0L, delayMs);
        if (boundedDelayMs == Long.MAX_VALUE || currentTimeMs > Long.MAX_VALUE - boundedDelayMs)
            return Long.MAX_VALUE;
        return currentTimeMs + boundedDelayMs;
    }

    private static long remainingMs(final long deadlineMs,
                                    final long currentTimeMs) {
        return deadlineMs == Long.MAX_VALUE
            ? Long.MAX_VALUE
            : Math.max(0L, deadlineMs - currentTimeMs);
    }
}
