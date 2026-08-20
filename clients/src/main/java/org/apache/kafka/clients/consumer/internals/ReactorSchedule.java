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
 * Immutable schedule produced by the reactor from all manager reconciliation results. It owns both projections of
 * one decision: the earliest deadline at which the reactor must run again, and the earliest deadline which must also
 * release an application waiter. Reactor-only work therefore cannot synthesize an application wakeup.
 */
final class ReactorSchedule {
    private final NextReconcile nextReconcile;
    private final NextReconcile applicationNextReconcile;
    private final long decidedAtMs;
    private final RequestManager reactorSource;
    private final RequestManager applicationSource;
    private final boolean deadlineNotificationDelivered;

    private ReactorSchedule(final NextReconcile nextReconcile,
                            final NextReconcile applicationNextReconcile,
                            final long decidedAtMs,
                            final RequestManager reactorSource,
                            final RequestManager applicationSource,
                            final boolean deadlineNotificationDelivered) {
        this.nextReconcile = Objects.requireNonNull(nextReconcile, "Next reconcile must be non-null");
        this.applicationNextReconcile = Objects.requireNonNull(
            applicationNextReconcile,
            "Application next reconcile must be non-null"
        );
        this.decidedAtMs = decidedAtMs;
        this.reactorSource = reactorSource;
        this.applicationSource = applicationSource;
        this.deadlineNotificationDelivered = deadlineNotificationDelivered;
    }

    static ReactorSchedule initial(final long timeoutMs,
                                   final long currentTimeMs) {
        return new ReactorSchedule(
            NextReconcile.atDeadlineAfter(currentTimeMs, timeoutMs),
            NextReconcile.atDeadlineAfter(currentTimeMs, timeoutMs),
            currentTimeMs,
            null,
            null,
            false
        );
    }

    /** Pure aggregation: the reactor remains the caller and sole owner of the published schedule. */
    static ReactorSchedule merge(final Collection<ManagerReconcileResult> results,
                                 final long currentTimeMs) {
        Objects.requireNonNull(results, "Manager reconcile results must be non-null");

        NextReconcile limiting = NextReconcile.onEvent();
        NextReconcile applicationLimiting = NextReconcile.onEvent();
        RequestManager reactorSource = null;
        RequestManager applicationSource = null;
        for (ManagerReconcileResult result : results) {
            for (NextReconcile candidate : result.nextReconciles()) {
                if (candidate.deadlineAtMs() < limiting.deadlineAtMs()) {
                    limiting = candidate;
                    reactorSource = result.manager();
                }
                if (candidate.applicationVisible()
                    && candidate.deadlineAtMs() < applicationLimiting.deadlineAtMs()) {
                    applicationLimiting = candidate;
                    applicationSource = result.manager();
                }
            }
        }
        return new ReactorSchedule(
            limiting,
            applicationLimiting,
            currentTimeMs,
            reactorSource,
            applicationSource,
            false
        );
    }

    long timeoutMs() {
        return remainingMs(decidedAtMs);
    }

    long remainingMs(final long currentTimeMs) {
        return nextReconcile.remainingMs(currentTimeMs);
    }

    long remainingMsForApplication(final long currentTimeMs) {
        return deadlineNotificationDelivered
            ? Long.MAX_VALUE
            : applicationNextReconcile.remainingMs(currentTimeMs);
    }

    long applicationRemainingMs(final long currentTimeMs) {
        return applicationNextReconcile.remainingMs(currentTimeMs);
    }

    boolean deadlineNotificationDelivered() {
        return deadlineNotificationDelivered;
    }

    NextReconcile.Type nextReconcileType() {
        return nextReconcile.type();
    }

    NextReconcile.Type applicationNextReconcileType() {
        return applicationNextReconcile.type();
    }

    long deadlineAtMs() {
        return applicationNextReconcile.deadlineAtMs();
    }

    long reactorDeadlineAtMs() {
        return nextReconcile.deadlineAtMs();
    }

    boolean shortens(final ReactorSchedule previous) {
        return applicationNextReconcile.deadlineAtMs()
            < previous.applicationNextReconcile.deadlineAtMs();
    }

    boolean sameSchedule(final ReactorSchedule other) {
        return applicationNextReconcileType() == other.applicationNextReconcileType()
            && deadlineAtMs() == other.deadlineAtMs()
            && compatibilityDeadline() == other.compatibilityDeadline()
            && semanticGeneration() == other.semanticGeneration()
            && Objects.equals(applicationSource, other.applicationSource);
    }

    boolean compatibilityDeadline() {
        return applicationNextReconcile.compatibilityDeadline();
    }

    long semanticGeneration() {
        return applicationNextReconcile.semanticGeneration();
    }

    long decidedAtMs() {
        return decidedAtMs;
    }

    Optional<String> source() {
        if (applicationSource == null)
            return Optional.empty();
        String simpleName = applicationSource.getClass().getSimpleName();
        return Optional.of(
            simpleName.isEmpty() ? applicationSource.getClass().getName() : simpleName
        );
    }

    Optional<RequestManager> sourceManager() {
        return Optional.ofNullable(applicationSource);
    }

    ReactorSchedule withDeadlineNotificationDelivered() {
        if (applicationNextReconcileType() != NextReconcile.Type.AT_DEADLINE)
            throw new IllegalStateException("Only deadline waits can deliver a deadline notification");
        return new ReactorSchedule(
            nextReconcile,
            applicationNextReconcile,
            decidedAtMs,
            reactorSource,
            applicationSource,
            true
        );
    }
}
