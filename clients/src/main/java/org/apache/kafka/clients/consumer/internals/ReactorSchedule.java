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

/** Immutable schedule produced by the reactor from all manager reconciliation results. */
final class ReactorSchedule {
    private final NextReconcile nextReconcile;
    private final long decidedAtMs;
    private final RequestManager source;
    private final boolean deadlineNotificationDelivered;

    private ReactorSchedule(final NextReconcile nextReconcile,
                            final long decidedAtMs,
                            final RequestManager source,
                            final boolean deadlineNotificationDelivered) {
        this.nextReconcile = Objects.requireNonNull(nextReconcile, "Next reconcile must be non-null");
        this.decidedAtMs = decidedAtMs;
        this.source = source;
        this.deadlineNotificationDelivered = deadlineNotificationDelivered;
    }

    static ReactorSchedule initial(final long timeoutMs,
                                   final long currentTimeMs) {
        return new ReactorSchedule(
            NextReconcile.atDeadlineAfter(currentTimeMs, timeoutMs),
            currentTimeMs,
            null,
            false
        );
    }

    /** Pure aggregation: the reactor remains the caller and sole owner of the published schedule. */
    static ReactorSchedule merge(final Collection<ManagerReconcileResult> results,
                                 final long currentTimeMs) {
        Objects.requireNonNull(results, "Manager reconcile results must be non-null");

        NextReconcile limiting = NextReconcile.onEvent();
        RequestManager source = null;
        for (ManagerReconcileResult result : results) {
            for (NextReconcile candidate : result.nextReconciles()) {
                if (candidate.deadlineAtMs() < limiting.deadlineAtMs()) {
                    limiting = candidate;
                    source = result.manager();
                }
            }
        }
        return new ReactorSchedule(limiting, currentTimeMs, source, false);
    }

    long timeoutMs() {
        return remainingMs(decidedAtMs);
    }

    long remainingMs(final long currentTimeMs) {
        return nextReconcile.remainingMs(currentTimeMs);
    }

    long remainingMsForApplication(final long currentTimeMs) {
        return deadlineNotificationDelivered ? Long.MAX_VALUE : remainingMs(currentTimeMs);
    }

    boolean deadlineNotificationDelivered() {
        return deadlineNotificationDelivered;
    }

    NextReconcile.Type nextReconcileType() {
        return nextReconcile.type();
    }

    long deadlineAtMs() {
        return nextReconcile.deadlineAtMs();
    }

    boolean shortens(final ReactorSchedule previous) {
        return deadlineAtMs() < previous.deadlineAtMs();
    }

    boolean sameSchedule(final ReactorSchedule other) {
        return nextReconcileType() == other.nextReconcileType()
            && deadlineAtMs() == other.deadlineAtMs()
            && compatibilityDeadline() == other.compatibilityDeadline()
            && semanticGeneration() == other.semanticGeneration()
            && Objects.equals(source, other.source);
    }

    boolean compatibilityDeadline() {
        return nextReconcile.compatibilityDeadline();
    }

    long semanticGeneration() {
        return nextReconcile.semanticGeneration();
    }

    long decidedAtMs() {
        return decidedAtMs;
    }

    Optional<String> source() {
        if (source == null)
            return Optional.empty();
        String simpleName = source.getClass().getSimpleName();
        return Optional.of(simpleName.isEmpty() ? source.getClass().getName() : simpleName);
    }

    Optional<RequestManager> sourceManager() {
        return Optional.ofNullable(source);
    }

    ReactorSchedule withDeadlineNotificationDelivered() {
        if (nextReconcileType() != NextReconcile.Type.AT_DEADLINE)
            throw new IllegalStateException("Only deadline waits can deliver a deadline notification");
        return new ReactorSchedule(nextReconcile, decidedAtMs, source, true);
    }
}
