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
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Retains each manager's latest scheduling contribution so an incremental reconciliation cannot erase another
 * manager's deadline. Legacy application waits and network poll delays are converted to absolute deadlines here,
 * before the reactor forms its single schedule, so early network returns cannot move them into the future.
 */
final class ManagerReconcileCache {
    private final Map<RequestManager, Entry> entries = new IdentityHashMap<>();
    private final List<RequestManager> managerOrder = new ArrayList<>();

    void update(final ManagerReconcileResult result, final long currentTimeMs) {
        Entry entry = entries.get(result.manager());
        if (entry == null) {
            entry = new Entry(result.manager());
            entries.put(result.manager(), entry);
            managerOrder.add(result.manager());
        }
        entry.update(
            result.nextReconciles(),
            result.pollResult().timeUntilNextPollMs,
            currentTimeMs
        );
    }

    Collection<ManagerReconcileResult> scheduleResults() {
        List<ManagerReconcileResult> results = new ArrayList<>(entries.size());
        for (RequestManager manager : managerOrder) {
            Entry entry = entries.get(manager);
            results.add(ManagerReconcileResult.scheduleOnly(entry.manager, entry.activeNextReconciles()));
        }
        return results;
    }

    void retainManagers(final Collection<ManagerReconcileResult> results) {
        Set<RequestManager> activeManagers = Collections.newSetFromMap(new IdentityHashMap<>());
        for (ManagerReconcileResult result : results)
            activeManagers.add(result.manager());
        entries.keySet().removeIf(manager -> !activeManagers.contains(manager));
        managerOrder.clear();
        for (ManagerReconcileResult result : results)
            managerOrder.add(result.manager());
    }

    void markDeadlineDelivered(final ReactorSchedule schedule) {
        for (Entry entry : entries.values())
            entry.markDeadlineDelivered(schedule);
    }

    private static final class Entry {
        private final RequestManager manager;
        private List<NextReconcile> nextReconciles = List.of();
        private NextReconcile compatibilityDeadline;
        private NextReconcile reactorPollDeadline = NextReconcile.onEvent();
        private List<NextReconcile> deliveredApplicationDeadlines = List.of();
        private boolean compatibilityDeadlineDelivered;

        private Entry(final RequestManager manager) {
            this.manager = manager;
        }

        private void update(final List<NextReconcile> candidates,
                            final long pollDelayMs,
                            final long currentTimeMs) {
            NextReconcile candidateCompatibilityDeadline = earliestCompatibilityDeadline(candidates);
            NextReconcile preservedCompatibilityDeadline = preserveCompatibilityDeadline(
                candidateCompatibilityDeadline,
                currentTimeMs
            );
            boolean preserveDelivered = compatibilityDeadlineDelivered
                && preservedCompatibilityDeadline == compatibilityDeadline;

            List<NextReconcile> updated = replaceCompatibilityDeadline(
                candidates,
                candidateCompatibilityDeadline,
                preservedCompatibilityDeadline
            );
            nextReconciles = List.copyOf(updated);
            compatibilityDeadline = preservedCompatibilityDeadline;
            deliveredApplicationDeadlines = retainDeliveredDeadlines(
                updated,
                preservedCompatibilityDeadline,
                preserveDelivered
            );
            compatibilityDeadlineDelivered =
                compatibilityDeadline != null && deliveredApplicationDeadlines.contains(compatibilityDeadline);
            updateReactorPollDeadline(pollDelayMs, currentTimeMs);
        }

        private void updateReactorPollDeadline(final long pollDelayMs,
                                               final long currentTimeMs) {
            NextReconcile proposedPollDeadline =
                NextReconcile.atReactorDeadlineAfter(currentTimeMs, pollDelayMs);
            if (reactorPollDeadline.remainingMs(currentTimeMs) > 0L
                && reactorPollDeadline.deadlineAtMs() <= proposedPollDeadline.deadlineAtMs()) {
                proposedPollDeadline = reactorPollDeadline;
            }
            reactorPollDeadline = proposedPollDeadline;
        }

        private NextReconcile preserveCompatibilityDeadline(final NextReconcile candidate,
                                                            final long currentTimeMs) {
            if (compatibilityDeadline == null || candidate == null)
                return candidate;
            if (compatibilityDeadlineDelivered && candidate.remainingMs(currentTimeMs) == 0L)
                return compatibilityDeadline;
            if (!compatibilityDeadlineDelivered
                && compatibilityDeadline.deadlineAtMs() <= candidate.deadlineAtMs()) {
                return compatibilityDeadline;
            }
            return candidate;
        }

        private static List<NextReconcile> replaceCompatibilityDeadline(
            final List<NextReconcile> candidates,
            final NextReconcile candidateCompatibilityDeadline,
            final NextReconcile preservedCompatibilityDeadline
        ) {
            List<NextReconcile> updated = new ArrayList<>(candidates.size());
            for (NextReconcile candidate : candidates) {
                updated.add(candidate == candidateCompatibilityDeadline ? preservedCompatibilityDeadline : candidate);
            }
            return updated;
        }

        private List<NextReconcile> retainDeliveredDeadlines(
            final List<NextReconcile> candidates,
            final NextReconcile preservedCompatibilityDeadline,
            final boolean preserveCompatibilityDelivery
        ) {
            List<NextReconcile> retained = new ArrayList<>();
            if (preserveCompatibilityDelivery)
                retained.add(preservedCompatibilityDeadline);
            for (NextReconcile delivered : deliveredApplicationDeadlines) {
                NextReconcile matching = matchingDecision(candidates, delivered);
                if (matching != null && !retained.contains(matching))
                    retained.add(matching);
            }
            return List.copyOf(retained);
        }

        private NextReconcile[] activeNextReconciles() {
            List<NextReconcile> active = new ArrayList<>(nextReconciles.size() + 1);
            for (NextReconcile next : nextReconciles) {
                if (!deliveredApplicationDeadlines.contains(next))
                    active.add(next);
            }
            if (reactorPollDeadline.type() != NextReconcile.Type.ON_EVENT)
                active.add(reactorPollDeadline);
            return active.toArray(new NextReconcile[0]);
        }

        private void markDeadlineDelivered(final ReactorSchedule schedule) {
            List<NextReconcile> delivered = new ArrayList<>(deliveredApplicationDeadlines);
            for (NextReconcile candidate : nextReconciles) {
                if (candidate.applicationVisible()
                    && candidate.deadlineAtMs() == schedule.deadlineAtMs()
                    && !delivered.contains(candidate)) {
                    delivered.add(candidate);
                }
            }
            deliveredApplicationDeadlines = List.copyOf(delivered);
            compatibilityDeadlineDelivered = compatibilityDeadline != null
                && deliveredApplicationDeadlines.contains(compatibilityDeadline);
        }

        private static NextReconcile matchingDecision(final List<NextReconcile> candidates,
                                                      final NextReconcile expected) {
            if (expected == null)
                return null;
            for (NextReconcile candidate : candidates) {
                if (candidate.applicationVisible() == expected.applicationVisible()
                    && candidate.type() == expected.type()
                    && candidate.deadlineAtMs() == expected.deadlineAtMs()
                    && candidate.compatibilityDeadline() == expected.compatibilityDeadline()
                    && candidate.semanticGeneration() == expected.semanticGeneration()) {
                    return candidate;
                }
            }
            return null;
        }

        private static NextReconcile earliestCompatibilityDeadline(final List<NextReconcile> candidates) {
            NextReconcile earliest = null;
            for (NextReconcile candidate : candidates) {
                if (candidate.compatibilityDeadline()
                    && (earliest == null || candidate.deadlineAtMs() < earliest.deadlineAtMs())) {
                    earliest = candidate;
                }
            }
            return earliest;
        }
    }
}
