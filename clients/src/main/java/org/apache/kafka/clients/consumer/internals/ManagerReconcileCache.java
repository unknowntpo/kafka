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
 * manager's deadline. Compatibility deadlines are preserved as absolute values across relative-time recomputation.
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
        entry.update(result.nextReconciles(), currentTimeMs);
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
        private boolean compatibilityDeadlineDelivered;

        private Entry(final RequestManager manager) {
            this.manager = manager;
        }

        private void update(final List<NextReconcile> candidates, final long currentTimeMs) {
            NextReconcile candidateCompatibilityDeadline = earliestCompatibilityDeadline(candidates);
            NextReconcile preservedCompatibilityDeadline = candidateCompatibilityDeadline;
            boolean preserveDelivered = false;

            if (compatibilityDeadline != null && candidateCompatibilityDeadline != null) {
                if (compatibilityDeadlineDelivered
                    && candidateCompatibilityDeadline.remainingMs(currentTimeMs) == 0L) {
                    preservedCompatibilityDeadline = compatibilityDeadline;
                    preserveDelivered = true;
                } else if (!compatibilityDeadlineDelivered
                    && compatibilityDeadline.deadlineAtMs() <= candidateCompatibilityDeadline.deadlineAtMs()) {
                    preservedCompatibilityDeadline = compatibilityDeadline;
                }
            }

            List<NextReconcile> updated = new ArrayList<>(candidates.size());
            for (NextReconcile candidate : candidates) {
                updated.add(candidate == candidateCompatibilityDeadline ? preservedCompatibilityDeadline : candidate);
            }
            nextReconciles = List.copyOf(updated);
            compatibilityDeadline = preservedCompatibilityDeadline;
            compatibilityDeadlineDelivered = preserveDelivered;
        }

        private NextReconcile[] activeNextReconciles() {
            if (!compatibilityDeadlineDelivered)
                return nextReconciles.toArray(new NextReconcile[0]);

            return nextReconciles.stream()
                .filter(next -> next != compatibilityDeadline)
                .toArray(NextReconcile[]::new);
        }

        private void markDeadlineDelivered(final ReactorSchedule schedule) {
            if (compatibilityDeadline != null
                && schedule.compatibilityDeadline()
                && compatibilityDeadline.deadlineAtMs() == schedule.deadlineAtMs()) {
                compatibilityDeadlineDelivered = true;
            }
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
