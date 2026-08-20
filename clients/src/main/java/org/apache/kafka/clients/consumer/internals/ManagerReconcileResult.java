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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Immutable output of one request-manager reconciliation. */
final class ManagerReconcileResult {
    private final RequestManager manager;
    private final PollResult pollResult;
    private final List<NextReconcile> nextReconciles;
    private final Set<StateTransition> stateTransitions;

    private ManagerReconcileResult(final RequestManager manager,
                                   final PollResult pollResult,
                                   final List<NextReconcile> nextReconciles,
                                   final Set<StateTransition> stateTransitions) {
        this.manager = Objects.requireNonNull(manager, "Request manager must be non-null");
        this.pollResult = Objects.requireNonNull(pollResult, "Poll result must be non-null");
        this.nextReconciles = List.copyOf(nextReconciles);
        this.stateTransitions = Set.copyOf(stateTransitions);
    }

    static ManagerReconcileResult of(final RequestManager manager,
                                     final PollResult pollResult,
                                     final NextReconcile... nextReconciles) {
        Objects.requireNonNull(manager, "Request manager must be non-null");
        return new ManagerReconcileResult(
            manager,
            pollResult,
            Arrays.asList(nextReconciles),
            Set.of()
        );
    }

    static ManagerReconcileResult of(final RequestManager manager,
                                     final PollResult pollResult,
                                     final Set<StateTransition> stateTransitions,
                                     final NextReconcile... nextReconciles) {
        Objects.requireNonNull(manager, "Request manager must be non-null");
        return new ManagerReconcileResult(
            manager,
            pollResult,
            Arrays.asList(nextReconciles),
            stateTransitions
        );
    }

    static ManagerReconcileResult scheduleOnly(final RequestManager manager,
                                               final NextReconcile... nextReconciles) {
        return of(manager, PollResult.EMPTY, nextReconciles);
    }

    RequestManager manager() {
        return manager;
    }

    PollResult pollResult() {
        return pollResult;
    }

    List<NextReconcile> nextReconciles() {
        return nextReconciles;
    }

    Set<StateTransition> stateTransitions() {
        return stateTransitions;
    }
}
