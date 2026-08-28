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

import java.util.Objects;

/** A typed intent addressed to exactly one request-manager state owner. */
interface ManagerCommand {

    enum Type {
        INVALIDATE_COORDINATOR_IF_CURRENT
    }

    Type type();

    /** Asks the coordinator owner to apply an observation only when its captured version is still current. */
    final class InvalidateCoordinatorIfCurrent implements ManagerCommand {
        private final ManagerEvent.CoordinatorUnavailableObserved observation;

        InvalidateCoordinatorIfCurrent(final ManagerEvent.CoordinatorUnavailableObserved observation) {
            this.observation = Objects.requireNonNull(observation, "Coordinator observation must be non-null");
        }

        @Override
        public Type type() {
            return Type.INVALIDATE_COORDINATOR_IF_CURRENT;
        }

        ManagerEvent.CoordinatorUnavailableObserved observation() {
            return observation;
        }

        @Override
        public String toString() {
            return type() + "(" + observation + ")";
        }
    }
}
