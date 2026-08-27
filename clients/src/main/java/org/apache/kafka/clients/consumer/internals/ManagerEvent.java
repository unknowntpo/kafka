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

/**
 * A typed fact produced by one request manager for deterministic routing to another state owner.
 * Manager events are returned in {@link NetworkClientDelegate.PollResult}; they are not placed on
 * a shared queue and request managers do not consume them directly.
 */
interface ManagerEvent {

    enum Type {
        COORDINATOR_INVALIDATION
    }

    Type type();

    String source();

    /** Reports that a response or disconnect made the currently known coordinator invalid. */
    final class CoordinatorInvalidation implements ManagerEvent {
        private final String source;
        private final String cause;
        private final long observedAtMs;

        CoordinatorInvalidation(final String source, final String cause, final long observedAtMs) {
            this.source = Objects.requireNonNull(source, "Source manager must be non-null");
            this.cause = Objects.toString(cause, "unknown");
            this.observedAtMs = observedAtMs;
        }

        @Override
        public Type type() {
            return Type.COORDINATOR_INVALIDATION;
        }

        @Override
        public String source() {
            return source;
        }

        String cause() {
            return cause;
        }

        long observedAtMs() {
            return observedAtMs;
        }

        @Override
        public String toString() {
            return "CoordinatorInvalidation(source=" + source
                + ", cause=" + cause
                + ", observedAtMs=" + observedAtMs + ")";
        }
    }
}
