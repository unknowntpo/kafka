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
        COORDINATOR_UNAVAILABLE_OBSERVED,
        COORDINATOR_FATAL_ERROR,
        FETCH_BUFFER_HAS_DATA,
        FETCH_PREPARATION_FAILED,
        FETCH_REQUEST_TERMINATED,
        FETCH_POSITIONS_UPDATE_FAILED
    }

    /** Reports a fatal coordinator-discovery error for publication to the application thread. */
    final class CoordinatorFatalError implements ManagerEvent {
        private final Throwable error;

        CoordinatorFatalError(final Throwable error) {
            this.error = Objects.requireNonNull(error, "Coordinator fatal error must be non-null");
        }

        @Override
        public Type type() {
            return Type.COORDINATOR_FATAL_ERROR;
        }

        @Override
        public String source() {
            return CoordinatorRequestManager.class.getSimpleName();
        }

        Throwable error() {
            return error;
        }

        @Override
        public String toString() {
            return "CoordinatorFatalError(error=" + error + ")";
        }
    }

    Type type();

    String source();

    /** Reports that a request observed its coordinator unavailable; only the coordinator owner may apply it. */
    final class CoordinatorUnavailableObserved implements ManagerEvent {
        private final String source;
        private final String cause;
        private final long observedAtMs;
        private final long observedCoordinatorVersion;

        CoordinatorUnavailableObserved(final String source,
                                       final String cause,
                                       final long observedAtMs,
                                       final long observedCoordinatorVersion) {
            this.source = Objects.requireNonNull(source, "Source manager must be non-null");
            this.cause = Objects.toString(cause, "unknown");
            this.observedAtMs = observedAtMs;
            this.observedCoordinatorVersion = observedCoordinatorVersion;
        }

        @Override
        public Type type() {
            return Type.COORDINATOR_UNAVAILABLE_OBSERVED;
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

        long observedCoordinatorVersion() {
            return observedCoordinatorVersion;
        }

        @Override
        public String toString() {
            return "CoordinatorUnavailableObserved(source=" + source
                + ", cause=" + cause
                + ", observedAtMs=" + observedAtMs
                + ", observedCoordinatorVersion=" + observedCoordinatorVersion + ")";
        }
    }

    /** Reports that the fetch domain already has records that the application may consume. */
    final class FetchBufferHasData implements ManagerEvent {
        static final FetchBufferHasData INSTANCE = new FetchBufferHasData();

        private FetchBufferHasData() {
        }

        @Override
        public Type type() {
            return Type.FETCH_BUFFER_HAS_DATA;
        }

        @Override
        public String source() {
            return FetchRequestManager.class.getSimpleName();
        }

        @Override
        public String toString() {
            return type().toString();
        }
    }

    /**
     * Reports a manager-local outcome which requires the application-side fetch wait to be re-evaluated.
     * The fact does not prescribe the wake effect; the composition-owned policy derives and coalesces that effect.
     */
    enum LocalProgress implements ManagerEvent {
        FETCH_PREPARATION_FAILED(Type.FETCH_PREPARATION_FAILED, FetchRequestManager.class.getSimpleName()),
        FETCH_REQUEST_TERMINATED(Type.FETCH_REQUEST_TERMINATED, FetchRequestManager.class.getSimpleName()),
        FETCH_POSITIONS_UPDATE_FAILED(Type.FETCH_POSITIONS_UPDATE_FAILED, OffsetsRequestManager.class.getSimpleName());

        private final Type type;
        private final String source;

        LocalProgress(final Type type, final String source) {
            this.type = type;
            this.source = source;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public String source() {
            return source;
        }
    }
}
