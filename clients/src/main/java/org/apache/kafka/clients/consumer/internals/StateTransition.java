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

/** Manager-owned state transitions consumed by the reactor for ordering or application-visible actions. */
enum StateTransition {
    COORDINATOR_DISCOVERED(false),
    FETCH_BUFFER_HAS_DATA(true),
    FETCH_PREPARATION_FAILED(true),
    FETCH_REQUEST_TERMINATED(true),
    FETCH_POSITIONS_UPDATE_FAILED(true);

    private final boolean requiresApplicationWakeup;

    StateTransition(final boolean requiresApplicationWakeup) {
        this.requiresApplicationWakeup = requiresApplicationWakeup;
    }

    boolean requiresApplicationWakeup() {
        return requiresApplicationWakeup;
    }
}
