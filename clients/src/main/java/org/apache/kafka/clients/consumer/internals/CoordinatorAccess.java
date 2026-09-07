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

import org.apache.kafka.common.Node;

import java.util.Optional;

/**
 * Coordinator capabilities for a dependent request manager. All access is confined to the
 * consumer network thread; separate node/version reads are not a cross-thread snapshot API.
 * The coordinator owner validates observations and retains control of discovery and lifecycle.
 * This interface deliberately grants no fatal-error consumption or unconditional invalidation.
 */
public interface CoordinatorAccess {
    Optional<Node> coordinator();

    long coordinatorVersion();

    /**
     * Non-consuming view of the current discovery error. This is a legacy delivery adapter,
     * not a permanent terminal state: the configured delivery path may subsequently clear it.
     */
    Optional<Throwable> fatalError();

    boolean markCoordinatorUnknownIfCurrent(String cause, long currentTimeMs, long observedVersion);

    void handleCoordinatorDisconnect(Throwable exception, long currentTimeMs, long observedVersion);
}
