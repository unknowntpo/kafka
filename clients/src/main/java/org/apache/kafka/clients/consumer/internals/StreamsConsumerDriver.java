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

import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplyAssignmentEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksAssignedCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksRevokedCallbackCompletedEvent;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

/** Streams group-protocol behavior selected by the regular consumer implementation. */
public final class StreamsConsumerDriver implements ConsumerDriver {

    private final Logger log;
    private final RequestManagers requestManagers;

    StreamsConsumerDriver(final LogContext logContext, final RequestManagers requestManagers) {
        this.log = logContext.logger(getClass());
        this.requestManagers = requestManagers;
    }

    @Override
    public boolean process(final ApplicationEvent event) {
        switch (event.type()) {
            case STREAMS_ON_TASKS_REVOKED_CALLBACK_COMPLETED:
                process((StreamsOnTasksRevokedCallbackCompletedEvent) event);
                return true;
            case STREAMS_ON_TASKS_ASSIGNED_CALLBACK_COMPLETED:
                process((StreamsOnTasksAssignedCallbackCompletedEvent) event);
                return true;
            case STREAMS_ON_ALL_TASKS_LOST_CALLBACK_COMPLETED:
                process((StreamsOnAllTasksLostCallbackCompletedEvent) event);
                return true;
            case APPLY_ASSIGNMENT:
                process((ApplyAssignmentEvent) event);
                return true;
            default:
                return false;
        }
    }

    private void process(final StreamsOnTasksRevokedCallbackCompletedEvent event) {
        if (requestManagers.streamsMembershipManager.isEmpty()) {
            log.warn("An internal error occurred; the Streams membership manager was not present, so the " +
                "notification of the onTasksRevoked callback execution could not be sent");
            return;
        }
        requestManagers.streamsMembershipManager.get().onTasksRevokedCallbackCompleted(event);
    }

    private void process(final StreamsOnTasksAssignedCallbackCompletedEvent event) {
        if (requestManagers.streamsMembershipManager.isEmpty()) {
            log.warn("An internal error occurred; the Streams membership manager was not present, so the " +
                "notification of the onTasksAssigned callback execution could not be sent");
            return;
        }
        requestManagers.streamsMembershipManager.get().onTasksAssignedCallbackCompleted(event);
    }

    private void process(final StreamsOnAllTasksLostCallbackCompletedEvent event) {
        if (requestManagers.streamsMembershipManager.isEmpty()) {
            log.warn("An internal error occurred; the Streams membership manager was not present, so the " +
                "notification of the onAllTasksLost callback execution could not be sent");
            return;
        }
        requestManagers.streamsMembershipManager.get().onAllTasksLostCallbackCompleted(event);
    }

    private void process(final ApplyAssignmentEvent event) {
        try {
            if (requestManagers.streamsMembershipManager.isEmpty()) {
                event.future().completeExceptionally(
                    new IllegalStateException("No Streams membership manager is available"));
                return;
            }
            requestManagers.streamsMembershipManager.get().applyAssignment(
                event.assignedPartitions(), event.addedPartitions());
            event.future().complete(null);
        } catch (Exception e) {
            event.future().completeExceptionally(e);
        }
    }
}
