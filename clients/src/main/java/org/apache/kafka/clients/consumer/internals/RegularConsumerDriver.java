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
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackCompletedEvent;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

/** Consumer-protocol behavior for {@link AsyncKafkaConsumer}. */
public final class RegularConsumerDriver implements ConsumerDriver {

    private final Logger log;
    private final RequestManagers requestManagers;

    RegularConsumerDriver(final LogContext logContext, final RequestManagers requestManagers) {
        this.log = logContext.logger(getClass());
        this.requestManagers = requestManagers;
    }

    @Override
    public boolean process(final ApplicationEvent event) {
        switch (event.type()) {
            case CONSUMER_REBALANCE_LISTENER_CALLBACK_COMPLETED:
                process((ConsumerRebalanceListenerCallbackCompletedEvent) event);
                return true;
            case APPLY_ASSIGNMENT:
                process((ApplyAssignmentEvent) event);
                return true;
            default:
                return false;
        }
    }

    private void process(final ConsumerRebalanceListenerCallbackCompletedEvent event) {
        if (requestManagers.consumerHeartbeatRequestManager.isEmpty()) {
            log.warn("An internal error occurred; the group membership manager was not present, so the " +
                "notification of the {} callback execution could not be sent", event.methodName());
            return;
        }
        requestManagers.consumerHeartbeatRequestManager.get().membershipManager()
            .consumerRebalanceListenerCallbackCompleted(event);
    }

    private void process(final ApplyAssignmentEvent event) {
        try {
            if (requestManagers.consumerMembershipManager.isEmpty()) {
                event.future().completeExceptionally(
                    new IllegalStateException("No regular consumer membership manager is available"));
                return;
            }
            requestManagers.consumerMembershipManager.get().applyAssignment(
                event.assignedPartitions(), event.addedPartitions());
            event.future().complete(null);
        } catch (Exception e) {
            event.future().completeExceptionally(e);
        }
    }
}
