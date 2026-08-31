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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.common.utils.internals.LogContext;

/**
 * Owns application-event behavior that differs between regular, Streams, and share consumers. The shared reactor
 * invokes one selected driver and remains independent of consumer type.
 */
public interface ConsumerDriver {

    enum Type {
        REGULAR,
        STREAMS,
        SHARE
    }

    /**
     * Processes an event owned by this driver.
     *
     * @return {@code true} when the event belongs to this driver; {@code false} for shared event processing
     */
    boolean process(ApplicationEvent event);

    static ConsumerDriver select(final LogContext logContext,
                                 final RequestManagers requestManagers,
                                 final Metadata metadata,
                                 final SubscriptionState subscriptions) {
        switch (requestManagers.consumerDriverType()) {
            case SHARE:
                return new ShareConsumerDriver(requestManagers, metadata, subscriptions);
            case STREAMS:
                return new StreamsConsumerDriver(logContext, requestManagers);
            case REGULAR:
                return new RegularConsumerDriver(logContext, requestManagers);
            default:
                throw new IllegalStateException("Unsupported consumer driver type " +
                    requestManagers.consumerDriverType());
        }
    }
}
