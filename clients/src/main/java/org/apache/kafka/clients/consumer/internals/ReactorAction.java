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

import org.apache.kafka.clients.consumer.internals.events.AsyncPollEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.MetadataErrorNotifiableEvent;
import org.apache.kafka.common.KafkaException;

import java.util.Objects;

/**
 * An application-visible effect selected by the reactor and executed only after the corresponding
 * {@link ReactorSchedule} has been published.
 */
public abstract class ReactorAction {

    public enum Type {
        WAKE_APPLICATION,
        PUBLISH_BACKGROUND_ERROR,
        NOTIFY_METADATA_ERROR,
        MARK_ASYNC_POLL_RECONCILIATION_COMPLETE,
        MARK_ASYNC_POLL_VALIDATE_POSITIONS_COMPLETE,
        COMPLETE_ASYNC_POLL
    }

    private static final ReactorAction WAKE_APPLICATION = new ReactorAction(Type.WAKE_APPLICATION) {
        @Override
        public void execute(final RequestManagers requestManagers) {
            requestManagers.wakeupApplicationThread();
        }
    };

    private final Type type;

    private ReactorAction(final Type type) {
        this.type = Objects.requireNonNull(type);
    }

    public Type type() {
        return type;
    }

    public abstract void execute(RequestManagers requestManagers);

    public static ReactorAction wakeApplication() {
        return WAKE_APPLICATION;
    }

    public static ReactorAction publishBackgroundError(final Throwable error) {
        Objects.requireNonNull(error);
        return new ReactorAction(Type.PUBLISH_BACKGROUND_ERROR) {
            @Override
            public void execute(final RequestManagers requestManagers) {
                requestManagers.publishBackgroundEvent(new ErrorEvent(error));
            }

            @Override
            public String toString() {
                return type() + "(" + error + ")";
            }
        };
    }

    public static ReactorAction markAsyncPollReconciliationComplete(final AsyncPollEvent event) {
        Objects.requireNonNull(event);
        return new ReactorAction(Type.MARK_ASYNC_POLL_RECONCILIATION_COMPLETE) {
            @Override
            public void execute(final RequestManagers requestManagers) {
                event.markReconciliationCheckComplete();
            }

            @Override
            public String toString() {
                return type() + "(" + event.type() + ")";
            }
        };
    }

    public static ReactorAction notifyMetadataError(final MetadataErrorNotifiableEvent event,
                                                    final Exception error) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(error);
        return new ReactorAction(Type.NOTIFY_METADATA_ERROR) {
            @Override
            public void execute(final RequestManagers requestManagers) {
                event.onMetadataError(error);
            }

            @Override
            public String toString() {
                return type() + "(" + event + ")";
            }
        };
    }

    public static ReactorAction markAsyncPollValidatePositionsComplete(final AsyncPollEvent event) {
        Objects.requireNonNull(event);
        return new ReactorAction(Type.MARK_ASYNC_POLL_VALIDATE_POSITIONS_COMPLETE) {
            @Override
            public void execute(final RequestManagers requestManagers) {
                event.markValidatePositionsComplete();
            }

            @Override
            public String toString() {
                return type() + "(" + event.type() + ")";
            }
        };
    }

    public static ReactorAction completeAsyncPoll(final AsyncPollEvent event,
                                                  final KafkaException error) {
        Objects.requireNonNull(event);
        return new ReactorAction(Type.COMPLETE_ASYNC_POLL) {
            @Override
            public void execute(final RequestManagers requestManagers) {
                if (error == null)
                    event.completeSuccessfully();
                else
                    event.completeExceptionally(error);
            }

            @Override
            public String toString() {
                return type() + "(" + event.type() + ", success=" + (error == null) + ")";
            }
        };
    }

    @Override
    public String toString() {
        return type.toString();
    }
}
