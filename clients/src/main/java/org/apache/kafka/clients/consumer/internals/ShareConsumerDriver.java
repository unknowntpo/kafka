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
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeAsyncEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeSyncEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementCommitCallbackRegistrationEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareFetchEvent;
import org.apache.kafka.clients.consumer.internals.events.SharePollEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareUnsubscribeEvent;
import org.apache.kafka.common.KafkaException;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/** Share-group behavior for {@link ShareConsumerImpl}. */
public final class ShareConsumerDriver implements ConsumerDriver {

    private final RequestManagers requestManagers;
    private final Metadata metadata;
    private final SubscriptionState subscriptions;

    ShareConsumerDriver(final RequestManagers requestManagers,
                        final Metadata metadata,
                        final SubscriptionState subscriptions) {
        this.requestManagers = requestManagers;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
    }

    @Override
    public boolean process(final ApplicationEvent event) {
        switch (event.type()) {
            case SHARE_POLL:
                process((SharePollEvent) event);
                return true;
            case SHARE_FETCH:
                process((ShareFetchEvent) event);
                return true;
            case SHARE_ACKNOWLEDGE_SYNC:
                process((ShareAcknowledgeSyncEvent) event);
                return true;
            case SHARE_ACKNOWLEDGE_ASYNC:
                process((ShareAcknowledgeAsyncEvent) event);
                return true;
            case SHARE_SUBSCRIPTION_CHANGE:
                process((ShareSubscriptionChangeEvent) event);
                return true;
            case SHARE_UNSUBSCRIBE:
                process((ShareUnsubscribeEvent) event);
                return true;
            case SHARE_ACKNOWLEDGE_ON_CLOSE:
                process((ShareAcknowledgeOnCloseEvent) event);
                return true;
            case SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK_REGISTRATION:
                process((ShareAcknowledgementCommitCallbackRegistrationEvent) event);
                return true;
            default:
                return false;
        }
    }

    private void process(final SharePollEvent event) {
        requestManagers.shareMembershipManager.ifPresent(manager -> manager.maybeReconcile(true));
        requestManagers.shareHeartbeatRequestManager.ifPresent(heartbeat -> {
            heartbeat.membershipManager().onConsumerPoll();
            heartbeat.resetPollTimer(event.pollTimeMs());
        });
        event.completeSuccessfully();
    }

    private void process(final ShareFetchEvent event) {
        requestManagers.shareConsumeRequestManager.ifPresent(manager -> manager.fetch(event.acknowledgementsMap()));
    }

    private void process(final ShareAcknowledgeSyncEvent event) {
        requestManagers.shareConsumeRequestManager.ifPresent(manager ->
            manager.commitSync(event.acknowledgementsMap(), event.deadlineMs())
                .whenComplete(complete(event.future())));
    }

    private void process(final ShareAcknowledgeAsyncEvent event) {
        requestManagers.shareConsumeRequestManager.ifPresent(manager ->
            manager.commitAsync(event.acknowledgementsMap(), event.deadlineMs()));
    }

    private void process(final ShareSubscriptionChangeEvent event) {
        if (requestManagers.shareHeartbeatRequestManager.isEmpty()) {
            event.future().completeExceptionally(
                new KafkaException("Group membership manager not present when processing a subscribe event"));
            return;
        }
        if (subscriptions.subscribeToShareGroup(event.topics()))
            metadata.requestUpdateForNewTopics();
        requestManagers.shareHeartbeatRequestManager.get().membershipManager().onSubscriptionUpdated();
        event.future().complete(null);
    }

    private void process(final ShareUnsubscribeEvent event) {
        if (requestManagers.shareHeartbeatRequestManager.isEmpty()) {
            event.future().completeExceptionally(
                new KafkaException("Group membership manager not present when processing an unsubscribe event"));
            return;
        }
        subscriptions.unsubscribe();
        requestManagers.shareHeartbeatRequestManager.get().membershipManager().leaveGroup()
            .whenComplete(complete(event.future()));
    }

    private void process(final ShareAcknowledgeOnCloseEvent event) {
        if (requestManagers.shareConsumeRequestManager.isEmpty()) {
            event.future().completeExceptionally(
                new KafkaException("Share consume manager not present when processing acknowledge-on-close"));
            return;
        }
        requestManagers.shareConsumeRequestManager.get()
            .acknowledgeOnClose(event.acknowledgementsMap(), event.deadlineMs())
            .whenComplete(complete(event.future()));
    }

    private void process(final ShareAcknowledgementCommitCallbackRegistrationEvent event) {
        requestManagers.shareConsumeRequestManager.ifPresent(manager ->
            manager.setAcknowledgementCommitCallbackRegistered(event.isCallbackRegistered()));
    }

    private <T> BiConsumer<? super T, ? super Throwable> complete(final CompletableFuture<T> future) {
        return (value, exception) -> {
            if (exception == null)
                future.complete(value);
            else
                future.completeExceptionally(exception);
        };
    }
}
