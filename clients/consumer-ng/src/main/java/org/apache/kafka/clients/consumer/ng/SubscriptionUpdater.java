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
package org.apache.kafka.clients.consumer.ng;

import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Cluster;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Subscription changes, on the I/O thread: a topic list, a client-side regex (resolved against the topics in
 * metadata, re-evaluated once per poll when metadata changed) or a broker-side RE2/J regex (sent in the heartbeat).
 * Same rules as the previous implementation's event processor.
 */
final class SubscriptionUpdater {

    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final Optional<ConsumerMembershipManager> membershipManager;
    private int metadataVersionSnapshot;

    SubscriptionUpdater(SubscriptionState subscriptions, ConsumerMetadata metadata, Optional<ConsumerMembershipManager> membershipManager) {
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.membershipManager = membershipManager;
    }

    void subscribe(Set<String> topics) {
        if (subscriptions.subscribe(topics))
            metadataVersionSnapshot = metadata.requestUpdateForNewTopics();
        membershipManager.ifPresent(ConsumerMembershipManager::onSubscriptionUpdated);
    }

    void subscribe(Pattern pattern) {
        subscriptions.subscribe(pattern);
        metadata.requestUpdateForNewTopics();
        updatePatternSubscription(metadata.fetch());
    }

    void subscribe(SubscriptionPattern pattern) {
        subscriptions.subscribe(pattern);
        membershipManager.ifPresent(ConsumerMembershipManager::onSubscriptionUpdated);
    }

    /** Once per poll: re-evaluate the client-side regex if metadata changed since the last evaluation. */
    void maybeUpdatePatternSubscription() {
        if (!subscriptions.hasPatternSubscription())
            return;
        if (metadataVersionSnapshot < metadata.updateVersion()) {
            metadataVersionSnapshot = metadata.updateVersion();
            updatePatternSubscription(metadata.fetch());
        }
    }

    private void updatePatternSubscription(Cluster cluster) {
        Set<String> topicsToSubscribe = new HashSet<>();
        for (String topic : cluster.topics())
            if (subscriptions.matchesSubscribedPattern(topic))
                topicsToSubscribe.add(topic);
        if (subscriptions.subscribeFromPattern(topicsToSubscribe))
            metadataVersionSnapshot = metadata.requestUpdateForNewTopics();
        // Even with no matching topic the member must (re)join with the (empty) subscription.
        membershipManager.ifPresent(ConsumerMembershipManager::onSubscriptionUpdated);
    }
}
