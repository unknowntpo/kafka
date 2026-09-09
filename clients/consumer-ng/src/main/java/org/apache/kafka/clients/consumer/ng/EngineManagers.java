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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerHeartbeatRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager;
import org.apache.kafka.clients.consumer.internals.MemberState;
import org.apache.kafka.clients.consumer.internals.MemberStateListener;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.OffsetCommitCallbackInvoker;
import org.apache.kafka.clients.consumer.internals.OffsetsRequestManager;
import org.apache.kafka.clients.consumer.internals.PositionsValidator;
import org.apache.kafka.clients.consumer.internals.RequestManager;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.TopicMetadataRequestManager;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The request managers reused from the previous implementation, wired the same way its {@code RequestManagers} did:
 * group-related ones only when {@code group.id} is set, the positions and topic-metadata ones always.
 */
final class EngineManagers {

    final Optional<CoordinatorRequestManager> coordinator;
    final Optional<CommitRequestManager> commit;
    final Optional<ConsumerMembershipManager> membership;
    final Optional<ConsumerHeartbeatRequestManager> heartbeat;
    final OffsetsRequestManager offsets;
    final TopicMetadataRequestManager topicMetadata;
    final SubscriptionUpdater subscriptionUpdater;

    EngineManagers(ConsumerConfig config, LogContext logContext, Time time, Metrics metrics, SubscriptionState subscriptions,
                   ConsumerMetadata metadata, BackgroundEventHandler backgroundEventHandler, NetworkClientDelegate network,
                   ApiVersions apiVersions, OffsetCommitCallbackInvoker commitCallbackInvoker,
                   MemberStateListener applicationMemberStateListener, Consumer<Boolean> onReconcilingChange) {
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config, GroupRebalanceConfig.ProtocolType.CONSUMER);
        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        int requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        int defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        IsolationLevel isolationLevel = IsolationLevel.valueOf(config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT));

        CoordinatorRequestManager coordinatorManager = null;
        CommitRequestManager commitManager = null;
        ConsumerMembershipManager membershipManager = null;
        ConsumerHeartbeatRequestManager heartbeatManager = null;
        if (groupRebalanceConfig.groupId != null) {
            coordinatorManager = new CoordinatorRequestManager(logContext, retryBackoffMs, retryBackoffMaxMs, groupRebalanceConfig.groupId);
            commitManager = new CommitRequestManager(time, logContext, subscriptions, config, coordinatorManager, commitCallbackInvoker,
                    groupRebalanceConfig.groupId, groupRebalanceConfig.groupInstanceId, metrics, metadata);
            membershipManager = new ConsumerMembershipManager(groupRebalanceConfig.groupId, groupRebalanceConfig.groupInstanceId,
                    groupRebalanceConfig.rackId, groupRebalanceConfig.rebalanceTimeoutMs,
                    Optional.ofNullable(config.getString(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG)),
                    subscriptions, commitManager, metadata, logContext, backgroundEventHandler, time, metrics,
                    config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
            membershipManager.registerStateListener(commitManager);
            membershipManager.registerStateListener(applicationMemberStateListener);
            membershipManager.registerStateListener(new MemberStateListener() {
                @Override
                public void onMemberEpochUpdated(Optional<Integer> memberEpoch, String memberId) {
                }

                @Override
                public void onMemberStateChange(MemberState state) {
                    onReconcilingChange.accept(state == MemberState.RECONCILING);
                }
            });
            heartbeatManager = new ConsumerHeartbeatRequestManager(logContext, time, config, coordinatorManager, subscriptions,
                    membershipManager, backgroundEventHandler, metrics);
        }
        this.coordinator = Optional.ofNullable(coordinatorManager);
        this.commit = Optional.ofNullable(commitManager);
        this.membership = Optional.ofNullable(membershipManager);
        this.heartbeat = Optional.ofNullable(heartbeatManager);
        this.topicMetadata = new TopicMetadataRequestManager(logContext, time, config);
        this.subscriptionUpdater = new SubscriptionUpdater(subscriptions, metadata, membership);
        PositionsValidator positionsValidator = new PositionsValidator(logContext, time, subscriptions, metadata);
        this.offsets = new OffsetsRequestManager(subscriptions, metadata, isolationLevel, time, retryBackoffMs, requestTimeoutMs,
                defaultApiTimeoutMs, apiVersions, network, commitManager, positionsValidator, logContext);
    }

    /** In scheduling order: coordinator first (the others send to it), positions and topic metadata last. */
    List<RequestManager> all() {
        List<RequestManager> list = new ArrayList<>();
        coordinator.ifPresent(list::add);
        commit.ifPresent(list::add);
        heartbeat.ifPresent(list::add);
        membership.ifPresent(list::add);
        list.add(offsets);
        list.add(topicMetadata);
        return list;
    }
}
