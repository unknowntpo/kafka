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
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.DefaultHostResolver;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import java.util.Map;

/**
 * Builds the engine's {@link NetworkClient}: the same wiring as {@code ClientUtils.createNetworkClient}, except
 * that the {@link Selector} reads into the engine's {@link MemoryPool} (the direct receive-buffer pool).
 */
final class EngineNetwork {

    private EngineNetwork() {
    }

    static NetworkClient createNetworkClient(ConsumerConfig config, LogContext logContext, Time time, Metrics metrics,
                                             ConsumerMetadata metadata, MemoryPool pool) {
        String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext);
        Selector selector = new Selector(NetworkReceive.UNLIMITED,
                config.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG), 0,
                metrics, time, ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX, Map.of(), false, false,
                channelBuilder, pool, logContext);
        return new NetworkClient(null, metadata, selector, clientId,
                1,
                config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG),
                config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                config.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG),
                config.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
                config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                time, true, new ApiVersions(), null, logContext,
                new DefaultHostResolver(), null,
                config.getLong(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG),
                MetadataRecoveryStrategy.forName(config.getString(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG)),
                ClientUtils.bootstrapConfiguration(config, config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)),
                config.getBoolean(CommonClientConfigs.METADATA_CLUSTER_CHECK_ENABLE_CONFIG));
    }
}
