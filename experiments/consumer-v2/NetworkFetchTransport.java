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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.PlaintextChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;

/** Bounded wire probe adapter: fixed node/partition, Fetch v12, no group or fetch session. */
final class NetworkFetchTransport implements FetchBridge.Transport {
    private final Metrics metrics = new Metrics();
    private final NetworkClient client;
    private final Node node;
    private final TopicPartition partition;
    private final FetchSessionHandler session;
    private final int fetchBytes;
    private final int fetchWaitMs;
    private Consumer<FetchResponseData.PartitionData> callback;
    private long offset;
    private boolean sent;
    private RuntimeException failure;

    NetworkFetchTransport(int port) throws Exception { this(port, "test", 4096, false); }
    NetworkFetchTransport(int port, String topic, int fetchBytes, boolean sessions) throws Exception {
        this(port, topic, fetchBytes, sessions, 500);
    }
    NetworkFetchTransport(int port, String topic, int fetchBytes, boolean sessions, int fetchWaitMs) throws Exception {
        this.fetchWaitMs = fetchWaitMs;
        partition = new TopicPartition(topic, 0);
        this.fetchBytes = fetchBytes;
        node = new Node(0, "127.0.0.1", port);
        LogContext log = new LogContext();
        session = sessions ? new FetchSessionHandler(log, 0) : null;
        PlaintextChannelBuilder builder = new PlaintextChannelBuilder(null);
        builder.configure(Collections.emptyMap());
        Selector selector = new Selector(-1L, metrics, Time.SYSTEM, "fetch-wire-probe", builder, log);
        client = new NetworkClient(selector, new ManualMetadataUpdater(Collections.singletonList(node)),
            "fetch-wire-probe", 1, 50L, 1000L, 65536, 65536, 2000, 1000L, 2000L,
            Time.SYSTEM, false, new ApiVersions(), log, MetadataRecoveryStrategy.NONE);
    }
    public void send(long requestOffset, Consumer<FetchResponseData.PartitionData> completion) {
        if (callback != null) throw new IllegalStateException("Request already pending");
        offset = requestOffset;
        callback = completion;
        sent = false;
    }
    private void sendIfReady() {
        if (callback == null || sent) return;
        long now = Time.SYSTEM.milliseconds();
        if (!client.ready(node, now)) return;
        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(Uuid.ZERO_UUID, offset,
            -1, fetchBytes, Optional.empty());
        java.util.Map<TopicPartition, FetchRequest.PartitionData> toSend;
        FetchSessionHandler.FetchRequestData sessionRequest = null;
        if (session != null) {
            FetchSessionHandler.Builder builder = session.newBuilder();
            builder.add(partition, partitionData);
            sessionRequest = builder.build();
            toSend = sessionRequest.toSend();
        } else toSend = Collections.singletonMap(partition, partitionData);
        FetchRequest.Builder builder = FetchRequest.Builder.forConsumer((short) 12, fetchWaitMs, 1, toSend)
            .isolationLevel(org.apache.kafka.common.IsolationLevel.READ_COMMITTED).setMaxBytes(fetchBytes);
        if (sessionRequest != null) builder.metadata(sessionRequest.metadata())
            .removed(sessionRequest.toForget()).replaced(sessionRequest.toReplace());
        sent = true;
        client.send(client.newClientRequest(node.idString(), builder, now, true, 2000, response -> {
            // NetworkClient catches callback exceptions, so report them after poll returns.
            try {
                if (!response.hasResponse()) throw new IllegalStateException("Fetch failed/disconnected/timed out");
                FetchResponse fetch = (FetchResponse) response.responseBody();
                if (session != null && !session.handleResponse(fetch, (short) 12))
                    throw new IllegalStateException("Fetch session response rejected");
                if (fetch.error().code() != 0) throw new IllegalStateException("Fetch top-level error");
                FetchResponseData.PartitionData data = fetch.responseData(Collections.emptyMap(), (short) 12).get(partition);
                if (data == null && session != null) data = new FetchResponseData.PartitionData()
                    .setPartitionIndex(0).setHighWatermark(-1).setLastStableOffset(-1).setLogStartOffset(-1)
                    .setRecords(MemoryRecords.EMPTY);
                if (data == null) throw new IllegalStateException("Missing fetch partition");
                Consumer<FetchResponseData.PartitionData> completion = callback;
                callback = null;
                sent = false;
                completion.accept(data);
            } catch (RuntimeException error) { failure = error; }
        }), now);
    }
    public void poll(boolean mayBlock) {
        sendIfReady();
        client.poll(mayBlock ? Long.MAX_VALUE : 0L, Time.SYSTEM.milliseconds());
        if (failure != null) throw failure;
        sendIfReady();
    }
    int sessionId() { return session == null ? 0 : session.sessionId(); }
    public void wakeup() { client.wakeup(); }
    public void cancel() { callback = null; sent = false; client.close(node.idString()); }
    public void close() { client.close(); metrics.close(); }
}
