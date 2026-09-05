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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConsumerAdmissionContractTest {
    private static final String DEFAULT_GROUP_ID = "group";
    private final LogContext logContext = new LogContext();
    private final MockTime time = new MockTime();
    private final long retryBackoffMs = 100;
    private final long retryBackoffMaxMs = 1000;
    private final long defaultApiTimeoutMs = 60_000;
    private final Node mockedNode = new Node(1, "localhost", 9092);
    private final ConsumerMetadata metadata = mock(ConsumerMetadata.class);
    private final Metrics metrics = new Metrics();
    private final Properties props = new Properties();
    private CoordinatorRequestManager coordinatorRequestManager;

    @AfterEach
    void closeMetrics() {
        metrics.close();
    }

    private CommitRequestManager create(boolean autoCommitEnabled, long autoCommitInterval) {
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommitEnabled);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, (int) autoCommitInterval);
        return new CommitRequestManager(time, logContext,
            new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST), new ConsumerConfig(props),
            coordinatorRequestManager, mock(OffsetCommitCallbackInvoker.class), DEFAULT_GROUP_ID, Optional.empty(),
            retryBackoffMs, retryBackoffMaxMs, OptionalDouble.of(0), metrics, metadata);
    }

    private void discoverCoordinator() {
        NetworkClientDelegate.UnsentRequest request = coordinatorRequestManager.poll(time.milliseconds()).unsentRequests.get(0);
        request.handler().onComplete(new ClientResponse(
            new RequestHeader(ApiKeys.FIND_COORDINATOR, request.requestBuilder().build().version(), "test", 1),
            request.handler(), mockedNode.idString(), time.milliseconds(), time.milliseconds(), false, null, null,
            org.apache.kafka.common.requests.FindCoordinatorResponse.prepareResponse(Errors.NONE, DEFAULT_GROUP_ID, mockedNode)));
    }

    @Test
    public void testCompletionBatchFinishesBeforeReentrantCommitAdmission() throws Exception {
        coordinatorRequestManager = new CoordinatorRequestManager(logContext, retryBackoffMs, retryBackoffMaxMs, DEFAULT_GROUP_ID);
        discoverCoordinator();
        // Let the legitimate discovery backoff expire before the completion race starts.
        time.sleep(retryBackoffMaxMs);
        CommitRequestManager manager = create(false, 100);
        TopicPartition tp = new TopicPartition("topic", 0);
        var firstCommit = manager.commitAsync(Map.of(tp, new OffsetAndMetadata(1)));
        // A completed operation makes distinct work ready before the second response invalidates its owner.
        var followup = firstCommit.thenCompose(ignored -> manager.commitAsync(Map.of(tp, new OffsetAndMetadata(2))));
        manager.fetchOffsets(Set.of(tp), time.milliseconds() + defaultApiTimeoutMs);
        var client = new org.apache.kafka.clients.MockClient(time, List.of(coordinatorRequestManager.coordinator().orElseThrow()));
        var asyncMetrics = mock(org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics.class);
        var managers = mock(RequestManagers.class);
        when(managers.entries()).thenReturn(List.of(coordinatorRequestManager, manager));
        var processor = mock(org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor.class);
        try (NetworkClientDelegate delegate = new NetworkClientDelegate(time, new ConsumerConfig(props), logContext,
                client, metadata, mock(org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler.class), false, asyncMetrics)) {
            ConsumerNetworkThread thread = new ConsumerNetworkThread(logContext, time, new java.util.concurrent.LinkedBlockingQueue<>(),
                new org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper(logContext),
                () -> processor, () -> delegate, () -> managers, asyncMetrics);
            try {
                thread.initializeResources();
                client.prepareResponse(new OffsetCommitResponse(0, Map.of(tp, Errors.NONE)));
                client.prepareResponse(new OffsetFetchResponse(new OffsetFetchResponseData().setGroups(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId(DEFAULT_GROUP_ID)
                        .setErrorCode(Errors.NOT_COORDINATOR.code()))), (short) 9));
                thread.runOnce();
                assertTrue(firstCommit.isDone());
                assertFalse(followup.isDone());
                assertTrue(coordinatorRequestManager.coordinator().isEmpty());
                assertEquals(0, client.inFlightRequestCount(), "no new request may be sent between completion callbacks");
                assertTrue(delegate.unsentRequests().isEmpty(), "no speculative work may be staged after the batch");

                client.prepareResponse(org.apache.kafka.common.requests.FindCoordinatorResponse.prepareResponse(
                    Errors.NONE, DEFAULT_GROUP_ID, mockedNode));
                thread.runOnce();
                assertTrue(coordinatorRequestManager.coordinator().isPresent());
                assertEquals(0, client.inFlightRequestCount(), "recovery does not admit sibling work in the discovery callback");
                thread.runOnce();
                assertEquals(1, client.inFlightRequestCount(), "the very next full pass sends the ready commit without a timer tick");
                assertInstanceOf(OffsetCommitRequest.Builder.class, client.requests().peek().requestBuilder());
            } finally {
                thread.close(java.time.Duration.ZERO);
            }
        }
    }
}
