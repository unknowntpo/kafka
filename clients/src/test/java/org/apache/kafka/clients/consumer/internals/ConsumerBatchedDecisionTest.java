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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Component tests: real managers and transport delegate, no broker or real clock. */
class ConsumerBatchedDecisionTest {
    private static final String GROUP_ID = "batch-group";
    private static final TopicPartition PARTITION = new TopicPartition("topic", 0);
    private static final Node NODE = new Node(1, "localhost", 9092);
    private final LogContext logContext = new LogContext();
    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(time);
    private final ConsumerMembershipManager membership = mock(ConsumerMembershipManager.class);
    private CoordinatorRequestManager coordinator;
    private CommitRequestManager commits;
    private MockClient client;
    private NetworkClientDelegate delegate;
    private ConsumerNetworkThread thread;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerMetadata metadata = mock(ConsumerMetadata.class);
        AsyncConsumerMetrics asyncMetrics = mock(AsyncConsumerMetrics.class);
        BackgroundEventHandler background = mock(BackgroundEventHandler.class);
        coordinator = new CoordinatorRequestManager(logContext, 100, 1000, GROUP_ID);
        discoverCoordinator();
        // The real discovery retry delay is already elapsed, not bypassed by the batch mechanism.
        time.sleep(1000);
        commits = new CommitRequestManager(time, logContext,
            new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST), config, coordinator,
            mock(OffsetCommitCallbackInvoker.class), GROUP_ID, Optional.empty(), 100, 1000,
            OptionalDouble.of(0), metrics, metadata);
        when(membership.groupInstanceId()).thenReturn(Optional.empty());
        when(membership.state()).thenReturn(MemberState.STABLE);
        ConsumerHeartbeatRequestManager.HeartbeatState heartbeatState = mock(ConsumerHeartbeatRequestManager.HeartbeatState.class);
        when(heartbeatState.buildRequestData()).thenAnswer(ignored -> new ConsumerGroupHeartbeatRequestData()
            .setGroupId(GROUP_ID).setMemberId("member").setMemberEpoch(1));
        ConsumerHeartbeatRequestManager heartbeat = new ConsumerHeartbeatRequestManager(logContext,
            time.timer(300_000), config, coordinator, membership, heartbeatState,
            new HeartbeatRequestState(logContext, time, 0, 100, 1000, 0), background, metrics);
        client = new MockClient(time, List.of(coordinator.coordinator().orElseThrow()));
        delegate = new NetworkClientDelegate(time, config, logContext, client, metadata, background, false, asyncMetrics);
        RequestManagers managers = mock(RequestManagers.class);
        when(managers.entries()).thenReturn(List.of(coordinator, commits, heartbeat));
        thread = new ConsumerNetworkThread(logContext, time, new LinkedBlockingQueue<>(),
            new CompletableEventReaper(logContext), () -> mock(ApplicationEventProcessor.class),
            () -> delegate, () -> managers, asyncMetrics);
        thread.initializeResources();
    }

    @AfterEach
    void tearDown() {
        if (thread != null)
            thread.close(Duration.ZERO);
        metrics.close();
    }

    private void discoverCoordinator() {
        NetworkClientDelegate.UnsentRequest request = coordinator.poll(time.milliseconds()).unsentRequests.get(0);
        request.handler().onComplete(new ClientResponse(
            new RequestHeader(ApiKeys.FIND_COORDINATOR, request.requestBuilder().build().version(), "test", 1),
            request.handler(), NODE.idString(), time.milliseconds(), time.milliseconds(), false, null, null,
            FindCoordinatorResponse.prepareResponse(Errors.NONE, GROUP_ID, NODE)));
    }

    private ClientRequest request(ApiKeys apiKey) {
        return client.requests().stream().filter(r -> r.requestBuilder().apiKey() == apiKey).findFirst().orElseThrow();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testHeartbeatInvalidationPrecedesFollowupAdmissionInEitherResponseOrder(boolean heartbeatFirst) {
        var first = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(1)));
        var followup = first.thenCompose(ignored -> commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(2))));
        thread.runOnce();
        ClientRequest commit = request(ApiKeys.OFFSET_COMMIT);
        ClientRequest heartbeat = request(ApiKeys.CONSUMER_GROUP_HEARTBEAT);
        var commitResponse = new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE));
        var heartbeatResponse = new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
            .setErrorCode(Errors.NOT_COORDINATOR.code()));
        if (heartbeatFirst) {
            client.respondToRequest(heartbeat, heartbeatResponse);
            client.respondToRequest(commit, commitResponse);
        } else {
            client.respondToRequest(commit, commitResponse);
            client.respondToRequest(heartbeat, heartbeatResponse);
        }

        long batchTime = time.milliseconds();
        thread.runOnce();
        assertTrue(first.isDone());
        assertFalse(first.isCompletedExceptionally(), "a valid success remains valid despite owner invalidation");
        assertFalse(followup.isDone());
        assertTrue(coordinator.coordinator().isEmpty());
        assertEquals(0, client.inFlightRequestCount(), "no transport send is interleaved with callbacks");
        assertEquals(1, delegate.unsentRequests().size(), "the post-batch pass admits discovery in this runOnce");
        assertEquals(ApiKeys.FIND_COORDINATOR, delegate.unsentRequests().peek().requestBuilder().apiKey());

        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, GROUP_ID, NODE));
        thread.runOnce();
        assertTrue(coordinator.coordinator().isPresent());
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(2, delegate.unsentRequests().size(), "recovery admits commit and heartbeat before returning");
        var nextCommit = delegate.unsentRequests().stream()
            .filter(r -> r.requestBuilder().apiKey() == ApiKeys.OFFSET_COMMIT).findFirst().orElseThrow();
        var builder = assertInstanceOf(OffsetCommitRequest.Builder.class, nextCommit.requestBuilder());
        assertEquals(2, builder.build().data().topics().get(0).partitions().get(0).committedOffset());
        assertEquals(coordinator.coordinator(), nextCommit.node());
        assertEquals(batchTime, time.milliseconds(), "no periodic timer tick is needed for this recovery");

        thread.runOnce();
        assertEquals(2, client.inFlightRequestCount(), "the next transport poll sends each admitted attempt once");
        thread.runOnce();
        assertEquals(2, client.inFlightRequestCount(), "an unchanged in-flight attempt cannot be duplicated");
    }

    @Test
    void testReadyFollowupIsAdmittedWithoutWaitingForUnrelatedHeartbeat() {
        var first = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(1)));
        var followup = first.thenCompose(ignored -> commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(2))));
        thread.runOnce();
        client.respondToRequest(request(ApiKeys.OFFSET_COMMIT), new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE)));
        thread.runOnce();
        assertTrue(first.isDone());
        assertFalse(followup.isDone());
        assertEquals(1, client.inFlightRequestCount(), "unrelated heartbeat is still in flight");
        assertEquals(1, delegate.unsentRequests().size(), "ready followup is admitted in the completion iteration");
        assertEquals(ApiKeys.OFFSET_COMMIT, delegate.unsentRequests().peek().requestBuilder().apiKey());
    }

    @Test
    void testImmediateFollowupResponseDoesNotRecursivelyDrainAnotherBatch() {
        var first = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(1)));
        var second = first.thenCompose(ignored -> commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(2))));
        var third = second.thenCompose(ignored -> commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(3))));
        thread.runOnce();
        client.respondToRequest(request(ApiKeys.OFFSET_COMMIT), new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE)));
        client.prepareResponse(new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE)));
        thread.runOnce();
        assertTrue(first.isDone());
        assertFalse(second.isDone(), "even an immediately available next response belongs to another I/O batch");
        assertFalse(third.isDone());
        assertEquals(1, delegate.unsentRequests().size());
        assertEquals(1, client.numAwaitingResponses());

        thread.runOnce();
        assertTrue(second.isDone());
        assertFalse(third.isDone());
        assertEquals(1, delegate.unsentRequests().size(), "only the next attempt is staged, not recursively executed");
    }

    @Test
    void testLateHeartbeatInvalidationCannotClearRediscoveredOwner() {
        var first = commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(1)));
        var followup = first.thenCompose(ignored -> commits.commitAsync(Map.of(PARTITION, new OffsetAndMetadata(2))));
        thread.runOnce();
        ClientRequest oldHeartbeat = request(ApiKeys.CONSUMER_GROUP_HEARTBEAT);
        long capturedVersion = coordinator.coordinatorVersion();
        coordinator.markCoordinatorUnknown("test rediscovery", time.milliseconds());
        discoverCoordinator();
        long currentVersion = coordinator.coordinatorVersion();
        assertTrue(currentVersion > capturedVersion);
        client.respondToRequest(request(ApiKeys.OFFSET_COMMIT), new OffsetCommitResponse(0, Map.of(PARTITION, Errors.NONE)));
        client.respondToRequest(oldHeartbeat, new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
            .setErrorCode(Errors.NOT_COORDINATOR.code())));

        thread.runOnce();
        assertFalse(first.isCompletedExceptionally());
        assertFalse(followup.isDone());
        assertTrue(coordinator.coordinator().isPresent());
        assertEquals(currentVersion, coordinator.coordinatorVersion());
        assertTrue(delegate.unsentRequests().stream().anyMatch(r -> r.requestBuilder().apiKey() == ApiKeys.OFFSET_COMMIT),
            "owner-side version fencing remains necessary even with an ordered completion batch");
    }
}
