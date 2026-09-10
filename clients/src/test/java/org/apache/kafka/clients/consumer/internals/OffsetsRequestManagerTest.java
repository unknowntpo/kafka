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
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OffsetsRequestManagerTest {

    private OffsetsRequestManager requestManager;
    private ConsumerMetadata metadata;
    private SubscriptionState subscriptionState;
    private final Time time = mock(Time.class);
    private ApiVersions apiVersions;
    private final CommitRequestManager commitRequestManager = mock(CommitRequestManager.class);
    private static final String TEST_TOPIC = "t1";
    private static final TopicPartition TEST_PARTITION_1 = new TopicPartition(TEST_TOPIC, 1);
    private static final TopicPartition TEST_PARTITION_2 = new TopicPartition(TEST_TOPIC, 2);
    private static final Node LEADER_1 = new Node(0, "host1", 9092);
    private static final Node LEADER_2 = new Node(0, "host2", 9092);
    private static final IsolationLevel DEFAULT_ISOLATION_LEVEL = IsolationLevel.READ_COMMITTED;
    private static final int RETRY_BACKOFF_MS = 500;
    private static final int REQUEST_TIMEOUT_MS = 500;
    private static final int DEFAULT_API_TIMEOUT_MS = 500;

    @BeforeEach
    public void setup() {
        LogContext logContext = new LogContext();
        metadata = mock(ConsumerMetadata.class);
        subscriptionState = mock(SubscriptionState.class);
        apiVersions = mock(ApiVersions.class);
        requestManager = new OffsetsRequestManager(
                subscriptionState,
                metadata,
                DEFAULT_ISOLATION_LEVEL,
                time,
                RETRY_BACKOFF_MS,
                REQUEST_TIMEOUT_MS,
                DEFAULT_API_TIMEOUT_MS,
                apiVersions,
                mock(NetworkClientDelegate.class),
                commitRequestManager,
                new PositionsValidator(logContext, time, subscriptionState, metadata),
                logContext
        );
    }

    @Test
    public void testListOffsetsRequest_Success() throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        mockSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> result = requestManager.fetchOffsets(
                timestampsToSearch,
                false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        Map<TopicPartition, OffsetAndTimestampInternal> expectedOffsets = Collections.singletonMap(
                TEST_PARTITION_1,
                new OffsetAndTimestampInternal(5L, -1, Optional.empty()));
        verifySuccessfulPollAndResponseReceived(result, expectedOffsets);
    }

    @Test
    public void testListOffsetsWaitingForMetadataUpdate_Timeout() {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // Building list offsets request fails with unknown leader
        mockFailedRequest_MissingLeader();
        CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> fetchOffsetsFuture =
            requestManager.fetchOffsets(timestampsToSearch, false);

        assertEquals(0, requestManager.requestsToSend());
        assertEquals(1, requestManager.requestsToRetry());
        verify(metadata).requestUpdate(true);
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        // Metadata update not happening within the time boundaries of the request future, so
        // future should time out.
        assertThrows(TimeoutException.class, () -> fetchOffsetsFuture.get(5L, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testListOffsetsRequestMultiplePartitions() throws ExecutionException,
            InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        timestampsToSearch.put(TEST_PARTITION_1, ListOffsetsRequest.EARLIEST_TIMESTAMP);
        timestampsToSearch.put(TEST_PARTITION_2, ListOffsetsRequest.EARLIEST_TIMESTAMP);


        Map<TopicPartition, Node> partitionLeaders = new HashMap<>();
        partitionLeaders.put(TEST_PARTITION_1, LEADER_1);
        partitionLeaders.put(TEST_PARTITION_2, LEADER_1);
        mockSuccessfulRequest(partitionLeaders);
        CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> result = requestManager.fetchOffsets(
                        timestampsToSearch,
                        false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        Map<TopicPartition, OffsetAndTimestampInternal> expectedOffsets = timestampsToSearch.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> new OffsetAndTimestampInternal(5L, -1, Optional.empty())));
        verifySuccessfulPollAndResponseReceived(result, expectedOffsets);
    }

    @Test
    public void testListOffsetsRequestEmpty() throws ExecutionException, InterruptedException {
        CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> result = requestManager.fetchOffsets(
                        Collections.emptyMap(),
                        false);
        assertEquals(0, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        NetworkClientDelegate.PollResult pollResult = requestManager.poll(time.milliseconds());
        assertTrue(pollResult.unsentRequests.isEmpty());

        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());

        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        assertTrue(result.get().isEmpty());
    }

    @Test
    public void testListOffsetsRequestUnknownOffset() throws ExecutionException,
            InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        mockSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> result = requestManager.fetchOffsets(
                timestampsToSearch,
                false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        List<ListOffsetsResponseData.ListOffsetsTopicResponse> topicResponses = Collections.singletonList(
                mockUnknownOffsetResponse(TEST_PARTITION_1));

        NetworkClientDelegate.PollResult retriedPoll = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(retriedPoll);
        NetworkClientDelegate.UnsentRequest unsentRequest = retriedPoll.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponse(unsentRequest, topicResponses);
        clientResponse.onComplete();
        Map<TopicPartition, OffsetAndTimestampInternal> expectedOffsets = Collections.singletonMap(TEST_PARTITION_1, null);
        verifyRequestSuccessfullyCompleted(result, expectedOffsets);
    }

    @Test
    public void testListOffsetsWaitingForMetadataUpdate_RetrySucceeds() throws ExecutionException,
            InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // Building list offsets request fails with unknown leader
        mockFailedRequest_MissingLeader();
        CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> fetchOffsetsFuture =
            requestManager.fetchOffsets(timestampsToSearch, false);
        assertEquals(0, requestManager.requestsToSend());
        assertEquals(1, requestManager.requestsToRetry());
        verify(metadata).requestUpdate(true);

        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        assertEquals(0, res.unsentRequests.size());
        assertFalse(fetchOffsetsFuture.isDone());

        // Cluster metadata update. Previously failed attempt to build the request should be retried
        // and succeed
        mockSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        requestManager.onUpdate(new ClusterResource(""));
        assertEquals(1, requestManager.requestsToSend());

        Map<TopicPartition, OffsetAndTimestampInternal> expectedOffsets = Collections.singletonMap(
                TEST_PARTITION_1, new OffsetAndTimestampInternal(5L, -1, Optional.empty()));
        verifySuccessfulPollAndResponseReceived(fetchOffsetsFuture, expectedOffsets);
    }

    /**
     * Test for KAFKA-20312: when regroupPartitionMapByNode sees a partition with null leader
     * (e.g. metadata race), it should skip that partition and add it to remainingToSearch instead
     * of throwing NullPointerException.
     */
    @Test
    public void testFetchOffsetsRegroupSkipsNullLeaderPartitionNoNPE() throws ExecutionException,
            InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        timestampsToSearch.put(TEST_PARTITION_1, ListOffsetsRequest.EARLIEST_TIMESTAMP);
        timestampsToSearch.put(TEST_PARTITION_2, ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // currentLeader returns a leader for both partitions (so both enter partitionDataMap)
        when(metadata.currentLeader(TEST_PARTITION_1)).thenReturn(testLeaderEpoch(LEADER_1, Optional.empty()));
        when(metadata.currentLeader(TEST_PARTITION_2)).thenReturn(testLeaderEpoch(LEADER_2, Optional.empty()));
        when(subscriptionState.isAssigned(any(TopicPartition.class))).thenReturn(true);

        // metadata.fetch() returns a cluster where PARTITION_2 has null leader (e.g. race: leader lost)
        List<PartitionInfo> partitions = new ArrayList<>();
        partitions.add(new PartitionInfo(TEST_TOPIC, 1, LEADER_1, null, null));
        partitions.add(new PartitionInfo(TEST_TOPIC, 2, null, null, null));
        Cluster clusterWithNullLeader = new Cluster("clusterId", Collections.singletonList(LEADER_1),
                partitions, Collections.emptySet(), Collections.emptySet());
        when(metadata.fetch()).thenReturn(clusterWithNullLeader);

        CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> fetchOffsetsFuture =
                assertDoesNotThrow(
                        () -> requestManager.fetchOffsets(timestampsToSearch, false),
                        "Should not throw NPE; only PARTITION_1 has a leader in regroup, so one request for LEADER_1");
        assertEquals(1, requestManager.requestsToSend());
        // requestsToRetry is populated when the in-flight request completes and remainingToSearch is non-empty, not yet
        assertEquals(0, requestManager.requestsToRetry());

        // Complete request for PARTITION_1
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponse(unsentRequest,
                Collections.singletonMap(TEST_PARTITION_1, new OffsetAndTimestampInternal(5L, -1, Optional.empty())));
        clientResponse.onComplete();
        assertFalse(fetchOffsetsFuture.isDone());

        // Metadata update: now both partitions have leaders; retry should send request for PARTITION_2
        mockSuccessfulRequest(Map.of(TEST_PARTITION_1, LEADER_1, TEST_PARTITION_2, LEADER_2));
        requestManager.onUpdate(new ClusterResource(""));
        assertEquals(1, requestManager.requestsToSend());

        // Complete the retry request (only PARTITION_2 in this batch)
        NetworkClientDelegate.PollResult retryPoll = requestManager.poll(time.milliseconds());
        assertEquals(1, retryPoll.unsentRequests.size());
        ClientResponse retryResponse = buildClientResponse(retryPoll.unsentRequests.get(0),
                Collections.singletonMap(TEST_PARTITION_2, new OffsetAndTimestampInternal(10L, -1, Optional.empty())));
        retryResponse.onComplete();

        Map<TopicPartition, OffsetAndTimestampInternal> expectedOffsets = new HashMap<>();
        expectedOffsets.put(TEST_PARTITION_1, new OffsetAndTimestampInternal(5L, -1, Optional.empty()));
        expectedOffsets.put(TEST_PARTITION_2, new OffsetAndTimestampInternal(10L, -1, Optional.empty()));
        verifyRequestSuccessfullyCompleted(fetchOffsetsFuture, expectedOffsets);
    }

    @ParameterizedTest
    @MethodSource("retriableErrors")
    public void testRequestFailsWithRetriableError_RetrySucceeds(Errors error) throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // List offsets request successfully built
        mockSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> fetchOffsetsFuture = requestManager.fetchOffsets(
                timestampsToSearch,
                false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Request successfully sent to single broker
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(res);
        assertFalse(fetchOffsetsFuture.isDone());

        // Response received with error
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponseWithErrors(
                unsentRequest,
                Collections.singletonMap(TEST_PARTITION_1, error));
        clientResponse.onComplete();
        assertFalse(fetchOffsetsFuture.isDone());
        assertEquals(1, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());
        // A retriable error should be followed by a metadata update request
        verify(metadata).requestUpdate(false);

        // Cluster metadata update. Failed requests should be retried and succeed
        mockSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        requestManager.onUpdate(new ClusterResource(""));
        assertEquals(1, requestManager.requestsToSend());

        Map<TopicPartition, OffsetAndTimestampInternal> expectedOffsets =
                Collections.singletonMap(TEST_PARTITION_1, new OffsetAndTimestampInternal(5L, -1, Optional.empty()));
        verifySuccessfulPollAndResponseReceived(fetchOffsetsFuture, expectedOffsets);
    }

    @Test
    public void testRequestNotSupportedErrorReturnsNullOffset() throws ExecutionException,
            InterruptedException {
        testResponseWithErrorCodeAndUnknownOffsets(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT);
    }

    @Test
    public void testRequestWithUnknownOffsetInResponseReturnsNullOffset() throws ExecutionException,
            InterruptedException {
        testResponseWithErrorCodeAndUnknownOffsets(Errors.NONE);
    }

    private void testResponseWithErrorCodeAndUnknownOffsets(Errors error) throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // List offsets request successfully built
        mockSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> fetchOffsetsFuture = requestManager.fetchOffsets(
                timestampsToSearch,
                false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Request successfully sent to single broker
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(res);
        assertFalse(fetchOffsetsFuture.isDone());

        // Response received with error
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponseWithErrors(
                unsentRequest,
                Collections.singletonMap(TEST_PARTITION_1, error));
        clientResponse.onComplete();

        // Null offsets should be returned for each partition
        Map<TopicPartition, OffsetAndTimestampInternal> expectedOffsets =
                Collections.singletonMap(TEST_PARTITION_1, null);
        verifyRequestSuccessfullyCompleted(fetchOffsetsFuture, expectedOffsets);
    }

    @Test
    public void testRequestPartiallyFailsWithRetriableError_RetrySucceeds() throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        timestampsToSearch.put(TEST_PARTITION_1, ListOffsetsRequest.EARLIEST_TIMESTAMP);
        timestampsToSearch.put(TEST_PARTITION_2, ListOffsetsRequest.EARLIEST_TIMESTAMP);

        Map<TopicPartition, OffsetAndTimestampInternal> expectedOffsets = timestampsToSearch.entrySet().stream()
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> new OffsetAndTimestampInternal(5L, -1, Optional.empty())));

        // List offsets request to 2 brokers successfully built
        Map<TopicPartition, Node> partitionLeaders = new HashMap<>();
        partitionLeaders.put(TEST_PARTITION_1, LEADER_1);
        partitionLeaders.put(TEST_PARTITION_2, LEADER_2);
        mockSuccessfulRequest(partitionLeaders);
        CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> fetchOffsetsFuture = requestManager.fetchOffsets(
                timestampsToSearch,
                false);
        assertEquals(2, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Requests successfully sent to both brokers
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(res, 2);
        assertFalse(fetchOffsetsFuture.isDone());

        // Mixed response with failures and successes. Offsets successfully fetched from one
        // broker but retriable UNKNOWN_LEADER_EPOCH received from second broker.
        NetworkClientDelegate.UnsentRequest unsentRequest1 = res.unsentRequests.get(0);
        long offsets = expectedOffsets.get(TEST_PARTITION_1).offset();
        ClientResponse clientResponse1 = buildClientResponse(
                unsentRequest1,
                Collections.singletonMap(TEST_PARTITION_1,
                        new OffsetAndTimestampInternal(offsets, -1L, Optional.empty())));
        clientResponse1.onComplete();
        NetworkClientDelegate.UnsentRequest unsentRequest2 = res.unsentRequests.get(1);
        ClientResponse clientResponse2 = buildClientResponseWithErrors(
            unsentRequest2,
            Collections.singletonMap(TEST_PARTITION_2, Errors.UNKNOWN_LEADER_EPOCH));
        clientResponse2.onComplete();

        assertFalse(fetchOffsetsFuture.isDone());
        assertEquals(1, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());
        // A retriable error should be followed by a metadata update request
        verify(metadata).requestUpdate(false);

        // Cluster metadata update. Failed requests should be retried
        mockSuccessfulRequest(partitionLeaders);
        requestManager.onUpdate(new ClusterResource(""));
        assertEquals(1, requestManager.requestsToSend());

        // Following poll should send the request and get a successful response
        NetworkClientDelegate.PollResult retriedPoll = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(retriedPoll);
        NetworkClientDelegate.UnsentRequest unsentRequest = retriedPoll.unsentRequests.get(0);
        long offsets2 = expectedOffsets.get(TEST_PARTITION_2).offset();
        ClientResponse clientResponse = buildClientResponse(unsentRequest,
            Collections.singletonMap(TEST_PARTITION_2,
                    new OffsetAndTimestampInternal(offsets2, -1L, Optional.empty())));
        clientResponse.onComplete();

        // Verify global result with the offset initially retrieved, and the offset that
        // initially failed but succeeded after a metadata update
        verifyRequestSuccessfullyCompleted(fetchOffsetsFuture, expectedOffsets);
    }

    @Test
    public void testRequestFailedResponse_NonRetriableAuthError() {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // List offsets request successfully built
        mockSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> fetchOffsetsFuture =
                requestManager.fetchOffsets(
                        timestampsToSearch,
                        false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Request successfully sent
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(res);

        // Response received with non-retriable auth error
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponseWithErrors(
                unsentRequest, Collections.singletonMap(TEST_PARTITION_2, Errors.TOPIC_AUTHORIZATION_FAILED));
        clientResponse.onComplete();

        verifyRequestCompletedWithErrorResponse(fetchOffsetsFuture, TopicAuthorizationException.class);
        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testRequestFailedResponse_NonRetriableErrorTimeout() {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // List offsets request successfully built
        mockSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> fetchOffsetsFuture =
                requestManager.fetchOffsets(
                        timestampsToSearch,
                        false);
        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Request successfully sent
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(res);

        // Response received
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponseWithErrors(
                unsentRequest, Collections.singletonMap(TEST_PARTITION_2, Errors.BROKER_NOT_AVAILABLE));
        clientResponse.onComplete();

        assertFalse(fetchOffsetsFuture.isDone());
        assertThrows(TimeoutException.class, () -> fetchOffsetsFuture.get(5L, TimeUnit.MILLISECONDS));

        // Request completed with error. Nothing pending to be sent or retried
        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testRequestFails_AuthenticationException() {
        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);

        // List offsets request successfully built
        mockSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> fetchOffsetsFuture =
            requestManager.fetchOffsets(
                    timestampsToSearch,
                    false);

        assertEquals(1, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());

        // Request successfully sent
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(res);

        // Response received with auth error
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        ClientResponse clientResponse =
            buildClientResponse(unsentRequest,
                Collections.emptyList(),
                false,
                new AuthenticationException("Authentication failed"));
        clientResponse.onComplete();

        assertTrue(fetchOffsetsFuture.isCompletedExceptionally());
        Throwable failure = assertThrows(ExecutionException.class, fetchOffsetsFuture::get);
        assertEquals(AuthenticationException.class, failure.getCause().getClass());

        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testResetPositionsSendNoRequestIfNoPartitionsNeedingReset() {
        when(subscriptionState.partitionsNeedingReset(time.milliseconds())).thenReturn(Collections.emptySet());
        requestManager.resetPositionsIfNeeded();
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testResetPositionsMissingLeader() {
        mockFailedRequest_MissingLeader();
        when(subscriptionState.partitionsNeedingReset(time.milliseconds())).thenReturn(Collections.singleton(TEST_PARTITION_1));
        when(subscriptionState.resetStrategy(any())).thenReturn(AutoOffsetResetStrategy.EARLIEST);
        requestManager.resetPositionsIfNeeded();
        verify(metadata).requestUpdate(true);
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testResetWaitsForMetadataWithoutSchedulerRedrive() {
        prepareMissingResetMetadata(Set.of(TEST_PARTITION_1));
        CompletableFuture<Void> result = requestManager.resetPositionsIfNeeded();
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            assertTrue(scheduler.poll(requestManager, 0).unsentRequests.isEmpty());
            for (int i = 1; i <= 100; i++) {
                assertNull(scheduler.poll(requestManager, i));
                requestManager.onUpdate(new ClusterResource("unchanged"));
                assertFalse(result.isDone());
                assertNull(scheduler.poll(requestManager, i));
            }
            assertEquals(Long.MAX_VALUE, scheduler.remainingMs(100));
            mockSuccessfulRequest(Map.of(TEST_PARTITION_1, LEADER_1));
            requestManager.onUpdate(new ClusterResource("leader available"));
            NetworkClientDelegate.PollResult poll = scheduler.poll(requestManager, 100);
            assertEquals(1, poll.unsentRequests.size());
            assertFalse(result.isDone());
            completeResetRequest(poll.unsentRequests.get(0), TEST_PARTITION_1);
            assertTrue(result.isDone());
            assertFalse(result.isCompletedExceptionally());
            verify(subscriptionState).maybeSeekUnvalidated(eq(TEST_PARTITION_1), any(), any());
            assertNull(scheduler.poll(requestManager, 101));
        }
    }

    @Test
    public void testRepeatedResetReusesPendingMetadataWork() {
        prepareMissingResetMetadata(Set.of(TEST_PARTITION_1));
        CompletableFuture<Void> result = requestManager.resetPositionsIfNeeded();
        assertSame(result, requestManager.resetPositionsIfNeeded());
        mockSuccessfulRequest(Map.of(TEST_PARTITION_1, LEADER_1));
        requestManager.onUpdate(new ClusterResource("leader available"));
        NetworkClientDelegate.PollResult poll = requestManager.poll(0);
        assertEquals(1, poll.unsentRequests.size());
        completeResetRequest(poll.unsentRequests.get(0), TEST_PARTITION_1);
        assertTrue(result.isDone());
    }

    @Test
    public void testPartialResetWaitsForMissingPartitionWithoutResendingKnownPartition() {
        prepareMissingResetMetadata(Set.of(TEST_PARTITION_1, TEST_PARTITION_2));
        mockSuccessfulRequest(Map.of(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Void> result = requestManager.resetPositionsIfNeeded();
        NetworkClientDelegate.PollResult first = requestManager.poll(0);
        assertEquals(1, first.unsentRequests.size());
        completeResetRequest(first.unsentRequests.get(0), TEST_PARTITION_1);
        assertFalse(result.isDone());
        mockSuccessfulRequest(Map.of(TEST_PARTITION_1, LEADER_1, TEST_PARTITION_2, LEADER_1));
        requestManager.onUpdate(new ClusterResource("second leader"));
        NetworkClientDelegate.PollResult second = requestManager.poll(0);
        assertEquals(1, second.unsentRequests.size());
        ListOffsetsRequest request = (ListOffsetsRequest) second.unsentRequests.get(0).requestBuilder().build();
        assertEquals(1, request.data().topics().get(0).partitions().size());
        assertEquals(TEST_PARTITION_2.partition(), request.data().topics().get(0).partitions().get(0).partitionIndex());
        completeResetRequest(second.unsentRequests.get(0), TEST_PARTITION_2);
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
    }

    @Test
    public void testFatalResetErrorDoesNotWaitForUnrelatedMetadata() {
        prepareMissingResetMetadata(Set.of(TEST_PARTITION_1, TEST_PARTITION_2));
        mockSuccessfulRequest(Map.of(TEST_PARTITION_1, LEADER_1));
        CompletableFuture<Void> result = requestManager.resetPositionsIfNeeded();
        NetworkClientDelegate.UnsentRequest request = requestManager.poll(0).unsentRequests.get(0);
        buildClientResponseWithErrors(request, Map.of(TEST_PARTITION_1, Errors.TOPIC_AUTHORIZATION_FAILED)).onComplete();
        assertTrue(result.isDone());
        assertFutureThrows(TopicAuthorizationException.class, requestManager.resetPositionsIfNeeded());
        mockSuccessfulRequest(Map.of(TEST_PARTITION_2, LEADER_1));
        requestManager.onUpdate(new ClusterResource("late metadata"));
        assertEquals(0, requestManager.requestsToSend());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMetadataDoesNotRestartObsoleteReset(boolean revoked) {
        prepareMissingResetMetadata(Set.of(TEST_PARTITION_1));
        CompletableFuture<Void> result = requestManager.resetPositionsIfNeeded();
        mockSuccessfulRequest(Map.of(TEST_PARTITION_1, LEADER_1));
        if (revoked)
            when(subscriptionState.isAssigned(TEST_PARTITION_1)).thenReturn(false);
        else
            when(subscriptionState.resetStrategy(TEST_PARTITION_1)).thenReturn(AutoOffsetResetStrategy.LATEST);
        requestManager.onUpdate(new ClusterResource("changed assignment or reset"));
        assertTrue(result.isDone());
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testCloseFailsMetadataWaitAndIgnoresLateMetadata() {
        prepareMissingResetMetadata(Set.of(TEST_PARTITION_1));
        CompletableFuture<Void> result = requestManager.resetPositionsIfNeeded();
        requestManager.closePendingPositionResets();
        assertTrue(result.isCompletedExceptionally());
        mockSuccessfulRequest(Map.of(TEST_PARTITION_1, LEADER_1));
        requestManager.onUpdate(new ClusterResource("late metadata"));
        assertEquals(0, requestManager.requestsToSend());
        assertTrue(requestManager.resetPositionsIfNeeded().isCompletedExceptionally());
    }

    private void prepareMissingResetMetadata(Set<TopicPartition> partitions) {
        mockFailedRequest_MissingLeader();
        when(subscriptionState.partitionsNeedingReset(time.milliseconds())).thenReturn(partitions);
        when(subscriptionState.resetStrategy(any())).thenReturn(AutoOffsetResetStrategy.EARLIEST);
        partitions.forEach(tp -> when(subscriptionState.isOffsetResetNeeded(tp)).thenReturn(true));
    }

    private void completeResetRequest(NetworkClientDelegate.UnsentRequest request, TopicPartition partition) {
        buildClientResponse(request, Map.of(partition, new OffsetAndTimestampInternal(5L, 1L, Optional.empty()))).onComplete();
    }

    @Test
    public void testResetPositionsSuccess_NoLeaderEpochInResponse() {
        testResetPositionsSuccessWithLeaderEpoch(Metadata.LeaderAndEpoch.noLeaderOrEpoch());
        verify(metadata, never()).updateLastSeenEpochIfNewer(any(), anyInt());
    }

    @Test
    public void testResetPositionsSuccess_LeaderEpochInResponse() {
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(Optional.of(LEADER_1),
                Optional.of(5));
        testResetPositionsSuccessWithLeaderEpoch(leaderAndEpoch);
        verify(metadata).updateLastSeenEpochIfNewer(TEST_PARTITION_1, leaderAndEpoch.epoch.get());
    }

    @Test
    public void testResetOffsetsAuthorizationFailure() {
        when(subscriptionState.partitionsNeedingReset(time.milliseconds())).thenReturn(Collections.singleton(TEST_PARTITION_1));
        when(subscriptionState.resetStrategy(any())).thenReturn(AutoOffsetResetStrategy.EARLIEST);
        mockSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));

        CompletableFuture<Void> resetResult = requestManager.resetPositionsIfNeeded();

        // Reset positions response with TopicAuthorizationException
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        assertFalse(resetResult.isDone());
        Errors topicAuthorizationFailedError = Errors.TOPIC_AUTHORIZATION_FAILED;
        ClientResponse clientResponse = buildClientResponseWithErrors(
                unsentRequest, Collections.singletonMap(TEST_PARTITION_1, topicAuthorizationFailedError));
        clientResponse.onComplete();

        assertTrue(unsentRequest.future().isDone());
        assertTrue(resetResult.isDone());
        assertFalse(unsentRequest.future().isCompletedExceptionally());

        verify(subscriptionState).requestFailed(any(), anyLong());
        verify(metadata).requestUpdate(false);

        // Following resetPositions should throw the exception
        CompletableFuture<Void> nextReset = assertDoesNotThrow(() -> requestManager.resetPositionsIfNeeded());
        assertEquals(0, requestManager.requestsToSend());
        assertTrue(nextReset.isCompletedExceptionally());
        assertFutureThrows(TopicAuthorizationException.class, nextReset);
    }

    @Nested
    class ValidationCompletionTest {
        private NetworkClientDelegate validationNetwork;

        private void realValidationState() {
            LogContext context = new LogContext();
            subscriptionState = new SubscriptionState(context, AutoOffsetResetStrategy.EARLIEST);
            subscriptionState.assignFromUser(Set.of(TEST_PARTITION_1));
            Metadata.LeaderAndEpoch leader = new Metadata.LeaderAndEpoch(Optional.of(LEADER_1), Optional.of(3));
            when(metadata.currentLeader(TEST_PARTITION_1)).thenReturn(leader);
            apiVersions = new ApiVersions();
            apiVersions.update(LEADER_1.idString(), NodeApiVersions.create());
            validationNetwork = mock(NetworkClientDelegate.class);
            subscriptionState.seekUnvalidated(TEST_PARTITION_1,
                    new SubscriptionState.FetchPosition(5, Optional.of(2), leader));
            requestManager = new OffsetsRequestManager(subscriptionState, metadata, DEFAULT_ISOLATION_LEVEL,
                    time, RETRY_BACKOFF_MS, REQUEST_TIMEOUT_MS, DEFAULT_API_TIMEOUT_MS, apiVersions,
                    validationNetwork, null,
                    new PositionsValidator(context, time, subscriptionState, metadata), context);
        }

        @Test
        void obsoleteMetadataResetDoesNotSendAfterSameStrategyReset() {
            realValidationState();
            when(metadata.fetch()).thenReturn(testClusterMetadata(Map.of()));
            subscriptionState.requestOffsetReset(TEST_PARTITION_1, AutoOffsetResetStrategy.EARLIEST);
            CompletableFuture<Void> first = requestManager.resetPositionsIfNeeded();
            subscriptionState.requestOffsetReset(TEST_PARTITION_1, AutoOffsetResetStrategy.EARLIEST);
            CompletableFuture<Void> second = requestManager.resetPositionsIfNeeded();
            assertFalse(first == second, "same strategy with a new position operation cannot coalesce");
            when(metadata.fetch()).thenReturn(testClusterMetadata(Map.of(TEST_PARTITION_1, LEADER_1)));
            requestManager.onUpdate(new ClusterResource("new-reset"));
            NetworkClientDelegate.PollResult poll = requestManager.poll(0);
            assertEquals(1, poll.unsentRequests.size());
            buildClientResponse(poll.unsentRequests.get(0), Map.of(TEST_PARTITION_1,
                    new OffsetAndTimestampInternal(20, -1, Optional.empty()))).onComplete();
            assertEquals(20, subscriptionState.position(TEST_PARTITION_1).offset);
            assertTrue(first.isDone());
            assertTrue(second.isDone());
        }

        @Test
        void partialObsoleteCommittedErrorRefetchesCurrentPartitionBeforeReset() {
            realCommittedPositionState();
            TopicPartition kept = new TopicPartition("kept", 0);
            subscriptionState.assignFromUser(Set.of(TEST_PARTITION_1, kept));
            when(metadata.currentLeader(kept)).thenReturn(
                    new Metadata.LeaderAndEpoch(Optional.of(LEADER_1), Optional.of(3)));
            CompletableFuture<CommitRequestManager.OffsetFetchResult> old = new CompletableFuture<>();
            CompletableFuture<CommitRequestManager.OffsetFetchResult> current = new CompletableFuture<>();
            when(commitRequestManager.fetchOffsets(anySet(), anyLong(), any())).thenReturn(old, current);
            CompletableFuture<Void> result = requestManager.updateFetchPositions(5000);
            subscriptionState.seek(TEST_PARTITION_1, 200);
            old.completeExceptionally(new TopicAuthorizationException(Set.of(TEST_PARTITION_1.topic())));
            verify(commitRequestManager).fetchOffsets(eq(Set.of(kept)), anyLong(), any());
            assertNull(subscriptionState.position(kept));
            assertFalse(subscriptionState.isOffsetResetNeeded(kept));
            assertFalse(result.isDone());
            current.complete(new CommitRequestManager.OffsetFetchResult(Map.of(kept, new OffsetAndMetadata(20)), Map.of()));
            assertEquals(20, subscriptionState.position(kept).offset);
            assertEquals(200, subscriptionState.position(TEST_PARTITION_1).offset);
            assertTrue(result.isDone());
            assertFalse(result.isCompletedExceptionally());
        }

        @Test
        void repeatedResetRejectsOldResponseEvenWithSameStrategy() {
            realValidationState();
            when(metadata.fetch()).thenReturn(testClusterMetadata(Map.of(TEST_PARTITION_1, LEADER_1)));
            subscriptionState.requestOffsetReset(TEST_PARTITION_1, AutoOffsetResetStrategy.EARLIEST);
            CompletableFuture<Void> first = requestManager.resetPositionsIfNeeded();
            NetworkClientDelegate.UnsentRequest old = requestManager.poll(0).unsentRequests.get(0);
            subscriptionState.requestOffsetReset(TEST_PARTITION_1, AutoOffsetResetStrategy.EARLIEST);
            CompletableFuture<Void> second = requestManager.resetPositionsIfNeeded();
            NetworkClientDelegate.UnsentRequest current = requestManager.poll(0).unsentRequests.get(0);
            buildClientResponse(old, Map.of(TEST_PARTITION_1,
                    new OffsetAndTimestampInternal(10, -1, Optional.empty()))).onComplete();
            assertTrue(subscriptionState.isOffsetResetNeeded(TEST_PARTITION_1));
            assertFalse(second.isDone());
            buildClientResponse(current, Map.of(TEST_PARTITION_1,
                    new OffsetAndTimestampInternal(20, -1, Optional.empty()))).onComplete();
            assertEquals(20, subscriptionState.position(TEST_PARTITION_1).offset);
            assertTrue(first.isDone());
            assertTrue(second.isDone());
        }

        @Test
        void seekReleasesResetWaitingForMetadata() {
            realValidationState();
            when(metadata.fetch()).thenReturn(testClusterMetadata(Map.of()));
            subscriptionState.requestOffsetReset(TEST_PARTITION_1, AutoOffsetResetStrategy.EARLIEST);
            CompletableFuture<Void> result = requestManager.resetPositionsIfNeeded();
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                assertTrue(scheduler.poll(requestManager, 0).unsentRequests.isEmpty());
                assertFalse(result.isDone());
                subscriptionState.seek(TEST_PARTITION_1, 200);
                requestManager.onPositionStateChanged();
                assertFalse(result.isDone());
                assertNotNull(scheduler.poll(requestManager, 1));
                assertTrue(result.isDone());
                assertEquals(200, subscriptionState.position(TEST_PARTITION_1).offset);
            }
        }

        @Test
        void reassignedPartitionDoesNotReuseOrApplyOldCommittedOffsets() {
            realCommittedPositionState();
            CompletableFuture<CommitRequestManager.OffsetFetchResult> old = new CompletableFuture<>();
            CompletableFuture<CommitRequestManager.OffsetFetchResult> current = new CompletableFuture<>();
            when(commitRequestManager.fetchOffsets(anySet(), anyLong(), any())).thenReturn(old, current);
            CompletableFuture<Void> first = requestManager.updateFetchPositions(5000);
            subscriptionState.assignFromUser(Set.of());
            subscriptionState.assignFromUser(Set.of(TEST_PARTITION_1));
            CompletableFuture<Void> second = requestManager.updateFetchPositions(5000);
            verify(commitRequestManager, times(2)).fetchOffsets(anySet(), anyLong(), any());
            old.complete(new CommitRequestManager.OffsetFetchResult(
                    Map.of(TEST_PARTITION_1, new OffsetAndMetadata(10)), Map.of()));
            assertNull(subscriptionState.position(TEST_PARTITION_1));
            CompletableFuture<Void> reused = requestManager.updateFetchPositions(5000);
            verify(commitRequestManager, times(2)).fetchOffsets(anySet(), anyLong(), any());
            current.complete(new CommitRequestManager.OffsetFetchResult(
                    Map.of(TEST_PARTITION_1, new OffsetAndMetadata(20)), Map.of()));
            assertEquals(20, subscriptionState.position(TEST_PARTITION_1).offset);
            assertTrue(first.isDone());
            assertTrue(second.isDone());
            assertTrue(reused.isDone());
            assertFalse(second.isCompletedExceptionally());
        }

        @Test
        void closeFencesLateCommittedOffsetResponse() {
            realCommittedPositionState();
            CompletableFuture<CommitRequestManager.OffsetFetchResult> response = new CompletableFuture<>();
            when(commitRequestManager.fetchOffsets(anySet(), anyLong(), any())).thenReturn(response);
            CompletableFuture<Void> result = requestManager.updateFetchPositions(5000);
            requestManager.closePendingPositionResets();
            assertTrue(result.isCompletedExceptionally());
            response.complete(new CommitRequestManager.OffsetFetchResult(
                    Map.of(TEST_PARTITION_1, new OffsetAndMetadata(10)), Map.of()));
            assertNull(subscriptionState.position(TEST_PARTITION_1));
        }

        private void realCommittedPositionState() {
            realValidationState();
            subscriptionState.assignFromUser(Set.of());
            subscriptionState.assignFromUser(Set.of(TEST_PARTITION_1));
            LogContext context = new LogContext();
            requestManager.closePendingPositionResets();
            requestManager = new OffsetsRequestManager(subscriptionState, metadata, DEFAULT_ISOLATION_LEVEL,
                    time, RETRY_BACKOFF_MS, REQUEST_TIMEOUT_MS, DEFAULT_API_TIMEOUT_MS, apiVersions,
                    validationNetwork, commitRequestManager,
                    new PositionsValidator(context, time, subscriptionState, metadata), context);
        }

        @Test
        void stalePartitionAuthorizationErrorDoesNotPoisonUnchangedPartition() {
            realValidationState();
            TopicPartition kept = new TopicPartition("kept", 0);
            subscriptionState.assignFromUser(Set.of(TEST_PARTITION_1, kept));
            Metadata.LeaderAndEpoch leader = new Metadata.LeaderAndEpoch(Optional.of(LEADER_1), Optional.of(3));
            when(metadata.currentLeader(kept)).thenReturn(leader);
            subscriptionState.seekUnvalidated(kept, new SubscriptionState.FetchPosition(5, Optional.of(2), leader));
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true);
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                NetworkClientDelegate.UnsentRequest old = scheduler.poll(requestManager, 0).unsentRequests.get(0);
                subscriptionState.seek(TEST_PARTITION_1, 200);
                requestManager.onPositionStateChanged();
                scheduler.poll(requestManager, 1);
                ClientResponse response = buildOffsetsForLeaderEpochResponse(old, List.of(TEST_PARTITION_1, kept), 100);
                ((OffsetsForLeaderEpochResponse) response.responseBody()).data().topics().find(TEST_PARTITION_1.topic())
                        .partitions().get(0).setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
                response.onComplete();
                scheduler.poll(requestManager, 1);
                assertTrue(result.isDone());
                assertFalse(result.isCompletedExceptionally());
                assertFalse(subscriptionState.awaitingValidation(kept));
                assertEquals(200, subscriptionState.position(TEST_PARTITION_1).offset);
            }
        }

        @Test
        void validSeekDetachesFromObsoleteValidationRpc() {
            realValidationState();
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true);
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                NetworkClientDelegate.UnsentRequest old = scheduler.poll(requestManager, 0).unsentRequests.get(0);
                subscriptionState.seek(TEST_PARTITION_1, 200);
                requestManager.onPositionStateChanged();
                assertFalse(result.isDone());
                scheduler.poll(requestManager, 1);
                assertTrue(result.isDone(), "valid seek must not await an obsolete RPC");
                buildOffsetsForLeaderEpochResponse(old, List.of(TEST_PARTITION_1), 1).onComplete();
                assertEquals(200, subscriptionState.position(TEST_PARTITION_1).offset);
                assertTrue(requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true).isDone());
            }
        }

        @Test
        void samePositionSeekDoesNotAcceptOldValidationResponse() {
            realValidationState();
            SubscriptionState.FetchPosition samePosition = subscriptionState.position(TEST_PARTITION_1);
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true);
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                NetworkClientDelegate.UnsentRequest old = scheduler.poll(requestManager, 0).unsentRequests.get(0);
                subscriptionState.seekUnvalidated(TEST_PARTITION_1, samePosition);
                requestManager.onPositionStateChanged();
                NetworkClientDelegate.PollResult next = scheduler.poll(requestManager, 1);
                assertEquals(1, next.unsentRequests.size(), "new seek owns its own validation");
                buildOffsetsForLeaderEpochResponse(old, List.of(TEST_PARTITION_1), 1).onComplete();
                assertTrue(subscriptionState.awaitingValidation(TEST_PARTITION_1));
                assertEquals(5, subscriptionState.position(TEST_PARTITION_1).offset);
                assertFalse(result.isDone());
                buildOffsetsForLeaderEpochResponse(next.unsentRequests.get(0), List.of(TEST_PARTITION_1), 100).onComplete();
                scheduler.poll(requestManager, 1);
                assertTrue(result.isDone());
                assertFalse(result.isCompletedExceptionally());
            }
        }

        @Test
        void reassignedSamePartitionRejectsOldValidationFailure() {
            realValidationState();
            SubscriptionState.FetchPosition samePosition = subscriptionState.position(TEST_PARTITION_1);
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true);
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                NetworkClientDelegate.UnsentRequest old = scheduler.poll(requestManager, 0).unsentRequests.get(0);
                subscriptionState.assignFromUser(Set.of());
                subscriptionState.assignFromUser(Set.of(TEST_PARTITION_1));
                subscriptionState.seekUnvalidated(TEST_PARTITION_1, samePosition);
                requestManager.onPositionStateChanged();
                NetworkClientDelegate.PollResult next = scheduler.poll(requestManager, 1);
                assertEquals(1, next.unsentRequests.size());
                buildOffsetsForLeaderEpochResponseWithErrors(old,
                        Map.of(TEST_PARTITION_1, Errors.TOPIC_AUTHORIZATION_FAILED)).onComplete();
                assertFalse(result.isDone());
                buildOffsetsForLeaderEpochResponse(next.unsentRequests.get(0), List.of(TEST_PARTITION_1), 100).onComplete();
                scheduler.poll(requestManager, 1);
                assertTrue(result.isDone());
                assertFalse(result.isCompletedExceptionally(), "old generation error must not poison new demand");
            }
        }

        @Test
        void validSeekActivatesParkedValidationWithoutNetworkPublication() {
            realValidationState();
            apiVersions.remove(LEADER_1.idString());
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true);
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                assertTrue(scheduler.poll(requestManager, 0).unsentRequests.isEmpty());
                assertNull(scheduler.poll(requestManager, 100));
                subscriptionState.seek(TEST_PARTITION_1, 200);
                requestManager.onPositionStateChanged();
                assertFalse(result.isDone(), "notification must not run the owner inline");
                assertNotNull(scheduler.poll(requestManager, 100));
                assertTrue(result.isDone());
                assertFalse(result.isCompletedExceptionally());
                assertEquals(200, subscriptionState.position(TEST_PARTITION_1).offset);
                assertNull(scheduler.poll(requestManager, 101), "consumed notification must not spin");
            }
        }

        @Test
        void sameTopicAssignmentActivatesParkedValidationWithoutMetadataUpdate() {
            realValidationState();
            apiVersions.remove(LEADER_1.idString());
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true);
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                scheduler.poll(requestManager, 0);
                assertNull(scheduler.poll(requestManager, 100));
                TopicPartition replacement = new TopicPartition(TEST_PARTITION_1.topic(), 99);
                assertFalse(subscriptionState.assignFromUser(Set.of(replacement)),
                        "same-topic assignment does not request new topic metadata");
                subscriptionState.seek(replacement, 300);
                requestManager.onPositionStateChanged();
                assertFalse(result.isDone());
                assertNotNull(scheduler.poll(requestManager, 100));
                assertTrue(result.isDone());
                assertFalse(result.isCompletedExceptionally());
                assertEquals(Set.of(replacement), subscriptionState.assignedPartitions());
            }
        }

        @Test
        void seekStillNeedingValidationRechecksOnceThenParks() {
            realValidationState();
            apiVersions.remove(LEADER_1.idString());
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true);
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                scheduler.poll(requestManager, 0);
                assertNull(scheduler.poll(requestManager, 100));
                subscriptionState.seekUnvalidated(TEST_PARTITION_1,
                        new SubscriptionState.FetchPosition(200, Optional.of(2),
                                new Metadata.LeaderAndEpoch(Optional.of(LEADER_1), Optional.of(3))));
                requestManager.onPositionStateChanged();
                NetworkClientDelegate.PollResult recheck = scheduler.poll(requestManager, 100);
                assertNotNull(recheck);
                assertTrue(recheck.unsentRequests.isEmpty());
                assertFalse(result.isDone(), "seek notification is not proof of validation");
                assertNull(scheduler.poll(requestManager, 101));
                apiVersions.update(LEADER_1.idString(), NodeApiVersions.create());
                NetworkClientDelegate.PollResult validation = scheduler.poll(requestManager, 101);
                assertEquals(1, validation.unsentRequests.size());
                buildOffsetsForLeaderEpochResponse(validation.unsentRequests.get(0),
                        List.of(TEST_PARTITION_1), 300).onComplete();
                scheduler.poll(requestManager, 101);
                assertTrue(result.isDone());
                assertFalse(result.isCompletedExceptionally());
                assertEquals(200, subscriptionState.position(TEST_PARTITION_1).offset);
            }
        }

        @Test
        void delayedResetPastValidationRetryDeadlineActivatesRetainedDemand() {
            realValidationState();
            subscriptionState.assignFromUser(Set.of(TEST_PARTITION_1, TEST_PARTITION_2));
            when(metadata.currentLeader(TEST_PARTITION_2)).thenReturn(
                    new Metadata.LeaderAndEpoch(Optional.of(LEADER_1), Optional.of(3)));
            when(metadata.fetch()).thenReturn(testClusterMetadata(
                    Map.of(TEST_PARTITION_1, LEADER_1, TEST_PARTITION_2, LEADER_1)));
            subscriptionState.setNextAllowedRetry(Set.of(TEST_PARTITION_1), 100);

            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true);
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                NetworkClientDelegate.PollResult initial = scheduler.poll(requestManager, 0);
                assertEquals(1, initial.unsentRequests.size());
                NetworkClientDelegate.UnsentRequest reset = initial.unsentRequests.get(0);
                assertInstanceOf(ListOffsetsRequest.Builder.class, reset.requestBuilder());
                assertFalse(result.isDone());
                assertNull(scheduler.poll(requestManager, 100),
                        "pending reset preparation must not be polled on validation retry expiry");

                when(time.milliseconds()).thenReturn(150L);
                buildClientResponse(reset, Map.of(TEST_PARTITION_2,
                        new OffsetAndTimestampInternal(10L, -1, Optional.empty()))).onComplete();
                assertFalse(result.isDone(), "reset completion does not complete the other partition's validation");
                assertEquals(10, subscriptionState.position(TEST_PARTITION_2).offset);

                NetworkClientDelegate.PollResult validation = scheduler.poll(requestManager, 150);
                assertNotNull(validation, "preparation completion must activate the retained owner");
                assertEquals(1, validation.unsentRequests.size(),
                        "elapsed validation retry must resume without another application poll");
                buildOffsetsForLeaderEpochResponse(validation.unsentRequests.get(0),
                        List.of(TEST_PARTITION_1), 100).onComplete();
                scheduler.poll(requestManager, 150);
                assertTrue(result.isDone());
                assertFalse(result.isCompletedExceptionally());
            }
        }

        @Test
        void missingLeaderWaitsForMetadataInsteadOfAnotherApplicationPoll() {
            realValidationState();
            Metadata.LeaderAndEpoch unknown = new Metadata.LeaderAndEpoch(Optional.of(Node.noNode()), Optional.of(3));
            when(metadata.currentLeader(TEST_PARTITION_1)).thenReturn(unknown);
            subscriptionState.seekUnvalidated(TEST_PARTITION_1,
                    new SubscriptionState.FetchPosition(5, Optional.of(2), unknown));
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true);
            assertFalse(result.isDone(), "missing leader must retain the operation");
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                assertTrue(scheduler.poll(requestManager, 0).unsentRequests.isEmpty());
                assertNull(scheduler.poll(requestManager, 100), "no periodic readiness probe");
                when(metadata.currentLeader(TEST_PARTITION_1)).thenReturn(
                        new Metadata.LeaderAndEpoch(Optional.of(LEADER_1), Optional.of(4)));
                when(metadata.updateVersion()).thenReturn(1);
                requestManager.onUpdate(new ClusterResource("validation"));
                assertFalse(result.isDone(), "metadata is not validation completion");
                NetworkClientDelegate.PollResult poll = scheduler.poll(requestManager, 100);
                assertEquals(1, poll.unsentRequests.size());
                buildOffsetsForLeaderEpochResponse(poll.unsentRequests.get(0), List.of(TEST_PARTITION_1), 100).onComplete();
                scheduler.poll(requestManager, 100);
                assertTrue(result.isDone());
                assertFalse(result.isCompletedExceptionally());
            }
        }

        @Test
        void retriableValidationResponseWaitsForActualBackoff() {
            realValidationState();
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true);
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                NetworkClientDelegate.UnsentRequest first = scheduler.poll(requestManager, 0).unsentRequests.get(0);
                buildOffsetsForLeaderEpochResponseWithErrors(first,
                        Map.of(TEST_PARTITION_1, Errors.NOT_LEADER_OR_FOLLOWER)).onComplete();
                NetworkClientDelegate.PollResult waiting = scheduler.poll(requestManager, 0);
                assertFalse(result.isDone(), "retry response is not successful validation");
                assertTrue(waiting.unsentRequests.isEmpty());
                assertEquals(RETRY_BACKOFF_MS, waiting.nextPollCondition.remainingMs(0));
                when(time.milliseconds()).thenReturn((long) RETRY_BACKOFF_MS - 1);
                assertNull(scheduler.poll(requestManager, RETRY_BACKOFF_MS - 1));
                when(time.milliseconds()).thenReturn((long) RETRY_BACKOFF_MS);
                NetworkClientDelegate.PollResult retry = scheduler.poll(requestManager, RETRY_BACKOFF_MS);
                assertEquals(1, retry.unsentRequests.size());
                buildOffsetsForLeaderEpochResponse(retry.unsentRequests.get(0), List.of(TEST_PARTITION_1), 100).onComplete();
                scheduler.poll(requestManager, RETRY_BACKOFF_MS);
                assertTrue(result.isDone());
                assertFalse(result.isCompletedExceptionally());
            }
        }

        @Test
        void apiVersionsPublicationActivatesOwnerWithoutInlineRequest() {
            realValidationState();
            apiVersions.remove(LEADER_1.idString());
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true);
            assertFalse(result.isDone());
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                assertTrue(scheduler.poll(requestManager, 0).unsentRequests.isEmpty());
                assertNull(scheduler.poll(requestManager, 100));
                apiVersions.update(LEADER_1.idString(), NodeApiVersions.create());
                assertEquals(0, requestManager.requestsToSend(), "transport must not run in the listener");
                NetworkClientDelegate.PollResult poll = scheduler.poll(requestManager, 100);
                assertEquals(1, poll.unsentRequests.size());
                apiVersions.update(LEADER_1.idString(), NodeApiVersions.create());
                assertTrue(scheduler.poll(requestManager, 100).unsentRequests.isEmpty(), "no duplicate in-flight validation");
                buildOffsetsForLeaderEpochResponse(poll.unsentRequests.get(0), List.of(TEST_PARTITION_1), 100).onComplete();
                scheduler.poll(requestManager, 100);
                assertTrue(result.isDone());
                assertFalse(result.isCompletedExceptionally());
            }
        }

        @Test
        void failedFirstHandshakeActivatesReconnectAtTransportBackoff() {
            realValidationState();
            apiVersions.remove(LEADER_1.idString());
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true);
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                scheduler.poll(requestManager, 0);
                when(validationNetwork.isUnavailable(LEADER_1)).thenReturn(true);
                when(validationNetwork.connectionDelay(LEADER_1, 0)).thenReturn(200L);
                apiVersions.remove(LEADER_1.idString()); // No cached entry existed for this failed handshake.
                NetworkClientDelegate.PollResult wait = scheduler.poll(requestManager, 0);
                assertTrue(wait.unsentRequests.isEmpty());
                assertEquals(200, wait.nextPollCondition.remainingMs(0));
                when(time.milliseconds()).thenReturn(199L);
                assertNull(scheduler.poll(requestManager, 199));
                when(time.milliseconds()).thenReturn(200L);
                when(validationNetwork.isUnavailable(LEADER_1)).thenReturn(false);
                assertTrue(scheduler.poll(requestManager, 200).unsentRequests.isEmpty());
                verify(validationNetwork, times(3)).tryConnect(LEADER_1);
                assertNull(scheduler.poll(requestManager, 300), "connecting waits for publication, not a periodic probe");
                apiVersions.update(LEADER_1.idString(), NodeApiVersions.create());
                NetworkClientDelegate.PollResult request = scheduler.poll(requestManager, 300);
                assertEquals(1, request.unsentRequests.size());
                buildOffsetsForLeaderEpochResponse(request.unsentRequests.get(0), List.of(TEST_PARTITION_1), 100).onComplete();
                scheduler.poll(requestManager, 300);
                assertTrue(result.isDone());
            }
        }

        @Test
        void reconnectBackoffExpiringDuringCheckDoesNotStrandDemand() {
            realValidationState();
            apiVersions.remove(LEADER_1.idString());
            when(validationNetwork.isUnavailable(LEADER_1)).thenReturn(true, false);
            when(validationNetwork.connectionDelay(LEADER_1, 0)).thenReturn(0L);
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true);
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                assertTrue(scheduler.poll(requestManager, 0).unsentRequests.isEmpty());
                verify(validationNetwork, times(2)).tryConnect(LEADER_1);
                assertFalse(result.isDone());
                assertNull(scheduler.poll(requestManager, 100));
                apiVersions.update(LEADER_1.idString(), NodeApiVersions.create());
                assertEquals(1, scheduler.poll(requestManager, 100).unsentRequests.size());
            }
        }

        @Test
        void authenticationFailureAfterMissingVersionsReachesRetainedOperation() {
            realValidationState();
            apiVersions.remove(LEADER_1.idString());
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> true);
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                scheduler.poll(requestManager, 0);
                apiVersions.remove(LEADER_1.idString());
                // NetworkClient publishes the final authentication state after removing versions.
                org.mockito.Mockito.doThrow(new org.apache.kafka.common.errors.AuthenticationException("denied"))
                        .when(validationNetwork).maybeThrowAuthFailure(LEADER_1);
                assertFalse(result.isDone());
                assertTrue(scheduler.poll(requestManager, 0).unsentRequests.isEmpty());
                assertFutureThrows(org.apache.kafka.common.errors.AuthenticationException.class, result);
            }
        }

        @Test
        void missingVersionsDeadlineAndLatePublicationDoNotRestartDemand() {
            realValidationState();
            apiVersions.remove(LEADER_1.idString());
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(100, () -> true);
            try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
                scheduler.poll(requestManager, 0);
                when(time.milliseconds()).thenReturn(100L);
                assertTrue(scheduler.poll(requestManager, 100).unsentRequests.isEmpty());
                assertFutureThrows(org.apache.kafka.common.errors.TimeoutException.class, result);
                apiVersions.update(LEADER_1.idString(), NodeApiVersions.create());
                assertNull(scheduler.poll(requestManager, 100));
            }
        }

        @Test
        void terminalGuardPreventsInitialConnectionAttempt() {
            realValidationState();
            apiVersions.remove(LEADER_1.idString());
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(5000, () -> false);
            assertTrue(result.isDone());
            verify(validationNetwork, never()).tryConnect(any());
        }

        @Test
        void responsePublishesPositionsBeforeOwnerCompletesFuture() {
            realValidationState();
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(1000, () -> true);
            assertFalse(result.isDone());
            assertFalse(subscriptionState.hasAllFetchPositions());
            NetworkClientDelegate.UnsentRequest request = requestManager.poll(0).unsentRequests.get(0);
            buildOffsetsForLeaderEpochResponse(request, List.of(TEST_PARTITION_1), 100).onComplete();
            assertTrue(subscriptionState.hasAllFetchPositions());
            assertFalse(result.isDone(), "response must activate the owner, not inline the next phase");
            requestManager.poll(0);
            assertTrue(result.isDone());
            assertFalse(result.isCompletedExceptionally());
        }

        @Test
        void cachedValidationErrorReachesWaitingOperation() {
            realValidationState();
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(1000, () -> true);
            NetworkClientDelegate.UnsentRequest request = requestManager.poll(0).unsentRequests.get(0);
            buildOffsetsForLeaderEpochResponseWithErrors(request,
                    Map.of(TEST_PARTITION_1, Errors.TOPIC_AUTHORIZATION_FAILED)).onComplete();
            assertFalse(result.isDone());
            requestManager.poll(0);
            assertFutureThrows(TopicAuthorizationException.class, result);
        }

        @Test
        void apiDeadlineExpiresWithoutResponseAndLateResponseCannotRestartWork() {
            realValidationState();
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(100, () -> true);
            NetworkClientDelegate.PollResult poll = requestManager.poll(0);
            assertEquals(100, poll.nextPollCondition.remainingMs(0));
            when(time.milliseconds()).thenReturn(100L);
            // The network-loop timestamp may precede work done by other managers.
            requestManager.poll(0);
            assertFutureThrows(org.apache.kafka.common.errors.TimeoutException.class, result);
            buildOffsetsForLeaderEpochResponse(poll.unsentRequests.get(0), List.of(TEST_PARTITION_1), 100).onComplete();
            assertTrue(requestManager.poll(100).unsentRequests.isEmpty());
        }

        @Test
        void waitsForExistingValidationWithoutDuplicateRpc() {
            realValidationState();
            CompletableFuture<Void> first = requestManager.updateFetchPositionsAndAwaitValidation(1000, () -> true);
            CompletableFuture<Void> second = requestManager.updateFetchPositionsAndAwaitValidation(2000, () -> true);
            NetworkClientDelegate.PollResult poll = requestManager.poll(0);
            assertEquals(1, poll.unsentRequests.size());
            assertFalse(first.isDone());
            assertFalse(second.isDone());
            buildOffsetsForLeaderEpochResponse(poll.unsentRequests.get(0), List.of(TEST_PARTITION_1), 100).onComplete();
            assertTrue(requestManager.poll(0).unsentRequests.isEmpty());
            assertTrue(first.isDone());
            assertTrue(second.isDone());
        }

        @Test
        void closeFailsPendingWaitAndLateResponseCannotReopenIt() {
            realValidationState();
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(1000, () -> true);
            NetworkClientDelegate.UnsentRequest request = requestManager.poll(0).unsentRequests.get(0);
            requestManager.closePendingPositionResets();
            assertFutureThrows(org.apache.kafka.common.KafkaException.class, result);
            buildOffsetsForLeaderEpochResponse(request, List.of(TEST_PARTITION_1), 100).onComplete();
            assertTrue(requestManager.poll(0).unsentRequests.isEmpty());
        }

        @Test
        void validSeekDoesNotWaitForStaleValidationRpc() {
            realValidationState();
            requestManager.updateFetchPositionsAndAwaitValidation(1000, () -> true);
            NetworkClientDelegate.UnsentRequest oldRequest = requestManager.poll(0).unsentRequests.get(0);
            subscriptionState.seek(TEST_PARTITION_1, 200);
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(1000, () -> true);
            assertTrue(result.isDone());
            buildOffsetsForLeaderEpochResponse(oldRequest, List.of(TEST_PARTITION_1), 100).onComplete();
            assertEquals(200, subscriptionState.position(TEST_PARTITION_1).offset);
        }

        @Test
        void closedOrTerminatedWaitCannotStartAnotherStage() {
            realValidationState();
            boolean[] active = {true};
            CompletableFuture<Void> result = requestManager.updateFetchPositionsAndAwaitValidation(1000, () -> active[0]);
            NetworkClientDelegate.UnsentRequest request = requestManager.poll(0).unsentRequests.get(0);
            active[0] = false;
            requestManager.poll(0);
            assertTrue(result.isDone());
            buildOffsetsForLeaderEpochResponse(request, List.of(TEST_PARTITION_1), 100).onComplete();
            assertTrue(requestManager.poll(0).unsentRequests.isEmpty());
            requestManager.closePendingPositionResets();
            assertTrue(requestManager.updateFetchPositionsAndAwaitValidation(1000, () -> true).isCompletedExceptionally());
        }
    }

    @Test
    public void testValidatePositionsSuccess() {
        int currentOffset = 5;
        int expectedEndOffset = 100;
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(Optional.of(LEADER_1),
                Optional.of(3));
        TopicPartition tp = TEST_PARTITION_1;
        SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(currentOffset,
                Optional.of(10), leaderAndEpoch);

        mockSuccessfulBuildRequestForValidatingPositions(position, LEADER_1);

        requestManager.validatePositionsIfNeeded();
        assertEquals(1, requestManager.requestsToSend(), "Invalid request count");

        verify(subscriptionState).setNextAllowedRetry(any(), anyLong());

        // Validate positions response with end offsets
        when(metadata.currentLeader(tp)).thenReturn(testLeaderEpoch(LEADER_1, leaderAndEpoch.epoch));
        NetworkClientDelegate.PollResult pollResult = requestManager.poll(time.milliseconds());
        NetworkClientDelegate.UnsentRequest unsentRequest = pollResult.unsentRequests.get(0);
        ClientResponse clientResponse = buildOffsetsForLeaderEpochResponse(unsentRequest,
                Collections.singletonList(tp), expectedEndOffset);
        clientResponse.onComplete();
        assertTrue(unsentRequest.future().isDone());
        assertFalse(unsentRequest.future().isCompletedExceptionally());
        verify(subscriptionState).maybeCompleteValidation(any(), any(), any());
    }

    @Test
    public void testValidatePositionsMissingLeader() {
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(Optional.of(Node.noNode()),
                Optional.of(5));
        SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(5L,
                Optional.of(10), leaderAndEpoch);
        when(subscriptionState.partitionsNeedingValidation(time.milliseconds())).thenReturn(Map.of(TEST_PARTITION_1, position));
        when(subscriptionState.position(any())).thenReturn(position, position);
        NodeApiVersions nodeApiVersions = NodeApiVersions.create();
        when(apiVersions.get(LEADER_1.idString())).thenReturn(nodeApiVersions);
        requestManager.validatePositionsIfNeeded();
        verify(metadata).requestUpdate(true);
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testValidatePositionsFailureWithUnrecoverableAuthException() {
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(Optional.of(LEADER_1),
                Optional.of(5));
        SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(5L,
                Optional.of(10), leaderAndEpoch);
        mockSuccessfulBuildRequestForValidatingPositions(position, LEADER_1);

        requestManager.validatePositionsIfNeeded();

        // Validate positions response with TopicAuthorizationException
        NetworkClientDelegate.PollResult res = requestManager.poll(time.milliseconds());
        NetworkClientDelegate.UnsentRequest unsentRequest = res.unsentRequests.get(0);
        ClientResponse clientResponse =
                buildOffsetsForLeaderEpochResponseWithErrors(unsentRequest, Collections.singletonMap(TEST_PARTITION_1, Errors.TOPIC_AUTHORIZATION_FAILED));
        clientResponse.onComplete();

        assertTrue(unsentRequest.future().isDone());
        assertFalse(unsentRequest.future().isCompletedExceptionally());

        // Following validatePositions should raise the previous exception without performing any
        // request
        assertThrows(TopicAuthorizationException.class, () -> requestManager.validatePositionsIfNeeded());
        assertEquals(0, requestManager.requestsToSend());
    }

    @Test
    public void testValidatePositionsAbortIfNoApiVersionsToCheckAgainstThenRecovers() {
        int currentOffset = 5;
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(Optional.of(LEADER_1),
                Optional.of(3));
        SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(currentOffset,
                Optional.of(10), leaderAndEpoch);

        when(subscriptionState.partitionsNeedingValidation(time.milliseconds())).thenReturn(Map.of(TEST_PARTITION_1, position));
        when(subscriptionState.position(any())).thenReturn(position, position);

        // No api version info initially available
        when(apiVersions.get(LEADER_1.idString())).thenReturn(null);
        requestManager.validatePositionsIfNeeded();
        assertEquals(0, requestManager.requestsToSend(), "Invalid request count");
        verify(subscriptionState, never()).completeValidation(TEST_PARTITION_1);
        verify(subscriptionState, never()).setNextAllowedRetry(any(), anyLong());

        // Api version updated, next validate positions should successfully build the request
        when(apiVersions.get(LEADER_1.idString())).thenReturn(NodeApiVersions.create());
        when(subscriptionState.partitionsNeedingValidation(time.milliseconds())).thenReturn(Map.of(TEST_PARTITION_1, position));
        when(subscriptionState.position(any())).thenReturn(position, position);
        requestManager.validatePositionsIfNeeded();
        assertEquals(1, requestManager.requestsToSend(), "Invalid request count");
    }

    @Test
    public void testUpdatePositionsWithCommittedOffsets() {
        long internalFetchCommittedTimeout = time.milliseconds() + DEFAULT_API_TIMEOUT_MS;
        TopicPartition tp1 = new TopicPartition("topic1", 1);
        Set<TopicPartition> initPartitions1 = Collections.singleton(tp1);
        Metadata.LeaderAndEpoch leaderAndEpoch = testLeaderEpoch(LEADER_1, Optional.of(1));

        // tp1 assigned and requires a position
        mockAssignedPartitionsMissingPositions(initPartitions1, initPartitions1, leaderAndEpoch);

        // Call to updateFetchPositions. Should send an OffsetFetch request and use the response to set positions
        CompletableFuture<CommitRequestManager.OffsetFetchResult> fetchResult = new CompletableFuture<>();
        when(commitRequestManager.fetchOffsets(eq(initPartitions1), eq(internalFetchCommittedTimeout), any())).thenReturn(fetchResult);
        CompletableFuture<Void> updatePositions1 = requestManager.updateFetchPositions(time.milliseconds());
        assertFalse(updatePositions1.isDone(), "Update positions should wait for the OffsetFetch request");
        verify(commitRequestManager).fetchOffsets(eq(initPartitions1), eq(internalFetchCommittedTimeout), any());

        // Receive response with committed offsets. Should complete the updatePositions operation (the set
        // of initializing partitions hasn't changed)
        when(subscriptionState.initializingPartitions()).thenReturn(initPartitions1);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(10, Optional.of(1), "");
        fetchResult.complete(new CommitRequestManager.OffsetFetchResult(
            Collections.singletonMap(tp1, offsetAndMetadata), Collections.emptyMap()));

        assertTrue(updatePositions1.isDone(), "Update positions should complete after the OffsetFetch response");
        SubscriptionState.FetchPosition expectedPosition = new SubscriptionState.FetchPosition(
                offsetAndMetadata.offset(), offsetAndMetadata.leaderEpoch(), leaderAndEpoch);
        verify(subscriptionState).seekUnvalidated(tp1, expectedPosition);
    }

    @Test
    public void testUpdatePositionsWithCommittedOffsetsReusesRequest() {
        long internalFetchCommittedTimeout = time.milliseconds() + DEFAULT_API_TIMEOUT_MS;
        TopicPartition tp1 = new TopicPartition("topic1", 1);
        Set<TopicPartition> initPartitions1 = Collections.singleton(tp1);
        Metadata.LeaderAndEpoch leaderAndEpoch = testLeaderEpoch(LEADER_1, Optional.of(1));

        // tp1 assigned and requires a position
        mockAssignedPartitionsMissingPositions(initPartitions1, initPartitions1, leaderAndEpoch);

        // call to updateFetchPositions. Should send an OffsetFetch request
        CompletableFuture<CommitRequestManager.OffsetFetchResult> fetchResult = new CompletableFuture<>();
        when(commitRequestManager.fetchOffsets(eq(initPartitions1), eq(internalFetchCommittedTimeout), any())).thenReturn(fetchResult);
        CompletableFuture<Void> updatePositions1 = requestManager.updateFetchPositions(time.milliseconds());
        assertFalse(updatePositions1.isDone(), "Update positions should wait for the OffsetFetch request");
        verify(commitRequestManager).fetchOffsets(eq(initPartitions1), eq(internalFetchCommittedTimeout), any());
        clearInvocations(commitRequestManager);

        // Call to updateFetchPositions again with the same set of initializing partitions should reuse request
        CompletableFuture<Void> updatePositions2 = requestManager.updateFetchPositions(time.milliseconds());
        verify(commitRequestManager, never()).fetchOffsets(eq(initPartitions1), eq(internalFetchCommittedTimeout), any());

        // Receive response with committed offsets, should complete both calls
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(10, Optional.of(1), "");
        fetchResult.complete(new CommitRequestManager.OffsetFetchResult(
            Collections.singletonMap(tp1, offsetAndMetadata), Collections.emptyMap()));

        assertTrue(updatePositions1.isDone());
        assertTrue(updatePositions2.isDone());
        SubscriptionState.FetchPosition expectedPosition = new SubscriptionState.FetchPosition(
                offsetAndMetadata.offset(), offsetAndMetadata.leaderEpoch(), leaderAndEpoch);
        verify(subscriptionState).seekUnvalidated(tp1, expectedPosition);
    }

    @Test
    public void testUpdatePositionsDoesNotApplyOffsetsIfPartitionNotInitializingAnymore() {
        long internalFetchCommittedTimeout = time.milliseconds() + DEFAULT_API_TIMEOUT_MS;
        TopicPartition tp1 = new TopicPartition("topic1", 1);
        Set<TopicPartition> initPartitions1 = Collections.singleton(tp1);
        Metadata.LeaderAndEpoch leaderAndEpoch = testLeaderEpoch(LEADER_1, Optional.of(1));

        // tp1 assigned and requires a position
        mockAssignedPartitionsMissingPositions(initPartitions1, initPartitions1, leaderAndEpoch);

        // call to updateFetchPositions will trigger an OffsetFetch request for tp1 (won't complete just yet)
        CompletableFuture<CommitRequestManager.OffsetFetchResult> fetchResult = new CompletableFuture<>();
        when(commitRequestManager.fetchOffsets(eq(initPartitions1), eq(internalFetchCommittedTimeout), any())).thenReturn(fetchResult);
        CompletableFuture<Void> updatePositions1 = requestManager.updateFetchPositions(time.milliseconds());
        assertFalse(updatePositions1.isDone());
        verify(commitRequestManager).fetchOffsets(eq(initPartitions1), eq(internalFetchCommittedTimeout), any());
        clearInvocations(commitRequestManager);

        // tp1 does not require a position anymore (ex. removed from the assignment, or got a position manually via
        // seek). When the OffsetFetch response is received, it should not update the position for tp1 to the
        // committed offset
        when(subscriptionState.initializingPartitions()).thenReturn(Collections.emptySet());
        fetchResult.complete(new CommitRequestManager.OffsetFetchResult(
            Collections.singletonMap(tp1, new OffsetAndMetadata(5)), Collections.emptyMap()));
        verify(subscriptionState, never()).seekUnvalidated(any(), any());
    }

    // This test ensures that we don't reset positions to the partition offsets for a partition assigned while the
    // updateFetchPositions is running (after the OffsetFetch request has been sent).
    @Test
    public void testUpdatePositionsDoesNotResetPositionBeforeRetrievingOffsetsForNewlyAddedPartition() {
        long internalFetchCommittedTimeout = time.milliseconds() + DEFAULT_API_TIMEOUT_MS;
        TopicPartition tp1 = new TopicPartition("topic1", 1);
        Set<TopicPartition> initPartitions1 = Collections.singleton(tp1);
        Metadata.LeaderAndEpoch leaderAndEpoch = testLeaderEpoch(LEADER_1, Optional.of(1));

        // tp1 assigned and requires a position
        mockAssignedPartitionsMissingPositions(initPartitions1, initPartitions1, leaderAndEpoch);

        // call to updateFetchPositions will trigger an OffsetFetch request for tp1 (won't complete just yet)
        CompletableFuture<CommitRequestManager.OffsetFetchResult> fetchResult = new CompletableFuture<>();
        when(commitRequestManager.fetchOffsets(eq(initPartitions1), eq(internalFetchCommittedTimeout), any())).thenReturn(fetchResult);
        CompletableFuture<Void> updatePositions1 = requestManager.updateFetchPositions(time.milliseconds());
        assertFalse(updatePositions1.isDone());
        verify(commitRequestManager).fetchOffsets(eq(initPartitions1), eq(internalFetchCommittedTimeout), any());
        clearInvocations(commitRequestManager);

        // tp2 added to the assignment when the Offset Fetch request is already sent including tp1 only
        TopicPartition tp2 = new TopicPartition("topic2", 2);
        Set<TopicPartition> initPartitions2 = Set.of(tp1, tp2);
        mockAssignedPartitionsMissingPositions(initPartitions2, initPartitions2, leaderAndEpoch);

        // tp2 requires a position, but shouldn't be reset after receiving the offset fetch response that will only
        // include the requested partition tp1
        when(subscriptionState.initializingPartitions()).thenReturn(initPartitions2);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(10, Optional.empty(), "");
        fetchResult.complete(new CommitRequestManager.OffsetFetchResult(
            Collections.singletonMap(tp1, offsetAndMetadata), Collections.emptyMap()));

        // Position should have been updated for tp1 using the committed offset
        SubscriptionState.FetchPosition expectedPosition = new SubscriptionState.FetchPosition(
            offsetAndMetadata.offset(), offsetAndMetadata.leaderEpoch(), leaderAndEpoch);
        verify(subscriptionState).seekUnvalidated(tp1, expectedPosition);

        // Reset positions shouldn't include tp2
        verify(subscriptionState).resetInitializingPositions(argThat(p -> !p.test(tp2)));
    }

    @Test
    public void testRemoteListOffsetsRequestTimeoutMs() {
        int requestTimeoutMs = 100;
        int defaultApiTimeoutMs = 500;
        // Overriding the requestManager to provide different request and default API timeout
        requestManager = new OffsetsRequestManager(
                subscriptionState,
                metadata,
                DEFAULT_ISOLATION_LEVEL,
                time,
                RETRY_BACKOFF_MS,
                requestTimeoutMs,
                defaultApiTimeoutMs,
                apiVersions,
                mock(NetworkClientDelegate.class),
                commitRequestManager,
                new PositionsValidator(new LogContext(), time, subscriptionState, metadata),
                new LogContext()
        );

        Map<TopicPartition, Long> timestampsToSearch = Collections.singletonMap(TEST_PARTITION_1,
                ListOffsetsRequest.EARLIEST_TIMESTAMP);
        mockSuccessfulRequest(Collections.singletonMap(TEST_PARTITION_1, LEADER_1));
        requestManager.fetchOffsets(timestampsToSearch, false);
        assertEquals(1, requestManager.requestsToSend());
        NetworkClientDelegate.PollResult retriedPoll = requestManager.poll(time.milliseconds());
        NetworkClientDelegate.UnsentRequest unsentRequest = retriedPoll.unsentRequests.get(0);
        AbstractRequest abstractRequest = unsentRequest.requestBuilder().build();
        assertInstanceOf(ListOffsetsRequest.class, abstractRequest);
        ListOffsetsRequest offsetFetchRequest = (ListOffsetsRequest) abstractRequest;
        assertEquals(requestTimeoutMs, offsetFetchRequest.timeoutMs());
    }

    private void mockAssignedPartitionsMissingPositions(Set<TopicPartition> assignedPartitions,
                                                        Set<TopicPartition> initializingPartitions,
                                                        Metadata.LeaderAndEpoch leaderAndEpoch) {
        when(subscriptionState.partitionsNeedingValidation(anyLong())).thenReturn(Map.of());
        assignedPartitions.forEach(tp -> {
            when(subscriptionState.isAssigned(tp)).thenReturn(true);
            when(metadata.currentLeader(tp)).thenReturn(leaderAndEpoch);
        });

        when(subscriptionState.hasAllFetchPositions()).thenReturn(false);
        when(subscriptionState.initializingPartitions()).thenReturn(initializingPartitions);
    }

    private void mockSuccessfulBuildRequestForValidatingPositions(SubscriptionState.FetchPosition position, Node leader) {
        when(subscriptionState.partitionsNeedingValidation(time.milliseconds())).thenReturn(Map.of(TEST_PARTITION_1, position));
        when(subscriptionState.positionOrNull(any())).thenReturn(position, position);
        NodeApiVersions nodeApiVersions = NodeApiVersions.create();
        when(apiVersions.get(leader.idString())).thenReturn(nodeApiVersions);
    }

    private void testResetPositionsSuccessWithLeaderEpoch(Metadata.LeaderAndEpoch leaderAndEpoch) {
        TopicPartition tp = TEST_PARTITION_1;
        Node leader = LEADER_1;
        AutoOffsetResetStrategy strategy = AutoOffsetResetStrategy.EARLIEST;
        long offset = 5L;
        when(subscriptionState.partitionsNeedingReset(time.milliseconds())).thenReturn(Collections.singleton(tp));
        when(subscriptionState.resetStrategy(any())).thenReturn(strategy);
        mockSuccessfulRequest(Collections.singletonMap(tp, leader));

        requestManager.resetPositionsIfNeeded();
        assertEquals(1, requestManager.requestsToSend());

        // Reset positions response with offsets
        when(metadata.currentLeader(tp)).thenReturn(testLeaderEpoch(leader, leaderAndEpoch.epoch));
        NetworkClientDelegate.PollResult pollResult = requestManager.poll(time.milliseconds());
        NetworkClientDelegate.UnsentRequest unsentRequest = pollResult.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponse(unsentRequest, Collections.singletonMap(tp,
                new OffsetAndTimestampInternal(offset, 1L, leaderAndEpoch.epoch)));
        clientResponse.onComplete();
        assertTrue(unsentRequest.future().isDone());
        assertFalse(unsentRequest.future().isCompletedExceptionally());
    }

    private ListOffsetsResponseData.ListOffsetsTopicResponse mockUnknownOffsetResponse(
            TopicPartition tp) {
        return new ListOffsetsResponseData.ListOffsetsTopicResponse()
                .setName(tp.topic())
                .setPartitions(Collections.singletonList(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                        .setPartitionIndex(tp.partition())
                        .setErrorCode(Errors.NONE.code())
                        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)));
    }

    private static Stream<Arguments> retriableErrors() {
        return Stream.of(
                Arguments.of(Errors.NOT_LEADER_OR_FOLLOWER),
                Arguments.of(Errors.REPLICA_NOT_AVAILABLE),
                Arguments.of(Errors.KAFKA_STORAGE_ERROR),
                Arguments.of(Errors.OFFSET_NOT_AVAILABLE),
                Arguments.of(Errors.LEADER_NOT_AVAILABLE),
                Arguments.of(Errors.FENCED_LEADER_EPOCH),
                Arguments.of(Errors.BROKER_NOT_AVAILABLE),
                Arguments.of(Errors.INVALID_REQUEST),
                Arguments.of(Errors.UNKNOWN_LEADER_EPOCH),
                Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION));
    }

    private void verifySuccessfulPollAndResponseReceived(
            CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> actualResult,
            Map<TopicPartition, OffsetAndTimestampInternal> expectedResult) throws ExecutionException,
            InterruptedException {
        // Following poll should send the request and get a response
        NetworkClientDelegate.PollResult retriedPoll = requestManager.poll(time.milliseconds());
        verifySuccessfulPollAwaitingResponse(retriedPoll);
        NetworkClientDelegate.UnsentRequest unsentRequest = retriedPoll.unsentRequests.get(0);
        ClientResponse clientResponse = buildClientResponse(unsentRequest, expectedResult);
        clientResponse.onComplete();
        verifyRequestSuccessfullyCompleted(actualResult, expectedResult);
    }


    private void verifyRequestCompletedWithErrorResponse(CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> actualResult,
                                                         Class<? extends Throwable> expectedFailure) {
        assertTrue(actualResult.isDone());
        assertTrue(actualResult.isCompletedExceptionally());
        Throwable failure = assertThrows(ExecutionException.class, actualResult::get);
        assertEquals(expectedFailure, failure.getCause().getClass());
    }

    private void mockSuccessfulRequest(Map<TopicPartition, Node> partitionLeaders) {
        partitionLeaders.forEach((tp, broker) -> {
            when(metadata.currentLeader(tp)).thenReturn(testLeaderEpoch(broker,
                    Metadata.LeaderAndEpoch.noLeaderOrEpoch().epoch));
            when(subscriptionState.isAssigned(tp)).thenReturn(true);
        });
        when(metadata.fetch()).thenReturn(testClusterMetadata(partitionLeaders));
    }

    private void mockFailedRequest_MissingLeader() {
        when(metadata.currentLeader(any(TopicPartition.class))).thenReturn(
                new Metadata.LeaderAndEpoch(Optional.empty(), Optional.of(1)));
        when(subscriptionState.isAssigned(any(TopicPartition.class))).thenReturn(true);
    }

    private void verifySuccessfulPollAwaitingResponse(NetworkClientDelegate.PollResult pollResult) {
        verifySuccessfulPollAwaitingResponse(pollResult, 1);
    }

    private void verifySuccessfulPollAwaitingResponse(NetworkClientDelegate.PollResult pollResult,
                                                      int requestCount) {
        assertEquals(0, requestManager.requestsToSend());
        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(requestCount, pollResult.unsentRequests.size());
    }

    private void verifyRequestSuccessfullyCompleted(
            CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> actualResult,
            Map<TopicPartition, OffsetAndTimestampInternal> expectedResult) throws ExecutionException, InterruptedException {
        assertEquals(0, requestManager.requestsToRetry());
        assertEquals(0, requestManager.requestsToSend());

        assertTrue(actualResult.isDone());
        assertFalse(actualResult.isCompletedExceptionally());
        Map<TopicPartition, OffsetAndTimestampInternal> partitionOffsets = actualResult.get();
        assertEquals(expectedResult, partitionOffsets);

        // Validate that the subscription state has been updated for all non-null offsets retrieved
        Map<TopicPartition, Long> validExpectedOffsets = expectedResult.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().offset()));
        verifySubscriptionStateUpdated(validExpectedOffsets);
    }

    private void verifySubscriptionStateUpdated(Map<TopicPartition, Long> expectedResult) {
        ArgumentCaptor<TopicPartition> tpCaptor = ArgumentCaptor.forClass(TopicPartition.class);
        ArgumentCaptor<Long> offsetCaptor = ArgumentCaptor.forClass(Long.class);

        verify(subscriptionState, times(expectedResult.size())).updateLastStableOffset(tpCaptor.capture(),
                offsetCaptor.capture());

        List<TopicPartition> updatedTp = tpCaptor.getAllValues();
        List<Long> updatedOffsets = offsetCaptor.getAllValues();
        assertEquals(expectedResult.keySet().size(), updatedOffsets.size());
        assertEquals(expectedResult.keySet(), new HashSet<>(updatedTp));

        assertEquals(expectedResult.values().size(), updatedOffsets.size());
        expectedResult.values().stream()
                .map(updatedOffsets::contains)
                .forEach(Assertions::assertTrue);
    }

    private Metadata.LeaderAndEpoch testLeaderEpoch(Node leader, Optional<Integer> epoch) {
        return new Metadata.LeaderAndEpoch(Optional.of(leader), epoch);
    }

    private Cluster testClusterMetadata(Map<TopicPartition, Node> partitionLeaders) {
        List<PartitionInfo> partitions =
                partitionLeaders.keySet().stream()
                        .map(tp -> new PartitionInfo(tp.topic(), tp.partition(),
                                partitionLeaders.get(tp), null, null))
                        .collect(Collectors.toList());

        return new Cluster("clusterId", partitionLeaders.values(), partitions,
                Collections.emptySet(),
                Collections.emptySet());
    }

    private ClientResponse buildClientResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final Map<TopicPartition, OffsetAndTimestampInternal> partitionsOffsets) {
        List<ListOffsetsResponseData.ListOffsetsTopicResponse> topicResponses = new
                ArrayList<>();
        partitionsOffsets.forEach((tp, offsetAndTimestamp) -> {
            ListOffsetsResponseData.ListOffsetsTopicResponse topicResponse = ListOffsetsResponse.singletonListOffsetsTopicResponse(
                    tp, Errors.NONE,
                    offsetAndTimestamp.timestamp(),
                    offsetAndTimestamp.offset(),
                    offsetAndTimestamp.leaderEpoch().orElse(ListOffsetsResponse.UNKNOWN_EPOCH));
            topicResponses.add(topicResponse);
        });

        return buildClientResponse(request, topicResponses, false, null);
    }

    private ClientResponse buildOffsetsForLeaderEpochResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final List<TopicPartition> partitions,
            final int endOffset) {

        AbstractRequest abstractRequest = request.requestBuilder().build();
        assertInstanceOf(OffsetsForLeaderEpochRequest.class, abstractRequest);
        OffsetsForLeaderEpochRequest offsetsForLeaderEpochRequest = (OffsetsForLeaderEpochRequest) abstractRequest;
        OffsetForLeaderEpochResponseData data = new OffsetForLeaderEpochResponseData();
        partitions.forEach(tp -> {
            OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult topic = data.topics().find(tp.topic());
            if (topic == null) {
                topic = new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult().setTopic(tp.topic());
                data.topics().add(topic);
            }
            topic.partitions().add(new OffsetForLeaderEpochResponseData.EpochEndOffset()
                    .setPartition(tp.partition())
                    .setErrorCode(Errors.NONE.code())
                    .setLeaderEpoch(3)
                    .setEndOffset(endOffset));
        });

        OffsetsForLeaderEpochResponse response = new OffsetsForLeaderEpochResponse(data);
        return new ClientResponse(
                new RequestHeader(ApiKeys.OFFSET_FOR_LEADER_EPOCH, offsetsForLeaderEpochRequest.version(), "", 1),
                request.handler(),
                "-1",
                time.milliseconds(),
                time.milliseconds(),
                false,
                null,
                null,
                response
        );
    }

    private ClientResponse buildOffsetsForLeaderEpochResponseWithErrors(
            final NetworkClientDelegate.UnsentRequest request,
            final Map<TopicPartition, Errors> partitionErrors) {

        AbstractRequest abstractRequest = request.requestBuilder().build();
        assertInstanceOf(OffsetsForLeaderEpochRequest.class, abstractRequest);
        OffsetsForLeaderEpochRequest offsetsForLeaderEpochRequest = (OffsetsForLeaderEpochRequest) abstractRequest;
        OffsetForLeaderEpochResponseData data = new OffsetForLeaderEpochResponseData();
        partitionErrors.keySet().forEach(tp -> {
            OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult topic = data.topics().find(tp.topic());
            if (topic == null) {
                topic = new OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult().setTopic(tp.topic());
                data.topics().add(topic);
            }
            topic.partitions().add(new OffsetForLeaderEpochResponseData.EpochEndOffset()
                    .setPartition(tp.partition())
                    .setErrorCode(partitionErrors.get(tp).code()));
        });

        OffsetsForLeaderEpochResponse response = new OffsetsForLeaderEpochResponse(data);
        return new ClientResponse(
                new RequestHeader(ApiKeys.OFFSET_FOR_LEADER_EPOCH, offsetsForLeaderEpochRequest.version(), "", 1),
                request.handler(),
                "-1",
                time.milliseconds(),
                time.milliseconds(),
                false,
                null,
                null,
                response
        );
    }

    private ClientResponse buildClientResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final List<ListOffsetsResponseData.ListOffsetsTopicResponse> topicResponses) {
        return buildClientResponse(request, topicResponses, false, null);
    }

    private ClientResponse buildClientResponseWithErrors(
            final NetworkClientDelegate.UnsentRequest request,
            final Map<TopicPartition, Errors> partitionErrors) {
        List<ListOffsetsResponseData.ListOffsetsTopicResponse> topicResponses = new ArrayList<>();
        partitionErrors.forEach((tp, error) -> topicResponses.add(ListOffsetsResponse.singletonListOffsetsTopicResponse(
                tp,
                error,
                ListOffsetsResponse.UNKNOWN_TIMESTAMP,
                ListOffsetsResponse.UNKNOWN_OFFSET,
                ListOffsetsResponse.UNKNOWN_EPOCH)));

        return buildClientResponse(request, topicResponses, false, null);
    }

    private ClientResponse buildClientResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final List<ListOffsetsResponseData.ListOffsetsTopicResponse> topicResponses,
            final boolean disconnected,
            final AuthenticationException authenticationException) {
        AbstractRequest abstractRequest = request.requestBuilder().build();
        assertInstanceOf(ListOffsetsRequest.class, abstractRequest);
        ListOffsetsRequest offsetFetchRequest = (ListOffsetsRequest) abstractRequest;
        ListOffsetsResponse response = new ListOffsetsResponse(new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(topicResponses));
        return new ClientResponse(
                new RequestHeader(ApiKeys.OFFSET_FETCH, offsetFetchRequest.version(), "", 1),
                request.handler(),
                "-1",
                time.milliseconds(),
                time.milliseconds(),
                disconnected,
                null,
                authenticationException,
                response
        );
    }
}
