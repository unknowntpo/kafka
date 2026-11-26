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
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * The async consumer fetches from one broker over several connections. These tests drive the rule that adds and
 * removes connections, and check where every partition is fetched.
 */
public class FetchRequestManagerConnectionsTest {

    private static final String TOPIC = "test";
    private static final int PARTITIONS = 4;
    private static final int FETCH_SIZE = 1000;
    // A record this large makes a response "full": at least half of what the broker may send for the partition.
    private static final int FULL_VALUE_SIZE = 600;
    // Every cycle is longer than the time over which one vote is cast
    private static final long CYCLE_MS = 20;

    private final Uuid topicId = Uuid.randomUuid();
    private final Map<Uuid, String> topicNames = Map.of(topicId, TOPIC);
    // Not at time 0: MockClient takes a connection as backing off until a time after 0.
    private final MockTime time = new MockTime(0, 1_000, 1_000_000_000L);
    private final LogContext logContext = new LogContext();
    private final Map<String, Integer> sessionIds = new HashMap<>();
    // The partitions of the fetch session of every connection, as the broker sees them
    private final Map<String, Set<TopicPartition>> sessionPartitions = new HashMap<>();
    private final List<TopicPartition> partitions = new ArrayList<>();

    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private MockClient client;
    private Metrics metrics;
    private NetworkClientDelegate networkClientDelegate;
    private FetchBuffer fetchBuffer;
    private FetchCollector<byte[], byte[]> collector;
    private FetchRequestManager fetcher;
    private Node broker;
    private int maxConnections = 4;
    private int nextSessionId = 100;

    private void setup() {
        subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        metadata = new ConsumerMetadata(0, 0, Long.MAX_VALUE, false, false, subscriptions, logContext,
                new ClusterResourceListeners());
        client = new MockClient(time, metadata);
        metrics = new Metrics(time);
        FetchMetricsManager metricsManager = new FetchMetricsManager(metrics, new FetchMetricsRegistry("consumer"));

        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        ConsumerConfig config = new ConsumerConfig(properties);
        networkClientDelegate = new NetworkClientDelegate(time, config, logContext, client, metadata,
                mock(BackgroundEventHandler.class), true, mock(AsyncConsumerMetrics.class));

        FetchConfig fetchConfig = new FetchConfig(1, Integer.MAX_VALUE, 0, FETCH_SIZE, Integer.MAX_VALUE, true,
                CommonClientConfigs.DEFAULT_CLIENT_RACK, IsolationLevel.READ_UNCOMMITTED);
        fetchBuffer = new FetchBuffer(logContext, time);
        fetcher = new FetchRequestManager(logContext, time, metadata, subscriptions, fetchConfig, fetchBuffer,
                metricsManager, networkClientDelegate, new ApiVersions(), 100) {
            @Override
            protected int maxConnectionsPerBroker() {
                return maxConnections;
            }
        };
        collector = new FetchCollector<>(logContext, metadata, subscriptions, fetchConfig,
                new Deserializers<>(new ByteArrayDeserializer(), new ByteArrayDeserializer(), metrics),
                metricsManager, time);

        Set<TopicPartition> assigned = new LinkedHashSet<>();
        for (int i = 0; i < PARTITIONS; i++)
            assigned.add(new TopicPartition(TOPIC, i));
        partitions.addAll(assigned);
        subscriptions.assignFromUser(assigned);
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, Map.of(TOPIC, PARTITIONS), Map.of(TOPIC, topicId)));
        for (TopicPartition tp : partitions)
            subscriptions.seekValidated(tp, new SubscriptionState.FetchPosition(0, Optional.empty(), metadata.currentLeader(tp)));
        broker = metadata.fetch().nodes().get(0);
    }

    @AfterEach
    public void teardown() {
        if (fetcher != null)
            fetcher.close();
        if (metrics != null)
            metrics.close();
    }

    @Test
    public void testConnectionsGrowWhileTheApplicationWaitsForFullResponses() {
        setup();
        // The first response starts the measurement, each later one votes once, and 8 votes add a connection.
        for (int i = 0; i < 8; i++)
            cycle(true, true);
        assertEquals(1, fetcher.connectionCount(broker.id()));
        cycle(true, true);
        assertEquals(2, fetcher.connectionCount(broker.id()));

        for (int i = 0; i < 40; i++)
            cycle(true, true);
        assertEquals(4, fetcher.connectionCount(broker.id()), "no more than the most connections per broker");

        // Every connection is fetched from at the same time, each with its own partitions and the broker's address.
        sendFetches();
        List<ClientRequest> inFlight = new ArrayList<>(client.requests());
        assertEquals(4, inFlight.size());
        Set<String> destinations = inFlight.stream().map(ClientRequest::destination).collect(Collectors.toSet());
        assertEquals(4, destinations.size());
        assertTrue(destinations.contains(broker.idString()));
        for (ClientRequest request : inFlight) {
            int id = Integer.parseInt(request.destination());
            assertEquals(broker.id(), AbstractFetch.brokerIdOf(id));
            assertEquals(1, requestedPartitions(request).size());
        }
    }

    @Test
    public void testNoMoreConnectionsWhenTheApplicationDoesNotWait() {
        setup();
        for (int i = 0; i < 40; i++)
            cycle(true, false);
        assertEquals(1, fetcher.connectionCount(broker.id()));
    }

    @Test
    public void testOneConnectionWhenTheMaximumIsOne() {
        maxConnections = 1;
        setup();
        for (int i = 0; i < 40; i++)
            cycle(true, true);
        assertEquals(1, fetcher.connectionCount(broker.id()));
        sendFetches();
        assertEquals(List.of(broker.idString()), destinations());
    }

    @Test
    public void testNoMoreConnectionsWhileTheBrokerThrottles() {
        setup();
        for (int i = 0; i < 40; i++) {
            consumeAll();
            fetchBuffer.awaitWakeup(time.timer(0));
            time.sleep(CYCLE_MS);
            sendFetches();
            for (ClientRequest request : new ArrayList<>(client.requests()))
                respond(request, true, 10);
            networkClientDelegate.poll(0, time.milliseconds());
        }
        assertEquals(1, fetcher.connectionCount(broker.id()));
    }

    @Test
    public void testConnectionsShrinkWhenResponsesAreNotFull() {
        setup();
        growToTwoConnections();

        // Every response that is not full votes for one less connection; 64 such votes remove one.
        for (int i = 0; i < 63; i++)
            cycle(false, true);
        assertEquals(2, fetcher.connectionCount(broker.id()));
        cycle(false, true);
        assertEquals(1, fetcher.connectionCount(broker.id()));

        // All partitions are back on the broker's first connection.
        consumeAll();
        sendFetches();
        assertEquals(List.of(broker.idString()), destinations());
        assertEquals(new HashSet<>(partitions), sessionPartitions.get(broker.idString()));
    }

    @Test
    public void testPartitionMovesToANewConnectionAndItsOldSessionForgetsIt() {
        setup();
        growToTwoConnections();
        String extra = String.valueOf(AbstractFetch.connectionId(broker.id(), 1));

        // The partitions with an odd number moved: the first connection's session forgets them, the new
        // connection asks for them from the next offset.
        Map<String, FetchRequest> requests = lastRequests;
        FetchRequest first = requests.get(broker.idString());
        assertFalse(first.metadata().isFull(), "the first connection keeps its fetch session");
        assertEquals(Set.of(partitions.get(1), partitions.get(3)), forgotten(first));
        assertEquals(Set.of(partitions.get(0), partitions.get(2)), sessionPartitions.get(broker.idString()));

        FetchRequest moved = requests.get(extra);
        assertTrue(moved.metadata().isFull(), "a new connection starts a fetch session of its own");
        Map<TopicPartition, Long> offsets = fetchOffsets(moved);
        assertEquals(Set.of(partitions.get(1), partitions.get(3)), offsets.keySet());
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet())
            assertEquals(subscriptions.position(entry.getKey()).offset, entry.getValue());
    }

    @Test
    public void testPartitionDoesNotMoveWhileItsConnectionHasARequestInFlight() {
        setup();
        growToTwoConnections();
        String extra = String.valueOf(AbstractFetch.connectionId(broker.id(), 1));

        // The extra connection keeps its request in flight while the responses of the first connection vote
        // the extra connection away.
        consumeAll();
        sendFetches();
        ClientRequest stuck = requestTo(extra);
        for (int i = 0; i < 64; i++) {
            consumeAll();
            time.sleep(CYCLE_MS);
            sendFetches();
            assertNoPartitionInTwoRequestsInFlight();
            respond(requestTo(broker.idString()), false, 0);
            networkClientDelegate.poll(0, time.milliseconds());
        }
        assertEquals(1, fetcher.connectionCount(broker.id()));

        // The partitions of the extra connection want to move back but must wait for its response.
        consumeAll();
        sendFetches();
        assertNoPartitionInTwoRequestsInFlight();
        assertEquals(Set.of(partitions.get(0), partitions.get(2)), sessionPartitions.get(broker.idString()));
        respond(requestTo(broker.idString()), false, 0);
        respond(stuck, false, 0);
        networkClientDelegate.poll(0, time.milliseconds());
        consumeAll();

        // Now they move, and continue from the position that the consumed data left.
        sendFetches();
        assertEquals(List.of(broker.idString()), destinations());
        Map<TopicPartition, Long> offsets = fetchOffsets(lastRequests.get(broker.idString()));
        assertTrue(offsets.keySet().containsAll(Set.of(partitions.get(1), partitions.get(3))));
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet())
            assertEquals(subscriptions.position(entry.getKey()).offset, entry.getValue());
        assertEquals(new HashSet<>(partitions), sessionPartitions.get(broker.idString()));
    }

    @Test
    public void testMovedPartitionWithDataThatCannotBePrefetchedIsNotFetched() {
        setup();
        // The ninth response adds a second connection; partitions 1 and 3 move to it with the next requests.
        for (int i = 0; i < 9; i++)
            cycle(true, true);
        assertEquals(2, fetcher.connectionCount(broker.id()));

        // Partition 1 still has buffered data, and that data does not continue from the consumed position.
        consumeAllBut(partitions.get(1), Set.of(partitions.get(0), partitions.get(2), partitions.get(3)));
        fetchBuffer.invalidatePrefetch(partitions.get(1));

        // Partition 1 moves with its buffered data, so its new connection gets no request; the first one does.
        sendFetches();
        assertEquals(List.of(broker.idString()), destinations());
        assertEquals(Set.of(partitions.get(0), partitions.get(2)), requestedPartitions(lastRequests.get(broker.idString())));
    }

    @Test
    public void testDisconnectOfAnExtraConnection() {
        setup();
        growToTwoConnections();
        String extraId = String.valueOf(AbstractFetch.connectionId(broker.id(), 1));
        Node extra = new Node(Integer.parseInt(extraId), broker.host(), broker.port());
        // Both connections have a fetch session and a request of it in flight.
        respondToAll(false);
        networkClientDelegate.poll(0, time.milliseconds());
        consumeAll();
        sendFetches();
        assertEquals(2, client.requests().size());
        assertFalse(lastRequests.get(extraId).metadata().isFull());

        // The extra connection breaks and backs off before it may connect again.
        client.disconnect(extraId);
        client.backoff(extra, 500);
        respond(requestTo(broker.idString()), false, 0);
        networkClientDelegate.poll(0, time.milliseconds());
        consumeAll();

        // The first connection goes on fetching its partitions; the others wait.
        sendFetches();
        assertEquals(List.of(broker.idString()), destinations());
        assertEquals(Set.of(partitions.get(0), partitions.get(2)), requestedPartitions(lastRequests.get(broker.idString())));
        respond(requestTo(broker.idString()), false, 0);
        networkClientDelegate.poll(0, time.milliseconds());

        // After the backoff the extra connection starts a new fetch session from the consumed positions.
        time.sleep(500);
        consumeAll();
        sendFetches();
        FetchRequest again = lastRequests.get(extraId);
        assertTrue(again.metadata().isFull());
        Map<TopicPartition, Long> offsets = fetchOffsets(again);
        assertEquals(Set.of(partitions.get(1), partitions.get(3)), offsets.keySet());
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet())
            assertEquals(subscriptions.position(entry.getKey()).offset, entry.getValue());
    }

    @Test
    public void testCloseSendsSessionCloseForEveryConnection() {
        setup();
        growToTwoConnections();
        respondToAll(true);
        networkClientDelegate.poll(0, time.milliseconds());
        int firstSession = sessionIds.get(broker.idString());
        String extraId = String.valueOf(AbstractFetch.connectionId(broker.id(), 1));
        int extraSession = sessionIds.get(extraId);
        assertNotEquals(firstSession, extraSession);

        NetworkClientDelegate.PollResult result = fetcher.pollOnClose(time.milliseconds());
        Map<String, FetchMetadata> closes = new HashMap<>();
        for (NetworkClientDelegate.UnsentRequest unsent : result.unsentRequests) {
            FetchRequest request = ((FetchRequest.Builder) unsent.requestBuilder()).build(ApiKeys.FETCH.latestVersion());
            closes.put(unsent.node().get().idString(), request.metadata());
        }
        assertEquals(Set.of(broker.idString(), extraId), closes.keySet());
        assertEquals(FetchMetadata.FINAL_EPOCH, closes.get(broker.idString()).epoch());
        assertEquals(firstSession, closes.get(broker.idString()).sessionId());
        assertEquals(FetchMetadata.FINAL_EPOCH, closes.get(extraId).epoch());
        assertEquals(extraSession, closes.get(extraId).sessionId());
    }

    /**
     * With one connection the requests are the same as without the code for several connections: the same
     * destination, and the same bytes as a request built directly from the partitions.
     */
    @Test
    public void testRequestsWithOneConnectionAreUnchanged() {
        setup();
        for (int round = 0; round < 3; round++) {
            consumeAll();
            sendFetches();
            List<ClientRequest> inFlight = new ArrayList<>(client.requests());
            assertEquals(1, inFlight.size());
            ClientRequest request = inFlight.get(0);
            assertEquals(broker.idString(), request.destination());

            FetchRequest.Builder builder = (FetchRequest.Builder) request.requestBuilder();
            FetchRequest actual = builder.build(ApiKeys.FETCH.latestVersion());
            Map<TopicPartition, FetchRequest.PartitionData> expectedData = new LinkedHashMap<>();
            for (TopicPartition tp : subscriptions.fetchablePartitions(tp -> true)) {
                SubscriptionState.FetchPosition position = subscriptions.position(tp);
                expectedData.put(tp, new FetchRequest.PartitionData(topicId, position.offset,
                        FetchRequest.INVALID_LOG_START_OFFSET, FETCH_SIZE, position.currentLeader.epoch, Optional.empty()));
            }
            FetchRequest expected = FetchRequest.Builder
                    .forConsumer(ApiKeys.FETCH.latestVersion(), 0, 1, expectedData)
                    .isolationLevel(IsolationLevel.READ_UNCOMMITTED)
                    .setMaxBytes(Integer.MAX_VALUE)
                    .metadata(actual.metadata())
                    .removed(List.of())
                    .replaced(List.of())
                    .rackId(CommonClientConfigs.DEFAULT_CLIENT_RACK)
                    .build(ApiKeys.FETCH.latestVersion());
            assertEquals(expected.data(), actual.data());
            assertEquals(serialize(expected), serialize(actual));
            assertEquals(round == 0 ? FetchMetadata.INITIAL_EPOCH : round, actual.metadata().epoch());
            respond(request, false, 0);
            networkClientDelegate.poll(0, time.milliseconds());
        }
        assertEquals(1, fetcher.connectionCount(broker.id()));
    }

    // ---- helpers

    private Map<String, FetchRequest> lastRequests = new HashMap<>();

    private void growToTwoConnections() {
        for (int i = 0; i < 9; i++)
            cycle(true, true);
        assertEquals(2, fetcher.connectionCount(broker.id()));
        consumeAll();
        sendFetches();
        assertEquals(2, client.requests().size());
        assertNoPartitionInTwoRequestsInFlight();
    }

    /** One round: the application takes all data, waits if asked to, and every connection gets a response. */
    private void cycle(boolean full, boolean applicationWaits) {
        consumeAll();
        if (applicationWaits)
            fetchBuffer.awaitWakeup(time.timer(0));
        time.sleep(CYCLE_MS);
        sendFetches();
        assertNoPartitionInTwoRequestsInFlight();
        respondToAll(full);
        networkClientDelegate.poll(0, time.milliseconds());
    }

    private void sendFetches() {
        fetcher.createFetchRequests();
        NetworkClientDelegate.PollResult result = fetcher.poll(time.milliseconds());
        networkClientDelegate.addAll(result.unsentRequests);
        networkClientDelegate.poll(0, time.milliseconds());
        lastRequests = new HashMap<>();
        for (NetworkClientDelegate.UnsentRequest unsent : result.unsentRequests) {
            String destination = unsent.node().get().idString();
            FetchRequest request = ((FetchRequest.Builder) unsent.requestBuilder()).build(ApiKeys.FETCH.latestVersion());
            lastRequests.put(destination, request);
            applyToSession(destination, request);
        }
    }

    private void applyToSession(String destination, FetchRequest request) {
        Set<TopicPartition> session = sessionPartitions.computeIfAbsent(destination, d -> new HashSet<>());
        if (request.metadata().isFull())
            session.clear();
        session.addAll(requestedPartitions(request));
        session.removeAll(forgotten(request));
    }

    private void respondToAll(boolean full) {
        for (ClientRequest request : new ArrayList<>(client.requests()))
            respond(request, full, 0);
    }

    private void respond(ClientRequest clientRequest, boolean full, int throttleTimeMs) {
        FetchRequest request = ((FetchRequest.Builder) clientRequest.requestBuilder()).build(ApiKeys.FETCH.latestVersion());
        String destination = clientRequest.destination();
        int sessionId;
        if (request.metadata().epoch() == FetchMetadata.INITIAL_EPOCH) {
            sessionId = nextSessionId++;
            sessionIds.put(destination, sessionId);
        } else {
            sessionId = sessionIds.get(destination);
        }
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> data = new LinkedHashMap<>();
        request.fetchData(topicNames).forEach((tp, partitionData) -> {
            byte[] value = new byte[full ? FULL_VALUE_SIZE : 10];
            MemoryRecords records = MemoryRecords.withRecords(partitionData.fetchOffset, Compression.NONE,
                    new SimpleRecord(value));
            data.put(tp, new FetchResponseData.PartitionData()
                    .setPartitionIndex(tp.partition())
                    .setHighWatermark(partitionData.fetchOffset + 100)
                    .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
                    .setLogStartOffset(0)
                    .setRecords(records));
        });
        client.respondToRequest(clientRequest, FetchResponse.of(Errors.NONE, throttleTimeMs, sessionId, data, List.of()));
    }

    private void consumeAll() {
        for (int i = 0; i < 100 && !fetchBuffer.isEmpty(); i++)
            collector.collectFetch(fetchBuffer);
    }

    /** Consumes the buffered data of {@code consumed} only, by pausing the other partition for a moment. */
    private void consumeAllBut(TopicPartition kept, Set<TopicPartition> consumed) {
        subscriptions.pause(kept);
        for (int i = 0; i < 100 && fetchBuffer.hasCompletedFetches(cf -> consumed.contains(cf.partition)); i++)
            collector.collectFetch(fetchBuffer);
        subscriptions.resume(kept);
    }

    private ClientRequest requestTo(String destination) {
        for (ClientRequest request : client.requests()) {
            if (request.destination().equals(destination))
                return request;
        }
        throw new AssertionError("no request in flight to " + destination);
    }

    private List<String> destinations() {
        return new ArrayList<>(lastRequests.keySet());
    }

    private void assertNoPartitionInTwoRequestsInFlight() {
        Set<TopicPartition> seen = new HashSet<>();
        for (ClientRequest request : client.requests()) {
            for (TopicPartition tp : sessionPartitions.getOrDefault(request.destination(), Set.of()))
                assertTrue(seen.add(tp), tp + " is in two fetch requests in flight");
        }
    }

    private Set<TopicPartition> requestedPartitions(ClientRequest request) {
        return requestedPartitions(((FetchRequest.Builder) request.requestBuilder()).build(ApiKeys.FETCH.latestVersion()));
    }

    private Set<TopicPartition> requestedPartitions(FetchRequest request) {
        return request.fetchData(topicNames).keySet().stream().map(TopicIdPartition::topicPartition).collect(Collectors.toSet());
    }

    private Set<TopicPartition> forgotten(FetchRequest request) {
        return request.forgottenTopics(topicNames).stream().map(TopicIdPartition::topicPartition).collect(Collectors.toSet());
    }

    private Map<TopicPartition, Long> fetchOffsets(FetchRequest request) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        request.fetchData(topicNames).forEach((tp, data) -> offsets.put(tp.topicPartition(), data.fetchOffset));
        return offsets;
    }

    private static ByteBuffer serialize(FetchRequest request) {
        return request.serializeWithHeader(new RequestHeader(ApiKeys.FETCH, request.version(), "client", 1));
    }
}
