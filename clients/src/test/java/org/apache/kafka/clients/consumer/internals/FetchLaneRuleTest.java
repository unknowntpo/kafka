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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The rule that decides whether a consumer opens one more connection to a broker
 * ({@link AbstractFetch#laneVote}), and the measurement it relies on ({@link FetchBuffer#waitedNanos()}).
 */
public class FetchLaneRuleTest {

    private final Time time = new MockTime(0, 0, 0);
    private final TopicPartition partition = new TopicPartition("topic-a", 0);
    private LogContext logContext;
    private SubscriptionState subscriptions;
    private FetchMetricsManager metricsManager;

    @BeforeEach
    public void setup() {
        logContext = new LogContext();
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class);
        ConsumerConfig config = new ConsumerConfig(p);
        subscriptions = createSubscriptionState(config, logContext);
        Metrics metrics = createMetrics(config, time);
        metricsManager = createFetchMetricsManager(metrics);
    }

    @Test
    public void testOneMoreConnectionOnlyWhenTheApplicationWaitsForFullResponses() {
        // full responses, no connection held back, the application without data half of the time
        assertEquals(1, AbstractFetch.laneVote(true, false, 0.5));
        assertEquals(1, AbstractFetch.laneVote(true, false, AbstractFetch.ADD_LANE_WAIT_SHARE));
    }

    @Test
    public void testNoVoteWhenTheApplicationIsTheLimit() {
        // The application is almost never without data: one more connection would only buffer more.
        assertEquals(0, AbstractFetch.laneVote(true, false, 0.0));
        assertEquals(0, AbstractFetch.laneVote(true, false, 0.05));
        // A connection had to stop fetching ahead because the application had not consumed yet.
        assertEquals(0, AbstractFetch.laneVote(true, true, 0.9));
    }

    @Test
    public void testOneLessConnectionWhenResponsesAreNotFull() {
        // The broker has less data than the connections can carry, whatever the application does.
        assertEquals(-1, AbstractFetch.laneVote(false, false, 0.9));
        assertEquals(-1, AbstractFetch.laneVote(false, true, 0.0));
    }

    @Test
    public void testNoMoreConnectionsThanPartitions() {
        assertEquals(1, AbstractFetch.maxLanes(0));
        assertEquals(1, AbstractFetch.maxLanes(1));
        assertEquals(Math.min(2, AbstractFetch.MAX_FETCH_LANES), AbstractFetch.maxLanes(2));
        assertEquals(AbstractFetch.MAX_FETCH_LANES, AbstractFetch.maxLanes(1000));
    }

    @Test
    public void testPollWithZeroTimeoutCountsAsTimeWithoutData() throws Exception {
        try (FetchBuffer fetchBuffer = new FetchBuffer(logContext)) {
            assertEquals(0, fetchBuffer.waitedNanos());

            // The application found nothing to consume and does not want to block: a timer that has expired.
            fetchBuffer.awaitWakeup(time.timer(0));
            Thread.sleep(20);
            long whileWithoutData = fetchBuffer.waitedNanos();
            assertTrue(whileWithoutData >= 20_000_000L, "time without data must count while it lasts: " + whileWithoutData);

            // Data arrives and the application takes it: the time without data ends.
            fetchBuffer.add(completedFetch());
            assertNotNull(fetchBuffer.poll());
            long afterData = fetchBuffer.waitedNanos();
            Thread.sleep(20);
            assertEquals(afterData, fetchBuffer.waitedNanos(), "time with data must not count");
        }
    }

    @Test
    public void testNoTimeWithoutDataWhenTheApplicationNeverFoundTheBufferEmpty() throws Exception {
        try (FetchBuffer fetchBuffer = new FetchBuffer(logContext)) {
            fetchBuffer.add(completedFetch());
            assertNotNull(fetchBuffer.poll());
            Thread.sleep(5);
            assertEquals(0, fetchBuffer.waitedNanos());
        }
    }

    private CompletedFetch completedFetch() {
        return new CompletedFetch(
                logContext.logger(CompletedFetch.class),
                subscriptions,
                BufferSupplier.create(),
                partition,
                new FetchResponseData.PartitionData(),
                new FetchMetricsAggregator(metricsManager, Set.of(partition)),
                0L);
    }

    @Test
    public void testBrokerThatThrottledLimitsForAWhile() {
        // a broker that never throttled this consumer does not limit it
        assertFalse(AbstractFetch.brokerLimits(false, 0));
        // right after a throttled response, and until the memory of it runs out, it does
        assertTrue(AbstractFetch.brokerLimits(true, 0));
        assertTrue(AbstractFetch.brokerLimits(true, AbstractFetch.THROTTLE_MEMORY_NANOS - 1));
        assertFalse(AbstractFetch.brokerLimits(true, AbstractFetch.THROTTLE_MEMORY_NANOS));
    }
}
