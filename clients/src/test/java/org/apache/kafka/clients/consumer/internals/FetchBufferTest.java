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
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This tests the {@link FetchBuffer} functionality in addition to what {@link FetcherTest} covers in its tests.
 * One of the main concerns of these tests are that we correctly handle both places that data is held internally:
 *
 * <ol>
 *     <li>A special "next in line" buffer</li>
 *     <li>The remainder of the buffers in a queue</li>
 * </ol>
 */
public class FetchBufferTest {

    private final Time time = new MockTime(0, 0, 0);
    private final TopicPartition topicAPartition0 = new TopicPartition("topic-a", 0);
    private final TopicPartition topicAPartition1 = new TopicPartition("topic-a", 1);
    private final TopicPartition topicAPartition2 = new TopicPartition("topic-a", 2);
    private final Set<TopicPartition> allPartitions = partitions(topicAPartition0, topicAPartition1, topicAPartition2);
    private LogContext logContext;

    private SubscriptionState subscriptions;

    private FetchMetricsManager metricsManager;

    @BeforeEach
    public void setup() {
        logContext = new LogContext();

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        ConsumerConfig config = new ConsumerConfig(p);

        subscriptions = createSubscriptionState(config, logContext);

        Metrics metrics = createMetrics(config, time);
        metricsManager = createFetchMetricsManager(metrics);
    }

    /**
     * Verifies the basics: we can add buffered data to the queue, peek to view them, and poll to remove them.
     */
    @Test
    public void testBasicPeekAndPoll() {
        try (FetchBuffer fetchBuffer = new FetchBuffer(logContext)) {
            CompletedFetch completedFetch = completedFetch(topicAPartition0);
            assertTrue(fetchBuffer.isEmpty());
            fetchBuffer.add(completedFetch);
            assertTrue(fetchBuffer.hasCompletedFetches(p -> true));
            assertFalse(fetchBuffer.isEmpty());
            assertNotNull(fetchBuffer.peek());
            assertSame(completedFetch, fetchBuffer.peek());
            assertSame(completedFetch, fetchBuffer.poll());
            assertNull(fetchBuffer.peek());
        }
    }

    /**
     * Verifies {@link FetchBuffer#close()}} closes the buffered data for both the queue and the next-in-line buffer.
     */
    @Test
    public void testCloseClearsData() {
        // We don't use the try-with-resources approach because we want to have access to the FetchBuffer after
        // the try block so that we can run our asserts on the object.
        FetchBuffer fetchBuffer = null;

        try {
            fetchBuffer = new FetchBuffer(logContext);
            assertNull(fetchBuffer.nextInLineFetch());
            assertTrue(fetchBuffer.isEmpty());

            fetchBuffer.add(completedFetch(topicAPartition0));
            assertFalse(fetchBuffer.isEmpty());

            fetchBuffer.setNextInLineFetch(completedFetch(topicAPartition0));
            assertNotNull(fetchBuffer.nextInLineFetch());
        } finally {
            if (fetchBuffer != null)
                fetchBuffer.close();
        }

        assertNull(fetchBuffer.nextInLineFetch());
        assertTrue(fetchBuffer.isEmpty());
    }

    /**
     * Tests that the buffer returns partitions for both the queue and the next-in-line buffer.
     */
    @Test
    public void testBufferedPartitions() {
        try (FetchBuffer fetchBuffer = new FetchBuffer(logContext)) {
            fetchBuffer.setNextInLineFetch(completedFetch(topicAPartition0));
            fetchBuffer.add(completedFetch(topicAPartition1));
            fetchBuffer.add(completedFetch(topicAPartition2));
            assertEquals(allPartitions, fetchBuffer.bufferedPartitions());

            fetchBuffer.setNextInLineFetch(null);
            assertEquals(partitions(topicAPartition1, topicAPartition2), fetchBuffer.bufferedPartitions());

            fetchBuffer.poll();
            assertEquals(partitions(topicAPartition2), fetchBuffer.bufferedPartitions());

            fetchBuffer.poll();
            assertEquals(partitions(), fetchBuffer.bufferedPartitions());
        }
    }

    /**
     * Tests that the buffer manipulates partitions for both the queue and the next-in-line buffer.
     */
    @Test
    public void testAddAllAndRetainAll() {
        try (FetchBuffer fetchBuffer = new FetchBuffer(logContext)) {
            fetchBuffer.setNextInLineFetch(completedFetch(topicAPartition0));
            fetchBuffer.addAll(Arrays.asList(completedFetch(topicAPartition1), completedFetch(topicAPartition2)));
            assertEquals(allPartitions, fetchBuffer.bufferedPartitions());

            fetchBuffer.retainAll(partitions(topicAPartition1, topicAPartition2));
            assertEquals(partitions(topicAPartition1, topicAPartition2), fetchBuffer.bufferedPartitions());

            fetchBuffer.retainAll(partitions(topicAPartition2));
            assertEquals(partitions(topicAPartition2), fetchBuffer.bufferedPartitions());

            fetchBuffer.retainAll(partitions());
            assertEquals(partitions(), fetchBuffer.bufferedPartitions());
        }
    }

    @Test
    public void testWakeup() throws Exception {
        try (FetchBuffer fetchBuffer = new FetchBuffer(logContext)) {
            final Thread waitingThread = new Thread(() -> {
                final Timer timer = time.timer(Duration.ofMinutes(1));
                fetchBuffer.awaitWakeup(timer);
            });
            waitingThread.start();
            fetchBuffer.wakeup();
            waitingThread.join(Duration.ofSeconds(30).toMillis());
            assertFalse(waitingThread.isAlive());
        }
    }

    /**
     * A {@link FetchBuffer#wakeup()} issued while no thread is waiting must not be lost: the next
     * {@link FetchBuffer#awaitWakeup(Timer)} returns immediately instead of waiting out its timer.
     */
    @Test
    public void testWakeupBeforeAwaitIsSticky() {
        try (FetchBuffer fetchBuffer = new FetchBuffer(logContext)) {
            fetchBuffer.wakeup();

            long startNs = System.nanoTime();
            fetchBuffer.awaitWakeup(time.timer(Duration.ofSeconds(10)));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
            assertTrue(elapsedMs < 5_000, "awaitWakeup should return immediately, took " + elapsedMs + " ms");
        }
    }

    /**
     * A thread that is actually parked in {@link FetchBuffer#awaitWakeup(Timer)} is released promptly by a
     * {@link FetchBuffer#wakeup()} from another thread. The waiter is confirmed to be parked (not merely started)
     * before the wakeup is issued so the signalling path is exercised, not only the sticky flag.
     */
    @Test
    public void testWakeupReleasesParkedThreadPromptly() throws Exception {
        try (FetchBuffer fetchBuffer = new FetchBuffer(logContext)) {
            final CountDownLatch started = new CountDownLatch(1);
            final AtomicLong waitedMs = new AtomicLong(-1);
            final Thread waitingThread = new Thread(() -> {
                final Timer timer = time.timer(Duration.ofSeconds(30));
                started.countDown();
                long startNs = System.nanoTime();
                fetchBuffer.awaitWakeup(timer);
                waitedMs.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs));
            });
            waitingThread.start();
            assertTrue(started.await(5, TimeUnit.SECONDS));
            TestUtils.waitForCondition(() -> waitingThread.getState() == Thread.State.TIMED_WAITING,
                "Waiting thread never parked in awaitWakeup");

            fetchBuffer.wakeup();

            waitingThread.join(TimeUnit.SECONDS.toMillis(5));
            assertFalse(waitingThread.isAlive(), "Waiting thread was not released by wakeup()");
            assertTrue(waitedMs.get() >= 0 && waitedMs.get() < 5_000,
                "awaitWakeup should return well before its 30 s timer, took " + waitedMs.get() + " ms");
        }
    }

    /**
     * Stress test for the hand-off between {@link FetchBuffer#awaitWakeup(Timer)} and
     * {@link FetchBuffer#wakeup()}. {@code wakeup()} sets the sticky flag first and only takes the lock when it
     * observes a waiter, so a wakeup that races with the waiter between marking itself waiting, checking the flag
     * and parking must never be lost. That interleaving cannot be forced deterministically from a test, so this
     * test races the two threads many times with the wakeup issued as close as possible to the wait and requires
     * every wait to return far sooner than its timer. A lost wakeup shows up as a wait that runs to the timer.
     */
    @Test
    public void testWakeupRacingWithAwaitIsNeverLost() throws Exception {
        final int iterations = 200;
        final long timerMs = 2_000;
        final long maxAcceptableWaitMs = 500;
        try (FetchBuffer fetchBuffer = new FetchBuffer(logContext)) {
            for (int i = 0; i < iterations; i++) {
                final CountDownLatch aboutToWait = new CountDownLatch(1);
                final AtomicLong waitedMs = new AtomicLong(-1);
                final Thread waitingThread = new Thread(() -> {
                    final Timer timer = time.timer(Duration.ofMillis(timerMs));
                    aboutToWait.countDown();
                    long startNs = System.nanoTime();
                    fetchBuffer.awaitWakeup(timer);
                    waitedMs.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs));
                });
                waitingThread.start();
                assertTrue(aboutToWait.await(5, TimeUnit.SECONDS));
                fetchBuffer.wakeup();
                waitingThread.join(timerMs + 5_000);
                assertFalse(waitingThread.isAlive(), "Iteration " + i + ": waiting thread did not return");
                assertTrue(waitedMs.get() < maxAcceptableWaitMs,
                    "Iteration " + i + ": wakeup was lost, awaitWakeup took " + waitedMs.get() + " ms");
            }
        }
    }

    private CompletedFetch completedFetch(TopicPartition tp) {
        FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData();
        FetchMetricsAggregator metricsAggregator = new FetchMetricsAggregator(metricsManager, allPartitions);
        return new CompletedFetch(
                logContext.logger(CompletedFetch.class),
                subscriptions,
                BufferSupplier.create(),
                tp,
                partitionData,
                metricsAggregator,
                0L);
    }

    /**
     * This is a handy utility method for returning a set from a varargs array.
     */
    private static Set<TopicPartition> partitions(TopicPartition... partitions) {
        return Set.of(partitions);
    }
}