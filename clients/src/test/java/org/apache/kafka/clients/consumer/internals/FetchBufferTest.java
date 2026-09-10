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
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBackgroundEventWakesAppBeforeOrAfterParking(boolean publishBeforeParking) throws Exception {
        BlockingQueue<BackgroundEvent> events = new LinkedBlockingQueue<>();
        ErrorEvent event = new ErrorEvent(new KafkaException("background failure"));
        try (FetchBuffer buffer = new FetchBuffer(logContext)) {
            BackgroundEventHandler handler = new BackgroundEventHandler(events, time,
                    mock(AsyncConsumerMetrics.class), () -> {
                        assertSame(event, events.peek(), "event must be visible before notification");
                        buffer.wakeup();
                    });
            CompletableFuture<Void> observed = new CompletableFuture<>();
            Thread waiter = new Thread(() -> {
                try {
                    buffer.awaitWakeup(Time.SYSTEM.timer(60_000));
                    assertSame(event, events.peek());
                    observed.complete(null);
                } catch (Throwable t) {
                    observed.completeExceptionally(t);
                }
            });
            try {
                if (publishBeforeParking)
                    handler.add(event);
                waiter.start();
                if (!publishBeforeParking) {
                    org.apache.kafka.test.TestUtils.waitForCondition(
                            () -> waiter.getState() == Thread.State.TIMED_WAITING,
                            "application should park before publication");
                    handler.add(event);
                }
                observed.get(5, TimeUnit.SECONDS);
                assertEquals(1, handler.drainEvents().size());
                assertTrue(handler.drainEvents().isEmpty());
            } finally {
                waiter.interrupt();
                waiter.join(5000);
            }
        }
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

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    public void testRealConditionWaitRegistersAndEndsActivity(int completion) throws Exception {
        try (FetchBuffer buffer = new FetchBuffer(logContext, time)) {
            CompletableFuture<Throwable> result = new CompletableFuture<>();
            Thread app = new Thread(() -> {
                try {
                    buffer.awaitWakeup(time.timer(60_000));
                    result.complete(null);
                } catch (Throwable t) {
                    result.complete(t);
                }
            }, "registered-poll-wait-test");
            try {
                app.start();
                org.apache.kafka.test.TestUtils.waitForCondition(() -> app.getState() == Thread.State.TIMED_WAITING,
                        "app should enter the condition wait");
                time.sleep(1000);
                assertEquals(1000, buffer.applicationPollWait().activityMs(time.milliseconds()));
                if (completion == 0)
                    buffer.wakeup();
                else if (completion == 1)
                    buffer.add(completedFetch(topicAPartition0));
                else if (completion == 2)
                    app.interrupt();
                else
                    buffer.close();
                Throwable error = result.get(5, TimeUnit.SECONDS);
                if (completion == 2)
                    assertInstanceOf(InterruptException.class, error);
                else
                    assertNull(error);
                time.sleep(10_000);
                assertEquals(1000, buffer.applicationPollWait().activityMs(time.milliseconds()),
                        "leaving await must not keep a subsequent user callback healthy");
            } finally {
                app.interrupt();
                buffer.wakeup();
                app.join(5000);
                assertFalse(app.isAlive());
            }
        }
    }

    @Test
    public void testNotificationBeforeWaitRecordsResponseWithoutOngoingWait() {
        try (FetchBuffer buffer = new FetchBuffer(logContext, time)) {
            buffer.wakeup();
            buffer.awaitWakeup(time.timer(60_000));
            long respondedMs = time.milliseconds();
            time.sleep(1000);
            assertEquals(respondedMs, buffer.applicationPollWait().activityMs(time.milliseconds()));
        }
    }

    @Test
    public void testWaitRegistrationNotifiesBackgroundAfterPublishingEpoch() throws Exception {
        try (FetchBuffer buffer = new FetchBuffer(logContext, time)) {
            CompletableFuture<Long> registration = new CompletableFuture<>();
            buffer.setWaitRegistrationListener(() -> registration.complete(
                buffer.applicationPollWait().currentWaitEpoch(time.milliseconds())));
            buffer.wakeup();
            buffer.awaitWakeup(time.timer(60_000));
            assertFalse(registration.isDone(), "a retained notification must not register a new wait");
            Thread app = new Thread(() -> buffer.awaitWakeup(time.timer(60_000)));
            app.start();
            try {
                assertTrue(registration.get(5, TimeUnit.SECONDS) > 0);
                buffer.wakeup();
                app.join(5000);
                assertFalse(app.isAlive());
                assertEquals(0, buffer.applicationPollWait().currentWaitEpoch(time.milliseconds()));
            } finally {
                buffer.wakeup();
                app.join(5000);
            }
        }
    }

    @Test
    public void testWaitRegistrationFailureReleasesLockAndEndsActivity() throws Exception {
        RuntimeException failure = new RuntimeException("wait registration failed");
        try (FetchBuffer buffer = new FetchBuffer(logContext, time)) {
            buffer.setWaitRegistrationListener(() -> {
                assertTrue(buffer.applicationPollWait().currentWaitEpoch(time.milliseconds()) > 0);
                throw failure;
            });

            assertSame(failure, assertThrows(RuntimeException.class,
                    () -> buffer.awaitWakeup(time.timer(60_000))));
            assertEquals(0, buffer.applicationPollWait().currentWaitEpoch(time.milliseconds()));
            time.sleep(1000);
            assertEquals(0, buffer.applicationPollWait().activityMs(time.milliseconds()));
            assertBufferAccessibleFromAnotherThread(buffer);
        }
    }

    @Test
    public void testWaitCleanupClockFailureReleasesLockAndEndsActivity() throws Exception {
        Time failingTime = spy(new MockTime(0, 100, 0));
        RuntimeException failure = new RuntimeException("cleanup clock read failed");
        Timer timer = failingTime.timer(1);
        try (FetchBuffer buffer = new FetchBuffer(logContext, time)) {
            buffer.setWaitRegistrationListener(() -> {
                assertTrue(buffer.applicationPollWait().currentWaitEpoch(100) > 0);
                doThrow(failure).when(failingTime).milliseconds();
            });

            assertSame(failure, assertThrows(RuntimeException.class, () -> buffer.awaitWakeup(timer)));
            assertEquals(100, timer.currentTimeMs(), "failed clock read must retain the cached timestamp");
            assertEquals(0, buffer.applicationPollWait().currentWaitEpoch(100));
            assertEquals(100, buffer.applicationPollWait().activityMs(200),
                    "failed cleanup must not leave ongoing wait activity");
            assertBufferAccessibleFromAnotherThread(buffer);
        }
    }

    private void assertBufferAccessibleFromAnotherThread(FetchBuffer buffer) throws Exception {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        Thread observer = new Thread(() -> {
            try {
                result.complete(buffer.isEmpty());
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        }, "fetch-buffer-lock-observer");
        // A regression must fail the bounded assertion without keeping the test process alive.
        observer.setDaemon(true);
        observer.start();
        try {
            assertTrue(result.get(5, TimeUnit.SECONDS));
        } finally {
            observer.join(5000);
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
