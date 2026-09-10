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

import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics.BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class BackgroundEventHandlerTest {
    private final BlockingQueue<BackgroundEvent> backgroundEventsQueue =  new LinkedBlockingQueue<>();

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBackgroundEventWakesApplication(boolean alreadyWaiting) throws Exception {
        try (FetchBuffer buffer = new FetchBuffer(new LogContext())) {
            assertEventWakesApplication(alreadyWaiting, buffer::wakeup,
                () -> buffer.awaitWakeup(Time.SYSTEM.timer(30_000)));
            assertTrue(buffer.isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBackgroundEventWakesShareApplication(boolean alreadyWaiting) throws Exception {
        try (ShareFetchBuffer buffer = new ShareFetchBuffer(new LogContext())) {
            assertEventWakesApplication(alreadyWaiting, buffer::wakeup,
                () -> buffer.awaitNotEmpty(Time.SYSTEM.timer(30_000)));
            assertTrue(buffer.isEmpty());
        }
    }

    private void assertEventWakesApplication(boolean alreadyWaiting, Runnable wakeup, Runnable await) throws Exception {
        ErrorEvent event = new ErrorEvent(new IllegalStateException("background error"));
        BackgroundEventHandler handler = new BackgroundEventHandler(backgroundEventsQueue, Time.SYSTEM,
            mock(AsyncConsumerMetrics.class), () -> {
                assertSame(event, backgroundEventsQueue.peek());
                wakeup.run();
            });
        CompletableFuture<Void> returned = new CompletableFuture<>();
        Thread waiter = new Thread(() -> {
            try {
                await.run();
                returned.complete(null);
            } catch (Throwable t) {
                returned.completeExceptionally(t);
            }
        });
        try {
            if (!alreadyWaiting)
                handler.add(event);
            waiter.start();
            if (alreadyWaiting) {
                TestUtils.waitForCondition(() -> waiter.getState() == Thread.State.TIMED_WAITING,
                    "Application did not park waiting for fetch data");
                handler.add(event);
            }
            returned.get(5, TimeUnit.SECONDS);
            assertEquals(java.util.List.of(event), handler.drainEvents());
        } finally {
            waiter.interrupt();
            waiter.join(5_000);
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetricsTest#groupNameProvider")
    public void testRecordBackgroundEventQueueSize(String groupName) {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, groupName)) {
            BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(
                backgroundEventsQueue,
                new MockTime(0),
                asyncConsumerMetrics, () -> { });
            // add event
            backgroundEventHandler.add(new ErrorEvent(new Throwable()));
            assertEquals(
                1,
                (double) metrics.metric(
                    metrics.metricName(BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME, groupName)
                ).metricValue()
            );

            // drain event
            backgroundEventHandler.drainEvents();
            assertEquals(
                0,
                (double) metrics.metric(
                    metrics.metricName(BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME, groupName)
                ).metricValue()
            );
        }
    }
}
