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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics.BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class BackgroundEventHandlerTest {
    private final BlockingQueue<BackgroundEvent> backgroundEventsQueue =  new LinkedBlockingQueue<>();

    @ParameterizedTest
    @MethodSource("org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetricsTest#groupNameProvider")
    public void testRecordBackgroundEventQueueSize(String groupName) {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, groupName)) {
            BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(
                backgroundEventsQueue,
                new MockTime(0),
                asyncConsumerMetrics);
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

    /**
     * {@link BackgroundEventHandler#add(BackgroundEvent)} must publish the event before it runs the application
     * wakeup hook, and run the hook exactly once per event. The hook records the queue size it observes, so a
     * hook that ran before publication would record 0 (and the application thread could miss the event).
     */
    @Test
    public void testAddPublishesEventBeforeRunningApplicationWakeup() {
        List<Integer> queueSizesSeenByWakeup = new ArrayList<>();
        BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(
            backgroundEventsQueue,
            new MockTime(0),
            mock(AsyncConsumerMetrics.class),
            () -> queueSizesSeenByWakeup.add(backgroundEventsQueue.size()));

        backgroundEventHandler.add(new ErrorEvent(new Throwable()));
        assertEquals(List.of(1), queueSizesSeenByWakeup);
        assertEquals(1, backgroundEventsQueue.size());

        backgroundEventHandler.add(new ErrorEvent(new Throwable()));
        assertEquals(List.of(1, 2), queueSizesSeenByWakeup);
        assertEquals(2, backgroundEventsQueue.size());
    }

    @Test
    public void testWakeupApplicationRunsHookWithoutPublishingEvent() {
        AtomicInteger wakeups = new AtomicInteger();
        BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(
            backgroundEventsQueue,
            new MockTime(0),
            mock(AsyncConsumerMetrics.class),
            wakeups::incrementAndGet);

        backgroundEventHandler.wakeupApplication();
        assertEquals(1, wakeups.get());
        assertTrue(backgroundEventsQueue.isEmpty());

        backgroundEventHandler.wakeupApplication();
        assertEquals(2, wakeups.get());
        assertTrue(backgroundEventsQueue.isEmpty());
    }

    @Test
    public void testThreeArgConstructorUsesNoOpApplicationWakeup() {
        BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(
            backgroundEventsQueue,
            new MockTime(0),
            mock(AsyncConsumerMetrics.class));

        assertDoesNotThrow(backgroundEventHandler::wakeupApplication);
        assertDoesNotThrow(() -> backgroundEventHandler.add(new ErrorEvent(new Throwable())));
        assertEquals(1, backgroundEventsQueue.size());
    }

    @Test
    public void testNullApplicationWakeupIsRejected() {
        assertThrows(NullPointerException.class, () -> new BackgroundEventHandler(
            backgroundEventsQueue,
            new MockTime(0),
            mock(AsyncConsumerMetrics.class),
            null));
    }
}
