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
import org.apache.kafka.clients.consumer.internals.events.CompletableBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionsAssignedEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics.BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
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

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    public void testDrainedCallbackCompletesOnNetworkOwner(int outcome) throws Exception {
        BackgroundEventHandler handler = new BackgroundEventHandler(backgroundEventsQueue,
                new MockTime(), mock(AsyncConsumerMetrics.class));
        PartitionsAssignedEvent callback = new PartitionsAssignedEvent(Collections.emptySet(), Collections.emptySortedSet());
        AtomicReference<Thread> completionThread = new AtomicReference<>();
        callback.future().whenComplete((ignored, error) -> completionThread.set(Thread.currentThread()));
        ExecutorService networkOwner = Executors.newSingleThreadExecutor();
        try {
            Thread owner = networkOwner.submit(() -> {
                handler.add(callback);
                return Thread.currentThread();
            }).get(10, TimeUnit.SECONDS);
            // The app drains the event, but ownership of its result stays with the network thread.
            assertEquals(Collections.singletonList(callback), handler.drainEvents());
            assertEquals(1, handler.pendingCallbackCount());
            assertFalse(callback.future().isDone());
            networkOwner.submit(() -> {
                if (outcome == 0)
                    callback.future().complete(null);
                else if (outcome == 1)
                    callback.future().completeExceptionally(new KafkaException("Callback failed"));
                else
                    handler.closePendingCallbacks();
            }).get(10, TimeUnit.SECONDS);
            assertSame(owner, completionThread.get());
            assertTrue(callback.future().isDone());
            assertEquals(outcome != 0, callback.future().isCompletedExceptionally());
            if (outcome == 2) {
                Throwable error = assertThrows(CompletionException.class, callback.future()::join).getCause();
                assertInstanceOf(TimeoutException.class, error);
                assertEquals("PartitionsAssignedEvent could not be completed before the consumer closed", error.getMessage());
            }
            assertEquals(0, handler.pendingCallbackCount());
        } finally {
            networkOwner.shutdownNow();
            assertTrue(networkOwner.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testCloseRejectsReentrantAndLateCallbacksWithoutReplacingTerminalResults() {
        BackgroundEventHandler handler = new BackgroundEventHandler(backgroundEventsQueue,
                new MockTime(), mock(AsyncConsumerMetrics.class));
        PartitionsAssignedEvent pending = new PartitionsAssignedEvent(Collections.emptySet(), Collections.emptySortedSet());
        PartitionsAssignedEvent reentrant = new PartitionsAssignedEvent(Collections.emptySet(), Collections.emptySortedSet());
        PartitionsAssignedEvent completed = new PartitionsAssignedEvent(Collections.emptySet(), Collections.emptySortedSet());
        AtomicInteger completions = new AtomicInteger();
        pending.future().whenComplete((ignored, error) -> {
            completions.incrementAndGet();
            handler.add(reentrant);
        });
        handler.add(pending);
        handler.add(completed);
        completed.future().complete(null);
        assertEquals(1, handler.pendingCallbackCount());

        handler.closePendingCallbacks();
        handler.closePendingCallbacks();
        PartitionsAssignedEvent late = new PartitionsAssignedEvent(Collections.emptySet(), Collections.emptySortedSet());
        handler.add(late);

        assertEquals(1, completions.get());
        assertTrue(pending.future().isCompletedExceptionally());
        assertTrue(reentrant.future().isCompletedExceptionally());
        assertTrue(late.future().isCompletedExceptionally());
        assertFalse(completed.future().isCompletedExceptionally());
        assertEquals(0, handler.pendingCallbackCount());
        assertEquals(2, backgroundEventsQueue.size());
    }

    @Test
    public void testFiniteCallbackDeadlineIsRejectedAndTerminalized() {
        BackgroundEventHandler handler = new BackgroundEventHandler(backgroundEventsQueue,
                new MockTime(), mock(AsyncConsumerMetrics.class));
        CompletableBackgroundEvent<Void> callback = new CompletableBackgroundEvent<Void>(
                BackgroundEvent.Type.PARTITIONS_ASSIGNED, 1000L) { };
        assertThrows(IllegalArgumentException.class, () -> handler.add(callback));
        assertTrue(callback.future().isCompletedExceptionally());
        assertTrue(backgroundEventsQueue.isEmpty());
        assertEquals(0, handler.pendingCallbackCount());
    }

}
