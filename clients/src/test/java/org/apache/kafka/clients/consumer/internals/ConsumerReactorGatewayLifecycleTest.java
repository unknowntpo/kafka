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

import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.CheckAndUpdatePositionsEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsumerReactorGatewayLifecycleTest {

    @Test
    public void testSubmitRacingCloseNeverAdmitsUnprocessedCompletableEvent() throws Exception {
        MockTime time = new MockTime();
        BlockingOfferQueue queue = new BlockingOfferQueue();
        RequestManagers requestManagers = mock(RequestManagers.class);
        when(requestManagers.entries()).thenReturn(List.of());
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, "test-group");
             ConsumerReactorGateway gateway = new ConsumerReactorGateway(
                 new LogContext(),
                 time,
                 5_000,
                 queue,
                 new CompletableEventReaper(new LogContext()),
                 () -> mock(ApplicationEventProcessor.class),
                 () -> mock(NetworkClientDelegate.class),
                 () -> requestManagers,
                 asyncConsumerMetrics
             )) {
            CheckAndUpdatePositionsEvent event = new CheckAndUpdatePositionsEvent(time.milliseconds() + 5_000L);
            AtomicReference<Throwable> submitFailure = new AtomicReference<>();
            Thread submitThread = new Thread(() -> {
                try {
                    gateway.submit(event);
                } catch (Throwable throwable) {
                    submitFailure.set(throwable);
                }
            });
            CountDownLatch closeReturned = new CountDownLatch(1);
            Thread closeThread = new Thread(() -> {
                gateway.close(Duration.ZERO);
                closeReturned.countDown();
            });

            submitThread.start();
            assertTrue(queue.offerStarted.await(5L, TimeUnit.SECONDS));
            closeThread.start();
            assertFalse(closeReturned.await(100L, TimeUnit.MILLISECONDS),
                "close must not cross an application-event acceptance already in progress");

            queue.releaseOffer.countDown();
            submitThread.join(5_000L);
            closeThread.join(5_000L);
            assertNull(submitFailure.get());
            TestUtils.waitForCondition(event.future()::isDone,
                "an event accepted before close must receive a terminal outcome");
            assertTrue(event.future().isCompletedExceptionally());
        }
    }

    private static final class BlockingOfferQueue extends LinkedBlockingQueue<ApplicationEvent> {
        private final CountDownLatch offerStarted = new CountDownLatch(1);
        private final CountDownLatch releaseOffer = new CountDownLatch(1);

        @Override
        public boolean offer(ApplicationEvent event) {
            offerStarted.countDown();
            try {
                assertTrue(releaseOffer.await(5L, TimeUnit.SECONDS));
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return false;
            }
            return super.offer(event);
        }
    }
}
