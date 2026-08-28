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
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerReactorLifecycleTest {

    @Test
    public void testCloseDoesNotWaitLongerThanTimeoutWhenCleanupIsBlocked() throws Exception {
        ReactorFixture fixture = new ReactorFixture();
        CountDownLatch cleanupStarted = new CountDownLatch(1);
        CountDownLatch releaseCleanup = new CountDownLatch(1);
        when(fixture.requestManagers.entries()).thenReturn(List.of());
        doAnswer(invocation -> {
            cleanupStarted.countDown();
            releaseCleanup.await(TestUtils.DEFAULT_MAX_WAIT_MS, TimeUnit.MILLISECONDS);
            return null;
        }).when(fixture.requestManagers).close();

        fixture.reactor.start();
        CountDownLatch closeReturned = new CountDownLatch(1);
        Thread closeThread = new Thread(() -> {
            fixture.reactor.close(Duration.ofMillis(25L));
            closeReturned.countDown();
        });
        closeThread.start();
        try {
            assertTrue(cleanupStarted.await(TestUtils.DEFAULT_MAX_WAIT_MS, TimeUnit.MILLISECONDS));
            assertTrue(closeReturned.await(500L, TimeUnit.MILLISECONDS),
                "close(Duration) must return when its reactor-close budget expires");
            assertTrue(fixture.reactor.isAlive(),
                "a timed-out close may return while daemon cleanup finishes in the background");
        } finally {
            releaseCleanup.countDown();
            closeThread.join(TestUtils.DEFAULT_MAX_WAIT_MS);
            fixture.reactor.join(TestUtils.DEFAULT_MAX_WAIT_MS);
        }
    }

    @Test
    public void testManagerPollFailurePublishesTerminalErrorAfterScheduleAndStopsReactor() {
        ReactorFixture fixture = new ReactorFixture();
        RequestManager manager = mock(RequestManager.class);
        RuntimeException failure = new RuntimeException("manager poll failed");
        AtomicLong errorPublicationGeneration = new AtomicLong(-1L);
        when(fixture.requestManagers.entries()).thenReturn(List.of(manager));
        when(manager.poll(anyLong())).thenThrow(failure);
        doAnswer(invocation -> {
            ErrorEvent event = invocation.getArgument(0);
            assertEquals(failure, event.error());
            errorPublicationGeneration.set(fixture.reactor.reactorScheduleGeneration());
            return null;
        }).when(fixture.requestManagers).publishBackgroundEvent(any(ErrorEvent.class));
        fixture.reactor.initializeResources();

        fixture.reactor.runOnce();

        verify(fixture.metrics).recordManagerPollFailure();
        verify(fixture.requestManagers).publishBackgroundEvent(any(ErrorEvent.class));
        verify(fixture.requestManagers).wakeupApplicationThread();
        verify(fixture.networkClientDelegate).poll(anyLong(), anyLong());
        assertTrue(errorPublicationGeneration.get() > 0L,
            "the failure must become visible only after the corresponding schedule publication");
        assertFalse(fixture.reactor.isRunning(),
            "an unexpected manager exception must not be disguised as a recoverable input wait");
    }

    private static final class ReactorFixture {
        private final BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
        private final ApplicationEventProcessor applicationEventProcessor = mock(ApplicationEventProcessor.class);
        private final CompletableEventReaper applicationEventReaper = mock(CompletableEventReaper.class);
        private final NetworkClientDelegate networkClientDelegate = mock(NetworkClientDelegate.class);
        private final RequestManagers requestManagers = mock(RequestManagers.class);
        private final AsyncConsumerMetrics metrics = mock(AsyncConsumerMetrics.class);
        private final ConsumerReactor reactor = new ConsumerReactor(
            new LogContext(),
            new MockTime(),
            applicationEventQueue,
            applicationEventReaper,
            () -> applicationEventProcessor,
            () -> networkClientDelegate,
            () -> requestManagers,
            metrics
        );
    }
}
