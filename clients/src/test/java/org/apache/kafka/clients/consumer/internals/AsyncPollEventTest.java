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

import org.apache.kafka.clients.consumer.internals.events.AsyncPollEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AsyncPollEventTest {
    @Test
    public void testOnlyFirstTerminalErrorWakesApplication() {
        Runnable wakeup = mock(Runnable.class);
        AsyncPollEvent event = new AsyncPollEvent(100, 0, wakeup);
        KafkaException first = new KafkaException("first");
        event.completeExceptionally(first);
        event.completeExceptionally(new KafkaException("late"));
        event.completeSuccessfully();
        assertSame(first, event.error().orElseThrow());
        verify(wakeup).run();
        verifyNoMoreInteractions(wakeup);
    }

    @Test
    public void testSuccessfulPreparationAndLateErrorDoNotWakeApplication() {
        Runnable wakeup = mock(Runnable.class);
        AsyncPollEvent event = new AsyncPollEvent(100, 0, wakeup);
        event.completeSuccessfully();
        event.completeSuccessfully();
        event.completeExceptionally(new KafkaException("late"));
        assertTrue(event.isComplete());
        assertTrue(event.error().isEmpty());
        verifyNoInteractions(wakeup);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    public void testErrorWakesApplicationBeforeOrAfterParking(boolean alreadyWaiting, boolean metadataError) throws Exception {
        try (FetchBuffer buffer = new FetchBuffer(new LogContext())) {
            AtomicReference<AsyncPollEvent> pending = new AtomicReference<>();
            KafkaException error = new KafkaException("terminal failure");
            AsyncPollEvent event = new AsyncPollEvent(Long.MAX_VALUE, 0, () -> {
                assertTrue(pending.get().isComplete());
                assertTrue(pending.get().isReconciliationCheckComplete());
                assertSame(error, pending.get().error().orElseThrow());
                buffer.wakeup();
            });
            pending.set(event);
            Runnable fail = () -> {
                if (metadataError)
                    event.onMetadataError(error);
                else
                    event.completeExceptionally(error);
            };
            CompletableFuture<Void> returned = new CompletableFuture<>();
            Thread application = new Thread(() -> {
                try {
                    buffer.awaitWakeup(Time.SYSTEM.timer(30_000));
                    assertSame(error, event.error().orElseThrow());
                    returned.complete(null);
                } catch (Throwable t) {
                    returned.completeExceptionally(t);
                }
            });
            try {
                if (!alreadyWaiting)
                    fail.run();
                application.start();
                if (alreadyWaiting) {
                    TestUtils.waitForCondition(() -> application.getState() == Thread.State.TIMED_WAITING,
                        "Application did not park waiting for fetch data");
                    fail.run();
                }
                returned.get(5, TimeUnit.SECONDS);
            } finally {
                application.interrupt();
                application.join(5_000);
            }
        }
    }
}
