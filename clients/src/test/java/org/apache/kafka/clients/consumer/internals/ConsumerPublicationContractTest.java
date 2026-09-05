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
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
class ConsumerPublicationContractTest {
    @Test
    void testErrorStatePrecedesReconciliationFutureRelease() {
        AsyncPollEvent event = new AsyncPollEvent(100, 0);
        KafkaException error = new KafkaException("metadata failure");
        var observer = event.reconciliationCheckFuture().thenRun(() -> {
            assertSame(error, event.error().orElseThrow());
            assertTrue(event.isComplete());
        });
        event.completeExceptionally(error);
        observer.join();
    }

    @Test
    void testMetadataErrorBeforeWaitRemainsLatched() {
        try (FetchBuffer buffer = new FetchBuffer(new LogContext())) {
            AsyncPollEvent event = new AsyncPollEvent(Long.MAX_VALUE, 0, buffer::wakeup);
            KafkaException error = new KafkaException("metadata failure");
            event.onMetadataError(error);
            buffer.awaitWakeup(Time.SYSTEM.timer(30_000));
            assertSame(error, event.error().orElseThrow());
            assertTrue(event.isComplete());
        }
    }

    @Test
    void testMetadataErrorDuringWaitPublishesThenReleasesWaiter() throws Exception {
        try (FetchBuffer buffer = new FetchBuffer(new LogContext())) {
            KafkaException error = new KafkaException("metadata failure");
            AsyncPollEvent event = new AsyncPollEvent(Long.MAX_VALUE, 0, buffer::wakeup);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread waiter = new Thread(() -> {
                try {
                    buffer.awaitWakeup(Time.SYSTEM.timer(30_000));
                    assertSame(error, event.error().orElseThrow());
                    assertTrue(event.isComplete());
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "publication-contract-waiter");
            waiter.start();
            try {
                TestUtils.waitForCondition(() -> waiter.getState() == Thread.State.TIMED_WAITING, 2_000,
                    "waiter did not enter the real buffer wait");
                event.onMetadataError(error);
                waiter.join(2_000);
                assertFalse(waiter.isAlive(), "recovery must not depend on the wait timeout");
                assertNull(failure.get());
            } finally {
                buffer.wakeup();
                waiter.interrupt();
                waiter.join(2_000);
            }
        }
    }

    @Test
    void testIndependentErrorsRemainDistinctAndEmptySuccessDoesNotNotify() {
        AtomicInteger notifications = new AtomicInteger();
        AsyncPollEvent first = new AsyncPollEvent(100, 0, notifications::incrementAndGet);
        AsyncPollEvent second = new AsyncPollEvent(100, 0, notifications::incrementAndGet);
        KafkaException firstError = new KafkaException("first");
        KafkaException secondError = new KafkaException("second");
        first.onMetadataError(firstError);
        second.onMetadataError(secondError);
        assertSame(firstError, first.error().orElseThrow());
        assertSame(secondError, second.error().orElseThrow());
        assertEquals(2, notifications.get());
        new AsyncPollEvent(100, 0, notifications::incrementAndGet).completeSuccessfully();
        assertEquals(2, notifications.get(), "an empty successful poll is not an error or data-availability notification");
    }
}
