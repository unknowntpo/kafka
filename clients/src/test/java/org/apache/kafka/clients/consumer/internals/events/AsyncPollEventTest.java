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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.common.KafkaException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AsyncPollEventTest {
    @Test
    void cancellationRequestRevokesPermissionWithoutCompletingOwnerFuture() throws Exception {
        AsyncPollEvent event = new AsyncPollEvent(5000, 0);
        AtomicReference<Thread> completionThread = new AtomicReference<>();
        event.reconciliationCheckFuture().thenRun(() -> completionThread.set(Thread.currentThread()));
        assertTrue(event.requestCancellation());
        assertFalse(event.requestCancellation());
        assertFalse(event.isActive());
        assertFalse(event.isComplete());
        assertNull(completionThread.get());

        Thread owner = new Thread(event::completeSuccessfully, "async-poll-completion-owner");
        owner.start();
        owner.join(5000);
        assertFalse(owner.isAlive());
        assertTrue(event.isComplete());
        assertSame(owner, completionThread.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void firstTerminalResultIsPreserved(boolean failed) {
        AtomicInteger notifications = new AtomicInteger();
        AsyncPollEvent event = new AsyncPollEvent(5000, 0, notifications::incrementAndGet, () -> { });
        KafkaException original = new KafkaException("original");
        if (failed)
            event.completeExceptionally(original);
        else
            event.completeSuccessfully();
        event.completeSuccessfully();
        event.completeExceptionally(new KafkaException("late failure"));
        assertTrue(event.isComplete());
        assertFalse(event.isActive());
        assertFalse(event.requestCancellation());
        assertEquals(failed ? 1 : 0, notifications.get());
        if (failed)
            assertSame(original, event.error().orElseThrow());
        else
            assertTrue(event.error().isEmpty());
    }
}
