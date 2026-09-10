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

import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEventHandler;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShareAcknowledgementEventHandlerTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testAcknowledgementWakesApplication(boolean alreadyWaiting) throws Exception {
        BlockingQueue<ShareAcknowledgementEvent> queue = new LinkedBlockingQueue<>();
        ShareAcknowledgementEvent event = new ShareAcknowledgementEvent(Map.of(), false, Optional.empty());
        try (ShareFetchBuffer buffer = new ShareFetchBuffer(new LogContext())) {
            ShareAcknowledgementEventHandler handler = new ShareAcknowledgementEventHandler(queue, () -> {
                assertSame(event, queue.peek());
                buffer.wakeup();
            });
            CompletableFuture<Void> returned = new CompletableFuture<>();
            Thread application = new Thread(() -> {
                try {
                    buffer.awaitNotEmpty(Time.SYSTEM.timer(30_000));
                    returned.complete(null);
                } catch (Throwable t) {
                    returned.completeExceptionally(t);
                }
            });
            try {
                if (!alreadyWaiting)
                    handler.add(event);
                application.start();
                if (alreadyWaiting) {
                    TestUtils.waitForCondition(() -> application.getState() == Thread.State.TIMED_WAITING,
                        "Application did not park waiting for fetch data");
                    handler.add(event);
                }
                returned.get(5, TimeUnit.SECONDS);
                assertEquals(List.of(event), handler.drainEvents());
                assertTrue(buffer.isEmpty());
            } finally {
                application.interrupt();
                application.join(5_000);
            }
        }
    }
}
