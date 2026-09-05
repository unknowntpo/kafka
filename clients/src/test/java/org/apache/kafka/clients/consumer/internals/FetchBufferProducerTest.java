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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Timeout(10)
class FetchBufferProducerTest {
    private final FetchBuffer buffer = spy(new FetchBuffer(new LogContext()));
    private final FetchBufferProducer producer = new FetchBufferProducer(buffer);

    @Test
    void testCompletionPublishesRemovalBeforeNotificationAndOnlyOnce() {
        doAnswer(invocation -> {
            assertFalse(producer.isRequestPending(1));
            assertTrue(producer.isRequestPending(2));
            return invocation.callRealMethod();
        }).when(buffer).wakeup();

        producer.requestCompleted(1);
        verify(buffer, never()).wakeup();
        producer.requestStarted(1);
        producer.requestStarted(2);
        producer.requestCompleted(1);
        producer.requestCompleted(1);
        verify(buffer, times(1)).wakeup();
    }

    @Test
    void testNotificationRequiresCurrentlyAvailableData() {
        producer.signalIfAvailable(tp -> true);
        verify(buffer, never()).wakeup();
        producer.add(completedFetch());
        assertTrue(producer.hasCompletedFetches());
        producer.signalIfAvailable(tp -> false);
        verify(buffer, never()).wakeup();
        producer.signalIfAvailable(tp -> true);
        verify(buffer).wakeup();
    }

    @Test
    void testPartiallyConsumedFetchCanStillEnableNotification() {
        buffer.setNextInLineFetch(completedFetch());
        assertFalse(producer.hasCompletedFetches(), "the queue itself is empty");
        producer.signalIfAvailable(tp -> true);
        verify(buffer).wakeup();
        buffer.setNextInLineFetch(null);
        producer.signalIfAvailable(tp -> true);
        verify(buffer, times(1)).wakeup();
    }

    @Test
    void testDataPublicationIsVisibleWhenWaitReturns() {
        MockTime time = new MockTime();
        CompletedFetch fetch = mock(CompletedFetch.class);
        producer.add(fetch);
        buffer.awaitWakeup(time.timer(1_000));
        assertEquals(fetch, buffer.poll());
    }

    @Test
    void testCompletionBeforeWaitIsLatched() {
        MockTime time = new MockTime();
        producer.requestStarted(1);
        producer.requestCompleted(1);
        buffer.awaitWakeup(time.timer(1_000));
        assertFalse(producer.hasPendingRequests());
    }

    @Test
    void testCompletionDuringWaitReleasesApplication() throws Exception {
        MockTime time = new MockTime();
        AtomicBoolean returned = new AtomicBoolean();
        Thread waiter = new Thread(() -> {
            buffer.awaitWakeup(time.timer(60_000));
            returned.set(true);
        });
        producer.requestStarted(1);
        try {
            waiter.start();
            TestUtils.waitForCondition(() -> waiter.getState() == Thread.State.TIMED_WAITING, "waiter did not enter buffer wait");
            producer.requestCompleted(1);
            waiter.join(2_000);
            assertFalse(waiter.isAlive());
            assertTrue(returned.get());
        } finally {
            buffer.wakeup();
            waiter.join(2_000);
            if (waiter.isAlive()) {
                waiter.interrupt();
                waiter.join(2_000);
            }
        }
    }

    @Test
    void testManagerHierarchyCannotReferenceRawBuffer() throws Exception {
        for (Class<?> type = FetchRequestManager.class; type != Object.class; type = type.getSuperclass()) {
            assertNoRawBufferDependency(type);
        }
        assertTrue(Modifier.isPrivate(AbstractFetch.class.getDeclaredField("bufferProducer").getModifiers()));
        assertTrue(Modifier.isFinal(AbstractFetch.class.getDeclaredField("bufferProducer").getModifiers()));
        assertTrue(Modifier.isFinal(FetchBufferProducer.class.getModifiers()));
        assertThrows(NoSuchMethodException.class, () -> FetchBufferProducer.class.getDeclaredMethod("wakeup"));
    }

    @Test
    void testDependencyGuardDetectsReintroducedBypass() {
        assertThrows(AssertionError.class, () -> assertNoRawBufferDependency(RawBufferBypass.class));
    }

    private static void assertNoRawBufferDependency(Class<?> type) throws IOException {
        String rawBuffer = FetchBuffer.class.getName().replace('.', '/');
        for (String constant : utf8Constants(type)) {
            assertFalse(constant.equals(rawBuffer) || constant.contains("L" + rawBuffer + ";"),
                    () -> type.getName() + " depends on raw buffer: " + constant);
        }
    }

    // Inspect class constants, not just fields: constructor calls, method bodies, casts and method references
    // must not reintroduce the raw type either. This is a dependency guard, not a Java security sandbox.
    private static Set<String> utf8Constants(Class<?> type) throws IOException {
        Set<String> constants = new HashSet<>();
        try (DataInputStream in = new DataInputStream(type.getResourceAsStream("/" + type.getName().replace('.', '/') + ".class"))) {
            assertEquals(0xCAFEBABE, in.readInt());
            in.readUnsignedShort();
            in.readUnsignedShort();
            int count = in.readUnsignedShort();
            for (int i = 1; i < count; i++) {
                int tag = in.readUnsignedByte();
                if (tag == 1) {
                    constants.add(in.readUTF());
                } else if (tag == 5 || tag == 6) {
                    in.readLong();
                    i++;
                } else {
                    skipConstant(in, tag);
                }
            }
        }
        return constants;
    }

    private static void skipConstant(DataInputStream in, int tag) throws IOException {
        switch (tag) {
            case 3:
            case 4:
            case 9:
            case 10:
            case 11:
            case 12:
            case 17:
            case 18:
                in.readInt();
                break;
            case 7:
            case 8:
            case 16:
            case 19:
            case 20:
                in.readUnsignedShort();
                break;
            case 15:
                in.readUnsignedByte();
                in.readUnsignedShort();
                break;
            default:
                throw new IOException("Unexpected constant-pool tag " + tag);
        }
    }

    private CompletedFetch completedFetch() {
        return new CompletedFetch(new LogContext().logger(CompletedFetch.class), mock(SubscriptionState.class),
                mock(BufferSupplier.class), new TopicPartition("fetch-capability-test", 0),
                new FetchResponseData.PartitionData(), mock(FetchMetricsAggregator.class), 0L);
    }

    private static class RawBufferBypass {
        void wake(FetchBuffer buffer) {
            buffer.wakeup();
        }
    }
}
