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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class OffsetCommitCallbackInvokerTest {

    @Mock
    private ConsumerInterceptors<?, ?> consumerInterceptors;
    private OffsetCommitCallbackInvoker offsetCommitCallbackInvoker;
    @Mock
    private Runnable wakeupApplication;

    @BeforeEach
    public void setup() {
        offsetCommitCallbackInvoker = new OffsetCommitCallbackInvoker(consumerInterceptors, wakeupApplication);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testEnqueueWakesApplicationAndRunsCallbackOnApplicationThread(boolean alreadyWaiting) throws Exception {
        try (FetchBuffer buffer = new FetchBuffer(new LogContext())) {
            OffsetCommitCallbackInvoker invoker = new OffsetCommitCallbackInvoker(consumerInterceptors, buffer::wakeup);
            Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(new TopicPartition("topic", 0), new OffsetAndMetadata(10));
            Exception error = new IllegalStateException("commit failed");
            AtomicReference<Thread> callbackThread = new AtomicReference<>();
            OffsetCommitCallback callback = (actualOffsets, actualError) -> {
                assertSame(offsets, actualOffsets);
                assertSame(error, actualError);
                callbackThread.set(Thread.currentThread());
            };
            CompletableFuture<Void> returned = new CompletableFuture<>();
            Thread application = new Thread(() -> {
                try {
                    buffer.awaitWakeup(Time.SYSTEM.timer(30_000));
                    invoker.executeCallbacks();
                    returned.complete(null);
                } catch (Throwable t) {
                    returned.completeExceptionally(t);
                }
            });
            try {
                if (!alreadyWaiting)
                    invoker.enqueueUserCallbackInvocation(callback, offsets, error);
                application.start();
                if (alreadyWaiting) {
                    TestUtils.waitForCondition(() -> application.getState() == Thread.State.TIMED_WAITING,
                        "Application did not park waiting for fetch data");
                    invoker.enqueueUserCallbackInvocation(callback, offsets, error);
                }
                returned.get(5, TimeUnit.SECONDS);
                assertSame(application, callbackThread.get());
            } finally {
                application.interrupt();
                application.join(5_000);
            }
        }
    }

    @Test
    public void testMultipleUserCallbacksInvoked() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        Map<TopicPartition, OffsetAndMetadata> offsets1 =
            Collections.singletonMap(t0, new OffsetAndMetadata(10L));
        Map<TopicPartition, OffsetAndMetadata> offsets2 =
            Collections.singletonMap(t0, new OffsetAndMetadata(20L));
        OffsetCommitCallback callback1 = mock(OffsetCommitCallback.class);
        OffsetCommitCallback callback2 = mock(OffsetCommitCallback.class);

        offsetCommitCallbackInvoker.enqueueUserCallbackInvocation(callback1, offsets1, null);
        offsetCommitCallbackInvoker.enqueueUserCallbackInvocation(callback2, offsets2, null);
        verify(callback1, never()).onComplete(any(), any());
        verify(callback2, never()).onComplete(any(), any());
        verify(wakeupApplication, times(2)).run();

        offsetCommitCallbackInvoker.executeCallbacks();
        InOrder inOrder = inOrder(callback1, callback2);
        inOrder.verify(callback1).onComplete(offsets1, null);
        inOrder.verify(callback2).onComplete(offsets2, null);

        offsetCommitCallbackInvoker.executeCallbacks();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testNoOnCommitOnEmptyInterceptors() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        Map<TopicPartition, OffsetAndMetadata> offsets1 =
            Collections.singletonMap(t0, new OffsetAndMetadata(10L));
        Map<TopicPartition, OffsetAndMetadata> offsets2 =
            Collections.singletonMap(t0, new OffsetAndMetadata(20L));
        when(consumerInterceptors.isEmpty()).thenReturn(true);

        offsetCommitCallbackInvoker.enqueueInterceptorInvocation(offsets1);
        offsetCommitCallbackInvoker.enqueueInterceptorInvocation(offsets2);
        offsetCommitCallbackInvoker.executeCallbacks();
        verify(consumerInterceptors, never()).onCommit(any());
        verifyNoInteractions(wakeupApplication);
    }

    @Test
    public void testOnlyInterceptors() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        Map<TopicPartition, OffsetAndMetadata> offsets1 =
            Collections.singletonMap(t0, new OffsetAndMetadata(10L));
        Map<TopicPartition, OffsetAndMetadata> offsets2 =
            Collections.singletonMap(t0, new OffsetAndMetadata(20L));
        when(consumerInterceptors.isEmpty()).thenReturn(false);

        offsetCommitCallbackInvoker.enqueueInterceptorInvocation(offsets1);
        offsetCommitCallbackInvoker.enqueueInterceptorInvocation(offsets2);
        verify(consumerInterceptors, never()).onCommit(any());
        verify(wakeupApplication, times(2)).run();

        offsetCommitCallbackInvoker.executeCallbacks();
        InOrder inOrder = inOrder(consumerInterceptors);
        inOrder.verify(consumerInterceptors).onCommit(offsets1);
        inOrder.verify(consumerInterceptors).onCommit(offsets2);

        offsetCommitCallbackInvoker.executeCallbacks();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testMixedCallbacksInterceptorsInvoked() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        Map<TopicPartition, OffsetAndMetadata> offsets1 =
            Collections.singletonMap(t0, new OffsetAndMetadata(10L));
        Map<TopicPartition, OffsetAndMetadata> offsets2 =
            Collections.singletonMap(t0, new OffsetAndMetadata(20L));
        OffsetCommitCallback callback1 = mock(OffsetCommitCallback.class);
        when(consumerInterceptors.isEmpty()).thenReturn(false);

        offsetCommitCallbackInvoker.enqueueInterceptorInvocation(offsets1);
        offsetCommitCallbackInvoker.enqueueInterceptorInvocation(offsets2);
        offsetCommitCallbackInvoker.enqueueUserCallbackInvocation(callback1, offsets1, null);
        verify(callback1, never()).onComplete(any(), any());
        verify(consumerInterceptors, never()).onCommit(any());

        offsetCommitCallbackInvoker.executeCallbacks();
        InOrder inOrder = inOrder(callback1, consumerInterceptors);
        inOrder.verify(consumerInterceptors).onCommit(offsets1);
        inOrder.verify(consumerInterceptors).onCommit(offsets2);
        inOrder.verify(callback1).onComplete(offsets1, null);

        offsetCommitCallbackInvoker.executeCallbacks();
        inOrder.verifyNoMoreInteractions();
    }


}
