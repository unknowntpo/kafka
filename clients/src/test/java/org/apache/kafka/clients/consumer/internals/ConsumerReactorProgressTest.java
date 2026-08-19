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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsumerReactorProgressTest {

    @Test
    public void testNoManagerDeadlineAllowsUnboundedApplicationWait() {
        ConsumerReactorProgress.ApplicationWait decision =
            ConsumerReactorProgress.decideApplicationWait(List.of(), 42L);

        assertEquals(Long.MAX_VALUE, decision.timeoutMs());
        assertEquals(42L, decision.decidedAtMs());
        assertEquals(ConsumerReactorProgress.WaitMode.AWAIT_EVENT, decision.waitMode());
        assertTrue(decision.source().isEmpty());
    }

    @Test
    public void testEarliestManagerDeadlineWins() {
        RequestManager slowerManager = mock(RequestManager.class);
        RequestManager limitingManager = mock(RequestManager.class);
        when(slowerManager.progressIntent(42L)).thenReturn(
            ConsumerReactorProgress.ProgressIntent.awaitDeadlineAfter(42L, 100L)
        );
        when(limitingManager.progressIntent(42L)).thenReturn(
            ConsumerReactorProgress.ProgressIntent.awaitDeadlineAfter(42L, 25L)
        );

        ConsumerReactorProgress.ApplicationWait decision =
            ConsumerReactorProgress.decideApplicationWait(List.of(slowerManager, limitingManager), 42L);

        assertEquals(25L, decision.timeoutMs());
        assertEquals(42L, decision.decidedAtMs());
        assertEquals(67L, decision.deadlineAtMs());
        assertEquals(ConsumerReactorProgress.WaitMode.AWAIT_DEADLINE, decision.waitMode());
        assertTrue(decision.source().isPresent());
    }

    @Test
    public void testApplicationWaitSubtractsElapsedTime() {
        RequestManager manager = mock(RequestManager.class);
        when(manager.progressIntent(42L)).thenReturn(
            ConsumerReactorProgress.ProgressIntent.awaitDeadlineAfter(42L, 25L)
        );

        ConsumerReactorProgress.ApplicationWait decision =
            ConsumerReactorProgress.decideApplicationWait(List.of(manager), 42L);

        assertEquals(15L, decision.remainingMs(52L));
        assertEquals(0L, decision.remainingMs(67L));
    }

    @Test
    public void testFiniteDeadlineShortensEventWait() {
        ConsumerReactorProgress.ApplicationWait eventWait =
            ConsumerReactorProgress.decideApplicationWait(List.of(), 42L);
        RequestManager manager = mock(RequestManager.class);
        when(manager.progressIntent(42L)).thenReturn(
            ConsumerReactorProgress.ProgressIntent.awaitDeadlineAfter(42L, 25L)
        );

        ConsumerReactorProgress.ApplicationWait deadlineWait =
            ConsumerReactorProgress.decideApplicationWait(List.of(manager), 42L);

        assertTrue(deadlineWait.shortens(eventWait));
    }
}
