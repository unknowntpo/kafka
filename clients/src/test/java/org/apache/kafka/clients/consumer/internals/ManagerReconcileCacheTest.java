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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class ManagerReconcileCacheTest {

    @Test
    public void testDeliveredPersistentZeroCompatibilityDeadlineIsSuppressed() {
        RequestManager manager = mock(RequestManager.class);
        ManagerReconcileCache cache = new ManagerReconcileCache();

        cache.update(compatibilityResult(manager, 42L, 0L), 42L);
        ReactorSchedule expired = schedule(cache, 42L);
        assertEquals(42L, expired.deadlineAtMs());
        cache.markDeadlineDelivered(expired);

        cache.update(compatibilityResult(manager, 52L, 0L), 52L);
        assertEquals(NextReconcile.Type.ON_EVENT, schedule(cache, 52L).nextReconcileType());

        cache.update(compatibilityResult(manager, 52L, 10L), 52L);
        assertEquals(62L, schedule(cache, 52L).deadlineAtMs());
    }

    @Test
    public void testIncrementalUpdateRetainsUnaffectedManagerDeadline() {
        RequestManager first = mock(RequestManager.class);
        RequestManager second = mock(RequestManager.class);
        ManagerReconcileCache cache = new ManagerReconcileCache();

        cache.update(compatibilityResult(first, 0L, 100L), 0L);
        cache.update(compatibilityResult(second, 0L, 50L), 0L);
        ReactorSchedule secondExpiresFirst = schedule(cache, 10L);
        assertEquals(50L, secondExpiresFirst.deadlineAtMs());

        cache.markDeadlineDelivered(secondExpiresFirst);
        cache.update(compatibilityResult(second, 50L, 0L), 50L);
        cache.update(compatibilityResult(first, 50L, 100L), 50L);

        ReactorSchedule firstStillPending = schedule(cache, 50L);
        assertEquals(100L, firstStillPending.deadlineAtMs());
        assertEquals(first, firstStillPending.sourceManager().orElseThrow());
    }

    @Test
    public void testNegativeCompatibilityDelayIsClampedToImmediate() {
        NextReconcile next = NextReconcile.atCompatibilityDeadlineAfter(42L, -1L);

        assertEquals(42L, next.deadlineAtMs());
        assertEquals(0L, next.remainingMs(42L));
    }

    @Test
    public void testEqualCompatibilityDeadlinesAreDeliveredTogether() {
        RequestManager first = mock(RequestManager.class);
        RequestManager second = mock(RequestManager.class);
        ManagerReconcileCache cache = new ManagerReconcileCache();

        cache.update(compatibilityResult(first, 42L, 0L), 42L);
        cache.update(compatibilityResult(second, 42L, 0L), 42L);
        cache.markDeadlineDelivered(schedule(cache, 42L));
        cache.update(compatibilityResult(first, 52L, 0L), 52L);
        cache.update(compatibilityResult(second, 52L, 0L), 52L);

        assertEquals(NextReconcile.Type.ON_EVENT, schedule(cache, 52L).nextReconcileType());
    }

    @Test
    public void testLegacyPollDeadlineDoesNotDriftAcrossEarlyReconciliation() {
        RequestManager manager = mock(RequestManager.class);
        ManagerReconcileCache cache = new ManagerReconcileCache();

        cache.update(pollResult(manager, 100L), 0L);
        cache.update(pollResult(manager, 100L), 10L);

        ReactorSchedule beforeDeadline = schedule(cache, 10L);
        assertEquals(100L, beforeDeadline.reactorDeadlineAtMs());
        assertEquals(Long.MAX_VALUE, beforeDeadline.deadlineAtMs());

        cache.update(pollResult(manager, 100L), 100L);
        assertEquals(200L, schedule(cache, 100L).reactorDeadlineAtMs());
    }

    @Test
    public void testDeliveringAnotherManagerDeadlineDoesNotReactivateEarlierDeadline() {
        RequestManager first = mock(RequestManager.class);
        RequestManager second = mock(RequestManager.class);
        ManagerReconcileCache cache = new ManagerReconcileCache();

        cache.update(nativeResult(first, 10L), 0L);
        cache.update(nativeResult(second, 20L), 0L);
        cache.markDeadlineDelivered(schedule(cache, 10L));
        assertEquals(20L, schedule(cache, 10L).deadlineAtMs());

        cache.markDeadlineDelivered(schedule(cache, 20L));
        assertEquals(NextReconcile.Type.ON_EVENT, schedule(cache, 20L).nextReconcileType());
    }

    private static ManagerReconcileResult compatibilityResult(final RequestManager manager,
                                                               final long currentTimeMs,
                                                               final long delayMs) {
        return ManagerReconcileResult.scheduleOnly(
            manager,
            NextReconcile.atCompatibilityDeadlineAfter(currentTimeMs, delayMs)
        );
    }

    private static ManagerReconcileResult pollResult(final RequestManager manager,
                                                      final long delayMs) {
        return ManagerReconcileResult.of(
            manager,
            new NetworkClientDelegate.PollResult(delayMs),
            NextReconcile.onEvent()
        );
    }

    private static ManagerReconcileResult nativeResult(final RequestManager manager,
                                                       final long deadlineAtMs) {
        return ManagerReconcileResult.scheduleOnly(
            manager,
            NextReconcile.atDeadlineAfter(0L, deadlineAtMs)
        );
    }

    private static ReactorSchedule schedule(final ManagerReconcileCache cache,
                                            final long currentTimeMs) {
        return ReactorSchedule.merge(cache.scheduleResults(), currentTimeMs);
    }
}
