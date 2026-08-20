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

import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class ReactorScheduleTest {

    @Test
    public void testNoManagerDeadlineAllowsUnboundedReactorSchedule() {
        ReactorSchedule schedule = ReactorSchedule.merge(List.of(), 42L);

        assertEquals(Long.MAX_VALUE, schedule.timeoutMs());
        assertEquals(42L, schedule.decidedAtMs());
        assertEquals(NextReconcile.Type.ON_EVENT, schedule.nextReconcileType());
        assertTrue(schedule.source().isEmpty());
    }

    @Test
    public void testEarliestManagerDeadlineWins() {
        CoordinatorRequestManager slowerManager = mock(CoordinatorRequestManager.class);
        ConsumerHeartbeatRequestManager limitingManager = mock(ConsumerHeartbeatRequestManager.class);

        ReactorSchedule schedule = ReactorSchedule.merge(
            List.of(
                ManagerReconcileResult.of(
                    slowerManager,
                    EMPTY,
                    NextReconcile.atDeadlineAfter(42L, 100L)
                ),
                ManagerReconcileResult.of(
                    limitingManager,
                    EMPTY,
                    NextReconcile.atDeadlineAfter(42L, 25L)
                )
            ),
            42L
        );

        assertEquals(25L, schedule.timeoutMs());
        assertEquals(67L, schedule.deadlineAtMs());
        assertEquals(NextReconcile.Type.AT_DEADLINE, schedule.nextReconcileType());
        assertEquals(ConsumerHeartbeatRequestManager.class.getSimpleName(), schedule.source().orElseThrow());
    }

    @Test
    public void testOneManagerMayReportMultipleReconcileConditions() {
        FetchRequestManager fetchRequestManager = mock(FetchRequestManager.class);

        ReactorSchedule schedule = ReactorSchedule.merge(
            List.of(ManagerReconcileResult.of(
                fetchRequestManager,
                EMPTY,
                NextReconcile.onEvent(),
                NextReconcile.atDeadlineAfter(42L, 100L),
                NextReconcile.atDeadlineAfter(42L, 25L)
            )),
            42L
        );

        assertEquals(67L, schedule.deadlineAtMs());
        assertEquals(FetchRequestManager.class.getSimpleName(), schedule.source().orElseThrow());
    }

    @Test
    public void testBlockedManagerStateChangeDoesNotMaskAnotherManagerDeadline() {
        CoordinatorRequestManager coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        ConsumerHeartbeatRequestManager heartbeatRequestManager = mock(ConsumerHeartbeatRequestManager.class);
        NextReconcile coordinatorDeadline = NextReconcile.atDeadlineAfter(42L, 25L);
        NextReconcile heartbeatDeadline = NextReconcile.atDeadlineAfter(42L, 50L);

        ReactorSchedule beforeCoordinatorStateChange = ReactorSchedule.merge(
            List.of(
                ManagerReconcileResult.of(coordinatorRequestManager, EMPTY, coordinatorDeadline),
                ManagerReconcileResult.of(heartbeatRequestManager, EMPTY, heartbeatDeadline)
            ),
            42L
        );
        ReactorSchedule afterCoordinatorStateChange = ReactorSchedule.merge(
            List.of(
                ManagerReconcileResult.of(coordinatorRequestManager, EMPTY, NextReconcile.onEvent()),
                ManagerReconcileResult.of(heartbeatRequestManager, EMPTY, heartbeatDeadline)
            ),
            52L
        );

        assertEquals(25L, beforeCoordinatorStateChange.timeoutMs());
        assertEquals(CoordinatorRequestManager.class.getSimpleName(), beforeCoordinatorStateChange.source().orElseThrow());
        assertEquals(40L, afterCoordinatorStateChange.timeoutMs());
        assertEquals(92L, afterCoordinatorStateChange.deadlineAtMs());
        assertEquals(ConsumerHeartbeatRequestManager.class.getSimpleName(), afterCoordinatorStateChange.source().orElseThrow());
    }

    @Test
    public void testReactorScheduleSubtractsElapsedTime() {
        RequestManager manager = mock(RequestManager.class);
        ReactorSchedule schedule = ReactorSchedule.merge(
            List.of(ManagerReconcileResult.of(
                manager,
                EMPTY,
                NextReconcile.atDeadlineAfter(42L, 25L)
            )),
            42L
        );

        assertEquals(15L, schedule.remainingMs(52L));
        assertEquals(0L, schedule.remainingMs(67L));
    }

    @Test
    public void testFiniteDeadlineShortensEventWait() {
        RequestManager manager = mock(RequestManager.class);
        ReactorSchedule eventWait = ReactorSchedule.merge(List.of(), 42L);
        ReactorSchedule deadlineWait = ReactorSchedule.merge(
            List.of(ManagerReconcileResult.of(
                manager,
                EMPTY,
                NextReconcile.atDeadlineAfter(42L, 25L)
            )),
            42L
        );

        assertTrue(deadlineWait.shortens(eventWait));
    }
}
