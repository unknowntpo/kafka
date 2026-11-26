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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two threads of {@link FetchLaneGate} are played by one test thread, so that every interleaving is exact.
 * "Receiver" is the thread of a stopped connection; "application" is the thread that makes room.
 */
public class FetchLaneGateTest {

    private static final int LANE = 7;

    private final FetchLaneGate gate = new FetchLaneGate();
    private final AtomicInteger restarts = new AtomicInteger();
    private boolean room;

    private FetchLaneGate.Waiter waiter(BooleanSupplier hasRoom) {
        return new FetchLaneGate.Waiter() {
            @Override
            public boolean hasRoom() {
                return hasRoom.getAsBoolean();
            }

            @Override
            public void lookAgain() {
                restarts.incrementAndGet();
            }
        };
    }

    @Test
    public void testApplicationStartsAWaitingConnectionOnceWhenThereIsRoom() {
        gate.waitForRoom(LANE, waiter(() -> room));
        assertTrue(gate.isWaiting(LANE), "no room: the connection waits");
        assertEquals(0, restarts.get());

        assertEquals(0, gate.roomMayHaveAppeared(), "data left the buffer, but not enough");
        assertTrue(gate.isWaiting(LANE));

        room = true;
        assertEquals(1, gate.roomMayHaveAppeared());
        assertEquals(1, restarts.get());
        assertFalse(gate.isWaiting(LANE));

        assertEquals(0, gate.roomMayHaveAppeared(), "a connection is started once per wait");
        assertEquals(1, restarts.get());
    }

    /**
     * The receiver looked and saw no room. Before it registers, the application makes room and finds nobody
     * waiting. The second look inside waitForRoom must catch this, or the connection would wait forever.
     */
    @Test
    public void testRoomThatAppearsBeforeTheConnectionRegistersIsNotLost() {
        // the receiver's first look: no room (not shown; it is the reason it calls waitForRoom)
        room = true;                                    // the application consumes the data ...
        assertEquals(0, gate.roomMayHaveAppeared());    // ... and finds nobody waiting

        gate.waitForRoom(LANE, waiter(() -> room));
        assertEquals(1, restarts.get(), "the second look starts the connection");
        assertFalse(gate.isWaiting(LANE));
    }

    /**
     * The application makes room while the receiver is between registering and its second look. Both see
     * room; exactly one of them may start the connection.
     */
    @Test
    public void testOnlyOneThreadStartsTheConnectionWhenBothSeeRoom() {
        AtomicInteger looks = new AtomicInteger();
        FetchLaneGate.Waiter[] self = new FetchLaneGate.Waiter[1];
        self[0] = waiter(() -> {
            // the first call is the receiver's second look; the application runs in the middle of it
            if (looks.getAndIncrement() == 0)
                assertEquals(1, gate.roomMayHaveAppeared());
            return true;
        });

        gate.waitForRoom(LANE, self[0]);
        assertEquals(1, restarts.get(), "the application took the start, so the receiver must not start it again");
        assertFalse(gate.isWaiting(LANE));
    }

    @Test
    public void testAConnectionThatDecidesAgainNoLongerWaits() {
        gate.waitForRoom(LANE, waiter(() -> room));
        gate.forget(LANE);   // somebody else started the connection, for example after a pause

        room = true;
        assertEquals(0, gate.roomMayHaveAppeared());
        assertEquals(0, restarts.get());
    }

    @Test
    public void testConnectionsWaitIndependently() {
        boolean[] roomOf = new boolean[2];
        gate.waitForRoom(0, waiter(() -> roomOf[0]));
        gate.waitForRoom(1, waiter(() -> roomOf[1]));

        roomOf[1] = true;
        assertEquals(1, gate.roomMayHaveAppeared());
        assertTrue(gate.isWaiting(0));
        assertFalse(gate.isWaiting(1));
    }
}
