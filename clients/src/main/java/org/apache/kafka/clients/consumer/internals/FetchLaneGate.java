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

import java.util.concurrent.ConcurrentHashMap;

/**
 * Lets a fetch connection that stopped because too much of its data is buffered start again as soon as the
 * application thread has made room, without a third thread in between.
 *
 * <p>This class is the mechanism only. It does not know what "room" means: the connection passes that rule in
 * as {@link Waiter#hasRoom()}, together with the task that starts the connection.
 *
 * <p>Two threads change it, and neither waits for the other:
 * <ul>
 *   <li>The receiver thread of a connection calls {@link #waitForRoom} when its rule says "no room".</li>
 *   <li>The application thread calls {@link #roomMayHaveAppeared} after buffered data has left the buffer.</li>
 * </ul>
 *
 * <p>No connection is forgotten, because each thread writes its own state first and reads the other's state
 * second: the receiver thread registers and then asks the rule again; the application thread removes the data
 * and then reads the registrations. Whatever the interleaving, at least one of them sees the other.
 *
 * <p>Whoever takes a waiter out, in {@link #waitForRoom} or {@link #roomMayHaveAppeared}, asks the connection
 * to look again, so that happens once per wait. It is a hint, not a decision: the connection decides on its
 * own thread whether it sends, so a hint that turns out to be unnecessary does no harm.
 */
final class FetchLaneGate {

    /** A connection that waits: its rule for "the wait is over", and how to reach its own thread. */
    interface Waiter {
        /**
         * Whether the connection should look again: there is room now, or it has nothing to wait for any
         * more. Safe to call from any thread; changes nothing; does not throw.
         */
        boolean hasRoom();

        /** Asks the connection to decide again, on its own thread. Does not block and does not throw. */
        void lookAgain();
    }

    private final ConcurrentHashMap<Integer, Waiter> waiting = new ConcurrentHashMap<>();

    /**
     * Called on the receiver thread of connection {@code id} when its rule said "no room". The connection
     * now waits, unless room appeared in the meantime: then it is asked to look again here.
     */
    void waitForRoom(int id, Waiter waiter) {
        waiting.put(id, waiter);
        // Look again after registering. Without this, room that appeared between the caller's first look
        // and the line above would reach nobody.
        if (waiter.hasRoom() && waiting.remove(id, waiter))
            waiter.lookAgain();
    }

    /** Called on the receiver thread of connection {@code id} when it decides again: an older wait is over. */
    void forget(int id) {
        waiting.remove(id);
    }

    /** For diagnostics and tests. */
    boolean isWaiting(int id) {
        return waiting.containsKey(id);
    }

    /**
     * Called on the application thread after buffered data has left the buffer. Returns how many connections
     * were asked to look again (for tests).
     */
    int roomMayHaveAppeared() {
        if (waiting.isEmpty())
            return 0;
        int asked = 0;
        for (Integer id : waiting.keySet()) {
            Waiter waiter = waiting.get(id);
            if (waiter != null && waiter.hasRoom() && waiting.remove(id, waiter)) {
                waiter.lookAgain();
                asked++;
            }
        }
        return asked;
    }
}
