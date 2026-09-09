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
package org.apache.kafka.clients.consumer.ng.loop;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Coalesces wake-ups of the event loop. Other threads call {@link #wakeupIfParked()} after handing the loop work;
 * the underlying wake-up (typically {@code Selector.wakeup()}, a system call) is issued at most once per park
 * cycle, however many callers there are.
 *
 * <p>Protocol for the loop thread:
 * <pre>
 *   while (running) {
 *       signal.markRunning();
 *       ... do work ...
 *       if (signal.prepareToPark(hasPendingWork)) {
 *           blockingSelect(timeout);      // may return early if a wake-up was issued
 *       }
 *   }
 * </pre>
 * {@link #prepareToPark(BooleanSupplier)} publishes the PARKED state <em>before</em> re-checking for pending work,
 * and {@link #wakeupIfParked()} is called <em>after</em> the work was enqueued. Under sequential consistency of the
 * atomic operations, at least one side observes the other, so a wake-up can never be lost.
 */
public final class LoopSignal {

    private static final int RUNNING = 0;
    private static final int PARKED = 1;

    private final AtomicInteger state = new AtomicInteger(RUNNING);
    private final AtomicLong wakeupsIssued = new AtomicLong();
    private final Runnable wakeup;

    /** @param wakeup the (possibly expensive) operation that interrupts the loop's blocking wait */
    public LoopSignal(Runnable wakeup) {
        this.wakeup = Objects.requireNonNull(wakeup, "wakeup");
    }

    /** Loop thread: declares that the loop is processing work and needs no wake-up. */
    public void markRunning() {
        state.set(RUNNING);
    }

    /**
     * Loop thread: declares that the loop is about to block, then re-checks for work that may have been enqueued
     * in the meantime.
     *
     * @return {@code true} if the loop may block; {@code false} if work is pending and the loop must not block
     */
    public boolean prepareToPark(BooleanSupplier hasPendingWork) {
        state.set(PARKED);
        if (hasPendingWork.getAsBoolean()) {
            state.set(RUNNING);
            return false;
        }
        return true;
    }

    /**
     * Any thread: wakes the loop if (and only if) it is parked. The first caller in a park cycle flips the state to
     * RUNNING and issues the wake-up; later callers in the same cycle do nothing.
     *
     * @return {@code true} if this call issued the wake-up
     */
    public boolean wakeupIfParked() {
        if (state.get() == PARKED && state.compareAndSet(PARKED, RUNNING)) {
            wakeupsIssued.incrementAndGet();
            wakeup.run();
            return true;
        }
        return false;
    }

    /** @return {@code true} if the loop has declared itself parked and nobody has woken it yet */
    public boolean isParked() {
        return state.get() == PARKED;
    }

    /** @return total number of wake-ups issued (for metrics and tests) */
    public long wakeupsIssued() {
        return wakeupsIssued.get();
    }
}
