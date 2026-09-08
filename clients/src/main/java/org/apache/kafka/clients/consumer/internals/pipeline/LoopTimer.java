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
package org.apache.kafka.clients.consumer.internals.pipeline;

import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Timer used by the consumer event loop. Each timed task owns its own deadline; there is no global
 * "minimum time to wait" computed across components. The loop asks {@link #timeToNextMs(long)} for how long it may
 * block and calls {@link #runExpired(long)} after waking up.
 *
 * <p>Every delay is clamped to at least {@link #MIN_DELAY_MS}. This is the structural guard against the busy loops
 * of the previous design: a component that asks to be re-run "immediately" is re-run at most once per millisecond,
 * and only that component is re-run, not every component.
 *
 * <p>Not thread-safe: all methods must be called on the loop thread.
 */
public final class LoopTimer {

    /** Smallest delay a task can be scheduled with, in milliseconds. */
    public static final long MIN_DELAY_MS = 1;

    /** Returned by {@link #timeToNextMs(long)} when nothing is scheduled. */
    public static final long NO_DEADLINE = Long.MAX_VALUE;

    /** A scheduled task that can be cancelled before it runs. */
    public interface Handle {

        /** @return {@code true} if the task was pending and is now cancelled */
        boolean cancel();

        /** @return {@code true} while the task is scheduled and neither run nor cancelled */
        boolean isPending();
    }

    private static final class Entry implements Comparable<Entry>, Handle {
        private final LoopTimer owner;
        private final long deadlineMs;
        private final long sequence;
        private final Runnable task;
        private boolean done;

        private Entry(LoopTimer owner, long deadlineMs, long sequence, Runnable task) {
            this.owner = owner;
            this.deadlineMs = deadlineMs;
            this.sequence = sequence;
            this.task = task;
        }

        @Override
        public int compareTo(Entry other) {
            int byDeadline = Long.compare(deadlineMs, other.deadlineMs);
            return byDeadline != 0 ? byDeadline : Long.compare(sequence, other.sequence);
        }

        @Override
        public boolean cancel() {
            if (done)
                return false;
            done = true;
            owner.cancelledInQueue++;
            owner.maybeCompact();
            return true;
        }

        @Override
        public boolean isPending() {
            return !done;
        }
    }

    private final PriorityQueue<Entry> queue = new PriorityQueue<>();
    private long nextSequence;
    private int cancelledInQueue;
    private long lastRunTimeMs;

    /** @return the {@code nowMs} passed to the most recent {@link #runExpired(long)}; tasks use it instead of re-reading the clock */
    public long lastRunTimeMs() {
        return lastRunTimeMs;
    }

    /**
     * Schedules {@code task} to run once {@code delayMs} milliseconds (clamped to at least {@link #MIN_DELAY_MS})
     * after {@code nowMs}.
     */
    public Handle schedule(long nowMs, long delayMs, Runnable task) {
        Objects.requireNonNull(task, "task");
        long delay = Math.max(MIN_DELAY_MS, delayMs);
        long deadline = delay >= NO_DEADLINE - nowMs ? NO_DEADLINE : nowMs + delay;
        Entry entry = new Entry(this, deadline, nextSequence++, task);
        queue.add(entry);
        return entry;
    }

    /** @return the earliest pending deadline, or {@link #NO_DEADLINE} if nothing is scheduled */
    public long nextDeadlineMs() {
        purgeCancelledHead();
        Entry head = queue.peek();
        return head == null ? NO_DEADLINE : head.deadlineMs;
    }

    /** @return how long the loop may block from {@code nowMs}; {@code 0} if a task is already due */
    public long timeToNextMs(long nowMs) {
        long next = nextDeadlineMs();
        if (next == NO_DEADLINE)
            return NO_DEADLINE;
        return Math.max(0, next - nowMs);
    }

    /**
     * Runs every task whose deadline is at or before {@code nowMs}, in deadline order. Tasks scheduled while running
     * are not run in the same pass even if already due, so a task that reschedules itself with a zero delay cannot
     * starve the rest of the loop.
     *
     * @return number of tasks run
     */
    public int runExpired(long nowMs) {
        lastRunTimeMs = nowMs;
        long lastSequenceToRun = nextSequence - 1;
        int ran = 0;
        while (true) {
            Entry head = queue.peek();
            if (head == null || head.deadlineMs > nowMs || head.sequence > lastSequenceToRun)
                break;
            queue.poll();
            if (head.done) {
                cancelledInQueue--;
                continue;
            }
            head.done = true;
            ran++;
            head.task.run();
        }
        return ran;
    }

    /** @return number of pending (not cancelled, not run) tasks */
    public int pendingCount() {
        return queue.size() - cancelledInQueue;
    }

    /** Rebuilds the queue without cancelled entries once they dominate it, so cancellations cannot leak memory. */
    private void maybeCompact() {
        if (cancelledInQueue < 64 || cancelledInQueue * 2 < queue.size())
            return;
        PriorityQueue<Entry> live = new PriorityQueue<>();
        for (Entry entry : queue) {
            if (!entry.done)
                live.add(entry);
        }
        queue.clear();
        queue.addAll(live);
        cancelledInQueue = 0;
    }

    private void purgeCancelledHead() {
        while (true) {
            Entry head = queue.peek();
            if (head == null || !head.done)
                return;
            queue.poll();
            cancelledInQueue--;
        }
    }
}
