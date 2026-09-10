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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.ToLongFunction;

/**
 * Selects which request managers the network thread polls during normal operation. Managers still
 * own protocol state and request construction; this class owns eligibility, subscriptions and deadlines.
 * {@link ConsumerNetworkThread} drives {@link #pollReady(long, ToLongFunction)} and performs network I/O
 * after the selected managers have returned their requests.
 *
 * <p>All access, including signal callbacks and close, belongs to the network thread. A signal makes a
 * manager eligible; it does not execute that manager or wake a blocked network selector. Input from
 * another thread must use the application's event/wakeup handoff before changing this scheduler.
 * This class does not replace the separate scan of {@link RequestManager#maximumTimeToWait(long)}.
 */
public final class RequestManagerScheduler implements AutoCloseable {
    // Manager identity is stable even if an implementation defines value-based equality.
    private final Map<RequestManager, Entry> entries = new IdentityHashMap<>();
    // Bit indexes preserve registration order; eligibility does not introduce a new priority order.
    private final List<Entry> entriesByOrder = new ArrayList<>();
    // A bit grants one poll attempt. Initial registration, expiry and input publication set it.
    private final BitSet ready = new BitSet();
    // Managers without a condition retain full-pass polling. Their numeric delay still bounds I/O.
    private final BitSet legacy = new BitSet();
    // At most one active deadline per manager. Order breaks ties so equal deadlines remain distinct.
    // Signal-only waits are absent; anyOf registrations contribute their earliest deadline.
    private final TreeSet<Waiting> timers = new TreeSet<>(Comparator
            .comparingLong((Waiting wait) -> wait.deadline).thenComparingInt(wait -> wait.entry.order));

    /** Register in the consumer's existing manager order and make new entries eligible once. */
    public void registerManagers(List<RequestManager> managers) {
        for (RequestManager manager : managers)
            entry(manager);
    }

    private Entry entry(RequestManager manager) {
        Entry result = entries.get(manager);
        if (result == null) {
            result = new Entry(manager, entries.size());
            entries.put(manager, result);
            entriesByOrder.add(result);
            ready.set(result.order);
        }
        return result;
    }

    /**
     * Poll one fixed snapshot of ready and legacy managers. Freeze each selected manager's local
     * batch before polling any of them, so a cross-manager handoff cannot consume newly queued work
     * merely because its recipient occurs later in this snapshot. Newly eligible managers outside
     * the snapshot wait for the next pass; this is not an immutable snapshot of all protocol state.
     *
     * @param nowMs time used for this scheduling pass
     * @param admit stages each result's requests and returns its numeric I/O delay; production passes
     *              {@link NetworkClientDelegate#addAll(NetworkClientDelegate.PollResult)}
     * @return the earliest numeric delay or registered deadline, or zero when work remains ready
     */
    public long pollReady(long nowMs, ToLongFunction<NetworkClientDelegate.PollResult> admit) {
        expire(nowMs);
        if (ready.isEmpty() && legacy.isEmpty())
            return remainingMs(nowMs);
        long delay = Long.MAX_VALUE;
        BitSet snapshot = (BitSet) ready.clone();
        snapshot.or(legacy);
        for (int order = snapshot.nextSetBit(0); order >= 0; order = snapshot.nextSetBit(order + 1))
            entriesByOrder.get(order).manager.onPollBatchStart();
        for (int order = snapshot.nextSetBit(0); order >= 0; order = snapshot.nextSetBit(order + 1)) {
            NetworkClientDelegate.PollResult result = pollPrepared(entriesByOrder.get(order).manager, nowMs);
            if (result != null)
                delay = Math.min(delay, admit.applyAsLong(result));
        }
        return Math.min(delay, remainingMs(nowMs));
    }

    /** Targeted polling for tests; production drains pollReady instead of visiting waiting managers. */
    public NetworkClientDelegate.PollResult poll(RequestManager manager, long nowMs) {
        manager.onPollBatchStart();
        return pollPrepared(manager, nowMs);
    }

    /** Consume eligibility, poll the owner, then replace its previous one-shot wait. */
    private NetworkClientDelegate.PollResult pollPrepared(RequestManager manager, long nowMs) {
        expire(nowMs);
        Entry entry = entry(manager);
        if (!ready.get(entry.order) && !legacy.get(entry.order))
            return null;
        ready.clear(entry.order);
        if (entry.waiting != null)
            entry.waiting.cancel();
        final NetworkClientDelegate.PollResult result;
        try {
            result = manager.poll(nowMs);
        } catch (RuntimeException | Error e) {
            ready.set(entry.order);
            throw e;
        }
        if (result.nextPollCondition == null) {
            legacy.set(entry.order);
        } else {
            legacy.clear(entry.order);
            if (entry.waiting == null)
                entry.waiting = new Waiting(entry);
            Waiting wait = entry.waiting;
            wait.reset();
            try {
                result.nextPollCondition.register(wait);
            } catch (RuntimeException | Error e) {
                wait.cancel();
                ready.set(entry.order);
                throw e;
            }
            // subscribe() may have synchronously observed a publication. Finish collecting all
            // cancellation handles before allowing wake() to detach this registration.
            wait.armed = true;
            if (wait.signalled || (wait.hasDeadline && wait.deadline <= nowMs))
                wait.wake();
            else if (wait.hasDeadline)
                timers.add(wait);
        }
        return result;
    }

    /** Inspect only the ready queue and earliest timer, never all waiting conditions. */
    public long remainingMs(long nowMs) {
        expire(nowMs);
        if (!ready.isEmpty())
            return 0;
        if (timers.isEmpty())
            return Long.MAX_VALUE;
        long remaining = timers.first().deadline - nowMs;
        return remaining < 0 ? Long.MAX_VALUE : remaining;
    }

    private void expire(long nowMs) {
        while (!timers.isEmpty() && timers.first().deadline <= nowMs)
            timers.first().wake();
    }

    @Override
    public void close() {
        for (Entry entry : entries.values()) {
            if (entry.waiting != null)
                entry.waiting.cancel();
        }
        entries.clear();
        entriesByOrder.clear();
        ready.clear();
        legacy.clear();
        timers.clear();
    }

    /** Stable manager/index association; retains one lazily allocated Waiting across activations. */
    private static final class Entry {
        private final RequestManager manager;
        private final int order;
        private Waiting waiting;

        private Entry(RequestManager manager, int order) {
            this.manager = manager;
            this.order = order;
        }
    }

    /**
     * One active registration for one manager. Any signal or the earliest deadline wins, cancels
     * the other subscriptions and marks the owner ready. Cancellation keeps this object reusable;
     * it does not retain the old subscriptions or leave an old timer in the index.
     */
    private final class Waiting implements NextPollCondition.Registration, Runnable {
        private final Entry entry;
        // Keep the common single-signal case free of an additional cancellation-handle list.
        private Runnable unsubscribe;
        private List<Runnable> additionalSubscriptions;
        private long deadline = Long.MAX_VALUE;
        // Long.MAX_VALUE is also a valid deadline, so it cannot stand in for "no deadline".
        private boolean hasDeadline;
        // active covers registration construction too; armed permits actual detach-and-ready work.
        private boolean armed;
        // Remembers publication during construction until all handles have been registered.
        private boolean signalled;
        private boolean active;

        private Waiting(Entry entry) {
            this.entry = entry;
        }

        /** Previous subscriptions and the timer must be detached before changing the timer key. */
        private void reset() {
            assert !active;
            deadline = Long.MAX_VALUE;
            hasDeadline = false;
            armed = false;
            signalled = false;
            additionalSubscriptions = null;
            active = true;
        }

        @Override
        public void onSignal(NextPollCondition.Signal signal, long observed) {
            Runnable cancellation = signal.subscribe(observed, this);
            if (unsubscribe == null) {
                unsubscribe = cancellation;
            } else {
                if (additionalSubscriptions == null)
                    additionalSubscriptions = new ArrayList<>();
                additionalSubscriptions.add(cancellation);
            }
        }

        @Override
        public void onDeadline(long deadlineMs) {
            hasDeadline = true;
            deadline = Math.min(deadline, deadlineMs);
        }

        @Override
        public void run() {
            wake();
        }

        private void wake() {
            if (!active)
                return;
            if (!armed) {
                signalled = true;
                return;
            }
            cancel();
            ready.set(entry.order);
        }

        private void cancel() {
            if (!active)
                return;
            active = false;
            if (unsubscribe != null) {
                unsubscribe.run();
                unsubscribe = null;
            }
            if (additionalSubscriptions != null) {
                for (Runnable detach : additionalSubscriptions)
                    detach.run();
                additionalSubscriptions.clear();
                additionalSubscriptions = null;
            }
            // Remove using the old deadline key before reset() mutates it for a later registration.
            timers.remove(this);
            // The entry retains this inactive slot for its next condition registration.
        }
    }
}
