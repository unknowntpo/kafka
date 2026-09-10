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

import org.apache.kafka.common.message.FetchResponseData;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/** Two-thread lease handoff prototype. All Kafka-facing mutations belong to background. */
final class FetchBridge implements AutoCloseable {
    interface Transport extends AutoCloseable {
        void send(long offset, Consumer<FetchResponseData.PartitionData> completion);
        void poll(boolean mayBlock) throws Exception;
        void wakeup(); // Thread-safe; must preserve a wakeup issued just before blocking poll.
        void cancel(); // Owner-only: no callback after return.
        void close() throws Exception;
    }
    interface IdleHook { void beforePoll(); }
    private static final class Ring<T> {
        private final Object[] cells;
        private volatile long head;
        private volatile long tail;
        Ring(int capacity) { cells = new Object[capacity]; }
        boolean empty() { return head == tail; }
        boolean offer(T value) {
            long t = tail;
            if (t - head == cells.length) return false;
            cells[(int) (t % cells.length)] = value;
            tail = t + 1;
            return true;
        }
        @SuppressWarnings("unchecked") T poll() {
            long h = head;
            if (h == tail) return null;
            T value = (T) cells[(int) (h % cells.length)];
            cells[(int) (h % cells.length)] = null;
            head = h + 1;
            return value;
        }
    }
    private static final class Command {
        final PrefetchWindow.Lease release;
        final long offset;
        Command(PrefetchWindow.Lease release, long offset) { this.release = release; this.offset = offset; }
    }
    private final Thread app = Thread.currentThread();
    private final Thread background;
    private final Transport transport;
    private final IdleHook idleHook;
    private final Ring<Command> commands;
    private final Ring<PrefetchWindow.Lease> completions;
    private final Set<PrefetchWindow.Lease> held = Collections.newSetFromMap(new IdentityHashMap<>());
    private final AtomicBoolean closing = new AtomicBoolean();
    private final AtomicBoolean seekPending = new AtomicBoolean();
    private final AtomicBoolean appSleeping = new AtomicBoolean();
    private volatile long appGeneration;
    private volatile Throwable failure;
    private volatile boolean terminated;
    private volatile boolean closeObserved;
    private volatile long retained;
    private volatile long received;
    private final long budget;
    private final int slots;
    private PrefetchWindow window; // Background-only from construction through final release.
    private PrefetchWindow.Request pending;

    FetchBridge(long budget, int slots, Transport transport, IdleHook idleHook) {
        if (budget <= 0 || slots <= 0 || slots > 1024) throw new IllegalArgumentException();
        this.budget = budget;
        this.slots = slots;
        this.transport = transport;
        this.idleHook = idleHook;
        // At most slots lease releases + one admitted seek, even when data CQ is full.
        commands = new Ring<>(slots + 1);
        completions = new Ring<>(slots);
        background = new Thread(this::run, "fetch-bridge-background");
        background.setDaemon(true);
        background.start();
    }
    private void checkApp() {
        if (Thread.currentThread() != app) throw new IllegalStateException("Single application owner required");
        if (failure != null) throw new IllegalStateException("Background failed", failure);
    }
    boolean trySeek(long offset) {
        checkApp();
        if (offset < 0) throw new IllegalArgumentException();
        if (closing.get() || !seekPending.compareAndSet(false, true)) return false;
        // Publish fence before the command. App cannot take a lease in the middle of its own call.
        appGeneration = Math.incrementExact(appGeneration);
        enqueue(new Command(null, offset));
        return true;
    }
    private void enqueue(Command command) {
        if (!commands.offer(command)) throw new AssertionError("Reserved control capacity missing");
        transport.wakeup();
    }
    boolean valid(PrefetchWindow.Lease lease) {
        checkApp();
        return !closing.get() && held.contains(lease) && lease.generation == appGeneration;
    }
    PrefetchWindow.Lease take() {
        checkApp();
        // Bounded drain: a fast producer must not keep the app inside stale-data cleanup forever.
        for (int i = 0; i < slots; i++) {
            PrefetchWindow.Lease lease = completions.poll();
            if (lease == null) return null;
            held.add(lease);
            if (valid(lease)) return lease;
            release(lease);
        }
        return null;
    }
    void release(PrefetchWindow.Lease lease) {
        checkApp();
        if (!held.remove(lease)) throw new IllegalStateException("Unknown or duplicate release");
        enqueue(new Command(lease, 0));
    }
    PrefetchWindow.Lease await(long timeoutNanos) {
        checkApp();
        if (timeoutNanos < 0 || timeoutNanos > 60_000_000_000L) throw new IllegalArgumentException();
        long deadline = System.nanoTime() + timeoutNanos;
        for (;;) {
            PrefetchWindow.Lease lease = take();
            if (lease != null) return lease;
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0 || terminated || closing.get()) return null;
            appSleeping.set(true);
            lease = take();
            if (lease != null || terminated || closing.get()) {
                appSleeping.set(false);
                return lease;
            }
            LockSupport.parkNanos(this, remaining);
            appSleeping.set(false);
            if (Thread.currentThread().isInterrupted()) throw new IllegalStateException("App interrupted");
        }
    }
    private void notifyApp() {
        if (appSleeping.getAndSet(false)) LockSupport.unpark(app);
    }
    private void response(PrefetchWindow.Request request, FetchResponseData.PartitionData records) {
        PrefetchWindow.Lease lease = window.complete(request, records);
        pending = null;
        if (lease != null) {
            if (!completions.offer(lease)) throw new AssertionError("Reserved lease slot missing");
            received++;
            notifyApp();
        }
    }
    private void run() {
        try {
            window = new PrefetchWindow(budget, slots, 0);
            for (;;) {
                for (int i = 0; i < 64; i++) {
                    Command command = commands.poll();
                    if (command == null) break;
                    if (command.release != null) window.release(command.release);
                    else {
                        if (!closeObserved) window.seek(command.offset);
                        seekPending.set(false);
                    }
                }
                if (closing.get() && !closeObserved) {
                    window.close();
                    transport.cancel();
                    if (pending != null) { window.fail(pending); pending = null; }
                    closeObserved = true;
                    notifyApp();
                }
                retained = window.retained();
                if (closeObserved && window.outstandingLeases() == 0 && commands.empty()) return;
                if (window.ready() && !closing.get()) {
                    PrefetchWindow.Request request = window.begin();
                    pending = request;
                    transport.send(request.offset, records -> response(request, records));
                }
                // The transport owns all blocking, including its own network deadlines.
                // An enqueue after this recheck supplies a persistent transport wakeup.
                boolean mayBlock = commands.empty() && (closeObserved || !closing.get());
                if (mayBlock) idleHook.beforePoll();
                transport.poll(mayBlock);
            }
        } catch (Throwable error) {
            failure = error;
        } finally {
            try { transport.close(); } catch (Exception error) { if (failure == null) failure = error; }
            terminated = true;
            notifyApp();
        }
    }
    void requestClose() { checkApp(); closing.set(true); transport.wakeup(); }
    boolean closeObserved() { return closeObserved; }
    long retained() { return retained; }
    long received() { return received; }
    boolean terminated() { return terminated; }
    public void close() {
        requestClose();
        // Synchronous app teardown relinquishes its buffers. Background never waits inside a callback.
        for (PrefetchWindow.Lease lease : held.toArray(new PrefetchWindow.Lease[0])) release(lease);
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (!terminated) {
            take(); // Closing rejects and releases every queued lease.
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) break;
            appSleeping.set(true);
            take(); // Recheck after arming; publication/termination signals the app.
            if (!terminated) LockSupport.parkNanos(this, remaining);
            appSleeping.set(false);
            if (Thread.currentThread().isInterrupted()) throw new IllegalStateException("Close interrupted");
        }
        if (!terminated) throw new AssertionError("Background close stalled");
        checkApp();
    }
}
