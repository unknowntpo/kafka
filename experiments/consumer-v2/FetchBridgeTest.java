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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;

import java.nio.channels.Selector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

public final class FetchBridgeTest {
    /** Real selector wait/wakeup; records are synthetic, not socket/Kafka broker responses. */
    static final class SelectorTransport implements FetchBridge.Transport {
        final Selector selector;
        final AtomicInteger permits = new AtomicInteger();
        final AtomicLong sent = new AtomicLong();
        volatile long requestedOffset;
        volatile long selects;
        private Consumer<FetchResponseData.PartitionData> callback;
        private final boolean emptyResponses;
        SelectorTransport() throws Exception { this(false); }
        SelectorTransport(boolean emptyResponses) throws Exception {
            this.emptyResponses = emptyResponses;
            selector = Selector.open();
        }
        public void send(long offset, Consumer<FetchResponseData.PartitionData> completion) {
            if (callback != null) throw new AssertionError("Only one in-flight request");
            requestedOffset = offset;
            callback = completion;
            sent.incrementAndGet();
        }
        void allow(int count) { permits.addAndGet(count); wakeup(); }
        private boolean deliver() {
            if (callback == null || permits.get() <= 0) return false;
            permits.decrementAndGet();
            Consumer<FetchResponseData.PartitionData> completion = callback;
            callback = null;
            completion.accept(new FetchResponseData.PartitionData().setRecords(emptyResponses ? MemoryRecords.EMPTY : MemoryRecords.withRecords(requestedOffset, Compression.NONE,
                new SimpleRecord(0, new byte[] {1}), new SimpleRecord(0, new byte[] {2}))));
            return true;
        }
        public void poll(boolean mayBlock) throws Exception {
            if (deliver()) return;
            selects++;
            if (mayBlock) selector.select(); else selector.selectNow();
            deliver();
        }
        public void wakeup() { selector.wakeup(); }
        public void cancel() { callback = null; }
        public void close() throws Exception { selector.close(); }
    }
    private static void check(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }
    private static void until(BooleanSupplier predicate, String message) {
        long end = System.nanoTime() + 3_000_000_000L;
        while (!predicate.getAsBoolean()) {
            if (System.nanoTime() >= end) throw new AssertionError(message);
            Thread.yield(); // Test observation only; no production polling delay.
        }
    }
    private static void streaming() throws Exception {
        SelectorTransport transport = new SelectorTransport();
        try (FetchBridge bridge = new FetchBridge(4096, 4, transport, () -> { })) {
            transport.allow(10000);
            for (int i = 0; i < 10000; i++) {
                PrefetchWindow.Lease lease = bridge.await(3_000_000_000L);
                check(lease != null && bridge.valid(lease), "missing or stale delivery");
                check(lease.offset == i * 2L, "FIFO or cursor lost across threads");
                check(lease.records.batches().iterator().next().baseOffset() == lease.offset, "payload mismatch");
                bridge.release(lease);
            }
            until(() -> bridge.retained() == 0, "all credits must return");
        }
        System.out.println("PASS 10000 real-record leases, FIFO, credit recovery, selector wakeup");
    }
    private static void seekBeforeOwnerApplies() throws Exception {
        SelectorTransport transport = new SelectorTransport();
        AtomicBoolean entered = new AtomicBoolean();
        AtomicBoolean proceed = new AtomicBoolean();
        AtomicBoolean armed = new AtomicBoolean();
        FetchBridge bridge = new FetchBridge(4096, 2, transport, () -> {
            if (armed.compareAndSet(true, false)) {
                entered.set(true);
                until(proceed::get, "test hook release");
            }
        });
        try {
            transport.allow(1);
            PrefetchWindow.Lease old = bridge.await(3_000_000_000L);
            check(old != null, "first lease");
            armed.set(true);
            transport.wakeup();
            until(entered::get, "background must stop before next poll");
            long oldSent = transport.sent.get();
            check(bridge.trySeek(100), "seek admission");
            check(!bridge.valid(old), "App fence must precede owner applying seek");
            check(!bridge.trySeek(200), "only one queued seek");
            bridge.release(old);
            proceed.set(true);
            // First permit resolves the stale in-flight response; second resolves post-seek request.
            transport.allow(2);
            PrefetchWindow.Lease current = bridge.await(3_000_000_000L);
            check(current != null && current.offset == 100 && bridge.valid(current), "stale in-flight response escaped");
            check(transport.sent.get() > oldSent, "post-seek request missing");
            bridge.release(current);
        } finally {
            proceed.set(true);
            bridge.close();
        }
        System.out.println("PASS immediate app seek fence, queued seek bound, old in-flight discard");
    }
    private static void fullQueueClose() throws Exception {
        SelectorTransport transport = new SelectorTransport();
        FetchBridge bridge = new FetchBridge(10000, 2, transport, () -> { });
        try {
            transport.allow(20);
            until(() -> bridge.received() == 2, "CQ should fill");
            bridge.requestClose();
            until(bridge::closeObserved, "close must progress with full CQ and paused app");
            check(bridge.received() == 2 && bridge.retained() > 0, "no overflow or early buffer release");
        } finally { bridge.close(); }
        check(bridge.retained() == 0, "close drains byte credits");
        System.out.println("PASS full CQ: close observed without app consuming, eventual release");
    }
    private static void releaseBeforeSelect() throws Exception {
        for (int i = 0; i < 100; i++) {
            SelectorTransport transport = new SelectorTransport();
            AtomicBoolean arm = new AtomicBoolean();
            AtomicBoolean entered = new AtomicBoolean();
            AtomicBoolean proceed = new AtomicBoolean();
            FetchBridge bridge = new FetchBridge(1, 1, transport, () -> {
                if (arm.compareAndSet(true, false)) {
                    entered.set(true);
                    until(proceed::get, "release-before-select hook");
                }
            });
            try {
                transport.allow(2);
                PrefetchWindow.Lease first = bridge.await(3_000_000_000L);
                check(first != null, "first oversize response");
                arm.set(true);
                transport.wakeup();
                until(entered::get, "owner reached check-to-select window");
                bridge.release(first); // wakeup occurs BEFORE select actually starts.
                proceed.set(true);
                PrefetchWindow.Lease second = bridge.await(3_000_000_000L);
                check(second != null && second.offset == 2, "release wakeup lost");
                bridge.release(second);
            } finally { proceed.set(true); bridge.close(); }
        }
        System.out.println("PASS 100 forced release-between-recheck-and-select races");
    }
    private static void emptyMetadataClose() throws Exception {
        SelectorTransport transport = new SelectorTransport(true);
        java.util.concurrent.atomic.AtomicReference<FetchBridge> reference = new java.util.concurrent.atomic.AtomicReference<>();
        AtomicBoolean closedAndWaiting = new AtomicBoolean();
        FetchBridge bridge = new FetchBridge(10000, 2, transport, () -> {
            FetchBridge current = reference.get();
            if (current != null && current.closeObserved()) closedAndWaiting.set(true);
        });
        reference.set(bridge);
        try {
            transport.allow(20);
            until(() -> bridge.received() == 2, "empty metadata CQ should fill");
            check(bridge.retained() == 0, "metadata lease has zero record bytes");
            bridge.requestClose();
            until(() -> bridge.terminated() || closedAndWaiting.get(), "close did not progress");
            check(!bridge.terminated(), "zero record bytes must not discard outstanding metadata leases");
        } finally { bridge.close(); }
        check(bridge.terminated(), "empty leases not drained at close");
        System.out.println("PASS zero-byte metadata leases remain bounded and participate in close");
    }
    public static void main(String[] args) throws Exception {
        emptyMetadataClose();
        streaming();
        seekBeforeOwnerApplies();
        fullQueueClose();
        releaseBeforeSelect();
    }
}
