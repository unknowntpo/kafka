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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.protocol.Errors;
import java.util.function.LongFunction;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collections;

/** Loopback Kafka protocol fixture. It is not a Kafka broker and provides no throughput evidence. */
public final class NetworkFetchTest {
    static final class Peer implements AutoCloseable {
        final ServerSocket server;
        final Thread thread;
        volatile Socket socket;
        volatile boolean closing;
        volatile Throwable failure;
        volatile int requests;
        volatile boolean holdResponse;
        final LongFunction<FetchResponseData.PartitionData> replies;
        Peer(boolean holdResponse) throws Exception { this(holdResponse, NetworkFetchTest::ordinary); }
        Peer(boolean holdResponse, LongFunction<FetchResponseData.PartitionData> replies) throws Exception {
            this.replies = replies;
            this.holdResponse = holdResponse;
            server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
            server.setSoTimeout(5000);
            thread = new Thread(this::run, "test-protocol-peer");
            thread.setDaemon(true);
            thread.start();
        }
        void run() {
            try (Socket accepted = server.accept()) {
                socket = accepted;
                accepted.setSoTimeout(5000);
                DataInputStream in = new DataInputStream(accepted.getInputStream());
                DataOutputStream out = new DataOutputStream(accepted.getOutputStream());
                while (!closing) {
                    int size = in.readInt();
                    if (size <= 0 || size > 65536) throw new AssertionError("Frame bound");
                    ByteBuffer buffer = ByteBuffer.wrap(in.readNBytes(size));
                    RequestHeader header = RequestHeader.parse(buffer);
                    if (header.apiKey() != ApiKeys.FETCH || header.apiVersion() != 12)
                        throw new AssertionError("Expected Kafka Fetch v12");
                    FetchRequest request = FetchRequest.parse(new ByteBufferAccessor(buffer), header.apiVersion());
                    long offset = request.data().topics().get(0).partitions().get(0).fetchOffset();
                    if (request.isolationLevel() != IsolationLevel.READ_COMMITTED)
                        throw new AssertionError("READ_COMMITTED must be requested on wire");
                    requests++;
                    if (holdResponse) { // Let the real client's request deadline expire; no response polling timer.
                        while (in.read() != -1) { }
                        return;
                    }
                    FetchResponseData body = new FetchResponseData().setResponses(Collections.singletonList(
                        new FetchResponseData.FetchableTopicResponse().setTopic("test")
                            .setPartitions(Collections.singletonList(replies.apply(offset)))));
                    ResponseHeader responseHeader = header.toResponseHeader();
                    short hv = ApiKeys.FETCH.responseHeaderVersion(header.apiVersion());
                    ObjectSerializationCache cache = new ObjectSerializationCache();
                    int length = responseHeader.data().size(cache, hv) + body.size(cache, header.apiVersion());
                    ByteBuffer frame = ByteBuffer.allocate(4 + length);
                    frame.putInt(length);
                    ByteBufferAccessor accessor = new ByteBufferAccessor(frame);
                    responseHeader.data().write(accessor, cache, hv);
                    body.write(accessor, cache, header.apiVersion());
                    out.write(frame.array());
                    out.flush();
                }
            } catch (EOFException expected) {
                // Consumer closes its channel at teardown.
            } catch (Throwable error) {
                if (!closing) failure = error;
            }
        }
        public void close() throws Exception {
            closing = true;
            server.close();
            if (socket != null) socket.close();
            thread.join(5000);
            if (thread.isAlive()) throw new AssertionError("Protocol peer leaked");
            if (failure != null) throw new AssertionError("Protocol peer failed", failure);
        }
    }
    private static FetchResponseData.PartitionData ordinary(long offset) {
        return new FetchResponseData.PartitionData().setPartitionIndex(0).setHighWatermark(1000000)
            .setLastStableOffset(1000000).setLogStartOffset(0).setRecords(MemoryRecords.withRecords(offset, Compression.NONE,
                new SimpleRecord(0, new byte[] {1}), new SimpleRecord(0, new byte[] {2})));
    }
    private static FetchResponseData.PartitionData transactional(long offset) {
        if (offset == 0) {
            return ordinary(0).setHighWatermark(20).setLastStableOffset(12).setRecords(
                MemoryRecords.withTransactionalRecords(0, Compression.NONE, 7L, (short) 0, 0, 0,
                    new SimpleRecord(0, new byte[] {9}), new SimpleRecord(0, new byte[] {9})))
                .setAbortedTransactions(Collections.singletonList(
                    new FetchResponseData.AbortedTransaction().setProducerId(7).setFirstOffset(0)));
        }
        if (offset == 2) {
            MemoryRecords aborted = MemoryRecords.withTransactionalRecords(2, Compression.NONE, 7L, (short) 0, 2, 0,
                new SimpleRecord(0, new byte[] {9}));
            MemoryRecords marker = MemoryRecords.withEndTransactionMarker(3, 0, 0, 7L, (short) 0,
                new EndTransactionMarker(ControlRecordType.ABORT, 0));
            MemoryRecords visible = MemoryRecords.withRecords(10, Compression.NONE,
                new SimpleRecord(0, new byte[] {1}), new SimpleRecord(0, new byte[] {2}));
            ByteBuffer joined = ByteBuffer.allocate(aborted.sizeInBytes() + marker.sizeInBytes() + visible.sizeInBytes());
            joined.put(aborted.buffer().duplicate()).put(marker.buffer().duplicate()).put(visible.buffer().duplicate()).flip();
            return ordinary(2).setHighWatermark(20).setLastStableOffset(12).setRecords(MemoryRecords.readableRecords(joined))
                .setAbortedTransactions(Collections.singletonList(
                    new FetchResponseData.AbortedTransaction().setProducerId(7).setFirstOffset(0)));
        }
        if (offset == 12) return ordinary(12).setRecords(MemoryRecords.EMPTY).setHighWatermark(30).setLastStableOffset(12);
        return ordinary(offset);
    }
    private static void decodedTransactions() throws Exception {
        try (Peer peer = new Peer(false, NetworkFetchTest::transactional);
             FetchBridge bridge = new FetchBridge(4096, 2, new NetworkFetchTransport(peer.server.getLocalPort()), () -> { });
             AppFetchDecoder app = new AppFetchDecoder(bridge)) {
            if (!app.poll(3_000_000_000L, 1).isEmpty() || app.position() != 2)
                throw new AssertionError("Aborted transaction metadata lost on wire");
            if (app.highWatermark() != 20 || app.lastStableOffset() != 12 || app.logStartOffset() != 0)
                throw new AssertionError("Offset metadata lost");
            java.util.List<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>> records = app.poll(3_000_000_000L, 1);
            if (records.size() != 1 || records.get(0).offset() != 10 || records.get(0).value()[0] != 1 || app.position() != 11)
                throw new AssertionError("Cross-response aborted transaction/control/gap failure");
            records = app.poll(3_000_000_000L, 1);
            if (records.size() != 1 || records.get(0).offset() != 11 || app.position() != 12)
                throw new AssertionError("Partial decode position failure");
            if (!app.poll(3_000_000_000L, 1).isEmpty()) throw new AssertionError("Expected exhaustion");
            if (!app.poll(3_000_000_000L, 1).isEmpty() || app.highWatermark() != 30 || app.lastStableOffset() != 12)
                throw new AssertionError("Empty response metadata discarded");
            if (!app.trySeek(100)) throw new AssertionError("App seek admission");
            records = app.poll(3_000_000_000L, 1);
            if (records.size() != 1 || records.get(0).offset() != 100 || app.position() != 101)
                throw new AssertionError("Seek after empty metadata failed");
            if (!app.trySeek(200)) throw new AssertionError("Partial decode seek admission");
            records = app.poll(3_000_000_000L, 1);
            if (records.size() != 1 || records.get(0).offset() != 200)
                throw new AssertionError("Old partially decoded record escaped seek");
        }
        System.out.println("PASS wire -> complete envelope -> app CompletedFetch: cross-response aborted/control/gap, partial decode, empty metadata, seek");
    }
    private static void partitionError() throws Exception {
        try (Peer peer = new Peer(false, offset -> ordinary(offset).setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code()))) {
            FetchBridge bridge = new FetchBridge(4096, 2, new NetworkFetchTransport(peer.server.getLocalPort()), () -> { });
            boolean rejected = false;
            try { bridge.await(3_000_000_000L); }
            catch (IllegalStateException expected) {
                Throwable error = expected;
                while (error != null) {
                    if (error instanceof org.apache.kafka.common.errors.OffsetOutOfRangeException) rejected = true;
                    error = error.getCause();
                }
            }
            if (!rejected || !bridge.terminated()) throw new AssertionError("Partition error code did not reach app");
        }
        System.out.println("PASS partition OFFSET_OUT_OF_RANGE error reaches app, no data delivery");
    }
    public static void main(String[] args) throws Exception {
        decodedTransactions();
        if (args.length > 0 && args[0].equals("envelope-only")) return;
        partitionError();
        try (Peer peer = new Peer(false);
             FetchBridge bridge = new FetchBridge(4096, 2, new NetworkFetchTransport(peer.server.getLocalPort()), () -> { })) {
            for (int i = 0; i < 1000; i++) {
                PrefetchWindow.Lease lease = bridge.await(3_000_000_000L);
                if (lease == null || lease.offset != i * 2L) throw new AssertionError("Wire response order/cursor mismatch");
                bridge.release(lease);
            }
            if (!bridge.trySeek(5000)) throw new AssertionError("Seek not admitted");
            PrefetchWindow.Lease afterSeek = bridge.await(3_000_000_000L);
            if (afterSeek == null || afterSeek.offset != 5000) throw new AssertionError("Old wire data escaped seek");
            bridge.release(afterSeek);
            System.out.println("PASS NetworkClient + Kafka Selector + loopback Fetch v12: 1000 responses and seek");
        }
        try (Peer peer = new Peer(true)) {
            FetchBridge bridge = new FetchBridge(4096, 2, new NetworkFetchTransport(peer.server.getLocalPort()), () -> { });
            boolean failed = false;
            try { bridge.await(4_000_000_000L); }
            catch (IllegalStateException expected) { failed = true; }
            if (!failed || !bridge.terminated()) throw new AssertionError("Network timeout did not reach app");
            System.out.println("PASS real NetworkClient request timeout wakes app; background terminates");
        }
    }
}
