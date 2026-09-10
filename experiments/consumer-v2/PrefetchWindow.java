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

import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.record.internal.RecordBatch;

import java.util.IdentityHashMap;
import java.util.Map;

/** Single-background-owner protocol prototype for one partition, not a consumer. */
final class PrefetchWindow {
    static final class Request {
        final long generation;
        final long offset;
        Request(long generation, long offset) {
            this.generation = generation;
            this.offset = offset;
        }
    }

    static final class Lease {
        final long generation;
        final long offset;
        final MemoryRecords records;
        final FetchResponseData.PartitionData partitionData;
        Lease(long generation, long offset, FetchResponseData.PartitionData partitionData) {
            this.generation = generation;
            this.offset = offset;
            this.partitionData = partitionData;
            this.records = (MemoryRecords) FetchResponse.recordsOrFail(partitionData);
        }
    }

    private final Thread owner = Thread.currentThread();
    private final long budget;
    private final int slots;
    private final Map<Lease, Integer> leases = new IdentityHashMap<>();
    private Request pending;
    private long generation;
    private long cursor;
    private long retained;
    private boolean closed;

    PrefetchWindow(long budget, int slots, long offset) {
        if (budget <= 0 || slots <= 0 || offset < 0)
            throw new IllegalArgumentException();
        this.budget = budget;
        this.slots = slots;
        this.cursor = offset;
    }

    private void checkOwner() {
        if (owner != Thread.currentThread())
            throw new IllegalStateException("Only background owner may mutate window");
    }

    boolean ready() {
        checkOwner();
        return !closed && pending == null && retained < budget && leases.size() < slots;
    }

    Request begin() {
        if (!ready()) return null;
        pending = new Request(generation, cursor);
        return pending;
    }

    // Caller supplies successful, complete, immutable partition records. Protocol errors are external.
    Lease complete(Request request, MemoryRecords records) {
        return complete(request, new FetchResponseData.PartitionData().setRecords(records));
    }

    Lease complete(Request request, FetchResponseData.PartitionData data) {
        checkOwner();
        if (request == null || request != pending)
            throw new IllegalStateException("Unknown or duplicate completion");
        if (closed || request.generation != generation) {
            pending = null;
            return null;
        }
        if (data.errorCode() != 0) {
            pending = null;
            throw Errors.forCode(data.errorCode()).exception();
        }
        if (FetchResponse.isDivergingEpoch(data)) {
            pending = null;
            throw new IllegalStateException("Leader truncation requires position validation");
        }
        MemoryRecords records = (MemoryRecords) FetchResponse.recordsOrFail(data);
        long next = cursor;
        try {
            // Validate before committing speculative progress. Decoder still validates on delivery.
            for (RecordBatch batch : records.batches()) {
                batch.ensureValid();
                long batchNext = batch.nextOffset();
                if (batchNext < 0) throw new IllegalArgumentException("Offset overflow");
                next = Math.max(next, batchNext);
            }
        } catch (RuntimeException e) {
            pending = null;
            throw e;
        }
        pending = null;
        cursor = next;
        Lease lease = new Lease(generation, request.offset, data);
        leases.put(lease, records.sizeInBytes());
        retained += records.sizeInBytes();
        return lease;
    }

    void fail(Request request) {
        checkOwner();
        if (request == null || request != pending) throw new IllegalStateException("Unknown request");
        pending = null;
    }

    // Returns whether this release makes new work ready; caller must enqueue the owner then.
    boolean release(Lease lease) {
        checkOwner();
        boolean before = ready();
        Integer bytes = leases.remove(lease);
        if (bytes == null) throw new IllegalStateException("Unknown or duplicate release");
        retained -= bytes;
        return !before && ready();
    }

    void seek(long offset) {
        checkOwner();
        if (closed || offset < 0) throw new IllegalStateException("Invalid seek");
        generation = Math.incrementExact(generation);
        cursor = offset;
        // Old leases stay charged until app relinquishes them. Old network request stays in flight.
    }

    boolean valid(Lease lease) {
        checkOwner();
        return !closed && lease.generation == generation && leases.containsKey(lease);
    }

    void close() {
        checkOwner();
        closed = true;
    }

    int outstandingLeases() { checkOwner(); return leases.size(); }
    long cursor() { checkOwner(); return cursor; }
    long retained() { checkOwner(); return retained; }
}
