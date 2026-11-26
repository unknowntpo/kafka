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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.RecordDeserializationException.DeserializationExceptionOrigin;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.CloseableIterator;

import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * {@link CompletedFetch} represents a {@link RecordBatch batch} of {@link Record records} that was returned from the
 * broker via a {@link FetchRequest}. It contains logic to maintain state between calls to
 * {@link #fetchRecords(FetchConfig, Deserializers, int)}.
 *
 * <p>The records may be decoded ahead of the application thread by a {@link FetchPredecoder} thread, see
 * {@link #predecode(FetchConfig, Deserializers, BufferSupplier)}. The decode state (the iterators, the current
 * batch and record, the offsets of the decode, the kept error and the decoded records) is used by one thread at a
 * time, the owner. The owner changes only through {@code owner}, which also makes the writes of the old owner
 * visible to the new one. The application thread reads its own copy of the position ({@code appNextFetchOffset},
 * {@code appLastEpoch} and {@code appExhausted}), so it never reads the decode state while a decoder writes it.
 */
public class CompletedFetch {

    final TopicPartition partition;
    final FetchResponseData.PartitionData partitionData;

    private final Logger log;
    private final SubscriptionState subscriptions;
    private final BufferSupplier decompressionBufferSupplier;
    private final Iterator<? extends RecordBatch> batches;
    private final Set<Long> abortedProducerIds;
    private final PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions;
    private final FetchMetricsAggregator metricAggregator;

    private int recordsRead;
    private int bytesRead;
    private RecordBatch currentBatch;
    private Record lastRecord;
    private CloseableIterator<Record> records;
    // True if the records of magic v2 batches may be read into one reused Record, see fetchRecords
    private boolean reuseRecords;
    // The array that compressed batches of this fetch are decompressed into, when nothing points into it any more
    private byte[] decompressionArray;
    private Exception cachedRecordException = null;
    private boolean corruptLastRecord = false;
    private long nextFetchOffset;
    private Optional<Integer> lastEpoch;
    // Set by the thread that drains this fetch. A running decode stops at the next record when it sees it.
    private volatile boolean isConsumed = false;
    private boolean exhausted = false;
    private boolean initialized = false;

    // The owner of the decode state: nobody yet (IDLE), a decoder thread (DECODING), the application thread
    // after a decoder has finished (DONE), or the application thread (APP).
    private static final int IDLE = 0;
    private static final int DECODING = 1;
    private static final int DONE = 2;
    private static final int APP = 3;
    private final AtomicInteger owner = new AtomicInteger(IDLE);
    // Records decoded by a decoder thread and not yet handed out. Null when there are none.
    private List<ConsumerRecord<?, ?>> predecoded;
    // predecodedBytes[i] is the value of bytesRead after the decoder decoded predecoded.get(i)
    private int[] predecodedBytes;
    // The number of predecoded records already handed out. Only the application thread uses it.
    private int predecodedPos;
    // An error that is not a KafkaException, such as an Error, found by a decoder thread. It is a RuntimeException
    // or an Error. The application thread throws it when it reaches that record, as it would have if it had decoded
    // the records itself.
    private Throwable deferredException;
    // The position as the application thread sees it. These lag behind the decode state while predecoded
    // records wait. Only the application thread uses them, after the constructor.
    private long appNextFetchOffset;
    private Optional<Integer> appLastEpoch = Optional.empty();
    private boolean appExhausted;

    // The buffer of the response may go back to the pool only if no object that outlives this fetch points into
    // it: no record, header or exception that was handed out. Records decoded by the reusing iterator keep
    // copies of their key and value, but their headers point into the records. Records of the streaming iterator
    // may point into the response with their key, value and headers.
    //
    // True once something that was handed out may point into the bytes of the response. Only the thread that
    // reads the records uses it.
    private boolean responseMayBeReferenced;
    // True once the application thread has taken all records and nothing that was handed out points into the
    // response. It is set by the application thread and read by the thread that drains this fetch, which at close
    // is the network thread, so it is volatile.
    private volatile boolean responseReusable;
    // Gives the buffer of the response back to the pool. Null if the response is not from a pool.
    private ReceiveBufferPool.Lease receiveLease;

    /** The offset right after the last batch of this response. A prefetch for this partition continues here. */
    final long prefetchOffset;
    /** The size of the records of this response. It counts against the prefetch limit while they are buffered. */
    final int sizeInBytes;
    /** True if the next fetch for this partition may be sent before this response has been consumed. */
    final boolean prefetchEligible;

    CompletedFetch(Logger log,
                   SubscriptionState subscriptions,
                   BufferSupplier decompressionBufferSupplier,
                   TopicPartition partition,
                   FetchResponseData.PartitionData partitionData,
                   FetchMetricsAggregator metricAggregator,
                   Long fetchOffset) {
        this.log = log;
        this.subscriptions = subscriptions;
        this.decompressionBufferSupplier = decompressionBufferSupplier;
        this.partition = partition;
        this.partitionData = partitionData;
        this.metricAggregator = metricAggregator;
        this.batches = FetchResponse.recordsOrFail(partitionData).batches().iterator();
        this.nextFetchOffset = fetchOffset;
        this.appNextFetchOffset = fetchOffset;
        this.lastEpoch = Optional.empty();
        this.abortedProducerIds = new HashSet<>();
        this.abortedTransactions = abortedTransactions(partitionData);

        Records fetchedRecords = FetchResponse.recordsOrFail(partitionData);
        long endOffset = fetchOffset;
        boolean scanned = true;
        try {
            for (RecordBatch batch : fetchedRecords.batches())
                endOffset = batch.nextOffset();
        } catch (RuntimeException e) {
            // Invalid data must be reported to the application when it is consumed, so it is not raised here.
            // No prefetch is sent past data that cannot be scanned.
            scanned = false;
        }
        this.prefetchOffset = endOffset;
        this.sizeInBytes = fetchedRecords.sizeInBytes();
        this.prefetchEligible = scanned
                && partitionData.errorCode() == Errors.NONE.code()
                && endOffset > fetchOffset
                && !FetchResponse.isPreferredReplica(partitionData)
                && !FetchResponse.isDivergingEpoch(partitionData);
    }

    long nextFetchOffset() {
        return appNextFetchOffset;
    }

    Optional<Integer> lastEpoch() {
        return appLastEpoch;
    }

    boolean isInitialized() {
        return initialized;
    }

    void setInitialized() {
        this.initialized = true;
    }

    public boolean isConsumed() {
        return isConsumed;
    }

    boolean isExhausted() {
        return appExhausted;
    }

    /**
     * Makes this fetch one part of the response that the lease tracks. Call it before the fetch is shared with
     * other threads. {@link #drain()} releases the part.
     *
     * @param responseBytes the bytes of the records of this fetch, which are part of the response
     */
    void receiveLease(ReceiveBufferPool.Lease lease, ByteBuffer responseBytes) {
        lease.attach(responseBytes);
        synchronized (this) {
            this.receiveLease = lease;
        }
    }


    /**
     * After each partition is parsed, we update the current metric totals with the total bytes
     * and number of records parsed. After all partitions have reported, we write the metric.
     */
    void recordAggregatedMetrics(int bytes, int records) {
        metricAggregator.record(partition, bytes, records);
    }

    /**
     * Draining a {@link CompletedFetch} will signal that the data has been consumed and the underlying resources
     * are closed. This is somewhat analogous to {@link Closeable#close() closing}, though no error will result if a
     * caller invokes {@link #fetchRecords(FetchConfig, Deserializers, int)}; an empty {@link List list} will be
     * returned instead.
     */
    void drain() {
        if (!isConsumed) {
            // Set first, so that a running decode stops at the next record.
            this.isConsumed = true;
            // Wait for a running decode. The response must not go back to the pool while a decoder reads it.
            takeOwnership();
            maybeCloseRecordStream();
            cachedRecordException = null;
            deferredException = null;
            releaseResponse();
            if (predecoded != null) {
                // Count only the records that were handed out, as if the application thread had decoded them.
                recordsRead = predecodedPos;
                bytesRead = predecodedPos == 0 ? 0 : predecodedBytes[predecodedPos - 1];
                predecoded = null;
                predecodedBytes = null;
            }
            recordAggregatedMetrics(bytesRead, recordsRead);

            // we move the partition to the end if we received some bytes. This way, it's more likely that partitions
            // for the same topic can remain together (allowing for more efficient serialization).
            if (bytesRead > 0)
                subscriptions.movePartitionToEnd(partition);
        }
    }

    /**
     * Makes the calling thread the owner of the decode state. If a decoder thread is decoding this fetch, this
     * waits until it is done. Called by the application thread, and by the thread that drains this fetch.
     */
    private void takeOwnership() {
        if (owner.get() == IDLE && owner.compareAndSet(IDLE, APP))
            return;
        if (owner.get() != DECODING)
            return;
        // An interrupt would make parkNanos return at once, so it is cleared while waiting and set again after.
        boolean interrupted = Thread.interrupted();
        while (owner.get() == DECODING) {
            LockSupport.parkNanos(this, 100_000L);
            interrupted |= Thread.interrupted();
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /**
     * Decodes all records of this fetch on the calling decoder thread, unless another thread has already started
     * on it. An error is not raised here. It is kept, and the application thread raises it when it reaches that
     * record, as if it had decoded the records itself. The decode stops early when this fetch is drained.
     *
     * @param bufferSupplier the decompression buffers to use; several decoder threads may use it at once
     */
    <K, V> void predecode(FetchConfig fetchConfig, Deserializers<K, V> deserializers, BufferSupplier bufferSupplier) {
        if (!owner.compareAndSet(IDLE, DECODING))
            return;
        List<ConsumerRecord<K, V>> decoded = new ArrayList<>();
        try {
            decode(fetchConfig, deserializers, bufferSupplier, Integer.MAX_VALUE, true, decoded);
        } finally {
            if (!decoded.isEmpty()) {
                @SuppressWarnings({"unchecked", "rawtypes"})
                List<ConsumerRecord<?, ?>> list = (List) decoded;
                predecoded = list;
            }
            // hands the decode state to the application thread
            owner.set(DONE);
        }
    }

    // Called by the application thread when it owns the decode state.
    private void syncAppView() {
        appNextFetchOffset = nextFetchOffset;
        appLastEpoch = lastEpoch;
        appExhausted = exhausted;
        if (exhausted)
            responseReusable = !responseMayBeReferenced;
    }

    /**
     * Releases the part of this fetch in the lease of the response. The buffer of the response may then go back to
     * the pool, but only if all records of this fetch were read and none of them points into the buffer. A fetch
     * that is dropped before its end, for example after a seek or when its partition is revoked, keeps the buffer
     * out of the pool, because it is hard to prove that no record handed out points into it.
     */
    private void releaseResponse() {
        ReceiveBufferPool.Lease lease;
        // Under a lock, so that the part is released only once, whatever thread drains this fetch.
        synchronized (this) {
            lease = receiveLease;
            receiveLease = null;
        }
        if (lease != null) {
            if (!responseReusable)
                lease.taint();
            lease.release();
            // The buffer may now hold another response. A read of these fields by mistake fails here instead of
            // reading the bytes of that response. A drained fetch returns no records, so nothing reads them.
            currentBatch = null;
            lastRecord = null;
        }
    }

    private void maybeEnsureValid(FetchConfig fetchConfig, RecordBatch batch) {
        if (fetchConfig.checkCrcs && batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
            try {
                batch.ensureValid();
            } catch (CorruptRecordException e) {
                throw new KafkaException("Record batch for partition " + partition + " at offset " +
                        batch.baseOffset() + " is invalid, cause: " + e.getMessage());
            }
        }
    }

    private void maybeEnsureValid(FetchConfig fetchConfig, Record record) {
        if (fetchConfig.checkCrcs) {
            try {
                record.ensureValid();
            } catch (CorruptRecordException e) {
                throw new KafkaException("Record for partition " + partition + " at offset " + record.offset()
                        + " is invalid, cause: " + e.getMessage());
            }
        }
    }

    private void maybeCloseRecordStream() {
        if (records != null) {
            if (records instanceof DefaultRecordBatch.ReusingRecordIterator) {
                // The records handed out keep only copies of their key and value, but their headers point into
                // the decompression array, so it is used again only if no record had headers.
                DefaultRecordBatch.ReusingRecordIterator iterator = (DefaultRecordBatch.ReusingRecordIterator) records;
                if (iterator.decompressionArray() != null)
                    decompressionArray = iterator.readHeaders() ? null : iterator.decompressionArray();
                // Without compression, the headers point into the response itself.
                else if (iterator.readHeaders())
                    responseMayBeReferenced = true;
            }
            records.close();
            records = null;
        }
    }

    private Record nextFetchedRecord(FetchConfig fetchConfig, BufferSupplier bufferSupplier) {
        while (true) {
            if (records == null || !records.hasNext()) {
                maybeCloseRecordStream();

                if (!batches.hasNext()) {
                    // Message format v2 preserves the last offset in a batch even if the last record is removed
                    // through compaction. By using the next offset computed from the last offset in the batch,
                    // we ensure that the offset of the next fetch will point to the next batch, which avoids
                    // unnecessary re-fetching of the same batch (in the worst case, the consumer could get stuck
                    // fetching the same batch repeatedly).
                    if (currentBatch != null)
                        nextFetchOffset = currentBatch.nextOffset();
                    exhausted = true;
                    return null;
                }

                currentBatch = batches.next();
                lastEpoch = maybeLeaderEpoch(currentBatch.partitionLeaderEpoch());
                maybeEnsureValid(fetchConfig, currentBatch);

                if (fetchConfig.isolationLevel == IsolationLevel.READ_COMMITTED && currentBatch.hasProducerId()) {
                    // remove from the aborted transaction queue all aborted transactions which have begun
                    // before the current batch's last offset and add the associated producerIds to the
                    // aborted producer set
                    consumeAbortedTransactionsUpTo(currentBatch.lastOffset());

                    long producerId = currentBatch.producerId();
                    if (containsAbortMarker(currentBatch)) {
                        abortedProducerIds.remove(producerId);
                    } else if (isBatchAborted(currentBatch)) {
                        log.debug("Skipping aborted record batch from partition {} with producerId {} and " +
                                        "offsets {} to {}",
                                partition, producerId, currentBatch.baseOffset(), currentBatch.lastOffset());
                        nextFetchOffset = currentBatch.nextOffset();
                        continue;
                    }
                }

                // For magic v2 data batches, read the records without creating any object for each record
                if (reuseRecords && currentBatch instanceof DefaultRecordBatch && !currentBatch.isControlBatch()) {
                    records = ((DefaultRecordBatch) currentBatch).reusingIterator(bufferSupplier, decompressionArray);
                    // the iterator owns the array until it is closed
                    decompressionArray = null;
                } else {
                    // The records of this iterator, and their headers, may point into the response.
                    responseMayBeReferenced = true;
                    records = currentBatch.streamingIterator(bufferSupplier);
                }
            } else {
                Record record = records.next();
                // skip any records out of range
                if (record.offset() >= nextFetchOffset) {
                    // we only do validation when the message should not be skipped.
                    maybeEnsureValid(fetchConfig, record);

                    // control records are not returned to the user
                    if (!currentBatch.isControlBatch()) {
                        return record;
                    } else {
                        // Increment the next fetch offset when we skip a control batch.
                        nextFetchOffset = record.offset() + 1;
                    }
                }
            }
        }
    }

    /**
     * The {@link RecordBatch batch} of {@link Record records} is converted to a {@link List list} of
     * {@link ConsumerRecord consumer records} and returned. {@link BufferSupplier Decompression} and
     * {@link Deserializer deserialization} of the {@link Record record's} key and value are performed in
     * this step.
     *
     * @param fetchConfig {@link FetchConfig Configuration} to use
     * @param deserializers {@link Deserializer}s to use to convert the raw bytes to the expected key and value types
     * @param maxRecords The number of records to return; the number returned may be {@code 0 <= maxRecords}
     * @return {@link ConsumerRecord Consumer records}
     */
    <K, V> List<ConsumerRecord<K, V>> fetchRecords(FetchConfig fetchConfig,
                                                   Deserializers<K, V> deserializers,
                                                   int maxRecords) {
        takeOwnership();
        if (predecoded != null && !isConsumed) {
            if (maxRecords <= 0)
                return Collections.emptyList();
            int end = (int) Math.min((long) predecodedPos + maxRecords, predecoded.size());
            // A view, not a copy: copying the references would cost about as much as decoding did.
            @SuppressWarnings({"unchecked", "rawtypes"})
            List<ConsumerRecord<K, V>> all = (List) predecoded;
            List<ConsumerRecord<K, V>> records = predecodedPos == 0 && end == all.size() ? all : all.subList(predecodedPos, end);
            predecodedPos = end;
            if (end < all.size()) {
                ConsumerRecord<K, V> last = records.get(records.size() - 1);
                appNextFetchOffset = last.offset() + 1;
                appLastEpoch = last.leaderEpoch();
            } else {
                predecoded = null;
                predecodedBytes = null;
                syncAppView();
            }
            return records;
        }
        try {
            return decode(fetchConfig, deserializers, decompressionBufferSupplier, maxRecords, false, new ArrayList<>());
        } finally {
            // A drained fetch keeps the position the application thread had, not where a decoder stopped.
            if (!isConsumed)
                syncAppView();
        }
    }

    /**
     * Decodes up to {@code maxRecords} records into {@code records}.
     *
     * @param bufferSupplier the decompression buffers to use
     * @param quiet true on a decoder thread: the decode stops when this fetch is drained, and an error is kept
     *              for the application thread instead of raised
     */
    private <K, V> List<ConsumerRecord<K, V>> decode(FetchConfig fetchConfig,
                                                     Deserializers<K, V> deserializers,
                                                     BufferSupplier bufferSupplier,
                                                     int maxRecords,
                                                     boolean quiet,
                                                     List<ConsumerRecord<K, V>> records) {
        throwKeptError();

        if (isConsumed)
            return Collections.emptyList();

        // The reused Record gives out buffers that are valid only until the next record. Only deserializers that
        // do not keep these buffers can use it.
        reuseRecords = deserializers.simple();

        try {
            for (int i = 0; i < maxRecords && !(quiet && isConsumed); i++) {
                // Only move to next record if there was no exception in the last fetch. Otherwise, we should
                // use the last record to do deserialization again.
                if (cachedRecordException == null) {
                    corruptLastRecord = true;
                    lastRecord = nextFetchedRecord(fetchConfig, bufferSupplier);
                    corruptLastRecord = false;
                }

                if (lastRecord == null)
                    break;

                // lastEpoch is the leader epoch of the current batch, so all its records can share it
                TimestampType timestampType = currentBatch.timestampType();
                ConsumerRecord<K, V> record = parseRecord(deserializers, partition, lastEpoch, timestampType, lastRecord);
                records.add(record);
                recordsRead++;
                bytesRead += lastRecord.sizeInBytes();
                nextFetchOffset = lastRecord.offset() + 1;
                keepDecodedBytes(quiet, records.size() - 1);
                // In some cases, the deserialization may have thrown an exception and the retry may succeed,
                // we allow user to move forward in this case.
                cachedRecordException = null;
            }
        } catch (SerializationException se) {
            cachedRecordException = se;
            if (raise(records, quiet))
                throw se;
        } catch (KafkaException e) {
            cachedRecordException = e;
            if (raise(records, quiet))
                throw new KafkaException("Received exception when fetching the next record from " + partition
                        + ". If needed, please seek past the record to "
                        + "continue consumption.", e);
        } catch (RuntimeException | Error e) {
            if (!quiet)
                throw e;
            deferredException = e;
        } finally {
            // An error may hold parts of the record, so the response is not reused after one.
            if (cachedRecordException != null || corruptLastRecord)
                responseMayBeReferenced = true;
        }
        return records;
    }

    /**
     * Throws the error that stopped the previous decode at the current record, if there is one.
     */
    private void throwKeptError() {
        // An error found by a decoder thread that is not a KafkaException, thrown as it was found.
        if (deferredException != null) {
            Throwable e = deferredException;
            deferredException = null;
            if (e instanceof Error)
                throw (Error) e;
            throw (RuntimeException) e;
        }

        // Error when fetching the next record before deserialization.
        if (corruptLastRecord)
            throw new KafkaException("Received exception when fetching the next record from " + partition
                    + ". If needed, please seek past the record to "
                    + "continue consumption.", cachedRecordException);
    }

    // An error is raised only on the application thread, and only if no record can be returned before it.
    private static boolean raise(List<?> records, boolean quiet) {
        return records.isEmpty() && !quiet;
    }

    // On a decoder thread, keeps the bytes read so far for the record at the given index of the decoded records.
    private void keepDecodedBytes(boolean quiet, int index) {
        if (!quiet)
            return;
        if (predecodedBytes == null)
            predecodedBytes = new int[64];
        else if (index == predecodedBytes.length)
            predecodedBytes = Arrays.copyOf(predecodedBytes, index * 2);
        predecodedBytes[index] = bytesRead;
    }

    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     */
    <K, V> ConsumerRecord<K, V> parseRecord(Deserializers<K, V> deserializers,
                                            TopicPartition partition,
                                            Optional<Integer> leaderEpoch,
                                            TimestampType timestampType,
                                            Record record) {
        ByteBuffer keyBytes = record.key();
        ByteBuffer valueBytes = record.value();
        Headers headers = new RecordHeaders(record.headers());
        K key;
        V value;
        try {
            key = keyBytes == null ? null : deserializers.keyDeserializer().deserialize(partition.topic(), headers, keyBytes);
        } catch (RuntimeException e) {
            log.error("Key Deserializers with error: {}", deserializers);
            throw newRecordDeserializationException(DeserializationExceptionOrigin.KEY, partition, timestampType, record, e, headers);
        }
        try {
            value = valueBytes == null ? null : deserializers.valueDeserializer().deserialize(partition.topic(), headers, valueBytes);
        } catch (RuntimeException e) {
            log.error("Value Deserializers with error: {}", deserializers);
            throw newRecordDeserializationException(DeserializationExceptionOrigin.VALUE, partition, timestampType, record, e, headers);
        }
        return new ConsumerRecord<>(partition.topic(), partition.partition(), record.offset(),
                record.timestamp(), timestampType,
                keyBytes == null ? ConsumerRecord.NULL_SIZE : keyBytes.remaining(),
                valueBytes == null ? ConsumerRecord.NULL_SIZE : valueBytes.remaining(),
                key, value, headers, leaderEpoch);
    }

    private static RecordDeserializationException newRecordDeserializationException(DeserializationExceptionOrigin origin,
                                                                                    TopicPartition partition,
                                                                                    TimestampType timestampType,
                                                                                    Record record,
                                                                                    RuntimeException e,
                                                                                    Headers headers) {
        // copies, because the buffers of a reused Record change with the next record
        return new RecordDeserializationException(origin, partition, record.offset(), record.timestamp(), timestampType,
                copyOf(record.key()), copyOf(record.value()), headers,
                "Error deserializing " + origin.name() + " for partition " + partition + " at offset " + record.offset()
                        + ". If needed, please seek past the record to continue consumption.", e);
    }

    private static ByteBuffer copyOf(ByteBuffer buffer) {
        if (buffer == null)
            return null;
        ByteBuffer copy = ByteBuffer.allocate(buffer.remaining());
        copy.put(buffer.duplicate());
        copy.flip();
        return copy;
    }

    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
    }

    private void consumeAbortedTransactionsUpTo(long offset) {
        if (abortedTransactions == null)
            return;

        while (!abortedTransactions.isEmpty() && abortedTransactions.peek().firstOffset() <= offset) {
            FetchResponseData.AbortedTransaction abortedTransaction = abortedTransactions.poll();
            abortedProducerIds.add(abortedTransaction.producerId());
        }
    }

    private boolean isBatchAborted(RecordBatch batch) {
        return batch.isTransactional() && abortedProducerIds.contains(batch.producerId());
    }

    private PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions(FetchResponseData.PartitionData partition) {
        if (partition.abortedTransactions() == null || partition.abortedTransactions().isEmpty())
            return null;

        PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions = new PriorityQueue<>(
                partition.abortedTransactions().size(), Comparator.comparingLong(FetchResponseData.AbortedTransaction::firstOffset)
        );
        abortedTransactions.addAll(partition.abortedTransactions());
        return abortedTransactions;
    }

    private boolean containsAbortMarker(RecordBatch batch) {
        if (!batch.isControlBatch())
            return false;

        Iterator<Record> batchIterator = batch.iterator();
        if (!batchIterator.hasNext())
            return false;

        Record firstRecord = batchIterator.next();
        return ControlRecordType.ABORT == ControlRecordType.parse(firstRecord.key());
    }
}
