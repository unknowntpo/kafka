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
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.DefaultRecord;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.ByteUtils;
import org.apache.kafka.common.utils.internals.CloseableIterator;

import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
 */
public class CompletedFetch {

    final TopicPartition partition;
    final FetchResponseData.PartitionData partitionData;

    private final Logger log;
    private final SubscriptionState subscriptions;
    private BufferSupplier decompressionBufferSupplier;
    private final Iterator<? extends RecordBatch> batches;
    private final Set<Long> abortedProducerIds;
    private final PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions;
    private final FetchMetricsAggregator metricAggregator;

    private int recordsRead;
    private int bytesRead;
    private RecordBatch currentBatch;
    private Record lastRecord;
    private CloseableIterator<Record> records;
    private Exception cachedRecordException = null;
    private boolean corruptLastRecord = false;
    private long nextFetchOffset;
    private Optional<Integer> lastEpoch;
    private volatile boolean isConsumed = false;
    // Told when this fetch is drained. Set by the buffer that holds the fetch; must be cheap and must not block.
    private volatile Runnable onDrained;

    void onDrained(Runnable onDrained) {
        this.onDrained = onDrained;
    }

    private boolean exhausted = false;
    private boolean initialized = false;

    // Dense decoding of the current batch: records are read straight from the batch bytes, and only the key,
    // the value and the ConsumerRecord are allocated. Records that are packed together in memory are much
    // cheaper to read later, in particular on another thread.
    private static final Headers NO_HEADERS = readOnlyHeaders();
    // Experiment switch: measures what the dense decode adds on its own (CONSUMER-3X-11, item 1).
    private static final boolean DENSE_DECODE = !"false".equals(System.getProperty("c3x.dense"));
    private boolean denseEnabled;
    private ByteBuffer denseRecords;
    // The array compressed batches are decompressed into. It is used again for the next batch, unless something
    // that outlives the batch may still point into it (record headers, or the bytes kept for an error).
    private byte[] denseScratch;
    private boolean denseScratchEscaped;
    /** True once something outside this fetch may point into the bytes of the response (see ReceiveBufferPool). */
    private boolean receiveEscaped;
    /** Gives the array of the response back for reuse, if this response came from a pooled array. */
    private ReceiveBufferPool.Lease receiveLease;
    private static final ThreadLocal<byte[]> DECODER_SCRATCH = new ThreadLocal<>();
    private ByteBuffer denseView;
    private int denseRemaining;
    private long denseBaseOffset;
    private long denseBaseTimestamp;
    private boolean denseLogAppendTime;
    private long denseMaxTimestamp;
    private TimestampType denseTimestampType;
    private Optional<Integer> denseLeaderEpoch;

    // Decoding state is owned by one thread at a time: a decoder thread (DECODING) or the application thread
    // (APP, or DONE after the decoder has finished). The hand-over goes through this field.
    private static final int IDLE = 0;
    private static final int DECODING = 1;
    private static final int DONE = 2;
    private static final int APP = 3;
    private final AtomicInteger owner = new AtomicInteger(IDLE);
    private volatile Thread waiter;
    /** Records decoded ahead of the application, not handed out yet. */
    private List<ConsumerRecord<?, ?>> predecoded;
    private int predecodedPos;
    // What the application thread has consumed so far. These trail the decoding state while predecoded
    // records are waiting.
    private long appNextFetchOffset;
    private Optional<Integer> appLastEpoch = Optional.empty();
    private boolean appExhausted;

    /** Offset right after the last complete batch of this response; where a fetch-ahead request continues. */
    final long fetchAheadOffset;
    /** Size of the records of this response, used to bound how much data is fetched ahead. */
    final int sizeInBytes;
    /** True if the next fetch for this partition can be sent before this response has been consumed. */
    final boolean fetchAheadEligible;

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
            // Invalid data must be reported to the application at the point where it is consumed, so it is
            // not raised here. No fetch-ahead is done past data that cannot be scanned.
            scanned = false;
        }
        this.fetchAheadOffset = endOffset;
        this.sizeInBytes = fetchedRecords.sizeInBytes();
        this.fetchAheadEligible = scanned
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

    /** Makes this fetch one part of the response that the lease tracks. Call before the fetch is shared. */
    void receiveLease(ReceiveBufferPool.Lease lease, ByteBuffer responseBytes) {
        lease.attach(responseBytes);
        this.receiveLease = lease;
    }

    boolean isExhausted() {
        return appExhausted;
    }

    /** Makes the calling application thread the owner of the decoding state; waits for a running decoder. */
    private void takeOwnership() {
        int state = owner.get();
        if (state == APP || state == DONE || owner.compareAndSet(IDLE, APP))
            return;
        waiter = Thread.currentThread();
        while (owner.get() == DECODING)
            LockSupport.parkNanos(this, 200_000L);
        waiter = null;
    }

    private void syncAppView() {
        appNextFetchOffset = nextFetchOffset;
        appLastEpoch = lastEpoch;
        appExhausted = exhausted;
    }

    /**
     * Decodes all records of this fetch on the calling (decoder) thread, unless the application thread has
     * already started on it. An error is not raised here; it is kept, and the application thread raises it
     * when it reaches that record, as if it had decoded the records itself.
     */
    <K, V> void predecode(FetchConfig fetchConfig, Deserializers<K, V> deserializers, BufferSupplier bufferSupplier) {
        if (!owner.compareAndSet(IDLE, DECODING))
            return;
        List<ConsumerRecord<K, V>> decoded = new ArrayList<>();
        try {
            this.decompressionBufferSupplier = bufferSupplier;
            // a decoder thread decodes one fetch at a time, so it can use one array for all of them
            denseScratch = DECODER_SCRATCH.get();
            decode(fetchConfig, deserializers, Integer.MAX_VALUE, true, decoded);
        } finally {
            // keep the array for the next fetch only if this fetch is done with it
            if (denseRecords == null && !denseScratchEscaped && denseScratch != null) {
                DECODER_SCRATCH.set(denseScratch);
                denseScratch = null;
            } else {
                DECODER_SCRATCH.remove();
            }
            if (!decoded.isEmpty()) {
                @SuppressWarnings({"unchecked", "rawtypes"})
                List<ConsumerRecord<?, ?>> list = (List) decoded;
                predecoded = list;
            }
            owner.set(DONE);
            Thread w = waiter;
            if (w != null)
                LockSupport.unpark(w);
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
            // set first, so that a running decoder stops early
            this.isConsumed = true;
            // after isConsumed: whoever is told must already see that this data no longer counts
            Runnable drained = onDrained;
            if (drained != null)
                drained.run();
            takeOwnership();
            // The response bytes can be used again only if every record was decoded and handed out, without an
            // error, and no record that was handed out points into them.
            boolean decodedToTheEnd = exhausted && appExhausted && predecoded == null && denseRecords == null;
            boolean noError = cachedRecordException == null && !corruptLastRecord;
            boolean clean = decodedToTheEnd && noError && !receiveEscaped && !denseScratchEscaped;
            maybeCloseRecordStream();
            cachedRecordException = null;
            // taken under a lock, so that a fetch can release its part only once, whoever drains it
            ReceiveBufferPool.Lease lease;
            synchronized (this) {
                lease = receiveLease;
                receiveLease = null;
            }
            if (lease != null) {
                if (!clean)
                    lease.taint();
                lease.release();
            }
            if (predecoded != null) {
                // only count what was handed to the application; bytes are prorated
                int unread = predecoded.size() - predecodedPos;
                bytesRead = (int) ((long) bytesRead * (recordsRead - unread) / recordsRead);
                recordsRead -= unread;
                predecoded = null;
            }
            recordAggregatedMetrics(bytesRead, recordsRead);

            // we move the partition to the end if we received some bytes. This way, it's more likely that partitions
            // for the same topic can remain together (allowing for more efficient serialization).
            if (bytesRead > 0)
                subscriptions.movePartitionToEnd(partition);
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
            records.close();
            records = null;
        }
    }

    private Record nextFetchedRecord(FetchConfig fetchConfig) {
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

                if (denseEnabled && currentBatch instanceof DefaultRecordBatch && !currentBatch.isControlBatch()) {
                    DefaultRecordBatch batch = (DefaultRecordBatch) currentBatch;
                    if (denseScratchEscaped) {
                        // for a batch that is not compressed, the "scratch" bytes are the bytes of the response
                        receiveEscaped = true;
                        denseScratch = null;
                        denseScratchEscaped = false;
                    }
                    denseRecords = batch.uncompressedRecords(decompressionBufferSupplier, denseScratch);
                    if (batch.isCompressed())
                        denseScratch = denseRecords.array();
                    denseView = denseRecords.duplicate();
                    denseRemaining = batch.countOrNull();
                    denseBaseOffset = batch.baseOffset();
                    denseBaseTimestamp = batch.baseTimestamp();
                    denseTimestampType = batch.timestampType();
                    denseLogAppendTime = denseTimestampType == TimestampType.LOG_APPEND_TIME;
                    denseMaxTimestamp = denseLogAppendTime ? batch.maxTimestamp() : 0;
                    denseLeaderEpoch = lastEpoch;
                    return null;
                }

                // records of this path, and their headers, may point into the bytes of the response
                receiveEscaped = true;
                records = currentBatch.streamingIterator(decompressionBufferSupplier);
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
            int end = (int) Math.min((long) predecodedPos + maxRecords, predecoded.size());
            // A view, not a copy: storing each reference again costs more than decoding it did.
            @SuppressWarnings({"unchecked", "rawtypes"})
            List<ConsumerRecord<K, V>> all = (List) predecoded;
            List<ConsumerRecord<K, V>> records = predecodedPos == 0 && end == all.size() ? all : all.subList(predecodedPos, end);
            predecodedPos = end;
            if (end < predecoded.size()) {
                ConsumerRecord<K, V> last = records.get(records.size() - 1);
                appNextFetchOffset = last.offset() + 1;
                appLastEpoch = last.leaderEpoch();
            } else {
                predecoded = null;
                syncAppView();
            }
            return records;
        }
        try {
            return decode(fetchConfig, deserializers, maxRecords, false, new ArrayList<>());
        } finally {
            syncAppView();
        }
    }

    /**
     * @param quiet if true (decoder thread), an error is kept for the application thread instead of raised
     */
    private <K, V> List<ConsumerRecord<K, V>> decode(FetchConfig fetchConfig,
                                                     Deserializers<K, V> deserializers,
                                                     int maxRecords,
                                                     boolean quiet,
                                                     List<ConsumerRecord<K, V>> records) {
        // Error when fetching the next record before deserialization.
        if (corruptLastRecord)
            throw new KafkaException("Received exception when fetching the next record from " + partition
                    + ". If needed, please seek past the record to "
                    + "continue consumption.", cachedRecordException);

        if (isConsumed)
            return Collections.emptyList();

        denseEnabled = DENSE_DECODE && deserializers.simple();

        try {
            for (int i = 0; i < maxRecords && !(quiet && isConsumed); i++) {
                if (denseRecords != null) {
                    i += decodeDense(deserializers, records, maxRecords - i) - 1;
                    continue;
                }

                // Only move to next record if there was no exception in the last fetch. Otherwise, we should
                // use the last record to do deserialization again.
                if (cachedRecordException == null) {
                    corruptLastRecord = true;
                    lastRecord = nextFetchedRecord(fetchConfig);
                    corruptLastRecord = false;
                }

                if (lastRecord == null) {
                    if (denseRecords == null)
                        break;
                    // a batch was opened for dense decoding; the next round decodes it
                    i--;
                    continue;
                }

                Optional<Integer> leaderEpoch = maybeLeaderEpoch(currentBatch.partitionLeaderEpoch());
                TimestampType timestampType = currentBatch.timestampType();
                ConsumerRecord<K, V> record = parseRecord(deserializers, partition, leaderEpoch, timestampType, lastRecord);
                records.add(record);
                recordsRead++;
                bytesRead += lastRecord.sizeInBytes();
                nextFetchOffset = lastRecord.offset() + 1;
                // In some cases, the deserialization may have thrown an exception and the retry may succeed,
                // we allow user to move forward in this case.
                cachedRecordException = null;
            }
        } catch (SerializationException se) {
            cachedRecordException = se;
            if (records.isEmpty() && !quiet)
                throw se;
        } catch (KafkaException e) {
            cachedRecordException = e;
            if (records.isEmpty() && !quiet)
                throw new KafkaException("Received exception when fetching the next record from " + partition
                        + ". If needed, please seek past the record to "
                        + "continue consumption.", e);
        } catch (RuntimeException e) {
            if (!quiet)
                throw e;
            cachedRecordException = e;
        }
        return records;
    }

    private static Headers readOnlyHeaders() {
        RecordHeaders headers = new RecordHeaders();
        headers.setReadOnly();
        return headers;
    }

    /**
     * Decodes up to {@code maxRecords} records of the current batch straight from its bytes. The position in the
     * batch only moves past a record once that record has been decoded, so a failed record is tried again by
     * the next call, as on the general path.
     *
     * @return the number of records added to {@code out}
     */
    private <K, V> int decodeDense(Deserializers<K, V> deserializers, List<ConsumerRecord<K, V>> out, int maxRecords) {
        final ByteBuffer buffer = denseRecords;
        final Deserializer<K> keyDeserializer = deserializers.keyDeserializer();
        final Deserializer<V> valueDeserializer = deserializers.valueDeserializer();
        final String topic = partition.topic();
        int added = 0;

        while (added < maxRecords && denseRemaining > 0) {
            final int start = buffer.position();
            final int end;
            final long offset;
            final long timestamp;
            final int keySize;
            final int keyPosition;
            final int valueSize;
            final int valuePosition;
            final Header[] headers;

            corruptLastRecord = true;
            try {
                int size = ByteUtils.readVarint(buffer);
                if (size < 0 || buffer.remaining() < size)
                    throw new InvalidRecordException("Invalid record size: expected " + size +
                            " bytes in record payload, but instead the buffer has only " + buffer.remaining() +
                            " remaining bytes.");
                end = buffer.position() + size;
                buffer.get(); // attributes, unused
                long timestampDelta = ByteUtils.readVarlong(buffer);
                offset = denseBaseOffset + ByteUtils.readVarint(buffer);
                if (offset < nextFetchOffset) {
                    // a record before the fetch position: skip it without decoding
                    buffer.position(end);
                    denseRemaining--;
                    corruptLastRecord = false;
                    continue;
                }
                timestamp = denseLogAppendTime ? denseMaxTimestamp : denseBaseTimestamp + timestampDelta;
                keySize = ByteUtils.readVarint(buffer);
                keyPosition = buffer.position();
                if (keySize > 0)
                    buffer.position(keyPosition + keySize);
                valueSize = ByteUtils.readVarint(buffer);
                valuePosition = buffer.position();
                if (valueSize > 0)
                    buffer.position(valuePosition + valueSize);
                int numHeaders = ByteUtils.readVarint(buffer);
                if (numHeaders < 0)
                    throw new InvalidRecordException("Found invalid number of record headers " + numHeaders);
                headers = numHeaders == 0 ? null : DefaultRecord.readHeaders(buffer, numHeaders);
                if (headers != null)
                    denseScratchEscaped = true;
                if (buffer.position() != end)
                    throw new InvalidRecordException("Invalid record size: expected to read " + (end - start) +
                            " bytes in record payload, but instead read " + (buffer.position() - start));
            } catch (java.nio.BufferUnderflowException | IllegalArgumentException e) {
                buffer.position(start);
                denseScratchEscaped = true;
                throw new InvalidRecordException("Found invalid record structure", e);
            } catch (InvalidRecordException e) {
                buffer.position(start);
                denseScratchEscaped = true;
                throw e;
            }
            corruptLastRecord = false;

            // these deserializers do not read the headers, so records without headers share one empty instance
            // for the call; the record itself gets its own, because the application may add headers to it
            Headers recordHeaders = new RecordHeaders(headers);
            K key;
            V value;
            try {
                key = keySize < 0 ? null : keyDeserializer.deserialize(topic, headers == null ? NO_HEADERS : recordHeaders,
                        view(keyPosition, keySize));
            } catch (RuntimeException e) {
                buffer.position(start);
                denseScratchEscaped = true;
                log.error("Key Deserializers with error: {}", deserializers);
                throw denseDeserializationException(DeserializationExceptionOrigin.KEY, offset, timestamp, keyPosition, keySize,
                        valuePosition, valueSize, recordHeaders, e);
            }
            try {
                value = valueSize < 0 ? null : valueDeserializer.deserialize(topic, headers == null ? NO_HEADERS : recordHeaders,
                        view(valuePosition, valueSize));
            } catch (RuntimeException e) {
                buffer.position(start);
                denseScratchEscaped = true;
                log.error("Value Deserializers with error: {}", deserializers);
                throw denseDeserializationException(DeserializationExceptionOrigin.VALUE, offset, timestamp, keyPosition, keySize,
                        valuePosition, valueSize, recordHeaders, e);
            }

            out.add(new ConsumerRecord<>(topic, partition.partition(), offset, timestamp, denseTimestampType,
                    keySize < 0 ? ConsumerRecord.NULL_SIZE : keySize,
                    valueSize < 0 ? ConsumerRecord.NULL_SIZE : valueSize,
                    key, value, recordHeaders, denseLeaderEpoch));
            recordsRead++;
            bytesRead += end - start;
            nextFetchOffset = offset + 1;
            cachedRecordException = null;
            denseRemaining--;
            added++;
        }

        if (denseRemaining == 0) {
            denseRecords = null;
            denseView = null;
        }
        return added;
    }

    /** A view of a part of the current batch; it is reused, so it is only given to deserializers that do not keep it. */
    private ByteBuffer view(int position, int size) {
        denseView.clear();
        denseView.position(position);
        denseView.limit(position + size);
        return denseView;
    }

    private ByteBuffer copyOfView(int position, int size) {
        return size < 0 ? null : view(position, size).slice();
    }

    private RecordDeserializationException denseDeserializationException(DeserializationExceptionOrigin origin,
                                                                         long offset,
                                                                         long timestamp,
                                                                         int keyPosition,
                                                                         int keySize,
                                                                         int valuePosition,
                                                                         int valueSize,
                                                                         Headers headers,
                                                                         RuntimeException e) {
        return new RecordDeserializationException(origin, partition, offset, timestamp, denseTimestampType,
                copyOfView(keyPosition, keySize), copyOfView(valuePosition, valueSize), headers,
                "Error deserializing " + origin.name() + " for partition " + partition + " at offset " + offset
                        + ". If needed, please seek past the record to continue consumption.", e);
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
        return new RecordDeserializationException(origin, partition, record.offset(), record.timestamp(), timestampType, record.key(), record.value(), headers,
                "Error deserializing " + origin.name() + " for partition " + partition + " at offset " + record.offset()
                        + ". If needed, please seek past the record to continue consumption.", e);
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
