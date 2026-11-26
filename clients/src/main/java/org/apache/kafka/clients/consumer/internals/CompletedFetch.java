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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

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
    private volatile boolean isConsumed = false;
    private boolean exhausted = false;
    private boolean initialized = false;

    // The buffer of the response may go back to the pool only if no object that outlives this fetch points into
    // it: no record, header or exception that was handed out. Records decoded by the reusing iterator keep
    // copies of their key and value, but their headers point into the records. Records of the streaming iterator
    // may point into the response with their key, value and headers.
    //
    // True once something that was handed out may point into the bytes of the response. Only the thread that
    // reads the records uses it.
    private boolean responseMayBeReferenced;
    // True once all records were read and nothing that was handed out points into the response. It is set by
    // the thread that reads the records and read by the thread that drains this fetch.
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
        return nextFetchOffset;
    }

    Optional<Integer> lastEpoch() {
        return lastEpoch;
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
        return exhausted;
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
            maybeCloseRecordStream();
            cachedRecordException = null;
            this.isConsumed = true;
            releaseResponse();
            recordAggregatedMetrics(bytesRead, recordsRead);

            // we move the partition to the end if we received some bytes. This way, it's more likely that partitions
            // for the same topic can remain together (allowing for more efficient serialization).
            if (bytesRead > 0)
                subscriptions.movePartitionToEnd(partition);
        }
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
                    responseReusable = !responseMayBeReferenced;
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
                    records = ((DefaultRecordBatch) currentBatch).reusingIterator(decompressionBufferSupplier, decompressionArray);
                    // the iterator owns the array until it is closed
                    decompressionArray = null;
                } else {
                    // The records of this iterator, and their headers, may point into the response.
                    responseMayBeReferenced = true;
                    records = currentBatch.streamingIterator(decompressionBufferSupplier);
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
        // Error when fetching the next record before deserialization.
        if (corruptLastRecord)
            throw new KafkaException("Received exception when fetching the next record from " + partition
                    + ". If needed, please seek past the record to "
                    + "continue consumption.", cachedRecordException);

        if (isConsumed)
            return Collections.emptyList();

        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        // The reused Record gives out buffers that are valid only until the next record. Only deserializers that
        // do not keep these buffers can use it.
        reuseRecords = deserializers.simple();

        try {
            for (int i = 0; i < maxRecords; i++) {
                // Only move to next record if there was no exception in the last fetch. Otherwise, we should
                // use the last record to do deserialization again.
                if (cachedRecordException == null) {
                    corruptLastRecord = true;
                    lastRecord = nextFetchedRecord(fetchConfig);
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
                // In some cases, the deserialization may have thrown an exception and the retry may succeed,
                // we allow user to move forward in this case.
                cachedRecordException = null;
            }
        } catch (SerializationException se) {
            cachedRecordException = se;
            if (records.isEmpty())
                throw se;
        } catch (KafkaException e) {
            cachedRecordException = e;
            if (records.isEmpty())
                throw new KafkaException("Received exception when fetching the next record from " + partition
                        + ". If needed, please seek past the record to "
                        + "continue consumption.", e);
        } finally {
            // An error may hold parts of the record, so the response is not reused after one.
            if (cachedRecordException != null || corruptLastRecord)
                responseMayBeReferenced = true;
        }
        return records;
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
