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

package org.apache.kafka.jmh.consumer;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.sun.management.OperatingSystemMXBean;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end benchmark of the async consumer ({@code group.protocol=consumer}, auto-commit on) against a
 * running broker. Each benchmark invocation is one {@code poll()} call, so the primary result is polls per
 * second; the {@code records} auxiliary counter gives records per second.
 *
 * <p>Configuration is read from system properties (pass them with {@code -jvmArgs} or {@code -D} on the
 * {@code java -jar} command line):
 * <ul>
 *     <li>{@code kafka.bench.bootstrap} (default {@value #DEFAULT_BOOTSTRAP}): the broker.</li>
 *     <li>{@code kafka.bench.topic} (default {@value #DEFAULT_TOPIC}): the topic to subscribe to. Its records
 *     must start with an 8-byte big-endian sequence number equal to the record offset.</li>
 *     <li>{@code kafka.bench.records} (default {@value #DEFAULT_RECORDS}): records per pass in
 *     {@code consume} mode; the consumer seeks back to the beginning once a pass has read this many.</li>
 *     <li>{@code kafka.bench.mode} (default {@code consume}): {@code consume} reads and validates records;
 *     {@code idle} sits at the log end and polls for {@value #IDLE_POLL_MS} ms; {@code unavailable} points
 *     the consumer at {@code kafka.bench.unavailable} (default {@value #DEFAULT_UNAVAILABLE_BOOTSTRAP}, a
 *     closed port) so the coordinator is never found.</li>
 * </ul>
 *
 * <p>Auxiliary counters ({@link AuxCounters.Type#OPERATIONS}, reported per second): {@code records},
 * {@code emptyPolls}, {@code seeksToBeginning}, {@code processCpuMillis} (process CPU time,
 * {@link OperatingSystemMXBean#getProcessCpuTime()}; 1000 = one core busy) and {@code networkThreadCpuMillis}
 * (CPU time of the threads whose name contains {@value #NETWORK_THREAD_NAME_FRAGMENT}, via
 * {@link ThreadMXBean#getThreadCpuTime(long)}; {@code 0} when thread CPU time is unsupported).
 */
@State(Scope.Thread)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class AsyncConsumerBrokerBenchmark {

    public enum BenchMode {
        CONSUME,
        IDLE,
        UNAVAILABLE
    }

    private static final String BOOTSTRAP_PROPERTY = "kafka.bench.bootstrap";
    private static final String UNAVAILABLE_PROPERTY = "kafka.bench.unavailable";
    private static final String TOPIC_PROPERTY = "kafka.bench.topic";
    private static final String RECORDS_PROPERTY = "kafka.bench.records";
    private static final String MODE_PROPERTY = "kafka.bench.mode";
    private static final String DEFAULT_BOOTSTRAP = "127.0.0.1:9092";
    private static final String DEFAULT_UNAVAILABLE_BOOTSTRAP = "127.0.0.1:1";
    private static final String DEFAULT_TOPIC = "jmh-input";
    private static final long DEFAULT_RECORDS = 2_000_000L;
    private static final long CONSUME_POLL_MS = 100;
    private static final long IDLE_POLL_MS = 1000;
    private static final long ASSIGNMENT_TIMEOUT_MS = 60_000;
    private static final String NETWORK_THREAD_NAME_FRAGMENT = "consumer_background_thread";
    private static final int SEQUENCE_PREFIX_BYTES = Long.BYTES;
    private static final long UNKNOWN_OFFSET = -1L;

    private BenchMode mode;
    private long recordCount;
    private Duration pollTimeout;
    private KafkaConsumer<byte[], byte[]> consumer;
    private Set<TopicPartition> assignment;
    /** Next expected offset per partition index; {@link #UNKNOWN_OFFSET} until the first record is seen. */
    private long[] nextOffsets;
    private long consumedInPass;

    /**
     * Per-second counters. JMH reports {@link AuxCounters.Type#OPERATIONS} counters as {@code counter/s},
     * so the CPU counters are CPU milliseconds per wall-clock second (1000 = one core).
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.OPERATIONS)
    public static class Counters {
        public long records;
        public long emptyPolls;
        public long seeksToBeginning;
        public long processCpuMillis;
        public long networkThreadCpuMillis;

        private final OperatingSystemMXBean os = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        private final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        private long processCpuStartNs;
        private long networkThreadCpuStartNs;

        @Setup(Level.Iteration)
        public void start() {
            processCpuStartNs = os.getProcessCpuTime();
            networkThreadCpuStartNs = networkThreadCpuNs();
        }

        @TearDown(Level.Iteration)
        public void stop() {
            processCpuMillis = TimeUnit.NANOSECONDS.toMillis(os.getProcessCpuTime() - processCpuStartNs);
            networkThreadCpuMillis = TimeUnit.NANOSECONDS.toMillis(networkThreadCpuNs() - networkThreadCpuStartNs);
        }

        private long networkThreadCpuNs() {
            if (!threads.isThreadCpuTimeSupported())
                return 0L;
            long total = 0L;
            for (ThreadInfo info : threads.getThreadInfo(threads.getAllThreadIds())) {
                if (info == null || !info.getThreadName().contains(NETWORK_THREAD_NAME_FRAGMENT))
                    continue;
                long cpuNs = threads.getThreadCpuTime(info.getThreadId());
                if (cpuNs > 0)
                    total += cpuNs;
            }
            return total;
        }
    }

    @Setup(Level.Trial)
    public void setup() {
        mode = BenchMode.valueOf(System.getProperty(MODE_PROPERTY, "consume").toUpperCase(Locale.ROOT));
        recordCount = Long.getLong(RECORDS_PROPERTY, DEFAULT_RECORDS);
        String topic = System.getProperty(TOPIC_PROPERTY, DEFAULT_TOPIC);
        String bootstrap = mode == BenchMode.UNAVAILABLE
            ? System.getProperty(UNAVAILABLE_PROPERTY, DEFAULT_UNAVAILABLE_BOOTSTRAP)
            : System.getProperty(BOOTSTRAP_PROPERTY, DEFAULT_BOOTSTRAP);
        pollTimeout = Duration.ofMillis(mode == BenchMode.IDLE ? IDLE_POLL_MS : CONSUME_POLL_MS);

        consumer = new KafkaConsumer<>(consumerProps(bootstrap));
        consumer.subscribe(List.of(topic));
        if (mode == BenchMode.UNAVAILABLE)
            return;

        assignment = waitForAssignment();
        int maxPartition = assignment.stream().mapToInt(TopicPartition::partition).max().orElse(0);
        nextOffsets = new long[maxPartition + 1];
        if (mode == BenchMode.CONSUME) {
            consumer.seekToBeginning(assignment);
            resetPass();
        } else {
            consumer.seekToEnd(assignment);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (consumer != null)
            consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)));
    }

    @Benchmark
    public long poll(Counters counters) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
        int count = records.count();
        if (count == 0) {
            counters.emptyPolls++;
            return 0L;
        }
        if (mode != BenchMode.CONSUME)
            throw new IllegalStateException("Unexpected " + count + " records in " + mode + " mode");

        for (ConsumerRecord<byte[], byte[]> record : records)
            validate(record);
        counters.records += count;
        consumedInPass += count;
        if (consumedInPass >= recordCount) {
            consumer.seekToBeginning(assignment);
            resetPass();
            counters.seeksToBeginning++;
        }
        return count;
    }

    private void validate(ConsumerRecord<byte[], byte[]> record) {
        int partition = record.partition();
        long expected = nextOffsets[partition];
        if (expected != UNKNOWN_OFFSET && record.offset() != expected)
            throw new IllegalStateException("Offset gap on partition " + partition + ": expected " + expected
                + " but got " + record.offset());
        byte[] value = record.value();
        if (value == null || value.length < SEQUENCE_PREFIX_BYTES)
            throw new IllegalStateException("Record at offset " + record.offset() + " has no sequence prefix");
        long sequence = ByteBuffer.wrap(value).getLong();
        if (sequence != record.offset())
            throw new IllegalStateException("Sequence mismatch at offset " + record.offset() + ": " + sequence);
        nextOffsets[partition] = record.offset() + 1;
    }

    private void resetPass() {
        consumedInPass = 0;
        Arrays.fill(nextOffsets, UNKNOWN_OFFSET);
    }

    private Set<TopicPartition> waitForAssignment() {
        long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ASSIGNMENT_TIMEOUT_MS);
        while (consumer.assignment().isEmpty()) {
            if (System.nanoTime() >= deadlineNs)
                throw new IllegalStateException("No partitions assigned after " + ASSIGNMENT_TIMEOUT_MS + " ms");
            consumer.poll(Duration.ofMillis(250));
        }
        return Set.copyOf(consumer.assignment());
    }

    private static Map<String, Object> consumerProps(String bootstrap) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "jmh-" + UUID.randomUUID());
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return props;
    }
}
