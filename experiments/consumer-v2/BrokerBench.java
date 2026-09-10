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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public final class BrokerBench {
    static final int FETCH_BYTES = 1024 * 1024;
    static final int POLL_RECORDS = 500;
    static final com.sun.management.OperatingSystemMXBean OS =
        (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    static void setup(String bootstrap, String topic, int count, int size) throws Exception {
        Properties config = new Properties(); config.put("bootstrap.servers", bootstrap);
        try (Admin admin = Admin.create(config)) {
            if (admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).size() != 1) throw new AssertionError("Expected one owned broker");
            admin.createTopics(Collections.singleton(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        }
        config.put("key.serializer", ByteArraySerializer.class.getName());
        config.put("value.serializer", ByteArraySerializer.class.getName());
        config.put("acks", "all"); config.put("linger.ms", "5"); config.put("batch.size", "65536");
        config.put("compression.type", "none"); config.put("buffer.memory", "33554432");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
            for (int i = 0; i < count; i++) {
                byte[] value = new byte[size]; value[0] = (byte) i;
                producer.send(new ProducerRecord<>(topic, 0, null, value));
            }
            producer.flush();
        }
        try (Admin admin = Admin.create(config)) {
            long end = admin.listOffsets(Collections.singletonMap(new TopicPartition(topic, 0), OffsetSpec.latest()))
                .all().get(10, TimeUnit.SECONDS).values().iterator().next().offset();
            if (end != count) throw new AssertionError("Preload end offset mismatch: " + end);
        }
        System.out.println("PRELOADED " + count);
    }
    interface Reader extends AutoCloseable {
        Iterable<ConsumerRecord<byte[], byte[]>> poll();
        void seek();
        void close();
    }
    static Reader original(String bootstrap, String topic) {
        Properties props = new Properties(); props.put("bootstrap.servers", bootstrap);
        props.put("group.protocol", "consumer"); props.put("enable.auto.commit", "false");
        props.put("isolation.level", "read_committed"); props.put("check.crcs", "true");
        props.put("max.poll.records", "500"); props.put("fetch.max.bytes", Integer.toString(FETCH_BYTES));
        props.put("max.partition.fetch.bytes", Integer.toString(FETCH_BYTES));
        props.put("fetch.min.bytes", "1"); props.put("fetch.max.wait.ms", "1");
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        TopicPartition tp = new TopicPartition(topic, 0);
        consumer.assign(Collections.singleton(tp)); consumer.seek(tp, 0);
        return new Reader() {
            public Iterable<ConsumerRecord<byte[], byte[]>> poll() { return consumer.poll(Duration.ofSeconds(2)); }
            public void seek() { consumer.seek(tp, 0); }
            public void close() { consumer.close(Duration.ofSeconds(5)); }
        };
    }
    static Reader candidate(int port, String topic, int depth) throws Exception {
        NetworkFetchTransport transport = new NetworkFetchTransport(port, topic, FETCH_BYTES, true, 1);
        FetchBridge bridge = new FetchBridge((long) FETCH_BYTES * depth, depth, transport, () -> { });
        AppFetchDecoder app = new AppFetchDecoder(bridge, topic);
        return new Reader() {
            public Iterable<ConsumerRecord<byte[], byte[]>> poll() { return app.poll(2_000_000_000L, POLL_RECORDS); }
            public void seek() {
                if (!app.trySeek(0)) throw new AssertionError("Seek backpressure after full dataset");
            }
            public void close() { try { app.close(); } finally { bridge.close(); } }
        };
    }
    static long expected;
    static long consumed;
    static long[] latency = new long[64];
    static long polls;
    static void window(Reader reader, int count, int size, long seconds, boolean measure) {
        long end = System.nanoTime() + seconds * 1_000_000_000L;
        long progress = System.nanoTime();
        while (System.nanoTime() < end) {
            long start = System.nanoTime();
            Iterable<ConsumerRecord<byte[], byte[]>> records = reader.poll();
            long elapsed = System.nanoTime() - start;
            if (measure) { latency[63 - Long.numberOfLeadingZeros(Math.max(1, elapsed))]++; polls++; }
            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (record.offset() != expected || record.value().length != size || record.value()[0] != (byte) expected)
                    throw new AssertionError("Data mismatch at " + expected + " got " + record.offset());
                expected++; consumed++; progress = System.nanoTime();
            }
            if (expected == count) { reader.seek(); expected = 0; }
            if (System.nanoTime() - progress > 5_000_000_000L) throw new AssertionError("No progress");
        }
    }
    public static void main(String[] args) throws Exception {
        String mode = args[0]; int port = Integer.parseInt(args[1]); String topic = args[2];
        int count = Integer.parseInt(args[3]); int size = Integer.parseInt(args[4]);
        String bootstrap = "127.0.0.1:" + port;
        if (mode.equals("setup")) { setup(bootstrap, topic, count, size); return; }
        try (Reader reader = mode.equals("original") ? original(bootstrap, topic) : candidate(port, topic, Integer.parseInt(mode))) {
            window(reader, count, size, 3, false);
            long beforeCount = consumed;
            for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) pool.resetPeakUsage();
            long beforeCpu = OS.getProcessCpuTime(); long start = System.nanoTime();
            window(reader, count, size, Long.parseLong(args[5]), true);
            double seconds = (System.nanoTime() - start) / 1e9;
            double cpuSeconds = (OS.getProcessCpuTime() - beforeCpu) / 1e9;
            long records = consumed - beforeCount; long peak = 0;
            for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans())
                if (pool.getType() == java.lang.management.MemoryType.HEAP) peak += pool.getPeakUsage().getUsed();
            long threshold = (long) Math.ceil(polls * .99); long seen = 0; double p99Upper = 0;
            for (int i = 0; i < latency.length; i++) { seen += latency[i]; if (seen >= threshold) { p99Upper = Math.pow(2, i + 1) / 1e6; break; } }
            System.out.printf(java.util.Locale.ROOT,
                "RESULT {\"mode\":\"%s\",\"size\":%d,\"records\":%d,\"seconds\":%.6f,\"cpu_seconds\":%.6f,\"MBps\":%.6f,\"cpu_seconds_per_GB\":%.6f,\"poll_p99_upper_ms\":%.6f,\"heap_pool_peaks_sum_bytes\":%d}%n",
                mode, size, records, seconds, cpuSeconds, records * (double) size / seconds / 1e6,
                cpuSeconds / (records * (double) size / 1e9), p99Upper, peak);
        }
    }
}
