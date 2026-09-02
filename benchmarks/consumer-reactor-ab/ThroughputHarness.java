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

package org.apache.kafka.tools.reactorbenchmark;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public final class ThroughputHarness {
    private ThroughputHarness() {
    }

    public static void main(String[] args) {
        Thread.currentThread().setName("consumer-reactor-throughput-application");
        Map<String, String> options = parseArgs(args);
        String bootstrapServers = required(options, "bootstrap-server");
        String topic = required(options, "topic");
        long warmupRecords = positiveLongOption(options, "warmup-records");
        long measurementRecords = positiveLongOption(options, "measurement-records");
        long timeoutMs = positiveLongOption(options, "timeout-ms");

        Properties properties = consumerProperties(bootstrapServers);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            consumeAtLeast(consumer, warmupRecords, timeoutMs);

            long cpuStartNs = processCpuTimeNs();
            long wallStartNs = System.nanoTime();
            Consumption measured = consumeAtLeast(consumer, measurementRecords, timeoutMs);
            long wallNs = System.nanoTime() - wallStartNs;
            long cpuNs = processCpuTimeNs() - cpuStartNs;

            double seconds = wallNs / 1_000_000_000.0;
            double recordsPerSecond = measured.records / seconds;
            double mbPerSecond = measured.bytes / (1024.0 * 1024.0) / seconds;
            double cpuNsPerRecord = (double) cpuNs / measured.records;
            System.out.printf(
                Locale.ROOT,
                "RESULT,throughput,%.3f,%.3f,%.6f,%d,%d,%.6f,%.6f,%.3f,%.3f,%.3f%n",
                wallNs / 1_000_000.0,
                cpuNs / 1_000_000.0,
                100.0 * cpuNs / wallNs,
                measured.records,
                measured.bytes,
                recordsPerSecond,
                mbPerSecond,
                cpuNsPerRecord,
                metric(consumer, "time-between-network-thread-poll-avg"),
                metric(consumer, "time-between-network-thread-poll-max")
            );
        }
    }

    private static Consumption consumeAtLeast(KafkaConsumer<byte[], byte[]> consumer,
                                               long targetRecords,
                                               long timeoutMs) {
        long records = 0L;
        long bytes = 0L;
        long lastProgressNs = System.nanoTime();
        long timeoutNs = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (records < targetRecords) {
            ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofMillis(100));
            if (!batch.isEmpty()) {
                lastProgressNs = System.nanoTime();
            } else if (System.nanoTime() - lastProgressNs > timeoutNs) {
                throw new IllegalStateException(
                    "Timed out after consuming " + records + " of " + targetRecords + " records"
                );
            }
            records += batch.count();
            for (ConsumerRecord<byte[], byte[]> record : batch) {
                if (record.key() != null) {
                    bytes += record.key().length;
                }
                if (record.value() != null) {
                    bytes += record.value().length;
                }
            }
        }
        return new Consumption(records, bytes);
    }

    private static Properties consumerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "reactor-throughput-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "reactor-throughput-" + UUID.randomUUID());
        return properties;
    }

    private static double metric(KafkaConsumer<byte[], byte[]> consumer, String name) {
        for (Map.Entry<MetricName, ? extends Metric> entry : consumer.metrics().entrySet()) {
            if (name.equals(entry.getKey().name()) && entry.getValue().metricValue() instanceof Number) {
                return ((Number) entry.getValue().metricValue()).doubleValue();
            }
        }
        return Double.NaN;
    }

    private static long processCpuTimeNs() {
        return ProcessHandle.current().info().totalCpuDuration()
            .orElseThrow(() -> new IllegalStateException("Process CPU time is unavailable"))
            .toNanos();
    }

    private static Map<String, String> parseArgs(String[] args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Arguments must be --name value pairs");
        }
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("Expected option name, found: " + args[i]);
            }
            options.put(args[i].substring(2), args[i + 1]);
        }
        return options;
    }

    private static String required(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required option --" + name);
        }
        return value;
    }

    private static long positiveLongOption(Map<String, String> options, String name) {
        long value = Long.parseLong(required(options, name));
        if (value <= 0) {
            throw new IllegalArgumentException("--" + name + " must be positive");
        }
        return value;
    }

    private static final class Consumption {
        private final long records;
        private final long bytes;

        private Consumption(long records, long bytes) {
            this.records = records;
            this.bytes = bytes;
        }
    }
}
