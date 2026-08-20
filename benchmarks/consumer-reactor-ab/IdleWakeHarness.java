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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class IdleWakeHarness {
    private static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(30);
    private static final byte[] VALUE = new byte[128];

    private IdleWakeHarness() {
    }

    public static void main(String[] args) throws Exception {
        Thread.currentThread().setName("consumer-reactor-ab-application");
        Map<String, String> options = parseArgs(args);
        String bootstrapServers = required(options, "bootstrap-server");
        String scenario = options.getOrDefault("scenario", "idle");
        String topic = "consumer-reactor-ab-" + UUID.randomUUID();

        createTopic(bootstrapServers, topic);
        try {
            if ("idle".equals(scenario)) {
                runIdle(
                    bootstrapServers,
                    topic,
                    positiveLongOption(options, "duration-ms", 60_000L)
                );
            } else if ("first-record".equals(scenario)) {
                runFirstRecord(
                    bootstrapServers,
                    topic,
                    nonNegativeIntOption(options, "warmup-samples", 10),
                    positiveIntOption(options, "samples", 100),
                    nonNegativeLongOption(options, "idle-ms", 1_000L),
                    positiveLongOption(options, "poll-timeout-ms", 30_000L)
                );
            } else {
                throw new IllegalArgumentException("Unknown scenario: " + scenario);
            }
        } finally {
            deleteTopic(bootstrapServers, topic);
        }
    }

    private static void runIdle(String bootstrapServers, String topic, long durationMs) {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties(bootstrapServers))) {
            initializePosition(consumer, topic);
            consumer.poll(Duration.ofMillis(500));

            long cpuStartNs = processCpuTimeNs();
            long wallStartNs = System.nanoTime();
            long deadlineNs = wallStartNs + TimeUnit.MILLISECONDS.toNanos(durationMs);
            int pollCalls = 0;
            int recordCount = 0;
            while (true) {
                long remainingNs = deadlineNs - System.nanoTime();
                if (remainingNs <= 0) {
                    break;
                }
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofNanos(remainingNs));
                pollCalls++;
                recordCount += records.count();
            }
            long wallNs = System.nanoTime() - wallStartNs;
            long cpuNs = processCpuTimeNs() - cpuStartNs;
            if (recordCount != 0) {
                throw new IllegalStateException("Idle topic unexpectedly contained " + recordCount + " records");
            }

            printResult(
                "idle",
                wallNs,
                cpuNs,
                recordCount,
                pollCalls,
                Double.NaN,
                Double.NaN,
                Double.NaN,
                metric(consumer, "time-between-network-thread-poll-avg"),
                metric(consumer, "time-between-network-thread-poll-max")
            );
        }
    }

    private static void runFirstRecord(String bootstrapServers,
                                       String topic,
                                       int warmupSamples,
                                       int samples,
                                       long idleMs,
                                       long pollTimeoutMs) throws Exception {
        Properties producerProperties = producerProperties(bootstrapServers);
        ScheduledExecutorService sender = Executors.newSingleThreadScheduledExecutor();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties(bootstrapServers));
             KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProperties)) {
            initializePosition(consumer, topic);

            for (int i = 0; i < warmupSamples; i++) {
                measureFirstRecord(consumer, producer, sender, topic, idleMs, pollTimeoutMs);
            }

            List<Double> latencyMs = new ArrayList<>(samples);
            long cpuStartNs = processCpuTimeNs();
            long wallStartNs = System.nanoTime();
            for (int i = 0; i < samples; i++) {
                double sample = measureFirstRecord(consumer, producer, sender, topic, idleMs, pollTimeoutMs);
                latencyMs.add(sample);
                System.out.printf(Locale.ROOT, "SAMPLE,first-record,%d,%.6f%n", i, sample);
            }
            long wallNs = System.nanoTime() - wallStartNs;
            long cpuNs = processCpuTimeNs() - cpuStartNs;

            Collections.sort(latencyMs);
            printResult(
                "first-record",
                wallNs,
                cpuNs,
                samples,
                0,
                percentile(latencyMs, 0.50),
                percentile(latencyMs, 0.95),
                percentile(latencyMs, 0.99),
                metric(consumer, "time-between-network-thread-poll-avg"),
                metric(consumer, "time-between-network-thread-poll-max")
            );
        } finally {
            sender.shutdownNow();
            sender.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    private static double measureFirstRecord(KafkaConsumer<byte[], byte[]> consumer,
                                             KafkaProducer<byte[], byte[]> producer,
                                             ScheduledExecutorService sender,
                                             String topic,
                                             long idleMs,
                                             long pollTimeoutMs) throws Exception {
        AtomicLong sendStartNs = new AtomicLong(-1L);
        var sendFuture = sender.schedule(() -> {
            try {
                sendStartNs.set(System.nanoTime());
                producer.send(new ProducerRecord<>(topic, VALUE)).get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, idleMs, TimeUnit.MILLISECONDS);

        long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(idleMs + pollTimeoutMs);
        ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
        long receivedNs = -1L;
        while (records.isEmpty()) {
            long remainingNs = deadlineNs - System.nanoTime();
            if (remainingNs <= 0) {
                throw new IllegalStateException("Timed out waiting for first record after idle period");
            }
            records = consumer.poll(Duration.ofNanos(remainingNs));
            receivedNs = System.nanoTime();
            if (records.isEmpty() && sendFuture.isDone()) {
                // Surface producer failures immediately instead of waiting for the consumer deadline.
                sendFuture.get(30, TimeUnit.SECONDS);
            }
        }
        sendFuture.get(30, TimeUnit.SECONDS);
        return (receivedNs - sendStartNs.get()) / 1_000_000.0;
    }

    private static void initializePosition(KafkaConsumer<byte[], byte[]> consumer, String topic) {
        TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(List.of(partition));
        consumer.seekToEnd(List.of(partition));
        consumer.position(partition, ADMIN_TIMEOUT);
    }

    private static Properties consumerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-reactor-ab-consumer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return properties;
    }

    private static Properties producerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "consumer-reactor-ab-producer-" + UUID.randomUUID());
        return properties;
    }

    private static void createTopic(String bootstrapServers, String topic) throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "consumer-reactor-ab-admin-" + UUID.randomUUID());
        try (Admin admin = Admin.create(properties)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private static void deleteTopic(String bootstrapServers, String topic) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "consumer-reactor-ab-admin-" + UUID.randomUUID());
        try (Admin admin = Admin.create(properties)) {
            admin.deleteTopics(List.of(topic)).all().get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to delete benchmark topic " + topic, e);
        }
    }

    private static double metric(KafkaConsumer<byte[], byte[]> consumer, String name) {
        for (Map.Entry<MetricName, ? extends Metric> entry : consumer.metrics().entrySet()) {
            if (name.equals(entry.getKey().name()) && entry.getValue().metricValue() instanceof Number) {
                return ((Number) entry.getValue().metricValue()).doubleValue();
            }
        }
        return Double.NaN;
    }

    private static void printResult(String scenario,
                                    long wallNs,
                                    long cpuNs,
                                    long records,
                                    long pollCalls,
                                    double p50Ms,
                                    double p95Ms,
                                    double p99Ms,
                                    double networkPollAvgMs,
                                    double networkPollMaxMs) {
        double wallMs = wallNs / 1_000_000.0;
        double cpuMs = cpuNs / 1_000_000.0;
        double cpuPercent = wallNs <= 0 ? Double.NaN : 100.0 * cpuNs / wallNs;
        double pollCallsPerSecond = wallNs <= 0 ? Double.NaN : pollCalls * 1_000_000_000.0 / wallNs;
        double networkPollRateHz = networkPollAvgMs <= 0 ? Double.NaN : 1_000.0 / networkPollAvgMs;
        System.out.printf(
            Locale.ROOT,
            "RESULT,%s,%.3f,%.3f,%.6f,%d,%d,%.6f,%.3f,%.3f,%.3f,%.3f,%.3f,%.6f%n",
            scenario,
            wallMs,
            cpuMs,
            cpuPercent,
            records,
            pollCalls,
            pollCallsPerSecond,
            p50Ms,
            p95Ms,
            p99Ms,
            networkPollAvgMs,
            networkPollMaxMs,
            networkPollRateHz
        );
    }

    private static double percentile(List<Double> sortedValues, double percentile) {
        if (sortedValues.isEmpty()) {
            return Double.NaN;
        }
        int index = Math.max(0, (int) Math.ceil(percentile * sortedValues.size()) - 1);
        return sortedValues.get(index);
    }

    private static long processCpuTimeNs() {
        return ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime();
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

    private static int positiveIntOption(Map<String, String> options, String name, int defaultValue) {
        int value = Integer.parseInt(options.getOrDefault(name, Integer.toString(defaultValue)));
        if (value <= 0) {
            throw new IllegalArgumentException("--" + name + " must be positive");
        }
        return value;
    }

    private static int nonNegativeIntOption(Map<String, String> options, String name, int defaultValue) {
        int value = Integer.parseInt(options.getOrDefault(name, Integer.toString(defaultValue)));
        if (value < 0) {
            throw new IllegalArgumentException("--" + name + " must not be negative");
        }
        return value;
    }

    private static long positiveLongOption(Map<String, String> options, String name, long defaultValue) {
        long value = nonNegativeLongOption(options, name, defaultValue);
        if (value == 0) {
            throw new IllegalArgumentException("--" + name + " must be positive");
        }
        return value;
    }

    private static long nonNegativeLongOption(Map<String, String> options, String name, long defaultValue) {
        long value = Long.parseLong(options.getOrDefault(name, Long.toString(defaultValue)));
        if (value < 0) {
            throw new IllegalArgumentException("--" + name + " must not be negative");
        }
        return value;
    }
}
