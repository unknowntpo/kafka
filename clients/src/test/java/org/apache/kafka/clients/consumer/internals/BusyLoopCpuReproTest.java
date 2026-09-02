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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * KAFKA-21010 / KAFKA-20970 busy-loop CPU reproduction. Not a regression test: it prints
 * process/thread CPU time and consumer metrics while polling a consumer whose bootstrap
 * servers can never resolve, so the coordinator is never discovered and no heartbeat
 * response ever arrives. Run manually with:
 *
 *   ./gradlew :clients:test --tests "*.BusyLoopCpuReproTest"
 */
@Tag("integration")
public class BusyLoopCpuReproTest {

    private static final long RUN_DURATION_MS = 30_000;

    @Test
    public void measureCpuWhileCoordinatorUnresolvable() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "unresolvable.invalid:9092");
        props.setProperty(CommonClientConfigs.BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG, "300000");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "busy-loop-repro");
        props.setProperty(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.setProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        ThreadMXBean threads = ManagementFactory.getThreadMXBean();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("busy-loop-topic"));

            long wallStart = System.nanoTime();
            long cpuStart = totalThreadCpuNanos(threads);
            long polls = 0;
            while (System.nanoTime() - wallStart < RUN_DURATION_MS * 1_000_000L) {
                consumer.poll(Duration.ofSeconds(1));
                polls++;
            }
            long cpuNanos = totalThreadCpuNanos(threads) - cpuStart;
            long wallNanos = System.nanoTime() - wallStart;

            System.out.printf("REPRO wall=%.1fs polls=%d cpu=%.2fs cpu-utilisation=%.1f%%%n",
                wallNanos / 1e9, polls, cpuNanos / 1e9, 100.0 * cpuNanos / wallNanos);
            printMetric(consumer.metrics(), "time-between-network-thread-poll-avg");
            printMetric(consumer.metrics(), "application-event-queue-poll-rate");
            printMetric(consumer.metrics(), "time-between-poll-avg");
        }
    }

    private static long totalThreadCpuNanos(ThreadMXBean threads) {
        long total = 0;
        for (long id : threads.getAllThreadIds()) {
            long cpu = threads.getThreadCpuTime(id);
            if (cpu > 0)
                total += cpu;
        }
        return total;
    }

    private static void printMetric(Map<MetricName, ? extends Metric> metrics, String name) {
        metrics.entrySet().stream()
            .filter(e -> e.getKey().name().equals(name))
            .forEach(e -> System.out.printf("REPRO metric %s = %s%n", name, e.getValue().metricValue()));
    }
}
