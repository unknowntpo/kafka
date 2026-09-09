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
package org.apache.kafka.clients.consumer.ng.bench;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ng.NgKafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Measurement harness for the next-generation consumer, printing the same per-interval lines as
 * {@code kafka-consumer-perf-test --show-detailed-stats} so the same steady-state tooling applies.
 *
 * <pre>
 * args: --bootstrap host:port --topic t [--partitions N | --group g] --records M [--max-poll-records 500]
 *       [--deserializer bytes|bytebuffer] [--interval-ms 1000] [--idle-ms 0] [--loops 1]
 * </pre>
 * With {@code --group} the consumer subscribes (group protocol); otherwise it assigns partitions 0..N-1 from offset 0.
 */
public final class ConsumeBench {

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (!args[i].startsWith("--"))
                throw new IllegalArgumentException("unknown arg " + args[i]);
            opts.put(args[i].substring(2), args[i + 1]);
        }
        String topic = opts.get("topic");
        if (topic == null)
            throw new IllegalArgumentException("--topic is required");
        run(opts.getOrDefault("bootstrap", "localhost:9092"), topic, opts.get("group"),
                Integer.parseInt(opts.getOrDefault("partitions", "1")),
                Long.parseLong(opts.getOrDefault("records", String.valueOf(Long.MAX_VALUE))),
                Integer.parseInt(opts.getOrDefault("max-poll-records", "500")),
                opts.getOrDefault("deserializer", "bytes"),
                Long.parseLong(opts.getOrDefault("interval-ms", "1000")),
                Long.parseLong(opts.getOrDefault("idle-ms", "0")),
                Integer.parseInt(opts.getOrDefault("loops", "1")));
    }

    private static void run(String bootstrap, String topic, String group, int partitions, long records, int maxPollRecords,
                            String deserializer, long intervalMs, long idleMs, int loops) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "ng-bench");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        if (group != null)
            props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        Deserializer<?> valueDeserializer = "bytebuffer".equals(deserializer) ? new ByteBufferDeserializer() : new ByteArrayDeserializer();
        NgKafkaConsumer<byte[], Object> consumer = new NgKafkaConsumer<>(new ConsumerConfig(props), new ByteArrayDeserializer(),
                uncheckedCast(valueDeserializer));
        List<TopicPartition> assigned = new ArrayList<>();
        if (group != null) {
            consumer.subscribe(List.of(topic));
        } else {
            for (int p = 0; p < partitions; p++)
                assigned.add(new TopicPartition(topic, p));
            consumer.assign(assigned);
            consumer.seekToBeginning(assigned);
        }

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        long start = System.currentTimeMillis();
        long lastReport = start;
        long lastBytes = 0;
        long lastRecords = 0;
        long consumed = 0;
        long bytes = 0;
        long deadline = idleMs > 0 ? start + idleMs : Long.MAX_VALUE;
        long nextLoopAt = records;
        int loop = 1;
        while (consumed < records * loops && System.currentTimeMillis() < deadline) {
            ConsumerRecords<byte[], Object> batch = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<byte[], Object> r : batch) {
                consumed++;
                bytes += r.serializedValueSize() + Math.max(0, r.serializedKeySize());
            }
            if (consumed >= nextLoopAt && loop < loops && group == null) {
                consumer.seekToBeginning(assigned); // --loops: consume the topic again for long steady windows
                loop++;
                nextLoopAt += records;
            }
            long now = System.currentTimeMillis();
            if (now - lastReport >= intervalMs) {
                double mb = bytes / (1024.0 * 1024.0);
                double dmb = (bytes - lastBytes) / (1024.0 * 1024.0);
                double secs = (now - lastReport) / 1000.0;
                System.out.printf("%s, 0, %.4f, %.4f, %d, %.4f, 0, 0, %.4f, %.4f%n", fmt.format(new Date(now)), mb, dmb / secs,
                        consumed, (consumed - lastRecords) / secs, dmb / secs, (consumed - lastRecords) / secs);
                lastReport = now;
                lastBytes = bytes;
                lastRecords = consumed;
            }
        }
        long end = System.currentTimeMillis();
        double secs = Math.max(1, end - start) / 1000.0;
        double mb = bytes / (1024.0 * 1024.0);
        System.out.printf("%s, %s, %.4f, %.4f, %d, %.4f, 0, %d, %.4f, %.4f%n", fmt.format(new Date(start)), fmt.format(new Date(end)),
                mb, mb / secs, consumed, consumed / secs, end - start, mb / secs, consumed / secs);
        consumer.close();
    }

    @SuppressWarnings("unchecked")
    private static <T> Deserializer<T> uncheckedCast(Deserializer<?> d) {
        return (Deserializer<T>) d;
    }
}
