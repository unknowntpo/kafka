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
import org.apache.kafka.clients.consumer.ng.FetchEngine;
import org.apache.kafka.clients.consumer.ng.RecordReader;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Measurement harness for the fetch-engine cut, printing the same per-interval lines as
 * {@code kafka-consumer-perf-test --show-detailed-stats} so the same steady-state tooling applies.
 *
 * <pre>
 * args: --bootstrap host:port --topic t --partitions N --records M [--credit-mb 64] [--max-poll-records 500]
 *       [--deserializer bytes|bytebuffer] [--interval-ms 1000] [--idle-ms 0] [--loops 1]
 * </pre>
 */
public final class ConsumeBench {

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (!args[i].startsWith("--"))
                throw new IllegalArgumentException("unknown arg " + args[i]);
            opts.put(args[i].substring(2), args[i + 1]);
        }
        String bootstrap = opts.getOrDefault("bootstrap", "localhost:9092");
        String topic = opts.get("topic");
        int partitions = Integer.parseInt(opts.getOrDefault("partitions", "1"));
        long records = Long.parseLong(opts.getOrDefault("records", String.valueOf(Long.MAX_VALUE)));
        long creditMb = Long.parseLong(opts.getOrDefault("credit-mb", "64"));
        int maxPollRecords = Integer.parseInt(opts.getOrDefault("max-poll-records", "500"));
        String deserializer = opts.getOrDefault("deserializer", "bytes");
        long intervalMs = Long.parseLong(opts.getOrDefault("interval-ms", "1000"));
        long idleMs = Long.parseLong(opts.getOrDefault("idle-ms", "0"));
        int loops = Integer.parseInt(opts.getOrDefault("loops", "1"));
        if (topic == null)
            throw new IllegalArgumentException("--topic is required");
        run(bootstrap, topic, partitions, records, creditMb, maxPollRecords, deserializer, intervalMs, idleMs, loops);
    }

    private static void run(String bootstrap, String topic, int partitions, long records, long creditMb, int maxPollRecords,
                            String deserializer, long intervalMs, long idleMs, int loops) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "ng-bench");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        ConsumerConfig config = new ConsumerConfig(props);

        List<TopicPartition> assigned = new ArrayList<>();
        for (int p = 0; p < partitions; p++)
            assigned.add(new TopicPartition(topic, p));

        RecordReader<?, ?>[] readerHolder = new RecordReader<?, ?>[1];
        FetchEngine engine = new FetchEngine(config, creditMb * 1024 * 1024, ignored -> {
            RecordReader<?, ?> r = readerHolder[0];
            if (r != null)
                r.onData();
        });
        Deserializer<?> valueDeserializer = "bytebuffer".equals(deserializer) ? new ByteBufferDeserializer() : new ByteArrayDeserializer();
        RecordReader<byte[], Object> reader = new RecordReader<>(engine, new ByteArrayDeserializer(),
                uncheckedCast(valueDeserializer), config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG));
        readerHolder[0] = reader;
        engine.assign(assigned);
        for (TopicPartition tp : assigned)
            reader.track(tp, 0L);
        engine.start();

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
            List<? extends ConsumerRecord<?, ?>> batch = reader.poll(maxPollRecords, 1000);
            for (ConsumerRecord<?, ?> r : batch) {
                consumed++;
                bytes += r.serializedValueSize() + Math.max(0, r.serializedKeySize());
            }
            if (consumed >= nextLoopAt && loop < loops) {
                // --loops: the topic is consumed again from the beginning, for steady-state runs longer than the data.
                reader.seekAll(0L);
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
        System.err.printf("pool: outstanding=%d allocated=%d queued=%d%n", engine.pool().outstandingBytes(), engine.pool().allocatedBytes(), engine.queuedBytes());
        engine.close();
    }

    @SuppressWarnings("unchecked")
    private static <T> Deserializer<T> uncheckedCast(Deserializer<?> d) {
        return (Deserializer<T>) d;
    }
}
