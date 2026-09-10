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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.HeartbeatRequestState;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Real broker benchmark: one invocation validates 1,000 records; JMH normalizes to records. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 3, time = 10)
@Fork(3)
@Threads(1)
public class AsyncConsumerBrokerBenchmark {
    private static final int RECORDS_PER_INVOCATION = 1000;

    private static KafkaConsumer<byte[], byte[]> consumer(String server) {
        Properties p = new Properties();
        p.put("bootstrap.servers", server);
        p.put("group.protocol", "consumer");
        p.put("group.id", "jmh-" + UUID.randomUUID());
        p.put("enable.auto.commit", "true");
        p.put("auto.offset.reset", "earliest");
        p.put("key.deserializer", ByteArrayDeserializer.class.getName());
        p.put("value.deserializer", ByteArrayDeserializer.class.getName());
        p.put("fetch.max.bytes", "1048576");
        p.put("max.partition.fetch.bytes", "1048576");
        p.put("fetch.max.wait.ms", "500");
        p.put("retry.backoff.ms", "100");
        return new KafkaConsumer<>(p);
    }

    @State(Scope.Thread)
    public static class ConsumerState {
        @Param({"127.0.0.1:9092"})
        public String server;
        @Param({"jmh-input"})
        public String topic;
        @Param({"5000000"})
        public long records;
        private KafkaConsumer<byte[], byte[]> consumer;
        private TopicPartition partition;
        private Iterator<ConsumerRecord<byte[], byte[]>> batch = Collections.emptyIterator();
        private long expected;
        private long validated;

        @Setup(Level.Trial)
        public void setup(BenchmarkParams params) {
            consumer = consumer(server);
            partition = new TopicPartition(topic, 0);
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (consumer.assignment().isEmpty()) {
                if (System.nanoTime() >= deadline)
                    throw new AssertionError("Group assignment timeout");
                consumer.poll(Duration.ofMillis(250));
            }
            if (!consumer.assignment().equals(Set.of(partition)))
                throw new AssertionError("Unexpected assignment");
            consumer.seek(partition, params.getBenchmark().endsWith(".idle") ? records : 0L);
        }

        @Setup(Level.Iteration)
        public void resetCounter() {
            validated = 0;
        }

        @TearDown(Level.Iteration)
        public void publishCounter() {
            ConsumerCpuProfiler.validatedRecords = validated;
            if (!consumer.assignment().equals(Set.of(partition)))
                throw new AssertionError("Lost group assignment");
        }

        @TearDown(Level.Trial)
        public void close() {
            if (consumer != null)
                consumer.close(Duration.ofSeconds(5));
        }
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS_PER_INVOCATION)
    public long consume(ConsumerState state, ConsumerCpuProfiler.CpuState cpu) {
        long checksum = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        for (int i = 0; i < RECORDS_PER_INVOCATION; i++) {
            while (!state.batch.hasNext()) {
                if (state.expected == state.records) {
                    state.consumer.seek(state.partition, 0L);
                    state.expected = 0;
                }
                if (System.nanoTime() >= deadline)
                    throw new AssertionError("Record progress timeout");
                state.batch = state.consumer.poll(Duration.ofMillis(250)).iterator();
            }
            ConsumerRecord<byte[], byte[]> record = state.batch.next();
            if (record.partition() != 0 || record.offset() != state.expected || record.value().length != 128)
                throw new AssertionError("Record structure mismatch");
            long sequence = ByteBuffer.wrap(record.value()).getLong();
            if (sequence != state.expected++)
                throw new AssertionError("Record sequence mismatch");
            checksum += sequence;
        }
        state.validated += RECORDS_PER_INVOCATION;
        return checksum;
    }

    @Benchmark
    public void idle(ConsumerState state, ConsumerCpuProfiler.CpuState cpu) {
        if (!state.consumer.poll(Duration.ofMillis(250)).isEmpty())
            throw new AssertionError("Unexpected idle records");
    }

    @State(Scope.Thread)
    public static class UnavailableState {
        @Param({"127.0.0.1:1"})
        public String server;
        private KafkaConsumer<byte[], byte[]> consumer;

        @Setup(Level.Trial)
        public void setup() {
            consumer = consumer(server);
            consumer.subscribe(List.of("unavailable-jmh"));
        }

        @TearDown(Level.Iteration)
        public void verify() throws ReflectiveOperationException {
            Object network = field(field(field(consumer, "delegate"), "applicationEventHandler"), "networkThread");
            RequestManagers managers = (RequestManagers) field(network, "requestManagers");
            HeartbeatRequestState heartbeat = (HeartbeatRequestState) field(
                managers.consumerHeartbeatRequestManager.orElseThrow(), "heartbeatRequestState");
            if (managers.coordinatorRequestManager.orElseThrow().coordinator().isPresent()
                || heartbeat.heartbeatIntervalMs() != 0 || heartbeat.requestInFlight())
                throw new AssertionError("Unavailable coordinator state changed");
        }

        @TearDown(Level.Trial)
        public void close() {
            if (consumer != null)
                consumer.close(Duration.ofSeconds(5));
        }
    }

    @Benchmark
    public void unavailable(UnavailableState state, ConsumerCpuProfiler.CpuState cpu) {
        if (!state.consumer.poll(Duration.ofMillis(250)).isEmpty())
            throw new AssertionError("Unexpected records without a coordinator");
    }

    private static Object field(Object object, String name) throws ReflectiveOperationException {
        for (Class<?> type = object.getClass(); type != null; type = type.getSuperclass()) {
            try {
                Field field = type.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(object);
            } catch (NoSuchFieldException ignored) {
                // Look in the superclass.
            }
        }
        throw new NoSuchFieldException(name);
    }
}
