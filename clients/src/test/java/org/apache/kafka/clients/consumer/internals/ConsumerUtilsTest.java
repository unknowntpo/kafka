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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.memory.CachingMemoryPool;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

public class ConsumerUtilsTest {

    private static final int FETCH_MAX_BYTES = 8 * 1024 * 1024;

    private static ConsumerConfig config() {
        return new ConsumerConfig(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.FETCH_MAX_BYTES_CONFIG, FETCH_MAX_BYTES));
    }

    @Test
    public void testCopyingDeserializersGetAReusingPoolSizedFromFetchMaxBytes() {
        MemoryPool pool = ConsumerUtils.receiveMemoryPool(config(),
                new Deserializers<>(new ByteArrayDeserializer(), new StringDeserializer(), null));
        assertInstanceOf(CachingMemoryPool.class, pool);
        assertEquals(2L * FETCH_MAX_BYTES, pool.size());
    }

    @Test
    public void testByteBufferDeserializerKeepsTheDefaultPool() {
        // ByteBufferDeserializer hands the receive buffer through to the application, so it must not be reused
        assertSame(MemoryPool.NONE, ConsumerUtils.receiveMemoryPool(config(),
                new Deserializers<>(new ByteArrayDeserializer(), new ByteBufferDeserializer(), null)));
        assertSame(MemoryPool.NONE, ConsumerUtils.receiveMemoryPool(config(),
                new Deserializers<>(new ByteBufferDeserializer(), new ByteArrayDeserializer(), null)));
    }

    @Test
    public void testCustomDeserializerKeepsTheDefaultPool() {
        Deserializer<byte[]> retaining = (topic, data) -> data;
        assertSame(MemoryPool.NONE, ConsumerUtils.receiveMemoryPool(config(),
                new Deserializers<>(retaining, new ByteArrayDeserializer(), null)));
        // even a subclass of a copying deserializer may override deserialize(...), so only exact classes qualify
        Deserializer<byte[]> subclass = new ByteArrayDeserializer() {
            @Override
            public byte[] deserialize(String topic, byte[] data) {
                return super.deserialize(topic, data);
            }
        };
        assertSame(MemoryPool.NONE, ConsumerUtils.receiveMemoryPool(config(),
                new Deserializers<>(subclass, new ByteArrayDeserializer(), null)));
    }
}
