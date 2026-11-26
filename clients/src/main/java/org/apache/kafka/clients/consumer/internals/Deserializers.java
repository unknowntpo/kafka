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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class Deserializers<K, V> implements AutoCloseable {

    private final Plugin<Deserializer<K>> keyDeserializerPlugin;
    private final Plugin<Deserializer<V>> valueDeserializerPlugin;

    // Deserializers of this project that do not keep the buffer they are given and do not read the headers.
    // A subclass is not in this list, because it may change that.
    private static final Set<Class<?>> SIMPLE = Set.of(
            ByteArrayDeserializer.class,
            BytesDeserializer.class,
            StringDeserializer.class,
            LongDeserializer.class,
            IntegerDeserializer.class,
            ShortDeserializer.class,
            DoubleDeserializer.class,
            FloatDeserializer.class,
            UUIDDeserializer.class,
            VoidDeserializer.class);

    public Deserializers(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Metrics metrics) {
        this.keyDeserializerPlugin = Plugin.wrapInstance(
                Objects.requireNonNull(keyDeserializer, "Key deserializer provided to Deserializers should not be null"),
                metrics,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        this.valueDeserializerPlugin = Plugin.wrapInstance(
                Objects.requireNonNull(valueDeserializer, "Value deserializer provided to Deserializers should not be null"),
                metrics,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    }

    @SuppressWarnings("unchecked")
    public Deserializers(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Metrics metrics) {
        String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);

        if (keyDeserializer == null) {
            keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            keyDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), true);
        } else {
            config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        }
        this.keyDeserializerPlugin = Plugin.wrapInstance(keyDeserializer, metrics, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);

        if (valueDeserializer == null) {
            valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            valueDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), false);
        } else {
            config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        }
        this.valueDeserializerPlugin = Plugin.wrapInstance(valueDeserializer, metrics, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    }

    /**
     * @return true if neither deserializer keeps the buffer it is given or reads the headers, so records can be
     *         decoded for them into buffers that are reused
     */
    public boolean simple() {
        return SIMPLE.contains(keyDeserializer().getClass()) && SIMPLE.contains(valueDeserializer().getClass());
    }

    public Deserializer<K> keyDeserializer() {
        return keyDeserializerPlugin.get();
    }

    public Deserializer<V> valueDeserializer() {
        return valueDeserializerPlugin.get();
    }

    @Override
    public void close() {
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        Utils.closeQuietly(keyDeserializerPlugin, "key deserializer", firstException);
        Utils.closeQuietly(valueDeserializerPlugin, "value deserializer", firstException);
        Throwable exception = firstException.get();

        if (exception != null) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close deserializers", exception);
        }
    }

    @Override
    public String toString() {
        return "Deserializers{" +
                "keyDeserializer=" + keyDeserializerPlugin.get() +
                ", valueDeserializer=" + valueDeserializerPlugin.get() +
                '}';
    }
}
