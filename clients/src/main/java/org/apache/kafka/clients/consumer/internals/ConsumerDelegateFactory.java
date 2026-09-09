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
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Service-provider hook through which an alternative implementation of the {@code group.protocol=consumer}
 * delegate can be supplied from another module (discovered with {@link java.util.ServiceLoader}). Exists so an
 * implementation that needs a newer Java release than this module can still back {@code KafkaConsumer}.
 */
public interface ConsumerDelegateFactory {

    /** @return a delegate for the given configuration, or {@code null} if this factory does not apply */
    <K, V> ConsumerDelegate<K, V> create(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer);
}
