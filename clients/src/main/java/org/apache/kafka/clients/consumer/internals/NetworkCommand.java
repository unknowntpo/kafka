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

import org.apache.kafka.clients.ClientResponse;

import java.util.function.BiConsumer;

/**
 * A transport intent produced by a request manager for the network delegate to stage.
 * Producing a command does not mean that the request has been sent.
 *
 * <p>The experiment has one implementation, {@link NetworkClientDelegate.UnsentRequest}.
 * Keeping the contract small lets the reactor observe completion ownership without taking
 * responsibility for request construction or transport state.</p>
 */
abstract class NetworkCommand {

    abstract void onCompletion(BiConsumer<ClientResponse, Throwable> callback);

    /**
     * Compatibility adapter for the network delegate's current concrete queue element.
     * Remove this method when the delegate accepts {@code NetworkCommand} directly.
     */
    abstract NetworkClientDelegate.UnsentRequest transportRequest();
}
