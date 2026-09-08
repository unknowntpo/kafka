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
package org.apache.kafka.clients.consumer.internals.events;

/**
 * An error from an attempt, started by the event loop on its own, to resolve missing fetch positions (for example
 * {@link org.apache.kafka.clients.consumer.NoOffsetForPartitionException} with no reset policy). Such an attempt
 * is not tied to a particular {@code poll()}, so by the time the application thread sees the event the condition
 * may be gone (the application seeked, or a later attempt succeeded). The application thread surfaces it only
 * while some partition still lacks a position; otherwise it is stale and dropped.
 */
public class FetchPositionsErrorEvent extends ErrorEvent {

    public FetchPositionsErrorEvent(Throwable t) {
        super(t);
    }
}
