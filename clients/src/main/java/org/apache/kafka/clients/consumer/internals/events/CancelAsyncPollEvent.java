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

import java.util.Objects;

/** Terminalize an abandoned application poll and release its retained work on the network owner. */
public final class CancelAsyncPollEvent extends ApplicationEvent {
    private final AsyncPollEvent target;

    public CancelAsyncPollEvent(AsyncPollEvent target) {
        super(Type.CANCEL_ASYNC_POLL);
        this.target = Objects.requireNonNull(target);
    }

    public AsyncPollEvent target() {
        return target;
    }
}
