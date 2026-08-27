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

import java.util.Set;

/** Converts the fetch-domain fact into an application-visible reaction without owning reactor ordering. */
final class FetchBufferHasDataHandler implements ManagerEventHandler<ManagerEvent.FetchBufferHasData> {
    @Override
    public Set<ManagerEvent.Type> eventTypes() {
        return Set.of(ManagerEvent.Type.FETCH_BUFFER_HAS_DATA);
    }

    @Override
    public Class<ManagerEvent.FetchBufferHasData> eventClass() {
        return ManagerEvent.FetchBufferHasData.class;
    }

    @Override
    public CoordinationPlan handle(final ManagerEvent.FetchBufferHasData event) {
        return CoordinationPlan.action(ReactorAction.wakeApplication());
    }
}
