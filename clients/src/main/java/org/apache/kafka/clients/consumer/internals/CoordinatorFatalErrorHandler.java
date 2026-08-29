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

import java.util.List;
import java.util.Set;

/** Converts a coordinator-owned fatal error fact into ordered application-visible effects. */
final class CoordinatorFatalErrorHandler implements ManagerEventHandler<ManagerEvent.CoordinatorFatalError> {
    @Override
    public Set<ManagerEvent.Type> eventTypes() {
        return Set.of(ManagerEvent.Type.COORDINATOR_FATAL_ERROR);
    }

    @Override
    public Class<ManagerEvent.CoordinatorFatalError> eventClass() {
        return ManagerEvent.CoordinatorFatalError.class;
    }

    @Override
    public CoordinationPlan handle(final ManagerEvent.CoordinatorFatalError event) {
        return new CoordinationPlan(
            List.of(),
            List.of(ReactorAction.publishBackgroundError(event.error()), ReactorAction.wakeApplication())
        );
    }
}
