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

/**
 * What a request manager is waiting for after a run, declared to the event loop (design semantics S1). Together
 * with the manager's own timer (its {@code timeUntilNextPollMs}) and commands from the application thread, this
 * is the complete set of triggers that may run the manager again; nothing else can. The loop verifies the trigger
 * against the version the manager declared under, so a condition that was already satisfied when declared, or an
 * input that arrived before the declaration, cannot re-run the manager: self-triggered busy loops are impossible
 * by construction for a manager that declares accurately.
 *
 * <ul>
 *   <li>{@link #ANY_INPUT}: any input with identity (a request completion of any manager, a command, a metadata
 *       change, an application poll) that arrives after the declaration. This is the previous implementation's
 *       behaviour restricted to "something happened", and the safe default for managers whose dependencies on
 *       other managers are not declared yet (contract R6).</li>
 *   <li>{@link #OWN_COMPLETION}: only the completion of one of this manager's own requests. For a manager with a
 *       request in flight and nothing else to do.</li>
 *   <li>{@link #TIMER_ONLY}: nothing but the manager's own timer. For a manager backing off.</li>
 * </ul>
 */
public enum WaitCondition {
    ANY_INPUT,
    OWN_COMPLETION,
    TIMER_ONLY
}
