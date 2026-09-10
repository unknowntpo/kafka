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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;

import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;

/**
 * A request manager exposes its next execution condition and produces outgoing requests when polled.
 * Scheduling conditions remain separate from the request batch.
 */
public interface RequestManager {

    /**
     * Query the current reason to invoke {@link #poll(long)}. The network thread re-evaluates this
     * after processing inputs and after the ordered manager pass; this is not a retained subscription.
     * The query must not consume results, produce requests or advance domain state. Eligibility may
     * mean handling an error or a local transition, not necessarily sending a request.
     *
     */
    NextPollCondition nextPollCondition(long currentTimeMs);

    /**
     * During normal operation of the {@link Consumer}, a request manager may need to send out network requests.
     * Implementations can return {@link PollResult their need for network I/O} by returning the requests here.
     * This method is called within a single-threaded context from
     * {@link ConsumerNetworkThread the consumer's network I/O thread}. As such, there should be no need for
     * synchronization protection in this method's implementation.
     *
     * <p/>
     *
     * <em>Note</em>: no network I/O occurs in this method. The method itself should not block for any reason. This
     * method is called from the consumer's network I/O thread, so quick execution of this method in <em>all</em>
     * request managers is critical to ensure that we can heartbeat in a timely fashion.
     *
     * @param currentTimeMs The current system time in milliseconds at which the method was called;
     *                      useful for determining if time-sensitive operations should be performed
     */
    PollResult poll(long currentTimeMs);

    /**
     * On shutdown of the {@link Consumer}, a request manager may need to send out network requests. Implementations
     * can signal that by returning the {@link PollResult close} requests here. Like {@link #poll(long)}, this method
     * is called within a single-threaded context from {@link ConsumerNetworkThread the consumer's network I/O thread}.
     * As such, there should be no need for synchronization protection in this method's implementation.
     *
     * <p/>
     *
     * <em>Note</em>: no network I/O occurs in this method. The method itself should not block for any reason. This
     * method is called as an (indirect) result of {@link Consumer#close() the consumer's close method} being invoked.
     * (Note that it is still invoked on the consumer's network I/O thread). Quick execution of this method in
     * <em>all</em> request managers is critical to ensure that we can complete as many of the consumer's shutdown
     * tasks as possible within the user-provided timeout.
     *
     * @param currentTimeMs The current system time in milliseconds at which the method was called
     */
    default PollResult pollOnClose(long currentTimeMs) {
        return EMPTY;
    }

    /**
     * Query when the application thread must next observe manager state. The returned condition must be
     * immutable and time-only because the network thread publishes it across threads.
     */
    default NextPollCondition applicationPollCondition(long currentTimeMs) {
        return NextPollCondition.idle();
    }

    /**
     * Signals the request manager that the consumer is closing to prepare for the proper actions to be taken.
     */
    default void signalClose() { }
}
