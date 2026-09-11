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

import java.util.Objects;

/**
 * The next reason to invoke a manager and recheck its state. Eligibility is not a guarantee that
 * a request can be sent or that an operation is still valid. Conditions do not own operations.
 *
 * <p>Conditions are immutable and may be published to the application thread when they describe
 * application observation. Cross-thread inputs use their existing queues and wakeups.
 */
public interface NextPollCondition {
    /** Whether the owner has a reason to run again. */
    boolean isReady(long currentTimeMs);

    /**
     * Delay until a time-based recheck, or zero if already ready. Long.MAX_VALUE means there is no
     * earlier finite deadline; input changes are checked separately and may make this return zero.
     */
    long remainingMs(long currentTimeMs);

    /** Local work can continue now; the owner must still check its domain preconditions. */
    static NextPollCondition ready() {
        return Constant.READY;
    }

    /**
     * No autonomous step is currently due. A documented command, completion or peer-state input
     * may change this answer on the next query; do not discard an unfinished operation's continuation.
     */
    static NextPollCondition idle() {
        return Constant.IDLE;
    }

    /** An absolute deadline on the consumer time source, not an independently scheduled timer. */
    static NextPollCondition at(long deadlineMs) {
        return new DeadlinePollCondition(deadlineMs);
    }

    /** Convert a non-negative relative budget once, at the point at which it was calculated. */
    static NextPollCondition after(long currentTimeMs, long delayMs) {
        if (delayMs < 0)
            throw new IllegalArgumentException("Delay must not be negative");
        long deadline = currentTimeMs > Long.MAX_VALUE - delayMs ? Long.MAX_VALUE : currentTimeMs + delayMs;
        return at(deadline);
    }

    /** Either reason can make the owner eligible. */
    static NextPollCondition either(NextPollCondition first, NextPollCondition second) {
        Objects.requireNonNull(first, "first");
        Objects.requireNonNull(second, "second");
        if (first == Constant.READY || second == Constant.READY)
            return Constant.READY;
        if (first == Constant.IDLE)
            return second;
        if (second == Constant.IDLE)
            return first;
        if (first instanceof DeadlinePollCondition && second instanceof DeadlinePollCondition) {
            DeadlinePollCondition firstDeadline = (DeadlinePollCondition) first;
            DeadlinePollCondition secondDeadline = (DeadlinePollCondition) second;
            return firstDeadline.deadlineMs() <= secondDeadline.deadlineMs() ? first : second;
        }
        return new EitherPollCondition(first, second);
    }

    enum Constant implements NextPollCondition {
        READY, IDLE;

        @Override
        public boolean isReady(long currentTimeMs) {
            return this == READY;
        }

        @Override
        public long remainingMs(long currentTimeMs) {
            return this == READY ? 0 : Long.MAX_VALUE;
        }
    }

}

final class DeadlinePollCondition implements NextPollCondition {
    private final long deadlineMs;

    DeadlinePollCondition(long deadlineMs) {
        this.deadlineMs = deadlineMs;
    }

    long deadlineMs() {
        return deadlineMs;
    }

    @Override
    public boolean isReady(long currentTimeMs) {
        return currentTimeMs >= deadlineMs;
    }

    @Override
    public long remainingMs(long currentTimeMs) {
        if (isReady(currentTimeMs))
            return 0;
        long remaining = deadlineMs - currentTimeMs;
        return remaining < 0 ? Long.MAX_VALUE : remaining;
    }

    @Override
    public String toString() {
        return "At(" + deadlineMs + ")";
    }
}

final class EitherPollCondition implements NextPollCondition {
    private final NextPollCondition first;
    private final NextPollCondition second;

    EitherPollCondition(NextPollCondition first, NextPollCondition second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean isReady(long currentTimeMs) {
        return first.isReady(currentTimeMs) || second.isReady(currentTimeMs);
    }

    @Override
    public long remainingMs(long currentTimeMs) {
        return Math.min(first.remainingMs(currentTimeMs), second.remainingMs(currentTimeMs));
    }

    @Override
    public String toString() {
        return "Either(" + first + ", " + second + ")";
    }
}
