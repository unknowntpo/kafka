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
 * Describes the local condition under which one request manager should be polled again.
 * This is neither a fact nor a command: it is the manager's contribution to the reactor's
 * aggregate timing decision.
 */
abstract class NextPollCondition {

    /**
     * Existing input path expected to make an {@link AwaitInput} manager worth evaluating again.
     * This is diagnostic context, not a subscription or second wakeup channel.
     */
    enum AwaitCause {
        NETWORK_COMPLETION,
        COORDINATOR_CHANGE,
        SHUTDOWN,
        LEGACY_UNSPECIFIED
    }

    private NextPollCondition() {
    }

    /** Compatibility projection consumed while retained manager deadlines remain millisecond based. */
    abstract long delayMs();

    /** Another input, such as a network completion or application command, must make progress possible. */
    static final class AwaitInput extends NextPollCondition {
        private final AwaitCause cause;

        private AwaitInput(final AwaitCause cause) {
            this.cause = Objects.requireNonNull(cause, "Await cause must be non-null");
        }

        AwaitCause cause() {
            return cause;
        }

        @Override
        public long delayMs() {
            return Long.MAX_VALUE;
        }
    }

    /** Time alone may make progress possible after this finite, positive delay. */
    static final class RetryAfter extends NextPollCondition {
        private final long delayMs;

        RetryAfter(final long delayMs) {
            if (delayMs <= 0L || delayMs == Long.MAX_VALUE)
                throw new IllegalArgumentException("Retry delay must be finite and positive");
            this.delayMs = delayMs;
        }

        @Override
        public long delayMs() {
            return delayMs;
        }
    }

    /**
     * Work was produced in this poll and the manager has more local work ready immediately.
     * A result without output may not use this condition.
     */
    static final class PollImmediately extends NextPollCondition {
        private static final PollImmediately INSTANCE = new PollImmediately();

        private PollImmediately() {
        }

        @Override
        public long delayMs() {
            return 0L;
        }
    }

    static NextPollCondition awaitInput() {
        return awaitInput(AwaitCause.LEGACY_UNSPECIFIED);
    }

    static NextPollCondition awaitInput(final AwaitCause cause) {
        return new AwaitInput(cause);
    }

    static NextPollCondition retryAfter(final long delayMs) {
        return new RetryAfter(delayMs);
    }

    static NextPollCondition pollImmediately() {
        return PollImmediately.INSTANCE;
    }
}
