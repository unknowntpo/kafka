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
 * A manager-local activation condition, not permission to bypass request eligibility.
 * Input waits contribute no timer; the ordinary full manager pass still re-evaluates them.
 */
public final class NextPollCondition {
    public enum Kind { POLL_IMMEDIATELY, RETRY_AFTER, AWAIT_INPUT }

    public enum Input { NETWORK_COMPLETION, COORDINATOR_CHANGE, APPLICATION_INPUT, SHUTDOWN, LEGACY_UNSPECIFIED }

    private final Kind kind;
    private final long delayMs;
    private final Input input;

    private NextPollCondition(Kind kind, long delayMs, Input input) {
        this.kind = kind;
        this.delayMs = delayMs;
        this.input = input;
    }

    public static NextPollCondition pollImmediately() {
        return new NextPollCondition(Kind.POLL_IMMEDIATELY, 0, null);
    }

    public static NextPollCondition retryAfter(long delayMs) {
        if (delayMs < 0)
            throw new IllegalArgumentException("Negative retry delay");
        return new NextPollCondition(Kind.RETRY_AFTER, delayMs, null);
    }

    public static NextPollCondition awaitInput(Input input) {
        return new NextPollCondition(Kind.AWAIT_INPUT, Long.MAX_VALUE, Objects.requireNonNull(input));
    }

    public Kind kind() {
        return kind;
    }

    public long delayMs() {
        return delayMs;
    }

    public Input input() {
        return input;
    }
}
