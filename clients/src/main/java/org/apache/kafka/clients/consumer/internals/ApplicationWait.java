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
 * A manager's contribution to the application-thread wait projection.
 *
 * <p>This is deliberately separate from {@link NextPollCondition}: a manager retry deadline answers when polling
 * that manager may become useful, while this value answers how long the application thread may remain blocked.
 * The legacy {@link RequestManager#maximumTimeToWait(long)} value is wrapped at the migration boundary so it cannot
 * be confused with a manager retry condition inside the reactor.
 */
final class ApplicationWait {
    private static final ApplicationWait UNBOUNDED = new ApplicationWait(Long.MAX_VALUE);

    private final long delayMs;

    private ApplicationWait(final long delayMs) {
        if (delayMs < 0L)
            throw new IllegalArgumentException("Application wait must be non-negative");
        this.delayMs = delayMs;
    }

    static ApplicationWait fromLegacyMaximumTimeToWait(final long delayMs) {
        return delayMs == Long.MAX_VALUE ? UNBOUNDED : new ApplicationWait(delayMs);
    }

    static ApplicationWait unbounded() {
        return UNBOUNDED;
    }

    long delayMs() {
        return delayMs;
    }
}
