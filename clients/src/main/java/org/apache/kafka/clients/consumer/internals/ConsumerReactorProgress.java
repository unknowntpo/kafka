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

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

/**
 * Computes immutable progress decisions that can be safely published by the consumer reactor.
 *
 * <p>This is the migration seam toward making the consumer reactor the single place that combines progress
 * constraints. Request managers describe what they are waiting for; the reactor turns those intents into one
 * immutable snapshot for the application thread.</p>
 */
final class ConsumerReactorProgress {

    private ConsumerReactorProgress() {
    }

    static ApplicationWait initialApplicationWait(final long timeoutMs,
                                                  final long currentTimeMs) {
        return new ApplicationWait(
            ProgressIntent.awaitDeadlineAfter(currentTimeMs, timeoutMs),
            currentTimeMs,
            null,
            false
        );
    }

    static ApplicationWait decideApplicationWait(final Collection<RequestManager> requestManagers,
                                                  final long currentTimeMs) {
        Objects.requireNonNull(requestManagers, "Request managers must be non-null");

        ProgressIntent limitingIntent = ProgressIntent.awaitEvent();
        String source = null;

        for (RequestManager requestManager : requestManagers) {
            ProgressIntent candidateIntent = requestManager.progressIntent(currentTimeMs);
            if (candidateIntent.deadlineAtMs() < limitingIntent.deadlineAtMs()) {
                limitingIntent = candidateIntent;
                source = requestManager.getClass().getSimpleName();
            }
        }

        return new ApplicationWait(limitingIntent, currentTimeMs, source, false);
    }

    enum WaitMode {
        AWAIT_EVENT,
        AWAIT_DEADLINE
    }

    static final class ProgressIntent {
        private static final ProgressIntent EVENT =
            new ProgressIntent(WaitMode.AWAIT_EVENT, Long.MAX_VALUE, false);

        private final WaitMode waitMode;
        private final long deadlineAtMs;
        private final boolean compatibilityDeadline;

        private ProgressIntent(final WaitMode waitMode,
                               final long deadlineAtMs,
                               final boolean compatibilityDeadline) {
            this.waitMode = Objects.requireNonNull(waitMode, "Wait mode must be non-null");
            this.deadlineAtMs = deadlineAtMs;
            this.compatibilityDeadline = compatibilityDeadline;
        }

        static ProgressIntent awaitEvent() {
            return EVENT;
        }

        static ProgressIntent awaitDeadlineAfter(final long currentTimeMs,
                                                 final long delayMs) {
            return awaitDeadlineAfter(currentTimeMs, delayMs, false);
        }

        static ProgressIntent awaitCompatibilityDeadlineAfter(final long currentTimeMs,
                                                              final long delayMs) {
            return awaitDeadlineAfter(currentTimeMs, delayMs, true);
        }

        private static ProgressIntent awaitDeadlineAfter(final long currentTimeMs,
                                                         final long delayMs,
                                                         final boolean compatibilityDeadline) {
            if (delayMs < 0L)
                throw new IllegalArgumentException("Progress delay must not be negative: " + delayMs);

            if (delayMs == Long.MAX_VALUE || currentTimeMs > Long.MAX_VALUE - delayMs)
                return awaitEvent();

            return new ProgressIntent(WaitMode.AWAIT_DEADLINE, currentTimeMs + delayMs, compatibilityDeadline);
        }

        WaitMode waitMode() {
            return waitMode;
        }

        long deadlineAtMs() {
            return deadlineAtMs;
        }

        long remainingMs(final long currentTimeMs) {
            return waitMode == WaitMode.AWAIT_EVENT
                ? Long.MAX_VALUE
                : Math.max(0L, deadlineAtMs - currentTimeMs);
        }

        boolean compatibilityDeadline() {
            return compatibilityDeadline;
        }
    }

    static final class ApplicationWait {
        private final ProgressIntent intent;
        private final long decidedAtMs;
        private final String source;
        private final boolean deadlineNotificationDelivered;

        private ApplicationWait(final ProgressIntent intent,
                                final long decidedAtMs,
                                final String source,
                                final boolean deadlineNotificationDelivered) {
            this.intent = Objects.requireNonNull(intent, "Progress intent must be non-null");
            this.decidedAtMs = decidedAtMs;
            this.source = source;
            this.deadlineNotificationDelivered = deadlineNotificationDelivered;
        }

        long timeoutMs() {
            return remainingMs(decidedAtMs);
        }

        long remainingMs(final long currentTimeMs) {
            return intent.remainingMs(currentTimeMs);
        }

        long remainingMsForApplication(final long currentTimeMs) {
            return deadlineNotificationDelivered ? Long.MAX_VALUE : remainingMs(currentTimeMs);
        }

        boolean deadlineNotificationDelivered() {
            return deadlineNotificationDelivered;
        }

        WaitMode waitMode() {
            return intent.waitMode();
        }

        long deadlineAtMs() {
            return intent.deadlineAtMs();
        }

        boolean shortens(final ApplicationWait previous) {
            return deadlineAtMs() < previous.deadlineAtMs();
        }

        boolean sameDecision(final ApplicationWait other) {
            return waitMode() == other.waitMode()
                && deadlineAtMs() == other.deadlineAtMs()
                && compatibilityDeadline() == other.compatibilityDeadline()
                && Objects.equals(source, other.source);
        }

        boolean sameSource(final ApplicationWait other) {
            return Objects.equals(source, other.source);
        }

        boolean compatibilityDeadline() {
            return intent.compatibilityDeadline();
        }

        boolean isPendingCompatibilityDeadline(final long currentTimeMs) {
            return waitMode() == WaitMode.AWAIT_DEADLINE
                && compatibilityDeadline()
                && remainingMs(currentTimeMs) > 0L
                && !deadlineNotificationDelivered();
        }

        long decidedAtMs() {
            return decidedAtMs;
        }

        Optional<String> source() {
            return Optional.ofNullable(source);
        }

        ApplicationWait withDeadlineNotificationDelivered() {
            if (waitMode() != WaitMode.AWAIT_DEADLINE)
                throw new IllegalStateException("Only deadline waits can deliver a deadline notification");
            return new ApplicationWait(intent, decidedAtMs, source, true);
        }
    }
}
