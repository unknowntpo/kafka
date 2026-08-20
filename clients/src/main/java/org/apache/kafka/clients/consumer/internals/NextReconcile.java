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
 * Describes when a request manager needs the reactor to reconcile it again. A deadline may also require an
 * application wakeup, or it may be reactor-only work such as a legacy network poll timeout.
 */
final class NextReconcile {

    enum Type {
        ON_EVENT,
        AT_DEADLINE
    }

    private static final NextReconcile EVENT =
        new NextReconcile(Type.ON_EVENT, Long.MAX_VALUE, false, true, 0L);

    private final Type type;
    private final long deadlineAtMs;
    private final boolean compatibilityDeadline;
    private final boolean applicationVisible;
    private final long semanticGeneration;

    private NextReconcile(final Type type,
                          final long deadlineAtMs,
                          final boolean compatibilityDeadline,
                          final boolean applicationVisible,
                          final long semanticGeneration) {
        this.type = Objects.requireNonNull(type, "Next reconcile type must be non-null");
        this.deadlineAtMs = deadlineAtMs;
        this.compatibilityDeadline = compatibilityDeadline;
        this.applicationVisible = applicationVisible;
        this.semanticGeneration = semanticGeneration;
    }

    static NextReconcile onEvent() {
        return EVENT;
    }

    static NextReconcile atDeadlineAfter(final long currentTimeMs,
                                         final long delayMs) {
        return atDeadlineAfter(currentTimeMs, delayMs, false, true);
    }

    static NextReconcile atCompatibilityDeadlineAfter(final long currentTimeMs,
                                                      final long delayMs) {
        return atDeadlineAfter(currentTimeMs, Math.max(0L, delayMs), true, true);
    }

    static NextReconcile atReactorDeadlineAfter(final long currentTimeMs,
                                                final long delayMs) {
        return atDeadlineAfter(currentTimeMs, Math.max(0L, delayMs), false, false);
    }

    private static NextReconcile atDeadlineAfter(final long currentTimeMs,
                                                 final long delayMs,
                                                 final boolean compatibilityDeadline,
                                                 final boolean applicationVisible) {
        if (delayMs < 0L)
            throw new IllegalArgumentException("Reconcile delay must not be negative: " + delayMs);

        if (delayMs == Long.MAX_VALUE || currentTimeMs > Long.MAX_VALUE - delayMs)
            return onEvent();

        return new NextReconcile(
            Type.AT_DEADLINE,
            currentTimeMs + delayMs,
            compatibilityDeadline,
            applicationVisible,
            0L
        );
    }

    NextReconcile withSemanticGeneration(final long semanticGeneration) {
        if (type == Type.ON_EVENT)
            return this;
        return new NextReconcile(
            type,
            deadlineAtMs,
            compatibilityDeadline,
            applicationVisible,
            semanticGeneration
        );
    }

    Type type() {
        return type;
    }

    long deadlineAtMs() {
        return deadlineAtMs;
    }

    long remainingMs(final long currentTimeMs) {
        return type == Type.ON_EVENT ? Long.MAX_VALUE : Math.max(0L, deadlineAtMs - currentTimeMs);
    }

    boolean compatibilityDeadline() {
        return compatibilityDeadline;
    }

    boolean applicationVisible() {
        return applicationVisible;
    }

    long semanticGeneration() {
        return semanticGeneration;
    }
}
