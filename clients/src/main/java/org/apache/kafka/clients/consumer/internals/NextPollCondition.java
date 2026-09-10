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
 * A one-shot condition for the next manager poll. Conditions and signals are confined to the
 * consumer network thread. External input must use the existing event queue and network wakeup.
 */
/**
 * A manager's next opportunity to recheck its state, expressed as input publication, a deadline,
 * or either of them. Readiness is permission to poll again, not a guarantee that a request can be sent.
 * The scheduler registers these dependencies instead of repeatedly evaluating every condition.
 *
 * <p>Conditions, signals and registration callbacks are network-thread confined. They do not provide
 * cross-thread signalling or interrupt network I/O; those remain the event queue and network wakeup's
 * responsibility. A manager should capture signal generations before work that can publish input.
 */
public interface NextPollCondition {
    /** Register once; publication activates the registration callback, not manager.poll itself. */
    void register(Registration registration);

    /** Sink used to flatten a condition into signal subscriptions and its earliest deadline. */
    interface Registration {
        void onSignal(Signal signal, long observed);

        void onDeadline(long deadlineMs);
    }

    /** Direct inspection for callers; production scheduling uses registration rather than a scan. */
    boolean isReady(long nowMs);

    /** Direct delay projection; scheduler deadlines are collected through Registration instead. */
    long remainingMs(long nowMs);

    /** Record an absolute deadline; this creates no independent timer thread or operating-system alarm. */
    static NextPollCondition after(long nowMs, long delayMs) {
        if (delayMs < 0)
            throw new IllegalArgumentException("Negative delay");
        long deadline = nowMs > Long.MAX_VALUE - delayMs ? Long.MAX_VALUE : nowMs + delayMs;
        return new NextPollCondition() {
            @Override
            public void register(Registration registration) {
                registration.onDeadline(deadline);
            }

            @Override
            public boolean isReady(long now) {
                return now >= deadline;
            }

            @Override
            public long remainingMs(long now) {
                if (isReady(now))
                    return 0;
                long remaining = deadline - now;
                return remaining < 0 ? Long.MAX_VALUE : remaining;
            }
        };
    }

    /** Either child can activate the manager; nesting preserves the earliest deadline. */
    static NextPollCondition anyOf(NextPollCondition first, NextPollCondition second) {
        Objects.requireNonNull(first);
        Objects.requireNonNull(second);
        return new NextPollCondition() {
            @Override
            public void register(Registration registration) {
                first.register(registration);
                second.register(registration);
            }

            @Override
            public boolean isReady(long now) {
                return first.isReady(now) || second.isReady(now);
            }

            @Override
            public long remainingMs(long now) {
                return Math.min(first.remainingMs(now), second.remainingMs(now));
            }
        };
    }

    /**
     * Network-thread-owned publication source with one-shot subscribers. A generation records that
     * input changed even when no subscriber was installed yet; it carries no protocol-state payload.
     * Owners update their state before publishing, and an awakened manager reads that state on its poll.
     */
    final class Signal {
        private static final Runnable NOOP = () -> { };
        private long generation;
        private Subscription first;
        private Subscription last;
        private int subscriberCount;

        /** Detach and notify the current subscriber generation; re-registration belongs to a later one. */
        public void publish() {
            generation++;
            // Detach this generation. New registrations belong to a later publication.
            Subscription current = first;
            first = null;
            last = null;
            subscriberCount = 0;
            while (current != null) {
                Subscription next = current.next;
                Runnable callback = current.callback;
                current.callback = null;
                current.previous = null;
                current.next = null;
                if (callback != null)
                    callback.run();
                current = next;
            }
        }

        /**
         * Invoke immediately if publication occurred after capture, otherwise install a one-shot
         * subscriber. The returned handle must also be collected in the immediate-callback case.
         */
        Runnable subscribe(long observed, Runnable subscriber) {
            if (generation != observed) {
                subscriber.run();
                return NOOP;
            }
            Subscription subscription = new Subscription(subscriber);
            subscription.previous = last;
            if (last == null)
                first = subscription;
            else
                last.next = subscription;
            last = subscription;
            subscriberCount++;
            return subscription;
        }

        int subscriberCount() {
            return subscriberCount;
        }

        /** The subscription is its own cancellation handle; no hash lookup or detach lambda. */
        private final class Subscription implements Runnable {
            private final long registeredGeneration = generation;
            private Runnable callback;
            private Subscription previous;
            private Subscription next;

            private Subscription(Runnable callback) {
                this.callback = callback;
            }

            @Override
            public void run() {
                if (callback == null)
                    return;
                callback = null;
                if (registeredGeneration != generation)
                    return; // An in-progress publication owns the detached next pointers.
                if (previous == null)
                    first = next;
                else
                    previous.next = next;
                if (next == null)
                    last = previous;
                else
                    next.previous = previous;
                previous = null;
                next = null;
                subscriberCount--;
            }
        }

        /** Capture now, before producing the poll result, so publication before registration is visible. */
        public NextPollCondition await() {
            long observed = generation;
            return new NextPollCondition() {
                @Override
                public void register(Registration registration) {
                    registration.onSignal(Signal.this, observed);
                }

                @Override
                public boolean isReady(long now) {
                    return generation != observed;
                }

                @Override
                public long remainingMs(long now) {
                    return isReady(now) ? 0 : Long.MAX_VALUE;
                }
            };
        }
    }
}
