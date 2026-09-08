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
package org.apache.kafka.clients.consumer.internals.pipeline;

/**
 * The event loop's conclusion at the end of one pass, published as a single immutable object (one volatile write)
 * before the loop blocks. It is the loop's answer to "what are you waiting for, and how long": the application
 * thread reads it to decide how long it may wait and whether the loop needs a poll to make progress, instead of
 * inferring that from fragments of shared state observed at different times (KIP-1371, problem 3).
 *
 * <p>{@link #stateVersion} advances whenever an input with identity reached the loop (a request completed, a
 * command was processed, metadata changed). A waiter that recorded a version and finds it unchanged has not
 * missed anything.
 */
public final class PassDecision {

    /** Before the first pass. */
    public static final PassDecision NONE = new PassDecision(0, 0, LoopTimer.NO_DEADLINE, false, false, 0, false, false);

    /** Sequence number of the pass that produced this decision. */
    public final long pass;
    /** Number of inputs with identity the loop has consumed so far (completions, commands, metadata changes). */
    public final long stateVersion;
    /** Earliest timer deadline the loop will wake for on its own, or {@link LoopTimer#NO_DEADLINE}. */
    public final long nextDeadlineMs;
    /** Every assigned partition had a valid fetch position at the end of the pass. */
    public final boolean allPositionsKnown;
    /** A fetch-position attempt has requests outstanding; positions may become known without a new poll. */
    public final boolean positionsAttemptInFlight;
    /** The application poll sequence up to which the reconciliation check has run. */
    public final long reconciliationCheckedPollSequence;
    /**
     * The group member is reconciling an assignment, so the application thread may be waiting for the
     * reconciliation check of its poll before it collects records; only then is an advance of
     * {@link #reconciliationCheckedPollSequence} something it waits for.
     */
    public final boolean reconciliationPending;
    /** The loop queued something for the application thread (background event) during this pass. */
    public final boolean backgroundEventsPending;

    public PassDecision(long pass,
                        long stateVersion,
                        long nextDeadlineMs,
                        boolean allPositionsKnown,
                        boolean positionsAttemptInFlight,
                        long reconciliationCheckedPollSequence,
                        boolean backgroundEventsPending,
                        boolean reconciliationPending) {
        this.pass = pass;
        this.stateVersion = stateVersion;
        this.nextDeadlineMs = nextDeadlineMs;
        this.allPositionsKnown = allPositionsKnown;
        this.positionsAttemptInFlight = positionsAttemptInFlight;
        this.reconciliationCheckedPollSequence = reconciliationCheckedPollSequence;
        this.backgroundEventsPending = backgroundEventsPending;
        this.reconciliationPending = reconciliationPending;
    }

    /**
     * @return {@code true} if something the application thread may be waiting for differs from {@code previous}:
     * positions became known (or an attempt ended), the reconciliation check advanced while a reconciliation is
     * pending, or a background event was queued. The pass number and timer deadline alone are not reasons to wake
     * it, and neither is a reconciliation check nobody waits for: every poll iteration advances the checked
     * sequence, so waking on it unconditionally makes the two threads wake each other for ever (contract R3: a
     * wake-up must correspond to a wait).
     */
    public boolean applicationVisibleChangeSince(PassDecision previous) {
        boolean reconciliationCheckWaitedFor = reconciliationPending || previous.reconciliationPending;
        boolean reconciliationCheckAdvanced = reconciliationCheckedPollSequence != previous.reconciliationCheckedPollSequence;
        return allPositionsKnown != previous.allPositionsKnown
                || positionsAttemptInFlight != previous.positionsAttemptInFlight
                || (reconciliationCheckWaitedFor && reconciliationCheckAdvanced)
                || (backgroundEventsPending && !previous.backgroundEventsPending);
    }

    @Override
    public String toString() {
        return "PassDecision(pass=" + pass + ", version=" + stateVersion + ", nextDeadlineMs=" + nextDeadlineMs +
                ", allPositionsKnown=" + allPositionsKnown + ", positionsAttemptInFlight=" + positionsAttemptInFlight +
                ", reconciliationCheckedPollSequence=" + reconciliationCheckedPollSequence +
                ", backgroundEventsPending=" + backgroundEventsPending +
                ", reconciliationPending=" + reconciliationPending + ")";
    }
}
