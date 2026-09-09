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
package org.apache.kafka.clients.consumer.ng.loop;

import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.RequestManager;
import org.apache.kafka.clients.consumer.internals.WaitCondition;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Drives one {@link RequestManager} from the event loop under design semantics S1: after every run the manager
 * declares what it is waiting for ({@link RequestManager#waitCondition()}), and it is run again only when
 * <ul>
 *   <li>its own timer expires (from its {@code timeUntilNextPollMs}, clamped to {@link LoopTimer#MIN_DELAY_MS}),</li>
 *   <li>the declared input arrives with a version strictly newer than the one it declared under, or</li>
 *   <li>a command from the application thread was processed in this pass.</li>
 * </ul>
 * Nothing else can run it. The verification against the declared version is what makes self-triggered busy loops
 * impossible for a manager that declares accurately: a condition that was already true when declared does not
 * count, and neither does an input that arrived before the declaration.
 *
 * <p>The default declaration {@link WaitCondition#ANY_INPUT} reproduces the previous implementation's behaviour
 * restricted to "an input with identity arrived": managers depend on each other through undeclared future chains
 * (an OffsetFetch completing in the commit manager queues a ListOffsets in the offsets manager), so until a
 * manager's dependencies are declared it must be run on any input (contract R6).
 *
 * <p>Order (semantics S6): a timer expiry does not run the manager on the spot; it marks the task {@link #isDue()
 * due}, and the loop runs every due or triggered manager in registration order in one place. Managers share state
 * through read-then-clear pairs (the commit manager reads the coordinator's fatal error, the heartbeat manager
 * clears it), so the order the previous implementation ran them in is a contract, whatever the trigger.
 *
 * <p>Liveness: the manager runs at most once per loop pass, whatever combination of triggers fired, and its timer is
 * always re-armed, also when {@code poll()} throws (then after {@link #FAILURE_RETRY_MS}).
 */
final class ManagerTask {

    /** Every manager is run at least this often even if it asked to wait forever, as a safety net. */
    static final long MAX_INTERVAL_MS = 1_000;
    /** Delay before a manager whose {@code poll()} threw is run again. */
    static final long FAILURE_RETRY_MS = 100;

    private final RequestManager manager;
    private final NetworkClientDelegate network;
    private final LoopTimer timer;
    private final LongSupplier currentPass;
    private final LongSupplier stateVersion;
    private final Runnable onResponse;
    private final Runnable onDue;
    private LoopTimer.Handle scheduled;
    private long scheduledDeadlineMs = Long.MAX_VALUE;
    private long lastRunPass = -1;
    /**
     * What the manager declared after its last run, and the versions it declared under. All written on the loop
     * thread only; volatile because the class also has callback-written state and SpotBugs' thread-safety
     * detectors require it for every shared primitive.
     */
    private volatile WaitCondition declared = WaitCondition.ANY_INPUT;
    private volatile long declaredStateVersion = -1;
    private volatile long declaredOwnCompletions = -1;
    /** Completions of this manager's own requests so far (advanced on the loop thread, inside the network poll). */
    private final AtomicLong ownCompletions = new AtomicLong();
    /** Whether the most recent run produced no request (observability, semantics S7). */
    private volatile boolean lastRunSentNothing;
    /** The manager's timer expired; it runs on the next ordered manager run. */
    private volatile boolean due;

    /**
     * @param currentPass  supplies the loop's current pass number; a task runs at most once per pass
     * @param stateVersion supplies the loop's state version (advanced by every input with identity)
     * @param onResponse   called when one of this manager's requests completes (loop thread, inside the network poll)
     * @param onDue        called when this manager's timer expires, so the loop schedules an ordered manager run
     */
    ManagerTask(RequestManager manager,
                NetworkClientDelegate network,
                LoopTimer timer,
                LongSupplier currentPass,
                LongSupplier stateVersion,
                Runnable onResponse,
                Runnable onDue) {
        this.manager = manager;
        this.network = network;
        this.timer = timer;
        this.currentPass = currentPass;
        this.stateVersion = stateVersion;
        this.onResponse = onResponse;
        this.onDue = onDue;
    }

    /** @return {@code true} if the manager's timer expired and it has not run since */
    boolean isDue() {
        return due;
    }

    RequestManager manager() {
        return manager;
    }

    /** @return what the manager declared after its last run */
    WaitCondition declared() {
        return declared;
    }

    /** @return {@code true} if the most recent run produced no request */
    boolean lastRunSentNothing() {
        return lastRunSentNothing;
    }

    /**
     * An input addressed to this manager alone (S2: inputs have identity), from the loop thread: the manager runs on
     * the next pass without the version check, as after its own timer. Used when the loop knows exactly which
     * manager has new work (a fetch request to create, an auto-commit to send) so the others are not re-run.
     */
    void trigger() {
        due = true;
    }

    /**
     * Loop thread, during a pass in which some input arrived: does the declared trigger allow a run now?
     *
     * @param commandProcessed a command from the application thread was processed in this pass
     */
    boolean wantsRun(boolean commandProcessed) {
        if (commandProcessed || due)
            return true;
        switch (declared) {
            case ANY_INPUT:
                return stateVersion.getAsLong() > declaredStateVersion;
            case OWN_COMPLETION:
                return ownCompletions.get() > declaredOwnCompletions;
            case TIMER_ONLY:
            default:
                return false;
        }
    }

    /**
     * Runs the manager unless it already ran in the current pass, then records its declaration.
     *
     * @return {@code true} if the manager was run
     */
    boolean run(long currentTimeMs) {
        long pass = currentPass.getAsLong();
        if (lastRunPass == pass)
            return false;
        lastRunPass = pass;
        due = false;
        NetworkClientDelegate.PollResult result;
        try {
            result = manager.poll(currentTimeMs);
            lastRunSentNothing = result.unsentRequests.isEmpty();
            for (NetworkClientDelegate.UnsentRequest request : result.unsentRequests) {
                request.whenComplete((response, error) -> {
                    ownCompletions.incrementAndGet();
                    onResponse.run();
                });
            }
            network.addAll(result);
        } catch (RuntimeException | Error e) {
            // Keep the manager alive: re-arm its timer before letting the loop log the failure.
            declare();
            reschedule(currentTimeMs, FAILURE_RETRY_MS, true);
            throw e;
        }
        declare();
        reschedule(currentTimeMs, Math.min(result.timeUntilNextPollMs, MAX_INTERVAL_MS), false);
        return true;
    }

    /** Records the manager's declaration and the versions it was made under (S1: verification baseline). */
    private void declare() {
        WaitCondition condition = manager.waitCondition();
        // A manager that declares nothing (null, e.g. a mock) gets the safe default.
        declared = condition == null ? WaitCondition.ANY_INPUT : condition;
        declaredStateVersion = stateVersion.getAsLong();
        declaredOwnCompletions = ownCompletions.get();
    }

    /**
     * Only touches the timer when the manager wants to run earlier than already scheduled (or {@code force});
     * otherwise the pending timer is kept. This bounds the number of timer entries a frequently re-run manager can
     * create.
     */
    private void reschedule(long currentTimeMs, long delayMs, boolean force) {
        long delay = Math.max(LoopTimer.MIN_DELAY_MS, delayMs);
        long deadline = currentTimeMs + delay;
        if (!force && scheduled != null && scheduled.isPending() && deadline >= scheduledDeadlineMs)
            return;
        if (scheduled != null)
            scheduled.cancel();
        scheduled = timer.schedule(currentTimeMs, delay, () -> {
            due = true;
            onDue.run();
        });
        scheduledDeadlineMs = deadline;
    }
}
