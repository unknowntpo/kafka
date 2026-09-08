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

import org.apache.kafka.clients.consumer.internals.pipeline.LoopTimer;

import java.util.function.LongSupplier;

/**
 * Drives one {@link RequestManager} from the event loop. The manager is run when its own timer fires and after
 * any request completion or command (a "manager pass", see below); its {@code timeUntilNextPollMs} only
 * reschedules its own timer. Nothing here is aggregated across managers, so a manager that asks to be re-run
 * "immediately" costs one run per {@link LoopTimer#MIN_DELAY_MS} and nothing else.
 *
 * <p>A completed request re-runs every manager, not only the one that sent it: managers depend on each other
 * through future chains that are not declared anywhere (an OffsetFetch completing in the commit manager queues a
 * ListOffsets in the offsets manager; a heartbeat response drives the membership manager, which drives commits).
 * Targeting single managers would need those dependencies to be explicit; until then a pass runs all of them,
 * exactly like the previous implementation did on every iteration, but only when something completed.
 *
 * <p>Liveness: the manager runs at most once per loop pass, whatever combination of triggers fired, and its timer is
 * always re-armed, also when {@code poll()} throws (then after {@link #FAILURE_RETRY_MS}). A task never depends on
 * another task's trigger to get its next turn.
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
    private final Runnable onResponse;
    private LoopTimer.Handle scheduled;
    private long scheduledDeadlineMs = Long.MAX_VALUE;
    private long lastRunPass = -1;

    /**
     * @param currentPass supplies the loop's current pass number; a task runs at most once per pass
     * @param onResponse  called when one of this manager's requests completes (loop thread, inside the network poll)
     */
    ManagerTask(RequestManager manager, NetworkClientDelegate network, LoopTimer timer, LongSupplier currentPass, Runnable onResponse) {
        this.manager = manager;
        this.network = network;
        this.timer = timer;
        this.currentPass = currentPass;
        this.onResponse = onResponse;
    }

    RequestManager manager() {
        return manager;
    }

    /**
     * Runs the manager unless it already ran in the current pass.
     *
     * @return {@code true} if the manager was run
     */
    boolean run(long currentTimeMs) {
        long pass = currentPass.getAsLong();
        if (lastRunPass == pass)
            return false;
        lastRunPass = pass;
        NetworkClientDelegate.PollResult result;
        try {
            result = manager.poll(currentTimeMs);
            for (NetworkClientDelegate.UnsentRequest request : result.unsentRequests)
                request.whenComplete((response, error) -> onResponse.run());
            network.addAll(result);
        } catch (RuntimeException | Error e) {
            // Keep the manager alive: re-arm its timer before letting the loop log the failure.
            reschedule(currentTimeMs, FAILURE_RETRY_MS, true);
            throw e;
        }
        reschedule(currentTimeMs, Math.min(result.timeUntilNextPollMs, MAX_INTERVAL_MS), false);
        return true;
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
        scheduled = timer.schedule(currentTimeMs, delay, () -> run(timer.lastRunTimeMs()));
        scheduledDeadlineMs = deadline;
    }
}
