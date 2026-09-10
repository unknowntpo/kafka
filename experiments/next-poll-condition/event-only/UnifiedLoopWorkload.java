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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Deterministic synthetic managers around the actual network loop; no fabricated per-poll work. */
public final class UnifiedLoopWorkload implements AutoCloseable {
    public final NetworkLoopFixture fixture;
    public long published;
    public long consumed;
    public long managerPolls;
    public long maximumWaitCalls;
    public long sequence;
    public long maxLagLoops;
    public final long[] latencyNs = new long[1000000];
    public int latencyCount;
    private final boolean measureLatency;
    private final String pattern;
    private final List<Manager> managers = new ArrayList<>();

    public UnifiedLoopWorkload(int count, String pattern, boolean conditional, boolean measureLatency) {
        this.pattern = pattern;
        this.measureLatency = measureLatency;
        List<RequestManager> entries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Manager manager = new Manager(conditional && !(pattern.equals("MIXED") && i % 2 == 0));
            managers.add(manager);
            entries.add(manager);
        }
        fixture = new NetworkLoopFixture(entries);
        fixture.runOnce(); // Initial registration is outside the measured region.
    }

    public long pass() {
        sequence++;
        if (pattern.equals("BUSY")) {
            for (Manager manager : managers)
                manager.publish();
        } else if (!pattern.equals("IDLE") && !pattern.equals("TIMER") && sequence % 64 == 0) {
            managers.get((int) ((sequence / 64) % managers.size())).publish();
        }
        fixture.runOnce();
        return consumed;
    }

    @Override
    public void close() { fixture.close(); }

    private final class Manager implements RequestManager {
        private final NextPollCondition.Signal signal = new NextPollCondition.Signal();
        private final boolean conditional;
        private boolean pending;
        private long deadline = 66;
        private long publishedLoop;
        private long publishedNs;

        private Manager(boolean conditional) { this.conditional = conditional; }

        private void publish() {
            if (pending)
                throw new AssertionError("Input was not consumed before next publication");
            pending = true;
            published++;
            publishedLoop = sequence;
            if (measureLatency)
                publishedNs = System.nanoTime();
            if (conditional)
                signal.publish();
        }

        @Override
        public NetworkClientDelegate.PollResult poll(long nowMs) {
            managerPolls++;
            if (pattern.equals("TIMER") && nowMs >= deadline) {
                published++;
                pending = true;
                publishedLoop = sequence;
                deadline = nowMs + 64;
            }
            if (pending) {
                pending = false;
                consumed++;
                maxLagLoops = Math.max(maxLagLoops, sequence - publishedLoop);
                if (measureLatency && latencyCount < latencyNs.length)
                    latencyNs[latencyCount++] = System.nanoTime() - publishedNs;
            }
            return conditional
                    ? new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), pattern.equals("TIMER") ? NextPollCondition.after(nowMs, deadline - nowMs) : signal.await())
                    : NetworkClientDelegate.PollResult.EMPTY;
        }

        @Override
        public long maximumTimeToWait(long nowMs) {
            maximumWaitCalls++;
            return Long.MAX_VALUE;
        }
    }
}
