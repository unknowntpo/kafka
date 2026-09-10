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

package org.apache.kafka.jmh.consumer;

import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.NextPollCondition;
import org.apache.kafka.clients.consumer.internals.RequestManager;
import org.apache.kafka.clients.consumer.internals.RequestManagerScheduler;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Isolates seven-manager deadline-index operations; contains no broker or transport. */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class DeadlineIndexBenchmark {
    @Param({"IDLE", "DUE", "SIGNAL", "MIXED"})
    public String workload;
    private RequestManagerScheduler scheduler;
    private final NextPollCondition.Signal[] signals = new NextPollCondition.Signal[7];
    private long now;
    private long calls;
    private long orderHash;

    @Setup(Level.Trial)
    public void setup() {
        scheduler = new RequestManagerScheduler();
        List<RequestManager> managers = new ArrayList<>();
        for (int i = 0; i < signals.length; i++) {
            final int id = i;
            signals[i] = new NextPollCondition.Signal();
            managers.add(time -> {
                calls++;
                orderHash = orderHash * 31 + id;
                if (workload.equals("MIXED") && id == 6)
                    return NetworkClientDelegate.PollResult.EMPTY;
                long delay = workload.equals("DUE") ? 1 : workload.equals("MIXED") ? (id + 1) * 8 : 1000000;
                NextPollCondition condition = NextPollCondition.after(time, delay);
                if (workload.equals("SIGNAL") || workload.equals("MIXED"))
                    condition = NextPollCondition.anyOf(condition, signals[id].await());
                return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), condition);
            });
        }
        scheduler.registerManagers(managers);
        scheduler.pollReady(0, r -> r.timeUntilNextPollMs);
    }

    public long step() {
        if (!workload.equals("IDLE"))
            now++;
        if (workload.equals("SIGNAL") || (workload.equals("MIXED") && now % 16 == 0))
            signals[(int) (now % signals.length)].publish();
        long delay = scheduler.pollReady(now, r -> r.timeUntilNextPollMs);
        return delay ^ calls ^ orderHash;
    }

    @Benchmark
    public long deadlineLoop(ConsumerCpuProfiler.CpuState cpu) {
        return step();
    }

    @TearDown(Level.Trial)
    public void close() {
        scheduler.close();
    }

    public static void main(String[] args) {
        for (String scenario : List.of("IDLE", "DUE", "SIGNAL", "MIXED")) {
            DeadlineIndexBenchmark benchmark = new DeadlineIndexBenchmark();
            benchmark.workload = scenario;
            benchmark.setup();
            long checksum = 0;
            for (int i = 0; i < 100000; i++)
                checksum = checksum * 31 + benchmark.step();
            System.out.println(scenario + " calls=" + benchmark.calls + " order=" + benchmark.orderHash + " checksum=" + checksum);
            benchmark.close();
        }
    }
}
