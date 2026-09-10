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

import java.lang.management.ManagementFactory;
import java.util.Arrays;

public class LoopMetricsProbe {
    public static void main(String[] args) {
        boolean conditional = Boolean.parseBoolean(args[0]);
        com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        bean.setThreadAllocatedMemoryEnabled(true);
        bean.setThreadCpuTimeEnabled(true);
        for (int count : new int[] {8, 32}) {
            for (String pattern : new String[] {"IDLE", "SPARSE", "BUSY", "MIXED"}) {
                try (NetworkLoopWorkload w = new NetworkLoopWorkload(count, pattern, conditional, false)) {
                    long warmUntil = System.nanoTime() + 500_000_000L;
                    do { for (int i = 0; i < 1024; i++) w.pass(); } while (System.nanoTime() < warmUntil);
                    for (int sample = 0; sample < 5; sample++) {
                        long allocations = bean.getThreadAllocatedBytes(Thread.currentThread().getId());
                        long cpu = bean.getCurrentThreadCpuTime();
                        long wall = System.nanoTime();
                        for (int i = 0; i < 100000; i++) w.pass();
                        wall = System.nanoTime() - wall;
                        cpu = bean.getCurrentThreadCpuTime() - cpu;
                        allocations = bean.getThreadAllocatedBytes(Thread.currentThread().getId()) - allocations;
                        System.out.println("COST," + count + "," + pattern + "," + sample + "," + cpu / 100000.0
                                + "," + allocations / 100000.0 + "," + wall / 100000.0);
                    }
                    if (w.published != w.consumed || w.maxLagLoops != 0) throw new AssertionError("Work mismatch");
                    System.out.println("WORK," + count + "," + pattern + "," + w.published + "," + w.consumed
                            + "," + w.managerPolls + "," + w.maximumWaitCalls + "," + w.fixture.transport.polls);
                }
                // Latency instrumentation is separate from CPU/allocation measurement.
                if (!pattern.equals("IDLE")) {
                    try (NetworkLoopWorkload w = new NetworkLoopWorkload(count, pattern, conditional, true)) {
                        for (int i = 0; i < 100000; i++) w.pass();
                        w.latencyCount = 0;
                        for (int i = 0; i < 20000; i++) w.pass();
                        Arrays.sort(w.latencyNs, 0, w.latencyCount);
                        int n = w.latencyCount;
                        System.out.println("LATENCY," + count + "," + pattern + "," + n + ","
                                + w.latencyNs[n / 2] + "," + w.latencyNs[(int) (n * 0.95)] + ","
                                + w.latencyNs[(int) (n * 0.99)] + "," + w.maxLagLoops);
                    }
                }
            }
        }
    }
}
