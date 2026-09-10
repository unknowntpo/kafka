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

import java.nio.file.Path;
import java.time.Duration;
import java.lang.management.ManagementFactory;
import jdk.jfr.Recording;

/** Separate CPU/allocation recordings, never used as timing benchmark results. */
public class BusyProfile {
    public static void main(String[] args) throws Exception {
        String mode = args[0];
        try (UnifiedLoopWorkload w = new UnifiedLoopWorkload(32, "BUSY", true, false)) {
            long warmUntil = System.nanoTime() + 1_000_000_000L;
            do { for (int i = 0; i < 1024; i++) w.pass(); } while (System.nanoTime() < warmUntil);
            try (Recording recording = new Recording()) {
                if (mode.equals("cpu")) {
                    recording.enable("jdk.ExecutionSample").withPeriod(Duration.ofMillis(5)).withStackTrace();
                } else {
                    recording.enable("jdk.ObjectAllocationSample").with("throttle", "300/s").withStackTrace();
                }
                recording.start();
                long until = System.nanoTime() + 5_000_000_000L;
                long passes = 0;
                com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
                long allocated = bean.getThreadAllocatedBytes(Thread.currentThread().getId());
                long cpu = bean.getCurrentThreadCpuTime();
                do { for (int i = 0; i < 1024; i++) w.pass(); passes += 1024; } while (System.nanoTime() < until);
                cpu = bean.getCurrentThreadCpuTime() - cpu;
                allocated = bean.getThreadAllocatedBytes(Thread.currentThread().getId()) - allocated;
                recording.stop();
                recording.dump(Path.of(args[1]));
                if (w.published != w.consumed) throw new AssertionError("unconsumed input");
                System.out.println(mode + ",passes=" + passes + ",cpuNsPerPass=" + (double) cpu / passes
                    + ",bytesPerPass=" + (double) allocated / passes + ",published=" + w.published);
            }
        }
    }
}
