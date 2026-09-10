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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MemoryProbe {
    static final List<NextPollCondition.Signal> signals = new ArrayList<>();
    static final List<RequestManager> managers = new ArrayList<>();
    static RequestManagerScheduler scheduler;
    static long clock;
    static long sink;

    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            allocation(args[0]);
            return;
        }
        scheduler = new RequestManagerScheduler();
        for (int i = 0; i < 1000; i++) {
            NextPollCondition.Signal signal = new NextPollCondition.Signal();
            signals.add(signal);
            managers.add(now -> new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(),
                    NextPollCondition.anyOf(signal.await(), NextPollCondition.after(now, 1000000))));
        }
        scheduler.registerManagers(managers);
        scheduler.pollReady(clock, result -> result.timeUntilNextPollMs);
        checkpoint("initial");
        cycle(100000);
        checkpoint("after100k");
        cycle(400000);
        checkpoint("after500k");
        scheduler.close();
        checkpoint("closed");
    }

    static void cycle(int count) {
        for (int i = 0; i < count; i++) {
            signals.get(i % signals.size()).publish();
            scheduler.pollReady(++clock, result -> result.timeUntilNextPollMs);
        }
    }

    static void checkpoint(String name) throws Exception {
        int subscribers = 0;
        for (NextPollCondition.Signal signal : signals) subscribers += signal.subscriberCount();
        int expected = name.equals("closed") ? 0 : 1000;
        if (subscribers != expected) throw new AssertionError("Subscriber count " + subscribers);
        System.out.println(name + " subscribers=" + subscribers);
        System.out.flush();
        if (new BufferedReader(new InputStreamReader(System.in)).readLine() == null)
            throw new AssertionError("Checkpoint acknowledgement missing");
    }

    static void allocation(String mode) {
        com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!bean.isThreadAllocatedMemorySupported()) throw new AssertionError("Allocation counters unavailable");
        bean.setThreadAllocatedMemoryEnabled(true);
        for (String workload : new String[] {"QUIET", "BURST", "BUSY", "SUCCESS"}) {
            NextPollConditionBenchmark benchmark = new NextPollConditionBenchmark();
            benchmark.mode = mode;
            benchmark.workload = workload;
            benchmark.setup();
            for (int i = 0; i < 2000; i++) sink += benchmark.pollPasses();
            for (int sample = 0; sample < 3; sample++) {
                long before = bean.getThreadAllocatedBytes(Thread.currentThread().getId());
                for (int i = 0; i < 500; i++) sink += benchmark.pollPasses();
                long bytes = bean.getThreadAllocatedBytes(Thread.currentThread().getId()) - before;
                System.out.println(workload + "," + sample + "," + bytes + "," + (bytes / 512000.0));
            }
        }
    }
}
