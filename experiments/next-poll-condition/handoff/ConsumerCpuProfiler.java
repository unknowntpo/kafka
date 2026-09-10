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

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Iteration CPU snapshots exclude trial setup/close; JMH owns timing, operations and aggregation. */
public class ConsumerCpuProfiler implements InternalProfiler {
    static volatile long validatedRecords;
    private static volatile long processNanos;
    private static volatile long wallNanos;
    private static volatile long cgroupMicros;
    private static volatile long throttledPeriods;

    private static long processCpu() {
        return ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime();
    }

    private static Path cgroup() throws IOException {
        String line = Files.readAllLines(Path.of("/proc/self/cgroup")).stream()
            .filter(value -> value.startsWith("0::")).findFirst().orElseThrow();
        return Path.of("/sys/fs/cgroup" + line.substring(3));
    }

    private static long stat(Path cgroup, String key) throws IOException {
        return Files.readAllLines(cgroup.resolve("cpu.stat")).stream()
            .filter(line -> line.startsWith(key + " ")).mapToLong(line -> Long.parseLong(line.split(" ")[1]))
            .findFirst().orElseThrow();
    }

    @State(Scope.Thread)
    public static class CpuState {
        private Path cgroup;
        private long cpu;
        private long wall;
        private long usage;
        private long throttles;

        @Setup(Level.Trial)
        public void verifyLimits() throws IOException {
            cgroup = cgroup();
            String quota = Files.readString(cgroup.resolve("cpu.max")).trim();
            String memory = Files.readString(cgroup.resolve("memory.max")).trim();
            String swap = Files.readString(cgroup.resolve("memory.swap.max")).trim();
            String affinity = Files.readAllLines(Path.of("/proc/self/status")).stream()
                .filter(line -> line.startsWith("Cpus_allowed_list:")).findFirst().orElseThrow().split(":")[1].trim();
            if (!quota.equals("50000 100000") || !memory.equals("536870912") || !swap.equals("0") || !affinity.equals("2"))
                throw new AssertionError("JMH fork resource limits differ");
            System.out.println("LIMITS " + cgroup + " cpu.max=" + quota + " memory.max=" + memory
                + " memory.swap.max=" + swap + " affinity=" + affinity);
            for (Path ancestor = cgroup; ancestor != null && ancestor.startsWith(Path.of("/sys/fs/cgroup")); ancestor = ancestor.getParent()) {
                for (String name : List.of("cpu.max", "memory.max", "memory.swap.max")) {
                    if (Files.exists(ancestor.resolve(name)))
                        System.out.println("ANCESTOR " + ancestor + " " + name + "=" + Files.readString(ancestor.resolve(name)).trim());
                }
            }
        }

        @Setup(Level.Iteration)
        public void start() throws IOException {
            usage = stat(cgroup, "usage_usec");
            throttles = stat(cgroup, "nr_throttled");
            cpu = processCpu();
            wall = System.nanoTime();
        }

        @TearDown(Level.Iteration)
        public void stop() throws IOException {
            wallNanos = System.nanoTime() - wall;
            processNanos = processCpu() - cpu;
            cgroupMicros = stat(cgroup, "usage_usec") - usage;
            throttledPeriods = stat(cgroup, "nr_throttled") - throttles;
        }

        @TearDown(Level.Trial)
        public void verifyMemory() throws IOException {
            String events = Files.readString(cgroup.resolve("memory.events"));
            System.out.println("MEMORY " + events.replace('\n', ';'));
            if (!events.contains("oom_kill 0\n") || !events.contains("oom 0\n"))
                throw new AssertionError("Memory limit exceeded");
        }
    }

    @Override
    public String getDescription() {
        return "Process and cgroup CPU per JMH iteration, with record-normalization verification";
    }

    @Override
    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {
        if (benchmarkParams.getThreads() != 1)
            throw new IllegalArgumentException("This profiler requires one benchmark worker");
        validatedRecords = 0;
    }

    @Override
    public Collection<? extends Result> afterIteration(BenchmarkParams params, IterationParams iteration, IterationResult result) {
        List<Result> metrics = new ArrayList<>();
        metrics.add(new ScalarResult("process.cpu", processNanos * 1000.0 / wallNanos, "ms/s", AggregationPolicy.AVG));
        metrics.add(new ScalarResult("cgroup.cpu", cgroupMicros * 1000000.0 / wallNanos, "ms/s", AggregationPolicy.AVG));
        metrics.add(new ScalarResult("cgroup.throttled", throttledPeriods, "periods", AggregationPolicy.AVG));
        if (params.getBenchmark().endsWith(".consume")) {
            long records = result.getMetadata().getAllOps();
            if (records != validatedRecords || records <= 0)
                throw new AssertionError("JMH operations must equal validated records: " + records + " != " + validatedRecords);
            metrics.add(new ScalarResult("process.cpu.per.million.records", processNanos / (double) records, "ms/Mrecord", AggregationPolicy.AVG));
            metrics.add(new ScalarResult("validated.records", records, "records", AggregationPolicy.SUM));
        }
        return metrics;
    }
}
