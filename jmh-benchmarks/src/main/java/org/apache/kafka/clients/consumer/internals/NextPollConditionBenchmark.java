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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.internals.LogContext;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/** Measures real coordinator polling plus scheduling, excluding transport and broker work. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Fork(3)
public class NextPollConditionBenchmark {
    @Param({"FULL", "SCHEDULED"})
    public String mode;

    @Param({"QUIET", "BURST", "BUSY", "SUCCESS"})
    public String workload;

    private CoordinatorRequestManager manager;
    private RequestManagerScheduler scheduler;
    private NetworkClientDelegate.UnsentRequest pending;
    private final TimeoutException failure = new TimeoutException("benchmark");
    private long now;
    private long requestsThisPass;
    private int completionPeriod;

    @Setup
    public void setup() {
        manager = new CoordinatorRequestManager(new LogContext(), 0, 0, "benchmark");
        scheduler = new RequestManagerScheduler();
        scheduler.registerManagers(Collections.singletonList(manager));
        completionPeriod = workload.equals("BUSY") ? 1 : workload.equals("BURST") ? 64 : 0;
    }

    @Benchmark
    @OperationsPerInvocation(1024)
    public long pollPasses() {
        long checksum = 0;
        for (int i = 0; i < 1024; i++)
            checksum += pollPass();
        return checksum;
    }

    // Keep a per-pass boundary in every group; otherwise QUIET can collapse into constant arithmetic.
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public long pollPass() {
        now++;
        if (workload.equals("SUCCESS")) {
            if (pending != null) {
                pending.handler().onComplete(new ClientResponse(
                        new RequestHeader(ApiKeys.FIND_COORDINATOR, pending.requestBuilder().build().version(), "", 1),
                        pending.handler(), "1", now - 1, now, false, null, null,
                        FindCoordinatorResponse.prepareResponse(Errors.NONE, "benchmark", new Node(1, "localhost", 9092))));
                pending = null;
            }
            if (now % 64 == 0)
                manager.markCoordinatorUnknown("benchmark invalidation", now);
        }
        if (pending != null && completionPeriod != 0 && now % completionPeriod == 0) {
            pending.handler().onFailure(now, failure);
            pending = null;
        }
        requestsThisPass = 0;
        long wait = mode.equals("SCHEDULED")
                ? scheduler.pollReady(now, this::admit) : admit(manager.poll(now));
        return wait + requestsThisPass;
    }

    private long admit(NetworkClientDelegate.PollResult result) {
        if (!result.unsentRequests.isEmpty()) {
            pending = result.unsentRequests.get(0);
            requestsThisPass++;
        }
        return result.timeUntilNextPollMs;
    }
}
