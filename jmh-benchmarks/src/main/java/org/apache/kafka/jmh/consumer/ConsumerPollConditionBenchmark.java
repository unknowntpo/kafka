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

import org.apache.kafka.clients.consumer.internals.NextPollCondition;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 8)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ConsumerPollConditionBenchmark {
    private static final long CURRENT_TIME_MS = 1_000_000L;

    @Param({"1", "4", "11", "32"})
    private int conditionCount;

    private long[] delays;
    private NextPollCondition[] conditions;

    @Setup
    public void setup() {
        delays = new long[conditionCount];
        conditions = new NextPollCondition[conditionCount];
        for (int i = 0; i < conditionCount; i++) {
            delays[i] = 50L + i * 17L;
            conditions[i] = NextPollCondition.after(CURRENT_TIME_MS, delays[i]);
        }
    }

    @Benchmark
    public long composeFreshDeadlines() {
        NextPollCondition result = NextPollCondition.after(CURRENT_TIME_MS, 5_000L);
        for (long delay : delays)
            result = NextPollCondition.either(result,
                    NextPollCondition.after(CURRENT_TIME_MS, delay));
        return result.remainingMs(CURRENT_TIME_MS);
    }

    @Benchmark
    public long composeExistingConditions() {
        NextPollCondition result = NextPollCondition.after(CURRENT_TIME_MS, 5_000L);
        for (NextPollCondition condition : conditions)
            result = NextPollCondition.either(result, condition);
        return result.remainingMs(CURRENT_TIME_MS);
    }

    @Benchmark
    public long numericMinimumReference() {
        long result = 5_000L;
        for (long delay : delays)
            result = Math.min(result, delay);
        return result;
    }
}
