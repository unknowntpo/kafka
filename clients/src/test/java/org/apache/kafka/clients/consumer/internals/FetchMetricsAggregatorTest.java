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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FetchMetricsAggregatorTest {

    private final TopicPartition tp0 = new TopicPartition("t", 0);
    private final TopicPartition tp1 = new TopicPartition("t", 1);

    @Test
    public void testCompletionRunsOnceAllPartitionsAreRecorded() {
        FetchMetricsManager metricsManager = new FetchMetricsManager(new Metrics(), new FetchMetricsRegistry());
        AtomicInteger completions = new AtomicInteger();
        FetchMetricsAggregator aggregator = new FetchMetricsAggregator(metricsManager, Set.of(tp0, tp1), completions::incrementAndGet);

        aggregator.record(tp0, 10, 1);
        assertEquals(0, completions.get(), "the fetch still has an undrained partition");

        aggregator.record(tp1, 10, 1);
        assertEquals(1, completions.get());
    }

    @Test
    public void testNoCompletionHookIsFine() {
        FetchMetricsManager metricsManager = new FetchMetricsManager(new Metrics(), new FetchMetricsRegistry());
        FetchMetricsAggregator aggregator = new FetchMetricsAggregator(metricsManager, Set.of(tp0));
        aggregator.record(tp0, 10, 1);
    }
}
