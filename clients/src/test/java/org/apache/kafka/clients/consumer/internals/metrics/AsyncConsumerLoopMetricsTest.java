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
package org.apache.kafka.clients.consumer.internals.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;

import org.junit.jupiter.api.Test;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The loop observability metrics (design semantics S7) are registered under the consumer group and removed on close. */
public class AsyncConsumerLoopMetricsTest {

    @Test
    public void loopMetricsAreRegisteredAndRemovedOnClose() {
        Metrics metrics = new Metrics();
        AsyncConsumerMetrics consumerMetrics = new AsyncConsumerMetrics(metrics, CONSUMER_METRIC_GROUP);
        MetricName passRate = metrics.metricName("background-pass-rate", CONSUMER_METRIC_GROUP);
        MetricName idleRuns = metrics.metricName("manager-runs-without-requests-rate", CONSUMER_METRIC_GROUP);
        assertTrue(metrics.metrics().containsKey(passRate));
        assertTrue(metrics.metrics().containsKey(idleRuns));

        consumerMetrics.recordBackgroundPass();
        consumerMetrics.recordManagerRunsWithoutRequests(3);
        assertTrue((double) metrics.metric(passRate).metricValue() > 0);
        assertTrue((double) metrics.metric(idleRuns).metricValue() > 0);

        consumerMetrics.close();
        assertFalse(metrics.metrics().containsKey(passRate));
        assertFalse(metrics.metrics().containsKey(idleRuns));
    }
}
