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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;

/** Real runOnce, metrics and reaper; deterministic manager list, clock and transport boundaries. */
public final class NetworkLoopFixture implements AutoCloseable {
    public final Clock clock = new Clock();
    public final LinkedBlockingQueue<ApplicationEvent> events = new LinkedBlockingQueue<>();
    public final Metrics metrics = new Metrics();
    public final AsyncConsumerMetrics asyncMetrics = new AsyncConsumerMetrics(metrics, "loop-validation");
    public final Transport transport = new Transport(clock, asyncMetrics);
    public final ConsumerNetworkThread thread;
    public final CompletableEventReaper reaper = new CompletableEventReaper(new LogContext());

    public NetworkLoopFixture(List<RequestManager> managers) {
        ManagerList list = new ManagerList(managers);
        Processor processor = new Processor(list);
        thread = new ConsumerNetworkThread(new LogContext(), clock, events, reaper,
                () -> processor, () -> transport, () -> list, asyncMetrics);
        thread.initializeResources();
    }

    public void runOnce() {
        clock.now++;
        thread.runOnce();
    }

    public void enqueue(Runnable action) {
        Action event = new Action(action);
        event.setEnqueuedMs(clock.now);
        events.add(event);
        thread.wakeup();
    }

    @Override
    public void close() {
        thread.cleanup();
        asyncMetrics.close();
        metrics.close();
    }

    private static final class ManagerList extends RequestManagers {
        private final List<RequestManager> managers;

        private ManagerList(List<RequestManager> managers) {
            super(new LogContext(), mock(ShareConsumeRequestManager.class),
                    Optional.empty(), Optional.empty(), Optional.empty());
            this.managers = managers;
        }

        @Override
        public List<RequestManager> entries() {
            return managers;
        }

        @Override
        public void close() { }
    }

    private static final class Processor extends ApplicationEventProcessor {
        private Processor(RequestManagers managers) {
            super(new LogContext(), managers, mock(Metadata.class), null);
        }

        @Override
        public void process(ApplicationEvent event) {
            ((Action) event).action.run();
        }
    }

    private static final class Action extends ApplicationEvent {
        private final Runnable action;

        private Action(Runnable action) {
            super(Type.ASYNC_POLL);
            this.action = action;
        }
    }

    public static final class Clock implements Time {
        public long now = 1;

        @Override
        public long milliseconds() { return now; }
        @Override
        public long nanoseconds() { return now * 1000000; }
        @Override
        public void sleep(long ms) { now += ms; }
        @Override
        public void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) {
            throw new UnsupportedOperationException("This fixture does not block");
        }
    }

    public static final class Transport extends NetworkClientDelegate {
        public long polls;
        public long waitMs;
        public long wakeups;
        public long admitted;
        public Runnable completion;
        public boolean captureRequests;
        public final List<UnsentRequest> requests = new ArrayList<>();

        private Transport(Time time, AsyncConsumerMetrics metrics) {
            super(time, config(), new LogContext(), null, null,
                    new BackgroundEventHandler(new LinkedBlockingQueue<>(), time, metrics), false, metrics);
        }

        @Override
        public long addAll(PollResult result) {
            admitted += result.unsentRequests.size();
            if (captureRequests)
                requests.addAll(result.unsentRequests);
            return result.timeUntilNextPollMs;
        }

        @Override
        public void poll(long timeoutMs, long currentTimeMs) {
            polls++;
            waitMs = timeoutMs;
            Runnable callback = completion;
            completion = null;
            if (callback != null)
                callback.run();
        }

        @Override
        public boolean hasAnyPendingRequests() { return false; }
        @Override
        public void wakeup() { wakeups++; }
        @Override
        public void close() { }

        private static ConsumerConfig config() {
            Map<String, Object> values = new HashMap<>();
            values.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            values.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            values.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            return new ConsumerConfig(values);
        }
    }
}
