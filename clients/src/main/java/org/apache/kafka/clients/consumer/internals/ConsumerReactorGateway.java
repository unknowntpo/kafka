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

import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.io.Closeable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * Thread-safe application-side gateway to the {@link ConsumerReactor}. The application thread can submit input,
 * wait for a submitted operation, signal the reactor, and read the published application wait. State transitions,
 * manager polling, schedule publication, and external action ordering remain owned by the reactor.
 */
public class ConsumerReactorGateway implements Closeable {

    private final Logger log;
    private final Time time;

    /**
     * Ownership-transfer queue from application callers to the single reactor thread. Admission uses
     * {@link BlockingQueue#offer(Object)} so submission remains non-blocking. The current queue is unbounded; any
     * future capacity or overload policy requires a separate compatibility decision.
     */
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;

    /** Execution owner hidden behind this thread-safe submission and lifecycle boundary. */
    private final ConsumerReactor reactor;
    private final IdempotentCloser closer = new IdempotentCloser();
    private final AsyncConsumerMetrics asyncConsumerMetrics;

    public ConsumerReactorGateway(final LogContext logContext,
                                   final Time time,
                                   final int initializationTimeoutMs,
                                   final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                   final CompletableEventReaper applicationEventReaper,
                                   final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier,
                                   final Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                                   final Supplier<RequestManagers> requestManagersSupplier,
                                   final AsyncConsumerMetrics asyncConsumerMetrics) {
        this.log = logContext.logger(ConsumerReactorGateway.class);
        this.time = time;
        this.applicationEventQueue = applicationEventQueue;
        this.asyncConsumerMetrics = asyncConsumerMetrics;
        ConsumerReactor reactor = new ConsumerReactor(logContext,
                time,
                applicationEventQueue,
                applicationEventReaper,
                applicationEventProcessorSupplier,
                networkClientDelegateSupplier,
                requestManagersSupplier,
                asyncConsumerMetrics);

        try {
            reactor.start(initializationTimeoutMs);
        } catch (Exception e) {
            try {
                reactor.close();
            } finally {
                reactor = null;
            }
            throw e;
        } finally {
            this.reactor = reactor;
        }
    }

    /**
     * Submit an {@link ApplicationEvent} and then signal the reactor. Admission always occurs before signaling.
     *
     * @param event An {@link ApplicationEvent} created by the application thread
     * @throws KafkaException if the consumer reactor is no longer alive
     */
    public void submit(final ApplicationEvent event) {
        Objects.requireNonNull(event, "ApplicationEvent provided to submit must be non-null");
        ensureReactorAlive();
        event.setEnqueuedMs(time.milliseconds());
        int queueSizeBeforeSubmit = applicationEventQueue.size();
        if (!applicationEventQueue.offer(event)) {
            throw new KafkaException("The consumer reactor input queue is full and cannot accept " + event.type());
        }
        // Use the pre-admission snapshot because the reactor may drain the event immediately after offer succeeds.
        asyncConsumerMetrics.recordApplicationEventQueueSize(queueSizeBeforeSubmit + 1);
        reactor.wakeup();
    }

    /**
     * Wakeup the {@link ConsumerReactor reactor thread} to pull the next event(s) from the queue.
     */
    public void signalReactor() {
        ensureReactorAlive();
        reactor.wakeup();
    }

    /**
     * Returns the delay for which the application thread can safely wait before it should be responsive
     * to results from the request managers. For example, the subscription state can change when heartbeats
     * are sent, so blocking for longer than the heartbeat interval might mean the application thread is not
     * responsive to changes.
     *
     * @return The maximum delay in milliseconds
     */
    public long applicationWaitMs() {
        return reactor.maximumTimeToWait();
    }

    /**
     * Monotonic publication generation used by component tests and diagnostics to verify that an
     * application-visible action observes the schedule published for its reactor phase.
     */
    public long reactorScheduleGeneration() {
        return reactor.reactorScheduleGeneration();
    }

    /**
     * Submit a {@link CompletableApplicationEvent}. The method blocks waiting for the result, and will
     * return the result value upon successful completion; otherwise throws an error.
     *
     * <p/>
     *
     * See {@link ConsumerUtils#getResult(Future)} for more details.
     *
     * @param event A {@link CompletableApplicationEvent} created by the polling thread
     * @return      Value that is the result of the event
     * @param <T>   Type of return value of the event
     */
    public <T> T submitAndAwait(final CompletableApplicationEvent<T> event) {
        Objects.requireNonNull(event, "CompletableApplicationEvent provided to submitAndAwait must be non-null");
        submit(event);
        // Check if the thread was interrupted before we start waiting, to ensure that we
        // propagate the exception even if we end up not having to wait (the event could complete
        // between the time it's added and the time we attempt to getResult)
        if (Thread.interrupted()) {
            throw new InterruptException("Interrupted waiting for results for application event " + event);
        }
        return ConsumerUtils.getResult(event.future());
    }

    @Override
    public void close() {
        close(Duration.ZERO);
    }

    public void close(final Duration timeout) {
        closer.close(
                () -> Utils.closeQuietly(() -> reactor.close(timeout), "consumer reactor"),
                () -> log.warn("The consumer reactor gateway was already closed")
        );
    }

    /**
     * Best-effort check that the consumer reactor is still alive. If the thread has
     * already terminated (due to a failure or shutdown), it will never process any events from
     * the queue. Rather than blocking indefinitely or timing out with a misleading error, this
     * fails fast with a clear error message.
     *
     * <p>Note: this is inherently racy — the thread could die between this check and the
     * subsequent queue admission. That narrow window is acceptable because any subsequent call to
     * {@link #submit(ApplicationEvent)} will detect the dead thread immediately.
     *
     * @throws KafkaException if the reactor is not alive
     */
    private void ensureReactorAlive() {
        if (reactor == null || !reactor.isAlive()) {
            throw new KafkaException(
                "The consumer reactor is not running and cannot process requests.");
        }
    }
}
