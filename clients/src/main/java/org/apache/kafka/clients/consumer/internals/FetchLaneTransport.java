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

import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.common.utils.internals.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * CONSUMER-3X-08: a thread for the extra connections to a broker. It owns one network client, sends the fetch
 * requests of those connections and reads their responses from the sockets.
 *
 * <p>Ownership, two modes. A request given to {@link #send} is handed back as a task when it finishes, and the
 * consumer network thread runs that task: the response is handled there. A request given to {@link #sendLocal}
 * completes on this thread: this thread then also owns the fetch session of the connection (CONSUMER-3X D-57).
 */
final class FetchLaneTransport implements AutoCloseable {

    private final Logger log;
    private final Time time;
    private final Supplier<NetworkClientDelegate> clientSupplier;
    private final Consumer<Runnable> completions;
    private final Queue<UnsentRequest> inbox = new ConcurrentLinkedQueue<>();
    // Experiment for CONSUMER-3X D-25: work that the consumer network thread gives back to this thread while it
    // handles one of this thread's responses, so the records are decoded where they were received.
    static final ThreadLocal<FetchLaneTransport> HANDLING_RESPONSE_OF = new ThreadLocal<>();
    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final Thread thread;
    private volatile NetworkClientDelegate client;
    private volatile boolean running = true;
    private volatile Throwable failure;

    /** Why this thread stopped, or null. Its connections fetch nothing any more, so the owner must report it. */
    Throwable failure() {
        return failure;
    }

    /**
     * @param clientSupplier creates the network client of this thread; called on this thread
     * @param completions    takes a task that completes a request; must be safe to call from this thread, and must
     *                       make the consumer network thread run the task
     */
    FetchLaneTransport(LogContext logContext, Time time, int index, Supplier<NetworkClientDelegate> clientSupplier, Consumer<Runnable> completions) {
        this.log = logContext.logger(getClass());
        this.time = time;
        this.clientSupplier = clientSupplier;
        this.completions = completions;
        this.thread = KafkaThread.daemon("fetch-receiver-" + index, this::run);
        this.thread.start();
    }

    /** Called on the consumer network thread. */
    void send(UnsentRequest request) {
        UnsentRequest copy = new UnsentRequest(request.requestBuilder(), request.node());
        copy.whenComplete((response, error) -> completions.accept(() -> {
            HANDLING_RESPONSE_OF.set(this);
            try {
                if (error != null)
                    request.future().completeExceptionally(error);
                else
                    request.future().complete(response);
            } finally {
                HANDLING_RESPONSE_OF.remove();
            }
        }));
        inbox.add(copy);
        if (!running)
            failRemaining();   // the thread is gone: fail the request instead of leaving it unanswered
        NetworkClientDelegate c = client;
        if (c != null)
            c.wakeup();
    }

    /**
     * Called on this thread only. Sends the request over this thread's network client; the handler runs on this
     * thread, so the response is handled where it was received (CONSUMER-3X D-57).
     */
    void sendLocal(org.apache.kafka.common.requests.AbstractRequest.Builder<?> builder,
                   org.apache.kafka.common.Node node,
                   java.util.function.BiConsumer<org.apache.kafka.clients.ClientResponse, Throwable> handler) {
        UnsentRequest request = new UnsentRequest(builder, java.util.Optional.of(node));
        request.whenComplete((response, error) -> {
            HANDLING_RESPONSE_OF.set(this);
            try {
                handler.accept(response, error);
            } finally {
                HANDLING_RESPONSE_OF.remove();
            }
        });
        client.add(request);
    }

    /** Runs the task on this thread. If the thread has stopped, the task is dropped; the caller must not depend on it. */
    void execute(Runnable task) {
        tasks.add(task);
        NetworkClientDelegate c = client;
        if (c != null && Thread.currentThread() != thread)
            c.wakeup();
    }

    // Experiment switch for CONSUMER-3X D-57: false restores the old order, in which every queued task (a decode
    // takes milliseconds) ran before the next fetch request was written to the socket.
    private static final boolean SEND_BEFORE_TASKS = !"false".equals(System.getProperty("c3x.sendBeforeTasks"));

    private void run() {
        try {
            client = clientSupplier.get();
            while (running) {
                UnsentRequest request;
                while ((request = inbox.poll()) != null)
                    client.add(request);
                Runnable task;
                if (SEND_BEFORE_TASKS) {
                    // One task at a time, and what is waiting to be sent goes out first: the broker then works on
                    // the next fetch while this thread decodes the previous one.
                    if ((task = tasks.poll()) != null) {
                        client.poll(0, time.milliseconds());
                        runTask(task);
                        continue;
                    }
                } else {
                    while ((task = tasks.poll()) != null)
                        runTask(task);
                }
                client.poll(inbox.isEmpty() && tasks.isEmpty() ? 1000 : 0, time.milliseconds());
            }
        } catch (Throwable t) {
            failure = t;
            log.error("The fetch receiver thread failed", t);
        } finally {
            failRemaining();
            try {
                if (client != null)
                    client.close();
            } catch (Exception e) {
                log.debug("Failed to close the network client of the fetch receiver thread", e);
            }
        }
    }

    /** A task that throws must not end this thread: the connections of this thread would stop without a sign. */
    private void runTask(Runnable task) {
        try {
            task.run();
        } catch (Throwable t) {
            log.error("A task of the fetch receiver thread failed", t);
        }
    }

    private void failRemaining() {
        running = false;
        UnsentRequest request;
        while ((request = inbox.poll()) != null)
            request.future().completeExceptionally(new org.apache.kafka.common.errors.DisconnectException("The fetch receiver thread stopped"));
    }

    @Override
    public void close() {
        running = false;
        NetworkClientDelegate c = client;
        if (c != null)
            c.wakeup();
        try {
            thread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
