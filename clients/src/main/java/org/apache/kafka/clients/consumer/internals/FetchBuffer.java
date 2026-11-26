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
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * {@code FetchBuffer} buffers up {@link CompletedFetch the results} from the broker responses as they are received.
 * It is essentially a wrapper around a {@link java.util.Queue} of {@link CompletedFetch}. There is at most one
 * {@link CompletedFetch} per partition in the queue.
 *
 * <p/>
 *
 * <em>Note</em>: this class is thread-safe with the intention that {@link CompletedFetch the data} will be
 * "produced" by a background thread and consumed by the application thread.
 */
public class FetchBuffer implements AutoCloseable {

    private final Logger log;
    private final ConcurrentLinkedQueue<CompletedFetch> completedFetches;
    private final Lock lock;
    private final Condition blockingCondition;
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();

    private final AtomicBoolean wokenup = new AtomicBoolean(false);

    private CompletedFetch nextInLineFetch;

    // Partitions whose buffered data was found stale by the application thread. No prefetch is sent for them
    // until a fetch has been sent from the consumed position again.
    private final Set<TopicPartition> prefetchInvalidated = ConcurrentHashMap.newKeySet();

    /** A summary of the buffered data of one partition. It decides whether and where a prefetch is sent. */
    static final class BufferedState {
        long prefetchOffset;
        long bytes;
        boolean eligible = true;
    }

    /**
     * Returns a summary of the data that is buffered and not yet consumed, for each partition that has such data.
     */
    Map<TopicPartition, BufferedState> bufferedStates() {
        try {
            lock.lock();
            final Map<TopicPartition, BufferedState> states = new HashMap<>();
            final CompletedFetch next = nextInLineFetch;
            if (next != null && !next.isConsumed())
                addBufferedState(states, next);
            // For a moment the collector has a fetch both as the next in line and at the head of the queue.
            // It must be counted once.
            completedFetches.forEach(cf -> {
                if (cf != next)
                    addBufferedState(states, cf);
            });
            return states;
        } finally {
            lock.unlock();
        }
    }

    private void addBufferedState(Map<TopicPartition, BufferedState> states, CompletedFetch cf) {
        BufferedState state = states.computeIfAbsent(cf.partition, tp -> new BufferedState());
        state.prefetchOffset = Math.max(state.prefetchOffset, cf.prefetchOffset);
        state.bytes += cf.sizeInBytes;
        if (!cf.prefetchEligible || prefetchInvalidated.contains(cf.partition))
            state.eligible = false;
    }

    /**
     * Stops prefetches for the partition, because its buffered data does not continue from the consumed position.
     */
    void invalidatePrefetch(TopicPartition partition) {
        prefetchInvalidated.add(partition);
    }

    /**
     * Allows prefetches for the partition again. It is called when a fetch is sent from the consumed position.
     */
    void clearPrefetchInvalidated(TopicPartition partition) {
        prefetchInvalidated.remove(partition);
    }

    // Marks that the application thread is not waiting for data
    private static final long NOT_WAITING = Long.MIN_VALUE;

    private final Time time;
    // The time the application thread spent without data, up to the last time it took data out of this buffer.
    // Guarded by the lock.
    private long waitedNanos;
    // When the application thread last found nothing to consume, or NOT_WAITING while it has data. Guarded by
    // the lock, so that a reader sees a wait either as running or as added to waitedNanos, never as both.
    private long waitingSinceNanos = NOT_WAITING;

    public FetchBuffer(final LogContext logContext) {
        this(logContext, Time.SYSTEM);
    }

    public FetchBuffer(final LogContext logContext, final Time time) {
        this.log = logContext.logger(FetchBuffer.class);
        this.time = time;
        this.completedFetches = new ConcurrentLinkedQueue<>();
        this.lock = new ReentrantLock();
        this.blockingCondition = lock.newCondition();
    }

    /**
     * Returns {@code true} if there are no completed fetches pending to return to the user.
     *
     * @return {@code true} if the buffer is empty, {@code false} otherwise
     */
    boolean isEmpty() {
        try {
            lock.lock();
            return completedFetches.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe. Has
     * visibility for testing.
     *
     * @return {@code true} if there are completed fetches that match the {@link Predicate}, {@code false} otherwise
     */
    boolean hasCompletedFetches(Predicate<CompletedFetch> predicate) {
        try {
            lock.lock();
            return completedFetches.stream().anyMatch(predicate);
        } finally {
            lock.unlock();
        }
    }

    // Told about each fetch that add() buffers, after it is buffered. Null if nobody listens.
    private volatile Consumer<CompletedFetch> addListener;

    void add(CompletedFetch completedFetch) {
        addAll(List.of(completedFetch));
        Consumer<CompletedFetch> listener = addListener;
        if (listener != null)
            listener.accept(completedFetch);
    }

    /**
     * Sets the listener that is told about each newly received fetch that {@link #add(CompletedFetch)} buffers. It
     * is called on the thread that adds the fetch, without a lock. Fetches that {@link #addAll(Collection)} puts
     * back, such as those of paused partitions, are not new and the listener is not told about them.
     */
    void setAddListener(Consumer<CompletedFetch> listener) {
        this.addListener = listener;
    }

    void addAll(Collection<CompletedFetch> completedFetches) {
        if (completedFetches == null || completedFetches.isEmpty())
            return;

        try {
            lock.lock();
            this.completedFetches.addAll(completedFetches);
            wokenup.set(true);
            blockingCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    CompletedFetch nextInLineFetch() {
        try {
            lock.lock();
            return nextInLineFetch;
        } finally {
            lock.unlock();
        }
    }

    void setNextInLineFetch(CompletedFetch nextInLineFetch) {
        try {
            lock.lock();
            this.nextInLineFetch = nextInLineFetch;
        } finally {
            lock.unlock();
        }
    }

    CompletedFetch peek() {
        try {
            lock.lock();
            return completedFetches.peek();
        } finally {
            lock.unlock();
        }
    }

    CompletedFetch poll() {
        try {
            lock.lock();
            CompletedFetch fetch = completedFetches.poll();
            if (fetch != null && waitingSinceNanos != NOT_WAITING) {
                // the application thread has data again
                waitedNanos += time.nanoseconds() - waitingSinceNanos;
                waitingSinceNanos = NOT_WAITING;
            }
            return fetch;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The total time the application thread has been without data: from a poll that found nothing to consume
     * until the next time it took data out of this buffer. A poll with a zero timeout counts as well, because it
     * also found nothing. The fetch code reads it to decide how many connections to use to a broker.
     */
    long waitedNanos() {
        try {
            lock.lock();
            return waitedNanos + (waitingSinceNanos == NOT_WAITING ? 0 : time.nanoseconds() - waitingSinceNanos);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Allows the caller to await a response from the broker for requested data. The method will block, returning only
     * under one of the following conditions:
     *
     * <ol>
     *     <li>The buffer was already woken</li>
     *     <li>The buffer was woken during the wait</li>
     *     <li>The remaining time on the {@link Timer timer} elapsed</li>
     *     <li>The thread was interrupted</li>
     * </ol>
     *
     * @param timer Timer that provides time to wait
     */
    void awaitWakeup(Timer timer) {
        try {
            lock.lock();
            // the caller found nothing to consume
            if (waitingSinceNanos == NOT_WAITING)
                waitingSinceNanos = time.nanoseconds();

            while (!wokenup.compareAndSet(true, false)) {
                // Update the timer before we head into the loop in case it took a while to get the lock.
                timer.update();

                if (timer.isExpired()) {
                    // If the thread was interrupted before we start waiting, it still counts as
                    // interrupted from the point of view of the KafkaConsumer.poll(Duration) contract.
                    // We only need to check this when we are not going to wait because waiting
                    // already checks whether the thread is interrupted.
                    if (Thread.interrupted())
                        throw new InterruptException("Interrupted waiting for results from fetching records");

                    break;
                }

                if (!blockingCondition.await(timer.remainingMs(), TimeUnit.MILLISECONDS)) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            throw new InterruptException("Interrupted waiting for results from fetching records", e);
        } finally {
            lock.unlock();
            timer.update();
        }
    }

    void wakeup() {
        try {
            lock.lock();
            wokenup.set(true);
            blockingCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Updates the buffer to retain only the fetch data that corresponds to the given partitions. Any previously
     * {@link CompletedFetch fetched data} is removed if its partition is not in the given set of partitions.
     *
     * @param partitions {@link Set} of {@link TopicPartition}s for which any buffered data should be kept
     */
    void retainAll(final Set<TopicPartition> partitions) {
        try {
            lock.lock();

            completedFetches.removeIf(cf -> maybeDrain(partitions, cf));

            if (maybeDrain(partitions, nextInLineFetch))
                nextInLineFetch = null;

            prefetchInvalidated.retainAll(partitions);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Drains (i.e. <em>removes</em>) the contents of the given {@link CompletedFetch} as its data should not
     * be returned to the user.
     */
    private boolean maybeDrain(final Set<TopicPartition> partitions, final CompletedFetch completedFetch) {
        if (completedFetch != null && !partitions.contains(completedFetch.partition)) {
            log.debug("Removing {} from buffered fetch data as it is not in the set of partitions to retain ({})", completedFetch.partition, partitions);
            completedFetch.drain();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Return the set of {@link TopicPartition partitions} for which we have data in the buffer.
     *
     * @return {@link TopicPartition Partition} set
     */
    Set<TopicPartition> bufferedPartitions() {
        try {
            lock.lock();

            final Set<TopicPartition> partitions = new HashSet<>();

            if (nextInLineFetch != null && !nextInLineFetch.isConsumed()) {
                partitions.add(nextInLineFetch.partition);
            }

            completedFetches.forEach(cf -> partitions.add(cf.partition));
            return partitions;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        try {
            lock.lock();

            idempotentCloser.close(
                    () -> retainAll(Collections.emptySet()),
                    () -> log.warn("The fetch buffer was already closed")
            );
        } finally {
            lock.unlock();
        }
    }
}
