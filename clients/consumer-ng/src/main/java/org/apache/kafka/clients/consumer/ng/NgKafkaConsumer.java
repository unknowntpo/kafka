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
package org.apache.kafka.clients.consumer.ng;

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RebalanceListener;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.ConsumerDelegate;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerInvoker;
import org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.clients.consumer.internals.Deserializers;
import org.apache.kafka.clients.consumer.internals.MemberStateListener;
import org.apache.kafka.clients.consumer.internals.OffsetCommitCallbackInvoker;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.FetchMetricsManager;
import org.apache.kafka.clients.consumer.internals.OffsetAndTimestampInternal;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionsAssignedEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionsRemovedEvent;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Pattern;

/**
 * The next-generation consumer's application side (CONSUMER-NG-04 §3), first cut: {@code subscribe} / {@code assign},
 * {@code poll} with rebalance callbacks, commits, positions and seeks, {@code close} that leaves the group. The
 * remaining {@link Consumer} methods throw {@link UnsupportedOperationException} until M2b.
 *
 * <p>Single-threaded like every {@code Consumer}: one application thread at a time.
 */
public final class NgKafkaConsumer<K, V> implements ConsumerDelegate<K, V> {

    private final Logger log;
    private final Time time;
    private final ConsumerConfig config;
    private final Metrics metrics;
    private final SubscriptionState subscriptions;
    private final ConsumerEngine engine;
    private final RecordReader<K, V> reader;
    private final Deserializers<K, V> deserializers;
    private final ConsumerInterceptors<K, V> interceptors;
    private final OffsetCommitCallbackInvoker commitCallbackInvoker;
    private final ConsumerRebalanceListenerInvoker rebalanceListenerInvoker;
    private final int maxPollRecords;
    private final long retryBackoffMs;
    private final long defaultApiTimeoutMs;
    private final boolean autoCommitEnabled;
    private final Optional<String> groupId;
    private final KafkaConsumerMetrics kafkaConsumerMetrics;
    private final FetchMetricsManager fetchMetricsManager;
    private final IsolationLevel isolationLevel;
    private final String clientId;
    private final AtomicReference<ConsumerGroupMetadata> groupMetadata = new AtomicReference<>();
    private final AtomicBoolean wakeupRequested = new AtomicBoolean();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final int requestTimeoutMs;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Thread applicationThread;
    private volatile boolean signalled;
    /** The most recent commitAsync; commitSync and close wait for it so its callback runs before they return. */
    private CompletableFuture<?> lastPendingAsyncCommit;

    public NgKafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this(config, keyDeserializer, valueDeserializer, Time.SYSTEM);
    }

    @SuppressWarnings("removal") // ConsumerGroupMetadata's constructors are deprecated for removal but have no replacement yet
    public NgKafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Time time) {
        this.config = config;
        this.time = time;
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config, GroupRebalanceConfig.ProtocolType.CONSUMER);
        LogContext logContext = ConsumerUtils.createLogContext(config, groupRebalanceConfig);
        this.log = logContext.logger(NgKafkaConsumer.class);
        this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
        this.metrics = ConsumerUtils.createMetrics(config, time);
        this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics);
        this.clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
        this.subscriptions = ConsumerUtils.createSubscriptionState(config, logContext);
        this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer, metrics);
        List<ConsumerInterceptor<K, V>> interceptorList = ConsumerUtils.configuredConsumerInterceptors(config);
        this.interceptors = new ConsumerInterceptors<>(interceptorList, metrics);
        this.commitCallbackInvoker = new OffsetCommitCallbackInvoker(interceptors);
        this.rebalanceListenerInvoker = new ConsumerRebalanceListenerInvoker(logContext, subscriptions, time,
                new RebalanceCallbackMetricsManager(metrics));
        this.maxPollRecords = config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.autoCommitEnabled = config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        groupId.ifPresent(id -> groupMetadata.set(new ConsumerGroupMetadata(id)));
        MemberStateListener memberStateListener = (memberEpoch, memberId) ->
                groupId.ifPresent(id -> groupMetadata.set(new ConsumerGroupMetadata(id, memberEpoch.orElse(-1), memberId, groupRebalanceConfig.groupInstanceId)));
        long creditBytes = 4L * config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG);
        this.fetchMetricsManager = ConsumerUtils.createFetchMetricsManager(metrics);
        ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(metrics.reporters(),
                interceptorList, List.of(deserializers.keyDeserializer(), deserializers.valueDeserializer()));
        this.engine = new ConsumerEngine(config, logContext, time, metrics, subscriptions, clusterResourceListeners, commitCallbackInvoker,
                memberStateListener, fetchMetricsManager, creditBytes, this::signalApplication);
        this.isolationLevel = IsolationLevel.valueOf(config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT));
        this.reader = new RecordReader<>(engine, subscriptions, this.deserializers.keyDeserializer(), this.deserializers.valueDeserializer(),
                config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG), isolationLevel, fetchMetricsManager);
        engine.start();
        log.debug("Consumer engine started");
    }

    // ---- wake-up plumbing between the engine and the application thread --------------------------------------------

    private void signalApplication() {
        signalled = true;
        Thread t = applicationThread;
        if (t != null)
            LockSupport.unpark(t);
    }

    private void parkUntil(long deadlineNanos) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining > 0 && !signalled)
            LockSupport.parkNanos(this, remaining);
        signalled = false;
    }

    // ---- subscription --------------------------------------------------------------------------------------------

    @Override
    public Set<TopicPartition> assignment() {
        ensureOpen();
        return Collections.unmodifiableSet(new HashSet<>(subscriptions.assignedPartitions()));
    }

    @Override
    public Set<String> subscription() {
        ensureOpen();
        return Collections.unmodifiableSet(new HashSet<>(subscriptions.subscription()));
    }

    @Override
    public void subscribe(Collection<String> topics) {
        subscribe(topics, Optional.empty());
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        if (callback == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        subscribe(topics, Optional.of(callback));
    }

    private void subscribe(Collection<String> topics, Optional<ConsumerRebalanceListener> listener) {
        ensureOpen();
        if (groupId.isEmpty())
            throw new org.apache.kafka.common.errors.InvalidGroupIdException("To use the group management or offset commit APIs, you must provide a valid group.id in the consumer configuration.");
        if (topics == null)
            throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
        if (topics.isEmpty()) {
            unsubscribe();
            return;
        }
        for (String topic : topics)
            if (topic == null || topic.trim().isEmpty())
                throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
        listener.ifPresent(l -> subscriptions.setRebalanceListener(l, this));
        Set<String> set = new HashSet<>(topics);
        await(engine.submit(e -> {
            e.subscriptionUpdater.subscribe(set);
            return null;
        }), defaultApiTimeoutMs, "subscribe");
        log.info("Subscribed to topic(s): {}", set);
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        ensureOpen();
        if (partitions == null)
            throw new IllegalArgumentException("Topic partitions collection to assign to cannot be null");
        for (TopicPartition tp : partitions)
            if (tp.topic() == null || tp.topic().trim().isEmpty())
                throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
        Set<TopicPartition> set = new HashSet<>(partitions);
        await(engine.submit(e -> {
            e.assign(set);
            return null;
        }), defaultApiTimeoutMs, "assign");
        log.info("Assigned to partition(s): {}", set);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        if (callback == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        subscribe(pattern, Optional.of(callback));
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribe(pattern, Optional.empty());
    }

    private void subscribe(Pattern pattern, Optional<ConsumerRebalanceListener> listener) {
        ensureOpen();
        throwIfNoGroup();
        if (pattern == null || pattern.toString().isEmpty())
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ? "null" : "empty"));
        listener.ifPresent(l -> subscriptions.setRebalanceListener(l, this));
        log.info("Subscribed to pattern: '{}'", pattern);
        await(engine.submit(e -> {
            e.subscriptionUpdater.subscribe(pattern);
            return null;
        }), defaultApiTimeoutMs, "subscribe");
    }

    @Override
    public void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener callback) {
        if (callback == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        subscribe(pattern, Optional.of(callback));
    }

    @Override
    public void subscribe(SubscriptionPattern pattern) {
        subscribe(pattern, Optional.empty());
    }

    private void subscribe(SubscriptionPattern pattern, Optional<ConsumerRebalanceListener> listener) {
        ensureOpen();
        throwIfNoGroup();
        if (pattern == null)
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be null");
        if (pattern.pattern().isEmpty())
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be empty");
        listener.ifPresent(l -> subscriptions.setRebalanceListener(l, this));
        log.info("Subscribing to regular expression {}", pattern);
        await(engine.submit(e -> {
            e.subscriptionUpdater.subscribe(pattern);
            return null;
        }), defaultApiTimeoutMs, "subscribe");
    }

    @Override
    public void unsubscribe() {
        ensureOpen();
        // Leaving the group runs the onPartitionsRevoked callback on this thread, so keep processing events while waiting.
        awaitProcessingEvents(engine.submit(e -> {
            e.subscriptions.unsubscribe();
            CompletableFuture<Void> left = e.membershipManager.isPresent() ? e.membershipManager.get().leaveGroup()
                    : CompletableFuture.completedFuture(null);
            return left;
        }), time.timer(defaultApiTimeoutMs), true, "unsubscribe");
        reader.close();
    }

    @Override
    public void setRebalanceListener(RebalanceListener callback) {
        ensureOpen();
        subscriptions.setRebalanceListener(callback, this);
    }

    // ---- poll -------------------------------------------------------------------------------------------------------

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        ensureOpen();
        applicationThread = Thread.currentThread();
        Timer timer = time.timer(timeout);
        if (subscriptions.hasNoSubscriptionOrUserAssignment())
            throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
        engine.onApplicationPoll(timer.currentTimeMs());
        try {
            do {
                maybeThrowWakeup();
                commitCallbackInvoker.executeCallbacks();
                processBackgroundEvents();
                Map<TopicPartition, OffsetAndMetadata> nextOffsets = new HashMap<>();
                Map<TopicPartition, List<ConsumerRecord<K, V>>> records = reader.drain(maxPollRecords, nextOffsets);
                if (!records.isEmpty())
                    return interceptors.onConsume(new ConsumerRecords<>(records, nextOffsets));
                long waitMs = waitBound(timer.remainingMs());
                parkUntil(System.nanoTime() + waitMs * 1_000_000L);
                timer.update();
                if (timer.notExpired())
                    engine.onApplicationPollIteration(timer.currentTimeMs());
            } while (timer.notExpired());
            return ConsumerRecords.empty();
        } finally {
            engine.onApplicationPollReturn();
        }
    }

    /**
     * How long one wait iteration may last: bounded by {@code retry.backoff.ms} while the engine needs poll inputs
     * (no assignment yet, a reconciliation pending, positions unknown), otherwise until data or an event wakes us.
     */
    private long waitBound(long remainingMs) {
        if (remainingMs <= retryBackoffMs)
            return Math.max(0, remainingMs);
        if (subscriptions.numAssignedPartitions() == 0 || engine.reconciling() || !engine.latestDecision().allPositionsKnown)
            return retryBackoffMs;
        return remainingMs;
    }

    private void maybeThrowWakeup() {
        if (wakeupRequested.compareAndSet(true, false) && !closing.get())
            throw new WakeupException(); // a wakeup that arrives once close() started is dropped, as before
    }

    private void processBackgroundEvents() {
        List<BackgroundEvent> events = engine.backgroundEventHandler.drainEvents();
        KafkaException first = null;
        for (BackgroundEvent event : events) {
            try {
                processBackgroundEvent(event);
            } catch (KafkaException e) {
                if (first == null)
                    first = e;
            }
        }
        if (first != null)
            throw first;
    }

    private void processBackgroundEvent(BackgroundEvent event) {
        switch (event.type()) {
            case ERROR:
                throw ((ErrorEvent) event).error();
            case PARTITIONS_ASSIGNED: {
                PartitionsAssignedEvent e = (PartitionsAssignedEvent) event;
                // The assignment changes on the I/O thread; block until it has, so the change happens inside poll().
                try {
                    await(engine.submit(engine -> {
                        engine.membershipManager.ifPresent(mm -> mm.applyAssignment(e.assignedPartitions(), e.addedPartitions()));
                        return null;
                    }), defaultApiTimeoutMs, "apply assignment");
                } catch (KafkaException error) {
                    engine.execute(() -> engine.membershipManager.ifPresent(mm -> mm.consumerRebalanceListenerCallbackCompleted(
                            new ConsumerRebalanceListenerCallbackCompletedEvent(ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED, e.future(), Optional.of(error)))));
                    throw error;
                }
                if (!subscriptions.hasRebalanceListener())
                    e.future().complete(null);
                else
                    invokeCallbackAndNotify(ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED, e.addedPartitions(), e.future());
                break;
            }
            case PARTITIONS_REMOVED: {
                PartitionsRemovedEvent e = (PartitionsRemovedEvent) event;
                for (TopicPartition tp : e.partitions())
                    reader.reset(tp);
                // Commit callbacks for offsets of these partitions run before their revocation is announced (the classic
                // consumer waits for in-flight async commits in onJoinPrepare; the previous implementation got it from timing).
                awaitPendingAsyncCommit(time.timer(requestTimeoutMs));
                commitCallbackInvoker.executeCallbacks();
                invokeCallbackAndNotify(e.methodName(), e.partitions(), e.future());
                break;
            }
            default:
                log.warn("Ignoring background event {}", event);
        }
    }

    private void invokeCallbackAndNotify(ConsumerRebalanceListenerMethodName method, SortedSet<TopicPartition> partitions,
                                         CompletableFuture<Void> future) {
        Exception error;
        try {
            switch (method) {
                case ON_PARTITIONS_REVOKED:
                    error = rebalanceListenerInvoker.invokePartitionsRevoked(partitions);
                    break;
                case ON_PARTITIONS_ASSIGNED:
                    error = rebalanceListenerInvoker.invokePartitionsAssigned(partitions);
                    break;
                case ON_PARTITIONS_LOST:
                    error = rebalanceListenerInvoker.invokePartitionsLost(partitions);
                    break;
                default:
                    throw new IllegalArgumentException("unknown callback " + method);
            }
        } catch (WakeupException | InterruptException e) {
            error = e;
        }
        Optional<KafkaException> wrapped = error == null ? Optional.empty()
                : Optional.of(ConsumerUtils.maybeWrapAsKafkaException(error, "User rebalance callback throws an error"));
        ConsumerRebalanceListenerCallbackCompletedEvent completed = new ConsumerRebalanceListenerCallbackCompletedEvent(method, future, wrapped);
        engine.execute(() -> engine.membershipManager.ifPresent(mm -> mm.consumerRebalanceListenerCallbackCompleted(completed)));
        if (wrapped.isPresent())
            throw wrapped.get();
    }

    // ---- commits ------------------------------------------------------------------------------------------------

    @Override
    public void commitSync() {
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public void commitSync(Duration timeout) {
        commitSync(subscriptions.allConsumed(), timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        ensureOpen();
        commitSyncInternal(offsets, timeout);
    }

    private void commitSyncInternal(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        throwIfNoGroup();
        Map<TopicPartition, OffsetAndMetadata> copy = Map.copyOf(offsets);
        Timer timer = time.timer(timeout);
        long deadline = timer.currentTimeMs() + timeout.toMillis();
        try {
            // As before, commitSync does not run rebalance callbacks while it waits (unsubscribe and close do): a revoke
            // delivered inside commitSync would be announced before the commit it is waiting for.
            if (!copy.isEmpty())
                awaitProcessingEvents(engine.submit(e -> e.commitManager.get().commitSync(copy, deadline)), timer, true, false, "commitSync");
            timer.update();
            interceptors.onCommit(copy);
        } finally {
            awaitPendingAsyncCommit(timer);
            commitCallbackInvoker.executeCallbacks();
        }
    }

    /** Waits (bounded by the timer) for the last commitAsync to complete so that its callback is ready to run. */
    private void awaitPendingAsyncCommit(Timer timer) {
        CompletableFuture<?> pending = lastPendingAsyncCommit;
        if (pending == null)
            return;
        try {
            pending.handle((v, t) -> null).get(Math.max(1, timer.remainingMs()), TimeUnit.MILLISECONDS);
            lastPendingAsyncCommit = null;
        } catch (java.util.concurrent.TimeoutException e) {
            log.debug("Pending asynchronous commit did not complete within the timeout");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptException(e);
        } catch (ExecutionException e) {
            lastPendingAsyncCommit = null; // the error reaches the user through the commit callback
        } finally {
            timer.update();
        }
    }

    @Override
    public void commitAsync() {
        commitAsync(null);
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        commitAsync(subscriptions.allConsumed(), callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        ensureOpen();
        throwIfNoGroup();
        Map<TopicPartition, OffsetAndMetadata> copy = Map.copyOf(offsets);
        lastPendingAsyncCommit = engine.submit(e -> e.commitManager.get().commitAsync(copy).whenComplete((committed, error) -> {
            if (error == null)
                commitCallbackInvoker.enqueueInterceptorInvocation(committed);
            if (callback != null)
                commitCallbackInvoker.enqueueUserCallbackInvocation(callback, copy, error == null ? null : (Exception) unwrap(error));
            signalApplication();
        }));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
        ensureOpen();
        throwIfNoGroup();
        if (partitions.isEmpty())
            return Map.of();
        Set<TopicPartition> copy = Set.copyOf(partitions);
        long deadline = time.milliseconds() + timeout.toMillis();
        return await(engine.submit(e -> e.commitManager.get().fetchOffsets(copy, deadline)), timeout.toMillis(), "committed").offsets();
    }

    // ---- positions ------------------------------------------------------------------------------------------------

    @Override
    public void seek(TopicPartition partition, long offset) {
        seek(partition, new OffsetAndMetadata(offset));
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        ensureOpen();
        if (offsetAndMetadata.offset() < 0)
            throw new IllegalArgumentException("seek offset must not be a negative number");
        log.info("Seeking to offset {} for partition {}", offsetAndMetadata.offset(), partition);
        await(engine.submit(e -> {
            if (!e.subscriptions.isAssigned(partition))
                throw new IllegalStateException("No current assignment for partition " + partition);
            offsetAndMetadata.leaderEpoch().ifPresent(epoch -> e.metadata.updateLastSeenEpochIfNewer(partition, epoch));
            SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(offsetAndMetadata.offset(),
                    offsetAndMetadata.leaderEpoch(), e.metadata.currentLeader(partition));
            e.subscriptions.seekUnvalidated(partition, position);
            e.seekReset(partition);
            return null;
        }), defaultApiTimeoutMs, "seek");
        reader.reset(partition);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        resetOffsets(partitions, AutoOffsetResetStrategy.EARLIEST);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        resetOffsets(partitions, AutoOffsetResetStrategy.LATEST);
    }

    private void resetOffsets(Collection<TopicPartition> partitions, AutoOffsetResetStrategy strategy) {
        ensureOpen();
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");
        Collection<TopicPartition> copy = List.copyOf(partitions);
        await(engine.submit(e -> {
            Collection<TopicPartition> parts = copy.isEmpty() ? e.subscriptions.assignedPartitions() : copy;
            e.subscriptions.requestOffsetReset(parts, strategy);
            for (TopicPartition tp : parts)
                e.seekReset(tp);
            return null;
        }), defaultApiTimeoutMs, "seek to " + strategy);
        for (TopicPartition tp : copy)
            reader.reset(tp);
    }

    @Override
    public long position(TopicPartition partition) {
        return position(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        ensureOpen();
        if (!subscriptions.isAssigned(partition))
            throw new IllegalStateException("You can only check the position for partitions assigned to this consumer.");
        applicationThread = Thread.currentThread();
        Timer timer = time.timer(timeout);
        engine.applicationWantsPositions(true);
        try {
            do {
                maybeThrowWakeup();
                processBackgroundEvents();
                SubscriptionState.FetchPosition position = subscriptions.hasValidPosition(partition) ? subscriptions.position(partition) : null;
                if (position != null)
                    return position.offset;
                engine.requestPositions();
                parkUntil(System.nanoTime() + Math.min(retryBackoffMs, timer.remainingMs()) * 1_000_000L);
                timer.update();
            } while (timer.notExpired());
        } finally {
            engine.applicationWantsPositions(false);
        }
        throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the position for partition " + partition + " could be determined");
    }

    @Override
    public Set<TopicPartition> paused() {
        ensureOpen();
        return Collections.unmodifiableSet(subscriptions.pausedPartitions());
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        ensureOpen();
        for (TopicPartition tp : partitions)
            subscriptions.pause(tp);
        engine.wakeup();
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        ensureOpen();
        for (TopicPartition tp : partitions)
            subscriptions.resume(tp);
        engine.wakeup();
    }

    // ---- unsupported in this cut ------------------------------------------------------------------------------------

    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        throw unsupported("registerMetricForSubscription");
    }

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        throw unsupported("unregisterMetricFromSubscription");
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        throw unsupported("clientInstanceId");
    }

    // ---- ConsumerDelegate ---------------------------------------------------------------------------------------

    @Override
    public String clientId() {
        return clientId;
    }

    @Override
    public Metrics metricsRegistry() {
        return metrics;
    }

    @Override
    public KafkaConsumerMetrics kafkaConsumerMetrics() {
        return kafkaConsumerMetrics;
    }

    /**
     * Test hook of the facade: process pending assignment/errors and wait until every assigned partition has a
     * position, or the timer expires. Returns true if positions are all known.
     */
    @Override
    public boolean updateAssignmentMetadataIfNeeded(Timer timer) {
        ensureOpen();
        applicationThread = Thread.currentThread();
        engine.onApplicationPoll(timer.currentTimeMs());
        try {
            do {
                maybeThrowWakeup();
                commitCallbackInvoker.executeCallbacks();
                processBackgroundEvents();
                if (subscriptions.hasAllFetchPositions() && !engine.reconciling())
                    return true;
                parkUntil(System.nanoTime() + Math.min(retryBackoffMs, Math.max(1, timer.remainingMs())) * 1_000_000L);
                timer.update();
                if (timer.notExpired())
                    engine.onApplicationPollIteration(timer.currentTimeMs());
            } while (timer.notExpired());
            return subscriptions.hasAllFetchPositions();
        } finally {
            engine.onApplicationPollReturn();
        }
    }

    // ---- misc -----------------------------------------------------------------------------------------------------

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(metrics.metrics());
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        ensureOpen();
        throwIfNoGroup();
        return groupMetadata.get();
    }

    @Override
    public void wakeup() {
        wakeupRequested.set(true);
        signalApplication();
    }

    @Override
    public void close() {
        close(CloseOptions.timeout(Duration.ofMillis(30_000)));
    }

    @Deprecated
    @Override
    public void close(Duration timeout) {
        close(CloseOptions.timeout(timeout));
    }

    @Override
    public void close(CloseOptions option) {
        // Closed is set at the end: the rebalance callbacks run during close may still call the consumer.
        if (!closing.compareAndSet(false, true))
            return;
        wakeupRequested.set(false); // we are closing with a timeout: no wake-ups from here on
        Duration timeout = option.timeout().orElse(Duration.ofMillis(30_000));
        // As before: requests made while closing get at most request.timeout.ms, so a huge close timeout still returns.
        Timer timer = time.timer(Math.min(timeout.toMillis(), requestTimeoutMs));
        try {
            // The previous implementation's order: auto-commit, stop committing / finding the coordinator, leave the
            // group, run the pending asynchronous commit callbacks, shut the network down.
            if (autoCommitEnabled && groupId.isPresent() && !subscriptions.allConsumed().isEmpty()) {
                try {
                    commitSyncInternal(subscriptions.allConsumed(), Duration.ofMillis(timer.remainingMs()));
                } catch (Exception e) {
                    log.warn("Auto-commit on close failed", e);
                }
            }
            timer.update();
            engine.execute(() -> {
                engine.commitManager.ifPresent(m -> m.signalClose());
                engine.coordinatorManager.ifPresent(m -> m.signalClose());
            });
            runRebalanceCallbacksOnClose();
            try {
                awaitProcessingEvents(engine.submit(e -> e.leaveGroupOnClose(option.groupMembershipOperation())), timer, false, "leave group");
            } catch (Exception e) {
                log.warn("Leaving the group on close failed", e);
            }
            timer.update();
            awaitPendingAsyncCommit(timer);
            commitCallbackInvoker.executeCallbacks();
        } finally {
            reader.close();
            timer.update();
            engine.close(timer.remainingMs());
            fetchMetricsManager.close();
            kafkaConsumerMetrics.close();
            metrics.close();
            deserializers.close();
            interceptors.close();
            closed.set(true);
            log.debug("Consumer closed");
        }
    }

    /**
     * As before: the application thread revokes (or, without a valid member epoch, loses) the group-assigned partitions
     * before the member leaves; {@code leaveGroupOnClose} itself runs no callbacks.
     */
    private void runRebalanceCallbacksOnClose() {
        ConsumerGroupMetadata metadata = groupMetadata.get();
        if (metadata == null || !subscriptions.hasAutoAssignedPartitions())
            return;
        SortedSet<TopicPartition> droppedPartitions = new TreeSet<>(Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
        droppedPartitions.addAll(subscriptions.assignedPartitions());
        if (droppedPartitions.isEmpty())
            return;
        Exception error = metadata.generationId() > 0
                ? rebalanceListenerInvoker.invokePartitionsRevoked(droppedPartitions)
                : rebalanceListenerInvoker.invokePartitionsLost(droppedPartitions);
        if (error != null)
            log.warn("Rebalance callback failed while closing the consumer", error);
    }

    // ---- helpers ------------------------------------------------------------------------------------------------

    /**
     * Waits for {@code future} while processing background events (rebalance callbacks, errors) on this thread, as
     * the previous implementation did for commitSync, unsubscribe and close: the operation may need a callback to run here.
     */
    private <T> T awaitProcessingEvents(CompletableFuture<T> future, Timer timer, boolean allowWakeup, String what) {
        return awaitProcessingEvents(future, timer, allowWakeup, true, what);
    }

    private <T> T awaitProcessingEvents(CompletableFuture<T> future, Timer timer, boolean allowWakeup, boolean processEvents, String what) {
        applicationThread = Thread.currentThread();
        future.whenComplete((v, t) -> signalApplication());
        while (true) {
            if (allowWakeup)
                maybeThrowWakeup();
            if (processEvents)
                processBackgroundEvents();
            if (future.isDone())
                return await(future, 0, what);
            timer.update();
            if (timer.isExpired())
                throw new TimeoutException("Timeout of " + timer.timeoutMs() + "ms expired before " + what + " completed");
            parkUntil(System.nanoTime() + Math.min(retryBackoffMs, timer.remainingMs()) * 1_000_000L);
        }
    }

    private <T> T await(CompletableFuture<T> future, long timeoutMs, String what) {
        try {
            return future.get(Math.max(0, timeoutMs), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new TimeoutException("Timeout of " + timeoutMs + "ms expired before " + what + " completed");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IllegalStateException || cause instanceof IllegalArgumentException)
                throw (RuntimeException) cause; // the caller's own mistake, detected on the I/O thread (e.g. seek of an unassigned partition)
            throw ConsumerUtils.maybeWrapAsKafkaException(cause);
        }
    }

    private static Throwable unwrap(Throwable t) {
        return t instanceof java.util.concurrent.CompletionException && t.getCause() != null ? t.getCause() : t;
    }

    private void ensureOpen() {
        if (closed.get())
            throw new IllegalStateException("This consumer has already been closed.");
    }

    private void throwIfNoGroup() {
        if (groupId.isEmpty())
            throw new org.apache.kafka.common.errors.InvalidGroupIdException("To use the group management or offset commit APIs, you must provide a valid group.id in the consumer configuration.");
    }

    // ---- topic metadata, offsets by time, lag (M2b) --------------------------------------------------------------

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return partitionsFor(topic, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        ensureOpen();
        List<PartitionInfo> parts = engine.metadata.fetch().partitionsForTopic(topic);
        if (!parts.isEmpty())
            return parts;
        if (timeout.toMillis() == 0L)
            throw new TimeoutException();
        long deadline = time.milliseconds() + timeout.toMillis();
        Map<String, List<PartitionInfo>> topicMetadata = await(engine.submit(e -> e.topicMetadataManager.requestTopicMetadata(topic, deadline)),
                timeout.toMillis(), "partitionsFor");
        return topicMetadata.getOrDefault(topic, Collections.emptyList());
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return listTopics(Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        ensureOpen();
        if (timeout.toMillis() == 0L)
            throw new TimeoutException();
        long deadline = time.milliseconds() + timeout.toMillis();
        return await(engine.submit(e -> e.topicMetadataManager.requestAllTopicsMetadata(deadline)), timeout.toMillis(), "listTopics");
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        ensureOpen();
        Objects.requireNonNull(timestampsToSearch, "Timestamps to search cannot be null");
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            // Exclude the earliest and latest offset here so the timestamp in the returned OffsetAndTimestamp is always positive.
            if (entry.getValue() < 0)
                throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                        entry.getValue() + ". The target time cannot be negative.");
        }
        if (timestampsToSearch.isEmpty())
            return new HashMap<>();
        if (timeout.toMillis() == 0L) {
            Map<TopicPartition, OffsetAndTimestamp> empty = new HashMap<>();
            timestampsToSearch.keySet().forEach(tp -> empty.put(tp, null));
            return empty;
        }
        Map<TopicPartition, OffsetAndTimestampInternal> offsets = listOffsets(Map.copyOf(timestampsToSearch), true, timeout);
        Map<TopicPartition, OffsetAndTimestamp> results = new HashMap<>(offsets.size());
        offsets.forEach((k, v) -> results.put(k, v != null ? v.buildOffsetAndTimestamp() : null));
        return results;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.EARLIEST_TIMESTAMP, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.LATEST_TIMESTAMP, timeout);
    }

    private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition> partitions, long timestamp, Duration timeout) {
        ensureOpen();
        Objects.requireNonNull(partitions, "Partitions cannot be null");
        // With a zero timeout the classic consumer returns an empty map; keep that.
        if (partitions.isEmpty() || timeout.isZero())
            return new HashMap<>();
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (TopicPartition tp : partitions)
            timestampsToSearch.put(tp, timestamp);
        Map<TopicPartition, Long> results = new HashMap<>();
        listOffsets(timestampsToSearch, false, timeout).forEach((tp, offset) -> results.put(tp, offset.offset()));
        return results;
    }

    private Map<TopicPartition, OffsetAndTimestampInternal> listOffsets(Map<TopicPartition, Long> timestampsToSearch,
                                                                         boolean requireTimestamps, Duration timeout) {
        try {
            return await(engine.submit(e -> e.offsetsManager.fetchOffsets(timestampsToSearch, requireTimestamps)),
                    timeout.toMillis(), "list offsets");
        } catch (TimeoutException e) {
            throw new TimeoutException("Failed to get offsets by times in " + timeout.toMillis() + "ms");
        }
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        ensureOpen();
        return await(engine.submit(e -> CompletableFuture.completedFuture(e.offsetsManager.currentLag(topicPartition, isolationLevel))),
                defaultApiTimeoutMs, "currentLag");
    }

    @Override
    public void enforceRebalance() {
        enforceRebalance(null);
    }

    @Override
    public void enforceRebalance(String reason) {
        // Same as the previous implementation: the new group protocol has no client-triggered rebalance.
        log.warn("Operation not supported in new consumer group protocol");
    }

    private static UnsupportedOperationException unsupported(String method) {
        return new UnsupportedOperationException(method + " is not implemented in this cut of the next-generation consumer (CONSUMER-NG-04 §4)");
    }
}
