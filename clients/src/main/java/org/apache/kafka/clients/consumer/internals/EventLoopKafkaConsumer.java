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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RebalanceListener;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.events.AllTopicsMetadataEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.ApplyAssignmentEvent;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.AsyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CheckAndUpdatePositionsEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.CurrentLagEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.EventProcessor;
import org.apache.kafka.clients.consumer.internals.events.FetchCommittedOffsetsEvent;
import org.apache.kafka.clients.consumer.internals.events.FetchPositionsErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.LeaveGroupOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionsAssignedEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionsRemovedEvent;
import org.apache.kafka.clients.consumer.internals.events.PausePartitionsEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetOffsetEvent;
import org.apache.kafka.clients.consumer.internals.events.ResumePartitionsEvent;
import org.apache.kafka.clients.consumer.internals.events.SeekUnvalidatedEvent;
import org.apache.kafka.clients.consumer.internals.events.StopFindCoordinatorOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.SyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicMetadataEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicPatternSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicRe2JPatternSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.UnsubscribeEvent;
import org.apache.kafka.clients.consumer.internals.events.UpdatePatternSubscriptionEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.AppInfoParser;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.internals.AbstractMembershipManager.TOPIC_PARTITION_COMPARATOR;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_JMX_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.configuredConsumerInterceptors;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createLogContext;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.swallow;

/**
 * The "event loop only" {@link Consumer} implementation for the KIP-848 group protocol
 * ({@code group.protocol=consumer}): a single background {@link ConsumerEventLoop} replaces the deadline-driven
 * {@code ConsumerNetworkThread}, while the fetch path ({@link FetchRequestManager}, {@link FetchBuffer},
 * {@link FetchCollector}) is the previous implementation's, unchanged. Steady-state {@link #poll(Duration)} creates
 * no events: the loop learns about polls through a volatile write, and the next fetch is requested through a flag
 * (waking the loop only if it is parked). This variant exists to measure the loop on its own.
 *
 * <p><em>Note:</em> this class should not be invoked directly; users should instead create a {@link KafkaConsumer}
 * as before.
 */
public class EventLoopKafkaConsumer<K, V> implements ConsumerDelegate<K, V> {

    private static final long NO_CURRENT_THREAD = -1L;

    /**
     * An {@link org.apache.kafka.clients.consumer.internals.events.EventProcessor} that is created and executes in the
     * application thread for the purpose of processing {@link BackgroundEvent background events} generated by the
     * {@link ConsumerNetworkThread network thread}.
     * Those events are generally of two types:
     *
     * <ul>
     *     <li>Errors that occur in the network thread that need to be propagated to the application thread</li>
     *     <li>{@link ConsumerRebalanceListener} callbacks that are to be executed on the application thread</li>
     * </ul>
     */
    private class BackgroundEventProcessor implements EventProcessor<BackgroundEvent> {

        @Override
        public void process(final BackgroundEvent event) {
            switch (event.type()) {
                case ERROR:
                    process((ErrorEvent) event);
                    break;

                case PARTITIONS_ASSIGNED:
                    process((PartitionsAssignedEvent) event);
                    break;

                case PARTITIONS_REMOVED:
                    process((PartitionsRemovedEvent) event);
                    break;

                default:
                    throw new IllegalArgumentException("Background event type " + event.type() + " was not expected");

            }
        }

        private void process(final ErrorEvent event) {
            if (event instanceof FetchPositionsErrorEvent && subscriptions.hasAllFetchPositions()) {
                // The loop's own positions attempt failed, but every partition has a position now (the
                // application seeked, or a later attempt succeeded): the error would refer to a state that is gone.
                log.debug("Dropping stale fetch positions error {}", event);
                return;
            }
            throw event.error();
        }

        /**
         * Processing this event will perform the actions needed in the app thread when new partitions are reconciled in the background:
         * - apply assignment changes (ensuring they happen in the background but triggered within the app thread poll)
         * - run onPartitionsAssigned callback if present
         * - notify background thread so it can carry on (e.g., send ack to the broker)
         */
        private void process(final PartitionsAssignedEvent event) {

            applyNewAssignment(event);

            if (!subscriptions.hasRebalanceListener()) {
                event.future().complete(null);
            } else {
                invokeRebalanceCallbackAndNotifyBackgroundThread(ON_PARTITIONS_ASSIGNED, event.addedPartitions(), event.future());
            }
        }

        /**
         * Send event to the background to update the assignment in the subscription state.
         * Block on it to complete to ensure the assignment change happens within a call to
         * consumer.poll.
         * Note that this event only happens when there is a pending assignment (reconciliation
         * completed in the background)
         */
        private void applyNewAssignment(final PartitionsAssignedEvent event) {
            ApplyAssignmentEvent applyEvent = new ApplyAssignmentEvent(
                event.assignedPartitions(),
                event.addedPartitions()
            );
            try {
                eventLoop.addAndGet(applyEvent);
            } catch (Exception e) {
                // Send error to the background thread, so it can complete the ongoing reconciliation (failed to update assignment to run callbacks)
                KafkaException error = ConsumerUtils.maybeWrapAsKafkaException(e, "Failed to apply the new assignment");
                eventLoop.add(new ConsumerRebalanceListenerCallbackCompletedEvent(ON_PARTITIONS_ASSIGNED, event.future(), Optional.of(error)));
                throw error;
            }
        }

        private void process(final PartitionsRemovedEvent event) {
            invokeRebalanceCallbackAndNotifyBackgroundThread(event.methodName(), event.partitions(), event.future());
        }

        private void invokeRebalanceCallbackAndNotifyBackgroundThread(
                ConsumerRebalanceListenerMethodName methodName,
                SortedSet<TopicPartition> partitions,
                CompletableFuture<Void> future) {
            ConsumerRebalanceListenerCallbackCompletedEvent invokedEvent = invokeRebalanceCallbacks(
                rebalanceListenerInvoker,
                methodName,
                partitions,
                future
            );
            eventLoop.add(invokedEvent);
            if (invokedEvent.error().isPresent()) {
                throw invokedEvent.error().get();
            }
        }

    }

    private final ConsumerEventLoop eventLoop;
    private final Time time;
    private final AtomicReference<Optional<ConsumerGroupMetadata>> groupMetadata = new AtomicReference<>(Optional.empty());
    private final FetchMetricsManager fetchMetricsManager;
    private final RebalanceCallbackMetricsManager rebalanceCallbackMetricsManager;
    private final AsyncConsumerMetrics asyncConsumerMetrics;
    private final KafkaConsumerMetrics kafkaConsumerMetrics;
    private Logger log;
    private final String clientId;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final BackgroundEventHandler backgroundEventHandler;
    private final BackgroundEventProcessor backgroundEventProcessor;
    private final CompletableEventReaper backgroundEventReaper;
    private final Deserializers<K, V> deserializers;

    /**
     * A thread-safe {@link FetchBuffer fetch buffer} for the results that are populated in the
     * {@link ConsumerEventLoop event loop} when the results are available. Shared between the two threads, as in
     * the previous implementation.
     */
    private final FetchBuffer fetchBuffer;
    private final FetchCollector<K, V> fetchCollector;
    private final ConsumerInterceptors<K, V> interceptors;
    private final IsolationLevel isolationLevel;

    private final SubscriptionState subscriptions;

    /**
     * This is a snapshot of the partitions assigned to this consumer. HOWEVER, this is only populated and used in
     * the case where this consumer is in a consumer group. Self-assigned partitions do not appear here.
     */
    private final AtomicReference<Set<TopicPartition>> groupAssignmentSnapshot = new AtomicReference<>(Collections.emptySet());
    private final ConsumerMetadata metadata;
    private final Metrics metrics;
    private final long retryBackoffMs;
    private final int requestTimeoutMs;
    private final Duration defaultApiTimeoutMs;
    private final boolean autoCommitEnabled;
    private volatile boolean closed = false;
    // Init value is needed to avoid NPE in case of exception raised in the constructor
    private Optional<ClientTelemetryReporter> clientTelemetryReporter = Optional.empty();

    private final WakeupTrigger wakeupTrigger = new WakeupTrigger();
    private final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker;
    private final ConsumerRebalanceListenerInvoker rebalanceListenerInvoker;
    // Last triggered async commit future. Used to wait until all previous async commits are completed.
    // We only need to keep track of the last one, since they are guaranteed to complete in order.
    private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> lastPendingAsyncCommit = null;

    // currentThread holds the threadId of the current thread accessing the AsyncKafkaConsumer
    // and is used to prevent multithreaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    private final AtomicInteger refCount = new AtomicInteger(0);

    private volatile boolean hasPendingReconciliation = false;

    private final MemberStateListener memberStateListener = new MemberStateListener() {
        @Override
        public void onMemberEpochUpdated(Optional<Integer> memberEpoch, String memberId) {
            updateGroupMetadata(memberEpoch, memberId);
        }

        @Override
        public void onGroupAssignmentUpdated(Set<TopicPartition> partitions) {
            setGroupAssignmentSnapshot(partitions);
        }

        @Override
        public void onMemberStateChange(MemberState memberState) {
            setHasPendingReconciliation(memberState == MemberState.RECONCILING);
        }
    };

    public EventLoopKafkaConsumer(final ConsumerConfig config,
                                  final Deserializer<K> keyDeserializer,
                                  final Deserializer<V> valueDeserializer) {
        this(null, Time.SYSTEM, config, keyDeserializer, valueDeserializer, Optional.empty(), null, null);
    }

    // Visible for testing: injected client, subscription state and metadata (the KafkaConsumer test constructor path)
    EventLoopKafkaConsumer(LogContext logContext,
                           Time time,
                           ConsumerConfig config,
                           Deserializer<K> keyDeserializer,
                           Deserializer<V> valueDeserializer,
                           KafkaClient client,
                           SubscriptionState subscriptions,
                           ConsumerMetadata metadata) {
        this(logContext, time, config, keyDeserializer, valueDeserializer, Optional.of(client), subscriptions, metadata);
    }

    @SuppressWarnings({"this-escape"})
    private EventLoopKafkaConsumer(final LogContext injectedLogContext,
                                   final Time time,
                                   final ConsumerConfig config,
                                   final Deserializer<K> keyDeserializer,
                                   final Deserializer<V> valueDeserializer,
                                   final Optional<KafkaClient> injectedClient,
                                   final SubscriptionState injectedSubscriptions,
                                   final ConsumerMetadata injectedMetadata) {
        try {
            final boolean injected = injectedClient.isPresent();
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
                config,
                GroupRebalanceConfig.ProtocolType.CONSUMER
            );
            this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
            this.autoCommitEnabled = config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
            LogContext logContext = injectedLogContext != null ? injectedLogContext : createLogContext(config, groupRebalanceConfig);
            this.backgroundEventQueue = new LinkedBlockingQueue<>();
            this.log = logContext.logger(getClass());

            log.debug("Initializing the Kafka consumer");
            this.defaultApiTimeoutMs = Duration.ofMillis(config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
            this.time = time;
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            this.clientTelemetryReporter = telemetryReporter(injected, clientId, config);
            this.clientTelemetryReporter.ifPresent(reporters::add);
            this.metrics = createMetrics(injected, config, time, reporters);
            this.asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, CONSUMER_METRIC_GROUP);
            this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics);
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
            this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);

            List<ConsumerInterceptor<K, V>> interceptorList = interceptorList(injected, config);
            this.interceptors = new ConsumerInterceptors<>(interceptorList, metrics);
            this.deserializers = deserializers(injected, config, keyDeserializer, valueDeserializer, metrics);
            this.subscriptions = injectedSubscriptions != null ? injectedSubscriptions : createSubscriptionState(config, logContext);
            this.metadata = metadata(injectedMetadata, config, logContext, subscriptions, metrics, interceptorList, deserializers);

            this.fetchMetricsManager = fetchMetricsManager(injected, metrics);
            FetchConfig fetchConfig = new FetchConfig(config);
            this.isolationLevel = fetchConfig.isolationLevel;

            ApiVersions apiVersions = new ApiVersions();
            this.backgroundEventHandler = new BackgroundEventHandler(
                backgroundEventQueue,
                time,
                asyncConsumerMetrics
            );

            // This FetchBuffer is shared between the application and event loop threads.
            this.fetchBuffer = new FetchBuffer(logContext);
            final PositionsValidator positionsValidator = new PositionsValidator(logContext, time, subscriptions, metadata);
            final Supplier<NetworkClientDelegate> networkClientDelegateSupplier = networkClientDelegateSupplier(
                    injectedClient, logContext, config, apiVersions);
            this.offsetCommitCallbackInvoker = new OffsetCommitCallbackInvoker(interceptors);
            this.groupMetadata.set(initializeGroupMetadata(config, groupRebalanceConfig));
            final Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplier(time,
                    logContext,
                    backgroundEventHandler,
                    metadata,
                    subscriptions,
                    fetchBuffer,
                    config,
                    groupRebalanceConfig,
                    apiVersions,
                    fetchMetricsManager,
                    networkClientDelegateSupplier,
                    clientTelemetryReporter,
                    metrics,
                    offsetCommitCallbackInvoker,
                    memberStateListener,
                    Optional.empty(),
                    positionsValidator
            );
            final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(logContext,
                    metadata,
                    subscriptions,
                    requestManagersSupplier
            );
            final ConsumerEventLoop loop = new ConsumerEventLoop(
                    logContext,
                    time,
                    defaultApiTimeoutMs.toMillis(),
                    subscriptions,
                    metadata,
                    applicationEventProcessorSupplier,
                    networkClientDelegateSupplier,
                    requestManagersSupplier,
                    backgroundEventHandler,
                    asyncConsumerMetrics,
                    fetchBuffer::wakeup
            );
            this.eventLoop = loop;
            this.rebalanceCallbackMetricsManager = new RebalanceCallbackMetricsManager(metrics);
            this.rebalanceListenerInvoker = new ConsumerRebalanceListenerInvoker(
                    logContext,
                    subscriptions,
                    time,
                    rebalanceCallbackMetricsManager
            );
            this.backgroundEventProcessor = new BackgroundEventProcessor();
            this.backgroundEventReaper = new CompletableEventReaper(logContext);

            // The FetchCollector is only used on the application thread.
            this.fetchCollector = new FetchCollector<>(logContext,
                    metadata,
                    subscriptions,
                    fetchConfig,
                    deserializers,
                    fetchMetricsManager,
                    time);


            if (groupMetadata.get().isPresent() &&
                GroupProtocol.of(config.getString(ConsumerConfig.GROUP_PROTOCOL_CONFIG)) == GroupProtocol.CONSUMER) {
                config.ignore(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG); // Used by background thread
            }
            config.logUnused();
            registerAppInfo(injected);
            loop.start(config.getInt(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG));
            log.debug("Kafka consumer initialized");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed; this is to prevent resource leak. see KAFKA-2121
            // we do not need to call `close` at all when `log` is null, which means no internal objects were initialized.
            if (this.log != null) {
                close(Duration.ZERO, CloseOptions.GroupMembershipOperation.LEAVE_GROUP, true);
            }
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

    // ---- constructor helpers. "injected" is the test path used by KafkaConsumer's test constructor: it mirrors the
    // previous implementation by skipping telemetry, interceptors, configured metrics and JMX registration.

    private static Optional<ClientTelemetryReporter> telemetryReporter(boolean injected, String clientId, ConsumerConfig config) {
        return injected ? Optional.empty() : CommonClientConfigs.telemetryReporter(clientId, config);
    }

    private static Metrics createMetrics(boolean injected, ConsumerConfig config, Time time, List<MetricsReporter> reporters) {
        return injected ? new Metrics(time) : ConsumerUtils.createMetrics(config, time, reporters);
    }

    private static <K, V> List<ConsumerInterceptor<K, V>> interceptorList(boolean injected, ConsumerConfig config) {
        return injected ? Collections.emptyList() : configuredConsumerInterceptors(config);
    }

    private static <K, V> Deserializers<K, V> deserializers(boolean injected,
                                                           ConsumerConfig config,
                                                           Deserializer<K> keyDeserializer,
                                                           Deserializer<V> valueDeserializer,
                                                           Metrics metrics) {
        return injected
            ? new Deserializers<>(keyDeserializer, valueDeserializer, metrics)
            : new Deserializers<>(config, keyDeserializer, valueDeserializer, metrics);
    }

    private static <K, V> ConsumerMetadata metadata(ConsumerMetadata injectedMetadata,
                                                    ConsumerConfig config,
                                                    LogContext logContext,
                                                    SubscriptionState subscriptions,
                                                    Metrics metrics,
                                                    List<ConsumerInterceptor<K, V>> interceptorList,
                                                    Deserializers<K, V> deserializers) {
        if (injectedMetadata != null)
            return injectedMetadata;
        ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(metrics.reporters(),
                interceptorList,
                Arrays.asList(deserializers.keyDeserializer(), deserializers.valueDeserializer()));
        ConsumerMetadata metadata = new ConsumerMetadata(config, subscriptions, logContext, clusterResourceListeners);
        ClientUtils.maybeBootstrapMetadataSynchronously(config, config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), metadata);
        return metadata;
    }

    private Supplier<NetworkClientDelegate> networkClientDelegateSupplier(Optional<KafkaClient> injectedClient,
                                                                          LogContext logContext,
                                                                          ConsumerConfig config,
                                                                          ApiVersions apiVersions) {
        if (injectedClient.isPresent())
            return NetworkClientDelegate.supplier(time, config, logContext, injectedClient.get(), metadata, backgroundEventHandler, false, asyncConsumerMetrics);
        return NetworkClientDelegate.supplier(time,
                logContext,
                metadata,
                config,
                apiVersions,
                metrics,
                fetchMetricsManager.throttleTimeSensor(),
                clientTelemetryReporter.map(ClientTelemetryReporter::telemetrySender).orElse(null),
                backgroundEventHandler,
                false,
                asyncConsumerMetrics);
    }

    private static FetchMetricsManager fetchMetricsManager(boolean injected, Metrics metrics) {
        // The injected metrics registry carries no client-id tag, so use the untagged registry as before.
        return injected
            ? new FetchMetricsManager(metrics, new FetchMetricsRegistry(CONSUMER_METRIC_GROUP_PREFIX))
            : createFetchMetricsManager(metrics);
    }

    private void registerAppInfo(boolean injected) {
        if (!injected)
            AppInfoParser.registerAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics, time.milliseconds());
    }

    private Optional<ConsumerGroupMetadata> initializeGroupMetadata(final ConsumerConfig config,
                                                                    final GroupRebalanceConfig groupRebalanceConfig) {
        final Optional<ConsumerGroupMetadata> groupMetadata = initializeGroupMetadata(
            groupRebalanceConfig.groupId,
            groupRebalanceConfig.groupInstanceId
        );
        if (groupMetadata.isEmpty()) {
            config.ignore(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
            config.ignore(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
        }
        return groupMetadata;
    }

    private Optional<ConsumerGroupMetadata> initializeGroupMetadata(final String groupId,
                                                                    final Optional<String> groupInstanceId) {
        if (groupId != null) {
            if (groupId.isEmpty()) {
                throw new InvalidGroupIdException("The configured " + ConsumerConfig.GROUP_ID_CONFIG
                    + " should not be an empty string or whitespace.");
            } else {
                return Optional.of(initializeConsumerGroupMetadata(groupId, groupInstanceId));
            }
        }
        return Optional.empty();
    }

    @SuppressWarnings("removal")
    private ConsumerGroupMetadata initializeConsumerGroupMetadata(final String groupId,
                                                                  final Optional<String> groupInstanceId) {
        return new ConsumerGroupMetadata(
            groupId,
            JoinGroupRequest.UNKNOWN_GENERATION_ID,
            JoinGroupRequest.UNKNOWN_MEMBER_ID,
            groupInstanceId
        );
    }

    @SuppressWarnings("removal")
    private void updateGroupMetadata(final Optional<Integer> memberEpoch, final String memberId) {
        memberEpoch.ifPresent(epoch -> groupMetadata.updateAndGet(
                oldGroupMetadataOptional -> oldGroupMetadataOptional.map(
                    oldGroupMetadata -> new ConsumerGroupMetadata(
                        oldGroupMetadata.groupId(),
                        memberEpoch.orElse(oldGroupMetadata.generationId()),
                        memberId,
                        oldGroupMetadata.groupInstanceId()
                    )
                )
            )
        );
    }

    void setGroupAssignmentSnapshot(final Set<TopicPartition> partitions) {
        groupAssignmentSnapshot.set(Collections.unmodifiableSet(partitions));
    }

    void setHasPendingReconciliation(final boolean hasPendingReconciliation) {
        this.hasPendingReconciliation = hasPendingReconciliation;
    }

    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        if (!metrics().containsKey(metric.metricName())) {
            clientTelemetryReporter.ifPresent(reporter -> reporter.metricChange(metric));
        } else {
            log.debug("Skipping registration for metric {}. Existing consumer metrics cannot be overwritten.", metric.metricName());
        }
    }

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        if (!metrics().containsKey(metric.metricName())) {
            clientTelemetryReporter.ifPresent(reporter -> reporter.metricRemoval(metric));
        } else {
            log.debug("Skipping unregistration for metric {}. Existing consumer metrics cannot be removed.", metric.metricName());
        }
    }

    /**
     * {@code poll()} on top of the {@link ConsumerEventLoop} and the {@link RecordSink}. In steady state the
     * application thread creates no events and makes no system calls: it runs pending callbacks and background
     * events, takes buffered data, and otherwise parks until the loop publishes data or a background event, a
     * callback completes, {@link #wakeup()} is called, or the timeout expires.
     *
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws java.lang.IllegalStateException if the consumer is not subscribed to any topics or manually assigned any
     *             partitions to consume from or an unexpected error occurred
     */
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        Timer timer = time.timer(timeout);

        acquireAndEnsureOpen();
        try {
            kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());

            if (subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
            }

            final long pollSequence = eventLoop.onApplicationPoll(timer.currentTimeMs());
            if (hasPendingReconciliation || !subscriptions.hasAllFetchPositions()) {
                // The loop's per-poll bookkeeping (reconciliation check, fetch position initialization, fetch
                // request creation) is needed before data can flow; wake it now instead of at its next timer.
                eventLoop.wakeup();
            }

            do {
                // We must not allow wake-ups between polling for fetches and returning the records: the consumed
                // position is updated while collecting, so a wake-up afterwards would lose the records.
                wakeupTrigger.maybeTriggerWakeup();
                offsetCommitCallbackInvoker.executeCallbacks();
                processBackgroundEvents();

                final Fetch<K, V> fetch = pollForFetches(timer, pollSequence);
                if (!fetch.isEmpty()) {
                    // Before returning the fetched records, ask for the next round of fetches so it is in flight
                    // while the user handles these records (the previous implementation's pre-fetch event).
                    eventLoop.requestFetch();

                    if (fetch.records().isEmpty()) {
                        log.trace("Returning empty records from `poll()` "
                            + "since the consumer's position has advanced for at least one topic partition");
                    }
                    return interceptors.onConsume(new ConsumerRecords<>(fetch.records(), fetch.nextOffsets()));
                }
            } while (timer.notExpired());

            return ConsumerRecords.empty();
        } finally {
            eventLoop.onApplicationPollReturn();
            kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
            release();
        }
    }

    private Fetch<K, V> pollForFetches(Timer timer, long pollSequence) {
        // if data is available already, return it immediately
        final Fetch<K, V> fetch = collectFetch(timer, pollSequence);
        if (!fetch.isEmpty())
            return fetch;

        // Same bounds as the previous implementation, minus its cross-thread maximumTimeToWait(): there is no
        // global wait time any more. When the loop may make fetching possible soon, wait at most retryBackoffMs.
        long pollTimeout = timer.remainingMs();
        if (pollTimeout > retryBackoffMs) {
            if (subscriptions.numAssignedPartitions() == 0 || !subscriptions.hasAllFetchPositions()) {
                pollTimeout = retryBackoffMs;
            } else {
                Set<TopicPartition> buffered = fetchBuffer.bufferedPartitions();
                if (subscriptions.hasFetchablePartitions(tp -> !buffered.contains(tp)))
                    pollTimeout = retryBackoffMs;
            }
        }

        log.trace("Polling for fetches with timeout {}", pollTimeout);
        Timer pollTimer = time.timer(pollTimeout);
        wakeupTrigger.setFetchAction(fetchBuffer);
        try {
            fetchBuffer.awaitWakeup(pollTimer);
        } catch (InterruptException e) {
            log.trace("Interrupt during fetch", e);
            throw e;
        } finally {
            timer.update(pollTimer.currentTimeMs());
            wakeupTrigger.clearTask();
        }
        return collectFetch(timer, pollSequence);
    }

    /**
     * Collects records from the {@link #fetchBuffer}. When partitions may be about to be revoked, buffered records
     * are not returned until the loop has run the reconciliation check for this poll (it commits and marks
     * partitions pending revocation); the loop wakes the buffer when that check completes.
     */
    private Fetch<K, V> collectFetch(Timer timer, long pollSequence) {
        while (hasPendingReconciliation && eventLoop.reconciliationCheckedPollSequence() < pollSequence) {
            if (timer.isExpired())
                return Fetch.empty();
            eventLoop.wakeup();
            Timer waitTimer = time.timer(Math.min(timer.remainingMs(), retryBackoffMs));
            wakeupTrigger.setFetchAction(fetchBuffer);
            try {
                fetchBuffer.awaitWakeup(waitTimer);
            } finally {
                timer.update(waitTimer.currentTimeMs());
                wakeupTrigger.clearTask();
            }
        }
        return fetchCollector.collectFetch(fetchBuffer);
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     */
    @Override
    public void commitSync() {
        commitSync(defaultApiTimeoutMs);
    }

    /**
     * This method sends a commit event to the EventHandler and return.
     */
    @Override
    public void commitAsync() {
        commitAsync(null);
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        commitAsync(Optional.empty(), callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        commitAsync(Optional.of(new HashMap<>(offsets)), callback);
    }

    private void commitAsync(Optional<Map<TopicPartition, OffsetAndMetadata>> offsets, OffsetCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            AsyncCommitEvent asyncCommitEvent = new AsyncCommitEvent(offsets);
            lastPendingAsyncCommit = commit(asyncCommitEvent).whenComplete((committedOffsets, throwable) -> {
                // Runs on the loop thread; wake the application thread so it runs the callback promptly.
                fetchBuffer.wakeup();
                if (throwable == null) {
                    offsetCommitCallbackInvoker.enqueueInterceptorInvocation(committedOffsets);
                }

                if (callback == null) {
                    if (throwable != null) {
                        log.error("Offset commit with offsets {} failed", committedOffsets, throwable);
                    }
                    return;
                }

                offsetCommitCallbackInvoker.enqueueUserCallbackInvocation(callback, committedOffsets, (Exception) throwable);
            });
        } finally {
            release();
        }
    }

    private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commit(final CommitEvent commitEvent) {
        throwIfGroupIdNotDefined();
        offsetCommitCallbackInvoker.executeCallbacks();

        if (commitEvent.offsets().isPresent() && commitEvent.offsets().get().isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        eventLoop.add(commitEvent);

        // This blocks until the background thread retrieves allConsumed positions to commit if none were explicitly specified.
        // This operation will ensure that the offsets to commit are not affected by fetches which may start after this
        ConsumerUtils.getResult(commitEvent.offsetsReady(), defaultApiTimeoutMs.toMillis());
        return commitEvent.future();
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        if (offset < 0)
            throw new IllegalArgumentException("seek offset must not be a negative number");

        acquireAndEnsureOpen();
        try {
            log.info("Seeking to offset {} for partition {}", offset, partition);
            SeekUnvalidatedEvent seekUnvalidatedEventEvent = new SeekUnvalidatedEvent(
                defaultApiTimeoutDeadlineMs(),
                partition,
                offset,
                Optional.empty()
            );
            eventLoop.addAndGet(seekUnvalidatedEventEvent);
        } finally {
            release();
        }
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        long offset = offsetAndMetadata.offset();
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }

        acquireAndEnsureOpen();
        try {
            if (offsetAndMetadata.leaderEpoch().isPresent()) {
                log.info("Seeking to offset {} for partition {} with epoch {}",
                    offset, partition, offsetAndMetadata.leaderEpoch().get());
            } else {
                log.info("Seeking to offset {} for partition {}", offset, partition);
            }

            eventLoop.addAndGet(new SeekUnvalidatedEvent(
                defaultApiTimeoutDeadlineMs(),
                partition,
                offsetAndMetadata.offset(),
                offsetAndMetadata.leaderEpoch()
            ));
        } finally {
            release();
        }
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        seek(partitions, AutoOffsetResetStrategy.EARLIEST);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        seek(partitions, AutoOffsetResetStrategy.LATEST);
    }

    private void seek(Collection<TopicPartition> partitions, AutoOffsetResetStrategy offsetResetStrategy) {
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        acquireAndEnsureOpen();
        try {
            eventLoop.addAndGet(new ResetOffsetEvent(
                partitions,
                offsetResetStrategy,
                defaultApiTimeoutDeadlineMs())
            );
        } finally {
            release();
        }
    }

    @Override
    public long position(TopicPartition partition) {
        return position(partition, defaultApiTimeoutMs);
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            if (!subscriptions.isAssigned(partition))
                throw new IllegalStateException("You can only check the position for partitions assigned to this consumer.");

            Timer timer = time.timer(timeout);
            do {
                SubscriptionState.FetchPosition position = subscriptions.validPosition(partition);
                if (position != null)
                    return position.offset;

                updateFetchPositions(timer);
                timer.update();
                wakeupTrigger.maybeTriggerWakeup();
            } while (timer.notExpired());

            throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the position " +
                "for partition " + partition + " could be determined");
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return committed(partitions, defaultApiTimeoutMs);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions,
                                                            final Duration timeout) {
        acquireAndEnsureOpen();
        long start = time.nanoseconds();
        try {
            throwIfGroupIdNotDefined();
            if (partitions.isEmpty()) {
                return Collections.emptyMap();
            }

            final FetchCommittedOffsetsEvent event = new FetchCommittedOffsetsEvent(
                partitions,
                calculateDeadlineMs(time, timeout));
            wakeupTrigger.setActiveTask(event.future());
            try {
                return eventLoop.addAndGet(event);
            } catch (TimeoutException e) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the last " +
                    "committed offset for partitions " + partitions + " could be determined. Try tuning " +
                    ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG + " larger to relax the threshold.");
            } finally {
                wakeupTrigger.clearTask();
            }
        } finally {
            kafkaConsumerMetrics.recordCommitted(time.nanoseconds() - start);
            release();
        }
    }

    private void throwIfGroupIdNotDefined() {
        if (groupMetadata.get().isEmpty()) {
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
        }
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(metrics.metrics());
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return partitionsFor(topic, defaultApiTimeoutMs);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            Cluster cluster = this.metadata.fetch();
            List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
            if (!parts.isEmpty())
                return parts;

            if (timeout.toMillis() == 0L) {
                throw new TimeoutException();
            }

            final TopicMetadataEvent topicMetadataEvent = new TopicMetadataEvent(topic, calculateDeadlineMs(time, timeout));
            wakeupTrigger.setActiveTask(topicMetadataEvent.future());
            try {
                Map<String, List<PartitionInfo>> topicMetadata =
                        eventLoop.addAndGet(topicMetadataEvent);

                return topicMetadata.getOrDefault(topic, Collections.emptyList());
            } finally {
                wakeupTrigger.clearTask();
            }
        } finally {
            release();
        }
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return listTopics(defaultApiTimeoutMs);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            if (timeout.toMillis() == 0L) {
                throw new TimeoutException();
            }

            final AllTopicsMetadataEvent topicMetadataEvent = new AllTopicsMetadataEvent(calculateDeadlineMs(time, timeout));
            wakeupTrigger.setActiveTask(topicMetadataEvent.future());
            try {
                return eventLoop.addAndGet(topicMetadataEvent);
            } finally {
                wakeupTrigger.clearTask();
            }
        } finally {
            release();
        }
    }

    @Override
    public Set<TopicPartition> paused() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(subscriptions.pausedPartitions());
        } finally {
            release();
        }
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            Objects.requireNonNull(partitions, "The partitions to pause must be nonnull");

            if (!partitions.isEmpty())
                eventLoop.addAndGet(new PausePartitionsEvent(partitions, defaultApiTimeoutDeadlineMs()));
        } finally {
            release();
        }
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            Objects.requireNonNull(partitions, "The partitions to resume must be nonnull");

            if (!partitions.isEmpty())
                eventLoop.addAndGet(new ResumePartitionsEvent(partitions, defaultApiTimeoutDeadlineMs()));
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, defaultApiTimeoutMs);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            // Keeping same argument validation error thrown by the current consumer implementation
            // to avoid API level changes.
            requireNonNull(timestampsToSearch, "Timestamps to search cannot be null");
            for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
                // Exclude the earliest and latest offset here so the timestamp in the returned
                // OffsetAndTimestamp is always positive.
                if (entry.getValue() < 0)
                    throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                        entry.getValue() + ". The target time cannot be negative.");
            }

            if (timestampsToSearch.isEmpty()) {
                return Collections.emptyMap();
            }
            ListOffsetsEvent listOffsetsEvent = new ListOffsetsEvent(
                    timestampsToSearch,
                    calculateDeadlineMs(time, timeout),
                    true);

            // If timeout is set to zero return empty immediately; otherwise try to get the results
            // and throw timeout exception if it cannot complete in time.
            if (timeout.toMillis() == 0L) {
                eventLoop.add(listOffsetsEvent);
                return listOffsetsEvent.emptyResults();
            }

            try {
                Map<TopicPartition, OffsetAndTimestampInternal> offsets = eventLoop.addAndGet(listOffsetsEvent);
                Map<TopicPartition, OffsetAndTimestamp> results = new HashMap<>(offsets.size());
                offsets.forEach((k, v) -> results.put(k, v != null ? v.buildOffsetAndTimestamp() : null));
                return results;
            } catch (TimeoutException e) {
                throw new TimeoutException("Failed to get offsets by times in " + timeout.toMillis() + "ms");
            }
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets(partitions, defaultApiTimeoutMs);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.EARLIEST_TIMESTAMP, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, defaultApiTimeoutMs);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.LATEST_TIMESTAMP, timeout);
    }

    private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition> partitions,
                                                           long timestamp,
                                                           Duration timeout) {
        acquireAndEnsureOpen();
        try {
            // Keeping same argument validation error thrown by the current consumer implementation
            // to avoid API level changes.
            requireNonNull(partitions, "Partitions cannot be null");

            if (partitions.isEmpty()) {
                return Collections.emptyMap();
            }

            Map<TopicPartition, Long> timestampToSearch = partitions
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), tp -> timestamp));
            ListOffsetsEvent listOffsetsEvent = new ListOffsetsEvent(
                    timestampToSearch,
                    calculateDeadlineMs(time, timeout),
                    false);

            // If timeout is set to zero return empty immediately; otherwise try to get the results
            // and throw timeout exception if it cannot complete in time.
            if (timeout.isZero()) {
                eventLoop.add(listOffsetsEvent);
                // It is used to align with classic consumer.
                // When the "timeout == 0", the classic consumer will return an empty map.
                // Therefore, the AsyncKafkaConsumer needs to be consistent with it.
                return new HashMap<>();
            }

            Map<TopicPartition, OffsetAndTimestampInternal> offsetAndTimestampMap;
            try {
                offsetAndTimestampMap = eventLoop.addAndGet(listOffsetsEvent);
                return offsetAndTimestampMap.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().offset()));
            } catch (TimeoutException e) {
                throw new TimeoutException("Failed to get offsets by times in " + timeout.toMillis() + "ms");
            }
        } finally {
            release();
        }
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        acquireAndEnsureOpen();
        try {
            return eventLoop.addAndGet(new CurrentLagEvent(
                topicPartition,
                isolationLevel,
                defaultApiTimeoutDeadlineMs()
            ));
        } finally {
            release();
        }
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        acquireAndEnsureOpen();
        try {
            throwIfGroupIdNotDefined();
            return groupMetadata.get().get();
        } finally {
            release();
        }
    }

    @Override
    public void enforceRebalance() {
        log.warn("Operation not supported in new consumer group protocol");
    }

    @Override
    public void enforceRebalance(String reason) {
        log.warn("Operation not supported in new consumer group protocol");
    }

    @Override
    public void close() {
        close(CloseOptions.timeout(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS)));
    }

    @Deprecated
    @Override
    public void close(Duration timeout) {
        close(CloseOptions.timeout(timeout));
    }

    @Override
    public void close(CloseOptions option) {
        Duration timeout = option.timeout().orElseGet(() -> Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));

        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        acquire();
        try {
            if (!closed) {
                // need to close before setting the flag since the close function
                // itself may trigger rebalance callback that needs the consumer to be open still
                close(timeout, option.groupMembershipOperation(), false);
            }
        } finally {
            closed = true;
            release();
        }
    }

    /**
     * Please keep these tenets in mind for the implementation of the {@link EventLoopKafkaConsumer}’s
     * {@link #close(Duration)} method. In the future, these tenets may be made officially part of the top-level
     * {@link KafkaConsumer#close(Duration)} API, but for now they remain here.
     *
     * <ol>
     *     <li>
     *         The execution of the {@link ConsumerRebalanceListener} callback (if applicable) must be performed on
     *         the application thread to ensure it does not interfere with the network I/O on the background thread.
     *     </li>
     *     <li>
     *         The {@link ConsumerRebalanceListener} callback execution must complete before an attempt to leave
     *         the consumer group is performed. In this context, “complete” does not necessarily imply
     *         <em>success</em>; execution is “complete” even if the execution <em>fails</em> with an error.
     *     </li>
     *     <li>
     *         Any error thrown during the {@link ConsumerRebalanceListener} callback execution will be caught to
     *         ensure it does not prevent execution of the remaining {@link #close()} logic.
     *     </li>
     *     <li>
     *         The application thread will be blocked during the entire duration of the execution of the
     *         {@link ConsumerRebalanceListener}. The consumer does not employ a mechanism to short-circuit the
     *         callback execution, so execution is not bound by the timeout in {@link #close(Duration)}.
     *     </li>
     *     <li>
     *         A given {@link ConsumerRebalanceListener} implementation may be affected by the application thread's
     *         interrupt state. If the callback implementation performs any blocking operations, it may result in
     *         an error. An implementation may choose to preemptively check the thread's interrupt flag via
     *         {@link Thread#isInterrupted()} or {@link Thread#isInterrupted()} and alter its behavior.
     *     </li>
     *     <li>
     *         If the application thread was interrupted <em>prior</em> to the execution of the
     *         {@link ConsumerRebalanceListener} callback, the thread's interrupt state will be preserved for the
     *         {@link ConsumerRebalanceListener} execution.
     *     </li>
     *     <li>
     *         If the application thread was interrupted <em>prior</em> to the execution of the
     *         {@link ConsumerRebalanceListener} callback <em>but</em> the callback cleared out the interrupt state,
     *         the {@link #close()} method will not make any effort to restore the application thread's interrupt
     *         state for the remainder of the execution of {@link #close()}.
     *     </li>
     *     <li>
     *         Leaving the consumer group is achieved by issuing a ‘leave group‘ network request. The consumer will
     *         attempt to leave the group on a “best-case” basis. There is no stated guarantee that the consumer will
     *         have successfully left the group before the {@link #close()} method completes processing.
     *     </li>
     *     <li>
     *         The consumer will attempt to leave the group regardless of the timeout elapsing or the application
     *         thread receiving an {@link InterruptException} or {@link InterruptedException}.
     *     </li>
     *     <li>
     *         The application thread will wait for confirmation that the consumer left the group until one of the
     *         following occurs:
     *
     *         <ol>
     *             <li>Confirmation that the ’leave group‘ response was received from the group coordinator</li>
     *             <li>The timeout provided by the user elapses</li>
     *             <li>An {@link InterruptException} or {@link InterruptedException} is thrown</li>
     *         </ol>
     *     </li>
     * </ol>
     */
    private void close(Duration timeout, CloseOptions.GroupMembershipOperation membershipOperation, boolean swallowException) {
        log.trace("Closing the Kafka consumer");
        AtomicReference<Throwable> firstException = new AtomicReference<>();

        // We are already closing with a timeout, don't allow wake-ups from here on.
        wakeupTrigger.disableWakeups();

        final Timer closeTimer = createTimerForCloseRequests(timeout);
        clientTelemetryReporter.ifPresent(ClientTelemetryReporter::initiateClose);
        closeTimer.update();
        // Prepare shutting down the network thread
        // Prior to closing the network thread, we need to make sure the following operations happen in the right
        // sequence...
        swallow(log, Level.ERROR, "Failed to auto-commit offsets",
            () -> autoCommitOnClose(closeTimer), firstException);
        swallow(log, Level.ERROR, "Failed to stop finding coordinator",
            this::stopFindCoordinatorOnClose, firstException);
        swallow(log, Level.ERROR, "Failed to run rebalance callbacks",
            this::runRebalanceCallbacksOnClose, firstException);
        swallow(log, Level.ERROR, "Failed to leave group while closing consumer",
            () -> leaveGroupOnClose(closeTimer, membershipOperation), firstException);
        swallow(log, Level.ERROR, "Failed invoking asynchronous commit callbacks while closing consumer",
            () -> awaitPendingAsyncCommitsAndExecuteCommitCallbacks(closeTimer, false), firstException);
        if (eventLoop != null)
            closeQuietly(() -> eventLoop.close(Duration.ofMillis(closeTimer.remainingMs())), "Failed shutting down network thread", firstException);
        closeTimer.update();

        // close() can be called from inside one of the constructors. In that case, it's possible that neither
        // the reaper nor the background event queue were constructed, so check them first to avoid NPE.
        if (backgroundEventReaper != null && backgroundEventQueue != null)
            backgroundEventReaper.reap(backgroundEventQueue);

        closeQuietly(interceptors, "consumer interceptors", firstException);
        closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
        closeQuietly(asyncConsumerMetrics, "async consumer metrics", firstException);
        closeQuietly(fetchMetricsManager, "consumer fetch metrics", firstException);
        closeQuietly(rebalanceCallbackMetricsManager, "consumer rebalance callback metrics");
        closeQuietly(metrics, "consumer metrics", firstException);
        closeQuietly(deserializers, "consumer deserializers", firstException);
        clientTelemetryReporter.ifPresent(reporter -> closeQuietly(reporter, "async consumer telemetry reporter", firstException));

        AppInfoParser.unregisterAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics);
        log.debug("Kafka consumer has been closed");
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }

    private Timer createTimerForCloseRequests(Duration timeout) {
        // this.time could be null if an exception occurs in constructor prior to setting the this.time field
        final Time time = (this.time == null) ? Time.SYSTEM : this.time;
        return time.timer(Math.min(timeout.toMillis(), requestTimeoutMs));
    }

    private void autoCommitOnClose(final Timer timer) {
        if (groupMetadata.get().isEmpty() || eventLoop == null)
            return;

        if (autoCommitEnabled)
            commitSyncAllConsumed(timer);

        eventLoop.add(new CommitOnCloseEvent());
    }

    private void runRebalanceCallbacksOnClose() {
        if (groupMetadata.get().isEmpty())
            return;

        int memberEpoch = groupMetadata.get().get().generationId();

        Exception error = null;

        if (rebalanceListenerInvoker != null) {

            Set<TopicPartition> assignedPartitions = groupAssignmentSnapshot.get();

            if (assignedPartitions.isEmpty())
                // Nothing to revoke.
                return;

            SortedSet<TopicPartition> droppedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
            droppedPartitions.addAll(assignedPartitions);

            if (memberEpoch > 0) {
                error = rebalanceListenerInvoker.invokePartitionsRevoked(droppedPartitions);
            } else {
                error = rebalanceListenerInvoker.invokePartitionsLost(droppedPartitions);
            }

        }

        if (error != null)
            throw ConsumerUtils.maybeWrapAsKafkaException(error);
    }

    private void leaveGroupOnClose(final Timer timer, final CloseOptions.GroupMembershipOperation membershipOperation) {
        if (groupMetadata.get().isEmpty() ||  eventLoop == null)
            return;

        log.debug("Leaving the consumer group during consumer close");
        try {
            eventLoop.addAndGet(new LeaveGroupOnCloseEvent(calculateDeadlineMs(timer), membershipOperation));
            log.info("Completed leaving the group");
        } catch (TimeoutException e) {
            log.warn("Consumer attempted to leave the group but couldn't " +
                "complete it within {} ms. It will proceed to close.", timer.timeoutMs());
        } finally {
            timer.update();
        }
    }

    private void stopFindCoordinatorOnClose() {
        if (groupMetadata.get().isEmpty() || eventLoop == null)
            return;
        log.debug("Stop finding coordinator during consumer close");
        eventLoop.add(new StopFindCoordinatorOnCloseEvent());
    }

    // Visible for testing
    void commitSyncAllConsumed(final Timer timer) {
        log.debug("Sending synchronous auto-commit on closing");
        try {
            commitSync(Duration.ofMillis(timer.remainingMs()));
        } catch (Exception e) {
            // consistent with async auto-commit failures, we do not propagate the exception
            log.warn("Synchronous auto-commit failed", e);
        }
        timer.update();
    }

    @Override
    public void wakeup() {
        wakeupTrigger.wakeup();
    }

    /**
     * This method sends a commit event to the EventHandler and waits for
     * the event to finish.
     *
     * @param timeout max wait time for the blocking operation.
     */
    @Override
    public void commitSync(final Duration timeout) {
        commitSync(Optional.empty(), timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(Optional.of(new HashMap<>(offsets)), defaultApiTimeoutMs);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        commitSync(Optional.of(new HashMap<>(offsets)), timeout);
    }

    private void commitSync(Optional<Map<TopicPartition, OffsetAndMetadata>> offsets, Duration timeout) {
        acquireAndEnsureOpen();
        long commitStart = time.nanoseconds();
        try {
            SyncCommitEvent syncCommitEvent = new SyncCommitEvent(offsets, calculateDeadlineMs(time, timeout));
            CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitFuture = commit(syncCommitEvent);

            Timer requestTimer = time.timer(timeout.toMillis());
            awaitPendingAsyncCommitsAndExecuteCommitCallbacks(requestTimer, true);

            wakeupTrigger.setActiveTask(commitFuture);
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = ConsumerUtils.getResult(commitFuture, requestTimer);
            interceptors.onCommit(committedOffsets);
        } finally {
            wakeupTrigger.clearTask();
            kafkaConsumerMetrics.recordCommitSync(time.nanoseconds() - commitStart);
            release();
        }
    }

    private void awaitPendingAsyncCommitsAndExecuteCommitCallbacks(Timer timer, boolean enableWakeup) {
        if (lastPendingAsyncCommit == null || offsetCommitCallbackInvoker == null) {
            return;
        }

        try {
            final CompletableFuture<Void> futureToAwait = new CompletableFuture<>();
            // We don't want the wake-up trigger to complete our pending async commit future,
            // so create new future here. Any errors in the pending async commit will be handled
            // by the async commit future / the commit callback - here, we just want to wait for it to complete.
            lastPendingAsyncCommit.whenComplete((v, t) -> futureToAwait.complete(null));
            if (enableWakeup) {
                wakeupTrigger.setActiveTask(futureToAwait);
            }
            ConsumerUtils.getResult(futureToAwait, timer);
            lastPendingAsyncCommit = null;
        } finally {
            if (enableWakeup) {
                wakeupTrigger.clearTask();
            }
            timer.update();
        }
        offsetCommitCallbackInvoker.executeCallbacks();
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        if (clientTelemetryReporter.isEmpty()) {
            throw new IllegalStateException("Telemetry is not enabled. Set config `" + ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG + "` to `true`.");
        }

        return ClientTelemetryUtils.fetchClientInstanceId(clientTelemetryReporter.get(), timeout);
    }

    @Override
    public Set<TopicPartition> assignment() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(subscriptions.assignedPartitions());
        } finally {
            release();
        }
    }

    /**
     * Get the current subscription, or an empty set if no such call has
     * been made.
     * @return The set of topics currently subscribed to
     */
    @Override
    public Set<String> subscription() {
        acquireAndEnsureOpen();
        try {
            return Set.copyOf(subscriptions.subscription());
        } finally {
            release();
        }
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            if (partitions == null) {
                throw new IllegalArgumentException("Topic partitions collection to assign to cannot be null");
            }

            if (partitions.isEmpty()) {
                unsubscribe();
                return;
            }

            for (TopicPartition tp : partitions) {
                String topic = (tp != null) ? tp.topic() : null;
                if (isBlank(topic))
                    throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
            }

            // Clear the buffered data which are not a part of newly assigned topics
            final Set<TopicPartition> currentTopicPartitions = new HashSet<>();

            for (TopicPartition tp : subscriptions.assignedPartitions()) {
                if (partitions.contains(tp))
                    currentTopicPartitions.add(tp);
            }

            fetchBuffer.retainAll(currentTopicPartitions);

            // assignment change event will trigger autocommit if it is configured and the group id is specified. This is
            // to make sure offsets of topic partitions the consumer is unsubscribing from are committed since there will
            // be no following rebalance.
            //
            // See the ApplicationEventProcessor.process() method that handles this event for more detail.
            eventLoop.addAndGet(new AssignmentChangeEvent(
                time.milliseconds(),
                defaultApiTimeoutDeadlineMs(),
                partitions
            ));
        } finally {
            release();
        }
    }

    @Override
    public void unsubscribe() {
        acquireAndEnsureOpen();
        try {
            fetchBuffer.retainAll(Collections.emptySet());
            Timer timer = time.timer(defaultApiTimeoutMs);
            UnsubscribeEvent unsubscribeEvent = new UnsubscribeEvent(calculateDeadlineMs(timer));
            eventLoop.add(unsubscribeEvent);
            log.info("Unsubscribing all topics or patterns and assigned partitions {}",
                    subscriptions.assignedPartitions());

            try {
                // If users have fatal error, they will get some exceptions in the background queue.
                // When running unsubscribe, these exceptions should be ignored, or users can't unsubscribe successfully.
                // We also skip processing assignment events (PARTITIONS_ASSIGNED, STREAMS_TASKS_ASSIGNED) because
                // they are not relevant anymore (consumer already unsubscribing).
                processBackgroundEvents(unsubscribeEvent.future(), timer,
                    e -> (e instanceof GroupAuthorizationException || e instanceof TopicAuthorizationException),
                    true);
                log.info("Unsubscribed all topics or patterns and assigned partitions");
            } catch (TimeoutException e) {
                log.error("Failed while waiting for the unsubscribe event to complete");
            }
            resetGroupMetadata();
        } catch (Exception e) {
            log.error("Unsubscribe failed", e);
            throw e;
        } finally {
            release();
        }
    }

    private void resetGroupMetadata() {
        groupMetadata.updateAndGet(
            oldGroupMetadataOptional -> oldGroupMetadataOptional
                .map(oldGroupMetadata -> initializeConsumerGroupMetadata(
                    oldGroupMetadata.groupId(),
                    oldGroupMetadata.groupInstanceId()
                ))
        );
    }

    // Visible for testing
    WakeupTrigger wakeupTrigger() {
        return wakeupTrigger;
    }

    /**
     * Set the fetch position to the committed position (if there is one)
     * or reset it using the offset reset policy the user has configured.
     *
     * @return true iff the operation completed without timing out
     */
    private boolean updateFetchPositions(final Timer timer) {
        try {
            CheckAndUpdatePositionsEvent checkAndUpdatePositionsEvent = new CheckAndUpdatePositionsEvent(calculateDeadlineMs(timer));
            wakeupTrigger.setActiveTask(checkAndUpdatePositionsEvent.future());
            eventLoop.addAndGet(checkAndUpdatePositionsEvent);
        } catch (TimeoutException e) {
            return false;
        } finally {
            wakeupTrigger.clearTask();
        }
        return true;
    }

    @Override
    public boolean updateAssignmentMetadataIfNeeded(Timer timer) {
        offsetCommitCallbackInvoker.executeCallbacks();
        if (subscriptions.hasPatternSubscription()) {
            try {
                eventLoop.addAndGet(new UpdatePatternSubscriptionEvent(calculateDeadlineMs(timer)));
            } catch (TimeoutException e) {
                return false;
            } finally {
                timer.update();
            }
        }
        processBackgroundEvents();

        return updateFetchPositions(timer);
    }

    @Override
    public void subscribe(Collection<String> topics) {
        subscribeInternal(topics, null);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        subscribeInternal(topics, listener);
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribeInternal(pattern, null);
    }

    @Override
    public void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        subscribeToRegex(pattern, listener);
    }

    @Override
    public void subscribe(SubscriptionPattern pattern) {
        subscribeToRegex(pattern, null);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        subscribeInternal(pattern, listener);
    }


    @Override
    public void setRebalanceListener(RebalanceListener callback) {
        acquireAndEnsureOpen();
        try {
            subscriptions.setRebalanceListener(callback, this);
        } finally {
            release();
        }
    }

    /**
     * Acquire the light lock and ensure that the consumer hasn't been closed.
     *
     * @throws IllegalStateException If the consumer has been closed
     */
    private void acquireAndEnsureOpen() {
        acquire();
        if (this.closed) {
            release();
            throw new IllegalStateException("This consumer has already been closed.");
        }

        try {
            metadata.maybeThrowBootstrapFatalException();
        } catch (RuntimeException e) {
            release();
            throw e;
        }
    }

    /**
     * Acquire the light lock protecting this consumer from multithreaded access. Instead of blocking
     * when the lock is not available, however, we just throw an exception (since multithreaded usage is not
     * supported).
     *
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        final Thread thread = Thread.currentThread();
        final long threadId = thread.getId();
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access. " +
                "currentThread(name: " + thread.getName() + ", id: " + threadId + ")" +
                " otherThread(id: " + currentThread.get() + ")"
            );
        refCount.incrementAndGet();
    }

    /**
     * Release the light lock protecting the consumer from multithreaded access.
     */
    private void release() {
        if (refCount.decrementAndGet() == 0)
            currentThread.set(NO_CURRENT_THREAD);
    }

    private void subscribeInternal(Pattern pattern, ConsumerRebalanceListener listener) {
        acquireAndEnsureOpen();
        try {
            throwIfGroupIdNotDefined();
            if (pattern == null || pattern.toString().isEmpty())
                throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ?
                    "null" : "empty"));
            if (listener != null)
                subscriptions.setRebalanceListener(listener, this);
            log.info("Subscribed to pattern: '{}'", pattern);
            eventLoop.addAndGet(new TopicPatternSubscriptionChangeEvent(
                pattern,
                defaultApiTimeoutDeadlineMs()
            ));
        } finally {
            release();
        }
    }

    /**
     * Subscribe to the RE2/J pattern. This will generate an event to update the pattern in the
     * subscription state, so it's included in the next heartbeat request sent to the broker.
     * No validation of the pattern is performed by the client (other than null/empty checks).
     */
    private void subscribeToRegex(SubscriptionPattern pattern, ConsumerRebalanceListener listener) {
        acquireAndEnsureOpen();
        try {
            throwIfGroupIdNotDefined();
            throwIfSubscriptionPatternIsInvalid(pattern);
            if (listener != null)
                subscriptions.setRebalanceListener(listener, this);
            log.info("Subscribing to regular expression {}", pattern);
            eventLoop.addAndGet(new TopicRe2JPatternSubscriptionChangeEvent(
                pattern,
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs))));
        } finally {
            release();
        }
    }

    private void throwIfSubscriptionPatternIsInvalid(SubscriptionPattern subscriptionPattern) {
        if (subscriptionPattern == null) {
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be null");
        }
        if (subscriptionPattern.pattern().isEmpty()) {
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be empty");
        }
    }

    private void subscribeInternal(Collection<String> topics, ConsumerRebalanceListener listener) {
        acquireAndEnsureOpen();
        try {
            throwIfGroupIdNotDefined();
            if (topics == null)
                throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
            if (topics.isEmpty()) {
                // treat subscribing to empty topic list as the same as unsubscribing
                unsubscribe();
            } else {
                for (String topic : topics) {
                    if (isBlank(topic))
                        throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
                }

                if (listener != null)
                    subscriptions.setRebalanceListener(listener, this);

                // Clear the buffered data which are not a part of newly assigned topics
                final Set<TopicPartition> currentTopicPartitions = new HashSet<>();
                for (TopicPartition tp : subscriptions.assignedPartitions()) {
                    if (topics.contains(tp.topic()))
                        currentTopicPartitions.add(tp);
                }
                fetchBuffer.retainAll(currentTopicPartitions);

                log.info("Subscribed to topic(s): {}", String.join(", ", topics));
                eventLoop.addAndGet(new TopicSubscriptionChangeEvent(
                    new HashSet<>(topics),
                    defaultApiTimeoutDeadlineMs()
                ));
            }
        } finally {
            release();
        }
    }

    /**
     * Process the events-if any-that were produced by the {@link ConsumerNetworkThread network thread}.
     * It is possible that {@link ErrorEvent an error}
     * could occur when processing the events. In such cases, the processor will take a reference to the first
     * error, continue to process the remaining events, and then throw the first error that occurred.
     *
     * Visible for testing.
     */
    boolean processBackgroundEvents() {
        return processBackgroundEvents(false);
    }

    /**
     * Checks if the given background event is an assignment update event.
     * Those are to update reconciled assignments, so should only be processed from poll() and not from unsubscribe().
     */
    private static boolean isAssignmentEvent(BackgroundEvent event) {
        return event.type() == BackgroundEvent.Type.PARTITIONS_ASSIGNED ||
               event.type() == BackgroundEvent.Type.STREAMS_TASKS_ASSIGNED;
    }

    /**
     * Process the events produced by the background thread.
     * It is possible that {@link ErrorEvent an error}
     * could occur when processing the events. In such cases, the processor will take a reference to the first
     * error, continue to process the remaining events, and then throw the first error that occurred.
     * Visible for testing.
     *
     * @param skipAssignmentEvents If true, skip processing events that update a new assignment after a reconciliation
     *                             (PARTITIONS_ASSIGNED and STREAMS_TASKS_ASSIGNED)
     *                             These events should only be processed from poll(), not from unsubscribe().
     * @return true if any events were drained from the queue
     */
    boolean processBackgroundEvents(boolean skipAssignmentEvents) {
        AtomicReference<KafkaException> firstError = new AtomicReference<>();

        List<BackgroundEvent> events = backgroundEventHandler.drainEvents();
        if (!events.isEmpty()) {
            long startMs = time.milliseconds();
            for (BackgroundEvent event : events) {
                asyncConsumerMetrics.recordBackgroundEventQueueTime(time.milliseconds() - event.enqueuedMs());
                try {
                    if (event instanceof CompletableEvent)
                        backgroundEventReaper.add((CompletableEvent<?>) event);

                    // Skip assignment events if requested (e.g., during unsubscribe).
                    // These events should only be processed from poll().
                    // Complete them exceptionally to unblock the reconciliation in the background.
                    if (skipAssignmentEvents && isAssignmentEvent(event)) {
                        if (event instanceof CompletableEvent) {
                            ((CompletableEvent<?>) event).future().completeExceptionally(
                                new KafkaException("Assignment event skipped because consumer is unsubscribing"));
                        }
                        log.debug("Skipped processing {} during unsubscribe", event.type());
                        continue;
                    }

                    backgroundEventProcessor.process(event);
                } catch (Throwable t) {
                    KafkaException e = ConsumerUtils.maybeWrapAsKafkaException(t);

                    if (!firstError.compareAndSet(null, e))
                        log.warn("An error occurred when processing the background event: {}", e.getMessage(), e);
                }
            }
            asyncConsumerMetrics.recordBackgroundEventQueueProcessingTime(time.milliseconds() - startMs);
        }

        backgroundEventReaper.reap(time.milliseconds());

        if (firstError.get() != null)
            throw firstError.get();

        return !events.isEmpty();
    }

    /**
     * This method can be used by cases where the caller has an event that needs to both block for completion but
     * also process background events. For some events, in order to fully process the associated logic, the
     * {@link ConsumerNetworkThread background thread} needs assistance from the application thread to complete.
     * If the application thread simply blocked on the event after submitting it, the processing would deadlock.
     * The logic herein is basically a loop that performs two tasks in each iteration:
     *
     * <ol>
     *     <li>Process background events, if any</li>
     *     <li><em>Briefly</em> wait for {@link CompletableApplicationEvent an event} to complete</li>
     * </ol>
     *
     * <p/>
     *
     * Each iteration gives the application thread an opportunity to process background events, which may be
     * necessary to complete the overall processing.
     * <p/>
     *
     * As an example, take {@link #unsubscribe()}. To start unsubscribing, the application thread enqueues an
     * {@link UnsubscribeEvent} on the application event queue. That event will eventually trigger the
     * rebalancing logic in the background thread. Critically, as part of this rebalancing work, the
     * {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)} callback needs to be invoked for any
     * partitions the consumer owns. However,
     * this callback must be executed on the application thread. To achieve this, the background thread enqueues a
     * {@link PartitionsRemovedEvent} on its background event queue. That event queue is
     * periodically queried by the application thread to see if there's work to be done. When the application thread
     * sees {@link PartitionsRemovedEvent}, it is processed, and then a
     * {@link ConsumerRebalanceListenerCallbackCompletedEvent} is then enqueued by the application thread on the
     * application event queue. Moments later, the background thread will see that event, process it, and continue
     * execution of the rebalancing logic. The rebalancing logic cannot complete until the
     * {@link ConsumerRebalanceListener} callback is performed.
     *
     * @param future                    Event that contains a {@link CompletableFuture}; it is on this future that the
     *                                  application thread will wait for completion
     * @param timer                     Overall timer that bounds how long to wait for the event to complete
     * @param ignoreErrorEventException Predicate to ignore background errors.
     *                                  Any exceptions found while processing background events that match the predicate won't be propagated.
     * @param skipAssignmentEvents      If true, skip processing PARTITIONS_ASSIGNED and STREAMS_TASKS_ASSIGNED
     *                                  events and complete them exceptionally. These events should only be
     *                                  processed from poll(), not from unsubscribe() or other operations.
     * @return the completed result of the supplied {@code future}
     * @throws TimeoutException if the operation does not complete before the timer expires
     */
    // Visible for testing
    <T> T processBackgroundEvents(Future<T> future, Timer timer, Predicate<Exception> ignoreErrorEventException,
                                  boolean skipAssignmentEvents) {
        do {
            boolean hadEvents = false;
            try {
                hadEvents = processBackgroundEvents(skipAssignmentEvents);
            } catch (Exception e) {
                if (!ignoreErrorEventException.test(e))
                    throw e;
            }

            try {
                if (future.isDone()) {
                    // If the event is done (either successfully or otherwise), go ahead and attempt to return
                    // without waiting. We use the ConsumerUtils.getResult() method here to handle the conversion
                    // of the exception types.
                    return ConsumerUtils.getResult(future);
                } else if (!hadEvents) {
                    // If the above processing yielded no events, then let's sit tight for a bit to allow the
                    // background thread to either finish the task, or populate the background event
                    // queue with things to process in our next loop.
                    Timer pollInterval = time.timer(100L);
                    return ConsumerUtils.getResult(future, pollInterval);
                }
            } catch (TimeoutException swallow) {
                // Ignore this as we will retry the event until the timeout expires.
            } finally {
                timer.update();
            }
        } while (timer.notExpired());

        throw new TimeoutException("Operation timed out before completion");
    }

    static ConsumerRebalanceListenerCallbackCompletedEvent invokeRebalanceCallbacks(ConsumerRebalanceListenerInvoker rebalanceListenerInvoker,
                                                                                    ConsumerRebalanceListenerMethodName methodName,
                                                                                    SortedSet<TopicPartition> partitions,
                                                                                    CompletableFuture<Void> future) {
        Exception e;

        try {
            switch (methodName) {
                case ON_PARTITIONS_REVOKED:
                    e = rebalanceListenerInvoker.invokePartitionsRevoked(partitions);
                    break;

                case ON_PARTITIONS_ASSIGNED:
                    e = rebalanceListenerInvoker.invokePartitionsAssigned(partitions);
                    break;

                case ON_PARTITIONS_LOST:
                    e = rebalanceListenerInvoker.invokePartitionsLost(partitions);
                    break;

                default:
                    throw new IllegalArgumentException("The method " + methodName.fullyQualifiedMethodName() + " to invoke was not expected");
            }
        } catch (WakeupException | InterruptException ex) {
            e = ex;
        }

        final Optional<KafkaException> error;

        if (e != null)
            error = Optional.of(ConsumerUtils.maybeWrapAsKafkaException(e, "User rebalance callback throws an error"));
        else
            error = Optional.empty();

        return new ConsumerRebalanceListenerCallbackCompletedEvent(methodName, future, error);
    }

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

    AsyncConsumerMetrics asyncConsumerMetrics() {
        return asyncConsumerMetrics;
    }

    // Visible for testing
    SubscriptionState subscriptions() {
        return subscriptions;
    }

    private long defaultApiTimeoutDeadlineMs() {
        return calculateDeadlineMs(time, defaultApiTimeoutMs);
    }
}
