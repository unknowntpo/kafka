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

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AsyncPollEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests that pin the desired contracts of the async consumer's background event loop.
 * Both tests FAIL on current trunk on purpose; they describe behaviour a redesign of the loop must provide.
 *
 * <h2>README</h2>
 * <table>
 *   <caption>Contracts pinned by this class</caption>
 *   <tr>
 *     <th>Test</th><th>JIRA</th><th>Behaviour at HEAD</th><th>Intended contract</th>
 *   </tr>
 *   <tr>
 *     <td>{@link #testMetadataErrorWakesParkedApplicationThread()}</td>
 *     <td>KAFKA-20397 (open; PR apache/kafka#21991 unmerged and only adds a pre-park check)</td>
 *     <td>{@code ConsumerNetworkThread.runOnce()} ends with {@code maybeFailOnMetadataError(...)}, which calls
 *         {@link AsyncPollEvent#onMetadataError(Exception)} but never {@link FetchBuffer#wakeup()}, so an
 *         application thread parked in {@link FetchBuffer#awaitWakeup(Timer)} sleeps until its timer expires.</td>
 *     <td>Any terminal outcome of the inflight poll event that the network thread produces (here: a metadata
 *         error) must wake the parked application thread immediately.</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #testIntervalAutoCommitDoesNotIncludeUndeliveredPositions()}</td>
 *     <td>KAFKA-18641 residual (see also KAFKA-18376)</td>
 *     <td>{@code ApplicationEventProcessor.process(AsyncPollEvent)} marks the reconciliation check complete and
 *         then calls {@code CommitRequestManager.updateTimerAndMaybeCommit(...)}, which reads
 *         {@code SubscriptionState.allConsumed()} on the background thread. When no reconciliation is pending,
 *         the application thread does not wait for that stage, so it may already have advanced positions via
 *         {@link FetchCollector#collectFetch(FetchBuffer)} for records that poll N has not yet returned.</td>
 *     <td>An interval auto-commit triggered by poll N commits only positions that a completed poll has
 *         returned to the application; positions advanced by poll N's own record collection are excluded.</td>
 *   </tr>
 * </table>
 */
@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class EventLoopContractRegressionTest {

    private static final String TOPIC = "topic";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);
    private static final long POLL_TIMEOUT_MS = 10_000L;
    private static final long WAKEUP_BOUND_MS = 500L;
    private static final int AUTO_COMMIT_INTERVAL_MS = 100;
    private static final int RECORD_COUNT = 10;

    private final Metrics metrics = new Metrics();
    private final FetchCollector<String, String> fetchCollector = mock(FetchCollector.class);
    private final ApplicationEventHandler applicationEventHandler = mock(ApplicationEventHandler.class);
    private final ConsumerMetadata metadata = mock(ConsumerMetadata.class);
    private final LinkedBlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
    private final CompletableEventReaper backgroundEventReaper = mock(CompletableEventReaper.class);
    private AsyncKafkaConsumer<String, String> consumer = null;

    @AfterEach
    public void tearDown() {
        backgroundEventQueue.clear();
        if (consumer != null) {
            try {
                consumer.close(CloseOptions.timeout(Duration.ZERO));
            } catch (Exception swallow) {
                // Best effort cleanup: the mocked ApplicationEventHandler does not answer close events.
            }
        }
        consumer = null;
        metrics.close();
        Mockito.framework().clearInlineMocks();
    }

    /**
     * KAFKA-20397: a metadata error raised on the network thread must wake an application thread that is
     * parked in {@link FetchBuffer#awaitWakeup(Timer)}.
     *
     * <p>Design. The consumer is built with a real (spied) {@link FetchBuffer}, {@link Time#SYSTEM}, a mocked
     * {@link ApplicationEventHandler} whose {@code maximumTimeToWait()} is {@code Long.MAX_VALUE}, and
     * {@code retryBackoffMs} equal to the poll timeout. The last point disables the bounded-wait heuristics in
     * {@code AsyncKafkaConsumer.pollForFetches()}, which would otherwise shorten the park to
     * {@code retry.backoff.ms}; the contract under test is that the wake-up comes from the error itself, not
     * from a periodic retry tick. {@code poll(10s)} runs on a separate thread. The spy counts down a latch when
     * {@code awaitWakeup} is entered and the test additionally waits for the thread to reach
     * {@code TIMED_WAITING}, so the error is injected only once the thread is really parked. The injection is
     * exactly what {@code ConsumerNetworkThread.maybeFailOnMetadataError} does: it calls
     * {@link AsyncPollEvent#onMetadataError(Exception)} on the inflight event and nothing else.
     *
     * <p>Why assert on {@code poll()} instead of on {@code awaitWakeup} returning: the user-visible contract is
     * that {@code poll()} surfaces the error promptly. On HEAD {@code poll()} returns
     * {@link ConsumerRecords#empty()} only after the full 10 s (the error is surfaced by the <em>next</em> poll),
     * which the assertion messages make explicit. The wake-up bound of 500 ms is generous
     * relative to the 10 s park, so the test is not timing sensitive in the passing case; in the failing case it
     * takes ~10 s by construction.
     */
    @Test
    public void testMetadataErrorWakesParkedApplicationThread() throws Exception {
        Time time = Time.SYSTEM;
        LogContext logContext = new LogContext();
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.NONE);
        FetchBuffer fetchBuffer = spy(new FetchBuffer(logContext));

        CountDownLatch enteredAwaitWakeup = new CountDownLatch(1);
        doAnswer(invocation -> {
            enteredAwaitWakeup.countDown();
            return invocation.callRealMethod();
        }).when(fetchBuffer).awaitWakeup(any(Timer.class));

        when(applicationEventHandler.maximumTimeToWait()).thenReturn(Long.MAX_VALUE);
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));

        consumer = newConsumer(time, fetchBuffer, subscriptions, POLL_TIMEOUT_MS);
        subscriptions.assignFromUser(Set.of(TP));
        subscriptions.seek(TP, 0);

        AtomicReference<Throwable> pollError = new AtomicReference<>();
        AtomicReference<ConsumerRecords<String, String>> pollResult = new AtomicReference<>();
        AtomicLong pollReturnedNanos = new AtomicLong();
        Thread pollThread = new Thread(() -> {
            try {
                pollResult.set(consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS)));
            } catch (Throwable t) {
                pollError.set(t);
            } finally {
                pollReturnedNanos.set(System.nanoTime());
            }
        }, "application-thread");
        pollThread.start();

        try {
            assertTrue(enteredAwaitWakeup.await(5, TimeUnit.SECONDS),
                "application thread never reached FetchBuffer.awaitWakeup");
            TestUtils.waitForCondition(() -> pollThread.getState() == Thread.State.TIMED_WAITING,
                "application thread did not park in FetchBuffer.awaitWakeup");

            ArgumentCaptor<AsyncPollEvent> eventCaptor = ArgumentCaptor.forClass(AsyncPollEvent.class);
            verify(applicationEventHandler).add(eventCaptor.capture());
            AsyncPollEvent inflightPoll = eventCaptor.getValue();
            assertFalse(inflightPoll.isComplete(), "inflight poll event must still be in progress");

            // Simulate ConsumerNetworkThread.maybeFailOnMetadataError(): it only notifies the event.
            KafkaException metadataError = new TopicAuthorizationException(Set.of(TOPIC));
            long errorRaisedNanos = System.nanoTime();
            inflightPoll.onMetadataError(metadataError);

            pollThread.join(POLL_TIMEOUT_MS + 5_000L);
            assertFalse(pollThread.isAlive(), "poll() did not return even after the poll timeout elapsed");

            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(pollReturnedNanos.get() - errorRaisedNanos);
            assertSame(metadataError, pollError.get(),
                "poll() must surface the metadata error raised while the application thread was parked, " +
                    "but it returned " + pollResult.get() + " (error=" + pollError.get() + ") after " +
                    elapsedMs + " ms");
            assertTrue(elapsedMs <= WAKEUP_BOUND_MS,
                "metadata error must wake the parked application thread within " + WAKEUP_BOUND_MS +
                    " ms, but poll() only returned after " + elapsedMs + " ms (poll timeout " +
                    POLL_TIMEOUT_MS + " ms)");
        } finally {
            if (pollThread.isAlive()) {
                pollThread.interrupt();
                pollThread.join(5_000L);
            }
        }
    }

    /**
     * KAFKA-18641 residual: an interval auto-commit triggered by poll N's {@link AsyncPollEvent} must not
     * include positions that poll N's own record collection advanced but has not yet returned to the
     * application.
     *
     * <p>Interleaving reproduced (single-threaded, fully deterministic): the application thread runs
     * {@link FetchCollector#collectFetch(FetchBuffer)} for poll N before the background thread processes poll
     * N's event. This is legal at HEAD because {@code AsyncKafkaConsumer.collectFetch()} only waits for the
     * reconciliation check when a reconciliation is pending, and the collect step advances
     * {@code SubscriptionState.position()} before the records are handed back. The background then processes
     * the event and, because the auto-commit interval has elapsed, snapshots {@code allConsumed()}.
     *
     * <p>Why committing 10 here loses records. The 10 records at offsets 0..9 are, at this instant, still
     * inside poll N: they have been pulled out of the buffer but {@code poll()} has not returned them and the
     * application has not processed them. If the auto-commit of offset 10 is sent and the process then dies
     * (crash, kill, or a rebalance that revokes the partition) before the application processes those records,
     * the group resumes from 10 and offsets 0..9 are never delivered to anyone. That is data loss under the
     * at-least-once guarantee that auto-commit documents: only offsets of records returned by a completed
     * {@code poll()} may be committed. KAFKA-18641 fixed one ordering of this race and KAFKA-18376 introduced
     * the non-blocking {@link AsyncPollEvent} that reopened this interleaving; the reconciliation-check gate
     * added afterwards only covers the pending-reconciliation case.
     *
     * <p>Expected on HEAD: FAILS, the OffsetCommitRequest carries offset 10. Desired: offset 0, the position as
     * of the last completed poll.
     */
    @Test
    public void testIntervalAutoCommitDoesNotIncludeUndeliveredPositions() {
        MockTime time = new MockTime(0);
        LogContext logContext = new LogContext();
        SubscriptionState subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        subscriptions.assignFromUser(Set.of(TP));
        subscriptions.seek(TP, 0);
        assertEquals(0L, subscriptions.position(TP).offset);

        ConsumerConfig config = autoCommitConfig();

        CoordinatorRequestManager coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "host1", 9092)));
        CommitRequestManager commitRequestManager = new CommitRequestManager(
            time,
            logContext,
            subscriptions,
            config,
            coordinatorRequestManager,
            mock(OffsetCommitCallbackInvoker.class),
            "group-id",
            Optional.empty(),
            metrics,
            metadata);

        OffsetsRequestManager offsetsRequestManager = mock(OffsetsRequestManager.class);
        when(offsetsRequestManager.updateFetchPositions(anyLong()))
            .thenReturn(CompletableFuture.completedFuture(null));
        FetchRequestManager fetchRequestManager = mock(FetchRequestManager.class);
        when(fetchRequestManager.createFetchRequests()).thenReturn(CompletableFuture.completedFuture(null));
        RequestManagers requestManagers = new RequestManagers(
            logContext,
            offsetsRequestManager,
            mock(TopicMetadataRequestManager.class),
            fetchRequestManager,
            Optional.of(coordinatorRequestManager),
            Optional.of(commitRequestManager),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
        ApplicationEventProcessor processor = new ApplicationEventProcessor(
            logContext,
            requestManagers,
            metadata,
            subscriptions);

        FetchMetricsManager fetchMetricsManager = mock(FetchMetricsManager.class);
        FetchCollector<String, String> realFetchCollector = new FetchCollector<>(
            logContext,
            metadata,
            subscriptions,
            new FetchConfig(config),
            new Deserializers<>(new StringDeserializer(), new StringDeserializer(), metrics),
            fetchMetricsManager,
            time);
        FetchBuffer fetchBuffer = new FetchBuffer(logContext);
        fetchBuffer.add(completedFetch(logContext, subscriptions, fetchMetricsManager));

        // The auto-commit interval has elapsed by the time poll N starts.
        time.sleep(AUTO_COMMIT_INTERVAL_MS);

        // (1) Poll N is submitted by the application thread.
        // Poll N-1 is processed after the auto-commit interval elapsed. It carries no snapshot, so the
        // background must not commit from live positions; it asks the application thread for a snapshot.
        processor.process(new AsyncPollEvent(time.milliseconds() + 1_000L, time.milliseconds()));
        assertTrue(commitRequestManager.autoCommitSnapshotRequested().get(),
            "an interval auto-commit that is due must request a snapshot from the application thread");
        // The application thread enters poll N and captures positions before collecting any record,
        // exactly as AsyncKafkaConsumer.checkInflightPoll does.
        AsyncPollEvent pollN = new AsyncPollEvent(time.milliseconds() + 1_000L, time.milliseconds(),
            subscriptions.allConsumed());

        // (2) Application thread collects poll N's records first; this advances the position to 10, but the
        //     records have not been returned from poll() yet.
        Fetch<String, String> fetch = realFetchCollector.collectFetch(fetchBuffer);
        assertEquals(RECORD_COUNT, fetch.numRecords());
        assertEquals(RECORD_COUNT, subscriptions.position(TP).offset);

        // (3) Background thread processes poll N, which triggers the interval auto-commit.
        processor.process(pollN);
        assertTrue(pollN.isComplete(), "poll event should complete with mocked positions/fetch stages");

        // (4) Read the OffsetCommitRequest that the background thread would send next.
        NetworkClientDelegate.PollResult result = commitRequestManager.poll(time.milliseconds());
        assertEquals(1, result.unsentRequests.size(), "interval auto-commit should have produced one request");
        OffsetCommitRequestData data =
            (OffsetCommitRequestData) result.unsentRequests.get(0).requestBuilder().build().data();
        assertEquals(1, data.topics().size());
        assertEquals(TOPIC, data.topics().get(0).name());
        assertEquals(1, data.topics().get(0).partitions().size());
        OffsetCommitRequestData.OffsetCommitRequestPartition partition = data.topics().get(0).partitions().get(0);
        assertEquals(TP.partition(), partition.partitionIndex());
        assertEquals(0L, partition.committedOffset(),
            "interval auto-commit triggered by poll N must commit the position as of the last completed poll " +
                "(0), not the position advanced by poll N's own, not yet returned, records (" + RECORD_COUNT + ")");
    }

    private AsyncKafkaConsumer<String, String> newConsumer(Time time,
                                                           FetchBuffer fetchBuffer,
                                                           SubscriptionState subscriptions,
                                                           long retryBackoffMs) {
        LogContext logContext = new LogContext();
        return new AsyncKafkaConsumer<>(
            logContext,
            "client-id",
            new Deserializers<>(new StringDeserializer(), new StringDeserializer(), metrics),
            fetchBuffer,
            fetchCollector,
            mock(FetchMetricsManager.class),
            mock(RebalanceCallbackMetricsManager.class),
            new ConsumerInterceptors<>(Collections.emptyList(), metrics),
            time,
            applicationEventHandler,
            backgroundEventQueue,
            backgroundEventReaper,
            mock(ConsumerRebalanceListenerInvoker.class),
            metrics,
            subscriptions,
            metadata,
            retryBackoffMs,
            30_000,
            1_000,
            "group-id",
            false,
            new PositionsValidator(logContext, time, subscriptions, metadata));
    }

    private static ConsumerConfig autoCommitConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, AUTO_COMMIT_INTERVAL_MS);
        return new ConsumerConfig(props);
    }

    /**
     * Builds one {@link CompletedFetch} for {@link #TP} at fetch offset 0 holding 10
     * records, mirroring {@code FetchCollectorTest.CompletedFetchBuilder}.
     */
    private static CompletedFetch completedFetch(LogContext logContext,
                                                 SubscriptionState subscriptions,
                                                 FetchMetricsManager fetchMetricsManager) {
        FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
            .setPartitionIndex(TP.partition())
            .setHighWatermark(1_000L)
            .setRecords(records());
        return new CompletedFetch(
            logContext.logger(CompletedFetch.class),
            subscriptions,
            BufferSupplier.create(),
            TP,
            partitionData,
            new FetchMetricsAggregator(fetchMetricsManager, Set.of(TP)),
            0L);
    }

    private static Records records() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer,
            Compression.NONE,
            TimestampType.CREATE_TIME,
            0L)) {
            for (int i = 0; i < RECORD_COUNT; i++)
                builder.append(0L, "key".getBytes(), ("value-" + i).getBytes());
            return builder.build();
        }
    }
}
