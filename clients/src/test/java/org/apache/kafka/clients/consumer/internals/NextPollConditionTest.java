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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NextPollConditionTest {
    @Test
    public void publicationBeforeRegistrationIsRetained() {
        NextPollCondition.Signal signal = new NextPollCondition.Signal();
        NextPollCondition wait = signal.await();
        signal.publish();
        assertTrue(wait.isReady(0));
        assertEquals(0, wait.remainingMs(0));
        assertFalse(signal.await().isReady(0));
    }

    @Test
    public void unrelatedSignalsDoNotWakeWaiters() {
        NextPollCondition.Signal own = new NextPollCondition.Signal();
        NextPollCondition wait = own.await();
        new NextPollCondition.Signal().publish();
        assertFalse(wait.isReady(100));
        own.publish();
        own.publish();
        assertTrue(wait.isReady(100));
    }

    @Test
    public void nestedConditionsPreserveEarliestDeadline() {
        NextPollCondition.Signal signal = new NextPollCondition.Signal();
        NextPollCondition condition = NextPollCondition.anyOf(NextPollCondition.after(10, 100),
                NextPollCondition.anyOf(signal.await(), NextPollCondition.after(10, 200)));
        assertEquals(60, condition.remainingMs(50));
        assertFalse(condition.isReady(109));
        assertTrue(condition.isReady(110));
        signal.publish();
        assertEquals(0, condition.remainingMs(50));
    }

    @Test
    public void deadlinesSaturateAndRejectNegativeDelays() {
        assertEquals(1, NextPollCondition.after(Long.MAX_VALUE - 1, 100).remainingMs(Long.MAX_VALUE - 1));
        assertEquals(Long.MAX_VALUE, NextPollCondition.after(0, Long.MAX_VALUE).remainingMs(-1));
        assertThrows(IllegalArgumentException.class, () -> NextPollCondition.after(0, -1));
    }

    @Test
    public void laterManagerPublicationPreventsNetworkSleep() {
        NextPollCondition.Signal signal = new NextPollCondition.Signal();
        RequestManager manager = now -> new NetworkClientDelegate.PollResult(Long.MAX_VALUE,
                Collections.emptyList(), signal.await());
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        assertNotNull(scheduler.poll(manager, 0));
        assertNull(scheduler.poll(manager, 1));
        signal.publish();
        assertEquals(0, scheduler.remainingMs(1));
        assertNotNull(scheduler.poll(manager, 1));
        assertNull(scheduler.poll(manager, 1));
    }

    @Test
    public void legacyManagersStillPollEveryPass() {
        int[] calls = {0};
        RequestManager manager = now -> {
            calls[0]++;
            return new NetworkClientDelegate.PollResult(100);
        };
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        scheduler.poll(manager, 0);
        scheduler.poll(manager, 1);
        assertEquals(2, calls[0]);
        assertEquals(Long.MAX_VALUE, scheduler.remainingMs(1));
    }

    @Test
    public void completionWakesCoordinatorAndBackoffDoesNotSlide() {
        CoordinatorRequestManager manager = new CoordinatorRequestManager(new LogContext(), 100, 100, "group");
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        NetworkClientDelegate.PollResult first = scheduler.poll(manager, 0);
        assertEquals(1, first.unsentRequests.size());
        assertNull(scheduler.poll(manager, 1));
        first.unsentRequests.get(0).handler().onFailure(10, new TimeoutException("test"));
        assertEquals(0, scheduler.remainingMs(10));
        NetworkClientDelegate.PollResult retry = scheduler.poll(manager, 10);
        assertTrue(retry.unsentRequests.isEmpty());
        long deadline = 10 + retry.timeUntilNextPollMs;
        assertNull(scheduler.poll(manager, deadline - 1));
        assertEquals(1, scheduler.remainingMs(deadline - 1));
        assertEquals(1, scheduler.poll(manager, deadline).unsentRequests.size());
        assertNull(scheduler.poll(manager, deadline));
    }

    @Test
    public void closeDuringBackoffCancelsDeadlineWithoutAnotherDiscovery() {
        CoordinatorRequestManager manager = new CoordinatorRequestManager(new LogContext(), 100, 100, "group");
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        NetworkClientDelegate.PollResult first = scheduler.poll(manager, 0);
        first.unsentRequests.get(0).handler().onFailure(10, new TimeoutException("test"));
        NetworkClientDelegate.PollResult retry = scheduler.poll(manager, 10);
        long deadline = 10 + retry.timeUntilNextPollMs;
        assertTrue(deadline > 11);
        assertNull(scheduler.poll(manager, 11));

        manager.signalClose();
        assertEquals(0, scheduler.remainingMs(11));
        NetworkClientDelegate.PollResult closed = scheduler.poll(manager, 11);
        assertNotNull(closed);
        assertTrue(closed.unsentRequests.isEmpty());
        assertEquals(Long.MAX_VALUE, scheduler.remainingMs(11));
        assertNull(scheduler.poll(manager, deadline));
        assertTrue(manager.pollOnClose(deadline).unsentRequests.isEmpty());
        scheduler.close();
    }

    @Test
    public void coordinatorCompletionActivatesOnlyDependentsAfterStateUpdate() {
        CoordinatorRequestManager coordinator = new CoordinatorRequestManager(new LogContext(), 0, 0, "group");
        List<Boolean> observations = new ArrayList<>();
        NextPollCondition.Signal unrelatedInput = new NextPollCondition.Signal();
        int[] unrelatedCalls = {0};
        RequestManager dependent = now -> {
            NextPollCondition changed = coordinator.stateChanged();
            observations.add(coordinator.coordinator().isPresent());
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), changed);
        };
        RequestManager unrelated = now -> {
            unrelatedCalls[0]++;
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), unrelatedInput.await());
        };
        List<NetworkClientDelegate.UnsentRequest> requests = new ArrayList<>();
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            scheduler.registerManagers(Arrays.asList(dependent, coordinator, unrelated));
            scheduler.pollReady(0, result -> {
                requests.addAll(result.unsentRequests);
                return result.timeUntilNextPollMs;
            });
            assertEquals(Collections.singletonList(false), observations);
            assertEquals(1, requests.size());
            for (int now = 1; now < 100; now++)
                scheduler.pollReady(now, result -> result.timeUntilNextPollMs);
            assertEquals(Collections.singletonList(false), observations);

            NetworkClientDelegate.UnsentRequest request = requests.get(0);
            Node node = new Node(1, "localhost", 9092);
            request.handler().onComplete(new ClientResponse(
                    new RequestHeader(ApiKeys.FIND_COORDINATOR, request.requestBuilder().build().version(), "", 1),
                    request.handler(), node.idString(), 0, 100, false, null, null,
                    FindCoordinatorResponse.prepareResponse(Errors.NONE, "group", node)));
            assertTrue(coordinator.coordinator().isPresent());
            assertEquals(Collections.singletonList(false), observations);
            assertEquals(0, scheduler.remainingMs(100));
            scheduler.pollReady(100, result -> {
                assertTrue(result.unsentRequests.isEmpty());
                return result.timeUntilNextPollMs;
            });
            assertEquals(Arrays.asList(false, true), observations);
            assertEquals(1, unrelatedCalls[0]);
            assertEquals(Long.MAX_VALUE, scheduler.remainingMs(100));
        }
    }

    @Test
    public void lateCoordinatorCompletionDoesNotActivateNewRegistrationAfterClose() {
        CoordinatorRequestManager coordinator = new CoordinatorRequestManager(new LogContext(), 0, 0, "group");
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        NetworkClientDelegate.UnsentRequest oldRequest = scheduler.poll(coordinator, 0).unsentRequests.get(0);
        scheduler.close();
        NextPollCondition.Signal newInput = new NextPollCondition.Signal();
        int[] newCalls = {0};
        RequestManager replacement = now -> {
            newCalls[0]++;
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), newInput.await());
        };
        scheduler.poll(replacement, 1);
        oldRequest.handler().onFailure(2, new TimeoutException("late response"));
        assertEquals(Long.MAX_VALUE, scheduler.remainingMs(2));
        assertNull(scheduler.poll(replacement, 2));
        assertEquals(1, newCalls[0]);
        newInput.publish();
        assertNotNull(scheduler.poll(replacement, 3));
        assertEquals(2, newCalls[0]);
        scheduler.close();
    }

    @Test
    public void invalidationWakesKnownCoordinator() {
        CoordinatorRequestManager manager = new CoordinatorRequestManager(new LogContext(), 0, 0, "group");
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        NetworkClientDelegate.UnsentRequest request = scheduler.poll(manager, 0).unsentRequests.get(0);
        Node node = new Node(1, "localhost", 9092);
        request.handler().onComplete(new ClientResponse(
                new RequestHeader(ApiKeys.FIND_COORDINATOR, request.requestBuilder().build().version(), "", 1),
                request.handler(), node.idString(), 0, 1, false, null, null,
                FindCoordinatorResponse.prepareResponse(Errors.NONE, "group", node)));
        assertTrue(manager.coordinator().isPresent());
        assertEquals(0, scheduler.remainingMs(1));
        assertTrue(scheduler.poll(manager, 1).unsentRequests.isEmpty());
        assertNull(scheduler.poll(manager, 1));
        manager.markCoordinatorUnknown("test", 2);
        assertEquals(0, scheduler.remainingMs(2));
        assertEquals(1, scheduler.poll(manager, 2).unsentRequests.size());
    }

    @Test
    public void closeDoesNotAdmitAnotherDiscovery() {
        CoordinatorRequestManager manager = new CoordinatorRequestManager(new LogContext(), 0, 0, "group");
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        scheduler.poll(manager, 0);
        manager.signalClose();
        assertTrue(scheduler.poll(manager, 1).unsentRequests.isEmpty());
        assertTrue(manager.pollOnClose(1).unsentRequests.isEmpty());
    }

    @Test
    public void idlePassesNeverEvaluateWaitingConditions() {
        NextPollCondition.Signal signal = new NextPollCondition.Signal();
        int[] polls = {0};
        RequestManager manager = now -> {
            polls[0]++;
            NextPollCondition input = signal.await();
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(),
                    new NextPollCondition() {
                        @Override
                        public void register(Registration registration) {
                            input.register(registration);
                        }

                        @Override
                        public boolean isReady(long nowMs) {
                            throw new AssertionError("Scheduler must not scan conditions");
                        }

                        @Override
                        public long remainingMs(long nowMs) {
                            throw new AssertionError("Scheduler must use its deadline queue");
                        }
                    });
        };
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        scheduler.registerManagers(Collections.singletonList(manager));
        for (int i = 0; i < 100; i++)
            scheduler.pollReady(i, result -> result.timeUntilNextPollMs);
        assertEquals(1, polls[0]);
        assertEquals(1, signal.subscriberCount());
        signal.publish();
        signal.publish();
        assertEquals(0, signal.subscriberCount());
        assertEquals(0, scheduler.remainingMs(100));
        scheduler.pollReady(100, result -> result.timeUntilNextPollMs);
        assertEquals(2, polls[0]);
        assertEquals(1, signal.subscriberCount());
    }

    @Test
    public void publicationBeforeArmEnqueuesExactlyOnce() {
        NextPollCondition.Signal signal = new NextPollCondition.Signal();
        NextPollCondition captured = signal.await();
        signal.publish();
        int[] polls = {0};
        RequestManager manager = now -> new NetworkClientDelegate.PollResult(Long.MAX_VALUE,
                Collections.emptyList(), polls[0]++ == 0 ? captured : signal.await());
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        scheduler.registerManagers(Collections.singletonList(manager));
        scheduler.pollReady(0, result -> result.timeUntilNextPollMs);
        assertEquals(1, polls[0]);
        assertEquals(0, scheduler.remainingMs(0));
        scheduler.pollReady(0, result -> result.timeUntilNextPollMs);
        assertEquals(2, polls[0]);
        assertEquals(Long.MAX_VALUE, scheduler.remainingMs(0));
    }

    @Test
    public void anyOfNotificationRemovesOtherSubscriptionsAndTimer() {
        NextPollCondition.Signal first = new NextPollCondition.Signal();
        NextPollCondition.Signal second = new NextPollCondition.Signal();
        int[] polls = {0};
        RequestManager manager = now -> new NetworkClientDelegate.PollResult(Long.MAX_VALUE,
                Collections.emptyList(), polls[0]++ == 0
                        ? NextPollCondition.anyOf(first.await(), NextPollCondition.anyOf(second.await(),
                                NextPollCondition.after(now, 10))) : first.await());
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        scheduler.registerManagers(Collections.singletonList(manager));
        scheduler.pollReady(0, result -> result.timeUntilNextPollMs);
        assertEquals(10, scheduler.remainingMs(0));
        second.publish();
        assertEquals(0, first.subscriberCount());
        assertEquals(0, second.subscriberCount());
        scheduler.pollReady(1, result -> result.timeUntilNextPollMs);
        assertEquals(Long.MAX_VALUE, scheduler.remainingMs(10));
        assertEquals(2, polls[0]);
        scheduler.close();
        assertEquals(0, first.subscriberCount());
        first.publish();
        assertEquals(Long.MAX_VALUE, scheduler.remainingMs(10));
    }

    @Test
    public void readySnapshotPreservesOrderAndDefersReentrantNotification() {
        NextPollCondition.Signal first = new NextPollCondition.Signal();
        NextPollCondition.Signal second = new NextPollCondition.Signal();
        List<String> order = new ArrayList<>();
        RequestManager a = now -> {
            order.add("a");
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), first.await());
        };
        RequestManager b = now -> {
            order.add("b");
            first.publish();
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), second.await());
        };
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        scheduler.registerManagers(Arrays.asList(a, b));
        scheduler.pollReady(0, result -> result.timeUntilNextPollMs);
        assertEquals(Arrays.asList("a", "b"), order);
        scheduler.pollReady(1, result -> result.timeUntilNextPollMs);
        assertEquals(Arrays.asList("a", "b", "a"), order);
        order.clear();
        second.publish();
        first.publish();
        scheduler.pollReady(2, result -> result.timeUntilNextPollMs);
        assertEquals(Arrays.asList("a", "b"), order);
    }

    @Test
    public void throwingManagerDoesNotLoseUnprocessedReadyEntries() {
        int[] calls = {0};
        RequestManager a = now -> {
            if (calls[0]++ == 0)
                throw new IllegalStateException("test");
            return NetworkClientDelegate.PollResult.EMPTY;
        };
        int[] second = {0};
        RequestManager b = now -> {
            second[0]++;
            return NetworkClientDelegate.PollResult.EMPTY;
        };
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        scheduler.registerManagers(Arrays.asList(a, b));
        assertThrows(IllegalStateException.class,
                () -> scheduler.pollReady(0, result -> result.timeUntilNextPollMs));
        scheduler.pollReady(1, result -> result.timeUntilNextPollMs);
        assertEquals(2, calls[0]);
        assertEquals(1, second[0]);
    }

    @Test
    public void timerQueueActivatesOnlyDueManagersAlongsideLegacyPolling() {
        int[] calls = {0, 0, 0};
        NextPollCondition.Signal signal = new NextPollCondition.Signal();
        RequestManager first = now -> new NetworkClientDelegate.PollResult(Long.MAX_VALUE,
                Collections.emptyList(), calls[0]++ == 0 ? NextPollCondition.after(now, 10) : signal.await());
        RequestManager second = now -> new NetworkClientDelegate.PollResult(Long.MAX_VALUE,
                Collections.emptyList(), calls[1]++ == 0 ? NextPollCondition.after(now, 20) : signal.await());
        RequestManager legacy = now -> {
            calls[2]++;
            return NetworkClientDelegate.PollResult.EMPTY;
        };
        RequestManagerScheduler scheduler = new RequestManagerScheduler();
        scheduler.registerManagers(Arrays.asList(first, second, legacy));
        assertEquals(10, scheduler.pollReady(0, result -> result.timeUntilNextPollMs));
        assertEquals(1, scheduler.pollReady(9, result -> result.timeUntilNextPollMs));
        assertEquals(10, scheduler.pollReady(10, result -> result.timeUntilNextPollMs));
        assertEquals(2, calls[0]);
        assertEquals(1, calls[1]);
        assertEquals(Long.MAX_VALUE, scheduler.pollReady(20, result -> result.timeUntilNextPollMs));
        assertEquals(2, calls[1]);
        assertEquals(4, calls[2]);
    }

    @Test
    public void signalPublicationSkipsCancelledSiblingAndDefersNewSubscription() {
        NextPollCondition.Signal signal = new NextPollCondition.Signal();
        int[] calls = {0, 0, 0};
        Runnable[] cancelSecond = {null};
        Runnable first = signal.subscribe(0, () -> {
            calls[0]++;
            cancelSecond[0].run();
            signal.subscribe(1, () -> calls[2]++);
        });
        cancelSecond[0] = signal.subscribe(0, () -> calls[1]++);
        signal.publish();
        assertEquals(1, calls[0]);
        assertEquals(0, calls[1]);
        assertEquals(0, calls[2]);
        assertEquals(1, signal.subscriberCount());
        first.run();
        cancelSecond[0].run();
        assertEquals(1, signal.subscriberCount());
        signal.publish();
        assertEquals(1, calls[2]);
        assertEquals(0, signal.subscriberCount());
    }

    @Test
    public void cancellationUnlinksHeadMiddleAndTailWithoutAffectingOtherOwners() {
        NextPollCondition.Signal signal = new NextPollCondition.Signal();
        int[] calls = {0};
        Runnable a = signal.subscribe(0, () -> calls[0]++);
        Runnable b = signal.subscribe(0, () -> calls[0]++);
        Runnable c = signal.subscribe(0, () -> calls[0]++);
        Runnable d = signal.subscribe(0, () -> calls[0]++);
        b.run();
        a.run();
        d.run();
        b.run();
        assertEquals(1, signal.subscriberCount());
        signal.publish();
        assertEquals(1, calls[0]);
        c.run();
        assertEquals(0, signal.subscriberCount());
    }
    @Test
    public void mergedSnapshotPreservesInterleavedOrderAndDefersNewlyReadyManager() {
        List<Integer> order = new ArrayList<>();
        NextPollCondition.Signal signal = new NextPollCondition.Signal();
        RequestManager first = now -> {
            order.add(0);
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), signal.await());
        };
        RequestManager legacy = now -> {
            order.add(1);
            signal.publish();
            return NetworkClientDelegate.PollResult.EMPTY;
        };
        RequestManager timer = now -> {
            order.add(2);
            return new NetworkClientDelegate.PollResult(10, Collections.emptyList(), NextPollCondition.after(now, 10));
        };
        RequestManager last = now -> {
            order.add(3);
            return NetworkClientDelegate.PollResult.EMPTY;
        };
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            scheduler.registerManagers(Arrays.asList(first, legacy, timer, last));
            scheduler.pollReady(0, result -> result.timeUntilNextPollMs);
            order.clear();
            scheduler.pollReady(1, result -> result.timeUntilNextPollMs);
            assertEquals(Arrays.asList(0, 1, 3), order);
            // Consume the pending notification without publishing another one.
            scheduler.poll(first, 2);
            order.clear();
            assertEquals(0, scheduler.pollReady(10, result -> result.timeUntilNextPollMs));
            assertEquals(Arrays.asList(1, 2, 3), order);
            order.clear();
            scheduler.pollReady(11, result -> result.timeUntilNextPollMs);
            assertEquals(Arrays.asList(0, 1, 3), order);
        }
    }

    @Test
    public void failedLegacyManagerInBothSetsIsPolledOnceOnRetry() {
        List<Integer> order = new ArrayList<>();
        int[] calls = {0};
        RequestManager first = now -> {
            order.add(0);
            if (++calls[0] == 2)
                throw new IllegalStateException("test");
            return NetworkClientDelegate.PollResult.EMPTY;
        };
        RequestManager second = now -> {
            order.add(1);
            return NetworkClientDelegate.PollResult.EMPTY;
        };
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            scheduler.registerManagers(Arrays.asList(first, second));
            scheduler.pollReady(0, result -> result.timeUntilNextPollMs);
            assertThrows(IllegalStateException.class,
                    () -> scheduler.pollReady(1, result -> result.timeUntilNextPollMs));
            order.clear();
            scheduler.pollReady(2, result -> result.timeUntilNextPollMs);
            assertEquals(Arrays.asList(0, 1), order);
            assertEquals(3, calls[0]);
        }
    }

    @Test
    public void legacyManagerCanSwitchToDeadlineAndBack() {
        int[] calls = {0};
        RequestManager manager = now -> ++calls[0] == 2
                ? new NetworkClientDelegate.PollResult(10, Collections.emptyList(), NextPollCondition.after(now, 10))
                : NetworkClientDelegate.PollResult.EMPTY;
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            scheduler.registerManagers(Collections.singletonList(manager));
            scheduler.pollReady(0, result -> result.timeUntilNextPollMs);
            assertEquals(10, scheduler.pollReady(1, result -> result.timeUntilNextPollMs));
            assertEquals(1, scheduler.pollReady(10, result -> result.timeUntilNextPollMs));
            assertEquals(2, calls[0]);
            scheduler.pollReady(11, result -> result.timeUntilNextPollMs);
            scheduler.pollReady(12, result -> result.timeUntilNextPollMs);
            assertEquals(4, calls[0]);
        }
    }

    @Test
    public void readyManagersAcrossBitmapWordsPreserveOrder() {
        List<Integer> order = new ArrayList<>();
        List<NextPollCondition.Signal> signals = new ArrayList<>();
        List<RequestManager> managers = new ArrayList<>();
        for (int i = 0; i < 130; i++) {
            final int id = i;
            NextPollCondition.Signal signal = new NextPollCondition.Signal();
            signals.add(signal);
            managers.add(now -> {
                order.add(id);
                return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), signal.await());
            });
        }
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            scheduler.registerManagers(managers);
            scheduler.pollReady(0, result -> result.timeUntilNextPollMs);
            assertEquals(130, order.size());
            order.clear();
            for (int id : Arrays.asList(129, 64, 0, 127, 63, 64))
                signals.get(id).publish();
            scheduler.pollReady(1, result -> result.timeUntilNextPollMs);
            assertEquals(Arrays.asList(0, 63, 64, 127, 129), order);
            assertEquals(Long.MAX_VALUE, scheduler.remainingMs(1));
        }
    }

    @Test
    public void notificationIntoLaterBitmapWordWaitsForNextSnapshot() {
        List<Integer> order = new ArrayList<>();
        List<RequestManager> managers = new ArrayList<>();
        NextPollCondition.Signal first = new NextPollCondition.Signal();
        NextPollCondition.Signal last = new NextPollCondition.Signal();
        for (int i = 0; i < 130; i++) {
            final int id = i;
            NextPollCondition.Signal signal = i == 0 ? first : i == 129 ? last : new NextPollCondition.Signal();
            managers.add(now -> {
                order.add(id);
                if (id == 0 && now > 0)
                    last.publish();
                return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), signal.await());
            });
        }
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            scheduler.registerManagers(managers);
            scheduler.pollReady(0, result -> result.timeUntilNextPollMs);
            order.clear();
            first.publish();
            assertEquals(0, scheduler.pollReady(1, result -> result.timeUntilNextPollMs));
            assertEquals(Collections.singletonList(0), order);
            order.clear();
            scheduler.pollReady(2, result -> result.timeUntilNextPollMs);
            assertEquals(Collections.singletonList(129), order);
        }
    }

    @Test
    public void closeClearsAllBitmapWordsAndRegistrationOrder() {
        int[] oldCalls = {0};
        NextPollCondition.Signal signal = new NextPollCondition.Signal();
        List<RequestManager> managers = new ArrayList<>();
        for (int i = 0; i < 130; i++) {
            managers.add(now -> {
                oldCalls[0]++;
                return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), signal.await());
            });
        }
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            scheduler.registerManagers(managers);
            scheduler.pollReady(0, result -> result.timeUntilNextPollMs);
            assertEquals(130, signal.subscriberCount());
            scheduler.close();
            assertEquals(0, signal.subscriberCount());
            signal.publish();
            assertEquals(Long.MAX_VALUE, scheduler.remainingMs(1));
            int[] newCalls = {0};
            scheduler.registerManagers(Collections.singletonList(now -> {
                newCalls[0]++;
                return NetworkClientDelegate.PollResult.EMPTY;
            }));
            scheduler.pollReady(2, result -> result.timeUntilNextPollMs);
            assertEquals(1, newCalls[0]);
            assertEquals(130, oldCalls[0]);
        }
    }

    @Test
    public void failedLegacyAtBitmapBoundaryRemainsUniqueOnRetry() {
        List<Integer> order = new ArrayList<>();
        List<RequestManager> managers = new ArrayList<>();
        int[] boundaryCalls = {0};
        for (int i = 0; i < 66; i++) {
            final int id = i;
            managers.add(now -> {
                order.add(id);
                if (id == 64 && ++boundaryCalls[0] == 2)
                    throw new IllegalStateException("test");
                return NetworkClientDelegate.PollResult.EMPTY;
            });
        }
        try (RequestManagerScheduler scheduler = new RequestManagerScheduler()) {
            scheduler.registerManagers(managers);
            scheduler.pollReady(0, result -> result.timeUntilNextPollMs);
            assertThrows(IllegalStateException.class,
                    () -> scheduler.pollReady(1, result -> result.timeUntilNextPollMs));
            order.clear();
            scheduler.pollReady(2, result -> result.timeUntilNextPollMs);
            List<Integer> expected = new ArrayList<>();
            for (int i = 0; i < 66; i++)
                expected.add(i);
            assertEquals(expected, order);
            assertEquals(3, boundaryCalls[0]);
        }
    }

}
