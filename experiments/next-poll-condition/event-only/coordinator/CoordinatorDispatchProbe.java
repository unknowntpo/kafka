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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Bounded dispatch-count experiment; transport is nonblocking, not a broker. */
public final class CoordinatorDispatchProbe {
    private static NetworkClientDelegate.PollResult waiting(NextPollCondition condition) {
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), condition);
    }

    public static void main(String[] args) {
        for (int count : new int[] {8, 32, 128})
            verifyDispatch(count);
        verifyRetry();
        verifyPublicationBeforeRegistration();
        System.out.println("PASS coordinator callback, dependency, invalidation, retry and pre-registration publication");
    }

    private static void verifyDispatch(int count) {
        CoordinatorRequestManager coordinator = new CoordinatorRequestManager(new LogContext(), 0, 0, "dispatch");
        int[] calls = new int[count];
        int[] discoveries = {0};
        List<RequestManager> managers = new ArrayList<>();
        managers.add(now -> { calls[0]++; return coordinator.poll(now); });
        managers.add(now -> {
            calls[1]++;
            // Capture the generation BEFORE reading the state being awaited.
            NextPollCondition changed = coordinator.stateChanged();
            if (coordinator.coordinator().isPresent())
                discoveries[0]++;
            return waiting(changed);
        });
        for (int i = 2; i < count; i++) {
            int index = i;
            NextPollCondition.Signal signal = new NextPollCondition.Signal();
            managers.add(new RequestManager() {
                @Override
                public NetworkClientDelegate.PollResult poll(long now) {
                    calls[index]++;
                    return waiting(signal.await());
                }

                @Override
                public long maximumTimeToWait(long now) {
                    throw new AssertionError("Event loop must not scan manager maximumTimeToWait");
                }
            });
        }
        try (NetworkLoopFixture f = new NetworkLoopFixture(managers)) {
            f.transport.captureRequests = true;
            f.runOnce();
            for (int i = 0; i < 1000; i++)
                f.runOnce();
            for (int c : calls)
                assertEquals(1, c, "Waiting manager must not be polled again");
            NetworkClientDelegate.UnsentRequest request = f.transport.requests.get(0);
            f.transport.completion = () -> request.handler().onComplete(new ClientResponse(
                    new RequestHeader(ApiKeys.FIND_COORDINATOR, request.requestBuilder().build().version(), "", 1),
                    request.handler(), "1", f.clock.now - 1, f.clock.now, false, null, null,
                    FindCoordinatorResponse.prepareResponse(Errors.NONE, "dispatch", new Node(1, "localhost", 9092))));
            f.runOnce(); // Deliver the real request handler callback during transport polling.
            assertTrue(coordinator.coordinator().isPresent());
            assertEquals(1, calls[1], "No recursive manager execution inside callback");
            f.runOnce();
            assertEquals(2, calls[0]);
            assertEquals(2, calls[1]);
            assertEquals(1, discoveries[0]);
            for (int i = 2; i < count; i++)
                assertEquals(1, calls[i], "Unrelated manager must stay asleep");
            assertEquals(1, f.transport.admitted);
            f.enqueue(() -> coordinator.markCoordinatorUnknown("probe", f.clock.now));
            f.runOnce();
            assertEquals(3, calls[0]);
            assertEquals(3, calls[1]);
            assertEquals(2, f.transport.admitted);
            assertEquals(1, f.transport.wakeups);
            for (int i = 2; i < count; i++)
                assertEquals(1, calls[i]);
            System.out.println("DISPATCH," + count + ",1000,0,2,0,2");
        }
    }

    private static void verifyRetry() {
        CoordinatorRequestManager coordinator = new CoordinatorRequestManager(new LogContext(), 100, 100, "dispatch");
        int[] calls = {0};
        RequestManager counted = now -> { calls[0]++; return coordinator.poll(now); };
        try (NetworkLoopFixture f = new NetworkLoopFixture(Collections.singletonList(counted))) {
            f.transport.captureRequests = true;
            f.runOnce();
            NetworkClientDelegate.UnsentRequest request = f.transport.requests.get(0);
            f.transport.completion = () -> request.handler().onFailure(f.clock.now, new TimeoutException("probe"));
            f.runOnce();
            f.runOnce();
            assertEquals(2, calls[0]);
            long remaining = f.transport.waitMs;
            assertTrue(remaining > 1 && remaining <= 100);
            f.clock.now += remaining - 2;
            f.runOnce();
            assertEquals(2, calls[0]);
            assertEquals(1, f.transport.admitted);
            f.runOnce();
            assertEquals(3, calls[0]);
            assertEquals(2, f.transport.admitted);
        }
    }

    private static void verifyPublicationBeforeRegistration() {
        CoordinatorRequestManager coordinator = new CoordinatorRequestManager(new LogContext(), 0, 0, "dispatch");
        NextPollCondition observed = coordinator.stateChanged();
        coordinator.signalClose(); // Publication precedes scheduler subscription.
        int[] calls = {0};
        RequestManager dependent = now -> waiting(++calls[0] == 1 ? observed : coordinator.stateChanged());
        try (NetworkLoopFixture f = new NetworkLoopFixture(Collections.singletonList(dependent))) {
            f.runOnce();
            assertEquals(0, f.transport.waitMs);
            f.runOnce();
            assertEquals(2, calls[0]);
            f.runOnce();
            assertEquals(2, calls[0]);
        }
    }
}
