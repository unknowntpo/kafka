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

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NetworkLoopConditionTest {
    private static NetworkClientDelegate.PollResult await(NextPollCondition.Signal signal) {
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), signal.await());
    }

    @Test
    public void earlierManagerWakesLaterManagerOnNextLoopWithoutSleeping() {
        NextPollCondition.Signal first = new NextPollCondition.Signal();
        NextPollCondition.Signal second = new NextPollCondition.Signal();
        int[] calls = {0, 0};
        RequestManager a = now -> {
            if (++calls[0] == 2)
                second.publish();
            return await(first);
        };
        RequestManager b = now -> {
            calls[1]++;
            return await(second);
        };
        try (NetworkLoopFixture f = new NetworkLoopFixture(Arrays.asList(a, b))) {
            f.runOnce();
            first.publish();
            f.runOnce();
            assertEquals(2, calls[0]);
            assertEquals(1, calls[1]);
            assertEquals(0, f.transport.waitMs);
            f.runOnce();
            assertEquals(2, calls[1]);
            assertEquals(3, f.transport.polls);
        }
    }

    @Test
    public void laterManagerWakesEarlierManagerWithoutRecursiveDrain() {
        NextPollCondition.Signal first = new NextPollCondition.Signal();
        NextPollCondition.Signal second = new NextPollCondition.Signal();
        int[] calls = {0, 0};
        RequestManager a = now -> {
            calls[0]++;
            return await(first);
        };
        RequestManager b = now -> {
            if (++calls[1] == 2)
                first.publish();
            return await(second);
        };
        try (NetworkLoopFixture f = new NetworkLoopFixture(Arrays.asList(a, b))) {
            f.runOnce();
            second.publish();
            f.runOnce();
            assertEquals(1, calls[0]);
            assertEquals(0, f.transport.waitMs);
            f.runOnce();
            assertEquals(2, calls[0]);
            assertEquals(2, calls[1]);
        }
    }

    @Test
    public void completionSuccessInvalidationAndCloseUseActualCoordinator() {
        CoordinatorRequestManager coordinator = new CoordinatorRequestManager(new LogContext(), 0, 0, "loop");
        try (NetworkLoopFixture f = new NetworkLoopFixture(Collections.singletonList(coordinator))) {
            f.transport.captureRequests = true;
            f.runOnce();
            NetworkClientDelegate.UnsentRequest request = f.transport.requests.get(0);
            f.transport.completion = () -> request.handler().onComplete(new ClientResponse(
                    new RequestHeader(ApiKeys.FIND_COORDINATOR, request.requestBuilder().build().version(), "", 1),
                    request.handler(), "1", f.clock.now - 1, f.clock.now, false, null, null,
                    FindCoordinatorResponse.prepareResponse(Errors.NONE, "loop", new Node(1, "localhost", 9092))));
            f.runOnce();
            assertTrue(coordinator.coordinator().isPresent());
            f.runOnce();
            assertEquals(1, f.transport.admitted);
            f.enqueue(() -> coordinator.markCoordinatorUnknown("test", f.clock.now));
            f.runOnce();
            assertEquals(2, f.transport.admitted);
            assertEquals(1, f.transport.wakeups);
            f.enqueue(coordinator::signalClose);
            f.runOnce();
            assertEquals(2, f.transport.admitted);
        }
    }

    @Test
    public void realCompletionFailureHonorsBackoffAcrossNetworkIterations() {
        CoordinatorRequestManager coordinator = new CoordinatorRequestManager(new LogContext(), 100, 100, "loop");
        try (NetworkLoopFixture f = new NetworkLoopFixture(Collections.singletonList(coordinator))) {
            f.transport.captureRequests = true;
            f.runOnce();
            NetworkClientDelegate.UnsentRequest request = f.transport.requests.get(0);
            f.transport.completion = () -> request.handler().onFailure(f.clock.now, new TimeoutException("test"));
            f.runOnce();
            f.runOnce();
            long remaining = f.transport.waitMs;
            assertTrue(remaining > 0 && remaining <= 100);
            f.clock.now += remaining - 2;
            f.runOnce();
            assertEquals(1, f.transport.admitted);
            assertEquals(1, f.transport.waitMs);
            f.runOnce();
            assertEquals(2, f.transport.admitted);
        }
    }

    @Test
    public void exceptionPreservesOtherReadyManagersForNextIteration() {
        int[] calls = {0, 0};
        RequestManager a = now -> {
            if (calls[0]++ == 0)
                throw new IllegalStateException("test");
            return NetworkClientDelegate.PollResult.EMPTY;
        };
        RequestManager b = now -> {
            calls[1]++;
            return NetworkClientDelegate.PollResult.EMPTY;
        };
        try (NetworkLoopFixture f = new NetworkLoopFixture(Arrays.asList(a, b))) {
            assertThrows(IllegalStateException.class, f::runOnce);
            assertEquals(0, f.transport.polls);
            f.runOnce();
            assertEquals(2, calls[0]);
            assertEquals(1, calls[1]);
        }
    }

    @Test
    public void legacyPollingAndApplicationWaitContractRemainActive() {
        NextPollCondition.Signal signal = new NextPollCondition.Signal();
        int[] calls = {0, 0, 0};
        RequestManager waiting = new RequestManager() {
            @Override
            public NetworkClientDelegate.PollResult poll(long now) {
                calls[0]++;
                return await(signal);
            }
            @Override
            public long maximumTimeToWait(long now) {
                calls[2]++;
                return 17;
            }
        };
        RequestManager legacy = now -> {
            calls[1]++;
            return new NetworkClientDelegate.PollResult(30);
        };
        try (NetworkLoopFixture f = new NetworkLoopFixture(Arrays.asList(waiting, legacy))) {
            f.runOnce();
            f.runOnce();
            assertEquals(1, calls[0]);
            assertEquals(2, calls[1]);
            assertEquals(2, calls[2]);
            assertEquals(17, f.thread.maximumTimeToWait());
            assertEquals(30, f.transport.waitMs);
        }
    }
}
