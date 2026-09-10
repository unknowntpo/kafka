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

import java.util.Collections;

public class TraceProbe {
    public static void main(String[] args) {
        boolean scheduled = args.length != 0;
        for (int period : new int[] {0, 1, 64, -1}) {
            CoordinatorRequestManager manager = new CoordinatorRequestManager(new LogContext(), 0, 0, "trace");
            RequestManagerScheduler scheduler = new RequestManagerScheduler();
            scheduler.registerManagers(Collections.singletonList(manager));
            NetworkClientDelegate.UnsentRequest[] pending = {null};
            int[] requests = {0};
            for (long now = 1; now <= 4000; now++) {
                if (period == -1) {
                    if (pending[0] != null) {
                        NetworkClientDelegate.UnsentRequest request = pending[0];
                        request.handler().onComplete(new ClientResponse(
                                new RequestHeader(ApiKeys.FIND_COORDINATOR, request.requestBuilder().build().version(), "", 1),
                                request.handler(), "1", now - 1, now, false, null, null,
                                FindCoordinatorResponse.prepareResponse(Errors.NONE, "trace", new Node(1, "localhost", 9092))));
                        pending[0] = null;
                    }
                    if (now % 64 == 0)
                        manager.markCoordinatorUnknown("trace invalidation", now);
                }
                if (pending[0] != null && period > 0 && now % period == 0) {
                    pending[0].handler().onFailure(now, new TimeoutException("trace"));
                    pending[0] = null;
                }
                requests[0] = 0;
                java.util.function.ToLongFunction<NetworkClientDelegate.PollResult> admit = result -> {
                    requests[0] += result.unsentRequests.size();
                    if (!result.unsentRequests.isEmpty())
                        pending[0] = result.unsentRequests.get(0);
                    return result.timeUntilNextPollMs;
                };
                long wait = scheduled ? scheduler.pollReady(now, admit) : admit.applyAsLong(manager.poll(now));
                System.out.println(period + ":" + now + ":" + requests[0] + ":" + wait + ":"
                        + manager.coordinator().isPresent() + ":" + manager.fatalError().isPresent());
            }
        }
    }
}
