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
import org.apache.kafka.clients.consumer.internals.events.CommitOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.LeaveGroupOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.StopFindCoordinatorOnCloseEvent;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

/**
 * The one place on the loop thread that defines and executes the order of the consumer's lifecycle transitions
 * (design semantics S6; KIP-1371 problem 4). The application thread still drives close(): it must run the
 * rebalance callbacks and the synchronous commit itself. What it sends to the loop are the same three close events
 * as before, but they no longer scatter into separate handlers: every transition passes through here, the expected
 * order is data ({@link Step}), a step requested out of order is logged, and shutdown runs every manager's
 * {@code pollOnClose} in registration order.
 *
 * <pre>
 *   RUNNING -> COMMIT_ON_CLOSE -> COORDINATOR_LOOKUP_STOPPED -> LEAVING -> LEFT -> SHUT_DOWN
 * </pre>
 * A consumer without a group skips the middle steps (its close events are never sent), which is why an advance
 * may jump forward; it must never go backward.
 */
final class LifecycleSequencer {

    enum Step {
        RUNNING,
        COMMIT_ON_CLOSE,
        COORDINATOR_LOOKUP_STOPPED,
        LEAVING,
        LEFT,
        SHUT_DOWN
    }

    private final Logger log;
    private final RequestManagers requestManagers;
    private Step step = Step.RUNNING;

    LifecycleSequencer(LogContext logContext, RequestManagers requestManagers) {
        this.log = logContext.logger(LifecycleSequencer.class);
        this.requestManagers = requestManagers;
    }

    Step step() {
        return step;
    }

    /**
     * Loop thread: applies {@code event} if it is a lifecycle transition.
     *
     * @return {@code true} if the event was a lifecycle event and has been handled here
     */
    boolean apply(ApplicationEvent event) {
        if (event instanceof CommitOnCloseEvent) {
            advance(Step.COMMIT_ON_CLOSE);
            requestManagers.commitRequestManager.ifPresent(RequestManager::signalClose);
            return true;
        }
        if (event instanceof StopFindCoordinatorOnCloseEvent) {
            advance(Step.COORDINATOR_LOOKUP_STOPPED);
            requestManagers.coordinatorRequestManager.ifPresent(RequestManager::signalClose);
            return true;
        }
        if (event instanceof LeaveGroupOnCloseEvent) {
            advance(Step.LEAVING);
            leave((LeaveGroupOnCloseEvent) event);
            return true;
        }
        return false;
    }

    private void leave(LeaveGroupOnCloseEvent event) {
        CompletableFuture<Void> leaving;
        if (requestManagers.consumerMembershipManager.isPresent()) {
            log.debug("Signal the ConsumerMembershipManager to leave the consumer group since the consumer is closing");
            leaving = requestManagers.consumerMembershipManager.get().leaveGroupOnClose(event.membershipOperation());
        } else if (requestManagers.streamsMembershipManager.isPresent()) {
            log.debug("Signal the StreamsMembershipManager to leave the streams group since the member is closing");
            leaving = requestManagers.streamsMembershipManager.get().leaveGroupOnClose(event.membershipOperation());
        } else {
            leaving = CompletableFuture.completedFuture(null);
        }
        leaving.whenComplete((ignored, error) -> {
            advance(Step.LEFT);
            if (error != null)
                event.future().completeExceptionally(error);
            else
                event.future().complete(null);
        });
    }

    /** Loop thread, once the loop has stopped: every manager's close-time requests, in registration order. */
    void shutdown(long currentTimeMs, NetworkClientDelegate network) {
        advance(Step.SHUT_DOWN);
        for (RequestManager manager : requestManagers.entries())
            network.addAll(manager.pollOnClose(currentTimeMs));
    }

    private void advance(Step next) {
        if (next.ordinal() < step.ordinal()) {
            log.warn("Lifecycle step {} requested after {}; the close sequence is out of order, applying anyway", next, step);
            return;
        }
        if (next.ordinal() > step.ordinal() + 1)
            log.debug("Lifecycle advancing from {} to {}", step, next);
        step = next;
    }
}
