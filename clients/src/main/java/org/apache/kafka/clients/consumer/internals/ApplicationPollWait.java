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

/**
 * Shared application-wait state read by the network-owned heartbeat and auto-commit managers.
 * Unlike NextPollCondition.Signal, this object crosses threads and synchronizes its own state.
 * FetchBuffer additionally serializes wait registration and notification with its condition lock.
 *
 * <p>Notification revokes a registration before waking the application. Background activity can
 * therefore acknowledge the existing bounded wait, but repeated notifications cannot indefinitely
 * renew an application that never resumes. This object neither marks managers ready nor performs I/O.
 */
final class ApplicationPollWait {
    // A returning older wait must not revoke a newer registration.
    private long epoch;
    private boolean waiting;
    private long deadlineMs;
    // Monotonic activity evidence, not the latest time at which any background thread happened to run.
    private long lastActivityMs = Long.MIN_VALUE;

    /** Publish one bounded application wait before releasing FetchBuffer's lock to park. */
    synchronized long begin(long nowMs, long deadlineMs) {
        epoch++;
        waiting = true;
        this.deadlineMs = deadlineMs;
        recordActivity(nowMs);
        return epoch;
    }

    /** Revoke before wakeup; a late notification cannot credit activity beyond the registered deadline. */
    synchronized void signal(long nowMs) {
        if (waiting) {
            waiting = false;
            recordActivity(Math.min(nowMs, deadlineMs));
        }
    }

    /** The application has resumed; credit that real progress only to the matching wait generation. */
    synchronized void end(long expectedEpoch, long nowMs) {
        if (expectedEpoch == epoch) {
            waiting = false;
            recordActivity(nowMs);
        }
    }

    synchronized void recordActivity(long nowMs) {
        lastActivityMs = Math.max(lastActivityMs, nowMs);
    }

    /** Zero means app work, notification, or expiry prevents a background auto-commit snapshot. */
    synchronized long currentWaitEpoch(long nowMs) {
        return waiting && nowMs < deadlineMs ? epoch : 0L;
    }

    synchronized long activityMs(long nowMs) {
        return waiting ? Math.max(lastActivityMs, Math.min(nowMs, deadlineMs)) : lastActivityMs;
    }
}
