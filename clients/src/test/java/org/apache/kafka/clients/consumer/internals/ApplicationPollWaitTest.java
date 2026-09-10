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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ApplicationPollWaitTest {
    @Test
    void testWaitingStopsAtApiDeadline() {
        ApplicationPollWait wait = new ApplicationPollWait();
        wait.begin(100, 1000);
        assertEquals(500, wait.activityMs(500));
        assertEquals(1000, wait.activityMs(5000));
        wait.signal(6000);
        assertEquals(1000, wait.activityMs(6000));
    }

    @Test
    void testRepeatedNotificationsDoNotRenewUnresponsiveApp() {
        ApplicationPollWait wait = new ApplicationPollWait();
        wait.begin(100, 10000);
        wait.signal(500);
        wait.signal(5000);
        assertEquals(500, wait.activityMs(9000));
    }

    @Test
    void testAppReturningThenBlockingInCallbackExpiresNormally() {
        ApplicationPollWait wait = new ApplicationPollWait();
        long epoch = wait.begin(100, 10000);
        wait.signal(500);
        wait.end(epoch, 600);
        assertEquals(600, wait.activityMs(9000));
    }

    @Test
    void testOldWaitCleanupCannotEndNewWait() {
        ApplicationPollWait wait = new ApplicationPollWait();
        long first = wait.begin(100, 1000);
        wait.signal(200);
        wait.end(first, 210);
        wait.begin(300, 2000);
        wait.end(first, 400);
        assertEquals(500, wait.activityMs(500));
    }
}
