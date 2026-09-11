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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NextPollConditionTest {
    @Test
    void relativeBudgetIsNotRenewedByInspection() {
        NextPollCondition deadline = NextPollCondition.after(100, 100);
        assertEquals(100, deadline.remainingMs(100));
        assertEquals(20, deadline.remainingMs(180));
        assertEquals(1, deadline.remainingMs(199));
        assertEquals(0, deadline.remainingMs(200));
    }

    @Test
    void overflowDoesNotTurnAFutureDeadlineIntoImmediateWork() {
        NextPollCondition deadline = NextPollCondition.after(Long.MAX_VALUE - 2, 10);
        assertFalse(deadline.isReady(Long.MAX_VALUE - 1));
        assertEquals(1, deadline.remainingMs(Long.MAX_VALUE - 1));
        assertTrue(deadline.isReady(Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, NextPollCondition.at(Long.MAX_VALUE).remainingMs(-1));
        assertThrows(IllegalArgumentException.class, () -> NextPollCondition.after(0, -1));
    }

    @Test
    void quiescenceIsDifferentFromAnActualMaximumDeadline() {
        assertFalse(NextPollCondition.idle().isReady(Long.MAX_VALUE));
        assertTrue(NextPollCondition.at(Long.MAX_VALUE).isReady(Long.MAX_VALUE));
        assertTrue(NextPollCondition.ready().isReady(0));
        assertTrue(NextPollCondition.after(0, 0).isReady(0));
    }

    @Test
    void compositionReusesConstantsAndEarlierDeadline() {
        NextPollCondition earlier = NextPollCondition.at(100);
        NextPollCondition later = NextPollCondition.at(200);
        assertSame(earlier, NextPollCondition.either(earlier, later));
        assertSame(earlier, NextPollCondition.either(later, earlier));
        assertSame(earlier, NextPollCondition.either(NextPollCondition.idle(), earlier));
        assertSame(NextPollCondition.ready(), NextPollCondition.either(earlier, NextPollCondition.ready()));
    }

    @Test
    void compositionPreservesArbitraryInputConditions() {
        NextPollCondition input = new NextPollCondition() {
            @Override
            public boolean isReady(long currentTimeMs) {
                return currentTimeMs == 42;
            }

            @Override
            public long remainingMs(long currentTimeMs) {
                return isReady(currentTimeMs) ? 0 : 10;
            }
        };
        NextPollCondition combined = NextPollCondition.either(input, NextPollCondition.at(100));
        assertEquals(10, combined.remainingMs(0));
        assertTrue(combined.isReady(42));
    }
}
