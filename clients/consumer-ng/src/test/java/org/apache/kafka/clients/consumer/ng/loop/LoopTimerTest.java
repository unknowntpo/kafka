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
package org.apache.kafka.clients.consumer.ng.loop;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LoopTimerTest {

    @Test
    public void runsInDeadlineOrderThenFifo() {
        LoopTimer timer = new LoopTimer();
        List<String> ran = new ArrayList<>();
        timer.schedule(0, 20, () -> ran.add("b"));
        timer.schedule(0, 10, () -> ran.add("a1"));
        timer.schedule(0, 10, () -> ran.add("a2"));
        assertEquals(10, timer.nextDeadlineMs());
        assertEquals(3, timer.timeToNextMs(7));

        assertEquals(0, timer.runExpired(9));
        assertEquals(2, timer.runExpired(10));
        assertEquals(List.of("a1", "a2"), ran);
        assertEquals(1, timer.runExpired(100));
        assertEquals(List.of("a1", "a2", "b"), ran);
        assertEquals(LoopTimer.NO_DEADLINE, timer.timeToNextMs(100));
        assertEquals(0, timer.pendingCount());
    }

    @Test
    public void zeroOrNegativeDelayIsClampedToMinimum() {
        LoopTimer timer = new LoopTimer();
        timer.schedule(100, 0, () -> { });
        timer.schedule(100, -5, () -> { });
        assertEquals(100 + LoopTimer.MIN_DELAY_MS, timer.nextDeadlineMs());
        assertEquals(0, timer.runExpired(100), "not due yet: a zero delay cannot spin the loop");
        assertEquals(2, timer.runExpired(100 + LoopTimer.MIN_DELAY_MS));
    }

    @Test
    public void selfReschedulingTaskDoesNotStarveOnePass() {
        LoopTimer timer = new LoopTimer();
        int[] runs = {0};
        Runnable[] task = new Runnable[1];
        task[0] = () -> {
            runs[0]++;
            timer.schedule(0, 0, task[0]);
        };
        timer.schedule(0, 0, task[0]);
        assertEquals(1, timer.runExpired(1_000), "rescheduled task waits for the next pass");
        assertEquals(1, runs[0]);
        assertEquals(1, timer.pendingCount());
        assertEquals(1, timer.runExpired(1_001));
        assertEquals(2, runs[0]);
    }

    @Test
    public void cancelledTaskNeverRunsAndDoesNotAffectDeadline() {
        LoopTimer timer = new LoopTimer();
        boolean[] ran = {false, false};
        LoopTimer.Handle first = timer.schedule(0, 5, () -> ran[0] = true);
        timer.schedule(0, 50, () -> ran[1] = true);
        assertTrue(first.cancel());
        assertFalse(first.isPending());
        assertFalse(first.cancel(), "cancelling twice is a no-op");
        assertEquals(50, timer.nextDeadlineMs());
        assertEquals(1, timer.pendingCount());
        assertEquals(1, timer.runExpired(60));
        assertFalse(ran[0]);
        assertTrue(ran[1]);
    }

    @Test
    public void hugeDelayDoesNotOverflow() {
        LoopTimer timer = new LoopTimer();
        timer.schedule(Long.MAX_VALUE - 10, Long.MAX_VALUE, () -> { });
        assertEquals(LoopTimer.NO_DEADLINE, timer.nextDeadlineMs());
    }
}
