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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LoopSignalTest {

    @Test
    public void wakeupsAreCoalescedPerParkCycle() {
        AtomicInteger wakeups = new AtomicInteger();
        LoopSignal signal = new LoopSignal(wakeups::incrementAndGet);

        assertFalse(signal.wakeupIfParked(), "loop is running: no wake-up");
        assertTrue(signal.prepareToPark(() -> false));
        assertTrue(signal.wakeupIfParked());
        assertFalse(signal.wakeupIfParked(), "already woken in this cycle");
        assertFalse(signal.wakeupIfParked());
        assertEquals(1, wakeups.get());
        assertEquals(1, signal.wakeupsIssued());

        signal.markRunning();
        assertTrue(signal.prepareToPark(() -> false));
        assertTrue(signal.wakeupIfParked());
        assertEquals(2, wakeups.get());
    }

    @Test
    public void workEnqueuedBeforeParkingPreventsBlocking() {
        LoopSignal signal = new LoopSignal(() -> { });
        ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
        queue.add("work");
        assertFalse(signal.prepareToPark(() -> !queue.isEmpty()));
        assertFalse(signal.isParked());
    }

    @Test
    public void concurrentProducersNeverLoseAWakeup() throws Exception {
        // Model of the loop: park, then wait for either a wake-up or pending work. Producers enqueue and signal.
        // Each cycle must see every item enqueued before the loop drained it; the loop must never block with work.
        AtomicLong wakeups = new AtomicLong();
        ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<>();
        Object monitor = new Object();
        LoopSignal signal = new LoopSignal(() -> {
            wakeups.incrementAndGet();
            synchronized (monitor) {
                monitor.notifyAll();
            }
        });
        int producers = 4;
        int perProducer = 5_000;
        AtomicInteger drained = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);

        Thread loop = new Thread(() -> {
            int total = producers * perProducer;
            while (drained.get() < total) {
                signal.markRunning();
                while (queue.poll() != null)
                    drained.incrementAndGet();
                if (signal.prepareToPark(() -> !queue.isEmpty())) {
                    synchronized (monitor) {
                        // NIO's Selector.wakeup() is sticky; emulate that with the isParked() check.
                        if (signal.isParked()) {
                            try {
                                monitor.wait(200);
                            } catch (InterruptedException e) {
                                return;
                            }
                        }
                    }
                }
            }
            done.countDown();
        }, "loop");
        loop.start();

        Thread[] threads = new Thread[producers];
        for (int p = 0; p < producers; p++) {
            threads[p] = new Thread(() -> {
                for (int i = 0; i < perProducer; i++) {
                    queue.add(i);
                    signal.wakeupIfParked();
                }
            }, "producer-" + p);
            threads[p].start();
        }
        for (Thread t : threads)
            t.join(30_000);
        assertTrue(done.await(30, TimeUnit.SECONDS), "loop starved: lost wake-up");
        assertEquals(producers * perProducer, drained.get());
        assertTrue(wakeups.get() < (long) producers * perProducer, "wake-ups must be coalesced: " + wakeups.get());
    }

}
