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

import java.util.concurrent.TimeUnit;

/** Experimental notification gate. Capture generation BEFORE checking application state. */
public final class ApplicationProgress {
    private long generation;
    public synchronized long generation() { return generation; }
    public synchronized void publish() { generation++; notifyAll(); }
    public synchronized boolean awaitChange(long observed, long timeoutNs) throws InterruptedException {
        long start = System.nanoTime();
        long remaining = timeoutNs;
        while (generation == observed && remaining > 0) {
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
            remaining = timeoutNs - (System.nanoTime() - start);
        }
        return generation != observed;
    }
}
