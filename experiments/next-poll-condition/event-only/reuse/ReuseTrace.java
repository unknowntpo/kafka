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

/** Deterministic per-pass semantic checksum; poll counts intentionally differ. */
public class ReuseTrace {
    public static void main(String[] args) {
        boolean conditional = Boolean.parseBoolean(args[0]);
        for (String pattern : new String[] {"IDLE", "SPARSE", "BUSY", "TIMER"}) {
            System.setProperty("oracle.pattern", pattern);
            try (UnifiedLoopWorkload w = new UnifiedLoopWorkload(32, pattern, conditional, false)) {
                long trace = 1;
                for (int i = 0; i < 10000; i++) {
                    long consumed = w.pass();
                    if (w.published != consumed) throw new AssertionError("unconsumed at " + i);
                    trace = trace * 31 + consumed;
                }
                System.out.println("TRACE," + pattern + "," + trace + "," + w.published + "," + w.consumed
                    + "," + w.managerPolls + "," + w.maximumWaitCalls + "," + w.fixture.transport.polls);
            }
        }
    }
}
