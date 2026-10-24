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
package org.apache.kafka.connect.runtime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Records test resources that are created but not yet released. A resource calls {@link #track(Object)}
 * when it is created and {@link #release(Object)} when it is closed; {@link ResourceLeakExtension} reports
 * whatever is still tracked after each test.
 */
public final class TestResourceTracker {

    private static final Map<Object, Throwable> OPEN = Collections.synchronizedMap(new IdentityHashMap<>());
    // only tests running under ResourceLeakExtension record resources, so other tests do not pin them in memory
    private static volatile boolean enabled = false;

    private TestResourceTracker() {
    }

    public static void track(Object resource) {
        if (!enabled) return;
        OPEN.put(resource, new Throwable(resource.getClass().getSimpleName() + " created here"));
    }

    public static void release(Object resource) {
        OPEN.remove(resource);
    }

    static void enable() {
        drain();
        enabled = true;
    }

    static List<Throwable> disable() {
        enabled = false;
        return drain();
    }

    private static List<Throwable> drain() {
        synchronized (OPEN) {
            List<Throwable> leaks = new ArrayList<>(OPEN.values());
            OPEN.clear();
            return leaks;
        }
    }
}
