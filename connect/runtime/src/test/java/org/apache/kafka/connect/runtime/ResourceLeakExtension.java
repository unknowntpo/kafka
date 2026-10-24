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

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;

/**
 * Fails a test that leaves a {@link TestResourceTracker tracked} resource open. JUnit runs this callback after
 * the test's own {@code @AfterEach} methods, so resources closed there are not reported.
 * Set {@code -Dkafka.test.resource.leak.mode=warn} to print the leaks instead of failing.
 */
public class ResourceLeakExtension implements BeforeEachCallback, AfterEachCallback {

    @Override
    public void beforeEach(ExtensionContext context) {
        TestResourceTracker.enable();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        List<Throwable> leaks = TestResourceTracker.disable();
        if (leaks.isEmpty()) return;
        AssertionError error = new AssertionError(context.getDisplayName() + " leaked " + leaks.size() + " resource(s)");
        leaks.forEach(error::addSuppressed);
        if ("warn".equals(System.getProperty("kafka.test.resource.leak.mode"))) {
            error.printStackTrace();
        } else {
            throw error;
        }
    }
}
