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
package org.apache.kafka.clients.consumer.ng;

/**
 * Entry point of the next-generation consumer module (CONSUMER-NG-01). The module targets Java 21 and reuses the
 * RPC layer of {@code kafka-clients}; it exists so measurement code and the implementation can be built and
 * tested against the trunk clients without touching them.
 */
public final class ConsumerNg {

    private ConsumerNg() {
    }

    /** @return the Java feature version this module is compiled for */
    public static int targetJavaVersion() {
        return 21;
    }
}
