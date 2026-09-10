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
package org.apache.kafka.clients;

import org.apache.kafka.common.message.ApiVersionsResponseData;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ApiVersionsTest {

    @Test
    public void changeListenerObservesPublishedStateAndAbsentRemoval() {
        ApiVersions apiVersions = new ApiVersions();
        List<NodeApiVersions> observations = new ArrayList<>();
        Runnable listener = () -> observations.add(apiVersions.get("1"));
        apiVersions.addChangeListener(listener);
        NodeApiVersions versions = NodeApiVersions.create();
        apiVersions.update("1", versions);
        apiVersions.remove("1");
        apiVersions.remove("1");
        assertEquals(Arrays.asList(versions, null, null), observations);
        apiVersions.removeChangeListener(listener);
        apiVersions.update("1", versions);
        assertEquals(3, observations.size());
    }

    @Test
    public void testFinalizedFeaturesUpdate() {
        ApiVersions apiVersions = new ApiVersions();
        assertEquals(-1, apiVersions.getMaxFinalizedFeaturesEpoch());

        apiVersions.update("2",
            new NodeApiVersions(NodeApiVersions.create().allSupportedApiVersions().values(),
                Arrays.asList(new ApiVersionsResponseData.SupportedFeatureKey()
                    .setName("transaction.version")
                    .setMaxVersion((short) 2)
                    .setMinVersion((short) 0)),
                Arrays.asList(new ApiVersionsResponseData.FinalizedFeatureKey()
                    .setName("transaction.version")
                    .setMaxVersionLevel((short) 2)
                    .setMinVersionLevel((short) 2)),
                1));
        ApiVersions.FinalizedFeaturesInfo info = apiVersions.getFinalizedFeaturesInfo();
        assertEquals(1, info.finalizedFeaturesEpoch);
        assertEquals((short) 2, info.finalizedFeatures.get("transaction.version"));

        apiVersions.update("1",
            new NodeApiVersions(NodeApiVersions.create().allSupportedApiVersions().values(),
                Arrays.asList(new ApiVersionsResponseData.SupportedFeatureKey()
                    .setName("transaction.version")
                    .setMaxVersion((short) 2)
                    .setMinVersion((short) 0)),
                Arrays.asList(new ApiVersionsResponseData.FinalizedFeatureKey()
                    .setName("transaction.version")
                    .setMaxVersionLevel((short) 1)
                    .setMinVersionLevel((short) 1)),
                0));
        // The stale update should be fenced.
        info = apiVersions.getFinalizedFeaturesInfo();
        assertEquals(1, info.finalizedFeaturesEpoch);
        assertEquals((short) 2, info.finalizedFeatures.get("transaction.version"));
    }

}
