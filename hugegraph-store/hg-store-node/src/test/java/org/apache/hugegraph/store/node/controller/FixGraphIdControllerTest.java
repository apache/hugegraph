/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.node.controller;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hugegraph.store.util.HgStoreException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FixGraphIdControllerTest {

    @Test
    public void testUpdateGraphIdRejectsIdsOutsideAllocatableDomain() {
        FixGraphIdController controller = new FixGraphIdController();
        long[] invalidGraphIds = {-1L, 65534L, 65536L};

        for (long graphId : invalidGraphIds) {
            String graphName = "invalid-" + graphId;
            Map<String, Long> graphIds =
                    Collections.singletonMap(graphName, graphId);

            HgStoreException exception = Assertions.assertThrows(
                    HgStoreException.class,
                    () -> controller.updateGraphId(0, graphIds));
            Assertions.assertTrue(exception.getMessage().contains("Invalid graph ID"));
            Assertions.assertTrue(exception.getMessage().contains(String.valueOf(graphId)));
            Assertions.assertTrue(exception.getMessage().contains(graphName));
        }
    }

    @Test
    public void testUpdateGraphIdRejectsDuplicateIds() {
        FixGraphIdController controller = new FixGraphIdController();
        Map<String, Long> graphIds = new LinkedHashMap<>();
        graphIds.put("graph-a", 5L);
        graphIds.put("graph-b", 5L);

        HgStoreException exception = Assertions.assertThrows(
                HgStoreException.class,
                () -> controller.updateGraphId(0, graphIds));
        Assertions.assertTrue(exception.getMessage().contains("graph-a"));
        Assertions.assertTrue(exception.getMessage().contains("graph-b"));
        Assertions.assertTrue(exception.getMessage().contains("5"));
    }
}
