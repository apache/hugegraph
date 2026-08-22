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

package org.apache.hugegraph.api.cypher;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.junit.Test;

public class CypherClientTest extends BaseUnitTest {

    @Test
    public void testNormalizeHandlesNullMapAndArrayValues() {
        Map<Object, Object> value = new LinkedHashMap<>();
        value.put(IdGenerator.of(1L),
                  new Object[]{IdGenerator.of("marko"), null});

        Object normalized = CypherClient.normalize(value);

        Assert.assertInstanceOf(Map.class, normalized);
        Map<?, ?> map = (Map<?, ?>) normalized;
        Assert.assertTrue(map.containsKey(1L));
        Assert.assertInstanceOf(List.class, map.get(1L));

        List<?> values = (List<?>) map.get(1L);
        Assert.assertEquals("marko", values.get(0));
        Assert.assertNull(values.get(1));
    }

    @Test
    public void testNormalizeHandlesCyclicReferences() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("private-value", value);

        Assert.assertThrows(IllegalArgumentException.class,
                            () -> CypherClient.normalize(value), e -> {
            Assert.assertContains("cyclic Cypher result", e.getMessage());
            Assert.assertFalse(e.getMessage().contains("private-value"));
        });
    }

    @Test
    public void testNormalizePreservesThirtyTwoContainerLayers() {
        Object value = "leaf";
        for (int i = 0; i < 32; i++) {
            value = new Object[]{value};
        }

        Object normalized = CypherClient.normalize(value);
        Object current = normalized;
        for (int i = 0; i < 32; i++) {
            Assert.assertInstanceOf(List.class, current);
            List<?> list = (List<?>) current;
            Assert.assertEquals(1, list.size());
            current = list.get(0);
        }

        Assert.assertEquals("leaf", current);
    }

    @Test
    public void testNormalizeRejectsThirtyThirdContainerLayer() {
        Object value = "leaf";
        for (int i = 0; i < 33; i++) {
            value = new Object[]{value};
        }
        Object deeplyNestedValue = value;

        Assert.assertThrows(IllegalArgumentException.class,
                            () -> CypherClient.normalize(deeplyNestedValue),
                            e -> Assert.assertContains(
                                    "max normalization depth 32",
                                    e.getMessage()));
    }

    @Test
    public void testNormalizePreservesPathLabelsAndObjects() {
        Path path = MutablePath.make()
                               .extend(IdGenerator.of("marko"),
                                       Set.of("a"))
                               .extend(IdGenerator.of("lop"),
                                       Set.of("b", "software"));

        Object normalized = CypherClient.normalize(path);

        Assert.assertInstanceOf(Map.class, normalized);
        Map<?, ?> map = (Map<?, ?>) normalized;
        Assert.assertTrue(map.containsKey("labels"));
        Assert.assertTrue(map.containsKey("objects"));

        Assert.assertInstanceOf(List.class, map.get("labels"));
        Assert.assertInstanceOf(List.class, map.get("objects"));

        List<?> labels = (List<?>) map.get("labels");
        List<?> objects = (List<?>) map.get("objects");
        Assert.assertEquals(2, labels.size());
        Assert.assertEquals(2, objects.size());

        Assert.assertEquals("marko", objects.get(0));
        Assert.assertEquals("lop", objects.get(1));
        List<?> firstLabels = (List<?>) labels.get(0);
        List<?> secondLabels = (List<?>) labels.get(1);
        Assert.assertTrue(firstLabels.contains("a"));
        Assert.assertTrue(secondLabels.contains("b"));
        Assert.assertTrue(secondLabels.contains("software"));
    }
}
