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

package org.apache.hugegraph.struct.schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class EdgeLabelMapTest {

    @Test
    public void testSingleLinkMetadataRegardlessOfOrder() throws Exception {
        // Exact field shapes emitted by the pre-consolidation Server JsonUtil.
        Map<String, Object> wire = map("{\"id\":3,\"name\":\"knows\",\"links\":[{\"1\":2}]," +
                                       "\"sourceLabel\":\"1\",\"targetLabel\":\"2\"}");
        EdgeLabel decoded = EdgeLabel.fromMap(wire, null);
        Assert.assertEquals(Collections.singleton(Pair.of(IdGenerator.of(1), IdGenerator.of(2))),
                            decoded.links());
        Map<String, Object> endpointsFirst = new LinkedHashMap<>();
        endpointsFirst.put(EdgeLabel.P.SOURCE_LABEL, wire.get(EdgeLabel.P.SOURCE_LABEL));
        endpointsFirst.put(EdgeLabel.P.TARGET_LABEL, wire.get(EdgeLabel.P.TARGET_LABEL));
        endpointsFirst.putAll(wire);
        Assert.assertEquals(decoded.links(), EdgeLabel.fromMap(endpointsFirst, null).links());
    }

    @Test
    public void testLegacyEndpointsWithoutLinks() throws Exception {
        Map<String, Object> wire = map("{\"id\":3,\"name\":\"knows\",\"sourceLabel\":\"1\"," +
                                       "\"targetLabel\":\"2\"}");
        EdgeLabel decoded = EdgeLabel.fromMap(wire, null);
        Assert.assertEquals(IdGenerator.of(1), decoded.sourceLabel());
        Assert.assertEquals(IdGenerator.of(2), decoded.targetLabel());
        Assert.assertEquals(1, decoded.links().size());
    }

    @Test
    public void testMultiLinksAcceptConsistentRedundantEndpoints() throws Exception {
        Map<String, Object> wire = map("{\"id\":3,\"name\":\"knows\",\"links\":[{\"1\":2},{\"2\":4}]," +
                                       "\"sourceLabel\":\"1\",\"targetLabel\":\"2\"}");
        EdgeLabel decoded = EdgeLabel.fromMap(wire, null);
        Assert.assertEquals(2, decoded.links().size());
        Assert.assertTrue(decoded.checkLinkEqual(IdGenerator.of(1), IdGenerator.of(2)));
        Assert.assertTrue(decoded.checkLinkEqual(IdGenerator.of(2), IdGenerator.of(4)));
    }

    @Test
    public void testConflictingEndpointsRejected() throws Exception {
        Map<String, Object> wire = map("{\"id\":3,\"name\":\"knows\",\"links\":[{\"1\":2}]," +
                                       "\"sourceLabel\":\"1\",\"targetLabel\":\"4\"}");
        IllegalArgumentException error = Assert.assertThrows(IllegalArgumentException.class,
                                                             () -> EdgeLabel.fromMap(wire, null));
        Assert.assertTrue(error.getMessage().contains("conflict"));
    }

    @Test
    public void testIncompleteEndpointsRejected() throws Exception {
        Map<String, Object> wire = map("{\"id\":3,\"name\":\"knows\",\"sourceLabel\":\"1\"}");
        Assert.assertThrows(IllegalArgumentException.class, () -> EdgeLabel.fromMap(wire, null));
    }

    @Test
    public void testMalformedLinksRejected() throws Exception {
        for (String value : new String[]{"null", "{}", "[{}]", "[{\"1\":null}]",
                                         "[{\"invalid\":2}]"}) {
            Map<String, Object> wire = map("{\"id\":3,\"name\":\"knows\",\"links\":" + value + "}");
            Assert.assertThrows(IllegalArgumentException.class, () -> EdgeLabel.fromMap(wire, null));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(String json) throws Exception {
        return new ObjectMapper().readValue(json, LinkedHashMap.class);
    }
}
