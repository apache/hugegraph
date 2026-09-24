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

package org.apache.hugegraph.api.traversers;

import java.util.List;
import java.util.Map;

import org.apache.hugegraph.api.BaseApiTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

import jakarta.ws.rs.core.Response;

public class EdgesApiTest extends BaseApiTest {

    static final String EDGE_PATH = TRAVERSERS_API + "/edges";
    static final String SHARES_PATH = TRAVERSERS_API + "/edges/shards";
    static final String SCAN_PATH = TRAVERSERS_API + "/edges/scan";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void testList() {
        Map<String, String> name2Ids = listAllVertexName2Ids();
        final String edgeGetPath = "graphspaces/DEFAULT/graphs/hugegraph/graph/edges";
        String vadasId = name2Ids.get("vadas");
        Map<String, Object> params = ImmutableMap.of(
                "vertex_id", id2Json(vadasId),
                "direction", "IN");
        Response r = client().get(edgeGetPath, params);
        String content = assertResponseStatus(200, r);
        List<Map<?, ?>> edges = assertJsonContains(content, "edges");
        Assert.assertNotNull(edges);
        Assert.assertFalse(edges.isEmpty());
        String edgeId = assertMapContains(edges.get(0), "id");
        Assert.assertNotNull(edgeId);

        r = client().get(EDGE_PATH, ImmutableMultimap.of("ids", edgeId));
        content = assertResponseStatus(200, r);
        List<?> edges2 = assertJsonContains(content, "edges");
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(edges.get(0), edges2.get(0));
    }

    @Test
    public void testShareAndScan() {
        Response r = client().get(SHARES_PATH, ImmutableMap.of("split_size",
                                                               1048576));
        String content = assertResponseStatus(200, r);
        List<Map<String, ?>> shards = assertJsonContains(content, "shards");
        Assert.assertNotNull(shards);
        Assert.assertFalse(shards.isEmpty());
        String start = assertMapContains(shards.get(0), "start");
        String end = assertMapContains(shards.get(0), "end");
        r = client().get(SCAN_PATH, ImmutableMap.of("start", start,
                                                    "end", end));
        content = assertResponseStatus(200, r);
        /*
         * Different storage backends return differently, so the return is
         * not checked
         */
        assertJsonContains(content, "edges");
    }
}
