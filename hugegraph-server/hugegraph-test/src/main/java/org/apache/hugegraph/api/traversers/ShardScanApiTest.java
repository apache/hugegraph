/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.api.BaseApiTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.ImmutableMap;

@RunWith(Parameterized.class)
public class ShardScanApiTest extends BaseApiTest {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> elements() {
        return Arrays.asList(new Object[]{"vertices", 6}, new Object[]{"edges", 5});
    }

    private final String elements;
    private final String path;
    private final int count;

    public ShardScanApiTest(String elements, int count) {
        this.elements = elements;
        this.path = TRAVERSERS_API + "/" + elements + "/scan";
        this.count = count;
    }

    private void prepareRocksDBData() {
        Assume.assumeTrue("Shard scan contract requires RocksDB",
                          "rocksdb".equals(System.getProperty("backend")));
        initPropertyKey();
        initVertexLabel();
        initEdgeLabel();
        initVertex();
        initEdge();
    }

    @Test
    public void testMissingBounds() {
        String content = assertResponseStatus(400, client().get(this.path,
                                             ImmutableMap.of("page_limit", 2)));
        String message = assertJsonContains(content, "message");
        Assert.assertTrue(message, message.contains("start parameter is required"));
    }

    @Test
    public void testMissingStartWithEmptyEnd() {
        String content = assertResponseStatus(400, client().get(this.path,
                                             ImmutableMap.of("end", "")));
        String message = assertJsonContains(content, "message");
        Assert.assertTrue(message, message.contains("start parameter is required"));
    }

    @Test
    public void testMissingEndWithEmptyStart() {
        String content = assertResponseStatus(400, client().get(this.path,
                                             ImmutableMap.of("start", "")));
        String message = assertJsonContains(content, "message");
        Assert.assertTrue(message, message.contains("end parameter is required"));
    }

    @Test
    public void testExplicitEmptyPageLimitsAndResumesScan() {
        this.prepareRocksDBData();
        Map<String, Object> params = new HashMap<>();
        params.put("start", "");
        params.put("end", "");
        params.put("page", "");
        params.put("page_limit", 2);
        Set<Object> ids = new HashSet<>();
        int requests = 0;
        String page;
        do {
            String content = assertResponseStatus(200, client().get(this.path, params));
            List<Map<String, Object>> result = assertJsonContains(content, this.elements);
            Assert.assertTrue(result.size() <= 2);
            for (Map<String, Object> element : result) {
                Assert.assertTrue("Duplicate scan element", ids.add(element.get("id")));
            }
            page = assertJsonContains(content, "page");
            params.put("page", page);
            Assert.assertTrue("Scan cursor did not terminate", ++requests < 10);
        } while (page != null && !page.isEmpty());
        Assert.assertEquals(this.count, ids.size());
        Assert.assertTrue(requests > 1);
    }

    @Test
    public void testOmittedPageRetainsUnpagedContract() {
        this.prepareRocksDBData();
        String content = assertResponseStatus(200, client().get(this.path,
                ImmutableMap.of("start", "", "end", "", "page_limit", 2)));
        List<?> result = assertJsonContains(content, this.elements);
        Assert.assertEquals(this.count, result.size());
        Assert.assertFalse(content.contains("\"page\""));
    }
}
