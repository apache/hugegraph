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

package org.apache.hugegraph.job.algorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.job.algorithm.AbstractAlgorithm.JsonMap;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.JsonUtil;
import org.junit.Test;

public class AlgorithmJsonMapTest {

    @Test
    public void testStringVertexIdsRoundTripAsValues() {
        List<String> ids = stringIds();
        JsonMap json = new JsonMap();
        json.startList();
        for (String id : ids) {
            json.append(IdGenerator.of(id).toString());
        }
        json.endList();
        Assert.assertEquals(ids, JsonUtil.fromJson(JsonUtil.toJson(json.asJson()), List.class));
    }

    @Test
    public void testStringVertexIdsRoundTripAsNumericResultKeys() {
        List<String> ids = stringIds();
        JsonMap json = new JsonMap();
        json.startObject();
        for (int i = 0; i < ids.size(); i++) {
            json.append(IdGenerator.of(ids.get(i)), (long) i);
        }
        json.endObject();
        Map<?, ?> results = JsonUtil.fromJson(JsonUtil.toJson(json.asJson()), Map.class);
        Assert.assertEquals(ids.size(), results.size());
        for (int i = 0; i < ids.size(); i++) {
            Assert.assertEquals(i, ((Number) results.get(ids.get(i))).intValue());
        }
    }

    @Test
    public void testStringKeysAndValuesRoundTripTogether() {
        JsonMap json = new JsonMap();
        json.startObject();
        for (String id : stringIds()) {
            json.append(id, id);
        }
        json.endObject();
        Map<?, ?> results = JsonUtil.fromJson(JsonUtil.toJson(json.asJson()), Map.class);
        for (String id : stringIds()) {
            Assert.assertEquals(id, results.get(id));
        }
    }

    @Test
    public void testAppendKeyEscapesNestedResultName() {
        String key = "label\\name\n";
        JsonMap json = new JsonMap();
        json.startObject();
        json.appendKey(key);
        json.startList();
        json.append("vertex\\name\t");
        json.endList();
        json.endObject();
        Map<?, ?> results = JsonUtil.fromJson(JsonUtil.toJson(json.asJson()), Map.class);
        Assert.assertEquals(List.of("vertex\\name\t"), results.get(key));
    }

    @Test
    public void testPlainNumericAndRawResultsKeepTheirShape() {
        JsonMap json = new JsonMap();
        json.startObject();
        json.append("degree", 3L);
        json.append("rank", 0.5D);
        json.appendRaw("stats", "{\"vertices\":2}");
        json.endObject();
        Assert.assertEquals("{\"degree\":3,\"rank\":0.5,\"stats\":{\"vertices\":2}}",
                            JsonUtil.toJson(json.asJson()));
    }

    @Test
    public void testNullStringInputsRemainInvalid() {
        JsonMap json = new JsonMap();
        Assert.assertThrows(NullPointerException.class, () -> json.append((String) null));
        Assert.assertThrows(NullPointerException.class, () -> json.appendKey(null));
    }

    private static List<String> stringIds() {
        List<String> ids = new ArrayList<>(List.of("plain", "path\\name", "quoted\"id", "中文顶点😀"));
        for (char control = 0; control < 32; control++) {
            ids.add("vertex" + control + "id");
        }
        return ids;
    }
}
