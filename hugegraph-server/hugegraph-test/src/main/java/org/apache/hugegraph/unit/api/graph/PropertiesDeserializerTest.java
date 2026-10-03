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

package org.apache.hugegraph.unit.api.graph;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.api.graph.PropertiesDeserializer;
import org.apache.hugegraph.api.schema.UserdataDeserializer;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

public class PropertiesDeserializerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static class Body {

        @JsonProperty("label")
        public String label;
        @JsonProperty("properties")
        @JsonDeserialize(using = PropertiesDeserializer.class)
        public Map<String, Object> properties;
        @JsonProperty("options")
        public Map<String, Object> options;
    }

    @Test
    public void testFractionsAreExactInPropertiesOnly() throws Exception {
        Body body = MAPPER.readValue(
                "{\"label\":\"account\"," +
                "\"properties\":{\"amount\":12345678901234567890.123456789012345678," +
                "\"rate\":1.10,\"count\":7,\"big\":123456789012345678901234567890," +
                "\"name\":\"a\",\"ok\":true,\"none\":null," +
                "\"tags\":[1.5,\"x\",[2.25]],\"nested\":{\"w\":0.1}}," +
                "\"options\":{\"alpha\":0.85}}", Body.class);

        Map<String, Object> props = body.properties;
        Assert.assertEquals(new BigDecimal("12345678901234567890.123456789012345678"),
                            props.get("amount"));
        Assert.assertEquals(new BigDecimal("1.10"), props.get("rate"));
        Assert.assertEquals(7, props.get("count"));
        Assert.assertEquals(new java.math.BigInteger("123456789012345678901234567890"),
                            props.get("big"));
        Assert.assertEquals("a", props.get("name"));
        Assert.assertEquals(Boolean.TRUE, props.get("ok"));
        Assert.assertTrue(props.containsKey("none"));
        Assert.assertNull(props.get("none"));
        List<?> tags = (List<?>) props.get("tags");
        Assert.assertEquals(new BigDecimal("1.5"), tags.get(0));
        Assert.assertEquals("x", tags.get(1));
        // content of an OBJECT value keeps Jackson's types: an array in an
        // array, and a fraction inside a nested object
        Assert.assertEquals(2.25d, ((List<?>) tags.get(2)).get(0));
        Assert.assertEquals(0.1d, ((Map<?, ?>) props.get("nested")).get("w"));
        // key order is kept
        Assert.assertEquals("amount", props.keySet().iterator().next());

        // everything outside "properties" keeps Jackson's default types
        Assert.assertEquals(0.85d, body.options.get("alpha"));

        // an OBJECT property with nested numbers round-trips them as numbers
        body = MAPPER.readValue(
                "{\"label\":\"a\",\"properties\":{\"meta\":{\"ratio\":0.25,\"n\":3," +
                "\"inner\":{\"p\":1.5},\"list\":[0.5]},\"amounts\":[1.5,2.50]}}", Body.class);
        Map<?, ?> meta = (Map<?, ?>) body.properties.get("meta");
        Assert.assertEquals(0.25d, meta.get("ratio"));
        Assert.assertEquals(3, meta.get("n"));
        Assert.assertEquals(1.5d, ((Map<?, ?>) meta.get("inner")).get("p"));
        Assert.assertEquals(0.5d, ((List<?>) meta.get("list")).get(0));
        Assert.assertEquals(new BigDecimal("2.50"), ((List<?>) body.properties.get("amounts")).get(1));
        Assert.assertEquals("{\"ratio\":0.25,\"n\":3,\"inner\":{\"p\":1.5},\"list\":[0.5]}",
                            org.apache.hugegraph.util.JsonUtil.toJson(meta));
    }

    public static class KeyBody {

        @JsonProperty("name")
        public String name;
        @JsonProperty("user_data")
        @JsonDeserialize(using = UserdataDeserializer.class)
        public Userdata userdata;
    }

    /** The same reading on a property key's user_data (PropertyKeyAPI). */
    @Test
    public void testPropertyKeyUserdataIsExact() throws Exception {
        KeyBody key = MAPPER.readValue(
                "{\"name\":\"fee\"," +
                "\"user_data\":{\"~default_value\":0.1234567890123456789,\"note\":\"x\"," +
                "\"weight\":2,\"rate\":0.85,\"tags\":[1.5]}}", KeyBody.class);
        Assert.assertEquals(new BigDecimal("0.1234567890123456789"),
                            key.userdata.get("~default_value"));
        Assert.assertEquals("x", key.userdata.get("note"));
        Assert.assertEquals(2, key.userdata.get("weight"));
        // only the default value is exact: other metadata keeps its types
        Assert.assertEquals(0.85d, key.userdata.get("rate"));
        Assert.assertEquals(1.5d, ((List<?>) key.userdata.get("tags")).get(0));
        key = MAPPER.readValue("{\"name\":\"fee\",\"user_data\":{\"~default_value\":[1.5, 2]}}",
                               KeyBody.class);
        Assert.assertEquals(new BigDecimal("1.5"),
                            ((List<?>) key.userdata.get("~default_value")).get(0));
        key = MAPPER.readValue("{\"name\":\"fee\"}", KeyBody.class);
        Assert.assertNull(key.userdata);
    }

    @Test
    public void testNullAndNonObject() throws Exception {
        Body body = MAPPER.readValue("{\"label\":\"x\",\"properties\":null}",
                                     Body.class);
        Assert.assertNull(body.properties);
        Assert.assertThrows(com.fasterxml.jackson.databind.JsonMappingException.class, () -> {
            MAPPER.readValue("{\"label\":\"x\",\"properties\":[1]}", Body.class);
        });
    }

    /** The list-API filter is read by the same rule as a request body. */
    @Test
    public void testFilterParsesLikeABody() throws Exception {
        Map<String, Object> filter = PropertiesDeserializer.parse(
                "{\"amount\":12345678901234567890.123456789012345678," +
                "\"amounts\":[1.5,2],\"meta\":{\"ratio\":0.25,\"n\":3}}");
        Assert.assertEquals(new BigDecimal("12345678901234567890.123456789012345678"),
                            filter.get("amount"));
        Assert.assertEquals(new BigDecimal("1.5"), ((List<?>) filter.get("amounts")).get(0));
        Map<?, ?> meta = (Map<?, ?>) filter.get("meta");
        Assert.assertEquals(0.25d, meta.get("ratio"));
        Assert.assertEquals(3, meta.get("n"));
        Assert.assertThrows(com.fasterxml.jackson.databind.JsonMappingException.class, () -> {
            PropertiesDeserializer.parse("[1]");
        });
        Assert.assertThrows(com.fasterxml.jackson.core.JsonProcessingException.class, () -> {
            PropertiesDeserializer.parse("{\"a\":1} x");
        });
    }
}
