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

package org.apache.hugegraph.core;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.hugegraph.util.Blob;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public abstract class PropertyCoreTest extends BaseCoreTest {

    protected abstract <V> V property(String key, V value);

    protected abstract <V> V propertyList(String key, Object... values);

    protected abstract <V> V propertySet(String key, Object... values);

    @Before
    public void initSchema() {
        SchemaManager schema = graph().schema();

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("string").asText().create();
        schema.propertyKey("bool").asBoolean().create();
        schema.propertyKey("byte").asByte().create();
        schema.propertyKey("int").asInt().create();
        schema.propertyKey("long").asLong().create();
        schema.propertyKey("float").asFloat().create();
        schema.propertyKey("double").asDouble().create();
        schema.propertyKey("blob").asBlob().create();
        schema.propertyKey("time").asDate().create();
        schema.propertyKey("uuid").asUUID().create();

        schema.propertyKey("list_string").asText().valueList().create();
        schema.propertyKey("list_bool").asBoolean().valueList().create();
        schema.propertyKey("list_byte").asByte().valueList().create();
        schema.propertyKey("list_int").asInt().valueList().create();
        schema.propertyKey("list_long").asLong().valueList().create();
        schema.propertyKey("list_float").asFloat().valueList().create();
        schema.propertyKey("list_double").asDouble().valueList().create();
        schema.propertyKey("list_blob").asBlob().valueList().create();
        schema.propertyKey("list_time").asDate().valueList().create();
        schema.propertyKey("list_uuid").asUUID().valueList().create();

        schema.propertyKey("set_string").asText().valueSet().create();
        schema.propertyKey("set_bool").asBoolean().valueSet().create();
        schema.propertyKey("set_byte").asByte().valueSet().create();
        schema.propertyKey("set_int").asInt().valueSet().create();
        schema.propertyKey("set_long").asLong().valueSet().create();
        schema.propertyKey("set_float").asFloat().valueSet().create();
        schema.propertyKey("set_double").asDouble().valueSet().create();
        schema.propertyKey("set_blob").asBlob().valueSet().create();
        schema.propertyKey("set_time").asDate().valueSet().create();
        schema.propertyKey("set_uuid").asUUID().valueSet().create();

        schema.vertexLabel("person")
              .properties("id", "string", "bool", "byte", "int",
                          "long", "float", "double",
                          "time", "uuid", "blob",
                          "list_string", "list_bool", "list_byte", "list_int",
                          "list_long", "list_float", "list_double",
                          "list_time", "list_uuid", "list_blob",
                          "set_string", "set_bool", "set_byte", "set_int",
                          "set_long", "set_float", "set_double",
                          "set_time", "set_uuid", "set_blob")
              .nullableKeys("string", "bool", "byte", "int",
                            "long", "float", "double",
                            "time", "uuid", "blob",
                            "list_string", "list_bool", "list_byte", "list_int",
                            "list_long", "list_float", "list_double",
                            "list_time", "list_uuid", "list_blob",
                            "set_string", "set_bool", "set_byte", "set_int",
                            "set_long", "set_float", "set_double",
                            "set_time", "set_uuid", "set_blob")
              .primaryKeys("id")
              .create();

        schema.edgeLabel("transfer")
              .properties("id", "string", "bool", "byte", "int",
                          "long", "float", "double",
                          "time", "uuid", "blob",
                          "list_string", "list_bool", "list_byte", "list_int",
                          "list_long", "list_float", "list_double",
                          "list_time", "list_uuid", "list_blob",
                          "set_string", "set_bool", "set_byte", "set_int",
                          "set_long", "set_float", "set_double",
                          "set_time", "set_uuid", "set_blob")
              .nullableKeys("string", "bool", "byte", "int",
                            "long", "float", "double",
                            "time", "uuid", "blob",
                            "list_string", "list_bool", "list_byte", "list_int",
                            "list_long", "list_float", "list_double",
                            "list_time", "list_uuid", "list_blob",
                            "set_string", "set_bool", "set_byte", "set_int",
                            "set_long", "set_float", "set_double",
                            "set_time", "set_uuid", "set_blob")
              .link("person", "person")
              .create();
    }

    public static class VertexPropertyCoreTest extends PropertyCoreTest {

        @Override
        protected <V> V property(String key, V value) {
            HugeGraph graph = graph();
            Vertex vertex = graph.addVertex(T.label, "person", "id", 1,
                                            key, value);
            graph.tx().commit();

            vertex = graph.vertices(vertex.id()).next();
            Assert.assertTrue(TraversalUtil.testProperty(vertex.property(key),
                                                         value));
            return vertex.value(key);
        }

        @Override
        protected <V> V propertyList(String key, Object... values) {
            HugeGraph graph = graph();
            key = "list_" + key;
            Vertex vertex = graph.addVertex(T.label, "person", "id", 2,
                                            key, Arrays.asList(values));
            graph.tx().commit();

            vertex = graph.vertices(vertex.id()).next();
            Assert.assertTrue(TraversalUtil.testProperty(vertex.property(key),
                                                         Arrays.asList(values)));
            return vertex.value(key);
        }

        @Override
        protected <V> V propertySet(String key, Object... values) {
            HugeGraph graph = graph();
            key = "set_" + key;
            Vertex vertex = graph.addVertex(T.label, "person", "id", 3,
                                            key, Arrays.asList(values));
            graph.tx().commit();

            vertex = graph.vertices(vertex.id()).next();
            Assert.assertTrue(TraversalUtil.testProperty(vertex.property(key),
                                                         ImmutableSet.copyOf(values)));
            Assert.assertFalse(TraversalUtil.testProperty(vertex.property(key),
                                                          ImmutableList.copyOf(values)));
            return vertex.value(key);
        }
    }

    public static class EdgePropertyCoreTest extends PropertyCoreTest {

        @Override
        protected <V> V property(String key, V value) {
            HugeGraph graph = graph();
            Vertex vertex1 = graph.addVertex(T.label, "person", "id", 1);
            Vertex vertex2 = graph.addVertex(T.label, "person", "id", 2);
            Edge edge = vertex1.addEdge("transfer", vertex2, "id", 1,
                                        key, value);
            graph.tx().commit();

            edge = graph.edges(edge.id()).next();
            Assert.assertTrue(TraversalUtil.testProperty(edge.property(key),
                                                         value));
            return edge.value(key);
        }

        @Override
        protected <V> V propertyList(String key, Object... values) {
            HugeGraph graph = graph();
            Vertex vertex1 = graph.addVertex(T.label, "person", "id", 1);
            Vertex vertex2 = graph.addVertex(T.label, "person", "id", 2);
            key = "list_" + key;
            Edge edge = vertex1.addEdge("transfer", vertex2, "id", 2,
                                        key, Arrays.asList(values));
            graph.tx().commit();

            edge = graph.edges(edge.id()).next();
            Assert.assertTrue(TraversalUtil.testProperty(edge.property(key),
                                                         Arrays.asList(values)));
            return edge.value(key);
        }

        @Override
        protected <V> V propertySet(String key, Object... values) {
            HugeGraph graph = graph();
            Vertex vertex1 = graph.addVertex(T.label, "person", "id", 1);
            Vertex vertex2 = graph.addVertex(T.label, "person", "id", 2);
            key = "set_" + key;
            Edge edge = vertex1.addEdge("transfer", vertex2, "id", 3,
                                        key, Arrays.asList(values));
            graph.tx().commit();

            edge = graph.edges(edge.id()).next();
            Assert.assertTrue(TraversalUtil.testProperty(edge.property(key),
                                                         ImmutableSet.copyOf(values)));
            Assert.assertFalse(TraversalUtil.testProperty(edge.property(key),
                                                          ImmutableList.copyOf(values)));
            return edge.value(key);
        }
    }

    @Test
    public void testTypeBoolean() {
        boolean gender = true;
        Assert.assertEquals(gender, property("bool", gender));

        gender = false;
        Assert.assertEquals(gender, property("bool", gender));

        List<Boolean> list = ImmutableList.of(true, false, true, false);
        Assert.assertEquals(list, propertyList("bool",
                                               true, false, true, false));

        Set<Boolean> set = ImmutableSet.of(true, false);
        Assert.assertEquals(set, propertySet("bool",
                                             true, false, true, false));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyList("bool", (byte) 1, true);
        }, e -> {
            Assert.assertContains("Invalid property value '[1, true]' " +
                                  "for key 'list_bool'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertySet("bool", (byte) 1, true);
        }, e -> {
            Assert.assertContains("Invalid property value '[1, true]' " +
                                  "for key 'set_bool'", e.getMessage());
        });
    }

    @Test
    public void testTypeByte() {
        Assert.assertEquals((byte) 3, property("byte", (byte) 3));
        Assert.assertEquals((byte) 3, property("byte", 3));
        Assert.assertEquals((byte) 18, property("byte", 18L));
        Assert.assertEquals(Byte.MIN_VALUE, property("byte", Byte.MIN_VALUE));
        Assert.assertEquals(Byte.MAX_VALUE, property("byte", Byte.MAX_VALUE));

        List<Byte> list = ImmutableList.of((byte) 1, (byte) 3, (byte) 3,
                                           (byte) 127, (byte) 128);
        Assert.assertEquals(list, propertyList("byte",
                                               (byte) 1, (byte) 3, (byte) 3,
                                               (byte) 127, (byte) 128));
        Assert.assertEquals(list, propertyList("byte",
                                               (byte) 1, 3, (long) 3,
                                               (byte) 127, (byte) 128));

        Set<Byte> set = ImmutableSet.of((byte) 1, (byte) 3,
                                        (byte) 127, (byte) 128);
        Assert.assertEquals(set, propertySet("byte",
                                             (byte) 1, (byte) 3, (byte) 3,
                                             (byte) 127, (byte) 128));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("byte", 1.0);
        }, e -> {
            Assert.assertContains("Invalid property value '1.0' " +
                                  "for key 'byte'", e.getMessage());
            Assert.assertContains("Can't read '1.0' as byte: ", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("byte", 128);
        }, e -> {
            Assert.assertContains("Invalid property value '128' " +
                                  "for key 'byte'", e.getMessage());
            Assert.assertContains("Can't read '128' as byte: " +
                                  "Value out of range", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("byte", "1");
        }, e -> {
            Assert.assertContains("Invalid property value '1' " +
                                  "for key 'byte'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyList("byte", (byte) 1, true);
        }, e -> {
            Assert.assertContains("Invalid property value '[1, true]' " +
                                  "for key 'list_byte'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyList("byte", (byte) 1, 128);
        }, e -> {
            Assert.assertContains("Invalid property value '[1, 128]' " +
                                  "for key 'list_byte'", e.getMessage());
            Assert.assertContains("Can't read '128' as byte: " +
                                  "Value out of range", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertySet("byte", (byte) 1, true);
        }, e -> {
            Assert.assertContains("Invalid property value '[1, true]' " +
                                  "for key 'set_byte'", e.getMessage());
        });
    }

    @Test
    public void testTypeInt() {
        Assert.assertEquals(18, property("int", 18));
        Assert.assertEquals(18, property("int", 18L));
        Assert.assertEquals(Integer.MAX_VALUE,
                            property("int", Integer.MAX_VALUE));
        Assert.assertEquals(Integer.MIN_VALUE,
                            property("int", Integer.MIN_VALUE));

        List<Integer> list = ImmutableList.of(1, 3, 3, 127, 128);
        Assert.assertEquals(list, propertyList("int",
                                               1, 3, 3, 127, 128));
        Assert.assertEquals(list, propertyList("int",
                                               1, 3, 3, (byte) 127, 128L));

        Set<Integer> set = ImmutableSet.of(1, 3, 127, 128);
        Assert.assertEquals(set, propertySet("int",
                                             1, 3, 3, 127, 128));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("int", 1.0);
        }, e -> {
            Assert.assertContains("Invalid property value '1.0' " +
                                  "for key 'int'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("int", Integer.MAX_VALUE + 1L);
        }, e -> {
            Assert.assertContains("Invalid property value '2147483648' " +
                                  "for key 'int'", e.getMessage());
            Assert.assertContains("Can't read '2147483648' as int: ",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("int", "1");
        }, e -> {
            Assert.assertContains("Invalid property value '1' " +
                                  "for key 'int'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyList("int", 1, true);
        }, e -> {
            Assert.assertContains("Invalid property value '[1, true]' " +
                                  "for key 'list_int'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyList("int", 1, Integer.MAX_VALUE + 1L);
        }, e -> {
            Assert.assertContains("Invalid property value '[1, 2147483648]' " +
                                  "for key 'list_int'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertySet("int", 1, true);
        }, e -> {
            Assert.assertContains("Invalid property value '[1, true]' " +
                                  "for key 'set_int'", e.getMessage());
        });
    }

    @Test
    public void testTypeLong() {
        Assert.assertEquals(18888888888L, property("long", 18888888888L));
        Assert.assertEquals(Long.MIN_VALUE, property("long", Long.MIN_VALUE));
        Assert.assertEquals(Long.MAX_VALUE, property("long", Long.MAX_VALUE));

        List<Long> list = ImmutableList.of(1L, 3L, 3L, 127L, 128L);
        Assert.assertEquals(list, propertyList("long",
                                               1L, 3L, 3L, 127L, 128L));
        Assert.assertEquals(list, propertyList("long",
                                               1, (byte) 3, 3,
                                               (short) 127, 128L));

        Set<Long> set = ImmutableSet.of(1L, 3L, 127L, 128L);
        Assert.assertEquals(set, propertySet("long",
                                             1, 3, 3, 127, 128));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("long", 1.0);
        }, e -> {
            Assert.assertContains("Invalid property value '1.0' " +
                                  "for key 'long'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("long", "1");
        }, e -> {
            Assert.assertContains("Invalid property value '1' " +
                                  "for key 'long'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyList("long", 1, true);
        }, e -> {
            Assert.assertContains("Invalid property value '[1, true]' " +
                                  "for key 'list_long'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertySet("long", 1, true);
        }, e -> {
            Assert.assertContains("Invalid property value '[1, true]' " +
                                  "for key 'set_long'", e.getMessage());
        });
    }

    @Test
    public void testTypeFloat() {
        Assert.assertEquals(1.86F, property("float", 1.86F));
        Assert.assertEquals(3.14f, property("float", 3.14d));
        Assert.assertEquals(Float.MIN_VALUE,
                            property("float", Float.MIN_VALUE));
        Assert.assertEquals(Float.MAX_VALUE,
                            property("float", Float.MAX_VALUE));
        Assert.assertEquals(Float.POSITIVE_INFINITY,
                            property("float", Float.POSITIVE_INFINITY));
        Assert.assertEquals(Float.POSITIVE_INFINITY,
                            property("float", Double.MAX_VALUE));
        Assert.assertEquals(Float.POSITIVE_INFINITY,
                            property("float", Double.POSITIVE_INFINITY));
        Assert.assertEquals(Float.NEGATIVE_INFINITY,
                            property("float", Float.NEGATIVE_INFINITY));
        Assert.assertEquals(Float.NEGATIVE_INFINITY,
                            property("float", -Double.MAX_VALUE));
        Assert.assertEquals(Float.POSITIVE_INFINITY,
                            property("float", -Double.NEGATIVE_INFINITY));
        Assert.assertEquals(Float.POSITIVE_INFINITY,
                            property("float", -2 * Double.NEGATIVE_INFINITY));
        Assert.assertEquals(Float.NaN, property("float", Float.NaN));

        List<Float> list = ImmutableList.of(1f, 3f, 3f, 127f, 128f);
        Assert.assertEquals(list, propertyList("float",
                                               1f, 3f, 3f, 127f, 128f));
        Assert.assertEquals(list, propertyList("float",
                                               1, (byte) 3, (long) 3,
                                               127f, 128d));

        Set<Float> set = ImmutableSet.of(1f, 3f, 127f, 128f);
        Assert.assertEquals(set, propertySet("float",
                                             1f, 3f, 3f, 127f, 128f));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("float", 'a');
        }, e -> {
            Assert.assertContains("Invalid property value " +
                                  "'a' for key 'float'",
                                  e.getMessage());
            Assert.assertContains("expect a value of type Float, " +
                                  "actual type Character",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("float", "1");
        }, e -> {
            Assert.assertContains("Invalid property value '1' " +
                                  "for key 'float'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyList("float", 1, true);
        }, e -> {
            Assert.assertContains("Invalid property value '[1, true]' " +
                                  "for key 'list_float'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertySet("float", 1, true);
        }, e -> {
            Assert.assertContains("Invalid property value '[1, true]' " +
                                  "for key 'set_float'", e.getMessage());
        });
    }

    @Test
    public void testTypeDouble() {
        Assert.assertEquals(1.86d, property("double", 1.86d));
        Assert.assertEquals(Double.MIN_VALUE,
                            property("double", Double.MIN_VALUE));
        Assert.assertEquals(Double.MAX_VALUE,
                            property("double", Double.MAX_VALUE));
        Assert.assertEquals(Double.POSITIVE_INFINITY,
                            property("double", Double.POSITIVE_INFINITY));
        Assert.assertEquals(Double.POSITIVE_INFINITY,
                            property("double", 2 * Double.POSITIVE_INFINITY));

        BigDecimal two = new BigDecimal(2);
        BigDecimal value = BigDecimal.valueOf(Double.MAX_VALUE).multiply(two);
        Assert.assertEquals(Double.POSITIVE_INFINITY,
                            property("double", value));
        Assert.assertEquals(Double.POSITIVE_INFINITY,
                            property("double", -Double.NEGATIVE_INFINITY));
        Assert.assertEquals(Double.POSITIVE_INFINITY,
                            property("double", -2 * Double.NEGATIVE_INFINITY));

        value = BigDecimal.valueOf(-Double.MAX_VALUE).multiply(two);
        Assert.assertEquals(Double.NEGATIVE_INFINITY,
                            property("double", value));
        value = BigDecimal.valueOf(Double.MIN_VALUE).divide(two);
        Assert.assertEquals(0.0, property("double", value));
        Assert.assertEquals(Double.NEGATIVE_INFINITY,
                            property("double", Double.NEGATIVE_INFINITY));
        Assert.assertEquals(Double.NaN, property("double", Double.NaN));

        List<Double> list = ImmutableList.of(1d, 3d, 3d, 127d, 128d);
        Assert.assertEquals(list, propertyList("double",
                                               1d, 3d, 3d, 127d, 128d));
        Assert.assertEquals(list, propertyList("double",
                                               1, (byte) 3, (long) 3,
                                               127f, 128d));

        Set<Double> set = ImmutableSet.of(1d, 3d, 127d, 128d);
        Assert.assertEquals(set, propertySet("double",
                                             1d, 3d, 3d, 127d, 128f));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("double", "1");
        }, e -> {
            Assert.assertContains("Invalid property value '1' " +
                                  "for key 'double'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyList("double", 1, true);
        }, e -> {
            Assert.assertContains("Invalid property value '[1, true]' " +
                                  "for key 'list_double'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertySet("double", 1, true);
        }, e -> {
            Assert.assertContains("Invalid property value '[1, true]' " +
                                  "for key 'set_double'", e.getMessage());
        });
    }

    @Test
    public void testTypeString() {
        Assert.assertEquals("Jame", property("string", "Jame"));

        List<String> list = ImmutableList.of("ab", "cde", "cde", "123");
        Assert.assertEquals(list, propertyList("string",
                                               "ab", "cde", "cde", "123"));

        Set<String> set = ImmutableSet.of("ab", "cde", "123");
        Assert.assertEquals(set, propertySet("string",
                                             "ab", "cde", "cde", "123", "cde"));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("string", 123);
        }, e -> {
            Assert.assertContains("Invalid property value '123' " +
                                  "for key 'string'", e.getMessage());
            Assert.assertContains(" expect a value of type String, " +
                                  "actual type Integer", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyList("string", "abc", true);
        }, e -> {
            Assert.assertContains("Invalid property value '[abc, true]' " +
                                  "for key 'list_string'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertySet("string", "abc", true);
        }, e -> {
            Assert.assertContains("Invalid property value '[abc, true]' " +
                                  "for key 'set_string'", e.getMessage());
        });
    }

    @Test
    public void testTypeDate() throws ParseException {
        Date time = new Date();
        Assert.assertEquals(time, property("time", time));

        Date expected = Utils.date("2018-12-12 00:00:00.000");
        String date = "2018-12-12";
        Assert.assertEquals(expected, property("time", date));
        date = "2018-12-12 00:00:00";
        Assert.assertEquals(expected, property("time", date));
        date = "2018-12-12 00:00:00.000";
        Assert.assertEquals(expected, property("time", date));

        Assert.assertEquals(expected, property("time", expected.getTime()));

        Date date1 = Utils.date("1970-01-01");
        Date date2 = Utils.date("2019-12-12");
        Date date3 = Utils.date("2020-5-28");

        List<Date> list = ImmutableList.of(date1, date2, date2, date3);
        Assert.assertEquals(list, propertyList("time",
                                               date1, date2, date2, date3));

        Set<Date> set = ImmutableSet.of(date1, date2, date3);
        Assert.assertEquals(set, propertySet("time",
                                             date1, date2, date2, date3));

        Assert.assertEquals(new Date(123), property("time", 123));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("time", 123f);
        }, e -> {
            Assert.assertContains("Invalid property value '123.0' " +
                                  "for key 'time'", e.getMessage());
            Assert.assertContains(" expect a value of type Date, " +
                                  "actual type Float", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyList("time", date1, true);
        }, e -> {
            Assert.assertContains(", true]' for key 'list_time'",
                                  e.getMessage());
            Assert.assertContains("expect a value of type List<Date>",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertySet("time", date1, true);
        }, e -> {
            Assert.assertContains(", true]' for key 'set_time'",
                                  e.getMessage());
            Assert.assertContains("expect a value of type Set<Date>",
                                  e.getMessage());
        });
    }

    @Test
    public void testTypeUUID() {
        UUID uid = UUID.randomUUID();
        Assert.assertEquals(uid, property("uuid", uid));

        UUID uid1 = UUID.randomUUID();
        UUID uid2 = UUID.randomUUID();
        UUID uid3 = UUID.randomUUID();

        List<UUID> list = ImmutableList.of(uid1, uid2, uid2, uid3);
        Assert.assertEquals(list, propertyList("uuid",
                                               uid1, uid2, uid2, uid3));

        Set<UUID> set = ImmutableSet.of(uid1, uid2, uid3);
        Assert.assertEquals(set, propertySet("uuid",
                                             uid1, uid2, uid2, uid3));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("uuid", 123f);
        }, e -> {
            Assert.assertContains("Invalid property value '123.0' " +
                                  "for key 'uuid'", e.getMessage());
            Assert.assertContains(" expect a value of type UUID, " +
                                  "actual type Float", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyList("uuid", uid1, true);
        }, e -> {
            Assert.assertContains(", true]' for key 'list_uuid'",
                                  e.getMessage());
            Assert.assertContains("expect a value of type List<UUID>",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertySet("uuid", uid1, true);
        }, e -> {
            Assert.assertContains(", true]' for key 'set_uuid'",
                                  e.getMessage());
            Assert.assertContains("expect a value of type Set<UUID>",
                                  e.getMessage());
        });
    }

    @Test
    public void testTypeBlob() {
        byte[] img = new byte[]{1, 2, 8, 50, 80, 96, 110, 125, -1, -10, -100};
        Assert.assertEquals(Blob.wrap(img), property("blob", img));

        Object buf = Blob.wrap(img);
        Assert.assertEquals(Blob.wrap(img), property("blob", buf));

        buf = BytesBuffer.wrap(img).forReadAll();
        Assert.assertEquals(Blob.wrap(img), property("blob", buf));

        Blob bytes = Blob.wrap(new byte[]{97, 49, 50, 51, 52});

        buf = ImmutableList.of((byte) 97, (byte) 49, 50, 51, 52);
        Assert.assertEquals(bytes, property("blob", buf));

        Object base64 = "YTEyMzQ=";
        Assert.assertEquals(bytes, property("blob", base64));

        Object hex = "0x6131323334";
        Assert.assertEquals(bytes, property("blob", hex));

        hex = "0x";
        Assert.assertEquals(Blob.wrap(new byte[]{}), property("blob", hex));

        Blob img1 = Blob.wrap(new byte[]{1, 2, 8, 50});
        Blob img2 = Blob.wrap(new byte[]{-1, -2, -8, -50});
        Blob img3 = Blob.wrap(new byte[]{1, 127, -128, 0});

        List<Blob> list = ImmutableList.of(img1, img2, img2, img3);
        Assert.assertEquals(list, propertyList("blob", img1, img2, img2, img3));

        Set<Blob> set = ImmutableSet.of(img1, img2, img2, img3);
        Assert.assertEquals(3, set.size());
        Assert.assertEquals(set, propertySet("blob", img1, img2, img2, img3));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Invalid base64
            property("blob", "#");
        }, e -> {
            Assert.assertContains("Invalid property value '#' for key 'blob'",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Invalid hex
            property("blob", "0xh");
        }, e -> {
            Assert.assertContains("Invalid property value '0xh' for key 'blob'",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("blob", 123f);
        }, e -> {
            Assert.assertContains("Invalid property value '123.0' " +
                                  "for key 'blob'", e.getMessage());
            Assert.assertContains("expect a value of type Blob, " +
                                  "actual type Float", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            property("blob", ImmutableList.of((byte) 97, 49, 50f));
        }, e -> {
            Assert.assertContains("Invalid property value '[97, 49, 50.0]' " +
                                  "for key 'blob'", e.getMessage());
            Assert.assertContains("expect byte or int value, but got '50.0'",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertyList("blob", img1, true);
        }, e -> {
            Assert.assertContains("Invalid property value '[Blob{01020832}, " +
                                  "true]' for key 'list_blob'", e.getMessage());
            Assert.assertContains("expect a value of type List<Blob>",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            propertySet("blob", img1, true);
        }, e -> {
            Assert.assertContains("Invalid property value '[Blob{01020832}, " +
                                  "true]' for key 'set_blob'", e.getMessage());
            Assert.assertContains("expect a value of type Set<Blob>",
                                  e.getMessage());
        });
    }
}
