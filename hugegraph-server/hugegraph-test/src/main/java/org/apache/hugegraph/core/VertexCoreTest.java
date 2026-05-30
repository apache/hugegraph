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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.Id.IdType;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.id.SnowflakeIdGenerator;
import org.apache.hugegraph.backend.id.SplicingIdGenerator;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.backend.store.BackendTable;
import org.apache.hugegraph.backend.store.Shard;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.exception.LimitExceedException;
import org.apache.hugegraph.exception.NoIndexException;
import org.apache.hugegraph.exception.NotAllowException;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.FakeObjects;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.traversal.optimize.ConditionP;
import org.apache.hugegraph.traversal.optimize.Text;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.GraphReadMode;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.type.define.WriteType;
import org.apache.hugegraph.util.Blob;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.LongEncoding;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class VertexCoreTest extends BaseCoreTest {

    @Before
    public void initSchema() {
        SchemaManager schema = graph().schema();

        LOG.debug("===============  propertyKey  ================");

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("dynamic").asBoolean().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("comment").asText().valueList().create();
        schema.propertyKey("contribution").asText().valueSet().create();
        schema.propertyKey("lived").asText().create();
        schema.propertyKey("description").asText().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("cpu").asText().create();
        schema.propertyKey("ram").asText().create();
        schema.propertyKey("band").asText().create();
        schema.propertyKey("price").asInt().create();
        schema.propertyKey("weight").asDouble().create();
        schema.propertyKey("birth").asDate().create();

        LOG.debug("===============  vertexLabel  ================");

        schema.vertexLabel("person")
              .properties("name", "age", "city", "birth")
              .primaryKeys("name")
              .nullableKeys("age", "birth")
              .create();
        schema.vertexLabel("computer")
              .properties("name", "band", "cpu", "ram", "price")
              .primaryKeys("name", "band")
              .nullableKeys("ram", "cpu", "price")
              .ifNotExist()
              .create();
        schema.vertexLabel("author")
              .properties("id", "name", "age", "lived")
              .primaryKeys("id")
              .nullableKeys("age", "lived")
              .create();
        schema.vertexLabel("language")
              .properties("name", "dynamic")
              .primaryKeys("name")
              .nullableKeys("dynamic")
              .create();
        schema.vertexLabel("book")
              .properties("name", "price")
              .primaryKeys("name")
              .nullableKeys("price")
              .create();
        schema.vertexLabel("review")
              .properties("id", "comment", "contribution")
              .primaryKeys("id")
              .nullableKeys("comment", "contribution")
              .create();
        schema.vertexLabel("fan")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .ttl(3000L)
              .ifNotExist()
              .create();
        schema.vertexLabel("follower")
              .properties("name", "age", "city", "birth")
              .primaryKeys("name")
              .ttl(3000L)
              .ttlStartTime("birth")
              .ifNotExist()
              .create();
    }

    @After
    public void resetGraphMode() {
        // In OLAP-related tests, if an error occurs midway, the graph mode will not be reset.
        graph().readMode(GraphReadMode.OLTP_ONLY);
    }

    protected void initPersonIndex(boolean indexCity) {
        SchemaManager schema = graph().schema();

        LOG.debug("===============  person index  ================");

        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        schema.indexLabel("personByBirth").onV("person").range()
              .by("birth").create();
        if (indexCity) {
            schema.indexLabel("personByCity").onV("person").secondary()
                  .by("city").create();
        }
    }

    protected void initComputerIndex() {
        SchemaManager schema = graph().schema();

        LOG.debug("===============  computer index  ================");

        schema.indexLabel("pcByBand").onV("computer")
              .secondary().by("band")
              .ifNotExist()
              .create();
        schema.indexLabel("pcByCpuAndRamAndBand").onV("computer")
              .secondary().by("cpu", "ram", "band")
              .ifNotExist()
              .create();
    }

    @Test
    public void testAddVertex() {
        HugeGraph graph = graph();

        // Directly save into the backend
        graph.addVertex(T.label, "book", "name", "java-3");

        graph.addVertex(T.label, "person", "name", "Baby",
                        "city", "Hongkong", "age", 3);
        graph.addVertex(T.label, "person", "name", "James",
                        "city", "Beijing", "age", 19);
        graph.addVertex(T.label, "person", "name", "Tom Cat",
                        "city", "Beijing", "age", 20);
        graph.addVertex(T.label, "person", "name", "Lisa",
                        "city", "Beijing", "age", 20);
        graph.addVertex(T.label, "person", "name", "Hebe",
                        "city", "Taipei", "age", 21);
        this.commitTx();

        long count = graph.traversal().V().count().next();
        Assert.assertEquals(6, count);
    }

    @Test
    public void testAddVertexWithInvalidPropertyType() {
        HugeGraph graph = graph();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "book", "name", 18);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "person", "name", "Baby",
                            "city", "Hongkong", "age", "should-be-int");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "person", "name", "Baby",
                            "city", "Hongkong", "age", 18.0);
        });
    }

    @Test
    public void testAddVertexWithInvalidPropertyValueOfInt() {
        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("int").asInt().create();
        schema.vertexLabel("number").properties("int").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            long value = Integer.MAX_VALUE + 1L;
            graph.addVertex(T.label, "number", "int", value);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            long value = Integer.MIN_VALUE - 1L;
            graph.addVertex(T.label, "number", "int", value);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "number", "int", Long.MAX_VALUE);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "number", "int", Long.MIN_VALUE);
        });
    }

    @Test
    public void testAddVertexWithInvalidPropertyValueOfLong() {
        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("long").asLong().create();
        schema.vertexLabel("number").properties("long").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            BigDecimal one = new BigDecimal(1);
            BigDecimal value = new BigDecimal(Long.MAX_VALUE).add(one);
            graph.addVertex(T.label, "number", "long", value);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            BigDecimal one = new BigDecimal(-1);
            BigDecimal value = new BigDecimal(Long.MIN_VALUE).add(one);
            graph.addVertex(T.label, "number", "long", value);
        });
    }

    @Test
    public void testAddVertexWithInvalidPropertyValueOfFloat() {
        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("float").asFloat().create();
        schema.vertexLabel("number").properties("float").create();

        double value = Float.MIN_VALUE / 2.0d;
        float fvalue = graph.addVertex(T.label, "number", "float", value)
                            .value("float");
        Assert.assertEquals(0.0f, fvalue, 0.0d);

        fvalue = graph.addVertex(T.label, "number", "float", -value)
                      .value("float");
        Assert.assertEquals(0.0f, fvalue, 0.0d);

        fvalue = graph.addVertex(T.label, "number", "float", Double.MIN_VALUE)
                      .value("float");
        Assert.assertEquals(0.0f, fvalue, 0.0d);
    }

    @Test
    public void testAddVertexWithInvalidPropertyValueOfDouble() {
        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("double").asDouble().create();
        schema.vertexLabel("number").properties("double").create();

        BigDecimal two = new BigDecimal(2);
        BigDecimal value = new BigDecimal(Double.MIN_VALUE).divide(two);
        double dvalue = graph.addVertex(T.label, "number", "double", value)
                             .value("double");
        Assert.assertEquals(0.0d, dvalue, 0.0d);

        value = new BigDecimal(-Double.MIN_VALUE).divide(two);
        dvalue = graph.addVertex(T.label, "number", "double", value)
                      .value("double");
        Assert.assertEquals(0.0d, dvalue, 0.0d);
    }

    @Test
    public void testAddVertexWithPropertyList() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "review", "id", 1,
                                        "comment", "looks good!",
                                        "comment", "LGTM!");
        this.mayCommitTx();

        vertex = vertex("review", "id", 1);
        Assert.assertEquals(ImmutableList.of("looks good!", "LGTM!"),
                            vertex.value("comment"));
        List<Object> props = vertex.value("comment");
        Assert.assertEquals(2, props.size());
        Assert.assertEquals("looks good!", props.get(0));
        Assert.assertEquals("LGTM!", props.get(1));

        vertex = graph.addVertex(T.label, "review", "id", 2,
                                 "comment",
                                 ImmutableList.of("looks good 2!", "LGTM!"));
        this.mayCommitTx();

        vertex = vertex("review", "id", 2);
        Assert.assertEquals(ImmutableList.of("looks good 2!", "LGTM!"),
                            vertex.value("comment"));
        props = vertex.value("comment");
        Assert.assertEquals(2, props.size());
        Assert.assertEquals("looks good 2!", props.get(0));
        Assert.assertEquals("LGTM!", props.get(1));

        vertex = graph.addVertex(T.label, "review", "id", 3,
                                 "comment",
                                 new String[]{"looks good 3!", "LGTM!"});
        this.mayCommitTx();

        vertex = vertex("review", "id", 3);
        Assert.assertEquals(ImmutableList.of("looks good 3!", "LGTM!"),
                            vertex.value("comment"));
        props = vertex.value("comment");
        Assert.assertEquals(2, props.size());
        Assert.assertEquals("looks good 3!", props.get(0));
        Assert.assertEquals("LGTM!", props.get(1));
    }

    @Test
    public void testAddVertexWithInvalidPropertyList() {
        HugeGraph graph = graph();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "review", "id", 1,
                            "comment", "looks good!",
                            "comment", 18);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "review", "id", 2,
                            "comment", ImmutableList.of("looks good 2!", 18));
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Object[] comments = new Object[]{"looks good 3!", "3"};
            comments[1] = 3;
            graph.addVertex(T.label, "review", "id", 3,
                            "comment", comments);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "review", "id", 3,
                            "comment", new int[]{1, 2});
        });
    }

    @Test
    public void testRemoveLeftRangeIndex() throws InterruptedException {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.propertyKey("updateTime").asLong().create();

        schema.vertexLabel("soft").properties("name", "updateTime")
              .primaryKeys("name").create();

        schema.indexLabel("softByUpdateTime").onV("soft").range()
              .by("updateTime").create();

        final long testDataCount = 10L;

        for (int i = 1; i <= testDataCount; i++) {
            graph.addVertex(T.label, "soft", "name", "soft" + i,
                            "updateTime", i);
        }
        this.commitTx();

        long count = graph.traversal().V()
                          .has("soft", "updateTime",
                               ConditionP.gt(0))
                          .limit(-1)
                          .count()
                          .next();
        Assert.assertEquals(testDataCount, count);

        for (int i = 1; i <= testDataCount; i++) {
            graph.addVertex(T.label, "soft", "name", "soft" + i,
                            "updateTime", 2 * i);
        }
        this.commitTx();

        count = graph.traversal().V()
                     .has("soft", "updateTime",
                          ConditionP.gt(0))
                     .limit(-1)
                     .count()
                     .next();
        Assert.assertEquals(testDataCount, count);

        schema.propertyKey("score").asInt().create();
        schema.vertexLabel("developer").properties("name", "age", "score")
              .primaryKeys("name").create();

        schema.indexLabel("developerByAge").onV("developer").range()
              .by("age").create();
        schema.indexLabel("developerByScore").onV("developer").range()
              .by("score").create();

        for (int i = 1; i <= testDataCount; i++) {
            graph.addVertex(T.label, "developer", "name", "developer" + i,
                            "age", i,
                            "score", i);
        }
        this.commitTx();

        count = graph.traversal().V()
                     .hasLabel("developer")
                     .has("age", ConditionP.gt(0))
                     .has("score", ConditionP.gt(0))
                     .limit(-1)
                     .count()
                     .next();
        Assert.assertEquals(testDataCount, count);

        for (int i = 1; i <= testDataCount; i++) {
            graph.addVertex(T.label, "developer", "name", "developer" + i,
                            "age", i * 2,
                            "score", i * 2);
        }
        this.commitTx();

        count = graph.traversal().V()
                     .hasLabel("developer")
                     .has("age", ConditionP.gt(0))
                     .has("score", ConditionP.gt(0))
                     .limit(-1)
                     .count()
                     .next();

        Assert.assertEquals(testDataCount, count);

        // Wait left index to be removed
        Thread.sleep(2000);

        count = graph.traversal().V()
                     .hasLabel("developer")
                     .has("age", ConditionP.gt(0))
                     .limit(-1)
                     .count()
                     .next();
        // Debug stop here to see whether the left index be correctly determined
        Assert.assertEquals(testDataCount, count);

        // mock test removeLeftIndexIfNeeded
        boolean removeLeftIndexOnOverwrite =
                Whitebox.getInternalState(params().graphTransaction(),
                                          "removeLeftIndexOnOverwrite");
        Whitebox.setInternalState(params().graphTransaction(),
                                  "removeLeftIndexOnOverwrite", true);

        for (int i = 1; i <= testDataCount; i++) {
            graph.addVertex(T.label, "developer", "name", "developer" + i,
                            "age", i * 3,
                            "score", i * 3);
        }
        this.commitTx();

        if (!removeLeftIndexOnOverwrite) {
            Whitebox.setInternalState(params().graphTransaction(),
                                      "removeLeftIndexOnOverwrite", false);
        }

        count = graph.traversal().V()
                     .hasLabel("developer")
                     .has("age", ConditionP.gt(0))
                     .has("score", ConditionP.gt(0))
                     .limit(-1)
                     .count()
                     .next();
        Assert.assertEquals(testDataCount, count);
    }

    @Test
    public void testLeftUnionIndex() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.propertyKey("updateTime").asLong().create();
        schema.propertyKey("country").asText().create();

        schema.vertexLabel("soft").properties("name", "country", "updateTime")
              .primaryKeys("name").create();

        schema.indexLabel("softByCountryAndUpdateTime").onV("soft").secondary()
              .by("country", "updateTime").create();

        final long testDataCount = 2L;

        for (int i = 1; i <= testDataCount; i++) {
            graph.addVertex(T.label, "soft", "name", "soft" + i,
                            "country", "china", "updateTime", i);
        }
        this.commitTx();

        long count = graph.traversal().V()
                          .hasLabel("soft")
                          .has("updateTime", 2L)
                          .has("country", "china")
                          .limit(-1)
                          .count()
                          .next();
        Assert.assertEquals(1, count);

        for (int i = 1; i <= testDataCount; i++) {
            graph.addVertex(T.label, "soft", "name", "soft" + i,
                            "country", "china", "updateTime", i * 2);
        }
        this.commitTx();

        count = graph.traversal().V()
                     .hasLabel("soft")
                     .has("updateTime", 2L)
                     .has("country", "china")
                     .limit(-1)
                     .count()
                     .next();
        Assert.assertEquals(1L, count);
    }

    @Test
    public void testAddVertexWithPropertySet() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "review", "id", 1,
                                        "contribution", "+1",
                                        "contribution", "+2",
                                        "contribution", "+2");
        this.mayCommitTx();

        vertex = vertex("review", "id", 1);
        Assert.assertEquals(ImmutableSet.of("+1", "+2"),
                            vertex.value("contribution"));

        vertex = graph.addVertex(T.label, "review", "id", 2,
                                 "contribution",
                                 ImmutableSet.of("+1", "+1", "+2"));
        this.mayCommitTx();

        vertex = vertex("review", "id", 2);
        Assert.assertEquals(ImmutableSet.of("+1", "+2"),
                            vertex.value("contribution"));
    }

    @Test
    public void testAddVertexWithCollectionIndex() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("tags").asText().valueSet().create();
        schema.propertyKey("category").asText().valueSet().create();
        schema.propertyKey("country").asText().create();
        schema.propertyKey("score").asInt().valueSet().create();

        schema.vertexLabel("soft").properties("name", "tags", "score",
                                              "country", "category")
              .primaryKeys("name")
              .create();

        schema.indexLabel("softByTag").onV("soft").by("tags")
              .secondary().create();

        schema.indexLabel("softByScore").onV("soft").by("score")
              .secondary().create();

        schema.indexLabel("softByCategory").onV("soft").by("category")
              .search().create();

        HugeGraph graph = graph();

        graph.addVertex(T.label, "soft", "name", "hugegraph",
                        "country", "china",
                        "score", ImmutableList.of(5, 4, 3),
                        "category", ImmutableList.of("graph database", "db"),
                        "tags", ImmutableList.of("graphdb", "gremlin"));

        graph.addVertex(T.label, "soft", "name", "neo4j",
                        "country", "usa",
                        "score", ImmutableList.of(5, 4),
                        "category", ImmutableList.of("graphdb", "database"),
                        "tags", ImmutableList.of("graphdb", "cypher"));

        graph.addVertex(T.label, "soft", "name", "janusgraph",
                        "country", "usa",
                        "score", ImmutableList.of(5),
                        "category",
                        ImmutableList.of("hello graph", "graph database"),
                        "tags", ImmutableList.of("graphdb", "gremlin"));

        // TODO: test mayCommitTx() after support textContains(collection, str)
        this.commitTx();

        List<Vertex> vertices;
        vertices = graph.traversal().V()
                        .has("soft", "category",
                             ConditionP.textContains("hello database"))
                        .toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph.traversal().V()
                        .has("soft", "category",
                             ConditionP.textContains("graph"))
                        .toList();
        Assert.assertEquals(2, vertices.size());

        Assert.assertThrows(IllegalStateException.class, () -> {
            graph.traversal().V().has("soft", "tags", "gremlin").toList();
        });

        // query by contains
        vertices = graph.traversal().V()
                        .has("soft", "tags", ConditionP.contains("gremlin"))
                        .toList();
        Assert.assertEquals(2, vertices.size());

        // secondary-index with list/set of number properties
        vertices = graph.traversal().V()
                        .has("soft", "score", ConditionP.contains(5))
                        .toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph.traversal().V()
                        .has("soft", "name", "hugegraph").toList();
        Assert.assertEquals(1, vertices.size());

        // add a new tag
        Vertex vertex = vertices.get(0);
        vertex.property("tags", ImmutableSet.of("new_tag"));

        this.commitTx();

        vertices = graph.traversal().V()
                        .has("soft", "tags", ConditionP.contains("new_tag"))
                        .toList();
        Assert.assertEquals(1, vertices.size());

        // delete tag gremlin
        vertex = graph.addVertex(T.label, "soft", "name", "hugegraph",
                                 "country", "china",
                                 "score", ImmutableList.of(5, 4, 3),
                                 "category", ImmutableList.of("hello graph",
                                                              "graph database"),
                                 "tags", ImmutableList.of("graphdb",
                                                          "new_tag"));
        this.mayCommitTx();

        vertices = graph.traversal().V()
                        .has("soft", "tags", ConditionP.contains("gremlin"))
                        .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V()
                        .has("soft", "tags", ConditionP.contains("new_tag"))
                        .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V()
                        .has("soft", "tags", ConditionP.contains("graphdb"))
                        .toList();
        Assert.assertEquals(3, vertices.size());

        // remove
        vertex.remove();
        this.mayCommitTx();

        vertices = graph.traversal().V()
                        .has("soft", "tags", ConditionP.contains("new_tag"))
                        .toList();
        Assert.assertEquals(0, vertices.size());

        vertices = graph.traversal().V()
                        .has("soft", "tags", ConditionP.contains("graphdb"))
                        .toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testAddVertexWithInvalidPropertySet() {
        HugeGraph graph = graph();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "review", "id", 1,
                            "contribution", "+1",
                            "contribution", 2);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "review", "id", 2,
                            "contribution", ImmutableSet.of("+1", 2));
        });
    }

    @Test
    public void testAddVertexWithInvalidVertexLabelType() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, true);
        });
    }

    @Test
    public void testAddVertexWithNotExistsVertexLabel() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "not-exists-label");
        });
    }

    @Test
    public void testAddVertexWithNotExistsPropKey() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "book", "not-exists-prop", "test");
        });
    }

    @Test
    public void testAddVertexWithNullableKeyAbsent() {
        Vertex vertex = graph().addVertex(T.label, "person", "name", "Baby",
                                          "city", "Hongkong");
        Assert.assertEquals("Baby", vertex.value("name"));
        Assert.assertEquals("Hongkong", vertex.value("city"));
    }

    @Test
    public void testAddVertexLabelNewVertexWithNonNullKeysAbsent() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().tx().open();
            // Absent 'city'
            graph().addVertex(T.label, "person", "name", "Baby", "age", 18);
            graph().tx().commit();
        });
    }

    @Test
    public void testAddVertexWithAppendedNullableKeysAbsent() {
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").nullableKeys("city").append();
        // Absent 'age' and 'city'
        Vertex vertex = graph().addVertex(T.label, "person", "name", "Baby");
        Assert.assertEquals("Baby", vertex.value("name"));

        this.mayCommitTx();
    }

    @Test
    public void testAddVertexWithDefaultPropertyValue() {
        SchemaManager schema = graph().schema();

        schema.propertyKey("fav").asText()
              .userdata(Userdata.DEFAULT_VALUE, "Movie").create();
        schema.propertyKey("cnt").asInt()
              .userdata(Userdata.DEFAULT_VALUE, 123).create();
        schema.vertexLabel("person")
              .properties("fav", "cnt")
              .nullableKeys("fav", "cnt").append();

        // No 'fav'
        Vertex vertex = graph().addVertex(T.label, "person",
                                          "name", "Baby", "city", "Shanghai");
        Assert.assertEquals("Baby", vertex.value("name"));
        Assert.assertFalse(vertex.values("fav").hasNext());
        Assert.assertFalse(vertex.values("age").hasNext());

        vertex = graph().vertex(vertex.id());
        Assert.assertEquals("Baby", vertex.value("name"));
        Assert.assertFalse(vertex.values("fav").hasNext());
        Assert.assertFalse(vertex.values("cnt").hasNext());
        Assert.assertFalse(vertex.values("age").hasNext());

        this.commitTx();

        // Exist 'fav' after commit then query
        vertex = graph().vertex(vertex.id());
        Assert.assertEquals("Baby", vertex.value("name"));
        Assert.assertEquals("Movie", vertex.value("fav"));
        Assert.assertEquals(123, vertex.value("cnt"));
        Assert.assertFalse(vertex.values("age").hasNext());
    }

    @Test
    public void testAddVertexWithNotExistsVertexPropKey() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "book", "age", 12);
        });
    }

    @Test
    public void testAddVertexWithoutPrimaryValues() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "book");
        });
    }

    @Test
    public void testAddVertexWithPrimaryValuesEmpty() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "book", "name", "");
        });
    }

    @Test
    public void testAddVertexWithoutVertexLabel() {
        HugeGraph graph = graph();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex("name", "test");
        });
    }

    @Test
    public void testAddVertexWithAutomaticIdStrategyButPassedId() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .useAutomaticId()
              .properties("name", "age", "city")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "programmer", T.id, "123456",
                            "name", "marko", "age", 18, "city", "Beijing");
        });
    }

    @Test
    public void testAddVertexWithAutomaticIdStrategy() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        // May be set forceString=true by config
        Whitebox.setInternalState(SnowflakeIdGenerator.instance(graph),
                                  "forceString", false);

        schema.vertexLabel("programmer")
              .useAutomaticId()
              .properties("name", "age", "city")
              .create();

        Vertex v1 = graph.addVertex(T.label, "programmer", "name", "marko",
                                    "age", 18, "city", "Beijing");
        this.mayCommitTx();

        List<Vertex> vertices = graph.traversal().V().toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "programmer", "name", "marko",
                       "age", 18, "city", "Beijing");

        Vertex v2 = graph.addVertex(T.label, "programmer", "name", "marko",
                                    "age", 18, "city", "Beijing");
        this.mayCommitTx();

        Assert.assertNotEquals(v1.id(), v2.id());

        vertices = graph.traversal().V().toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V(v2.id()).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "programmer", "name", "marko",
                       "age", 18, "city", "Beijing");
    }

    @Test
    public void testAddVertexWithAutomaticIdStrategyAndForceStringId() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        // May be set forceString=false by config
        Whitebox.setInternalState(SnowflakeIdGenerator.instance(graph),
                                  "forceString", true);

        schema.vertexLabel("programmer")
              .useAutomaticId()
              .properties("name", "age", "city")
              .create();

        Vertex v1 = graph.addVertex(T.label, "programmer", "name", "marko",
                                    "age", 18, "city", "Beijing");
        this.mayCommitTx();

        List<Vertex> vertices = graph.traversal().V().toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "programmer", "name", "marko",
                       "age", 18, "city", "Beijing");

        Vertex v2 = graph.addVertex(T.label, "programmer", "name", "marko",
                                    "age", 18, "city", "Beijing");
        this.mayCommitTx();

        Assert.assertNotEquals(v1.id(), v2.id());

        vertices = graph.traversal().V().toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V(v2.id()).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "programmer", "name", "marko",
                       "age", 18, "city", "Beijing");
    }

    @Test
    public void testAddVertexWithPrimaryKeyIdStrategyButPassedId() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .usePrimaryKeyId()
              .properties("name", "age", "city")
              .primaryKeys("name", "age")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "programmer", T.id, "123456",
                            "name", "marko", "age", 18, "city", "Beijing");
        });
    }

    @Test
    public void testAddVertexWithPrimaryKeyIdStrategy() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .usePrimaryKeyId()
              .properties("name", "age", "city")
              .primaryKeys("name", "age")
              .create();
        graph.addVertex(T.label, "programmer", "name", "marko",
                        "age", 18, "city", "Beijing");
        this.mayCommitTx();

        String programmerId = graph.vertexLabel("programmer").id().asString();
        String vid = String.format("%s:%s!%s", programmerId, "marko", "1I");
        List<Vertex> vertices = graph.traversal().V(vid).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(vid, vertices.get(0).id().toString());
        assertContains(vertices,
                       T.label, "programmer", "name", "marko",
                       "age", 18, "city", "Beijing");
    }

    @Test
    public void testAddVertexWithCustomizeStringIdStrategy() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .useCustomizeStringId()
              .properties("name", "age", "city")
              .create();
        graph.addVertex(T.label, "programmer", T.id, "123456", "name", "marko",
                        "age", 18, "city", "Beijing");
        this.mayCommitTx();

        List<Vertex> vertices = graph.traversal().V("123456").toList();
        Assert.assertEquals(1, vertices.size());
        Id id = (Id) vertices.get(0).id();
        Assert.assertEquals(IdType.STRING, id.type());
        Assert.assertEquals("123456", id.asString());
        assertContains(vertices,
                       T.label, "programmer", "name", "marko",
                       "age", 18, "city", "Beijing");
    }

    @Test
    public void testAddVertexWithCustomizeStringIdStrategyWithoutValidId() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .useCustomizeStringId()
              .properties("name", "age", "city")
              .create();

        // Expect id, but no id
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "programmer", "name", "marko",
                            "age", 18, "city", "Beijing");
        });

        // Expect string id, but got number id
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "programmer", T.id, 123456,
                            "name", "marko", "age", 18, "city", "Beijing");
        });

        // Expect id length <= BytesBuffer.ID_LEN_MAX
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            String largeId = new String(new byte[BytesBuffer.ID_LEN_MAX]) + ".";
            assert largeId.length() == BytesBuffer.ID_LEN_MAX + 1;
            graph.addVertex(T.label, "programmer", T.id, largeId,
                            "name", "marko", "age", 18, "city", "Beijing");
        });
    }

    @Test
    public void testAddVertexWithCustomizeNumberIdStrategy() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .useCustomizeNumberId()
              .properties("name", "age", "city")
              .create();
        graph.addVertex(T.label, "programmer", T.id, 123456, "name", "marko",
                        "age", 18, "city", "Beijing");
        graph.addVertex(T.label, "programmer", T.id, 61695499031416832L,
                        "name", "marko", "age", 19, "city", "Beijing");
        this.mayCommitTx();

        List<Vertex> vertices = graph.traversal().V(123456).toList();
        Assert.assertEquals(1, vertices.size());
        Id id = (Id) vertices.get(0).id();
        Assert.assertEquals(IdType.LONG, id.type());
        Assert.assertEquals(123456, id.asLong());
        assertContains(vertices,
                       T.label, "programmer", "name", "marko",
                       "age", 18, "city", "Beijing");

        vertices = graph.traversal().V(61695499031416832L).toList();
        Assert.assertEquals(1, vertices.size());
        id = (Id) vertices.get(0).id();
        Assert.assertEquals(IdType.LONG, id.type());
        Assert.assertEquals(61695499031416832L, id.asLong());
        assertContains(vertices,
                       T.label, "programmer", "name", "marko",
                       "age", 19, "city", "Beijing");
    }

    @Test
    public void testAddVertexWithCustomizeNumberIdStrategyWithoutValidId() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .useCustomizeNumberId()
              .properties("name", "age", "city")
              .create();

        // Expect id, but no id
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "programmer", "name", "marko",
                            "age", 18, "city", "Beijing");
        });

        // Expect number id, but got string id
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "programmer", T.id, "123456",
                            "name", "marko", "age", 18, "city", "Beijing");
        });
    }

    @Test
    public void testAddVertexWithCustomizeUuidIdStrategy() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .useCustomizeUuidId()
              .properties("name", "age", "city")
              .create();
        graph.addVertex(T.label, "programmer",
                        T.id, "835e1153-9281-4957-8691-cf79258e90eb",
                        "name", "marko", "age", 18, "city", "Beijing");
        graph.addVertex(T.label, "programmer",
                        T.id, UUID.fromString(
                        "835e1153-9281-4957-8691-cf79258e90eb"),
                        "name", "marko", "age", 18, "city", "Beijing");
        graph.addVertex(T.label, "programmer",
                        T.id, "835e1153928149578691cf79258e90ee",
                        "name", "marko", "age", 19, "city", "Beijing");

        this.commitTx();
        Assert.assertEquals(2L, graph.traversal().V().count().next());

        Object uuid = Text.uuid("835e1153928149578691cf79258e90eb");
        List<Vertex> vertices = graph.traversal().V(uuid).toList();
        Assert.assertEquals(1, vertices.size());
        Id id = (Id) vertices.get(0).id();
        Assert.assertEquals(IdType.UUID, id.type());
        Assert.assertEquals("835e1153-9281-4957-8691-cf79258e90eb",
                            id.asString());
        assertContains(vertices,
                       T.label, "programmer", "name", "marko",
                       "age", 18, "city", "Beijing");

        uuid = Text.uuid("835e1153-9281-4957-8691-cf79258e90ee");
        vertices = graph.traversal().V(uuid).toList();
        Assert.assertEquals(1, vertices.size());
        id = (Id) vertices.get(0).id();
        Assert.assertEquals(IdType.UUID, id.type());
        Assert.assertEquals("835e1153-9281-4957-8691-cf79258e90ee",
                            id.asString());
        assertContains(vertices,
                       T.label, "programmer", "name", "marko",
                       "age", 19, "city", "Beijing");
    }

    @Test
    public void testAddVertexWithCustomizeUuidIdStrategyWithoutValidId() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .useCustomizeUuidId()
              .properties("name", "age", "city")
              .create();

        // Expect id, but no id
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "programmer", "name", "marko",
                            "age", 18, "city", "Beijing");
        });

        // Expect uuid id, but got number id
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "programmer", T.id, 123456,
                            "name", "marko", "age", 18, "city", "Beijing");
        });

        // Expect uuid id, but got invalid string id
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "programmer", T.id, "123456",
                            "name", "marko", "age", 18, "city", "Beijing");
        });
    }

    @Test
    public void testAddVertexWithTx() {
        HugeGraph graph = graph();
        GraphTransaction tx = params().openTransaction();

        tx.addVertex(T.label, "book", "name", "java-4");
        tx.addVertex(T.label, "book", "name", "java-5");

        try {
            tx.commit();
        } finally {
            tx.close();
        }

        long count = graph.traversal().V().count().next();
        Assert.assertEquals(2, count);
    }

    @Test
    public void testAddVertexWithTtl() {
        Vertex vertex = graph().addVertex(T.label, "fan", "name", "Baby",
                                          "age", 3, "city", "Beijing");
        this.commitTx();

        Iterator<Vertex> vertices = graph().vertices(vertex);
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex, vertices.next());
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }
        vertices = graph().vertices(vertex);
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testAddVertexWithTtlAndTtlStartTime() {
        Vertex vertex = graph().addVertex(
                T.label, "follower", "name",
                "Baby", "age", 3, "city", "Beijing",
                "birth", graph().now() - 1000L);
        this.commitTx();

        Iterator<Vertex> vertices = graph().vertices(vertex);
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex, vertices.next());
        this.commitTx();

        try {
            Thread.sleep(1100L);
        } catch (InterruptedException e) {
            // Ignore
        }
        vertices = graph().vertices(vertex);
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex, vertices.next());
        this.commitTx();

        try {
            Thread.sleep(1100L);
        } catch (InterruptedException e) {
            // Ignore
        }
        vertices = graph().vertices(vertex);
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testAddVertexWithSecondaryIndexAndTtl() {
        graph().schema().indexLabel("fanByCity").onV("fan").by("city")
               .secondary().ifNotExist().create();

        Vertex vertex = graph().addVertex(T.label, "fan", "name", "Baby",
                                          "age", 3, "city", "Beijing");
        this.commitTx();

        Iterator<Vertex> vertices = graph().traversal().V()
                                           .has("city", "Beijing");
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex, vertices.next());
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        vertices = graph().traversal().V().has("city", "Beijing");
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testAddVertexWithRangeIndexAndTtl() {
        graph().schema().indexLabel("fanByAge").onV("fan").by("age")
               .range().ifNotExist().create();

        Vertex vertex1 = graph().addVertex(T.label, "fan", "name", "Baby1",
                                           "age", 3, "city", "Beijing");
        @SuppressWarnings("unused")
        Vertex vertex2 = graph().addVertex(T.label, "fan", "name", "Baby2",
                                           "age", 4, "city", "Beijing");
        Vertex vertex3 = graph().addVertex(T.label, "fan", "name", "Baby3",
                                           "age", 5, "city", "Beijing");
        @SuppressWarnings("unused")
        Vertex vertex4 = graph().addVertex(T.label, "fan", "name", "Baby4",
                                           "age", 6, "city", "Beijing");
        Vertex vertex5 = graph().addVertex(T.label, "fan", "name", "Baby5",
                                           "age", 7, "city", "Beijing");
        this.commitTx();

        Iterator<Vertex> vertices = graph().traversal().V().has("age", 5);
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex3, vertices.next());

        vertices = graph().traversal().V().has("age", P.gt(6));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex5, vertices.next());

        vertices = graph().traversal().V().has("age", P.gte(6));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("age", P.lt(4));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex1, vertices.next());

        vertices = graph().traversal().V().has("age", P.lte(4));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        vertices = graph().traversal().V().has("age", 5);
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("age", P.gt(6));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("age", P.gte(6));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("age", P.lt(4));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("age", P.lte(4));
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testAddVertexWithShardIndexAndTtl() {
        graph().schema().indexLabel("fanByCityAndAge").onV("fan")
               .by("city", "age").shard().ifNotExist().create();

        Vertex vertex1 = graph().addVertex(T.label, "fan", "name", "Baby1",
                                           "age", 3, "city", "Beijing");
        @SuppressWarnings("unused")
        Vertex vertex2 = graph().addVertex(T.label, "fan", "name", "Baby2",
                                           "age", 4, "city", "Beijing");
        Vertex vertex3 = graph().addVertex(T.label, "fan", "name", "Baby3",
                                           "age", 5, "city", "Beijing");
        @SuppressWarnings("unused")
        Vertex vertex4 = graph().addVertex(T.label, "fan", "name", "Baby4",
                                           "age", 6, "city", "Beijing");
        Vertex vertex5 = graph().addVertex(T.label, "fan", "name", "Baby5",
                                           "age", 7, "city", "Beijing");
        Vertex vertex6 = graph().addVertex(T.label, "fan", "name", "Baby6",
                                           "age", 5, "city", "Shanghai");
        this.commitTx();

        Iterator<Vertex> vertices = graph().traversal().V().has("city",
                                                                "Shanghai");
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex6, vertices.next());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", 5);
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex3, vertices.next());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.gt(6));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex5, vertices.next());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.gte(6));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.lt(4));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex1, vertices.next());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.lte(4));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        vertices = graph().traversal().V().has("city", "Shanghai");
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", 5);
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.gt(6));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.gte(6));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.lt(4));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.lte(4));
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testAddVertexWithSearchIndexAndTtl() {
        graph().schema().indexLabel("fanByCity").onV("fan")
               .by("city").search().ifNotExist().create();

        Vertex vertex = graph().addVertex(T.label, "fan", "name", "Baby",
                                          "age", 3, "city", "Beijing Haidian");
        this.commitTx();

        Iterator<Vertex> vertices = graph().traversal().V().has(
                "city", Text.contains("Haidian"));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex, vertices.next());
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        vertices = graph().traversal().V().has("city",
                                               Text.contains("Haidian"));
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testAddVertexWithUniqueIndexAndTtl() {
        graph().schema().indexLabel("fanByCity").onV("fan")
               .by("city").unique().ifNotExist().create();

        graph().addVertex(T.label, "fan", "name", "Baby",
                          "age", 3, "city", "Beijing");
        this.commitTx();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().addVertex(T.label, "fan", "name", "Baby2",
                              "age", 3, "city", "Beijing");
            this.commitTx();
        });

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        graph().addVertex(T.label, "fan", "name", "Baby2",
                          "age", 3, "city", "Beijing");
        this.commitTx();
    }

    @Test
    public void testOverrideVertexWithSecondaryIndexAndTtl() {
        graph().schema().indexLabel("fanByCity").onV("fan").by("city")
               .secondary().ifNotExist().create();

        Vertex vertex = graph().addVertex(T.label, "fan", "name", "Baby",
                                          "age", 3, "city", "Beijing");
        this.commitTx();

        Iterator<Vertex> vertices = graph().traversal().V()
                                           .has("city", "Beijing");
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex, vertices.next());
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // Override
        vertex = graph().addVertex(T.label, "fan", "name", "Baby",
                                   "age", 3, "city", "Shanghai");
        this.commitTx();

        // Due to overridden vertices are expired,
        // query will lead to async delete
        vertices = graph().traversal().V().has("city", "Beijing");
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Shanghai");
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex, vertices.next());
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All edges are expired after 6s
        vertices = graph().traversal().V().has("city", "Beijing");
        Assert.assertFalse(vertices.hasNext());
        vertices = graph().traversal().V().has("city", "Shanghai");
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testOverrideVertexWithRangeIndexAndTtl() {
        graph().schema().indexLabel("fanByAge").onV("fan").by("age")
               .range().ifNotExist().create();

        Vertex vertex1 = graph().addVertex(T.label, "fan", "name", "Baby1",
                                           "age", 3, "city", "Beijing");
        @SuppressWarnings("unused")
        Vertex vertex2 = graph().addVertex(T.label, "fan", "name", "Baby2",
                                           "age", 5, "city", "Beijing");
        Vertex vertex3 = graph().addVertex(T.label, "fan", "name", "Baby3",
                                           "age", 7, "city", "Beijing");
        @SuppressWarnings("unused")
        Vertex vertex4 = graph().addVertex(T.label, "fan", "name", "Baby4",
                                           "age", 9, "city", "Beijing");
        Vertex vertex5 = graph().addVertex(T.label, "fan", "name", "Baby5",
                                           "age", 11, "city", "Beijing");
        this.commitTx();

        Iterator<Vertex> vertices = graph().traversal().V().has("age", 7);
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex3, vertices.next());

        vertices = graph().traversal().V().has("age", P.gt(9));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex5, vertices.next());

        vertices = graph().traversal().V().has("age", P.gte(9));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("age", P.lt(5));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex1, vertices.next());

        vertices = graph().traversal().V().has("age", P.lte(5));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // Override
        Vertex vertex6 = graph().addVertex(T.label, "fan", "name", "Baby6",
                                           "age", 4, "city", "Beijing");
        @SuppressWarnings("unused")
        Vertex vertex7 = graph().addVertex(T.label, "fan", "name", "Baby7",
                                           "age", 6, "city", "Beijing");
        Vertex vertex8 = graph().addVertex(T.label, "fan", "name", "Baby8",
                                           "age", 8, "city", "Beijing");
        @SuppressWarnings("unused")
        Vertex vertex9 = graph().addVertex(T.label, "fan", "name", "Baby9",
                                           "age", 10, "city", "Beijing");
        Vertex vertex10 = graph().addVertex(T.label, "fan", "name", "Baby10",
                                            "age", 12, "city", "Beijing");
        this.commitTx();
        /*
         * Due to overridden vertices are expired,
         * query will lead to async delete
         */
        vertices = graph().traversal().V().has("age", 7);
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("age", 8);
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex8, vertices.next());

        vertices = graph().traversal().V().has("age", P.gt(10));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex10, vertices.next());

        vertices = graph().traversal().V().has("age", P.gte(10));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("age", P.lt(6));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex6, vertices.next());

        vertices = graph().traversal().V().has("age", P.lte(6));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All edges are expired after 6s
        vertices = graph().traversal().V().has("age", 7);
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("age", 8);
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("age", P.gt(10));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("age", P.gte(10));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("age", P.lt(6));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("age", P.lte(6));
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testOverrideVertexWithShardIndexAndTtl() {
        graph().schema().indexLabel("fanByCityAndAge").onV("fan")
               .by("city", "age").shard().ifNotExist().create();

        Vertex vertex1 = graph().addVertex(T.label, "fan", "name", "Baby1",
                                           "age", 1, "city", "Beijing");
        @SuppressWarnings("unused")
        Vertex vertex2 = graph().addVertex(T.label, "fan", "name", "Baby2",
                                           "age", 3, "city", "Beijing");
        Vertex vertex3 = graph().addVertex(T.label, "fan", "name", "Baby3",
                                           "age", 5, "city", "Beijing");
        @SuppressWarnings("unused")
        Vertex vertex4 = graph().addVertex(T.label, "fan", "name", "Baby4",
                                           "age", 7, "city", "Beijing");
        Vertex vertex5 = graph().addVertex(T.label, "fan", "name", "Baby5",
                                           "age", 9, "city", "Beijing");
        Vertex vertex6 = graph().addVertex(T.label, "fan", "name", "Baby6",
                                           "age", 5, "city", "Shanghai");
        this.commitTx();

        Iterator<Vertex> vertices = graph().traversal().V().has("city",
                                                                "Shanghai");
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex6, vertices.next());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", 5);
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex3, vertices.next());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.gt(7));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex5, vertices.next());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.gte(7));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.lt(3));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex1, vertices.next());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.lte(3));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        Vertex vertex7 = graph().addVertex(T.label, "fan", "name", "Baby7",
                                           "age", 2, "city", "Beijing");
        @SuppressWarnings("unused")
        Vertex vertex8 = graph().addVertex(T.label, "fan", "name", "Baby8",
                                           "age", 4, "city", "Beijing");
        Vertex vertex9 = graph().addVertex(T.label, "fan", "name", "Baby9",
                                           "age", 6, "city", "Beijing");
        @SuppressWarnings("unused")
        Vertex vertex10 = graph().addVertex(T.label, "fan",
                                            "name", "Baby10", "age", 8,
                                            "city", "Beijing");
        Vertex vertex11 = graph().addVertex(T.label, "fan",
                                            "name", "Baby101", "age", 10,
                                            "city", "Beijing");
        Vertex vertex12 = graph().addVertex(T.label, "fan",
                                            "name", "Baby12", "age", 6,
                                            "city", "Shenzhen");
        this.commitTx();

        // Due to overridden vertices are expired, query will lead to async delete
        vertices = graph().traversal().V().has("city", "Shanghai");
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Shenzhen");
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex12, vertices.next());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", 5);
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", 6);
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex9, vertices.next());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.gt(8));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex11, vertices.next());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.gte(8));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.lt(4));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex7, vertices.next());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.lte(4));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All vertices are expired after 6s
        vertices = graph().traversal().V().has("city", "Shanghai");
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Shenzhen");
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", 5);
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", 6);
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.gt(8));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.gte(8));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.lt(4));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.lte(4));
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testOverrideVertexWithSearchIndexAndTtl() {
        graph().schema().indexLabel("fanByCity").onV("fan")
               .by("city").search().ifNotExist().create();

        Vertex vertex = graph().addVertex(T.label, "fan", "name", "Baby1",
                                          "age", 3, "city", "Beijing Haidian");
        this.commitTx();

        Iterator<Vertex> vertices = graph().traversal().V().has(
                "city", Text.contains("Haidian"));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex, vertices.next());
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        vertex = graph().addVertex(T.label, "fan", "name", "Baby2",
                                   "age", 3, "city", "Shanghai Pudong");
        this.commitTx();

        // Due to overridden vertices are expired, query will lead to async delete
        vertices = graph().traversal().V().has("city",
                                               Text.contains("Haidian"));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city",
                                               Text.contains("Pudong"));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex, vertices.next());
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All vertices are expired after 6s
        vertices = graph().traversal().V().has("city",
                                               Text.contains("Pudong"));
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testQueryVertexWithTtlInTx() {
        Vertex vertex1 = graph().addVertex(T.label, "fan", "name", "Baby1",
                                           "age", 3, "city", "Beijing");
        this.commitTx();
        // Add vertices in tx
        Vertex vertex2 = graph().addVertex(T.label, "fan", "name", "Baby2",
                                           "age", 3, "city", "Beijing");

        Iterator<Vertex> vertices = graph().vertices(vertex1);
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex1, vertices.next());

        vertices = graph().vertices(vertex2);
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(vertex2, vertices.next());
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All vertices are expired after 3s
        vertices = graph().vertices(vertex1);
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().vertices(vertex2);
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testQueryVertexWithSecondaryIndexAndTtlInTx() {
        graph().schema().indexLabel("fanByCity").onV("fan").by("city")
               .secondary().ifNotExist().create();

        graph().addVertex(T.label, "fan", "name", "Baby1",
                          "age", 3, "city", "Beijing");
        this.commitTx();
        // Add vertices in tx
        graph().addVertex(T.label, "fan", "name", "Baby2",
                          "age", 3, "city", "Beijing");

        Iterator<Vertex> vertices = graph().traversal().V()
                                           .has("city", "Beijing");
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All vertices are expired after 3s
        vertices = graph().traversal().V().has("city", "Beijing");
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testQueryVertexWithRangeIndexAndTtlInTx() {
        graph().schema().indexLabel("fanByAge").onV("fan").by("age")
               .range().ifNotExist().create();

        graph().addVertex(T.label, "fan", "name", "Baby1",
                          "age", 3, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby2",
                          "age", 4, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby3",
                          "age", 5, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby4",
                          "age", 6, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby5",
                          "age", 7, "city", "Beijing");
        this.commitTx();

        // Add vertices in tx
        graph().addVertex(T.label, "fan", "name", "Baby6",
                          "age", 3, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby7",
                          "age", 4, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby8",
                          "age", 5, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby9",
                          "age", 6, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby10",
                          "age", 7, "city", "Beijing");

        Iterator<Vertex> vertices = graph().traversal().V().has("age", 5);
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("age", P.gt(6));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("age", P.gte(6));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(4, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("age", P.lt(4));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("age", P.lte(4));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(4, IteratorUtils.count(vertices));
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All vertices are expired after 3s
        vertices = graph().traversal().V().has("age", 5);
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("age", P.gt(6));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("age", P.gte(6));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("age", P.lt(4));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("age", P.lte(4));
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testQueryVertexWithShardIndexAndTtlInTx() {
        graph().schema().indexLabel("fanByCityAndAge").onV("fan")
               .by("city", "age").shard().ifNotExist().create();

        graph().addVertex(T.label, "fan", "name", "Baby1",
                          "age", 3, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby2",
                          "age", 4, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby3",
                          "age", 5, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby4",
                          "age", 6, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby5",
                          "age", 7, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby6",
                          "age", 5, "city", "Shanghai");
        this.commitTx();

        // Add vertices in tx
        graph().addVertex(T.label, "fan", "name", "Baby7",
                          "age", 3, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby8",
                          "age", 4, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby9",
                          "age", 5, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby10",
                          "age", 6, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby11",
                          "age", 7, "city", "Beijing");
        graph().addVertex(T.label, "fan", "name", "Baby12",
                          "age", 5, "city", "Shanghai");

        Iterator<Vertex> vertices = graph().traversal().V()
                                           .has("city", "Shanghai");
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", 5);
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.gt(6));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.gte(6));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(4, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.lt(4));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.lte(4));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(4, IteratorUtils.count(vertices));
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All vertices are expired after 3s
        vertices = graph().traversal().V().has("city", "Shanghai");
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", 5);
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.gt(6));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.gte(6));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.lt(4));
        Assert.assertFalse(vertices.hasNext());

        vertices = graph().traversal().V().has("city", "Beijing")
                          .has("age", P.lte(4));
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testQueryVertexWithSearchIndexAndTtlInTx() {
        graph().schema().indexLabel("fanByCity").onV("fan")
               .by("city").search().ifNotExist().create();

        graph().addVertex(T.label, "fan", "name", "Baby1",
                          "age", 3, "city", "Beijing Haidian");
        this.commitTx();

        graph().addVertex(T.label, "fan", "name", "Baby2",
                          "age", 3, "city", "Beijing Haidian");

        Iterator<Vertex> vertices = graph().traversal().V().has(
                "city", Text.contains("Haidian"));
        Assert.assertTrue(vertices.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(vertices));
        this.commitTx();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All vertices are expired after 3s
        vertices = graph().traversal().V().has("city", Text.contains("Haidian"));
        Assert.assertFalse(vertices.hasNext());
    }

    @Test
    public void testAddOlapNoneProperties() {
        Assume.assumeTrue("Not support olap properties",
                          storeFeatures().supportsOlapProperties());

        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        String olapPropName = "olap";
        schema.propertyKey(olapPropName)
              .asText().valueSingle()
              .writeType(WriteType.OLAP_COMMON)
              .ifNotExist().create();

        this.init10VerticesAndCommit();

        String author = graph.vertexLabel("author").id().asString();
        Id id1 = SplicingIdGenerator.splicing(author,
                                              LongEncoding.encodeNumber(1));
        Id id2 = SplicingIdGenerator.splicing(author,
                                              LongEncoding.encodeNumber(2));

        String language = graph.vertexLabel("language").id().asString();
        Id id3 = SplicingIdGenerator.splicing(language, "java");
        Id id4 = SplicingIdGenerator.splicing(language, "c++");
        Id id5 = SplicingIdGenerator.splicing(language, "python");

        String book = graph.vertexLabel("book").id().asString();
        Id id6 = SplicingIdGenerator.splicing(book, "java-1");
        Id id7 = SplicingIdGenerator.splicing(book, "java-2");
        Id id8 = SplicingIdGenerator.splicing(book, "java-3");
        Id id9 = SplicingIdGenerator.splicing(book, "java-4");
        Id id10 = SplicingIdGenerator.splicing(book, "java-5");

        graph.addVertex(T.id, id1.asObject(), olapPropName, "a");
        graph.addVertex(T.id, id2.asObject(), olapPropName, "b");
        graph.addVertex(T.id, id3.asObject(), olapPropName, "c");
        graph.addVertex(T.id, id4.asObject(), olapPropName, "d");
        graph.addVertex(T.id, id5.asObject(), olapPropName, "e");
        graph.addVertex(T.id, id6.asObject(), olapPropName, "f");
        graph.addVertex(T.id, id7.asObject(), olapPropName, "g");
        graph.addVertex(T.id, id8.asObject(), olapPropName, "h");
        graph.addVertex(T.id, id9.asObject(), olapPropName, "i");
        graph.addVertex(T.id, id10.asObject(), olapPropName, "j");

        this.commitTx();

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V().has(olapPropName, "a").hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V().has(olapPropName, "c").hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V().has(olapPropName, "f").hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        graph.readMode(GraphReadMode.ALL);
        Assert.assertThrows(NoIndexException.class, () -> {
            graph.traversal().V().has(olapPropName, "a").hasNext();
        });

        Assert.assertThrows(NoIndexException.class, () -> {
            graph.traversal().V().has(olapPropName, "c").hasNext();
        });

        Assert.assertThrows(NoIndexException.class, () -> {
            graph.traversal().V().has(olapPropName, "f").hasNext();
        });

        graph.readMode(GraphReadMode.OLTP_ONLY);
    }

    @Test
    public void testAddOlapSecondaryProperties() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        Assume.assumeTrue("Not support olap properties",
                          storeFeatures().supportsOlapProperties());

        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        String olapPropName = "wcc";
        schema.propertyKey(olapPropName)
              .asText().valueSingle()
              .writeType(WriteType.OLAP_SECONDARY)
              .ifNotExist().create();

        this.init10VerticesAndCommit();

        String author = graph.vertexLabel("author").id().asString();
        Id id1 = SplicingIdGenerator.splicing(author,
                                              LongEncoding.encodeNumber(1));
        Id id2 = SplicingIdGenerator.splicing(author,
                                              LongEncoding.encodeNumber(2));

        String language = graph.vertexLabel("language").id().asString();
        Id id3 = SplicingIdGenerator.splicing(language, "java");
        Id id4 = SplicingIdGenerator.splicing(language, "c++");
        Id id5 = SplicingIdGenerator.splicing(language, "python");

        String book = graph.vertexLabel("book").id().asString();
        Id id6 = SplicingIdGenerator.splicing(book, "java-1");
        Id id7 = SplicingIdGenerator.splicing(book, "java-2");
        Id id8 = SplicingIdGenerator.splicing(book, "java-3");
        Id id9 = SplicingIdGenerator.splicing(book, "java-4");
        Id id10 = SplicingIdGenerator.splicing(book, "java-5");

        graph.addVertex(T.id, id1.asObject(), olapPropName, "a");
        graph.addVertex(T.id, id2.asObject(), olapPropName, "b");
        graph.addVertex(T.id, id3.asObject(), olapPropName, "c");
        graph.addVertex(T.id, id4.asObject(), olapPropName, "d");
        graph.addVertex(T.id, id5.asObject(), olapPropName, "e");
        graph.addVertex(T.id, id6.asObject(), olapPropName, "f");
        graph.addVertex(T.id, id7.asObject(), olapPropName, "g");
        graph.addVertex(T.id, id8.asObject(), olapPropName, "h");
        graph.addVertex(T.id, id9.asObject(), olapPropName, "i");
        graph.addVertex(T.id, id10.asObject(), olapPropName, "j");

        this.commitTx();

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V().has(olapPropName, "a").hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V().has(olapPropName, "c").hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V().has(olapPropName, "f").hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        graph.readMode(GraphReadMode.ALL);
        List<Vertex> vertices = graph.traversal().V()
                                     .has(olapPropName, "a").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id1).next(), vertices.get(0));

        vertices = graph.traversal().V().has(olapPropName, "c").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id3).next(), vertices.get(0));

        vertices = graph.traversal().V().has(olapPropName, "f").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id6).next(), vertices.get(0));

        graph.readMode(GraphReadMode.OLTP_ONLY);
    }

    @Test
    public void testAddOlapRangeProperties() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        Assume.assumeTrue("Not support olap properties",
                          storeFeatures().supportsOlapProperties());

        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        String olapPropName = "pagerank";
        schema.propertyKey(olapPropName)
              .asDouble().valueSingle()
              .writeType(WriteType.OLAP_RANGE)
              .ifNotExist().create();

        this.init10VerticesAndCommit();

        String author = graph.vertexLabel("author").id().asString();
        Id id1 = SplicingIdGenerator.splicing(author,
                                              LongEncoding.encodeNumber(1));
        Id id2 = SplicingIdGenerator.splicing(author,
                                              LongEncoding.encodeNumber(2));

        String language = graph.vertexLabel("language").id().asString();
        Id id3 = SplicingIdGenerator.splicing(language, "java");
        Id id4 = SplicingIdGenerator.splicing(language, "c++");
        Id id5 = SplicingIdGenerator.splicing(language, "python");

        String book = graph.vertexLabel("book").id().asString();
        Id id6 = SplicingIdGenerator.splicing(book, "java-1");
        Id id7 = SplicingIdGenerator.splicing(book, "java-2");
        Id id8 = SplicingIdGenerator.splicing(book, "java-3");
        Id id9 = SplicingIdGenerator.splicing(book, "java-4");
        Id id10 = SplicingIdGenerator.splicing(book, "java-5");

        graph.addVertex(T.id, id1.asObject(), olapPropName, 0.1D);
        graph.addVertex(T.id, id2.asObject(), olapPropName, 0.2D);
        graph.addVertex(T.id, id3.asObject(), olapPropName, 0.3D);
        graph.addVertex(T.id, id4.asObject(), olapPropName, 0.4D);
        graph.addVertex(T.id, id5.asObject(), olapPropName, 0.5D);
        graph.addVertex(T.id, id6.asObject(), olapPropName, 0.6D);
        graph.addVertex(T.id, id7.asObject(), olapPropName, 0.7D);
        graph.addVertex(T.id, id8.asObject(), olapPropName, 0.8D);
        graph.addVertex(T.id, id9.asObject(), olapPropName, 0.9D);
        graph.addVertex(T.id, id10.asObject(), olapPropName, 1.0D);

        this.commitTx();

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V().has(olapPropName, 0.1D).hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V().has(olapPropName, 0.3D).hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V().has(olapPropName, 0.6D).hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        graph.traversal().V(id1).next();

        graph.readMode(GraphReadMode.ALL);

        List<Vertex> vertices = graph.traversal().V()
                                     .has(olapPropName, 0.1D).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id1).next(), vertices.get(0));

        vertices = graph.traversal().V().has(olapPropName, 0.3D).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id3).next(), vertices.get(0));

        vertices = graph.traversal().V().has(olapPropName, 0.6D).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id6).next(), vertices.get(0));

        vertices = graph.traversal().V().has(olapPropName, P.gt(0.9D)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id10).next(),
                            vertices.get(0));

        vertices = graph.traversal().V().has(olapPropName, P.lt(0.2D)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id1).next(), vertices.get(0));

        vertices = graph.traversal().V().has(olapPropName, P.gte(0.9D))
                        .toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(graph.traversal().V(id9).next()));
        Assert.assertTrue(vertices.contains(
                graph.traversal().V(id10).next()));

        vertices = graph.traversal().V().has(olapPropName, P.lte(0.2D))
                        .toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(graph.traversal().V(id1).next()));
        Assert.assertTrue(vertices.contains(graph.traversal().V(id2).next()));

        vertices = graph.traversal().V()
                        .has(olapPropName, P.inside(0.2D, 0.9D)).toList();
        Assert.assertEquals(6, vertices.size());
        Assert.assertTrue(vertices.contains(graph.traversal().V(id3).next()));
        Assert.assertTrue(vertices.contains(graph.traversal().V(id4).next()));
        Assert.assertTrue(vertices.contains(graph.traversal().V(id5).next()));
        Assert.assertTrue(vertices.contains(graph.traversal().V(id6).next()));
        Assert.assertTrue(vertices.contains(graph.traversal().V(id7).next()));
        Assert.assertTrue(vertices.contains(graph.traversal().V(id8).next()));

        graph.readMode(GraphReadMode.OLTP_ONLY);
    }

    @Test
    public void testAddOlapRangeAndOlapSecondaryProperties() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        Assume.assumeTrue("Not support olap properties",
                          storeFeatures().supportsOlapProperties());

        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        schema.propertyKey("pagerank")
              .asDouble().valueSingle()
              .writeType(WriteType.OLAP_RANGE)
              .ifNotExist().create();
        schema.propertyKey("wcc")
              .asText().valueSingle()
              .writeType(WriteType.OLAP_SECONDARY)
              .ifNotExist().create();

        this.init10VerticesAndCommit();

        String author = graph.vertexLabel("author").id().asString();
        Id id1 = SplicingIdGenerator.splicing(author,
                                              LongEncoding.encodeNumber(1));
        Id id2 = SplicingIdGenerator.splicing(author,
                                              LongEncoding.encodeNumber(2));

        String language = graph.vertexLabel("language").id().asString();
        Id id3 = SplicingIdGenerator.splicing(language, "java");
        Id id4 = SplicingIdGenerator.splicing(language, "c++");
        Id id5 = SplicingIdGenerator.splicing(language, "python");

        String book = graph.vertexLabel("book").id().asString();
        Id id6 = SplicingIdGenerator.splicing(book, "java-1");
        Id id7 = SplicingIdGenerator.splicing(book, "java-2");
        Id id8 = SplicingIdGenerator.splicing(book, "java-3");
        Id id9 = SplicingIdGenerator.splicing(book, "java-4");
        Id id10 = SplicingIdGenerator.splicing(book, "java-5");

        graph.addVertex(T.id, id1.asObject(), "pagerank", 0.1D);
        graph.addVertex(T.id, id2.asObject(), "pagerank", 0.2D);
        graph.addVertex(T.id, id3.asObject(), "pagerank", 0.3D);
        graph.addVertex(T.id, id4.asObject(), "pagerank", 0.4D);
        graph.addVertex(T.id, id5.asObject(), "pagerank", 0.5D);
        graph.addVertex(T.id, id6.asObject(), "pagerank", 0.6D);
        graph.addVertex(T.id, id7.asObject(), "pagerank", 0.7D);
        graph.addVertex(T.id, id8.asObject(), "pagerank", 0.8D);
        graph.addVertex(T.id, id9.asObject(), "pagerank", 0.9D);
        graph.addVertex(T.id, id10.asObject(), "pagerank", 1.0D);

        this.commitTx();

        graph.addVertex(T.id, id1.asObject(), "wcc", "a");
        graph.addVertex(T.id, id2.asObject(), "wcc", "a");
        graph.addVertex(T.id, id3.asObject(), "wcc", "c");
        graph.addVertex(T.id, id4.asObject(), "wcc", "c");
        graph.addVertex(T.id, id5.asObject(), "wcc", "c");
        graph.addVertex(T.id, id6.asObject(), "wcc", "f");
        graph.addVertex(T.id, id7.asObject(), "wcc", "f");
        graph.addVertex(T.id, id8.asObject(), "wcc", "f");
        graph.addVertex(T.id, id9.asObject(), "wcc", "f");
        graph.addVertex(T.id, id10.asObject(), "wcc", "f");

        this.commitTx();

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V()
                 .has("pagerank", 0.1D)
                 .has("wcc", "a")
                 .hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V()
                 .has("pagerank", 0.3D)
                 .has("wcc", "b")
                 .hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V()
                 .has("pagerank", 0.6D)
                 .has("wcc", "f")
                 .hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        graph.readMode(GraphReadMode.ALL);

        List<Vertex> vertices = graph.traversal().V()
                                     .has("pagerank", 0.1D)
                                     .has("wcc", "a")
                                     .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id1).next(), vertices.get(0));

        vertices = graph.traversal().V()
                        .has("pagerank", 0.3D)
                        .has("wcc", "c")
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id3).next(), vertices.get(0));

        vertices = graph.traversal().V()
                        .has("pagerank", 0.6D)
                        .has("wcc", "f")
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id6).next(), vertices.get(0));

        vertices = graph.traversal().V()
                        .has("pagerank", P.gt(0.9D))
                        .has("wcc", "f")
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id10).next(),
                            vertices.get(0));

        vertices = graph.traversal().V()
                        .has("pagerank", P.lt(0.2D))
                        .has("wcc", "a")
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id1).next(), vertices.get(0));

        vertices = graph.traversal().V()
                        .has("pagerank", P.gte(0.9D))
                        .has("wcc", "f")
                        .toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(graph.traversal().V(id9).next()));
        Assert.assertTrue(vertices.contains(graph.traversal().V(id10).next()));

        vertices = graph.traversal().V()
                        .has("pagerank", P.lte(0.2D))
                        .has("wcc", "a")
                        .toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(graph.traversal().V(id1).next()));
        Assert.assertTrue(vertices.contains(graph.traversal().V(id2).next()));

        vertices = graph.traversal().V()
                        .has("pagerank", P.inside(0.2D, 0.9D))
                        .has("wcc", "c")
                        .toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(graph.traversal().V(id3).next()));
        Assert.assertTrue(vertices.contains(graph.traversal().V(id4).next()));
        Assert.assertTrue(vertices.contains(graph.traversal().V(id5).next()));

        vertices = graph.traversal().V()
                        .has("pagerank", P.inside(0.2D, 0.9D))
                        .has("wcc", "f")
                        .toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(graph.traversal().V(id6).next()));
        Assert.assertTrue(vertices.contains(graph.traversal().V(id7).next()));
        Assert.assertTrue(vertices.contains(graph.traversal().V(id8).next()));

        graph.readMode(GraphReadMode.OLTP_ONLY);
    }

    @Test
    public void testQueryOlapRangeAndRegularSecondaryProperties() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        Assume.assumeTrue("Not support olap properties",
                          storeFeatures().supportsOlapProperties());

        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        schema.indexLabel("authorByAge").onV("author").range()
              .by("age").create();
        schema.indexLabel("authorByLived").onV("author").secondary()
              .by("lived").create();

        schema.propertyKey("pagerank")
              .asDouble().valueSingle()
              .writeType(WriteType.OLAP_RANGE)
              .ifNotExist().create();
        schema.propertyKey("wcc")
              .asText().valueSingle()
              .writeType(WriteType.OLAP_SECONDARY)
              .ifNotExist().create();

        this.init10VerticesAndCommit();

        String author = graph.vertexLabel("author").id().asString();
        Id id1 = SplicingIdGenerator.splicing(author,
                                              LongEncoding.encodeNumber(1));
        Id id2 = SplicingIdGenerator.splicing(author,
                                              LongEncoding.encodeNumber(2));

        graph.addVertex(T.id, id1.asObject(), "pagerank", 0.1D);
        graph.addVertex(T.id, id2.asObject(), "pagerank", 0.2D);

        this.commitTx();

        graph.addVertex(T.id, id1.asObject(), "wcc", "a");
        graph.addVertex(T.id, id2.asObject(), "wcc", "b");

        this.commitTx();

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V()
                 .has("pagerank", 0.1D)
                 .has("lived", "Canadian")
                 .hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V()
                 .has("wcc", "a")
                 .has("age", 62)
                 .hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        Assert.assertEquals(GraphReadMode.OLTP_ONLY, graph.readMode());
        Assert.assertThrows(NotAllowException.class, () -> {
            graph.traversal().V()
                 .has("pagerank", 0.1D)
                 .has("age", 62)
                 .hasNext();
        }, e -> {
            Assert.assertContains("Not allowed to query by olap property key",
                                  e.getMessage());
        });

        graph.readMode(GraphReadMode.ALL);

        List<Vertex> vertices = graph.traversal().V()
                                     .has("pagerank", 0.1D)
                                     .has("lived", "Canadian")
                                     .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id1).next(), vertices.get(0));

        vertices = graph.traversal().V()
                        .has("pagerank", P.lte(0.1D))
                        .has("age", P.gte(62))
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id1).next(), vertices.get(0));

        vertices = graph.traversal().V()
                        .has("pagerank", P.lte(0.1D))
                        .has("age", P.gte(62))
                        .has("lived", "Canadian")
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id1).next(), vertices.get(0));

        vertices = graph.traversal().V()
                        .has("age", P.gt(5))
                        .has("wcc", "b")
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(graph.traversal().V(id2).next(), vertices.get(0));

        vertices = graph.traversal().V()
                        .has("pagerank", P.gte(0.1D))
                        .has("age", P.gte(10))
                        .toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(graph.traversal().V(id1).next()));
        Assert.assertTrue(vertices.contains(graph.traversal().V(id2).next()));

        Set<Vertex> vertexSet = graph.traversal().V()
                                     .has("pagerank", P.lte(0.9D))
                                     .has("wcc", P.within("a", "b"))
                                     .has("age", P.gt(20))
                                     .has("lived", P.within("Canadian",
                                                            "California"))
                                     .toSet();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertexSet.contains(graph.traversal().V(id1).next()));
        Assert.assertTrue(vertexSet.contains(graph.traversal().V(id2).next()));

        graph.readMode(GraphReadMode.OLTP_ONLY);
    }

    @Test
    public void testQueryOlapWithUpdates() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        Assume.assumeTrue("Not support olap properties",
                          storeFeatures().supportsOlapProperties());

        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        schema.propertyKey("pagerank")
              .asDouble().valueSingle()
              .writeType(WriteType.OLAP_RANGE)
              .ifNotExist().create();

        this.init10VerticesAndCommit();

        String author = graph.vertexLabel("author").id().asString();
        Id id1 = SplicingIdGenerator.splicing(author,
                                              LongEncoding.encodeNumber(1));
        Id id2 = SplicingIdGenerator.splicing(author,
                                              LongEncoding.encodeNumber(2));

        graph.addVertex(T.id, id1.asObject(), "pagerank", 0.1D);
        graph.addVertex(T.id, id2.asObject(), "pagerank", 0.2D);

        graph.readMode(GraphReadMode.ALL);

        // FIXME: expect to throw error here

        //        Assert.assertThrows(IllegalArgumentException.class, () -> {
        //            graph.traversal().V().has("pagerank", 0.1D).toList();
        //        }, e -> {
        //            Assert.assertContains("not allowed to query by olap " +
        //                                 "when there are uncommitted records",
        //                                 e.toString());
        //        });

        this.commitTx();

        List<Vertex> vertices = graph.traversal().V()
                                     .has("pagerank", 0.2D)
                                     .toList();
        Assert.assertEquals(1, vertices.size());

        graph.readMode(GraphReadMode.OLTP_ONLY);
    }

    @Test
    public void testQueryAll() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query all
        List<Vertex> vertices = graph.traversal().V().toList();

        Assert.assertEquals(10, vertices.size());

        assertContains(vertices,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");

        assertContains(vertices, T.label, "language", "name", "java");

        assertContains(vertices, T.label, "book", "name", "java-1");
    }

    @Test
    public void testQueryAllWithGraphAPI() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query all
        List<Vertex> vertices = ImmutableList.copyOf(graph.vertices());

        Assert.assertEquals(10, vertices.size());

        assertContains(vertices,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");

        assertContains(vertices, T.label, "language", "name", "java");

        assertContains(vertices, T.label, "book", "name", "java-1");
    }

    @Test
    public void testQueryAllWithLimit() {
        HugeGraph graph = graph();
        this.init10VerticesAndCommit();

        // Query all with limit
        List<Vertex> vertices = graph.traversal().V().limit(6).toList();
        Assert.assertEquals(6, vertices.size());
    }

    @Test
    public void testQueryAllWithLimitAfterUncommittedDelete() {
        HugeGraph graph = graph();
        this.init10VerticesAndCommit();

        // Query all with limit after delete
        graph.traversal().V().limit(6).drop().iterate();

        // Query count with uncommitted records
        graph.traversal().V().count().next();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Query with limit
            graph.traversal().V().limit(3).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Query with offset
            graph.traversal().V().range(1, -1).toList();
        });

        // Query all with limit after delete once
        this.commitTx();
        Assert.assertEquals(4L, graph.traversal().V().count().next());
        List<Vertex> vertices = graph.traversal().V().limit(3).toList();
        Assert.assertEquals(3, vertices.size());

        // Query all with limit after delete twice
        graph.traversal().V().limit(3).drop().iterate();
        this.commitTx();
        vertices = graph.traversal().V().limit(3).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryAllWithLimitAfterUncommittedInsert() {
        HugeGraph graph = graph();
        this.init10VerticesAndCommit();

        // Query all with limit after insert
        graph.addVertex(T.label, "author", "id", 3,
                        "name", "test", "age", 88,
                        "lived", "California");

        // Query count with uncommitted records
        Assert.assertEquals(11L, graph.traversal().V().count().next());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Query with limit
            graph.traversal().V().limit(3).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Query with offset
            graph.traversal().V().range(1, -1).toList();
        });

        // Do commit
        this.commitTx();

        // Query count
        Assert.assertEquals(11L, graph.traversal().V().count().next());

        // Query with limit
        Assert.assertEquals(3L, graph.traversal().V().limit(3).toList().size());

        // Query with offset
        Assert.assertEquals(10L, graph.traversal().V().range(1, -1).toList().size());
    }

    @Test
    public void testQueryAllWithLimitByQueryVertices() {
        HugeGraph graph = graph();
        this.init10VerticesAndCommit();

        Query query = new Query(HugeType.VERTEX);
        query.limit(1);
        Iterator<Vertex> iter = graph.vertices(query);
        List<Vertex> vertices = IteratorUtils.list(iter);
        Assert.assertEquals(1, vertices.size());
        CloseableIterator.closeIterator(iter);
    }

    @Test
    public void testQueryAllWithLimit0() {
        HugeGraph graph = graph();
        this.init10VerticesAndCommit();

        // Query all with limit 0
        List<Vertex> vertices = graph.traversal().V().limit(0).toList();

        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryAllWithNoLimit() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query all with limit -1 (mean no-limit)
        List<Vertex> vertices = graph.traversal().V().limit(-1).toList();
        Assert.assertEquals(10, vertices.size());
    }

    @Test
    public void testQueryAllWithIllegalLimit() {
        HugeGraph graph = graph();
        this.init10VerticesAndCommit();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().limit(-2).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().limit(-18).toList();
        });
    }

    @Test
    public void testQueryAllWithOffset() {
        HugeGraph graph = graph();
        this.init10VerticesAndCommit();

        List<Vertex> vertices = graph.traversal().V().range(8, 100).toList();
        Assert.assertEquals(2, vertices.size());

        List<Vertex> vertices2 = graph.traversal().V().range(8, -1).toList();
        Assert.assertEquals(vertices, vertices2);
    }

    @Test
    public void testQueryAllWithOffsetAndLimit() {
        HugeGraph graph = graph();
        this.init10VerticesAndCommit();

        List<Vertex> vertices = graph.traversal().V().range(8, 9).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V().range(0, 4).toList();
        Assert.assertEquals(4, vertices.size());

        vertices = graph.traversal().V().range(-2, 4).toList();
        Assert.assertEquals(4, vertices.size());

        vertices = graph.traversal().V().range(10, -1).toList();
        Assert.assertEquals(0, vertices.size());

        vertices = graph.traversal().V().range(0, -1).toList();
        Assert.assertEquals(10, vertices.size());

        vertices = graph.traversal().V().range(-2, -1).toList();
        Assert.assertEquals(10, vertices.size());
    }

    @Test
    public void testQueryAllWithOffsetAndLimitWithMultiTimes() {
        HugeGraph graph = graph();
        this.init10VerticesAndCommit();

        List<Vertex> vertices = graph.traversal().V()
                                     .range(1, 6)
                                     .range(4, 8)
                                     .toList();
        // [5, 6)
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V()
                        .range(1, -1)
                        .range(6, 8)
                        .toList();
        // [7, 9)
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V()
                        .range(1, 6)
                        .range(6, 8)
                        .toList();
        // [7, 6) will be converted to NoneStep by EarlyLimitStrategy
        Assert.assertEquals(0, vertices.size());

        vertices = graph.traversal().V()
                        .range(1, 6)
                        .range(7, 8)
                        .toList();
        // [8, 6) will be converted to NoneStep by EarlyLimitStrategy
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryAllWithIllegalOffsetOrLimit() {
        HugeGraph graph = graph();
        this.init10VerticesAndCommit();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().range(8, 7).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().range(-1, -2).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().range(0, -2).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().range(-4, -2).toList();
        });
    }

    @Test
    public void testQueryWithSplicingId() {
        HugeGraph graph = graph();
        init10Vertices();

        List<Vertex> vertices = graph.traversal().V().toList();
        String bookId = graph.vertexLabel("book").id().asString();
        Assert.assertTrue(Utils.containsId(vertices,
                                           SplicingIdGenerator.splicing(bookId, "java-1")));
        Assert.assertTrue(Utils.containsId(vertices,
                                           SplicingIdGenerator.splicing(bookId, "java-3")));
        Assert.assertTrue(Utils.containsId(vertices,
                                           SplicingIdGenerator.splicing(bookId, "java-5")));
    }

    @Test
    public void testQueryById() {
        HugeGraph graph = graph();
        init10Vertices();

        String authorId = graph.vertexLabel("author").id().asString();
        // Query vertex by id
        Id id = SplicingIdGenerator.splicing(authorId, "11");
        List<Vertex> vertices = graph.traversal().V(id).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");

        vertices = graph.traversal().V().hasId(id).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");

        vertices = graph.traversal().V().has(T.id, id).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");
    }

    @Test
    public void testQueryByIdNotFound() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query vertex by id which not exists
        Id id = SplicingIdGenerator.splicing("author", "not-exists-id");
        Assert.assertTrue(graph.traversal().V(id).toList().isEmpty());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            graph.traversal().V(id).next();
        });
    }

    @Test
    public void testQueryByIdWithGraphAPI() {
        HugeGraph graph = graph();
        init10Vertices();

        Object id = graph.traversal().V().toList().get(0).id();
        List<Vertex> vertices = ImmutableList.copyOf(graph.vertices(id));
        Assert.assertEquals(1, vertices.size());

        vertices = ImmutableList.copyOf(graph.vertices(id, id));
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testQueryByIdWithGraphAPIAndNotCommittedUpdate() {
        HugeGraph graph = graph();
        init10Vertices();

        Vertex vertex = graph.traversal().V()
                             .hasLabel("author")
                             .has("id", 1).next();
        Object id = vertex.id();
        Assert.assertTrue(graph.vertices(id).hasNext());
        Assert.assertTrue(graph.vertices(id, id).hasNext());

        vertex.property("age", 66);

        List<Vertex> vertices = ImmutableList.copyOf(graph.vertices(id));
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(66, (int) vertices.get(0).value("age"));

        vertices = ImmutableList.copyOf(graph.vertices(id, id));
        Assert.assertEquals(2, vertices.size());
        Assert.assertEquals(66, (int) vertices.get(1).value("age"));
    }

    @Test
    public void testQueryByIdWithGraphAPIAndNotCommittedRemoved() {
        HugeGraph graph = graph();
        this.init10VerticesAndCommit();

        List<Vertex> vertices = graph.traversal().V()
                                     .hasLabel("author").toList();

        Vertex vertex1 = vertices.get(0);
        Vertex vertex2 = vertices.get(1);
        Assert.assertTrue(graph.vertices(vertex1.id()).hasNext());
        Assert.assertTrue(graph.vertices(vertex2.id()).hasNext());

        vertex1.remove();
        vertex2.remove();
        Assert.assertFalse(graph.vertices(vertex1.id()).hasNext());
        Assert.assertFalse(graph.vertices(vertex2.id()).hasNext());
        Assert.assertFalse(graph.vertices(vertex1.id(), vertex2.id())
                                .hasNext());

        graph.tx().rollback();
        Assert.assertTrue(graph.vertices(vertex1.id()).hasNext());
        Assert.assertTrue(graph.vertices(vertex2.id()).hasNext());
        Assert.assertTrue(graph.vertices(vertex1.id(), vertex2.id()).hasNext());
    }

    @Test
    public void testQueryByInvalidSysprop() {
        HugeGraph graph = graph();
        init10Vertices();

        List<Vertex> vertices = graph.traversal().V().hasLabel("author")
                                     .has("id", 1).toList();
        Assert.assertEquals(1, vertices.size());
        Vertex vertex = vertices.get(0);
        Id author = graph.vertexLabel(vertex.label()).id();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().hasLabel("author").has("ID", 1).toList();
        }, e -> {
            Assert.assertContains("Undefined property key: 'ID'",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().hasLabel("author").has("NAME", "n1").toList();
        }, e -> {
            Assert.assertContains("Undefined property key: 'NAME'",
                                  e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            query.eq(HugeKeys.LABEL, author);
            query.query((Id) vertex.id());
            graph.vertices(query).hasNext();
        }, e -> {
            Assert.assertContains("Not supported querying by id and conditions",
                                  e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            query.eq(HugeKeys.LABEL, author);
            query.eq(HugeKeys.NAME, "n1");
            graph.vertices(query).hasNext();
        }, e -> {
            Assert.assertContains("Not supported querying vertices by",
                                  e.getMessage());
            Assert.assertContains("NAME == n1", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            query.eq(HugeKeys.LABEL, author);
            query.eq(HugeKeys.NAME, "n2");
            query.query(Condition.eq(IdGenerator.of("fake"), "n3"));
            graph.vertices(query).hasNext();
        }, e -> {
            Assert.assertContains("Can't do index query with [",
                                  e.getMessage());
            Assert.assertContains("LABEL == ", e.getMessage());
            Assert.assertContains("NAME == n2", e.getMessage());
        });
    }

    @Test
    public void testQueryByLabel() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query by vertex label
        List<Vertex> vertices = graph.traversal().V().hasLabel("book").toList();
        String bookId = graph.vertexLabel("book").id().asString();

        Assert.assertEquals(5, vertices.size());
        Assert.assertTrue(Utils.containsId(vertices,
                                           SplicingIdGenerator.splicing(bookId, "java-1")));
        Assert.assertTrue(Utils.containsId(vertices,
                                           SplicingIdGenerator.splicing(bookId, "java-2")));
        Assert.assertTrue(Utils.containsId(vertices,
                                           SplicingIdGenerator.splicing(bookId, "java-3")));
        Assert.assertTrue(Utils.containsId(vertices,
                                           SplicingIdGenerator.splicing(bookId, "java-4")));
        Assert.assertTrue(Utils.containsId(vertices,
                                           SplicingIdGenerator.splicing(bookId, "java-5")));
    }

    @Test
    public void testQueryByLabelWithLimit() {
        HugeGraph graph = graph();
        this.init10VerticesAndCommit();

        // Query by vertex label with limit
        List<Vertex> vertices = graph.traversal().V().hasLabel("book")
                                     .limit(3).toList();
        Assert.assertEquals(3, vertices.size());

        // Query by vertex label with limit
        graph.traversal().V().hasLabel("book").limit(3).drop().iterate();
        this.commitTx();

        vertices = graph.traversal().V().hasLabel("book")
                        .limit(3).toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testQueryByLabelNotExists() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query by not exists vertex label
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().hasLabel("xx").toList();
        });
    }

    @Test
    public void testQueryByLabelAndKeyName() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query by vertex label and key-name
        List<Vertex> vertices = graph.traversal().V().hasLabel("language")
                                     .has("dynamic").toList();

        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "language", "name", "python", "dynamic", true);
    }

    @Test
    public void testQueryByPrimaryValues() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query vertex by primary-values
        List<Vertex> vertices = graph.traversal().V()
                                     .hasLabel("author")
                                     .has("id", 1).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");
    }

    @Test
    public void testQueryByPrimaryValuesAndProps() {
        HugeGraph graph = graph();
        init10Vertices();

        // Query vertex by primary-values
        List<Vertex> vertices = graph.traversal().V()
                                     .hasLabel("author").has("id", 1)
                                     .has("name", "James Gosling").toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");

        vertices = graph.traversal().V().hasLabel("author")
                        .has("id", 1).has("name", "fake-name").toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryFilterByPropName() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support CONTAINS_KEY query",
                          storeFeatures().supportsQueryWithContainsKey());
        init10Vertices();

        VertexLabel language = graph.vertexLabel("language");
        PropertyKey dynamic = graph.propertyKey("dynamic");
        // Query vertex by condition (does contain the property name?)
        ConditionQuery q = new ConditionQuery(HugeType.VERTEX);
        q.eq(HugeKeys.LABEL, language.id());
        q.key(HugeKeys.PROPERTIES, dynamic.id());
        List<Vertex> vertices = ImmutableList.copyOf(graph.vertices(q));

        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "language", "name", "python", "dynamic", true);

        // Query vertex by condition (does contain the property name?)
        q = new ConditionQuery(HugeType.VERTEX);
        q.key(HugeKeys.PROPERTIES, dynamic.id());
        vertices = ImmutableList.copyOf(graph.vertices(q));

        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "language", "name", "python", "dynamic", true);
    }

    @Test
    public void testQueryByHasKey() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support CONTAINS_KEY query",
                          storeFeatures().supportsQueryWithContainsKey());
        init10Vertices();

        List<Vertex> vertices = graph.traversal().V()
                                     .hasLabel("language")
                                     .hasKey("dynamic")
                                     .toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "language", "name", "python",
                       "dynamic", true);

        vertices = graph.traversal().V().hasKey("age").toList();
        Assert.assertEquals(2, vertices.size());
        assertContains(vertices,
                       T.label, "author", "id", 1,
                       "name", "James Gosling", "age", 62,
                       "lived", "Canadian");
        assertContains(vertices,
                       T.label, "author", "id", 2,
                       "name", "Guido van Rossum", "age", 61,
                       "lived", "California");
    }

    @Test
    public void testQueryByHasKeys() {
        HugeGraph graph = graph();
        init10Vertices();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().hasLabel("language")
                 .hasKey("dynamic", "name").toList();
        });
    }

    @Test
    public void testQueryByHasValue() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support CONTAINS query",
                          storeFeatures().supportsQueryWithContains());
        init10Vertices();

        List<Vertex> vertices = graph.traversal().V()
                                     .hasLabel("language").hasValue(true)
                                     .toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "language", "name", "python",
                       "dynamic", true);

        vertices = graph.traversal().V().hasValue(62).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "author", "id", 1,
                       "name", "James Gosling", "age", 62,
                       "lived", "Canadian");
    }

    @Test
    public void testQueryByHasValues() {
        HugeGraph graph = graph();
        init10Vertices();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().hasLabel("language")
                 .hasValue("python", "java").toList();
        });
    }

    @Test
    public void testQueryByStringPropWithOneResult() {
        // city is "Taipei"
        HugeGraph graph = graph();
        initPersonIndex(true);
        init5Persons();

        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("city", "Taipei")
                                     .toList();

        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "person", "name", "Hebe",
                       "city", "Taipei", "age", 21);
    }

    @Test
    public void testQueryByStringPropWithMultiResults() {
        // NOTE: InMemoryDBStore would fail due to it not support index ele-ids

        HugeGraph graph = graph();
        initPersonIndex(true);
        init5Persons();

        // city is "Beijing"
        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("city", "Beijing").toList();

        Assert.assertEquals(3, vertices.size());

        assertContains(vertices,
                       T.label, "person", "name", "James",
                       "city", "Beijing", "age", 19);
        assertContains(vertices,
                       T.label, "person", "name", "Tom Cat",
                       "city", "Beijing", "age", 20);
        assertContains(vertices,
                       T.label, "person", "name", "Lisa",
                       "city", "Beijing", "age", 20);

        // city is "Beijing" && limit 2
        vertices = graph.traversal().V().hasLabel("person")
                        .has("city", "Beijing").limit(2).toList();
        Assert.assertEquals(2, vertices.size());

        // limit after delete
        graph.traversal().V().hasLabel("person")
             .has("city", "Beijing").limit(2)
             .drop().iterate();
        this.commitTx();

        vertices = graph.traversal().V().hasLabel("person")
                        .has("city", "Beijing").limit(2).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryByIntPropWithOneResult() {
        // age = 19
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", 19).toList();

        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "person", "name", "James",
                       "city", "Beijing", "age", 19);
    }

    @Test
    public void testQueryByIntPropWithMultiResults() {
        // age = 20
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", 20).toList();

        Assert.assertEquals(2, vertices.size());

        assertContains(vertices,
                       T.label, "person", "name", "Tom Cat",
                       "city", "Beijing", "age", 20);
        assertContains(vertices,
                       T.label, "person", "name", "Lisa",
                       "city", "Beijing", "age", 20);
    }

    @Test
    public void testQueryByIntPropWithNonResult() {
        // age = 18
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", 18).toList();

        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryByIntPropWithDifferentDataType() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = graph.traversal().V().has("age", 21).toList();
        Assert.assertEquals(1, vertices.size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().has("age", 21.0).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().has("age", "21").toList();
        });
    }

    @Test
    public void testQueryByIntPropWithNegativeNumber() {
        HugeGraph graph = graph();
        initPersonIndex(false);

        graph.addVertex(T.label, "person", "name", "Louise",
                        "city", "Hongkong", "age", 17,
                        "birth", Utils.date("2012-01-01"));
        graph.addVertex(T.label, "person", "name", "Sean",
                        "city", "Beijing", "age", -10,
                        "birth", Utils.date("2029-01-01"));

        List<Vertex> vertices = graph.traversal().V().has("age", -10).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "person", "name", "Sean",
                       "city", "Beijing", "age", -10,
                       "birth", Utils.date("2029-01-01"));

        vertices = graph.traversal().V()
                        .has("age", P.between(-11, 0))
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("Sean", vertices.get(0).value("name"));

        vertices = graph.traversal().V().has("age", P.gt(-11)).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V().has("age", P.gte(-10)).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V().has("age", P.gt(-10)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("Louise", vertices.get(0).value("name"));

        vertices = graph.traversal().V().has("age", P.gt(-9)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("Louise", vertices.get(0).value("name"));

        vertices = graph.traversal().V().has("age", P.lt(-10)).toList();
        Assert.assertEquals(0, vertices.size());

        vertices = graph.traversal().V().has("age", P.lte(-10)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("Sean", vertices.get(0).value("name"));

        vertices = graph.traversal().V().has("age", P.lt(0)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("Sean", vertices.get(0).value("name"));
    }

    @Test
    public void testQueryByIntPropUsingLtWithOneResult() {
        // age < 19
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", P.lt(19)).toList();

        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "person", "name", "Baby",
                       "city", "Hongkong", "age", 3);
    }

    @Test
    public void testQueryByIntPropUsingLtWithMultiResults() {
        // age < 21
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", P.lt(21)).toList();

        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testQueryByIntPropUsingLteWithMultiResults() {
        // age <= 20
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", P.lte(20)).toList();

        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testQueryByIntPropUsingGtWithOneResult() {
        // age > 20
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", P.gt(20)).toList();

        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryByIntPropUsingGtWithMultiResults() {
        // age > 1
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", P.gt(1)).toList();

        Assert.assertEquals(5, vertices.size());
    }

    @Test
    public void testQueryByIntPropUsingGtWithNonResult() {
        // age > 30
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", P.gt(30)).toList();

        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryByIntPropUsingGteWithMultiResults() {
        // age >= 20
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", P.gte(20)).toList();

        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testQueryByIntPropUsingInsideWithOneResult() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        // 3 < age && age < 20 (that's age == 19)
        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", P.inside(3, 20)).toList();

        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(19, vertices.get(0).property("age").value());
    }

    @Test
    public void testQueryByIntPropUsingInsideWithMultiResults() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        // 19 < age && age < 21 (that's age == 20)
        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", P.inside(19, 21)).toList();

        Assert.assertEquals(2, vertices.size());

        // 3 < age && age < 21 (that's age == 19 or age == 20)
        vertices = graph.traversal().V().hasLabel("person")
                        .has("age", P.inside(3, 21)).toList();

        Assert.assertEquals(3, vertices.size());

        // 0 < age && age < 22 (that's all)
        vertices = graph.traversal().V().hasLabel("person")
                        .has("age", P.inside(0, 22)).toList();

        Assert.assertEquals(5, vertices.size());
    }

    @Test
    public void testQueryByIntPropUsingInsideWithNonResult() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        // 3 < age && age < 19
        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", P.inside(3, 19)).toList();

        Assert.assertEquals(0, vertices.size());

        // 0 < age && age < 3
        vertices = graph.traversal().V().hasLabel("person")
                        .has("age", P.inside(0, 3)).toList();

        Assert.assertEquals(0, vertices.size());

        // 20 < age && age < 21
        vertices = graph.traversal().V().hasLabel("person")
                        .has("age", P.inside(20, 21)).toList();

        Assert.assertEquals(0, vertices.size());

        // 21 < age && age < 25
        vertices = graph.traversal().V().hasLabel("person")
                        .has("age", P.inside(21, 25)).toList();

        Assert.assertEquals(0, vertices.size());

        // 21 < age && age < 20
        vertices = graph.traversal().V().hasLabel("person")
                        .has("age", P.inside(21, 20)).toList();

        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryByIntPropUsingBetweenWithOneResult() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        // 3 <= age && age < 19 (that's age == 3)
        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", P.between(3, 19)).toList();

        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryByIntPropUsingBetweenWithMultiResults() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        // 19 <= age && age < 21
        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", P.between(19, 21)).toList();

        Assert.assertEquals(3, vertices.size());

        // 3 <= age && age < 21
        vertices = graph.traversal().V().hasLabel("person")
                        .has("age", P.between(3, 21)).toList();

        Assert.assertEquals(4, vertices.size());

        // 3 <= age && age < 21 && limit 3
        vertices = graph.traversal().V().hasLabel("person")
                        .has("age", P.between(3, 21)).limit(3).toList();

        Assert.assertEquals(3, vertices.size());

        // limit after delete
        graph.traversal().V().hasLabel("person")
             .has("age", P.between(3, 21)).limit(3)
             .drop().iterate();
        this.commitTx();

        vertices = graph.traversal().V().hasLabel("person")
                        .has("age", P.between(3, 21)).limit(3).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryByIntPropUsingBetweenAfterPropOverride() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        // -1 <= age && age < 21
        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", P.between(-1, 21)).toList();
        Assert.assertEquals(4, vertices.size());

        // override vertex without age (in memory)
        graph.addVertex(T.label, "person", "name", "Baby", "city", "Hongkong");

        // -1 <= age && age < 21
        vertices = graph.traversal().V().hasLabel("person")
                        .has("age", P.between(-1, 21)).toList();
        Assert.assertEquals(3, vertices.size());

        // override vertex without age (in backend) and make left index
        graph.addVertex(T.label, "person", "name", "Baby", "city", "Hongkong")
             .remove(); // avoid merge property mode
        this.mayCommitTx();

        // query again after commit
        vertices = graph.traversal().V().hasLabel("person")
                        .has("age", P.between(-1, 21)).toList();
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testQueryByIntPropUsingBetweenWithNonResult() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        // 4 <= age && age < 19
        List<Vertex> vertices = graph.traversal().V().hasLabel("person")
                                     .has("age", P.between(4, 19)).toList();

        Assert.assertEquals(0, vertices.size());

        // 3 <= age && age < 3
        vertices = graph.traversal().V().hasLabel("person")
                        .has("age", P.between(3, 3)).toList();

        Assert.assertEquals(0, vertices.size());

        // 21 <= age && age < 20
        vertices = graph.traversal().V().hasLabel("person")
                        .has("age", P.between(21, 20)).toList();

        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryByIntProperty() {
        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("int").asInt().create();
        schema.vertexLabel("number").primaryKeys("id")
              .properties("id", "int").create();
        schema.indexLabel("numberByInt").range()
              .onV("number").by("int").create();

        graph().addVertex(T.label, "number", "id", 1, "int", 0);
        graph().addVertex(T.label, "number", "id", 2, "int", 12345678);
        graph().addVertex(T.label, "number", "id", 3, "int", 1000000001L);
        graph().addVertex(T.label, "number", "id", 4, "int", -1);
        graph().addVertex(T.label, "number", "id", 5,
                          "int", Integer.MAX_VALUE);
        graph().addVertex(T.label, "number", "id", 6,
                          "int", Integer.MIN_VALUE);
        graph().addVertex(T.label, "number", "id", 7,
                          "int", Integer.MAX_VALUE - 1);
        graph().addVertex(T.label, "number", "id", 8,
                          "int", Integer.MIN_VALUE + 1);

        this.mayCommitTx();

        List<Vertex> vertices = graph.traversal().V().hasLabel("number")
                                     .has("int", 0).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 1, "int", 0);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("int", 12345678).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 2, "int", 12345678);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("int", 1000000001L).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 3, "int", 1000000001);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("int", -1).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 4, "int", -1);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("int", Integer.MAX_VALUE).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 5,
                       "int", Integer.MAX_VALUE);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("int", Integer.MIN_VALUE).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 6,
                       "int", Integer.MIN_VALUE);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("int", Integer.MAX_VALUE - 1).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 7,
                       "int", Integer.MAX_VALUE - 1);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("int", Integer.MIN_VALUE + 1).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 8,
                       "int", Integer.MIN_VALUE + 1);

        // query by null property
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V().hasLabel("person")
                 .has("age", (Object) null).toList();
        }, e -> {
            Assert.assertContains("Invalid data type of query value",
                                  e.getMessage());
        });
    }

    @Test
    public void testQueryByLongProperty() {
        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("long").asLong().create();
        schema.vertexLabel("number").primaryKeys("id")
              .properties("id", "long").create();
        schema.indexLabel("numberByLong").range()
              .onV("number").by("long").create();

        final long largeLong = 9123456789087654321L;

        graph().addVertex(T.label, "number", "id", 1, "long", 0L);
        graph().addVertex(T.label, "number", "id", 2, "long", 7L);
        graph().addVertex(T.label, "number", "id", 3, "long", 1000000001);
        graph().addVertex(T.label, "number", "id", 4, "long", -1L);
        graph().addVertex(T.label, "number", "id", 5, "long", largeLong);
        graph().addVertex(T.label, "number", "id", 6, "long", -largeLong);
        graph().addVertex(T.label, "number", "id", 7, "long", Long.MAX_VALUE);
        graph().addVertex(T.label, "number", "id", 8, "long", Long.MIN_VALUE);
        graph().addVertex(T.label, "number", "id", 9,
                          "long", Long.MAX_VALUE - 1);
        graph().addVertex(T.label, "number", "id", 10,
                          "long", Long.MIN_VALUE + 1);

        this.mayCommitTx();

        List<Vertex> vertices = graph.traversal().V().hasLabel("number")
                                     .has("long", 0).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 1, "long", 0L);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("long", 7).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 2, "long", 7L);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("long", 1000000001).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 3,
                       "long", 1000000001L);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("long", -1).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 4, "long", -1L);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("long", largeLong).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 5,
                       "long", largeLong);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("long", -largeLong).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 6,
                       "long", -largeLong);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("long", Long.MAX_VALUE).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 7,
                       "long", Long.MAX_VALUE);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("long", Long.MIN_VALUE).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 8,
                       "long", Long.MIN_VALUE);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("long", Long.MAX_VALUE - 1).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 9,
                       "long", Long.MAX_VALUE - 1);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("long", Long.MIN_VALUE + 1).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 10,
                       "long", Long.MIN_VALUE + 1);
    }

    @Test
    public void testQueryByFloatProperty() {
        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("float").asFloat().create();
        schema.vertexLabel("number").primaryKeys("id")
              .properties("id", "float").create();
        schema.indexLabel("numberByFloat").range()
              .onV("number").by("float").create();

        final float secondBiggest = 0x1.fffffdP+127f;
        final float secondSmallest = 0x0.000003P-126f;

        graph().addVertex(T.label, "number", "id", 1, "float", 7);
        graph().addVertex(T.label, "number", "id", 2, "float", 3.14f);
        graph().addVertex(T.label, "number", "id", 3, "float", 3.141592f);
        graph().addVertex(T.label, "number", "id", 4, "float", 1234.567d);
        graph().addVertex(T.label, "number", "id", 5,
                          "float", Float.MAX_VALUE);
        graph().addVertex(T.label, "number", "id", 6,
                          "float", -Float.MAX_VALUE);
        graph().addVertex(T.label, "number", "id", 7,
                          "float", Float.MIN_VALUE);
        graph().addVertex(T.label, "number", "id", 8,
                          "float", -Float.MIN_VALUE);
        graph().addVertex(T.label, "number", "id", 9,
                          "float", secondBiggest);
        graph().addVertex(T.label, "number", "id", 10,
                          "float", -secondBiggest);
        graph().addVertex(T.label, "number", "id", 11,
                          "float", secondSmallest);
        graph().addVertex(T.label, "number", "id", 12,
                          "float", -secondSmallest);

        this.mayCommitTx();

        List<Vertex> vertices = graph.traversal().V().hasLabel("number")
                                     .has("float", 7).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 1, "float", 7f);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("float", 3.14f).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 2, "float", 3.14f);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("float", 3.141592f).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 3,
                       "float", 3.141592f);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("float", 1234.567d).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 4,
                       "float", 1234.567f);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("float", Float.MAX_VALUE).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 5,
                       "float", Float.MAX_VALUE);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("float", -Float.MAX_VALUE).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 6,
                       "float", -Float.MAX_VALUE);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("float", Float.MIN_VALUE).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 7,
                       "float", Float.MIN_VALUE);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("float", -Float.MIN_VALUE).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 8,
                       "float", -Float.MIN_VALUE);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("float", secondBiggest).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 9,
                       "float", secondBiggest);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("float", -secondBiggest).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 10,
                       "float", -secondBiggest);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("float", secondSmallest).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 11,
                       "float", secondSmallest);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("float", -secondSmallest).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 12,
                       "float", -secondSmallest);
    }

    @Test
    public void testQueryByDoubleProperty() {
        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("double").asDouble().create();
        schema.vertexLabel("number").primaryKeys("id")
              .properties("id", "double").create();
        schema.indexLabel("numberByDouble").range()
              .onV("number").by("double").create();

        final double max7 = Double.parseDouble(String.valueOf(Float.MAX_VALUE));

        /*
         * The double precision type typically has a range of around 1E-307 to
         * 1E+308 with a precision of at least 15 digits. (postgresql)
         * https://www.postgresql.org/docs/9.5/datatype-numeric.html#DATATYPE-NUMERIC-TABLE
         */
        final double max15 = new BigDecimal(Double.MAX_VALUE)
                .movePointLeft(308)
                .setScale(15, BigDecimal.ROUND_DOWN)
                .movePointRight(308)
                .doubleValue(); // 1.797693134862315E308
        final double min15 = new BigDecimal(1.234567890987654321d)
                .setScale(15, BigDecimal.ROUND_DOWN)
                .movePointLeft(307)
                .doubleValue(); // 1.234567890987654E-307

        graph().addVertex(T.label, "number", "id", 1, "double", 7);
        graph().addVertex(T.label, "number", "id", 2, "double", 3.14f);
        graph().addVertex(T.label, "number", "id", 3, "double", Math.PI);
        graph().addVertex(T.label, "number", "id", 4,
                          "double", 12345678901234.567d); // 12345678901234.566
        graph().addVertex(T.label, "number", "id", 5, "double", max7);
        graph().addVertex(T.label, "number", "id", 6, "double", -max7);
        graph().addVertex(T.label, "number", "id", 7, "double", max15);
        graph().addVertex(T.label, "number", "id", 8, "double", -max15);
        graph().addVertex(T.label, "number", "id", 9, "double", min15);
        graph().addVertex(T.label, "number", "id", 10, "double", -min15);

        this.mayCommitTx();

        List<Vertex> vertices = graph.traversal().V().hasLabel("number")
                                     .has("double", 7).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 1, "double", 7d);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", 3.14f).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 2, "double", 3.14d);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", Math.PI).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 3, "double", Math.PI);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", 12345678901234.567d).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 4,
                       "double", 12345678901234.567d);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", Float.MAX_VALUE).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 5, "double", max7);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", -Float.MAX_VALUE).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 6, "double", -max7);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", max15).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 7, "double", max15);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", -max15).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 8, -max15);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", min15).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 9, "double", min15);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", -min15).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 10, -min15);
    }

    @Test
    public void testQueryByDoublePropertyWithMaxMinValue() {
        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("double").asDouble().create();
        schema.vertexLabel("number").primaryKeys("id")
              .properties("id", "double").create();
        schema.indexLabel("numberByDouble").range()
              .onV("number").by("double").create();

        final double secondBiggest = 0x1.ffffffffffffeP+1023;
        final double secondSmallest = 0x0.0000000000002P-1022;

        graph().addVertex(T.label, "number", "id", 0,
                          "double", 0.123456789012345678901d);
        graph().addVertex(T.label, "number", "id", 1,
                          "double", Double.MAX_VALUE);
        graph().addVertex(T.label, "number", "id", 2,
                          "double", -Double.MAX_VALUE);
        graph().addVertex(T.label, "number", "id", 3,
                          "double", Double.MIN_VALUE);
        graph().addVertex(T.label, "number", "id", 4,
                          "double", -Double.MIN_VALUE);
        graph().addVertex(T.label, "number", "id", 5,
                          "double", secondBiggest);
        graph().addVertex(T.label, "number", "id", 6,
                          "double", -secondBiggest);
        graph().addVertex(T.label, "number", "id", 7,
                          "double", secondSmallest);
        graph().addVertex(T.label, "number", "id", 8,
                          "double", -secondSmallest);

        this.mayCommitTx();

        List<Vertex> vertices = graph.traversal().V().hasLabel("number")
                                     .has("double", 0.123456789012345678901d)
                                     .toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 0,
                       "double", 0.123456789012345678901d);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", Double.MAX_VALUE).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 1,
                       "double", Double.MAX_VALUE);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", -Double.MAX_VALUE).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 2,
                       "double", -Double.MAX_VALUE);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", Double.MIN_VALUE).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 3,
                       "double", Double.MIN_VALUE);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", -Double.MIN_VALUE).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 4,
                       "double", -Double.MIN_VALUE);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", secondBiggest).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 5,
                       "double", secondBiggest);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", -secondBiggest).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 6,
                       "double", -secondBiggest);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", secondSmallest).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 7,
                       "double", secondSmallest);

        vertices = graph.traversal().V().hasLabel("number")
                        .has("double", -secondSmallest).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "number", "id", 8,
                       "double", -secondSmallest);
    }

    @Test
    public void testQueryByDateProperty() {
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = null;

        Date[] dates = new Date[]{
                Utils.date("2012-01-01 00:00:00.000"),
                Utils.date("2013-01-01 00:00:00.000"),
                Utils.date("2014-01-01 00:00:00.000"),
                Utils.date("2015-01-01 00:00:00.000"),
                Utils.date("2016-01-01 00:00:00.000")
        };

        vertices = graph.traversal().V().hasLabel("person")
                        .has("birth", dates[0])
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(dates[0], vertices.get(0).value("birth"));

        vertices = graph.traversal().V().hasLabel("person")
                        .has("birth", P.gt(dates[0]))
                        .toList();
        Assert.assertEquals(4, vertices.size());

        vertices = graph.traversal().V().hasLabel("person")
                        .has("birth", P.between(dates[3], dates[4]))
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(dates[3], vertices.get(0).value("birth"));

        vertices = graph.traversal().V().hasLabel("person")
                        .has("birth", P.between(dates[1], dates[4]))
                        .toList();
        Assert.assertEquals(3, vertices.size());

        // limit
        vertices = graph.traversal().V().hasLabel("person")
                        .has("birth", P.between(dates[1], dates[4]))
                        .limit(2).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertEquals(dates[1], vertices.get(0).value("birth"));
        Assert.assertEquals(dates[2], vertices.get(1).value("birth"));

        // limit after delete
        graph.traversal().V().hasLabel("person")
             .has("birth", P.between(dates[1], dates[4]))
             .limit(2).drop().iterate();
        this.commitTx();

        vertices = graph.traversal().V().hasLabel("person")
                        .has("birth", P.between(dates[1], dates[4]))
                        .limit(2).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(dates[3], vertices.get(0).value("birth"));
    }

    @Test
    public void testQueryByDatePropertyInString() {
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = null;

        String[] dates = new String[]{
                "2012-01-01 00:00:00.000",
                "2013-01-01 00:00:00.000",
                "2014-01-01 00:00:00.000",
                "2015-01-01 00:00:00.000",
                "2016-01-01 00:00:00.000"
        };

        vertices = graph.traversal().V().hasLabel("person")
                        .has("birth", dates[0])
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(Utils.date(dates[0]),
                            vertices.get(0).value("birth"));

        vertices = graph.traversal().V().hasLabel("person")
                        .has("birth", P.gt(dates[0]))
                        .toList();
        Assert.assertEquals(4, vertices.size());

        vertices = graph.traversal().V().hasLabel("person")
                        .has("birth", P.between(dates[3], dates[4]))
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(Utils.date(dates[3]),
                            vertices.get(0).value("birth"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testQueryByDatePropertyWithUnion() {
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        GraphTraversalSource g = graph.traversal();
        List<Vertex> vertices = null;

        String[] dates = new String[]{
                "2012-01-01 00:00:00.000",
                "2013-01-01 00:00:00.000",
                "2014-01-01 00:00:00.000",
                "2015-01-01 00:00:00.000",
                "2016-01-01 00:00:00.000"
        };

        vertices = g.V()
                    .hasLabel("person")
                    .union(__.<Vertex>has("birth", dates[0]))
                    .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(Utils.date(dates[0]),
                            vertices.get(0).value("birth"));

        vertices = g.V()
                    .hasLabel("person")
                    .union(__.<Vertex>has("birth", P.gt(dates[0])))
                    .toList();
        Assert.assertEquals(4, vertices.size());

        vertices = g.V()
                    .hasLabel("person")
                    .union(__.<Vertex>has("birth", P.lt(dates[1])),
                           __.<Vertex>has("birth", P.gt(dates[3])))
                    .toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testQueryByDatePropertyInMultiFormatString() {
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = null;

        String[] dates = new String[]{
                "2012-01-01",
                "2013-01-01 00:00:00.000",
                "2014-01-01 00:00:00.000",
                "2015-01-01 00:00:00",
                "2016-01-01 00:00:00.000"
        };

        vertices = graph.traversal().V().hasLabel("person")
                        .has("birth", dates[0])
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(Utils.date(dates[0]),
                            vertices.get(0).value("birth"));

        vertices = graph.traversal().V().hasLabel("person")
                        .has("birth", P.gt(dates[0]))
                        .toList();
        Assert.assertEquals(4, vertices.size());

        vertices = graph.traversal().V().hasLabel("person")
                        .has("birth", P.between(dates[3], dates[4]))
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(Utils.date(dates[3]),
                            vertices.get(0).value("birth"));
    }

    @Test
    public void testQueryByUUIDProperty() {
        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("uuid").asUUID().create();
        schema.vertexLabel("user").primaryKeys("id")
              .properties("id", "uuid").create();
        schema.indexLabel("userByUuid").secondary()
              .onV("user").by("uuid").create();

        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        graph().addVertex(T.label, "user", "id", 1, "uuid", uuid1);
        graph().addVertex(T.label, "user", "id", 2, "uuid", uuid2);

        this.mayCommitTx();

        List<Vertex> vertices = graph.traversal().V().hasLabel("user")
                                     .has("uuid", uuid1).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "user", "id", 1, "uuid", uuid1);

        vertices = graph.traversal().V().hasLabel("user")
                        .has("uuid", uuid2).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "user", "id", 2, "uuid", uuid2);
    }

    @Test
    public void testQueryByBlobProperty() {
        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("blob").asBlob().create();
        schema.vertexLabel("user").primaryKeys("id")
              .properties("id", "blob").create();
        schema.indexLabel("userByBlob").secondary()
              .onV("user").by("blob").create();

        Blob blob1 = Blob.wrap(new byte[]{97, 49, 50, 51, 52});
        Blob blob2 = Blob.wrap(new byte[]{97, 98, 50, 51, 52});
        graph().addVertex(T.label, "user", "id", 1, "blob", blob1);
        graph().addVertex(T.label, "user", "id", 2, "blob", blob2);

        this.mayCommitTx();

        List<Vertex> vertices = graph.traversal().V().hasLabel("user")
                                     .has("blob", blob1).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "user", "id", 1, "blob", blob1);

        vertices = graph.traversal().V().hasLabel("user")
                        .has("blob", blob2).toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices, T.label, "user", "id", 2, "blob", blob2);
    }

    @Test
    public void testQueryByTextContainsProperty() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("authorByLived").onV("author")
             .search().by("lived").create();

        graph.addVertex(T.label, "author", "id", 1,
                        "name", "James Gosling", "age", 62,
                        "lived", "San Francisco Bay Area");

        // Uncommitted
        List<Vertex> vertices = graph.traversal().V()
                                     .hasLabel("author")
                                     .has("lived", Text.contains("Bay Area"))
                                     .toList();
        Assert.assertEquals(1, vertices.size());

        // Committed
        this.commitTx();
        vertices = graph.traversal().V()
                        .hasLabel("author")
                        .has("lived", Text.contains("Bay Area"))
                        .toList();

        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "San Francisco Bay Area");

        Assert.assertThrows(NoIndexException.class, () -> {
            graph.traversal().V().hasLabel("author")
                 .has("lived", "Bay Area")
                 .toList();
        }, e -> {
            Assert.assertContains("may not match secondary condition",
                                  e.getMessage());
        });
    }

    @Test
    public void testQueryByTextContainsAndExactMatchProperty() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("authorByLivedSearch").onV("author")
             .search().by("lived").create();
        graph.schema().indexLabel("authorByLivedSecondary").onV("author")
             .secondary().by("lived").create();

        graph.addVertex(T.label, "author", "id", 1,
                        "name", "James Gosling", "age", 62,
                        "lived", "San Francisco Bay Area");
        this.mayCommitTx();

        // By authorByLivedSearch index
        List<Vertex> vertices = graph.traversal().V()
                                     .hasLabel("author")
                                     .has("lived", Text.contains("Bay Area"))
                                     .toList();
        Assert.assertEquals(1, vertices.size());

        // By authorByLivedSecondary index
        vertices = graph.traversal().V()
                        .hasLabel("author")
                        .has("lived", "San Francisco Bay Area")
                        .toList();
        Assert.assertEquals(1, vertices.size());
        assertContains(vertices,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "San Francisco Bay Area");

        vertices = graph.traversal().V()
                        .hasLabel("author")
                        .has("lived", "Bay Area")
                        .toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryByTextContainsPropertyOrderByMatchedCount() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("authorByLived").onV("author")
             .search().by("lived").create();

        graph.addVertex(T.label, "author", "id", 1, "name", "Tank", "age", 16,
                        "lived", "Beijing");
        graph.addVertex(T.label, "author", "id", 2, "name", "Dim", "age", 40,
                        "lived", "Shenzhen area");
        graph.addVertex(T.label, "author", "id", 3, "name", "Tom", "age", 19,
                        "lived", "New York Bay");
        graph.addVertex(T.label, "author", "id", 4, "name", "Jason", "age", 20,
                        "lived", "Tokyo Bay");
        graph.addVertex(T.label, "author", "id", 5, "name", "James", "age", 62,
                        "lived", "San Francisco Bay Area");
        this.commitTx();

        List<Vertex> vertices = graph.traversal().V()
                                     .hasLabel("author")
                                     .has("lived", Text.contains("Bay Area"))
                                     .toList();

        Assert.assertEquals(4, vertices.size());
        Assert.assertEquals("James", vertices.get(0).value("name"));
        Assert.assertEquals("Tom", vertices.get(1).value("name"));
        Assert.assertEquals("Jason", vertices.get(2).value("name"));
        Assert.assertEquals("Dim", vertices.get(3).value("name"));
        assertContains(vertices,
                       T.label, "author", "id", 2, "name", "Dim",
                       "age", 40, "lived", "Shenzhen area");
        assertContains(vertices,
                       T.label, "author", "id", 3, "name", "Tom",
                       "age", 19, "lived", "New York Bay");
        assertContains(vertices,
                       T.label, "author", "id", 4, "name", "Jason",
                       "age", 20, "lived", "Tokyo Bay");
        assertContains(vertices,
                       T.label, "author", "id", 5, "name", "James",
                       "age", 62, "lived", "San Francisco Bay Area");

        vertices = graph.traversal().V().hasLabel("author")
                        .has("lived", Text.contains("Bay Area"))
                        .limit(1)
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("James", vertices.get(0).value("name"));

        vertices = graph.traversal().V().hasLabel("author")
                        .has("lived", Text.contains("Bay Area"))
                        .skip(1)
                        .toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertEquals("Tom", vertices.get(0).value("name"));
        Assert.assertEquals("Jason", vertices.get(1).value("name"));
        Assert.assertEquals("Dim", vertices.get(2).value("name"));

        vertices = graph.traversal().V().hasLabel("author")
                        .has("lived", Text.contains("Bay Area"))
                        .skip(2).limit(1)
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("Jason", vertices.get(0).value("name"));
    }

    @Test
    public void testQueryByTextContainsPropertyOrderByMatchedCountWithPaging() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();

        graph.schema().indexLabel("authorByLived").onV("author")
             .search().by("lived").create();

        graph.addVertex(T.label, "author", "id", 1, "name", "Tank", "age", 16,
                        "lived", "Beijing");
        graph.addVertex(T.label, "author", "id", 2, "name", "Dim", "age", 40,
                        "lived", "Shenzhen area");
        graph.addVertex(T.label, "author", "id", 3, "name", "Tom", "age", 19,
                        "lived", "New York Bay");
        graph.addVertex(T.label, "author", "id", 4, "name", "Jason", "age", 20,
                        "lived", "Tokyo Bay");
        graph.addVertex(T.label, "author", "id", 5, "name", "James", "age", 62,
                        "lived", "San Francisco Bay Area");
        this.commitTx();

        List<Vertex> vertices = graph.traversal().V()
                                     .hasLabel("author")
                                     .has("lived", Text.contains("Bay Area"))
                                     .has("~page", "").limit(2)
                                     .toList();
        Assert.assertEquals(2, vertices.size());
        assertContains(vertices,
                       T.label, "author", "id", 3, "name", "Tom",
                       "age", 19, "lived", "New York Bay");
        assertContains(vertices,
                       T.label, "author", "id", 4, "name", "Jason",
                       "age", 20, "lived", "Tokyo Bay");
    }

    @Test
    public void testQueryByTextContainsPropertyWithLeftIndex() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("authorByLived").onV("author")
             .search().by("lived").create();

        graph.addVertex(T.label, "author", "id", 1,
                        "name", "James Gosling", "age", 62,
                        "lived", "San Francisco Bay Area");
        this.commitTx();

        // Override the origin vertex with different property value
        // and then lead to index left
        graph.addVertex(T.label, "author", "id", 1,
                        "name", "James Gosling", "age", 62,
                        "lived", "San Francisco, California, U.S.");
        this.commitTx();

        // Set breakpoint to observe the store before and after this statement
        List<Vertex> vertices = graph.traversal().V().hasLabel("author")
                                     .has("lived", Text.contains("Bay Area"))
                                     .toList();

        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryByNeq() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("authorByLived1").onV("author")
             .secondary().by("lived").create();
        graph.schema().indexLabel("authorByLived2").onV("author")
             .search().by("lived").create();

        graph.addVertex(T.label, "author", "id", 1,
                        "name", "James Gosling", "age", 62,
                        "lived", "San Francisco Bay Area");
        this.mayCommitTx();

        Assert.assertThrows(NoIndexException.class, () -> {
            graph.traversal().V().hasLabel("author")
                 .has("lived", P.neq("Beijing"))
                 .toList();
        }, e -> {
            Assert.assertEquals("Don't accept query based on properties " +
                                "[lived] that are not indexed in label " +
                                "'author', may not match secondary" +
                                "/not-equal condition", e.getMessage());
        });

        Assert.assertThrows(NoIndexException.class, () -> {
            graph.traversal().V().has("lived", P.neq("Beijing"))
                 .toList();
        }, e -> {
            Assert.assertContains("Don't accept query based on properties " +
                                  "[lived] that are not indexed in any label",
                                  e.getMessage());
            Assert.assertContains("may not match not-equal condition",
                                  e.getMessage());
        });
    }

    @Test
    public void testQueryWithMultiLayerConditions() {
        HugeGraph graph = graph();
        initPersonIndex(false);
        init5Persons();

        List<Object> vertices = graph.traversal().V().hasLabel("person").has(
                "age",
                P.not(P.lte(10).and(P.not(P.between(11, 20))))
                 .and(P.lt(29).or(P.eq(35)).or(P.gt(45)))
        ).values("name").toList();

        // There is duplicate results with OR condition
        Assert.assertEquals(5, vertices.size());

        Set<String> names = ImmutableSet.of("Hebe", "James",
                                            "Tom Cat", "Lisa");
        for (Object name : vertices) {
            Assert.assertTrue(names.contains(name));
        }
    }

    @Test
    public void testQueryByIntPropOfOverrideVertex() {
        HugeGraph graph = graph();
        initPersonIndex(false);

        graph.addVertex(T.label, "person", "name", "Zhangyi",
                        "city", "Beijing", "age", 28);
        graph.addVertex(T.label, "person", "name", "Zhangyi",
                        "city", "Hongkong", "age", 29);
        this.mayCommitTx();

        List<Vertex> vertices = graph.traversal().V().has("age", 28).toList();
        Assert.assertEquals(0, vertices.size());
        vertices = graph.traversal().V().has("age", 29).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryByStringPropOfOverrideVertex() {
        HugeGraph graph = graph();
        initPersonIndex(true);

        graph.addVertex(T.label, "person", "name", "Zhangyi",
                        "city", "Beijing", "age", 28);
        graph.addVertex(T.label, "person", "name", "Zhangyi",
                        "city", "Hongkong", "age", 29);
        this.mayCommitTx();

        List<Vertex> vertices = graph.traversal().V().has("city", "Beijing")
                                     .toList();
        Assert.assertEquals(0, vertices.size());
        vertices = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryWithTxNotCommittedByNoCondition() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "person", "name", "marko",
                        "age", 18, "city", "Beijing");
        graph.addVertex(T.label, "person", "name", "james",
                        "age", 19, "city", "Hongkong");

        List<Vertex> vertices;
        vertices = graph.traversal().V().toList();
        Assert.assertEquals(2, vertices.size());

        graph.tx().rollback();
        vertices = graph.traversal().V().toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryWithTxNotCommittedById() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "person", "name", "marko",
                        "age", 18, "city", "Beijing");

        List<Vertex> vertices;
        String personId = graph.vertexLabel("person").id().asString();
        String vid = SplicingIdGenerator.splicing(personId, "marko").asString();
        vertices = graph.traversal().V(vid).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(vid, vertices.get(0).id().toString());

        graph.tx().rollback();
        vertices = graph.traversal().V(personId + ":marko").toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryWithTxNotCommittedByIntProp() {
        HugeGraph graph = graph();
        initPersonIndex(false);

        graph.addVertex(T.label, "person", "name", "marko",
                        "age", 18, "city", "Beijing");

        List<Vertex> vertices;
        vertices = graph.traversal().V().has("age", 18).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(18, (int) vertices.get(0).<Integer>value("age"));

        graph.tx().rollback();
        vertices = graph.traversal().V("person:marko").toList();
        Assert.assertEquals(0, vertices.size());

        // TODO: add test for: append, eliminate, remove
    }

    @Test
    public void testQueryWithTxNotCommittedByIdInOtherThread()
            throws InterruptedException {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "person", "name", "marko",
                        "age", 18, "city", "Beijing");

        AtomicInteger size = new AtomicInteger(-1);
        Thread t = new Thread(() -> {
            try {
                List<Vertex> vertices = graph.traversal()
                                             .V("person:marko")
                                             .toList();
                size.set(vertices.size());
            } finally {
                params().closeTx();
            }
        });
        t.start();
        t.join();
        Assert.assertEquals(0, size.get());
    }

    @Test
    public void testQueryWithTxNotCommittedUpdatedProp() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "person", "name", "marko",
                        "age", 18, "city", "Beijing");
        Vertex v = graph.addVertex(T.label, "person", "name", "james",
                                   "age", 19, "city", "Hongkong");
        this.commitTx();

        v.property("age", 20);
        // Don't commit

        List<Vertex> vertices = graph.traversal().V()
                                     .where(__.values("age").is(20))
                                     .toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v.id(), vertices.get(0).id());
    }

    @Test
    public void testQueryByJointIndexes() {
        HugeGraph graph = graph();

        initPersonIndex(true);
        this.init5Persons();

        List<Vertex> vertices;

        // results count < 2
        vertices = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V()
                        .has("city", "Hongkong").has("age", 2).toList();
        Assert.assertEquals(0, vertices.size());

        vertices = graph.traversal().V()
                        .has("city", "Taipei").has("age", 3).toList();
        Assert.assertEquals(0, vertices.size());

        vertices = graph.traversal().V()
                        .has("city", "Hongkong").has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V()
                        .has("city", "Hongkong").has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V()
                        .has("city", "Hongkong").has("age", 3)
                        .skip(1).toList();
        Assert.assertEquals(0, vertices.size());

        // results count >= 2
        vertices = graph.traversal().V().has("age", 20).toList();
        Assert.assertEquals(2, vertices.size());
        vertices = graph.traversal().V().has("age", 19).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph.traversal().V().has("city", "Beijing").toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph.traversal().V()
                        .has("city", "Beijing").has("age", P.lt(21)).toList();
        Assert.assertEquals(3, vertices.size());

        Object tx = Whitebox.invoke(graph.getClass(),
                                    "graphTransaction", graph);
        Object old = Whitebox.getInternalState(tx,
                                               "indexTx.indexIntersectThresh");
        try {
            for (int i = 1; i < 6; i++) {
                Whitebox.setInternalState(tx,
                                          "indexTx.indexIntersectThresh", i);

                vertices = graph.traversal().V()
                                .has("city", "Beijing").has("age", 20).toList();
                Assert.assertEquals(2, vertices.size());

                vertices = graph.traversal().V()
                                .has("city", "Beijing").has("age", 20)
                                .skip(1).toList();
                Assert.assertEquals(1, vertices.size());
            }
        } finally {
            Whitebox.setInternalState(tx, "indexTx.indexIntersectThresh", old);
        }
    }

    @Test
    public void testQueryByJointIndexesAndCompositeIndexForOneLabel() {
        initPersonIndex(true);

        graph().addVertex(T.label, "person", "name", "Tom",
                          "city", "Hongkong", "age", 3);
        this.commitTx();

        List<Vertex> vertices;
        vertices = graph().traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph().traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph().traversal().V().has("city", "Hongkong")
                          .has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());

        /*
         * NOTE: may block if not commit records:
         * Lock [hugegraph_il_delete:3] is locked by other operation
         */
        graph().schema().indexLabel("personByCityAndAge").onV("person")
               .by("city", "age").ifNotExist().create();

        vertices = graph().traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph().traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph().traversal().V().has("city", "Hongkong")
                          .has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryByJointIndexesAndCompositeIndexForMultiLabel() {
        SchemaManager schema = graph().schema();

        schema.vertexLabel("dog").properties("name", "age", "city")
              .primaryKeys("name").nullableKeys("age").create();
        schema.indexLabel("dogByCityAndAge").onV("dog")
              .by("city", "age").create();

        schema.vertexLabel("cat").properties("name", "age", "city")
              .primaryKeys("name").nullableKeys("age").create();
        schema.indexLabel("catByCity").onV("cat").secondary()
              .by("city").create();
        schema.indexLabel("catByAge").onV("cat").range()
              .by("age").create();

        graph().addVertex(T.label, "dog", "name", "Tom",
                          "city", "Hongkong", "age", 3);
        graph().addVertex(T.label, "cat", "name", "Baby",
                          "city", "Hongkong", "age", 3);
        this.mayCommitTx();

        List<Vertex> vertices;
        vertices = graph().traversal().V().has("age", 3)
                          .has("city", "Hongkong").toList();
        Assert.assertEquals(2, vertices.size());
        Set<String> labels = new HashSet<>();
        labels.add(vertices.get(0).label());
        labels.add(vertices.get(1).label());
        Set<String> expectedLabels = ImmutableSet.of("dog", "cat");
        Assert.assertEquals(expectedLabels, labels);
    }

    @Test
    public void testQueryByJointIndexesOnlyWithCompositeIndex() {
        SchemaManager schema = graph().schema();
        schema.vertexLabel("dog").properties("name", "age", "city")
              .nullableKeys("age").create();
        schema.indexLabel("dogByNameAndCity").onV("dog").secondary()
              .by("name", "city").create();
        schema.indexLabel("dogByCityAndAge").onV("dog").secondary()
              .by("city", "age").create();

        graph().addVertex(T.label, "dog", "name", "Tom",
                          "city", "Hongkong", "age", 3);
        this.mayCommitTx();

        List<Vertex> vertices = graph().traversal().V().has("age", 3)
                                       .has("city", "Hongkong")
                                       .has("name", "Tom").toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryByJointIndexesWithRangeIndex() {
        initPersonIndex(true);

        graph().addVertex(T.label, "person", "name", "Tom",
                          "city", "Hongkong", "age", 3);

        List<Vertex> vertices;
        vertices = graph().traversal().V().has("age", P.gt(2))
                          .has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryByJointIndexesWithCompositeIndexIncludeOtherField() {
        SchemaManager schema = graph().schema();
        schema.vertexLabel("dog").properties("name", "age", "city")
              .nullableKeys("age").create();
        schema.indexLabel("dogByAge").onV("dog").range().by("age").create();
        schema.indexLabel("dogByCityAndName").onV("dog").secondary()
              .by("city", "name").create();

        graph().addVertex(T.label, "dog", "name", "Tom",
                          "city", "Hongkong", "age", 3);
        this.mayCommitTx();

        List<Vertex> vertices = graph().traversal().V().has("age", P.gt(2))
                                       .has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryByJointIndexesWithOnlyRangeIndexes() {
        SchemaManager schema = graph().schema();
        schema.vertexLabel("dog").properties("name", "age", "weight").create();
        schema.indexLabel("dogByAge").onV("dog").range().by("age").create();
        schema.indexLabel("dogByWeight").onV("dog").range().by("weight")
              .create();

        graph().addVertex(T.label, "dog", "name", "Tom",
                          "age", 8, "weight", 3);
        this.mayCommitTx();

        List<Vertex> vertices = graph().traversal().V().has("age", P.gt(2))
                                       .has("weight", P.lt(10)).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryByJointIndexesWithSearchAndRangeIndexes() {
        SchemaManager schema = graph().schema();
        schema.vertexLabel("dog").properties("name", "age", "description")
              .create();

        schema.indexLabel("dogByAge").onV("dog")
              .range().by("age").create();
        schema.indexLabel("dogByDescription").onV("dog")
              .search().by("description").create();

        graph().addVertex(T.label, "dog", "name", "Bella", "age", 1,
                          "description", "black hair and eyes");
        graph().addVertex(T.label, "dog", "name", "Daisy", "age", 2,
                          "description", "yellow hair yellow tail");
        graph().addVertex(T.label, "dog", "name", "Coco", "age", 3,
                          "description", "yellow hair golden tail");

        this.commitTx();

        List<Vertex> vertices;
        vertices = graph().traversal().V()
                          .has("age", P.gte(2))
                          .has("description", Text.contains("yellow hair"))
                          .toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("age", P.gt(2))
                          .has("description", Text.contains("yellow hair"))
                          .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V()
                          .has("age", P.gt(0))
                          .has("description", Text.contains("black golden"))
                          .toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testQueryByJointIndexesWithSearchAndRangeAndSecondaryIndexes() {
        SchemaManager schema = graph().schema();
        schema.vertexLabel("dog")
              .properties("name", "age", "description", "city")
              .create();

        schema.indexLabel("dogByAge").onV("dog")
              .range().by("age").create();
        schema.indexLabel("dogByDescription").onV("dog")
              .search().by("description")
              .create();
        schema.indexLabel("dogByCity").onV("dog")
              .secondary().by("city").create();

        graph().addVertex(T.label, "dog", "name", "Bella", "age", 1,
                          "city", "Beijing",
                          "description", "black hair and eyes");
        graph().addVertex(T.label, "dog", "name", "Daisy", "age", 2,
                          "city", "Shanghai",
                          "description", "yellow hair yellow tail");
        graph().addVertex(T.label, "dog", "name", "Coco", "age", 3,
                          "city", "Shanghai",
                          "description", "yellow hair golden tail");

        this.commitTx();

        List<Vertex> vertices;
        vertices = graph().traversal().V()
                          .has("age", P.gte(2))
                          .has("description", Text.contains("yellow hair"))
                          .has("city", "Shanghai")
                          .toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("age", P.gt(2))
                          .has("description", Text.contains("yellow hair"))
                          .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V()
                          .has("age", P.gt(2))
                          .has("description", Text.contains("yellow hair"))
                          .has("city", "Beijing")
                          .toList();
        Assert.assertEquals(0, vertices.size());

        vertices = graph().traversal().V()
                          .has("age", P.gt(0))
                          .has("description", Text.contains("black golden"))
                          .toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("age", P.gt(0))
                          .has("description", Text.contains("black golden"))
                          .has("city", "Beijing")
                          .toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryByJointIndexesWithTwoSearchAndOneRangeIndexes() {
        SchemaManager schema = graph().schema();
        schema.vertexLabel("dog")
              .properties("name", "age", "description", "city")
              .create();

        schema.indexLabel("dogByAge").onV("dog")
              .range().by("age").create();
        schema.indexLabel("dogByDescription").onV("dog")
              .search().by("description")
              .create();
        schema.indexLabel("dogByCity").onV("dog")
              .search().by("city").create();

        graph().addVertex(T.label, "dog", "name", "Bella", "age", 1,
                          "city", "Beijing Haidian",
                          "description", "black hair and eyes");
        graph().addVertex(T.label, "dog", "name", "Daisy", "age", 2,
                          "city", "Shanghai Zhangjiang",
                          "description", "yellow hair yellow tail");
        graph().addVertex(T.label, "dog", "name", "Coco", "age", 3,
                          "city", "Shanghai Pudong",
                          "description", "yellow hair golden tail");

        this.commitTx();

        List<Vertex> vertices;
        vertices = graph().traversal().V()
                          .has("description", Text.contains("yellow hair"))
                          .has("city", Text.contains("Shanghai"))
                          .toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("age", P.gte(2))
                          .has("description", Text.contains("yellow hair"))
                          .has("city", Text.contains("Zhangjiang"))
                          .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V()
                          .has("age", P.gt(2))
                          .has("description", Text.contains("yellow hair"))
                          .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V()
                          .has("age", P.gt(2))
                          .has("description", Text.contains("yellow hair"))
                          .has("city", Text.contains("Beijing"))
                          .toList();
        Assert.assertEquals(0, vertices.size());

        vertices = graph().traversal().V()
                          .has("age", P.gt(0))
                          .has("description", Text.contains("black golden"))
                          .toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("age", P.gt(0))
                          .has("description", Text.contains("black golden"))
                          .has("city", Text.contains("Beijing"))
                          .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V()
                          .has("age", P.gt(0))
                          .has("description", Text.contains("black golden"))
                          .has("city", Text.contains("Chaoyang"))
                          .toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryByJointIndexesWithSearchAndTwoRangeIndexesAndWithin() {
        SchemaManager schema = graph().schema();

        schema.propertyKey("type").asInt().ifNotExist().create();
        schema.propertyKey("kid").asInt().ifNotExist().create();
        schema.propertyKey("name").asText().ifNotExist().create();
        schema.propertyKey("confirmType").asInt().ifNotExist().create();

        schema.vertexLabel("test")
              .properties("confirmType", "name", "kid", "type")
              .nullableKeys("confirmType", "name", "type")
              .primaryKeys("kid")
              .ifNotExist().create();

        schema.indexLabel("test_by_type")
              .onV("test").by("type")
              .range()
              .ifNotExist().create();
        schema.indexLabel("test_by_confirmType")
              .onV("test").by("confirmType")
              .range()
              .ifNotExist().create();
        schema.indexLabel("test_by_name")
              .onV("test").by("name")
              .search()
              .ifNotExist().create();

        HugeGraph graph = graph();

        graph.addVertex(T.label, "test", "name", "诚信",
                        "confirmType", 0, "type", 0, "kid", 0);
        graph.addVertex(T.label, "test", "name", "诚信",
                        "confirmType", 1, "type", 1, "kid", 1);
        graph.addVertex(T.label, "test", "name", "诚信文明",
                        "confirmType", 2, "type", 1, "kid", 2);
        graph.addVertex(T.label, "test", "name", "诚信文明",
                        "confirmType", 3, "type", 1, "kid", 3);

        this.mayCommitTx();

        List<Vertex> vertices;
        vertices = graph().traversal().V()
                          .has("type", 1)
                          .has("confirmType", P.within(0, 1, 2, 3))
                          .has("name", Text.contains("诚信"))
                          .toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph().traversal().V()
                          .has("type", 1)
                          .has("confirmType", P.within(0, 1, 2, 3))
                          .has("name", Text.contains("文明"))
                          .toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("type", 0)
                          .has("confirmType", P.within(0, 1, 3))
                          .has("name", Text.contains("诚信"))
                          .toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryByShardIndex() {
        SchemaManager schema = graph().schema();

        schema.indexLabel("personByCityAndAge").onV("person").shard()
              .by("city", "age").create();

        HugeGraph graph = graph();

        graph.addVertex(T.label, "person", "name", "p1",
                        "city", "Hongkong", "age", 15);
        graph.addVertex(T.label, "person", "name", "p2",
                        "city", "Hongkong", "age", 18);
        graph.addVertex(T.label, "person", "name", "p3",
                        "city", "Beijing", "age", 21);
        graph.addVertex(T.label, "person", "name", "p4",
                        "city", "Beijing", "age", 23);
        graph.addVertex(T.label, "person", "name", "p5",
                        "city", "Beijing", "age", 29);
        this.mayCommitTx();

        List<Vertex> vertices = graph().traversal().V()
                                       .has("city", "Hongkong")
                                       .toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("city", "Beijing")
                          .toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph().traversal().V()
                          .has("city", "Hongkong")
                          .has("age", 15).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V()
                          .has("city", "Hongkong")
                          .has("age", P.between(10, 20)).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("city", "Beijing")
                          .has("age", P.between(20, 30)).toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph().traversal().V()
                          .has("city", "Beijing")
                          .has("age", P.lt(29)).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("city", "Beijing")
                          .has("age", P.lte(29)).toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph().traversal().V()
                          .has("city", "Beijing")
                          .has("age", P.gt(21)).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("city", "Beijing")
                          .has("age", P.gte(21)).toList();
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testQueryByShardIndexWithMoreStringMoreNumericFields() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("province").asText().create();
        schema.vertexLabel("user")
              .properties("province", "city", "age", "weight")
              .create();
        graph().addVertex(T.label, "user", "province", "Shandong", "city",
                          "Jinan", "age", 20, "weight", 60);
        graph().addVertex(T.label, "user", "province", "Shandong", "city",
                          "Jinan", "age", 25, "weight", 60);
        graph().addVertex(T.label, "user", "province", "Shandong", "city",
                          "Jinan", "age", 30, "weight", 60);
        graph().addVertex(T.label, "user", "province", "Shandong", "city",
                          "Jinan", "age", 30, "weight", 70);
        graph().addVertex(T.label, "user", "province", "Shandong", "city",
                          "Jinan", "age", 30, "weight", 80);
        graph().addVertex(T.label, "user", "province", "Shandong", "city",
                          "Jinan", "age", 30, "weight", 90);
        graph().addVertex(T.label, "user", "province", "Shandong", "city",
                          "Qingdao", "age", 20, "weight", 60);
        graph().addVertex(T.label, "user", "province", "Guangdong", "city",
                          "Guangzhou", "age", 20, "weight", 60);
        graph().addVertex(T.label, "user", "province", "Guangdong", "city",
                          "Guangzhou", "age", 31, "weight", 90);
        graph().addVertex(T.label, "user", "province", "Guangdong", "city",
                          "Foshan", "age", 35, "weight", 60);
        graph().addVertex(T.label, "user", "province", "Guangdong", "city",
                          "Foshan", "age", 38, "weight", 70);
        graph().addVertex(T.label, "user", "province", "Guangdong", "city",
                          "Foshan", "age", 38, "weight", 80);

        schema.indexLabel("userByProvinceCityAgeWeight").onV("user").shard()
              .by("province", "city", "age", "weight").create();

        // Query by prefix "province"
        List<Vertex> vertices = graph().traversal().V()
                                       .has("province", "Shandong")
                                       .toList();
        Assert.assertEquals(7, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Guangdong")
                          .toList();
        Assert.assertEquals(5, vertices.size());

        // Query by prefix "province", "city"
        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .toList();
        Assert.assertEquals(6, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Qingdao")
                          .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Guangdong")
                          .has("city", "Guangzhou")
                          .toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Guangdong")
                          .has("city", "Foshan")
                          .toList();
        Assert.assertEquals(3, vertices.size());

        // Query by prefix "province", "city", "age"
        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", 20)
                          .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", 25)
                          .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", 30)
                          .toList();
        Assert.assertEquals(4, vertices.size());

        // Query by prefix "province", "city" and range "age"
        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", P.lt(25))
                          .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", P.lte(25))
                          .toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", P.gt(25))
                          .toList();
        Assert.assertEquals(4, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", P.gte(25))
                          .toList();
        Assert.assertEquals(5, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", P.between(25, 31))
                          .toList();
        Assert.assertEquals(5, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", P.between(20, 31))
                          .toList();
        Assert.assertEquals(6, vertices.size());

        // Query by prefix "province", "city", "age", "weight"
        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", 30)
                          .has("weight", 60)
                          .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", 30)
                          .has("weight", 70)
                          .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", 30)
                          .has("weight", 80)
                          .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", 30)
                          .has("weight", 90)
                          .toList();
        Assert.assertEquals(1, vertices.size());

        // Query by prefix "province", "city", "age" and range "weight"
        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", 30)
                          .has("weight", P.lt(80))
                          .toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", 30)
                          .has("weight", P.lte(80))
                          .toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", 30)
                          .has("weight", P.gt(80))
                          .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", 30)
                          .has("weight", P.gte(80))
                          .toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", 30)
                          .has("weight", P.between(80, 91))
                          .toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", 30)
                          .has("weight", P.between(70, 91))
                          .toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph().traversal().V()
                          .has("province", "Shandong")
                          .has("city", "Jinan")
                          .has("age", 30)
                          .has("weight", P.between(60, 91))
                          .toList();
        Assert.assertEquals(4, vertices.size());

        // Invalid query
        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V()
                   .has("city", "Jinan")
                   .toList();
        });

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V()
                   .has("age", 30)
                   .toList();
        });

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V()
                   .has("weight", 60)
                   .toList();
        });

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V()
                   .has("city", "Jinan")
                   .has("age", 30)
                   .toList();
        });

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V()
                   .has("province", "Shandong")
                   .has("age", 30)
                   .toList();
        });

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V()
                   .has("province", "Shandong")
                   .has("age", P.between(10, 30))
                   .toList();
        });
    }

    @Test
    public void testQueryCount() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("authorByLived").onV("author")
             .secondary().by("lived").create();
        graph.schema().indexLabel("authorByAge").onV("author")
             .range().by("age").create();
        init10Vertices();

        initPersonIndex(true);
        init100Persons();

        GraphTraversalSource g = graph.traversal();

        Assert.assertEquals(110L, g.V().count().next());

        Assert.assertEquals(2L, g.V().hasLabel("author").count().next());
        Assert.assertEquals(3L, g.V().hasLabel("language").count().next());
        Assert.assertEquals(5L, g.V().hasLabel("book").count().next());
        Assert.assertEquals(8L, g.V().hasLabel("book", "language")
                                 .count().next());

        Assert.assertEquals(1L, g.V().hasLabel("author")
                                 .has("lived", "California")
                                 .count().next());
        Assert.assertEquals(1L, g.V().hasLabel("author")
                                 .has("age", 61).count().next());
        Assert.assertEquals(2L, g.V().hasLabel("author")
                                 .has("age", P.gte(61)).count().next());
        Assert.assertEquals(1L, g.V().hasLabel("author")
                                 .has("age", P.lt(62)).count().next());

        Assert.assertEquals(1L, g.V().hasLabel("author")
                                 .has("age", P.lt(62))
                                 .has("lived", "California")
                                 .count().next());
        Assert.assertEquals(0L, g.V().hasLabel("author")
                                 .has("age", P.lt(62))
                                 .has("lived", "Canadian")
                                 .count().next());
        Assert.assertEquals(1L, g.V().hasLabel("author")
                                 .has("age", P.lt(63))
                                 .has("lived", "Canadian")
                                 .count().next());
        Assert.assertEquals(2L, g.V().hasLabel("author")
                                 .has("age", P.lt(63))
                                 .has("lived", P.within("California",
                                                        "Canadian"))
                                 .count().next());

        Assert.assertEquals(9L, g.V().hasLabel("person")
                                 .has("age", 8)
                                 .count().next());
        Assert.assertEquals(50L, g.V().hasLabel("person")
                                  .has("city", "Beijing")
                                  .count().next());
        Assert.assertEquals(5L, g.V().hasLabel("person")
                                 .has("age", 8)
                                 .has("city", "Beijing")
                                 .count().next());
        Assert.assertEquals(3L, g.V().hasLabel("person")
                                 .has("age", 8)
                                 .has("city", "Beijing")
                                 .limit(3).count().next());

        Assert.assertEquals(110L, g.V().count().min().next());
        Assert.assertEquals(5L, g.V().hasLabel("book").count().max().next());

        Assert.assertEquals(2L, g.V().hasLabel("author")
                                 .values("age").count().next());
        Assert.assertEquals(8L, g.V().hasLabel("author")
                                 .values().count().next());
    }

    @Test
    public void testQueryCountWithOptimizeByIndex() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("authorByLived").onV("author")
             .secondary().by("lived").create();
        graph.schema().indexLabel("authorByAge").onV("author")
             .range().by("age").create();
        init10Vertices();

        initPersonIndex(true);
        init100Persons();

        boolean old = Whitebox.getInternalState(params().graphTransaction(),
                                                "optimizeAggrByIndex");
        Whitebox.setInternalState(params().graphTransaction(),
                                  "optimizeAggrByIndex", true);
        try {
            GraphTraversalSource g = graph.traversal();

            Assert.assertEquals(110L, g.V().count().next());

            Assert.assertEquals(2L, g.V().hasLabel("author").count().next());
            Assert.assertEquals(3L, g.V().hasLabel("language").count().next());
            Assert.assertEquals(5L, g.V().hasLabel("book").count().next());
            Assert.assertEquals(8L, g.V().hasLabel("book", "language")
                                     .count().next());

            Assert.assertEquals(1L, g.V().hasLabel("author")
                                     .has("lived", "California")
                                     .count().next());
            Assert.assertEquals(1L, g.V().hasLabel("author")
                                     .has("age", 61).count().next());
            Assert.assertEquals(2L, g.V().hasLabel("author")
                                     .has("age", P.gte(61)).count().next());
            Assert.assertEquals(1L, g.V().hasLabel("author")
                                     .has("age", P.lt(62)).count().next());

            Assert.assertEquals(1L, g.V().hasLabel("author")
                                     .has("age", P.lt(62))
                                     .has("lived", "California")
                                     .count().next());
            Assert.assertEquals(0L, g.V().hasLabel("author")
                                     .has("age", P.lt(62))
                                     .has("lived", "Canadian")
                                     .count().next());
            Assert.assertEquals(1L, g.V().hasLabel("author")
                                     .has("age", P.lt(63))
                                     .has("lived", "Canadian")
                                     .count().next());
            Assert.assertEquals(2L, g.V().hasLabel("author")
                                     .has("age", P.lt(63))
                                     .has("lived", P.within("California",
                                                            "Canadian"))
                                     .count().next());

            Assert.assertEquals(9L, g.V().hasLabel("person")
                                     .has("age", 8)
                                     .count().next());
            Assert.assertEquals(50L, g.V().hasLabel("person")
                                      .has("city", "Beijing")
                                      .count().next());
            Assert.assertEquals(5L, g.V().hasLabel("person")
                                     .has("age", 8)
                                     .has("city", "Beijing")
                                     .count().next());
            Assert.assertEquals(3L, g.V().hasLabel("person")
                                     .has("age", 8)
                                     .has("city", "Beijing")
                                     .limit(3).count().next());

            Assert.assertEquals(110L, g.V().count().min().next());
            Assert.assertEquals(5L, g.V().hasLabel("book").count().max().next());

            Assert.assertEquals(2L, g.V().hasLabel("author")
                                     .values("age").count().next());
            Assert.assertEquals(8L, g.V().hasLabel("author")
                                     .values().count().next());
        } finally {
            Whitebox.setInternalState(params().graphTransaction(),
                                      "optimizeAggrByIndex", old);
        }
    }

    @Test
    public void testAddVertexWithUniqueIndex() {
        SchemaManager schema = graph().schema();
        schema.vertexLabel("user")
              .properties("name")
              .create();
        schema.indexLabel("userByName").onV("user").by("name").unique()
              .create();
        Vertex v = graph().addVertex(T.label, "user", "name", "Tom");
        this.commitTx();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().addVertex(T.label, "user", "name", "Tom");
            this.commitTx();
        });

        v.remove();
        graph().addVertex(T.label, "user", "name", "Tom");
        this.commitTx();
    }

    @Test
    public void testAddVerticesWithUniqueIndexInTx() {
        SchemaManager schema = graph().schema();
        schema.vertexLabel("user")
              .properties("name")
              .create();
        schema.indexLabel("userByName").onV("user").by("name").unique()
              .create();
        graph().addVertex(T.label, "user", "name", "Tom");
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().addVertex(T.label, "user", "name", "Tom");
            this.commitTx();
        });
    }

    @Test
    public void testUpdatePropertyToValueOfRemovedVertexWithUniqueIndex() {
        SchemaManager schema = graph().schema();
        schema.vertexLabel("user")
              .properties("name")
              .create();
        schema.indexLabel("userByName").onV("user").by("name").unique()
              .create();
        Vertex tom = graph().addVertex(T.label, "user", "name", "Tom");
        Vertex jack = graph().addVertex(T.label, "user", "name", "Jack");
        Vertex james = graph().addVertex(T.label, "user", "name", "James");
        this.commitTx();

        tom.remove();
        this.commitTx();

        jack.property("name", "Tom");
        this.commitTx();

        james.remove();
        jack.property("name", "James");
        this.commitTx();
    }

    @Test
    public void testAddVerticesWithUniqueIndexForNullableProperties() {
        SchemaManager schema = graph().schema();
        schema.vertexLabel("user")
              .properties("name", "city", "age")
              .nullableKeys("name", "city", "age")
              .create();
        schema.indexLabel("userByNameCityAge")
              .onV("user").by("name", "city", "age")
              .unique().create();
        graph().addVertex(T.label, "user", "name", "Tom",
                          "city", "Beijing", "age", 18);
        this.commitTx();

        // Nullable properties
        graph().addVertex(T.label, "user", "name", "Tom", "city", "Beijing");
        this.commitTx();
        graph().addVertex(T.label, "user", "name", "Tom", "age", 18);
        this.commitTx();
        graph().addVertex(T.label, "user", "city", "Beijing", "age", 18);
        this.commitTx();
        graph().addVertex(T.label, "user", "name", "Tom");
        this.commitTx();
        graph().addVertex(T.label, "user", "age", 18);
        this.commitTx();
        graph().addVertex(T.label, "user", "city", "Beijing");
        this.commitTx();
        graph().addVertex(T.label, "user");
        this.commitTx();

        // Empty String properties
        graph().addVertex(T.label, "user", "name", "", "city", "", "age", 18);
        this.commitTx();
        graph().addVertex(T.label, "user", "name", "", "city", "");
        this.commitTx();
        graph().addVertex(T.label, "user", "name", "", "age", 18);
        this.commitTx();
        graph().addVertex(T.label, "user", "city", "", "age", 18);
        this.commitTx();
        graph().addVertex(T.label, "user", "name", "");
        this.commitTx();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().addVertex(T.label, "user", "age", 18);
            this.commitTx();
        }, e -> {
            String message = e.getMessage();
            Assert.assertTrue(message.contains("Unique constraint " +
                                               "userByNameCityAge"));
            Assert.assertTrue(message.contains("conflict is found"));
        });
        graph().addVertex(T.label, "user", "city", "");
        this.commitTx();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().addVertex(T.label, "user");
            this.commitTx();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().addVertex(T.label, "user", "city", "\u0001");
            this.commitTx();
        });
    }

    @Test
    public void testQueryByUniqueIndex() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        schema.vertexLabel("node")
              .properties("name")
              .useAutomaticId()
              .ifNotExist()
              .create();
        schema.indexLabel("nodeByName")
              .unique()
              .onV("node")
              .by("name")
              .ifNotExist()
              .create();

        graph.addVertex(T.label, "node", "name", "tom");
        this.mayCommitTx();

        Assert.assertThrows(NoIndexException.class, () -> {
            graph.traversal().V().hasLabel("node").has("name", "tom").next();
        }, (e) -> {
            Assert.assertEquals("Don't accept query based on properties " +
                                "[name] that are not indexed in label 'node'," +
                                " may not match secondary condition",
                                e.getMessage());
        });
    }

    @Test
    public void testRemoveVertex() {
        HugeGraph graph = graph();
        init10Vertices();

        List<Vertex> vertices = graph.traversal().V().toList();
        Assert.assertEquals(10, vertices.size());
        assertContains(vertices,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");

        Vertex vertex = vertex("author", "id", 1);
        vertex.remove();
        this.mayCommitTx();

        vertices = graph.traversal().V().toList();
        Assert.assertEquals(9, vertices.size());
        assertNotContains(vertices,
                          T.label, "author", "id", 1, "name", "James Gosling",
                          "age", 62, "lived", "Canadian");
    }

    @Test
    public void testRemoveVertexById() {
        HugeGraph graph = graph();

        graph.schema().vertexLabel("author2")
             .properties("id", "name", "age", "lived")
             .primaryKeys("id")
             .nullableKeys("age", "lived")
             .enableLabelIndex(false)
             .create();
        graph.addVertex(T.label, "author2", "id", 1,
                        "name", "James Gosling", "age", 62,
                        "lived", "Canadian");
        graph.addVertex(T.label, "author", "id", 2,
                        "name", "Guido van Rossum", "age", 61,
                        "lived", "California");
        this.mayCommitTx();

        List<Vertex> vertices = graph.traversal().V().toList();
        Assert.assertEquals(2, vertices.size());
        assertContains(vertices,
                       T.label, "author2", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");

        // remove vertex without label index
        Vertex vertex = vertex("author2", "id", 1);
        graph.removeVertex(vertex.label(), vertex.id());
        this.mayCommitTx();

        vertices = graph.traversal().V().toList();
        Assert.assertEquals(1, vertices.size());
        assertNotContains(vertices,
                          T.label, "author2", "id", 1, "name", "James Gosling",
                          "age", 62, "lived", "Canadian");

        // remove vertex with label index
        vertex = vertex("author", "id", 2);
        graph.removeVertex(vertex.label(), vertex.id());
        this.mayCommitTx();

        vertices = graph.traversal().V().toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testRemoveVertexOfNotExists() {
        HugeGraph graph = graph();
        init10Vertices();

        List<Vertex> vertices = graph.traversal().V().toList();
        Assert.assertEquals(10, vertices.size());
        assertContains(vertices,
                       T.label, "author", "id", 1, "name", "James Gosling",
                       "age", 62, "lived", "Canadian");

        Vertex vertex = vertex("author", "id", 1);
        vertex.remove();
        this.mayCommitTx();

        vertices = graph.traversal().V().toList();
        Assert.assertEquals(9, vertices.size());
        assertNotContains(vertices,
                          T.label, "author", "id", 1, "name", "James Gosling",
                          "age", 62, "lived", "Canadian");

        // Remove again
        vertex.remove();
        this.mayCommitTx();
    }

    @Test
    public void testRemoveVertexAfterAddVertexWithTx() {
        HugeGraph graph = graph();
        GraphTransaction tx = params().openTransaction();

        Vertex java1 = tx.addVertex(T.label, "book", "name", "java-1");
        tx.addVertex(T.label, "book", "name", "java-2");
        tx.commit();
        java1.remove();
        tx.commit();
        tx.close();

        List<Vertex> vertices = graph.traversal().V().toList();
        Assert.assertEquals(1, vertices.size());
        assertNotContains(vertices, T.label, "book", "name", "java-1");
    }

    @Test
    public void testRemoveVertexOneByOne() {
        HugeGraph graph = graph();
        init10Vertices();

        List<Vertex> vertices = graph.traversal().V().toList();
        Assert.assertEquals(10, vertices.size());

        for (int i = 0; i < vertices.size(); i++) {
            vertices.get(i).remove();
            this.mayCommitTx();
            Assert.assertEquals(9 - i, graph.traversal().V().toList().size());
        }
    }

    @Test
    public void testAddVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom");
        this.mayCommitTx();

        // Add properties
        vertex.property("name", "Tom2");
        vertex.property("age", 10);
        vertex.property("lived", "USA");
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Tom2", vertex.property("name").value());
        Assert.assertEquals(10, vertex.property("age").value());
        Assert.assertEquals("USA", vertex.property("lived").value());
    }

    @Test
    public void testAddVertexPropertyExisted() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom");
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Tom", vertex.property("name").value());

        vertex.property("name", "Tom2");
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Tom2", vertex.property("name").value());
    }

    @Test
    public void testAddVertexPropertyExistedWithIndex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "person", "name", "Tom",
                                        "city", "Hongkong", "age", 3);
        this.mayCommitTx();

        vertex = vertex("person", "name", "Tom");
        Assert.assertEquals(3, vertex.property("age").value());

        vertex.property("age", 4);
        this.mayCommitTx();

        Assert.assertEquals(4, vertex.property("age").value());
        vertex = vertex("person", "name", "Tom");
        Assert.assertEquals(4, vertex.property("age").value());
    }

    @Test
    public void testAddVertexPropertyNotInVertexLabel() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("time", "2017-1-1");
        });
    }

    @Test
    public void testAddVertexPropertyWithNotExistPropKey() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Jim");
        this.mayCommitTx();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("prop-not-exist", "2017-1-1");
        });
    }

    @Test
    public void testAddVertexPropertyOfPrimaryKey() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Jim");
        this.mayCommitTx();

        Assert.assertEquals(1, vertex.property("id").value());
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Update primary key property
            vertex.property("id", 2);
        });
    }

    @Test
    public void testAddVertexPropertyWithSpecialValueForSecondaryIndex() {
        HugeGraph graph = graph();
        initPersonIndex(true);

        graph.addVertex(T.label, "person", "name", "1",
                        "city", "b\u0001", "age", 1);
        graph.addVertex(T.label, "person", "name", "2",
                        "city", "c\u0002", "age", 2);
        graph.addVertex(T.label, "person", "name", "3",
                        "city", "d\u0003", "age", 3);
        this.mayCommitTx();

        List<Vertex> vertices;

        vertices = graph.traversal().V().has("city", "b\u0001").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("1", vertices.get(0).value("name"));

        vertices = graph.traversal().V().has("city", "c\u0002").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("2", vertices.get(0).value("name"));

        vertices = graph.traversal().V().has("city", "d\u0003").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("3", vertices.get(0).value("name"));

        String backend = graph.backend();
        Set<String> nonZeroBackends = ImmutableSet.of("postgresql",
                                                      "rocksdb", "hbase", "hstore");
        if (nonZeroBackends.contains(backend)) {
            Assert.assertThrows(Exception.class, () -> {
                graph.addVertex(T.label, "person", "name", "0",
                                "city", "a\u0000", "age", 0);
                this.commitTx();
            }, e -> {
                if (e instanceof BackendException) {
                    Assert.assertContains("0x00", e.getCause().getMessage());
                } else {
                    Assert.assertContains("0x00", e.getMessage());
                }
            });
        } else {
            graph.addVertex(T.label, "person", "name", "0",
                            "city", "a\u0000", "age", 0);
            this.commitTx();

            vertices = graph.traversal().V().has("city", "a\u0000").toList();
            Assert.assertEquals(1, vertices.size());
            Assert.assertEquals("0", vertices.get(0).value("name"));
        }
    }

    @Test
    public void testAddVertexPropertyWithIllegalValueForSecondaryIndex() {
        HugeGraph graph = graph();
        initPersonIndex(true);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "person", "name", "Baby",
                            "city", "\u0000", "age", 3);
            this.commitTx();
        }, e -> {
            Assert.assertContains("Illegal leading char '\\u0' in index",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "person", "name", "Baby",
                            "city", "\u0001", "age", 3);
            this.commitTx();
        }, e -> {
            Assert.assertContains("Illegal leading char '\\u1' in index",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "person", "name", "Baby",
                            "city", "\u0002", "age", 3);
            this.commitTx();
        }, e -> {
            Assert.assertContains("Illegal leading char '\\u2' in index",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "person", "name", "Baby",
                            "city", "\u0003", "age", 3);
            this.commitTx();
        }, e -> {
            Assert.assertContains("Illegal leading char '\\u3' in index",
                                  e.getMessage());
        });
    }

    @Test
    public void testAddVertexMultiTimesWithMergedProperties() {
        Assume.assumeTrue("Not support merge vertex property",
                          storeFeatures().supportsMergeVertexProperty());

        HugeGraph graph = graph();
        // "city" is a non-nullable property
        graph.addVertex(T.label, "person", "name", "marko", "city", "Beijing",
                        "birth", "1992-11-17 12:00:00.000");
        this.commitTx();
        Vertex vertex = vertex("person", "name", "marko");
        Assert.assertEquals("Beijing", vertex.value("city"));
        Assert.assertEquals(Utils.date("1992-11-17 12:00:00.000"),
                            vertex.value("birth"));
        Assert.assertFalse(vertex.property("age").isPresent());
        // append property 'age'
        graph.addVertex(T.label, "person", "name", "marko", "city", "Beijing",
                        "age", 26);
        this.commitTx();
        Assert.assertEquals("Beijing", vertex.value("city"));
        vertex = vertex("person", "name", "marko");
        Assert.assertEquals(26, (int) vertex.value("age"));
        Assert.assertEquals(Utils.date("1992-11-17 12:00:00.000"),
                            vertex.value("birth"));
        // update property "birth" and keep the original properties unchanged
        graph.addVertex(T.label, "person", "name", "marko", "city", "Beijing",
                        "birth", "1993-11-17 12:00:00.000");
        this.commitTx();
        vertex = vertex("person", "name", "marko");
        Assert.assertEquals("Beijing", vertex.value("city"));
        Assert.assertEquals(26, (int) vertex.value("age"));
        Assert.assertEquals(Utils.date("1993-11-17 12:00:00.000"),
                            vertex.value("birth"));
    }

    @Test
    public void testRemoveVertexProperty() {
        HugeGraph graph = graph();
        Vertex v = graph.addVertex(T.label, "author", "id", 1,
                                   "name", "Tom", "age", 10, "lived", "USA");
        this.mayCommitTx();

        // Remove "name" property
        Assert.assertTrue(v.property("name").isPresent());
        Assert.assertTrue(v.property("lived").isPresent());
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            v.property("name").remove();
            this.commitTx();
        });
        Assert.assertTrue(v.property("name").isPresent());
        Assert.assertTrue(v.property("lived").isPresent());

        // Remove "lived" property
        Vertex vertex = vertex("author", "id", 1);
        Assert.assertTrue(vertex.property("name").isPresent());
        Assert.assertTrue(vertex.property("lived").isPresent());
        vertex.property("lived").remove();
        this.mayCommitTx();
        Assert.assertTrue(vertex.property("name").isPresent());
        Assert.assertFalse(vertex.property("lived").isPresent());

        vertex = vertex("author", "id", 1);
        Assert.assertEquals(10, vertex.property("age").value());
        Assert.assertTrue(vertex.property("name").isPresent());
        Assert.assertFalse(vertex.property("lived").isPresent());
    }

    @Test
    public void testRemoveVertexPropertyOfPrimaryKey() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "age", 10,
                                        "lived", "USA");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("id").remove();
        });
    }

    @Test
    public void testRemoveVertexPropertyNullableWithIndex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "person", "name", "Baby",
                                        "city", "Hongkong", "age", 3);
        vertex.property("age").remove();
    }

    @Test
    public void testRemoveVertexPropertyNonNullWithIndex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "person", "name", "Baby",
                                        "city", "Hongkong", "age", 3);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("name").remove();
        });
    }

    @Test
    public void testRemoveVertexPropertyNullableWithoutIndex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Baby", "lived", "Hongkong");
        vertex.property("age").remove();
    }

    @Test
    public void testRemoveVertexPropertyNonNullWithoutIndex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Baby", "lived", "Hongkong");
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("name").remove();
        });
    }

    @Test
    public void testQueryVertexByPropertyWithEmptyString() {
        HugeGraph graph = graph();
        initPersonIndex(true);

        graph.addVertex(T.label, "person", "name", "Baby", "city", "");
        this.commitTx();

        Vertex vertex = graph.traversal().V().has("city", "").next();
        Assert.assertEquals("Baby", vertex.value("name"));
        Assert.assertEquals("", vertex.value("city"));
    }

    @Test
    public void testQueryVertexByBooleanPredicate() {
        HugeGraph graph = graph();
        Assume.assumeFalse("skip this test for hstore",
                           Objects.equals("hstore", graph.backend()));

        graph.schema().indexLabel("languageByDynamic").onV("language")
             .secondary().by("dynamic").create();

        graph.addVertex(T.label, "language", "name", "java",
                        "dynamic", true);
        graph.addVertex(T.label, "language", "name", "rust",
                        "dynamic", false);
        graph.addVertex(T.label, "language", "name", "c");
        this.commitTx();

        List<Vertex> neqTrueVertices = graph.traversal().V()
                                             .hasLabel("language")
                                             .has("dynamic", P.neq(true))
                                             .toList();
        Assert.assertEquals(1, neqTrueVertices.size());
        Assert.assertEquals("rust", neqTrueVertices.get(0).value("name"));

        List<Vertex> neqFalseVertices = graph.traversal().V()
                                               .hasLabel("language")
                                               .has("dynamic", P.neq(false))
                                               .toList();
        Assert.assertEquals(1, neqFalseVertices.size());
        Assert.assertEquals("java", neqFalseVertices.get(0).value("name"));

        List<Vertex> hasLtVertices = graph.traversal().V()
                                           .hasLabel("language")
                                           .has("dynamic", P.lt(true))
                                           .toList();
        Assert.assertEquals(1, hasLtVertices.size());
        Assert.assertEquals("rust", hasLtVertices.get(0).value("name"));

        List<Vertex> whereLtVertices = graph.traversal().V()
                                             .hasLabel("language")
                                             .where(__.has("dynamic",
                                                           P.lt(true)))
                                             .toList();
        Assert.assertEquals(1, whereLtVertices.size());
        Assert.assertEquals("rust", whereLtVertices.get(0).value("name"));

        List<Vertex> matchLtVertices = graph.traversal().V()
                                             .hasLabel("language")
                                             .match(__.as("start")
                                                      .where(__.has("dynamic",
                                                                    P.lt(true)))
                                                      .as("matched"))
                                             .<Vertex>select("matched")
                                             .toList();
        Assert.assertEquals(1, matchLtVertices.size());
        Assert.assertEquals("rust", matchLtVertices.get(0).value("name"));

        List<Vertex> compoundOrVertices = graph.traversal().V()
                                                .hasLabel("language")
                                                .has("dynamic",
                                                     P.gt(true).or(P.eq(true)))
                                                .toList();
        Assert.assertEquals(1, compoundOrVertices.size());
        Assert.assertEquals("java", compoundOrVertices.get(0).value("name"));

        Assert.assertEquals(0, graph.traversal().V()
                                    .hasLabel("language")
                                    .has("dynamic", P.lt(false))
                                    .toList().size());

        List<Vertex> gteFalseVertices = graph.traversal().V()
                                             .hasLabel("language")
                                             .has("dynamic", P.gte(false))
                                             .toList();
        Assert.assertEquals(2, gteFalseVertices.size());
        Set<String> gteFalseNames = new HashSet<>();
        for (Vertex vertex : gteFalseVertices) {
            gteFalseNames.add(vertex.value("name"));
        }
        Assert.assertEquals(ImmutableSet.of("java", "rust"), gteFalseNames);
    }

    @Test
    public void testQueryVertexBeforeAfterUpdateMultiPropertyWithIndex() {
        HugeGraph graph = graph();
        initPersonIndex(true);

        Vertex vertex = graph.addVertex(T.label, "person", "name", "Baby",
                                        "city", "Hongkong", "age", 3);
        this.commitTx();

        List<Vertex> vl = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
        vl = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
        vl = graph.traversal().V().has("age", 5).toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().V().has("city", "Shanghai").toList();
        Assert.assertEquals(0, vl.size());

        vertex.property("age", 5);
        vertex.property("city", "Shanghai");
        this.commitTx();

        vl = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().V().has("age", 5).toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
        vl = graph.traversal().V().has("city", "Shanghai").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
    }

    @Test
    public void testQueryVertexBeforeAfterUpdatePropertyWithSecondaryIndex() {
        HugeGraph graph = graph();
        initPersonIndex(true);

        Vertex vertex = graph.addVertex(T.label, "person", "name", "Baby",
                                        "city", "Hongkong", "age", 3);
        this.commitTx();

        List<Vertex> vl = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
        vl = graph.traversal().V().has("city", "Shanghai").toList();
        Assert.assertEquals(0, vl.size());

        vertex.property("city", "Shanghai");
        this.commitTx();

        vl = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().V().has("city", "Shanghai").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
    }

    @Test
    public void testQueryVertexBeforeAfterUpdatePropertyWithRangeIndex() {
        HugeGraph graph = graph();
        initPersonIndex(false);

        Vertex vertex = graph.addVertex(T.label, "person", "name", "Baby",
                                        "city", "Hongkong", "age", 3);
        this.commitTx();

        List<Vertex> vl = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
        vl = graph.traversal().V().has("age", 5).toList();
        Assert.assertEquals(0, vl.size());

        vertex.property("age", 5);
        this.commitTx();

        vl = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().V().has("age", 5).toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("Baby", vl.get(0).value("name"));
    }

    @Test
    public void testQueryVertexWithNullablePropertyInCompositeIndex() {
        HugeGraph graph = graph();
        initComputerIndex();

        graph.addVertex(T.label, "computer", "name", "1st", "band", "10Gbps",
                        "cpu", "2GHz", "ram", "8GB", "price", 1000);
        this.commitTx();

        List<Vertex> vl = graph.traversal().V().has("cpu", "2GHz").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("1st", vl.get(0).value("name"));

        vl = graph.traversal().V().has("cpu", "2GHz")
                  .has("ram", "8GB").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals("1st", vl.get(0).value("name"));
    }

    @Test
    public void testUpdateVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        this.mayCommitTx();

        vertex.property("lived").remove();
        vertex.property("lived", "Shanghai");
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testUpdateVertexPropertyTwice() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        this.mayCommitTx();

        vertex.property("lived").remove();
        vertex.property("lived").remove();

        vertex.property("lived", "Hangzhou");
        vertex.property("lived", "Shanghai");

        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testUpdateVertexPropertyOfNewVertex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");

        vertex.property("lived").remove();
        vertex.property("lived", "Shanghai");

        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testUpdateVertexPropertyOfPrimaryKey() {
        HugeGraph graph = graph();
        graph.addVertex(T.label, "author", "id", 1,
                        "name", "Tom", "lived", "Beijing");
        this.mayCommitTx();

        Vertex vertex = vertex("author", "id", 1);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Update primary key property
            vertex.property("id", 2);
        });
    }

    @Test
    public void testUpdateVertexPropertyOfPrimaryKeyOfNewVertex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Update primary key property
            vertex.property("id", 2);
        });
    }

    @Test
    public void testUpdateVertexPropertyOfAddingVertex() {
        HugeGraph graph = graph();
        Vertex vertex0 = graph.addVertex(T.label, "author", "id", 1,
                                         "name", "Tom", "lived", "Beijing");
        vertex0.property("lived").remove();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        this.commitTx();

        graph.addVertex(T.label, "author", "id", 1,
                        "name", "Tom", "lived", "Beijing");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("lived").remove();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("lived", "Shanghai");
        });
    }

    @Test
    public void testUpdateVertexPropertyOfRemovingVertex() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        this.commitTx();

        vertex.remove();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("lived").remove();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("lived", "Shanghai");
        });
    }

    @Test
    public void testUpdateVertexPropertyOfRemovingVertexWithDrop() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        this.commitTx();

        graph.traversal().V(vertex.id()).drop().iterate();

        // Update on dirty vertex
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("lived").remove();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            vertex.property("lived", "Shanghai");
        });
    }

    @Test
    public void testUpdateVertexPropertyOfAggregateType() {
        Assume.assumeTrue("Not support aggregate property",
                          storeFeatures().supportsAggregateProperty());

        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.propertyKey("worstScore")
              .asInt().valueSingle().calcMin()
              .ifNotExist().create();
        schema.propertyKey("bestScore")
              .asInt().valueSingle().calcMax()
              .ifNotExist().create();
        schema.propertyKey("testNum")
              .asInt().valueSingle().calcSum()
              .ifNotExist().create();
        schema.propertyKey("rank")
              .asInt().valueSet().calcSet()
              .ifNotExist().create();
        schema.propertyKey("reword")
              .asInt().valueList().calcList()
              .ifNotExist().create();

        schema.vertexLabel("student")
              .properties("name", "worstScore", "bestScore",
                          "testNum", "rank", "reword")
              .primaryKeys("name")
              .nullableKeys("worstScore", "bestScore",
                            "testNum", "rank", "reword")
              .ifNotExist()
              .create();

        graph.addVertex(T.label, "student", "name", "Tom", "worstScore", 55,
                        "bestScore", 96, "testNum", 1, "rank", 8, "reword", 8);
        this.commitTx();

        Vertex tom = graph.traversal().V().hasLabel("student")
                          .has("name", "Tom").next();
        Assert.assertEquals(55, tom.value("worstScore"));
        Assert.assertEquals(96, tom.value("bestScore"));
        Assert.assertEquals(1, tom.value("testNum"));
        Assert.assertEquals(ImmutableSet.of(8), tom.value("rank"));
        Assert.assertEquals(ImmutableList.of(8), tom.value("reword"));

        tom.property("worstScore", 45);
        tom.property("bestScore", 98);
        tom.property("testNum", 1);
        tom.property("rank", 12);
        tom.property("reword", 12);
        this.commitTx();

        tom = graph.traversal().V().hasLabel("student")
                   .has("name", "Tom").next();
        Assert.assertEquals(45, tom.value("worstScore"));
        Assert.assertEquals(98, tom.value("bestScore"));
        Assert.assertEquals(2, tom.value("testNum"));
        Assert.assertEquals(ImmutableSet.of(8, 12), tom.value("rank"));
        Assert.assertEquals(ImmutableList.of(8, 12), tom.value("reword"));

        tom.property("worstScore", 65);
        tom.property("bestScore", 99);
        tom.property("testNum", 1);
        tom.property("rank", 8);
        tom.property("reword", 8);
        this.commitTx();

        tom = graph.traversal().V().hasLabel("student")
                   .has("name", "Tom").next();
        Assert.assertEquals(45, tom.value("worstScore"));
        Assert.assertEquals(99, tom.value("bestScore"));
        Assert.assertEquals(3, tom.value("testNum"));
        Assert.assertEquals(ImmutableSet.of(8, 12), tom.value("rank"));
        Assert.assertEquals(ImmutableList.of(8, 12, 8),
                            tom.value("reword"));

        tom.property("worstScore", 75);
        tom.property("bestScore", 100);
        tom.property("testNum", 1);
        tom.property("rank", 1);
        tom.property("reword", 1);
        this.commitTx();

        tom = graph.traversal().V().hasLabel("student")
                   .has("name", "Tom").next();
        Assert.assertEquals(45, tom.value("worstScore"));
        Assert.assertEquals(100, tom.value("bestScore"));
        Assert.assertEquals(4, tom.value("testNum"));
        Assert.assertEquals(ImmutableSet.of(1, 8, 12),
                            tom.value("rank"));
        Assert.assertEquals(ImmutableList.of(8, 12, 8, 1),
                            tom.value("reword"));
    }

    @Test
    public void testAddAndUpdateVertexPropertyOfAggregateType() {
        Assume.assumeTrue("Not support aggregate property",
                          storeFeatures().supportsAggregateProperty());

        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.propertyKey("worstScore")
              .asInt().valueSingle().calcMin()
              .ifNotExist().create();
        schema.propertyKey("bestScore")
              .asInt().valueSingle().calcMax()
              .ifNotExist().create();
        schema.propertyKey("testNum")
              .asInt().valueSingle().calcSum()
              .ifNotExist().create();
        schema.propertyKey("rank")
              .asInt().valueSet().calcSet()
              .ifNotExist().create();
        schema.propertyKey("reword")
              .asInt().valueList().calcList()
              .ifNotExist().create();

        schema.vertexLabel("student")
              .properties("name", "worstScore", "bestScore",
                          "testNum", "rank", "reword")
              .primaryKeys("name")
              .nullableKeys("worstScore", "bestScore",
                            "testNum", "rank", "reword")
              .ifNotExist()
              .create();

        Vertex tom = graph.addVertex(T.label, "student", "name", "Tom",
                                     "worstScore", 55, "bestScore", 96,
                                     "testNum", 1, "rank", 8, "reword", 8);
        tom.property("worstScore", 65);
        tom.property("bestScore", 94);
        tom.property("testNum", 2);
        tom.property("rank", 12);
        tom.property("reword", 12);

        Vertex result = graph.traversal().V().hasLabel("student")
                             .has("name", "Tom").next();
        Assert.assertEquals(65, result.value("worstScore"));
        Assert.assertEquals(94, result.value("bestScore"));
        Assert.assertEquals(2, result.value("testNum"));
        Assert.assertEquals(ImmutableSet.of(8, 12), result.value("rank"));
        Assert.assertEquals(ImmutableList.of(8, 12), result.value("reword"));

        this.commitTx();

        result = graph.traversal().V().hasLabel("student")
                      .has("name", "Tom").next();
        Assert.assertEquals(65, result.value("worstScore"));
        Assert.assertEquals(94, result.value("bestScore"));
        Assert.assertEquals(2, result.value("testNum"));
        Assert.assertEquals(ImmutableSet.of(8, 12), result.value("rank"));
        Assert.assertEquals(ImmutableList.of(8, 12), result.value("reword"));

        tom = graph.addVertex(T.label, "student", "name", "Tom",
                              "worstScore", 55, "bestScore", 96,
                              "testNum", 1, "rank", 2, "reword", 2);
        tom.property("worstScore", 75);
        tom.property("bestScore", 92);
        tom.property("testNum", 2);
        tom.property("rank", 1);
        tom.property("reword", 1);

        Assert.assertEquals(75, tom.value("worstScore"));
        Assert.assertEquals(92, tom.value("bestScore"));
        Assert.assertEquals(2, tom.value("testNum"));
        Assert.assertEquals(ImmutableSet.of(1, 2),
                            tom.value("rank"));
        Assert.assertEquals(ImmutableList.of(2, 1),
                            tom.value("reword"));

        Assert.assertEquals(75, tom.property("worstScore").value());
        Assert.assertEquals(92, tom.property("bestScore").value());
        Assert.assertEquals(2, tom.property("testNum").value());
        Assert.assertEquals(ImmutableSet.of(1, 2),
                            tom.property("rank").value());
        Assert.assertEquals(ImmutableList.of(2, 1),
                            tom.value("reword"));

        this.commitTx();
        result = graph.traversal().V().hasLabel("student")
                      .has("name", "Tom").next();
        Assert.assertEquals(65, result.value("worstScore"));
        Assert.assertEquals(94, result.value("bestScore"));
        Assert.assertEquals(4, result.value("testNum"));
        Assert.assertEquals(ImmutableSet.of(1, 2, 8, 12),
                            result.value("rank"));
        Assert.assertEquals(ImmutableList.of(8, 12, 2, 1),
                            result.value("reword"));
    }

    @Test
    public void testQueryVertexByAggregateProperty() {
        Assume.assumeTrue("Not support aggregate property",
                          storeFeatures().supportsAggregateProperty());

        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.propertyKey("worstScore")
              .asInt().valueSingle().calcMin()
              .ifNotExist().create();
        schema.propertyKey("bestScore")
              .asInt().valueSingle().calcMax()
              .ifNotExist().create();
        schema.propertyKey("testNum")
              .asInt().valueSingle().calcSum()
              .ifNotExist().create();
        schema.propertyKey("no")
              .asText().valueSingle().calcOld()
              .ifNotExist().create();
        schema.propertyKey("rank")
              .asInt().valueSet().calcSet()
              .ifNotExist().create();
        schema.propertyKey("reword")
              .asInt().valueList().calcList()
              .ifNotExist().create();

        schema.vertexLabel("student")
              .properties("name", "worstScore", "bestScore",
                          "testNum", "no", "rank", "reword")
              .primaryKeys("name")
              .nullableKeys("worstScore", "bestScore",
                            "testNum", "no", "rank", "reword")
              .ifNotExist()
              .create();

        schema.indexLabel("studentByWorstScore")
              .onV("student").by("worstScore").range().ifNotExist().create();
        schema.indexLabel("studentByBestScore")
              .onV("student").by("bestScore").range().ifNotExist().create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("studentByTestNum")
                  .onV("student").by("testNum").range().ifNotExist().create();
        }, e -> {
            Assert.assertContains("The aggregate type SUM is not indexable",
                                  e.getMessage());
        });
        schema.indexLabel("studentByNo")
              .onV("student").by("no").secondary().ifNotExist().create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("studentByRank")
                  .onV("student").by("rank").secondary().ifNotExist().create();
        }, e -> {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                    "The aggregate type SET is not indexable"));
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("studentByReword")
                  .onV("student").by("reword").secondary().ifNotExist()
                  .create();
        }, e -> {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                    "The aggregate type LIST is not indexable"));
        });

        graph.addVertex(T.label, "student", "name", "Tom", "worstScore", 55,
                        "bestScore", 96, "testNum", 1, "no", "001");
        this.commitTx();

        List<Vertex> vertices = graph.traversal().V().hasLabel("student")
                                     .has("name", "Tom").toList();
        Assert.assertEquals(1, vertices.size());
        Vertex tom = vertices.get(0);
        Assert.assertEquals(55, tom.value("worstScore"));
        Assert.assertEquals(96, tom.value("bestScore"));
        Assert.assertEquals(1, tom.value("testNum"));
        Assert.assertEquals("001", tom.value("no"));

        List<Vertex> results = graph.traversal().V()
                                    .has("worstScore", P.gt(50)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("worstScore", P.lt(60)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("worstScore", 55).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", P.gt(50)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", P.lt(100)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", 96).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("no", "001").toList();
        Assert.assertEquals(vertices, results);

        tom.property("worstScore", 45);
        tom.property("bestScore", 98);
        tom.property("testNum", 1);
        tom.property("no", "002");
        this.commitTx();

        vertices = graph.traversal().V().hasLabel("student")
                        .has("name", "Tom").toList();
        Assert.assertEquals(1, vertices.size());
        tom = vertices.get(0);
        Assert.assertEquals(45, tom.value("worstScore"));
        Assert.assertEquals(98, tom.value("bestScore"));
        Assert.assertEquals(2, tom.value("testNum"));
        Assert.assertEquals("001", tom.value("no"));

        results = graph.traversal().V().has("worstScore", P.gt(30)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("worstScore", P.lt(60)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("worstScore", 45).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", P.gt(50)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", P.lt(100)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", 98).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("no", "001").toList();
        Assert.assertEquals(vertices, results);

        tom = graph.traversal().V().hasLabel("student")
                   .has("name", "Tom").next();
        tom.property("worstScore", 65);
        tom.property("bestScore", 99);
        tom.property("testNum", 1);
        tom.property("no", "003");
        this.commitTx();

        vertices = graph.traversal().V().hasLabel("student")
                        .has("name", "Tom").toList();
        Assert.assertEquals(1, vertices.size());
        tom = vertices.get(0);
        Assert.assertEquals(45, tom.value("worstScore"));
        Assert.assertEquals(99, tom.value("bestScore"));
        Assert.assertEquals(3, tom.value("testNum"));
        Assert.assertEquals("001", tom.value("no"));

        results = graph.traversal().V().has("worstScore", P.gt(30)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("worstScore", P.lt(60)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("worstScore", 45).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", P.gt(50)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", P.lt(100)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", 99).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("no", "001").toList();
        Assert.assertEquals(vertices, results);

        tom = graph.traversal().V().hasLabel("student")
                   .has("name", "Tom").next();
        tom.property("worstScore", 75);
        tom.property("bestScore", 100);
        tom.property("testNum", 1);
        tom.property("no", "004");
        this.commitTx();

        vertices = graph.traversal().V().hasLabel("student")
                        .has("name", "Tom").toList();
        Assert.assertEquals(1, vertices.size());
        tom = vertices.get(0);
        Assert.assertEquals(45, tom.value("worstScore"));
        Assert.assertEquals(100, tom.value("bestScore"));
        Assert.assertEquals(4, tom.value("testNum"));
        Assert.assertEquals("001", tom.value("no"));

        results = graph.traversal().V().has("worstScore", P.gt(30)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("worstScore", P.lt(60)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("worstScore", 45).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", P.gt(50)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", P.lt(101)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", 100).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("no", "001").toList();
        Assert.assertEquals(vertices, results);

        tom = graph.traversal().V().hasLabel("student")
                   .has("name", "Tom").next();
        tom.property("worstScore", 35);
        tom.property("bestScore", 99);
        tom.property("testNum", 1);
        tom.property("no", "005");
        this.commitTx();

        tom.property("worstScore", 65);
        tom.property("bestScore", 93);
        tom.property("testNum", 1);
        tom.property("no", "006");
        this.commitTx();

        tom.property("worstScore", 58);
        tom.property("bestScore", 63);
        tom.property("testNum", 1);
        tom.property("no", "007");
        this.commitTx();

        vertices = graph.traversal().V().hasLabel("student")
                        .has("name", "Tom").toList();
        Assert.assertEquals(1, vertices.size());
        tom = vertices.get(0);
        Assert.assertEquals(35, tom.value("worstScore"));
        Assert.assertEquals(100, tom.value("bestScore"));
        Assert.assertEquals(7, tom.value("testNum"));
        Assert.assertEquals("001", tom.value("no"));

        results = graph.traversal().V().has("worstScore", P.gt(30)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("worstScore", P.lt(60)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("worstScore", 35).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", P.gt(50)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", P.lt(101)).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("bestScore", 100).toList();
        Assert.assertEquals(vertices, results);

        results = graph.traversal().V().has("no", "001").toList();
        Assert.assertEquals(vertices, results);
    }

    @Test
    public void testUpdateVertexWithAggregatePropertyMultiTimes() {
        Assume.assumeTrue("Not support aggregate property",
                          storeFeatures().supportsAggregateProperty());

        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.propertyKey("worstScore")
              .asInt().valueSingle().calcMin()
              .ifNotExist().create();
        schema.propertyKey("bestScore")
              .asInt().valueSingle().calcMax()
              .ifNotExist().create();
        schema.propertyKey("testNum")
              .asInt().valueSingle().calcSum()
              .ifNotExist().create();
        schema.propertyKey("no")
              .asText().valueSingle().calcOld()
              .ifNotExist().create();
        schema.propertyKey("rank")
              .asInt().valueSet().calcSet()
              .ifNotExist().create();
        schema.propertyKey("reword")
              .asInt().valueList().calcList()
              .ifNotExist().create();

        schema.vertexLabel("student")
              .properties("name", "worstScore", "bestScore",
                          "testNum", "no", "rank", "reword")
              .primaryKeys("name")
              .nullableKeys("worstScore", "bestScore",
                            "testNum", "no", "rank", "reword")
              .ifNotExist()
              .create();

        schema.indexLabel("studentByWorstScore")
              .onV("student").by("worstScore").range().ifNotExist().create();
        schema.indexLabel("studentByBestScore")
              .onV("student").by("bestScore").range().ifNotExist().create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("studentByTestNum")
                  .onV("student").by("testNum").range().ifNotExist().create();
        }, e -> {
            Assert.assertContains("The aggregate type SUM is not indexable",
                                  e.getMessage());
        });
        schema.indexLabel("studentByNo")
              .onV("student").by("no").secondary().ifNotExist().create();

        graph.addVertex(T.label, "student", "name", "Tom", "worstScore", 55,
                        "bestScore", 96, "testNum", 1, "no", "001",
                        "rank", 1, "reword", 1);
        this.commitTx();

        List<Vertex> vertices = graph.traversal().V().hasLabel("student")
                                     .has("name", "Tom").toList();
        Assert.assertEquals(1, vertices.size());
        Vertex tom = vertices.get(0);
        Assert.assertEquals(55, tom.value("worstScore"));
        Assert.assertEquals(96, tom.value("bestScore"));
        Assert.assertEquals(1, tom.value("testNum"));
        Assert.assertEquals("001", tom.value("no"));
        Assert.assertEquals(ImmutableSet.of(1), tom.value("rank"));
        Assert.assertEquals(ImmutableList.of(1), tom.value("reword"));

        Set<Integer> ranks = new HashSet<>();
        List<Integer> rewords = new ArrayList<>();
        int rank;
        int reword;
        ranks.add(1);
        rewords.add(1);
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            tom.property("worstScore", 65);
            tom.property("bestScore", 94);
            tom.property("testNum", 1);
            tom.property("no", "002");
            rank = random.nextInt();
            ranks.add(rank);
            tom.property("rank", rank);
            reword = random.nextInt();
            rewords.add(reword);
            tom.property("reword", reword);
            this.commitTx();

            tom = graph.traversal().V().hasLabel("student")
                       .has("name", "Tom").next();

            Assert.assertEquals(55, tom.value("worstScore"));
            Assert.assertEquals(96, tom.value("bestScore"));
            Assert.assertEquals(i + 2, tom.value("testNum"));
            Assert.assertEquals("001", tom.value("no"));
            Assert.assertEquals(ranks, tom.value("rank"));
            Assert.assertEquals(rewords, tom.value("reword"));
        }
    }

    // Insert -> {Insert, Delete, Append, Eliminate}
    @Test
    public void testInsertAndInsertVertex() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "author", "id", 1,
                        "name", "Tom", "lived", "Beijing");
        graph.addVertex(T.label, "author", "id", 1,
                        "name", "Tom", "lived", "Shanghai");
        this.mayCommitTx();

        Vertex vertex = vertex("author", "id", 1);
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testInsertAndDeleteVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.remove();
        this.mayCommitTx();

        Assert.assertNull(vertex("author", "id", 1));
    }

    @Test
    public void testInsertAndAppendVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived", "Shanghai");
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testInsertAndEliminateVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived").remove();
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertFalse(vertex.property("lived").isPresent());
    }

    // Delete -> {Insert, Delete, Append, Eliminate}
    @Test
    public void testDeleteAndInsertVertex() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "author", "id", 1,
                        "name", "Tom", "lived", "Beijing");

        graph.traversal().V().hasLabel("author").has("id", 1).next().remove();
        graph.addVertex(T.label, "author", "id", 1,
                        "name", "Tom", "lived", "Shanghai");
        this.mayCommitTx();

        Vertex vertex = vertex("author", "id", 1);
        Assert.assertTrue(vertex.property("lived").isPresent());
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testDeleteAndDeleteVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.remove();
        vertex.remove();
        this.mayCommitTx();

        Assert.assertNull(vertex("author", "id", 1));
    }

    @Test
    public void testDeleteAndAppendVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.remove();
        vertex.property("lived", "Shanghai");
        this.mayCommitTx();

        Assert.assertNull(vertex("author", "id", 1));
    }

    @Test
    public void testDeleteAndEliminateVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.remove();
        vertex.property("lived").remove();
        this.mayCommitTx();

        Assert.assertNull(vertex("author", "id", 1));
    }

    // Append -> {Insert, Delete, Append, Eliminate}
    @Test
    public void testAppendAndInsertVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived", "Wuhan");
        graph.addVertex(T.label, "author", "id", 1, "name", "Tom",
                        "lived", "Shanghai");
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertTrue(vertex.property("lived").isPresent());
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testAppendAndDeleteVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived", "Wuhan");
        vertex.remove();
        this.mayCommitTx();

        Assert.assertNull(vertex("author", "id", 1));
    }

    @Test
    public void testAppendAndAppendSameVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived", "Wuhan");
        vertex.property("lived", "Shanghai");
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testAppendAndAppendDifferentVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("name", "Tomcat");
        vertex.property("lived", "Shanghai");
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertEquals("Tomcat", vertex.property("name").value());
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testAppendAndEliminateSameVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived", "Shanghai");
        vertex.property("lived").remove();
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertFalse(vertex.property("lived").isPresent());
    }

    @Test
    public void testAppendAndEliminateDifferentVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("name", "Tomcat");
        vertex.property("lived").remove();
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertFalse(vertex.property("lived").isPresent());
        Assert.assertEquals("Tomcat", vertex.property("name").value());
    }

    // Eliminate -> {Insert, Delete, Append, Eliminate}
    @Test
    public void testEliminateAndInsertVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived").remove();
        graph.addVertex(T.label, "author", "id", 1, "name", "Tom",
                        "lived", "Shanghai");
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertTrue(vertex.property("lived").isPresent());
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testEliminateAndDeleteVertex() {
        HugeGraph graph = graph();

        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived").remove();
        vertex.remove();
        this.mayCommitTx();

        Assert.assertNull(vertex("author", "id", 1));
    }

    @Test
    public void testEliminateAndAppendSameVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived").remove();
        vertex.property("lived", "Shanghai");
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertTrue(vertex.property("lived").isPresent());
        Assert.assertEquals("Shanghai", vertex.property("lived").value());
    }

    @Test
    public void testEliminateAndAppendDifferentVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");

        vertex.property("lived").remove();
        vertex.property("name", "Tomcat");
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertFalse(vertex.property("lived").isPresent());
        Assert.assertEquals("Tomcat", vertex.property("name").value());
    }

    @Test
    public void testEliminateAndEliminateSameVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("lived").remove();
        vertex.property("lived").remove();
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertFalse(vertex.property("lived").isPresent());
    }

    @Test
    public void testEliminateAndEliminateDifferentVertexProperty() {
        HugeGraph graph = graph();
        Vertex vertex = graph.addVertex(T.label, "author", "id", 1, "age", 18,
                                        "name", "Tom", "lived", "Beijing");
        vertex.property("age").remove();
        vertex.property("lived").remove();
        this.mayCommitTx();

        vertex = vertex("author", "id", 1);
        Assert.assertFalse(vertex.property("age").isPresent());
        Assert.assertFalse(vertex.property("lived").isPresent());
    }

    @Test
    public void testOverrideVertex() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "person", "name", "marko",
                        "age", 18, "city", "Beijing");
        this.mayCommitTx();
        Vertex vertex = vertex("person", "name", "marko");
        Assert.assertTrue(vertex.property("age").isPresent());
        Assert.assertEquals(18, vertex.value("age"));
        Assert.assertTrue(vertex.property("city").isPresent());
        Assert.assertEquals("Beijing", vertex.value("city"));

        graph.addVertex(T.label, "person", "name", "marko", "city", "Wuhan");
        this.mayCommitTx();
        vertex = vertex("person", "name", "marko");
        Assert.assertFalse(vertex.property("age").isPresent());
        Assert.assertTrue(vertex.property("city").isPresent());
        Assert.assertEquals("Wuhan", vertex.value("city"));

        graph.addVertex(T.label, "person", "name", "marko",
                        "age", 19, "city", "Shanghai");
        this.mayCommitTx();
        vertex = vertex("person", "name", "marko");
        Assert.assertTrue(vertex.property("age").isPresent());
        Assert.assertEquals(19, vertex.value("age"));
        Assert.assertTrue(vertex.property("city").isPresent());
        Assert.assertEquals("Shanghai", vertex.value("city"));
    }

    @Test
    public void testScanVertex() {
        HugeGraph graph = graph();
        // TODO: also support test scan by range
        Assume.assumeTrue("Not support scan",
                          storeFeatures().supportsScanToken() ||
                          storeFeatures().supportsScanKeyRange());
        this.init10VerticesAndCommit();

        List<Vertex> vertices = new LinkedList<>();

        long splitSize = 1 * 1024 * 1024;
        List<Shard> splits = graph.metadata(HugeType.VERTEX, "splits",
                                            splitSize);
        for (Shard split : splits) {
            ConditionQuery q = new ConditionQuery(HugeType.VERTEX);
            q.scan(split.start(), split.end());
            vertices.addAll(ImmutableList.copyOf(graph.vertices(q)));
        }

        Assert.assertEquals(10, vertices.size());
    }

    @Test
    public void testScanVertexInPaging() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        HugeGraph graph = graph();
        Assume.assumeTrue("Not support scan",
                          storeFeatures().supportsScanToken() ||
                          storeFeatures().supportsScanKeyRange());
        this.init10VerticesAndCommit();

        List<Vertex> vertices = new LinkedList<>();
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);

        String backend = graph.backend();
        if (backend.equals("cassandra") || backend.equals("scylladb")) {
            query.scan(String.valueOf(Long.MIN_VALUE),
                       String.valueOf(Long.MAX_VALUE));
        } else {
            query.scan(BackendTable.ShardSplitter.START,
                       BackendTable.ShardSplitter.END);
        }

        query.limit(1);
        String page = PageInfo.PAGE_NONE;
        while (page != null) {
            query.page(page);
            Iterator<Vertex> iterator = graph.vertices(query);
            while (iterator.hasNext()) {
                Vertex vertex = iterator.next();
                Assert.assertTrue(query.test((HugeElement) vertex));
                vertices.add(vertex);
            }
            page = PageInfo.pageInfo(iterator);
            CloseableIterator.closeIterator(iterator);
        }
        Assert.assertEquals(10, vertices.size());
    }

    @Test
    public void testScanVertexWithSplitSizeLt1MB() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support scan",
                          storeFeatures().supportsScanToken() ||
                          storeFeatures().supportsScanKeyRange());
        init10Vertices();

        long splitSize = 1 * 1024 * 1024 - 1;
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.metadata(HugeType.VERTEX, "splits", splitSize);
        });
    }

    @Test
    public void testScanVertexWithSplitSizeTypeError() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support scan",
                          storeFeatures().supportsScanToken() ||
                          storeFeatures().supportsScanKeyRange());
        init10Vertices();

        String splitSize = "123456";
        Assert.assertThrows(ClassCastException.class, () -> {
            graph.metadata(HugeType.VERTEX, "splits", splitSize);
        });
    }

    @Test
    public void testScanVertexWithoutSplitSize() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support scan",
                          storeFeatures().supportsScanToken() ||
                          storeFeatures().supportsScanKeyRange());
        init10Vertices();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.metadata(HugeType.VERTEX, "splits");
        });

        long splitSize = 1 * 1024 * 1024;
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.metadata(HugeType.VERTEX, "splits", splitSize, "invalid-arg");
        });
    }

    @Test
    public void testQuerySingleIndexedPropertyByEqual() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertices = graph.traversal().V()
                                     .has("band", "lenovo").toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V().has("band", "apple").toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQuerySingleIndexedPropertyByNotEqual() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertices = graph.traversal().V()
                                     .has("band", "acer").toList();
        Assert.assertEquals(0, vertices.size());

        vertices = graph.traversal().V().has("band", "Hp").toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryComplexIndexedPropertyByEqualOnePrefix() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertices = graph.traversal().V()
                                     .has("cpu", "3.2GHz").toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph.traversal().V().has("cpu", "4.6GHz").toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryComplexIndexedPropertyByNotEqualOnePrefix() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertices = graph.traversal().V()
                                     .has("cpu", "2.8GHz").toList();
        Assert.assertEquals(0, vertices.size());

        vertices = graph.traversal().V().has("cpu", "4.8GHz").toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryComplexIndexedPropertyByEqualTwoPrefix() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertices = graph.traversal().V()
                                     .has("cpu", "3.2GHz")
                                     .has("ram", "16GB")
                                     .toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V()
                        .has("ram", "32GB")
                        .has("cpu", "4.6GHz")
                        .toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryComplexIndexedPropertyByNotEqualTwoPrefix() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertices = graph.traversal().V()
                                     .has("cpu", "3.3GHz")
                                     .has("ram", "16GB")
                                     .toList();
        Assert.assertEquals(0, vertices.size());

        vertices = graph.traversal().V()
                        .has("ram", "32GB")
                        .has("cpu", "4.8GHz")
                        .toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryComplexIndexedPropertyByEqualAll() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertices = graph.traversal().V()
                                     .has("cpu", "3.2GHz")
                                     .has("band", "lenovo")
                                     .has("ram", "16GB")
                                     .toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V()
                        .has("ram", "32GB")
                        .has("cpu", "4.6GHz")
                        .has("band", "microsoft")
                        .toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryComplexIndexedPropertyByNotEqualAll() {
        HugeGraph graph = graph();
        init5Computers();

        List<Vertex> vertices = graph.traversal().V()
                                     .has("cpu", "3.3GHz")
                                     .has("band", "apple")
                                     .has("ram", "16GB")
                                     .toList();
        Assert.assertEquals(0, vertices.size());

        vertices = graph.traversal().V()
                        .has("ram", "32GB")
                        .has("cpu", "4.8GHz")
                        .has("band", "microsoft")
                        .toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryByPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100Books();

        GraphTraversal<Vertex, Vertex> iter = graph.traversal().V()
                                                   .has("~page", "").limit(10);
        Assert.assertEquals(10, IteratorUtils.count(iter));
        String page = TraversalUtil.page(iter);
        CloseableIterator.closeIterator(iter);

        List<Vertex> vertices;

        vertices = graph.traversal().V()
                        .has("~page", page).limit(1)
                        .toList();
        Assert.assertEquals(1, vertices.size());
        Vertex vertex1 = vertices.get(0);

        vertices = graph.traversal().V()
                        .has("~page", page).limit(33)
                        .toList();
        Assert.assertEquals(33, vertices.size());
        Vertex vertex2 = vertices.get(0);
        Assert.assertEquals(vertex1.id(), vertex2.id());
        Assert.assertEquals(vertex1.label(), vertex2.label());
        Assert.assertEquals(IteratorUtils.asList(vertex1.properties()),
                            IteratorUtils.asList(vertex2.properties()));

        vertices = graph.traversal().V()
                        .has("~page", page).limit(89)
                        .toList();
        Assert.assertEquals(89, vertices.size());
        Vertex vertex3 = vertices.get(88);

        vertices = graph.traversal().V()
                        .has("~page", page).limit(90)
                        .toList();
        Assert.assertEquals(90, vertices.size());
        Vertex vertex4 = vertices.get(88);
        Assert.assertEquals(vertex3.id(), vertex4.id());
        Assert.assertEquals(vertex3.label(), vertex4.label());
        Assert.assertEquals(IteratorUtils.asList(vertex3.properties()),
                            IteratorUtils.asList(vertex4.properties()));

        vertices = graph.traversal().V()
                        .has("~page", page).limit(91)
                        .toList();
        Assert.assertEquals(90, vertices.size());
    }

    @Test
    public void testQueryByPageResultsMatchedAll() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100Books();

        List<Vertex> all = graph.traversal().V().toList();

        GraphTraversal<Vertex, Vertex> iter;

        String page = PageInfo.PAGE_NONE;
        int size = 22;

        Set<Vertex> pageAll = new HashSet<>();
        for (int i = 0; i < 100 / size; i++) {
            iter = graph.traversal().V()
                        .has("~page", page).limit(size);
            @SuppressWarnings("unchecked")
            List<Vertex> vertices = IteratorUtils.asList(iter);
            Assert.assertEquals(size, vertices.size());

            pageAll.addAll(vertices);

            page = TraversalUtil.page(iter);
            CloseableIterator.closeIterator(iter);
        }

        iter = graph.traversal().V()
                    .has("~page", page).limit(size);
        @SuppressWarnings("unchecked")
        List<Vertex> vertices = IteratorUtils.asList(iter);
        Assert.assertEquals(12, vertices.size());
        pageAll.addAll(vertices);
        page = TraversalUtil.page(iter);

        Assert.assertEquals(100, pageAll.size());
        Assert.assertTrue(all.containsAll(pageAll));
        Assert.assertNull(page);
    }

    @Test
    public void testQueryByPageResultsMatchedAllWithFullPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100Books();

        List<Vertex> all = graph.traversal().V().toList();

        GraphTraversal<Vertex, Vertex> iter;

        String page = PageInfo.PAGE_NONE;
        int size = 20;

        Set<Vertex> pageAll = new HashSet<>();
        for (int i = 0; i < 100 / size; i++) {
            iter = graph.traversal().V()
                        .has("~page", page).limit(size);
            @SuppressWarnings("unchecked")
            List<Vertex> vertices = IteratorUtils.asList(iter);
            Assert.assertEquals(size, vertices.size());

            pageAll.addAll(vertices);

            page = TraversalUtil.page(iter);
            CloseableIterator.closeIterator(iter);
        }
        Assert.assertEquals(100, pageAll.size());
        Assert.assertTrue(all.containsAll(pageAll));

        if (page != null) {
            iter = graph.traversal().V().has("~page", page);
            long count = IteratorUtils.count(iter);
            Assert.assertEquals(0L, count);

            page = TraversalUtil.page(iter);
            CloseableIterator.closeIterator(iter);
        }
        Assert.assertNull(page);
    }

    @Test
    public void testQueryByPageWithInvalidPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100Books();

        // Illegal base64 character
        Assert.assertThrows(BackendException.class, () -> {
            graph.traversal().V()
                 .has("~page", "!abc123#").limit(10)
                 .toList();
        });

        // Invalid page
        Assert.assertThrows(BackendException.class, () -> {
            graph.traversal().V()
                 .has("~page", "abc123").limit(10)
                 .toList();
        });
    }

    @Test
    public void testQueryByPageWithSpecialBase64Chars() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());
        final String pageWith3Base64Chars = "AAAAADsyABwAEAqI546LS6WW57unBgA" +
                                            "EAAAAAPB////+8H////4alhxAZS8va6" +
                                            "opcAKpklipAAQAAAAAAAAAAQ==";

        final String pageWithSpace = "AAAAADsyABwAEAqI546LS6WW57unBgAEAAAAAP" +
                                     "B//// 8H////4alhxAZS8va6opcAKpklipAAQA" +
                                     "AAAAAAAAAQ==";

        HugeGraph graph = graph();
        init100Books();

        // Contains valid character '+' and '/' and '='
        GraphTraversal<Vertex, Vertex> traversal;
        traversal = graph.traversal().V()
                         .has("~page", pageWith3Base64Chars).limit(10);
        Assert.assertNotNull(traversal);
        CloseableIterator.closeIterator(traversal);

        // Contains invalid base64 character ' ', will be replaced to '+'
        traversal = graph.traversal().V()
                         .has("~page", pageWithSpace).limit(10);
        Assert.assertNotNull(traversal);
        CloseableIterator.closeIterator(traversal);
    }

    @Test
    public void testQueryByPageWithInvalidLimit() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        initPageTestData();
        GraphTraversalSource g = graph.traversal();
        long limit = Query.defaultCapacity() + 1;

        Assert.assertThrows(IllegalStateException.class, () -> {
            g.V().has("~page", "").limit(0).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            g.V().has("~page", "").limit(limit).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            g.V().has("name", "marko").has("~page", "").limit(limit).toList();
        });
    }

    @Test
    public void testQueryByPageWithCapacityAndNoLimit() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        initPageTestData();
        GraphTraversalSource g = graph.traversal();

        long capacity = 10;
        long old = Query.defaultCapacity(capacity);
        try {
            GraphTraversal<Vertex, Vertex> iter;
            iter = g.V().has("~page", "").limit(capacity);
            Assert.assertEquals(10, IteratorUtils.count(iter));
            CloseableIterator.closeIterator(iter);

            Assert.assertThrows(IllegalArgumentException.class, () -> {
                /*
                 * When query vertices/edge in page, the limit will be regard
                 * as page size, it shouldn't exceed capacity
                 */
                g.V().has("~page", "").limit(capacity + 1).toList();
            });

            Assert.assertThrows(LimitExceedException.class, () -> {
                g.V().has("~page", "").limit(-1).toList();
            });
        } finally {
            Query.defaultCapacity(old);
        }
    }

    @Test
    public void testQueryInPageWithoutCapacity() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        initPageTestData();

        long old = Query.defaultCapacity(Query.NO_CAPACITY);
        try {
            GraphTraversal<Vertex, Vertex> iter;
            iter = g.V().has("~page", "").limit(-1);
            Assert.assertEquals(34, IteratorUtils.count(iter));
            CloseableIterator.closeIterator(iter);

            iter = g.V().has("~page", "").limit(20);
            Assert.assertEquals(20, IteratorUtils.count(iter));
            CloseableIterator.closeIterator(iter);

            iter = g.V().has("age", 30).has("~page", "").limit(-1);
            Assert.assertEquals(18, IteratorUtils.count(iter));
            CloseableIterator.closeIterator(iter);

            iter = g.V().has("age", 30).has("~page", "").limit(10);
            Assert.assertEquals(10, IteratorUtils.count(iter));
            CloseableIterator.closeIterator(iter);
        } finally {
            Query.defaultCapacity(old);
        }
    }

    @Test
    public void testQueryByPageWithOffset() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100Books();

        Assert.assertThrows(IllegalStateException.class, () -> {
            graph.traversal().V()
                 .has("~page", "").range(2, 10)
                 .toList();
        });
    }

    @Test
    public void testQueryByPageWithtUncommittedRecords() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();

        graph.addVertex(T.label, "author", "id", 1,
                        "name", "James Gosling", "age", 62,
                        "lived", "Canadian");
        graph.addVertex(T.label, "author", "id", 2,
                        "name", "Guido van Rossum", "age", 61,
                        "lived", "California");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().V()
                 .has("~page", "")
                 .toList();
        });
    }

    @Test
    public void testQueryByLabelInPageWithLimitLtePageSize() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        initPageTestData();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            g.V().hasLabel("unknown").has("~page", "").limit(1).toList();
        });

        Assert.assertThrows(IllegalStateException.class, () -> {
            g.V().hasLabel("programmer").has("~page", "").limit(0).toList();
        });

        List<Vertex> vertices = g.V().hasLabel("programmer")
                                 .has("~page", "").limit(1)
                                 .toList();
        Assert.assertEquals(1, vertices.size());
        vertices.forEach(v -> {
            Assert.assertEquals("programmer", v.label());
        });

        vertices = g.V().hasLabel("software")
                    .has("~page", "").limit(5)
                    .toList();
        Assert.assertEquals(5, vertices.size());
        vertices.forEach(v -> {
            Assert.assertEquals("software", v.label());
        });

        vertices = g.V().hasLabel("programmer")
                    .has("~page", "").limit(10)
                    .toList();
        Assert.assertEquals(10, vertices.size());
        vertices.forEach(v -> {
            Assert.assertEquals("programmer", v.label());
        });
    }

    @Test
    public void testQueryByLabelInPageWithLimitGtPageSize() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        initPageTestData();

        // Limit > page-size(10), test internal paging
        List<Vertex> vertices = g.V().hasLabel("programmer")
                                 .has("~page", "").limit(11)
                                 .toList();
        Assert.assertEquals(11, vertices.size());
        vertices.forEach(v -> {
            Assert.assertEquals("programmer", v.label());
        });

        vertices = g.V().hasLabel("software")
                    .has("~page", "").limit(15)
                    .toList();
        Assert.assertEquals(15, vertices.size());
        vertices.forEach(v -> {
            Assert.assertEquals("software", v.label());
        });

        vertices = g.V().hasLabel("programmer")
                    .has("~page", "").limit(20)
                    .toList();
        // Programmer only has 18
        Assert.assertEquals(18, vertices.size());
        vertices.forEach(v -> {
            Assert.assertEquals("programmer", v.label());
        });
    }

    @Test
    public void testQueryBySingleLabelInPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        initPageTestData();

        GraphTraversal<Vertex, Vertex> iter = g.V().hasLabel("programmer")
                                               .has("~page", "").limit(1);
        Assert.assertEquals(1, IteratorUtils.count(iter));

        String page = TraversalUtil.page(iter);
        List<Vertex> vertices;

        vertices = g.V().hasLabel("programmer")
                    .has("~page", page).limit(1).toList();
        Assert.assertEquals(1, vertices.size());
        Vertex vertex1 = vertices.get(0);

        vertices = g.V().hasLabel("programmer")
                    .has("~page", page).limit(9).toList();
        Assert.assertEquals(9, vertices.size());
        Vertex vertex2 = vertices.get(0);
        Assert.assertEquals(vertex1.id(), vertex2.id());
        Assert.assertEquals(vertex1.label(), vertex2.label());
        Assert.assertEquals(IteratorUtils.asList(vertex1.properties()),
                            IteratorUtils.asList(vertex2.properties()));

        vertices = g.V().hasLabel("programmer")
                    .has("~page", page).limit(17).toList();
        Assert.assertEquals(17, vertices.size());
        Vertex vertex3 = vertices.get(16);

        vertices = g.V().hasLabel("programmer")
                    .has("~page", page).limit(18).toList();
        Assert.assertEquals(17, vertices.size());
        Vertex vertex4 = vertices.get(16);
        Assert.assertEquals(vertex3.id(), vertex4.id());
        Assert.assertEquals(vertex3.label(), vertex4.label());
        Assert.assertEquals(IteratorUtils.asList(vertex3.properties()),
                            IteratorUtils.asList(vertex4.properties()));
    }

    @Test
    public void testQueryByMultiLabelInPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        initPageTestData();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            g.V().hasLabel("unknown", "programmer")
             .has("~page", "").limit(1)
             .toList();
        });

        GraphTraversal<Vertex, Vertex> iter;

        iter = g.V().hasLabel("programmer", "software").has("~page", "")
                .limit(1);
        Assert.assertEquals(1, IteratorUtils.count(iter));

        String page = TraversalUtil.page(iter);
        List<Vertex> vertices;

        vertices = g.V().hasLabel("programmer", "software")
                    .has("~page", page).limit(1).toList();
        Assert.assertEquals(1, vertices.size());
        Vertex vertex1 = vertices.get(0);

        vertices = g.V().hasLabel("programmer", "software")
                    .has("~page", page).limit(9).toList();
        Assert.assertEquals(9, vertices.size());
        Vertex vertex2 = vertices.get(0);
        Assert.assertEquals(vertex1.id(), vertex2.id());
        Assert.assertEquals(vertex1.label(), vertex2.label());
        Assert.assertEquals(IteratorUtils.asList(vertex1.properties()),
                            IteratorUtils.asList(vertex2.properties()));

        vertices = g.V().hasLabel("programmer", "software")
                    .has("~page", page).limit(18).toList();
        Assert.assertEquals(18, vertices.size());
        Vertex vertex3 = vertices.get(17);

        vertices = g.V().hasLabel("programmer", "software")
                    .has("~page", page).limit(33).toList();
        Assert.assertEquals(33, vertices.size());
        Vertex vertex4 = vertices.get(17);
        Assert.assertEquals(vertex3.id(), vertex4.id());
        Assert.assertEquals(vertex3.label(), vertex4.label());
        Assert.assertEquals(IteratorUtils.asList(vertex3.properties()),
                            IteratorUtils.asList(vertex4.properties()));
    }

    @Test
    public void testQueryByPropertyInPageWithLimitLtePageSize() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        initPageTestData();

        Assert.assertThrows(IllegalStateException.class, () -> {
            g.V().has("name", "marko").has("~page", "").limit(0).toList();
        });

        // Secondary
        List<Vertex> vertices = g.V().has("name", "marko")
                                 .has("~page", "").limit(1)
                                 .toList();
        Assert.assertEquals(1, vertices.size());
        vertices.forEach(v -> {
            Assert.assertEquals("marko", v.value("name"));
        });

        // Range
        vertices = g.V().has("price", P.between(200, 400))
                    .has("~page", "").limit(5)
                    .toList();
        Assert.assertEquals(5, vertices.size());
        vertices.forEach(v -> {
            Assert.assertTrue((int) v.value("price") >= 200);
            Assert.assertTrue((int) v.value("price") < 400);
        });

        // Search
        vertices = g.V().has("city", Text.contains("Beijing"))
                    .has("~page", "").limit(10)
                    .toList();
        Assert.assertEquals(10, vertices.size());
        vertices.forEach(v -> {
            Assert.assertTrue(((String) v.value("city")).contains("Beijing"));
        });
    }

    @Test
    public void testQueryByPropertyInPageWithLimitGtPageSize() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        initPageTestData();

        // Limit > page-size(10), test internal paging
        // Secondary
        List<Vertex> vertices = g.V().has("name", "marko")
                                 .has("~page", "").limit(11)
                                 .toList();
        Assert.assertEquals(11, vertices.size());
        vertices.forEach(v -> {
            Assert.assertEquals("marko", v.value("name"));
        });

        // Range
        vertices = g.V().has("price", P.between(100, 400))
                    .has("~page", "").limit(15)
                    .toList();
        Assert.assertEquals(12, vertices.size());
        vertices.forEach(v -> {
            Assert.assertTrue((int) v.value("price") >= 100);
            Assert.assertTrue((int) v.value("price") < 400);
        });

        // Search
        vertices = g.V().has("city", Text.contains("Beijing"))
                    .has("~page", "").limit(20)
                    .toList();
        Assert.assertEquals(12, vertices.size());
        vertices.forEach(v -> {
            Assert.assertTrue(((String) v.value("city")).contains("Beijing"));
        });
    }

    @Test
    public void testQueryBySingleSecondaryPropertyInPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        initPageTestData();

        GraphTraversal<Vertex, Vertex> iter = g.V().has("name", "marko")
                                               .has("~page", "").limit(1);
        Assert.assertEquals(1, IteratorUtils.count(iter));

        String page = TraversalUtil.page(iter);
        List<Vertex> vertices;

        vertices = g.V().has("name", "marko")
                    .has("~page", page).limit(1).toList();
        Assert.assertEquals(1, vertices.size());
        Vertex vertex1 = vertices.get(0);

        vertices = g.V().has("name", "marko")
                    .has("~page", page).limit(9).toList();
        Assert.assertEquals(9, vertices.size());
        Assert.assertTrue(vertices.contains(vertex1));

        vertices = g.V().has("name", "marko")
                    .has("~page", page).limit(18).toList();
        Assert.assertEquals(18, vertices.size());
        Vertex vertex3 = vertices.get(17);

        vertices = g.V().has("name", "marko")
                    .has("~page", page).limit(40).toList();
        Assert.assertEquals(33, vertices.size());
        Assert.assertTrue(vertices.contains(vertex3));
    }

    @Test
    public void testQueryBySingleRangePropertyInPage() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        initPageTestData();

        GraphTraversal<Vertex, Vertex> iter = g.V().has("price", P.gte(100))
                                               .has("~page", "").limit(1);
        Assert.assertEquals(1, IteratorUtils.count(iter));

        String page = TraversalUtil.page(iter);
        List<Vertex> vertices;

        vertices = g.V().has("price", P.gte(100))
                    .has("~page", page).limit(1).toList();
        Assert.assertEquals(1, vertices.size());
        Vertex vertex1 = vertices.get(0);

        vertices = g.V().has("price", P.gte(100))
                    .has("~page", page).limit(9).toList();
        Assert.assertEquals(9, vertices.size());
        Assert.assertTrue(vertices.contains(vertex1));

        vertices = g.V().has("price", P.gte(100))
                    .has("~page", page).limit(11).toList();
        Assert.assertEquals(11, vertices.size());
        Vertex vertex3 = vertices.get(10);

        vertices = g.V().has("price", P.gte(100))
                    .has("~page", page).limit(20).toList();
        Assert.assertEquals(15, vertices.size());
        Assert.assertTrue(vertices.contains(vertex3));
    }

    @Test
    public void testQueryBySingleSearchPropertyInPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        initPageTestData();

        GraphTraversal<Vertex, Vertex> iter;

        iter = g.V().has("city", Text.contains("Beijing Shanghai"))
                .has("~page", "").limit(1);
        Assert.assertEquals(1, IteratorUtils.count(iter));

        String page = TraversalUtil.page(iter);
        List<Vertex> vertices;

        vertices = g.V().has("city", Text.contains("Beijing Shanghai"))
                    .has("~page", page).limit(1).toList();
        Assert.assertEquals(1, vertices.size());
        Vertex vertex1 = vertices.get(0);

        vertices = g.V().has("city", Text.contains("Beijing Shanghai"))
                    .has("~page", page).limit(9).toList();
        Assert.assertEquals(9, vertices.size());
        Assert.assertTrue(vertices.contains(vertex1));

        vertices = g.V().has("city", Text.contains("Beijing Shanghai"))
                    .has("~page", page).limit(11).toList();
        Assert.assertEquals(11, vertices.size());
        Vertex vertex3 = vertices.get(10);

        vertices = g.V().has("city", Text.contains("Beijing Shanghai"))
                    .has("~page", page).limit(20).toList();
        Assert.assertEquals(17, vertices.size());
        Assert.assertTrue(vertices.contains(vertex3));
    }

    @Test
    public void testQueryByCompositePropertyInPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        initPageTestData();

        GraphTraversal<Vertex, Vertex> iter;

        iter = g.V().has("name", "marko").has("age", 30)
                .has("~page", "").limit(1);
        Assert.assertEquals(1, IteratorUtils.count(iter));

        String page = TraversalUtil.page(iter);
        List<Vertex> vertices;

        vertices = g.V().has("name", "marko").has("age", 30)
                    .has("~page", page).limit(1).toList();
        Assert.assertEquals(1, vertices.size());
        Vertex vertex1 = vertices.get(0);

        vertices = g.V().has("name", "marko").has("age", 30)
                    .has("~page", page).limit(9).toList();
        Assert.assertEquals(9, vertices.size());
        Assert.assertTrue(vertices.contains(vertex1));

        vertices = g.V().has("name", "marko").has("age", 30)
                    .has("~page", page).limit(11).toList();
        Assert.assertEquals(11, vertices.size());
        Vertex vertex3 = vertices.get(10);

        vertices = g.V().has("name", "marko").has("age", 30)
                    .has("~page", page).limit(40).toList();
        Assert.assertEquals(17, vertices.size());
        Assert.assertTrue(vertices.contains(vertex3));
    }

    @Test
    public void testQueryByJointPropertyInPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        initPageTestData();

        Assert.assertThrows(HugeException.class, () -> {
            g.V().has("name", "marko").has("city", Text.contains("Beijing"))
             .has("~page", "").limit(10).toList();
        });

        Assert.assertThrows(HugeException.class, () -> {
            g.V().has("age", 30).has("city", Text.contains("Beijing"))
             .has("~page", "").limit(10).toList();
        });

        Assert.assertThrows(HugeException.class, () -> {
            g.V().has("name", "marko").has("lang", "java")
             .has("~page", "").limit(10).toList();
        });

        Assert.assertThrows(HugeException.class, () -> {
            g.V().has("lang", "java").has("price", 200)
             .has("~page", "").limit(10).toList();
        });
    }

    @Test
    public void testQueryByRangeIndexInPage() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        initPageTestData();

        // There are 4 vertices matched
        GraphTraversal<Vertex, Vertex> iter = g.V().hasLabel("software")
                                               .has("price", P.eq(100))
                                               .has("~page", "")
                                               .limit(3);

        List<Vertex> vertices1 = IteratorUtils.list(iter);
        String page = TraversalUtil.page(iter);
        Assert.assertEquals(3, vertices1.size());
        List<Vertex> vertices2 = g.V().hasLabel("software")
                                  .has("price", P.eq(100))
                                  .has("~page", page).limit(3)
                                  .toList();
        Assert.assertEquals(1, vertices2.size());
        Assert.assertTrue(CollectionUtil.intersect(vertices1, vertices2)
                                        .isEmpty());

        // There are 8 vertices matched
        iter = g.V().hasLabel("software")
                .has("price", P.gt(200))
                .has("~page", "")
                .limit(5);

        vertices1 = IteratorUtils.list(iter);
        Assert.assertEquals(5, vertices1.size());
        page = TraversalUtil.page(iter);
        vertices2 = g.V().hasLabel("software").has("price", P.gt(200))
                     .has("~page", page).limit(5).toList();
        Assert.assertEquals(3, vertices2.size());
        Assert.assertTrue(CollectionUtil.intersect(vertices1, vertices2)
                                        .isEmpty());

        // There are 8 vertices matched
        iter = g.V().hasLabel("software")
                .has("price", P.lt(300))
                .has("~page", "")
                .limit(5);

        vertices1 = IteratorUtils.list(iter);
        Assert.assertEquals(5, vertices1.size());
        page = TraversalUtil.page(iter);
        vertices2 = g.V().hasLabel("software").has("price", P.lt(300))
                     .has("~page", page).limit(5).toList();
        Assert.assertEquals(3, vertices2.size());
        Assert.assertTrue(CollectionUtil.intersect(vertices1, vertices2)
                                        .isEmpty());
    }

    @Test
    public void testQueryByUnionIndexInPageWithSomeIndexNoData() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        GraphTraversalSource g = graph.traversal();
        initPageTestData();

        // Author has index by name but no data
        schema.indexLabel("authorByName")
              .onV("author")
              .by("name")
              .secondary()
              .ifNotExist()
              .create();

        GraphTraversal<Vertex, Vertex> iter = g.V().has("name", "marko")
                                               .has("~page", "").limit(1);
        Assert.assertEquals(1, IteratorUtils.count(iter));

        String page = TraversalUtil.page(iter);
        List<Vertex> vertices;

        vertices = g.V().has("name", "marko")
                    .has("~page", page).limit(1).toList();
        Assert.assertEquals(1, vertices.size());
        Vertex vertex1 = vertices.get(0);

        vertices = g.V().has("name", "marko")
                    .has("~page", page).limit(9).toList();
        Assert.assertEquals(9, vertices.size());
        Assert.assertTrue(vertices.contains(vertex1));

        vertices = g.V().has("name", "marko")
                    .has("~page", page).limit(18).toList();
        Assert.assertEquals(18, vertices.size());
        Vertex vertex3 = vertices.get(17);

        vertices = g.V().has("name", "marko")
                    .has("~page", page).limit(40).toList();
        Assert.assertEquals(33, vertices.size());
        Assert.assertTrue(vertices.contains(vertex3));
    }

    @Test
    public void testQueryBySecondaryIndexWithLimitAndOffset() {
        initPersonIndex(true);
        init5Persons();

        List<Vertex> vertices = graph().traversal().V()
                                       .has("city", "Beijing").toList();
        Assert.assertEquals(3, vertices.size());

        List<Vertex> verticesSkip = graph().traversal().V()
                                           .has("city", "Beijing")
                                           .skip(1).toList();
        Assert.assertEquals(2, verticesSkip.size());

        Set<Vertex> vertices1 = graph().traversal().V()
                                       .has("city", "Beijing")
                                       .range(0, 2).toSet();
        Assert.assertEquals(2, vertices1.size());

        Set<Vertex> vertices2 = graph().traversal().V()
                                       .has("city", "Beijing")
                                       .range(2, 3).toSet();
        Assert.assertEquals(1, vertices2.size());

        vertices1.addAll(vertices2);
        Assert.assertEquals(vertices.size(), vertices1.size());
        Assert.assertTrue(vertices.containsAll(vertices1));
    }

    @Test
    public void testQueryByRangeIndexWithLimitAndOffset() {
        initPersonIndex(false);
        init5Persons();

        List<Vertex> vertices = graph().traversal().V()
                                       .has("age", P.between(5, 22)).toList();
        Assert.assertEquals(4, vertices.size());

        List<Vertex> verticesSkip = graph().traversal().V()
                                           .has("age", P.between(5, 22))
                                           .skip(1).toList();
        Assert.assertEquals(3, verticesSkip.size());

        Set<Vertex> vertices1 = graph().traversal().V()
                                       .has("age", P.between(5, 22))
                                       .range(0, 3).toSet();
        Assert.assertEquals(3, vertices1.size());
        Set<Vertex> vertices2 = graph().traversal().V()
                                       .has("age", P.between(5, 22))
                                       .range(3, 4).toSet();
        Assert.assertEquals(1, vertices2.size());

        vertices1.addAll(vertices2);
        Assert.assertEquals(vertices.size(), vertices1.size());
        Assert.assertTrue(vertices.containsAll(vertices1));
    }

    @Test
    public void testQueryByLabelIndexWithLimitAndOffset() {
        init5Persons();

        List<Vertex> vertices = graph().traversal().V().hasLabel("person")
                                       .toList();
        Assert.assertEquals(5, vertices.size());

        List<Vertex> verticesSkip = graph().traversal().V().hasLabel("person")
                                           .skip(1).toList();
        Assert.assertEquals(4, verticesSkip.size());

        Set<Vertex> vertices1 = graph().traversal().V()
                                       .hasLabel("person")
                                       .range(0, 3).toSet();
        Assert.assertEquals(3, vertices1.size());

        Set<Vertex> vertices2 = graph().traversal().V()
                                       .hasLabel("person")
                                       .range(3, 5).toSet();
        Assert.assertEquals(2, vertices2.size());

        Set<Vertex> vertices3 = graph().traversal().V()
                                       .hasLabel("person")
                                       .limit(4).toSet();
        Assert.assertEquals(4, vertices3.size());

        vertices1.addAll(vertices2);
        Assert.assertEquals(vertices.size(), vertices1.size());
        Assert.assertTrue(vertices.containsAll(vertices1));
    }

    @Test
    public void testAddCustomizedIdVerticesContainsExisted() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("programmer")
              .useCustomizeStringId()
              .properties("name", "age", "city")
              .create();
        schema.vertexLabel("designer")
              .useCustomizeStringId()
              .properties("name", "age", "city")
              .create();

        graph.addVertex(T.label, "programmer", T.id, "123456", "name", "marko",
                        "age", 18, "city", "Beijing");
        this.commitTx();

        graph.addVertex(T.label, "programmer", T.id, "123456", "name", "marko",
                        "age", 19, "city", "Beijing");
        this.commitTx();

        graph.addVertex(T.label, "designer", T.id, "123456", "name", "marko",
                        "age", 18, "city", "Beijing");
        Assert.assertThrows(HugeException.class, () -> {
            this.commitTx();
        });
    }

    @Test
    public void testQueryVerticesByIdsWithHasIdFilterAndNumberId() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("user").useCustomizeNumberId().create();

        graph.addVertex(T.label, "user", T.id, 123);
        graph.addVertex(T.label, "user", T.id, 456);
        graph.addVertex(T.label, "user", T.id, 789);
        this.mayCommitTx();

        GraphTraversalSource g = graph.traversal();
        List<Vertex> vertices;

        vertices = g.V().hasId(P.within(123)).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = g.V(123, 456).hasId(P.within(123)).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = g.V(123, 456).hasId(123).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = g.V(123, 456, 789).hasId(P.within(123, 456)).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = g.V(123, 456, 789).hasId(456, 789).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = g.V(123, 456, 789).hasId(P.within(123, 456, 789)).toList();
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testQueryVerticesByLabelsWithOneLabelNotExist() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("user1").useCustomizeNumberId().create();
        schema.vertexLabel("user2").useCustomizeNumberId().create();

        graph.addVertex(T.label, "user1", T.id, 123);
        graph.addVertex(T.label, "user2", T.id, 456);
        graph.addVertex(T.label, "user2", T.id, 789);
        this.mayCommitTx();

        GraphTraversalSource g = graph.traversal();
        List<Vertex> vertices;

        vertices = g.V().hasLabel("user1").toList();
        Assert.assertEquals(1, vertices.size());

        vertices = g.V().hasLabel("user2").toList();
        Assert.assertEquals(2, vertices.size());

        vertices = g.V().hasLabel("user1", "user2").toList();
        Assert.assertEquals(3, vertices.size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            g.V().hasLabel("user3").toList();
        }, e -> {
            Assert.assertEquals("Undefined vertex label: 'user3'",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            g.V().hasLabel("user1", "user3").toList();
        }, e -> {
            Assert.assertEquals("Undefined vertex label: 'user3'",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            g.V().hasLabel("user3", "user1").toList();
        }, e -> {
            Assert.assertEquals("Undefined vertex label: 'user3'",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            g.V().hasLabel("user3", "user4").toList();
        }, e -> {
            Assert.assertEquals("Undefined vertex label: 'user3'",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            g.V().hasLabel("user4", "user3").toList();
        }, e -> {
            Assert.assertEquals("Undefined vertex label: 'user4'",
                                e.getMessage());
        });
    }

    @Test
    public void testQueryByJointLabels() {
        HugeGraph graph = graph();
        init5Persons();
        init5Computers();
        init10Vertices();

        GraphTraversalSource g = graph.traversal();

        List<Vertex> vertices = g.V().hasLabel("person").hasLabel("computer")
                                 .toList();
        Assert.assertEquals(0, vertices.size());

        vertices = g.V().hasLabel("person").hasLabel("person").toList();
        Assert.assertEquals(5, vertices.size());

        vertices = g.V().hasLabel("person", "computer").hasLabel("person")
                    .toList();
        Assert.assertEquals(5, vertices.size());
        for (Vertex vertex : vertices) {
            Assert.assertEquals("person", vertex.label());
        }

        vertices = g.V().hasLabel("person", "computer").hasLabel("computer")
                    .toList();
        Assert.assertEquals(5, vertices.size());
        for (Vertex vertex : vertices) {
            Assert.assertEquals("computer", vertex.label());
        }

        vertices = g.V().hasLabel("person").hasLabel("person", "computer")
                    .toList();
        Assert.assertEquals(5, vertices.size());
        for (Vertex vertex : vertices) {
            Assert.assertEquals("person", vertex.label());
        }

        vertices = g.V().hasLabel("person", "computer")
                    .hasLabel("person", "author").toList();
        Assert.assertEquals(5, vertices.size());
        for (Vertex vertex : vertices) {
            Assert.assertEquals("person", vertex.label());
        }

        vertices = g.V().hasLabel("person", "computer")
                    .hasLabel("book", "language").toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryByHasIdEmptyList() {
        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();

        List<Vertex> vertices = g.V().hasId(Collections.EMPTY_LIST).toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryByHasIdEmptyListInPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();

        GraphTraversal<Vertex, Vertex> iter = g.V()
                                               .hasId(Collections.EMPTY_LIST)
                                               .has("~page", "").limit(1);
        Assert.assertEquals(0, IteratorUtils.count(iter));

        String page = TraversalUtil.page(iter);
        Assert.assertNull(page);
    }

    @Test
    public void testAddVertexWithSpecialSymbolInPrimaryValues() {
        HugeGraph graph = graph();

        Vertex vertex1 = graph.addVertex(T.label, "person", "name",
                                         "xyz\u0001abc", "city", "Hongkong",
                                         "age", 11);
        Vertex vertex2 = graph.addVertex(T.label, "person", "name",
                                         "xyz\u0002abc", "city", "Hongkong",
                                         "age", 12);
        Vertex vertex3 = graph.addVertex(T.label, "person", "name",
                                         "xyz\u0003abc", "city", "Hongkong",
                                         "age", 13);
        this.mayCommitTx();

        GraphTraversalSource g = graph.traversal();

        Assert.assertEquals(vertex1, g.V().hasLabel("person")
                                      .has("name", "xyz\u0001abc").next());
        Assert.assertEquals(vertex2, g.V().hasLabel("person")
                                      .has("name", "xyz\u0002abc").next());
        Assert.assertEquals(vertex3, g.V().hasLabel("person")
                                      .has("name", "xyz\u0003abc").next());

        if (!graph.backend().equals("postgresql")) {
            Vertex vertex0 = graph.addVertex(T.label, "person", "name",
                                             "xyz\u0000abc", "city", "Hongkong",
                                             "age", 10);
            Assert.assertEquals(vertex0, g.V().hasLabel("person")
                                          .has("name", "xyz\u0000abc").next());
        }

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "person", "name",
                            "\u0000", "city", "Hongkong",
                            "age", 15);
        }, e -> {
            graph.tx().rollback();
            Assert.assertContains("Illegal leading char '\\u0' in index",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "person", "name",
                            "\u0001", "city", "Hongkong",
                            "age", 15);
        }, e -> {
            graph.tx().rollback();
            Assert.assertContains("Illegal leading char '\\u1' in index",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "person", "name",
                            "\u0002", "city", "Hongkong",
                            "age", 15);
        }, e -> {
            graph.tx().rollback();
            Assert.assertContains("Illegal leading char '\\u2' in index",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "person", "name",
                            "\u0003", "city", "Hongkong",
                            "age", 15);
        }, e -> {
            graph.tx().rollback();
            Assert.assertContains("Illegal leading char '\\u3' in index",
                                  e.getMessage());
        });
    }

    @Test
    public void testQueryBySearchIndexWithSpecialSymbol() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("personByCity")
             .onV("person")
             .by("city")
             .search()
             .ifNotExist().create();

        Vertex vertex1 = graph.addVertex(T.label, "person", "name", "1",
                                         "city", "xyz\u0002abc",
                                         "age", 15);
        Vertex vertex2 = graph.addVertex(T.label, "person", "name", "2",
                                         "city", "\u0002",
                                         "age", 15);
        Vertex vertex3 = graph.addVertex(T.label, "person", "name", "3",
                                         "city", "xyz\u0003abc",
                                         "age", 15);
        Vertex vertex4 = graph.addVertex(T.label, "person", "name", "4",
                                         "city", "\u0003",
                                         "age", 15);
        Vertex vertex5 = graph.addVertex(T.label, "person", "name", "5",
                                         "city", "xyz\u0001abc",
                                         "age", 15);
        Vertex vertex6 = graph.addVertex(T.label, "person", "name", "6",
                                         "city", "\u0001",
                                         "age", 15);
        this.commitTx();

        GraphTraversalSource g = graph.traversal();

        String city;
        city = g.V().hasLabel("person").has("name", "1")
                .next().value("city");
        Assert.assertEquals(vertex1.value("city"), city);

        city = g.V().hasLabel("person").has("name", "2")
                .next().value("city");
        Assert.assertEquals(vertex2.value("city"), city);

        city = g.V().hasLabel("person").has("name", "3")
                .next().value("city");
        Assert.assertEquals(vertex3.value("city"), city);

        city = g.V().hasLabel("person").has("name", "4")
                .next().value("city");
        Assert.assertEquals(vertex4.value("city"), city);

        city = g.V().hasLabel("person").has("name", "5")
                .next().value("city");
        Assert.assertEquals(vertex5.value("city"), city);

        city = g.V().hasLabel("person").has("name", "6")
                .next().value("city");
        Assert.assertEquals(vertex6.value("city"), city);

        List<Vertex> vertices;
        vertices = g.V().has("city", Text.contains("abc")).toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(vertex1));
        Assert.assertTrue(vertices.contains(vertex3));
        Assert.assertTrue(vertices.contains(vertex5));

        vertices = g.V().has("city", Text.contains("\u0002")).toList();
        Assert.assertEquals(0, vertices.size());

        vertices = g.V().has("city", Text.contains("\u0003")).toList();
        Assert.assertEquals(0, vertices.size());

        vertices = g.V().has("city", Text.contains("\u0001")).toList();
        Assert.assertEquals(0, vertices.size());

        String backend = graph.backend();
        if (ImmutableSet.of("rocksdb", "hbase", "hstore").contains(backend)) {
            Assert.assertThrows(Exception.class, () -> {
                graph.addVertex(T.label, "person", "name", "0",
                                "city", "xyz\u0000efg", "age", 0);
                graph.tx().commit();
            }, e -> {
                Assert.assertContains("can't contains byte '0x00'",
                                      e.getMessage());
            });
        } else if (backend.equals("postgresql")) {
            Assert.assertThrows(BackendException.class, () -> {
                graph.addVertex(T.label, "person", "name", "7",
                                "city", "xyz\u0000efg",
                                "age", 15);
                graph.tx().commit();
            }, e -> {
                graph.tx().rollback();
                Assert.assertContains("invalid byte sequence for encoding " +
                                      "\"UTF8\": 0x00",
                                      e.getCause().getMessage());
            });
        } else {
            graph.addVertex(T.label, "person", "name", "8",
                            "city", "xyz\u0000efg",
                            "age", 15);
            graph.addVertex(T.label, "person", "name", "9",
                            "city", "\u0000",
                            "age", 15);
            graph.tx().commit();

            Assert.assertTrue(g.V().hasLabel("person")
                               .has("city", Text.contains("efg"))
                               .hasNext());
            Assert.assertFalse(g.V().hasLabel("person")
                                .has("city", Text.contains("u0000"))
                                .hasNext());
        }
    }

    @Test
    public void testEnhanceTextMatch() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("personByName").onV("person")
             .by("name").search().ifNotExist().create();

        Vertex vertex1 = graph.addVertex(T.label, "person", "name", "秦始皇",
                                         "city", "Hongkong", "age", 15);
        Vertex vertex2 = graph.addVertex(T.label, "person", "name", "始皇",
                                         "city", "Hongkong", "age", 18);
        Vertex vertex3 = graph.addVertex(T.label, "person", "name", "秦始皇2",
                                         "city", "Beijing", "age", 21);
        Vertex vertex4 = graph.addVertex(T.label, "person", "name", "秦始皇3",
                                         "city", "Beijing", "age", 23);
        Vertex vertex5 = graph.addVertex(T.label, "person", "name", "秦始皇帝",
                                         "city", "Beijing", "age", 29);
        this.commitTx();

        GraphTraversalSource g = graph.traversal();

        List<Vertex> vertices;
        vertices = g.V().has("name", Text.contains("秦始皇")).toList();
        Assert.assertEquals(5, vertices.size());
        Assert.assertTrue(vertices.contains(vertex2));

        vertices = g.V().has("name", Text.contains("(秦始皇)")).toList();
        Assert.assertEquals(4, vertices.size());
        Assert.assertTrue(vertices.contains(vertex1));
        Assert.assertTrue(vertices.contains(vertex3));
        Assert.assertTrue(vertices.contains(vertex4));
        Assert.assertTrue(vertices.contains(vertex5));

        vertices = g.V().has("name", Text.contains("(秦始皇帝)")).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(vertex5));

        vertices = g.V().has("name", Text.contains("(秦始皇2|秦始皇3)")).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(vertex3));
        Assert.assertTrue(vertices.contains(vertex4));
    }

    private void init10VerticesAndCommit() {
        this.init10Vertices();
        this.commitTx();
    }

    private void init10Vertices() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "author", "id", 1,
                        "name", "James Gosling", "age", 62,
                        "lived", "Canadian");
        graph.addVertex(T.label, "author", "id", 2,
                        "name", "Guido van Rossum", "age", 61,
                        "lived", "California");

        graph.addVertex(T.label, "language", "name", "java");
        graph.addVertex(T.label, "language", "name", "c++");
        graph.addVertex(T.label, "language", "name", "python",
                        "dynamic", true);

        graph.addVertex(T.label, "book", "name", "java-1");
        graph.addVertex(T.label, "book", "name", "java-2");
        graph.addVertex(T.label, "book", "name", "java-3");
        graph.addVertex(T.label, "book", "name", "java-4");
        graph.addVertex(T.label, "book", "name", "java-5");

        this.mayCommitTx();
    }

    private void init100Books() {
        HugeGraph graph = graph();

        for (int i = 0; i < 100; i++) {
            graph.addVertex(T.label, "book", "name", "java-" + i, "price", i);
        }

        this.commitTx();
    }

    private void init5Persons() {
        HugeGraph graph = graph();

        graph.addVertex(T.label, "person", "name", "Baby",
                        "city", "Hongkong", "age", 3,
                        "birth", Utils.date("2012-01-01"));
        graph.addVertex(T.label, "person", "name", "James",
                        "city", "Beijing", "age", 19,
                        "birth", Utils.date("2013-01-01 00:00:00.000"));
        graph.addVertex(T.label, "person", "name", "Tom Cat",
                        "city", "Beijing", "age", 20,
                        "birth", Utils.date("2014-01-01 00:00:00"));
        graph.addVertex(T.label, "person", "name", "Lisa",
                        "city", "Beijing", "age", 20,
                        "birth", Utils.date("2015-01-01 00:00:00.000"));
        graph.addVertex(T.label, "person", "name", "Hebe",
                        "city", "Taipei", "age", 21,
                        "birth", Utils.date("2016-01-01 00:00:00.000"));

        this.commitTx();
    }

    private void init100Persons() {
        HugeGraph graph = graph();

        for (int i = 0; i < 100; i++) {
            graph.addVertex(T.label, "person", "name", "person-" + i,
                            "city", i % 2 == 0 ? "Beijing" : "Hongkong",
                            "birth", i % 10 == 3 ? Utils.date("2012-01-01") :
                                     Utils.date("2018-01-01"),
                            "age", i % 11);
        }

        this.commitTx();
    }

    private void init5Computers() {
        this.initComputerIndex();

        HugeGraph graph = graph();

        graph.addVertex(T.label, "computer", "name", "YangTian T6900C",
                        "band", "lenovo", "cpu", "3.2GHz", "ram", "8GB",
                        "price", 4599);
        graph.addVertex(T.label, "computer", "name", "Fengxing K450e",
                        "band", "lenovo", "cpu", "3.2GHz", "ram", "16GB",
                        "price", 6099);
        graph.addVertex(T.label, "computer", "name", "iMac MK482CH/A",
                        "band", "apple", "cpu", "3.3GHz", "ram", "32GB",
                        "price", 15990);
        graph.addVertex(T.label, "computer", "name", "Surface Studio",
                        "band", "microsoft", "cpu", "4.6GHz", "ram", "32GB",
                        "price", 35990);
        graph.addVertex(T.label, "computer", "name", "Zen AIO Pro",
                        "band", "asus", "cpu", "3.2GHz", "ram", "16GB",
                        "price", 6999);

        this.commitTx();
    }

    private void initPageTestData() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("lang").asText().ifNotExist().create();

        schema.vertexLabel("programmer")
              .properties("name", "age", "city")
              .useCustomizeStringId()
              .nullableKeys("age")
              .ifNotExist()
              .create();

        schema.vertexLabel("software")
              .properties("name", "lang", "price")
              .useCustomizeStringId()
              .nullableKeys("price")
              .ifNotExist()
              .create();

        schema.indexLabel("programmerByNameAndAge")
              .onV("programmer")
              .by("name", "age")
              .secondary()
              .ifNotExist()
              .create();

        schema.indexLabel("programmerByAge")
              .onV("programmer")
              .range()
              .by("age")
              .ifNotExist()
              .create();

        schema.indexLabel("programmerByCity")
              .onV("programmer")
              .search()
              .by("city")
              .ifNotExist()
              .create();

        schema.indexLabel("softwareByName")
              .onV("software")
              .secondary()
              .by("name")
              .ifNotExist()
              .create();

        schema.indexLabel("softwareByLang")
              .onV("software")
              .secondary()
              .by("lang")
              .ifNotExist()
              .create();

        schema.indexLabel("softwareByPrice")
              .onV("software")
              .by("price")
              .range()
              .ifNotExist()
              .create();

        String[] cities = {"Beijing Haidian", "Beijing Chaoyang", "Shanghai"};
        for (int i = 1; i <= 18; i++) {
            String id = "p_marko" + i;
            /*
             * The city of each programmer is:
             * [1, 6]: Beijing Haidian, [7, 12]: Beijing Chaoyang,
             * [13, 18]: Shanghai
             */
            String city = cities[(i - 1) / 6];
            graph().addVertex(T.label, "programmer", T.id, id, "name", "marko",
                              "age", 30, "city", city);
        }

        for (int i = 1; i <= 16; i++) {
            String id = "s_marko" + i;
            /*
             * The price of each software is:
             * [1, 4]: 100, [5, 8]: 200, [9, 12]: 300, [13, 16]: 400
             */
            int price = ((i - 1) / 4 + 1) * 100;
            graph().addVertex(T.label, "software", T.id, id, "name", "marko",
                              "lang", "java", "price", price);
        }
        this.commitTx();
    }

    private Vertex vertex(String label, String pkName, Object pkValue) {
        List<Vertex> vertices = graph().traversal().V()
                                       .hasLabel(label).has(pkName, pkValue)
                                       .toList();
        Assert.assertTrue(vertices.size() <= 1);
        return vertices.size() == 1 ? vertices.get(0) : null;
    }

    private static void assertContains(List<Vertex> vertices,
                                       Object... keyValues) {
        Assert.assertTrue(Utils.contains(vertices, new FakeObjects.FakeVertex(keyValues)));
    }

    private static void assertNotContains(List<Vertex> vertices,
                                          Object... keyValues) {
        Assert.assertFalse(Utils.contains(vertices, new FakeObjects.FakeVertex(keyValues)));
    }
}
