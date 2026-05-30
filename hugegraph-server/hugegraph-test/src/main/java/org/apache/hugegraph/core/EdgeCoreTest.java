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

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.backend.store.BackendTable;
import org.apache.hugegraph.backend.store.Shard;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.exception.LimitExceedException;
import org.apache.hugegraph.exception.NoIndexException;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.FakeObjects;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.traversal.optimize.ConditionP;
import org.apache.hugegraph.traversal.optimize.Text;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.util.collection.CollectionFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class EdgeCoreTest extends BaseCoreTest {

    @Before
    public void initSchema() {
        SchemaManager schema = graph().schema();

        LOG.debug("===============  propertyKey  ================");

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("dynamic").asBoolean().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("timestamp").asLong().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("comment").asText().valueSet().create();
        schema.propertyKey("contribution").asText().create();
        schema.propertyKey("score").asInt().create();
        schema.propertyKey("lived").asText().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("amount").asFloat().create();
        schema.propertyKey("message").asText().create();
        schema.propertyKey("place").asText().create();
        schema.propertyKey("tool").asText().create();
        schema.propertyKey("reason").asText().create();
        schema.propertyKey("hurt").asBoolean().create();
        schema.propertyKey("arrested").asBoolean().create();
        schema.propertyKey("date").asDate().create();

        LOG.debug("===============  vertexLabel  ================");

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .enableLabelIndex(false)
              .create();
        schema.vertexLabel("author")
              .properties("id", "name", "age", "lived")
              .primaryKeys("id")
              .enableLabelIndex(false)
              .create();
        schema.vertexLabel("language")
              .properties("name", "dynamic")
              .primaryKeys("name")
              .nullableKeys("dynamic")
              .enableLabelIndex(false)
              .create();
        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .enableLabelIndex(false)
              .create();

        LOG.debug("===============  edgeLabel  ================");

        schema.edgeLabel("transfer")
              .properties("id", "amount", "timestamp", "message")
              .nullableKeys("message")
              .multiTimes().sortKeys("id")
              .link("person", "person")
              .enableLabelIndex(false)
              .create();
        schema.edgeLabel("authored").singleTime()
              .properties("contribution", "comment", "score")
              .nullableKeys("score", "contribution", "comment")
              .link("author", "book")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("write").properties("time")
              .multiTimes().sortKeys("time")
              .link("author", "book")
              .enableLabelIndex(false)
              .create();
        schema.edgeLabel("look").properties("time", "score")
              .nullableKeys("score")
              .multiTimes().sortKeys("time")
              .link("person", "book")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("know").singleTime()
              .link("author", "author")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("followedBy").singleTime()
              .link("author", "person")
              .enableLabelIndex(false)
              .create();
        schema.edgeLabel("friend").singleTime()
              .link("person", "person")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("follow").singleTime()
              .link("person", "author")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("created").singleTime()
              .link("author", "language")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("strike").link("person", "person")
              .properties("id", "timestamp", "place", "tool", "reason",
                          "hurt", "arrested")
              .multiTimes().sortKeys("id")
              .nullableKeys("tool", "reason", "hurt")
              .enableLabelIndex(false)
              .ifNotExist().create();
        schema.edgeLabel("read").link("person", "book")
              .properties("place", "date")
              .ttl(3000L)
              .enableLabelIndex(true)
              .ifNotExist()
              .create();
        schema.edgeLabel("borrow").link("person", "book")
              .properties("place", "date")
              .ttl(3000L)
              .ttlStartTime("date")
              .enableLabelIndex(true)
              .ifNotExist()
              .create();
    }

    protected void initStrikeIndex() {
        SchemaManager schema = graph().schema();

        LOG.debug("===============  strike index  ================");

        schema.indexLabel("strikeByTimestamp").onE("strike").range()
              .by("timestamp").create();
        schema.indexLabel("strikeByTool").onE("strike").secondary()
              .by("tool").create();
        schema.indexLabel("strikeByPlaceToolReason").onE("strike").secondary()
              .by("place", "tool", "reason").create();
    }

    @Test
    public void testAddEdge() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido = graph.addVertex(T.label, "author", "id", 2,
                                       "name", "Guido van Rossum", "age", 61,
                                       "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                                        "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3);

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(5, edges.size());
        assertContains(edges, "created", james, java);
        assertContains(edges, "created", guido, python);
        assertContains(edges, "authored", james, java1);
        assertContains(edges, "authored", james, java2);
        assertContains(edges, "authored", james, java3);
    }

    @Test
    public void testAddEdgeWithOverrideEdge() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido = graph.addVertex(T.label, "author", "id", 2,
                                       "name", "Guido van Rossum", "age", 61,
                                       "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                                        "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        // Group 1
        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("created", java);
        guido.addEdge("created", python);

        // Group 2
        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3, "score", 4);

        james.addEdge("authored", java1);
        james.addEdge("authored", java3, "score", 5);

        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(5, edges.size());
        assertContains(edges, "created", james, java);
        assertContains(edges, "created", guido, python);
        assertContains(edges, "authored", james, java1);
        assertContains(edges, "authored", james, java2);
        assertContains(edges, "authored", james, java3, "score", 5);
    }

    @Test
    public void testAddEdgeWithProp() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("write", book, "time", "2017-4-28");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "write", james, book,
                       "time", "2017-4-28");
    }

    @Test
    public void testAddEdgeWithProps() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("authored", book,
                      "contribution", "1990-1-1", "score", 5);

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "authored", james, book,
                       "contribution", "1990-1-1", "score", 5);
    }

    @Test
    public void testAddEdgeWithPropSet() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("authored", book,
                      "comment", "good book!",
                      "comment", "good book too!");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "authored", james, book);
        Edge edge = edges.get(0);
        Object comments = edge.property("comment").value();
        Assert.assertEquals(ImmutableSet.of("good book!", "good book too!"),
                            comments);
    }

    @Test
    public void testAddEdgeWithPropSetAndOverridProp() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("authored", book,
                      "comment", "good book!",
                      "comment", "good book!",
                      "comment", "good book too!");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "authored", james, book);
        Edge edge = edges.get(0);
        Object comments = edge.property("comment").value();
        Assert.assertEquals(ImmutableSet.of("good book!", "good book too!"),
                            comments);
    }

    @Test
    public void testAddEdgeToSameVerticesWithMultiTimes() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("write", book, "time", "2017-4-28");
        james.addEdge("write", book, "time", "2017-5-21");
        james.addEdge("write", book, "time", "2017-5-25");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(3, edges.size());
        assertContains(edges, "write", james, book,
                       "time", "2017-4-28");
        assertContains(edges, "write", james, book,
                       "time", "2017-5-21");
        assertContains(edges, "write", james, book,
                       "time", "2017-5-25");
    }

    @Test
    public void testAddEdgeToSameVerticesWithMultiTimesAndOverrideEdge() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("write", book, "time", "2017-4-28");
        james.addEdge("write", book, "time", "2017-5-21");
        james.addEdge("write", book, "time", "2017-5-25");

        james.addEdge("write", book, "time", "2017-4-28");
        james.addEdge("write", book, "time", "2017-5-21");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(3, edges.size());
        assertContains(edges, "write", james, book,
                       "time", "2017-4-28");
        assertContains(edges, "write", james, book,
                       "time", "2017-5-21");
        assertContains(edges, "write", james, book,
                       "time", "2017-5-25");
    }

    @Test
    public void testAddEdgeWithNotExistsEdgeLabel() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("label-not-exists", book, "time", "2017-4-28");
        });
    }

    @Test
    public void testAddEdgeWithoutSortkey() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("write", book);
        });
    }

    @Test
    public void testAddEdgeWithLargeSortkey() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            final int LEN = BytesBuffer.EID_LEN_MAX;
            String largeTime = "{large-time}" + new String(new byte[LEN]);
            james.addEdge("write", book, "time", largeTime);
            graph.tx().commit();
        }, e -> {
            Assert.assertContains(String.format("The max length of edge id is %s",
                                                BytesBuffer.EID_LEN_MAX),
                                  e.getMessage());
        });
    }

    @Test
    public void testAddEdgeWithInvalidSortkey() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");
        graph.tx().commit();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("write", book, "time", "\u00002017-5-27");
            graph.tx().commit();
        }, e -> {
            Assert.assertContains("Illegal leading char '\\u0' in index",
                                  e.getMessage());
        });

        String backend = graph.backend();
        if (backend.equals("postgresql")) {
            Assert.assertThrows(BackendException.class, () -> {
                james.addEdge("write", book, "time", "2017-5-27\u0000");
                graph.tx().commit();
            }, e -> {
                // pgsql need to clear and reset state (like auto-commit)
                graph.tx().rollback();
                Assert.assertContains("invalid byte sequence for encoding " +
                                      "\"UTF8\": 0x00",
                                      e.getCause().getMessage());
            });

            Assert.assertThrows(BackendException.class, () -> {
                graph.traversal().V(james.id())
                     .outE("write").has("time", "2017-5-27\u0000")
                     .toList();
            }, e -> {
                Assert.assertContains("Zero bytes may not occur in string " +
                                      "parameters", e.getCause().getMessage());
            });
        } else if (ImmutableSet.of("rocksdb", "hbase", "hstore").contains(backend)) {
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                james.addEdge("write", book, "time", "2017-5-27\u0000");
                graph.tx().commit();
            }, e -> {
                Assert.assertContains("Can't contains byte '0x00' in string",
                                      e.getMessage());
            });

            Assert.assertThrows(IllegalArgumentException.class, () -> {
                graph.traversal().V(james.id())
                     .outE("write").has("time", "2017-5-27\u0000")
                     .toList();
            }, e -> {
                Assert.assertContains("Can't contains byte '0x00' in string",
                                      e.getMessage());
            });
        } else {
            james.addEdge("write", book, "time", "2017-5-27\u0000");
            graph.tx().commit();

            List<Edge> edges = graph.traversal().V(james.id())
                                    .outE("write")
                                    .has("time", "2017-5-27\u0000")
                                    .toList();
            Assert.assertEquals(1, edges.size());
            Assert.assertEquals("2017-5-27\u0000", edges.get(0).value("time"));
        }
    }

    @Test
    public void testAddEdgeWithNotExistsPropKey() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "propkey-not-exists", "value");
        });
    }

    @Test
    public void testAddEdgeWithNotExistsEdgePropKey() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "age", 18);
        });
    }

    @Test
    public void testAddEdgeWithNullableKeysAbsent() {
        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "Java in action");

        Edge edge = baby.addEdge("look", java, "time", "2017-09-09");
        Assert.assertEquals("2017-09-09", edge.value("time"));
    }

    @Test
    public void testAddEdgeWithNonNullKeysAbsent() {
        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "Java in action");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Absent 'time'
            baby.addEdge("look", java, "score", 99);
        });
    }

    @Test
    public void testAddEdgeWithInvalidPropValueType() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("authored", book, "score", 5);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "score", 5.1);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "score", "five");
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "score", "5");
        });
    }

    @Test
    public void testAddEdgeWithDefaultPropertyValue() {
        SchemaManager schema = graph().schema();

        schema.propertyKey("fav").asText()
              .userdata(Userdata.DEFAULT_VALUE, "Movie").create();
        schema.propertyKey("cnt").asInt()
              .userdata(Userdata.DEFAULT_VALUE, 123).create();
        schema.edgeLabel("authored")
              .properties("fav", "cnt")
              .nullableKeys("fav", "cnt").append();

        Vertex marko = graph().addVertex(T.label, "author", "id", 1,
                                         "name", "marko", "age", 28,
                                         "lived", "Beijing");
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "Java in action");

        Edge edge = marko.addEdge("authored", java,
                                  "contribution", "2010-01-01",
                                  "score", 99);

        // No 'fav'
        Assert.assertEquals("2010-01-01", edge.value("contribution"));
        Assert.assertFalse(edge.values("fav").hasNext());
        Assert.assertFalse(edge.values("age").hasNext());

        edge = graph().edge(edge.id());
        Assert.assertEquals("2010-01-01", edge.value("contribution"));
        Assert.assertFalse(edge.values("fav").hasNext());
        Assert.assertFalse(edge.values("age").hasNext());

        graph().tx().commit();

        // Exist 'fav' after commit then query
        edge = graph().edge(edge.id());
        Assert.assertEquals("2010-01-01", edge.value("contribution"));
        Assert.assertEquals("Movie", edge.value("fav"));
        Assert.assertEquals(123, edge.value("cnt"));
    }

    @Test
    public void testOverrideEdge() {
        HugeGraph graph = graph();
        Vertex marko = graph().addVertex(T.label, "author", "id", 1,
                                         "name", "marko", "age", 28,
                                         "lived", "Beijing");
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "Java in action");

        Object edgeId = marko.addEdge("authored", java,
                                      "contribution", "2010-01-01",
                                      "score", 99).id();
        graph.tx().commit();
        Edge edge = graph.edges(edgeId).next();
        Assert.assertTrue(edge.property("contribution").isPresent());
        Assert.assertEquals("2010-01-01", edge.value("contribution"));
        Assert.assertTrue(edge.property("score").isPresent());
        Assert.assertEquals(99, edge.value("score"));

        marko.addEdge("authored", java, "score", 100).id();
        graph.tx().commit();
        edge = graph.edges(edgeId).next();
        Assert.assertFalse(edge.property("contribution").isPresent());
        Assert.assertTrue(edge.property("score").isPresent());
        Assert.assertEquals(100, edge.value("score"));

        marko.addEdge("authored", java, "contribution", "2011-01-01",
                      "score", 101).id();
        graph.tx().commit();
        edge = graph.edges(edgeId).next();
        Assert.assertTrue(edge.property("contribution").isPresent());
        Assert.assertEquals("2011-01-01", edge.value("contribution"));
        Assert.assertTrue(edge.property("score").isPresent());
        Assert.assertEquals(101, edge.value("score"));
    }

    @Test
    public void testAddEdgeWithTtl() {
        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "Java in action");
        Edge edge = baby.addEdge("read", java, "place", "library of school",
                                 "date", "2019-12-23 12:00:00");
        graph().tx().commit();

        Iterator<Edge> edges = graph().edges(edge);
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge, edges.next());
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }
        edges = graph().edges(edge);
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testAddEdgeWithTtlAndTtlStartTime() {
        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "Java in action");
        Edge edge = baby.addEdge("borrow", java, "place", "library of school",
                                 "date", graph().now() - 2000L);
        graph().tx().commit();

        Iterator<Edge> edges = graph().edges(edge);
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge, edges.next());
        graph().tx().commit();

        try {
            Thread.sleep(500L);
        } catch (InterruptedException e) {
            // Ignore
        }

        edges = graph().edges(edge);
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge, edges.next());
        graph().tx().commit();

        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            // Ignore
        }
        edges = graph().edges(edge);
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testAddEdgeWithSecondaryIndexAndTtl() {
        graph().schema().indexLabel("readByPlace").onE("read").by("place")
               .secondary().ifNotExist().create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "Java in action");
        Edge edge = baby.addEdge("read", java, "place", "library of school",
                                 "date", "2019-12-23 12:00:00");
        graph().tx().commit();

        Iterator<Edge> edges = graph().traversal().E()
                                      .has("place", "library of school");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge, edges.next());
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        edges = graph().traversal().E().has("place", "library of school");
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testAddEdgeWithRangeIndexAndTtl() {
        graph().schema().indexLabel("readByDate").onE("read").by("date")
               .range().ifNotExist().create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "Java1");
        Vertex java2 = graph().addVertex(T.label, "book", "name", "Java2");
        Vertex java3 = graph().addVertex(T.label, "book", "name", "Java3");
        Vertex java4 = graph().addVertex(T.label, "book", "name", "Java4");
        Vertex java5 = graph().addVertex(T.label, "book", "name", "Java5");
        Edge edge1 = baby.addEdge("read", java1, "place", "library of school",
                                  "date", "2019-12-23 12:00:00");
        @SuppressWarnings("unused")
        Edge edge2 = baby.addEdge("read", java2, "place", "library of school",
                                  "date", "2019-12-23 13:00:00");
        Edge edge3 = baby.addEdge("read", java3, "place", "library of school",
                                  "date", "2019-12-23 14:00:00");
        @SuppressWarnings("unused")
        Edge edge4 = baby.addEdge("read", java4, "place", "library of school",
                                  "date", "2019-12-23 15:00:00");
        Edge edge5 = baby.addEdge("read", java5, "place", "library of school",
                                  "date", "2019-12-23 16:00:00");
        graph().tx().commit();

        Iterator<Edge> edges = graph().traversal().E()
                                      .has("date", "2019-12-23 14:00:00");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge3, edges.next());

        edges = graph().traversal().E()
                       .has("date", P.gt("2019-12-23 15:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge5, edges.next());

        edges = graph().traversal().E()
                       .has("date", P.gte("2019-12-23 15:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));

        edges = graph().traversal().E()
                       .has("date", P.lt("2019-12-23 13:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge1, edges.next());

        edges = graph().traversal().E()
                       .has("date", P.lte("2019-12-23 13:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        edges = graph().traversal().E().has("date", "2019-12-23 14:00:00");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E()
                       .has("date", P.gt("2019-12-23 15:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E()
                       .has("date", P.gte("2019-12-23 15:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E()
                       .has("date", P.lt("2019-12-23 13:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E()
                       .has("date", P.lte("2019-12-23 13:00:00"));
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testAddEdgeWithShardIndexAndTtl() {
        graph().schema().indexLabel("readByPlaceAndDate").onE("read")
               .by("place", "date").shard().ifNotExist().create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "Java1");
        Vertex java2 = graph().addVertex(T.label, "book", "name", "Java2");
        Vertex java3 = graph().addVertex(T.label, "book", "name", "Java3");
        Vertex java4 = graph().addVertex(T.label, "book", "name", "Java4");
        Vertex java5 = graph().addVertex(T.label, "book", "name", "Java5");
        Vertex java6 = graph().addVertex(T.label, "book", "name", "Java6");
        Edge edge1 = baby.addEdge("read", java1, "place", "library of school",
                                  "date", "2019-12-23 12:00:00");
        @SuppressWarnings("unused")
        Edge edge2 = baby.addEdge("read", java2, "place", "library of school",
                                  "date", "2019-12-23 13:00:00");
        Edge edge3 = baby.addEdge("read", java3, "place", "library of school",
                                  "date", "2019-12-23 14:00:00");
        @SuppressWarnings("unused")
        Edge edge4 = baby.addEdge("read", java4, "place", "library of school",
                                  "date", "2019-12-23 15:00:00");
        Edge edge5 = baby.addEdge("read", java5, "place", "library of school",
                                  "date", "2019-12-23 16:00:00");
        Edge edge6 = baby.addEdge("read", java6, "place", "home",
                                  "date", "2019-12-23 14:00:00");
        graph().tx().commit();

        Iterator<Edge> edges = graph().traversal().E().has("place", "home");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge6, edges.next());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", "2019-12-23 14:00:00");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge3, edges.next());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.gt("2019-12-23 15:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge5, edges.next());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.gte("2019-12-23 15:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.lt("2019-12-23 13:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge1, edges.next());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.lte("2019-12-23 13:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        edges = graph().traversal().E().has("place", "home");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", "2019-12-23 14:00:00");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.gt("2019-12-23 15:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.gte("2019-12-23 15:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.lt("2019-12-23 13:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.lte("2019-12-23 13:00:00"));
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testAddEdgeWithSearchIndexAndTtl() {
        graph().schema().indexLabel("readByPlace").onE("read")
               .by("place").search().ifNotExist().create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "Java in action");
        Edge edge = baby.addEdge("read", java, "place", "library of school",
                                 "date", "2019-12-23 12:00:00");
        graph().tx().commit();

        Iterator<Edge> edges = graph().traversal().E()
                                      .has("place", Text.contains("library"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge, edges.next());
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        edges = graph().traversal().E().has("place", Text.contains("library"));
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testAddEdgeWithUniqueIndexAndTtl() {
        graph().schema().indexLabel("readByPlace").onE("read")
               .by("place").unique().ifNotExist().create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "Java in action");
        baby.addEdge("read", java, "place", "library of school",
                     "date", "2019-12-23 12:00:00");
        graph().tx().commit();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            baby.addEdge("read", java, "place", "library of school",
                         "date", "2019-12-23 12:00:00");
            graph().tx().commit();
        });

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        baby.addEdge("read", java, "place", "library of school",
                     "date", "2019-12-23 12:00:00");
        graph().tx().commit();
    }

    @Test
    public void testOverrideEdgeWithSecondaryIndexAndTtl() {
        graph().schema().indexLabel("readByPlace").onE("read").by("place")
               .secondary().ifNotExist().create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "Java in action");
        Edge edge = baby.addEdge("read", java, "place", "library of school",
                                 "date", "2019-12-23 12:00:00");
        graph().tx().commit();

        Iterator<Edge> edges = graph().traversal().E()
                                      .has("place", "library of school");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge, edges.next());
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // Override
        edge = baby.addEdge("read", java, "place", "home",
                            "date", "2019-12-23 12:00:00");
        graph().tx().commit();

        // Due to overridden edges are expired, query will lead to async delete
        edges = graph().traversal().E().has("place", "library of school");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "home");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge, edges.next());
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All edges are expired after 6s
        edges = graph().traversal().E().has("place", "library of school");
        Assert.assertFalse(edges.hasNext());
        edges = graph().traversal().E().has("place", "home");
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testOverrideEdgeWithRangeIndexAndTtl() {
        graph().schema().indexLabel("readByDate").onE("read").by("date")
               .range().ifNotExist().create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "Java1");
        Vertex java2 = graph().addVertex(T.label, "book", "name", "Java2");
        Vertex java3 = graph().addVertex(T.label, "book", "name", "Java3");
        Vertex java4 = graph().addVertex(T.label, "book", "name", "Java4");
        Vertex java5 = graph().addVertex(T.label, "book", "name", "Java5");
        Edge edge1 = baby.addEdge("read", java1, "place", "library of school",
                                  "date", "2019-12-23 12:00:00");
        @SuppressWarnings("unused")
        Edge edge2 = baby.addEdge("read", java2, "place", "library of school",
                                  "date", "2019-12-23 13:00:00");
        Edge edge3 = baby.addEdge("read", java3, "place", "library of school",
                                  "date", "2019-12-23 14:00:00");
        @SuppressWarnings("unused")
        Edge edge4 = baby.addEdge("read", java4, "place", "library of school",
                                  "date", "2019-12-23 15:00:00");
        Edge edge5 = baby.addEdge("read", java5, "place", "library of school",
                                  "date", "2019-12-23 16:00:00");
        graph().tx().commit();

        Iterator<Edge> edges = graph().traversal().E()
                                      .has("date", "2019-12-23 14:00:00");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge3, edges.next());

        edges = graph().traversal().E()
                       .has("date", P.gt("2019-12-23 15:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge5, edges.next());

        edges = graph().traversal().E()
                       .has("date", P.gte("2019-12-23 15:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));

        edges = graph().traversal().E()
                       .has("date", P.lt("2019-12-23 13:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge1, edges.next());

        edges = graph().traversal().E()
                       .has("date", P.lte("2019-12-23 13:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // Override
        Edge edge6 = baby.addEdge("read", java1, "place", "library of school",
                                  "date", "2019-12-23 12:01:00");
        @SuppressWarnings("unused")
        Edge edge7 = baby.addEdge("read", java2, "place", "library of school",
                                  "date", "2019-12-23 13:01:00");
        Edge edge8 = baby.addEdge("read", java3, "place", "library of school",
                                  "date", "2019-12-23 14:01:00");
        @SuppressWarnings("unused")
        Edge edge9 = baby.addEdge("read", java4, "place", "library of school",
                                  "date", "2019-12-23 15:01:00");
        Edge edge10 = baby.addEdge("read", java5,
                                   "place", "library of school",
                                   "date", "2019-12-23 16:01:00");
        graph().tx().commit();
        // Due to overridden edges are expired, query will lead to async delete
        edges = graph().traversal().E().has("date", "2019-12-23 14:00:00");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("date", "2019-12-23 14:01:00");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge8, edges.next());

        edges = graph().traversal().E()
                       .has("date", P.gt("2019-12-23 15:01:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge10, edges.next());

        edges = graph().traversal().E()
                       .has("date", P.gte("2019-12-23 15:01:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));

        edges = graph().traversal().E()
                       .has("date", P.lt("2019-12-23 13:01:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge6, edges.next());

        edges = graph().traversal().E()
                       .has("date", P.lte("2019-12-23 13:01:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All edges are expired after 6s
        edges = graph().traversal().E().has("date", "2019-12-23 14:00:00");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("date", "2019-12-23 14:01:00");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E()
                       .has("date", P.gt("2019-12-23 15:01:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E()
                       .has("date", P.gte("2019-12-23 15:01:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E()
                       .has("date", P.lt("2019-12-23 13:01:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E()
                       .has("date", P.lte("2019-12-23 13:01:00"));
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testOverrideEdgeWithShardIndexAndTtl() {
        graph().schema().indexLabel("readByPlaceAndDate").onE("read")
               .by("place", "date").shard().ifNotExist().create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "Java1");
        Vertex java2 = graph().addVertex(T.label, "book", "name", "Java2");
        Vertex java3 = graph().addVertex(T.label, "book", "name", "Java3");
        Vertex java4 = graph().addVertex(T.label, "book", "name", "Java4");
        Vertex java5 = graph().addVertex(T.label, "book", "name", "Java5");
        Vertex java6 = graph().addVertex(T.label, "book", "name", "Java6");
        Edge edge1 = baby.addEdge("read", java1, "place", "library of school",
                                  "date", "2019-12-23 12:00:00");
        @SuppressWarnings("unused")
        Edge edge2 = baby.addEdge("read", java2, "place", "library of school",
                                  "date", "2019-12-23 13:00:00");
        Edge edge3 = baby.addEdge("read", java3, "place", "library of school",
                                  "date", "2019-12-23 14:00:00");
        @SuppressWarnings("unused")
        Edge edge4 = baby.addEdge("read", java4, "place", "library of school",
                                  "date", "2019-12-23 15:00:00");
        Edge edge5 = baby.addEdge("read", java5, "place", "library of school",
                                  "date", "2019-12-23 16:00:00");
        Edge edge6 = baby.addEdge("read", java6, "place", "home",
                                  "date", "2019-12-23 14:00:00");
        graph().tx().commit();

        Iterator<Edge> edges = graph().traversal().E().has("place", "home");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge6, edges.next());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", "2019-12-23 14:00:00");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge3, edges.next());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.gt("2019-12-23 15:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge5, edges.next());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.gte("2019-12-23 15:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.lt("2019-12-23 13:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge1, edges.next());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.lte("2019-12-23 13:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        Edge edge7 = baby.addEdge("read", java1, "place", "library of school",
                                  "date", "2019-12-23 12:01:00");
        @SuppressWarnings("unused")
        Edge edge8 = baby.addEdge("read", java2, "place", "library of school",
                                  "date", "2019-12-23 13:01:00");
        Edge edge9 = baby.addEdge("read", java3, "place", "library of school",
                                  "date", "2019-12-23 14:01:00");
        @SuppressWarnings("unused")
        Edge edge10 = baby.addEdge("read", java4,
                                   "place", "library of school",
                                   "date", "2019-12-23 15:01:00");
        Edge edge11 = baby.addEdge("read", java5,
                                   "place", "library of school",
                                   "date", "2019-12-23 16:01:00");
        Edge edge12 = baby.addEdge("read", java6, "place", "library",
                                   "date", "2019-12-23 14:01:00");
        graph().tx().commit();

        // Due to overridden edges are expired, query will lead to async delete
        edges = graph().traversal().E().has("place", "home");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge12, edges.next());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", "2019-12-23 14:00:00");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", "2019-12-23 14:01:00");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge9, edges.next());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.gt("2019-12-23 15:01:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge11, edges.next());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.gte("2019-12-23 15:01:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.lt("2019-12-23 13:01:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge7, edges.next());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.lte("2019-12-23 13:01:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All edges are expired after 6s
        edges = graph().traversal().E().has("place", "home");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", "2019-12-23 14:00:00");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", "2019-12-23 14:01:00");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.gt("2019-12-23 15:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.gte("2019-12-23 15:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.lt("2019-12-23 13:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.lte("2019-12-23 13:00:00"));
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testOverrideEdgeWithSearchIndexAndTtl() {
        graph().schema().indexLabel("readByPlace").onE("read")
               .by("place").search().ifNotExist().create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "Java in action");
        Edge edge = baby.addEdge("read", java, "place", "library of school",
                                 "date", "2019-12-23 12:00:00");
        graph().tx().commit();

        Iterator<Edge> edges = graph().traversal().E()
                                      .has("place", Text.contains("school"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge, edges.next());
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        edge = baby.addEdge("read", java, "place", "library of city",
                            "date", "2019-12-23 12:00:00");
        graph().tx().commit();

        // Due to overridden edges are expired, query will lead to async delete
        edges = graph().traversal().E().has("place", Text.contains("school"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", Text.contains("city"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge, edges.next());
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All edges are expired after 6s
        edges = graph().traversal().E().has("place", Text.contains("library"));
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testQueryEdgeWithTtlInTx() {
        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java1 = graph().addVertex(T.label, "book",
                                         "name", "Java1 in action");
        Vertex java2 = graph().addVertex(T.label, "book",
                                         "name", "Java2 in action");
        Edge edge1 = baby.addEdge("read", java1, "place", "library of school",
                                  "date", "2019-12-23 12:00:00");
        graph().tx().commit();
        // Add edges in tx
        Edge edge2 = baby.addEdge("read", java2, "place", "library of school",
                                  "date", "2019-12-23 12:00:00");

        Iterator<Edge> edges = graph().edges(edge1);
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge1, edges.next());

        edges = graph().edges(edge2);
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(edge2, edges.next());
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All edges are expired after 3s
        edges = graph().edges(edge1);
        Assert.assertFalse(edges.hasNext());

        edges = graph().edges(edge2);
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testQueryEdgeWithSecondaryIndexAndTtlInTx() {
        graph().schema().indexLabel("readByPlace").onE("read").by("place")
               .secondary().ifNotExist().create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java1 = graph().addVertex(T.label, "book",
                                         "name", "Java1 in action");
        Vertex java2 = graph().addVertex(T.label, "book",
                                         "name", "Java2 in action");
        baby.addEdge("read", java1, "place", "library of school",
                     "date", "2019-12-23 12:00:00");
        graph().tx().commit();
        // Add edges in tx
        baby.addEdge("read", java2, "place", "library of school",
                     "date", "2019-12-23 12:00:00");

        Iterator<Edge> edges = graph().traversal().E()
                                      .has("place", "library of school");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All edges are expired after 3s
        edges = graph().traversal().E().has("place", "library of school");
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testQueryEdgeWithRangeIndexAndTtlInTx() {
        graph().schema().indexLabel("readByDate").onE("read").by("date")
               .range().ifNotExist().create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "Java1");
        Vertex java2 = graph().addVertex(T.label, "book", "name", "Java2");
        Vertex java3 = graph().addVertex(T.label, "book", "name", "Java3");
        Vertex java4 = graph().addVertex(T.label, "book", "name", "Java4");
        Vertex java5 = graph().addVertex(T.label, "book", "name", "Java5");
        Vertex java6 = graph().addVertex(T.label, "book", "name", "Java6");
        Vertex java7 = graph().addVertex(T.label, "book", "name", "Java7");
        Vertex java8 = graph().addVertex(T.label, "book", "name", "Java8");
        Vertex java9 = graph().addVertex(T.label, "book", "name", "Java9");
        Vertex java10 = graph().addVertex(T.label, "book", "name", "Java10");
        baby.addEdge("read", java1, "place", "library of school",
                     "date", "2019-12-23 12:00:00");
        baby.addEdge("read", java2, "place", "library of school",
                     "date", "2019-12-23 13:00:00");
        baby.addEdge("read", java3, "place", "library of school",
                     "date", "2019-12-23 14:00:00");
        baby.addEdge("read", java4, "place", "library of school",
                     "date", "2019-12-23 15:00:00");
        baby.addEdge("read", java5, "place", "library of school",
                     "date", "2019-12-23 16:00:00");
        graph().tx().commit();

        // Add edges in tx
        baby.addEdge("read", java6, "place", "library of school",
                     "date", "2019-12-23 12:00:00");
        baby.addEdge("read", java7, "place", "library of school",
                     "date", "2019-12-23 13:00:00");
        baby.addEdge("read", java8, "place", "library of school",
                     "date", "2019-12-23 14:00:00");
        baby.addEdge("read", java9, "place", "library of school",
                     "date", "2019-12-23 15:00:00");
        baby.addEdge("read", java10, "place", "library of school",
                     "date", "2019-12-23 16:00:00");

        Iterator<Edge> edges = graph().traversal().E()
                                      .has("date", "2019-12-23 14:00:00");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));

        edges = graph().traversal().E()
                       .has("date", P.gt("2019-12-23 15:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));

        edges = graph().traversal().E()
                       .has("date", P.gte("2019-12-23 15:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(4, IteratorUtils.count(edges));

        edges = graph().traversal().E()
                       .has("date", P.lt("2019-12-23 13:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));

        edges = graph().traversal().E()
                       .has("date", P.lte("2019-12-23 13:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(4, IteratorUtils.count(edges));
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All edges are expired after 3s
        edges = graph().traversal().E().has("date", "2019-12-23 14:00:00");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E()
                       .has("date", P.gt("2019-12-23 15:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E()
                       .has("date", P.gte("2019-12-23 15:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E()
                       .has("date", P.lt("2019-12-23 13:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E()
                       .has("date", P.lte("2019-12-23 13:00:00"));
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testQueryEdgeWithShardIndexAndTtlInTx() {
        graph().schema().indexLabel("readByPlaceAndDate").onE("read")
               .by("place", "date").shard().ifNotExist().create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "Java1");
        Vertex java2 = graph().addVertex(T.label, "book", "name", "Java2");
        Vertex java3 = graph().addVertex(T.label, "book", "name", "Java3");
        Vertex java4 = graph().addVertex(T.label, "book", "name", "Java4");
        Vertex java5 = graph().addVertex(T.label, "book", "name", "Java5");
        Vertex java6 = graph().addVertex(T.label, "book", "name", "Java6");
        Vertex java7 = graph().addVertex(T.label, "book", "name", "Java7");
        Vertex java8 = graph().addVertex(T.label, "book", "name", "Java8");
        Vertex java9 = graph().addVertex(T.label, "book", "name", "Java9");
        Vertex java10 = graph().addVertex(T.label, "book", "name", "Java10");
        Vertex java11 = graph().addVertex(T.label, "book", "name", "Java11");
        Vertex java12 = graph().addVertex(T.label, "book", "name", "Java12");
        baby.addEdge("read", java1, "place", "library of school",
                     "date", "2019-12-23 12:00:00");
        baby.addEdge("read", java2, "place", "library of school",
                     "date", "2019-12-23 13:00:00");
        baby.addEdge("read", java3, "place", "library of school",
                     "date", "2019-12-23 14:00:00");
        baby.addEdge("read", java4, "place", "library of school",
                     "date", "2019-12-23 15:00:00");
        baby.addEdge("read", java5, "place", "library of school",
                     "date", "2019-12-23 16:00:00");
        baby.addEdge("read", java6, "place", "home",
                     "date", "2019-12-23 14:00:00");
        graph().tx().commit();

        // Add edges in tx
        baby.addEdge("read", java7, "place", "library of school",
                     "date", "2019-12-23 12:00:00");
        baby.addEdge("read", java8, "place", "library of school",
                     "date", "2019-12-23 13:00:00");
        baby.addEdge("read", java9, "place", "library of school",
                     "date", "2019-12-23 14:00:00");
        baby.addEdge("read", java10, "place", "library of school",
                     "date", "2019-12-23 15:00:00");
        baby.addEdge("read", java11, "place", "library of school",
                     "date", "2019-12-23 16:00:00");
        baby.addEdge("read", java12, "place", "home",
                     "date", "2019-12-23 14:00:00");

        Iterator<Edge> edges = graph().traversal().E().has("place", "home");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", "2019-12-23 14:00:00");
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.gt("2019-12-23 15:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.gte("2019-12-23 15:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(4, IteratorUtils.count(edges));

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.lt("2019-12-23 13:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.lte("2019-12-23 13:00:00"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(4, IteratorUtils.count(edges));
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All edges are expired after 3s
        edges = graph().traversal().E().has("place", "home");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", "2019-12-23 14:00:00");
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.gt("2019-12-23 15:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.gte("2019-12-23 15:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.lt("2019-12-23 13:00:00"));
        Assert.assertFalse(edges.hasNext());

        edges = graph().traversal().E().has("place", "library of school")
                       .has("date", P.lte("2019-12-23 13:00:00"));
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testQueryEdgeWithSearchIndexAndTtlInTx() {
        graph().schema().indexLabel("readByPlace").onE("read")
               .by("place").search().ifNotExist().create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java1 = graph().addVertex(T.label, "book",
                                         "name", "Java1 in action");
        Vertex java2 = graph().addVertex(T.label, "book",
                                         "name", "Java2 in action");
        baby.addEdge("read", java1, "place", "library of school",
                     "date", "2019-12-23 12:00:00");
        graph().tx().commit();

        baby.addEdge("read", java2, "place", "library of school",
                     "date", "2019-12-23 12:00:00");

        Iterator<Edge> edges = graph().traversal().E()
                                      .has("place", Text.contains("library"));
        Assert.assertTrue(edges.hasNext());
        Assert.assertEquals(2, IteratorUtils.count(edges));
        graph().tx().commit();

        try {
            Thread.sleep(3100L);
        } catch (InterruptedException e) {
            // Ignore
        }

        // All edges are expired after 3s
        edges = graph().traversal().E().has("place", Text.contains("library"));
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testQueryAllEdges() {
        HugeGraph graph = graph();
        init18Edges();

        // All edges
        List<Edge> edges = graph.traversal().E().toList();

        Assert.assertEquals(18, edges.size());

        Vertex james = vertex("author", "id", 1);
        Vertex guido = vertex("author", "id", 2);

        Vertex java = vertex("language", "name", "java");
        Vertex python = vertex("language", "name", "python");

        Vertex java1 = vertex("book", "name", "java-1");
        Vertex java2 = vertex("book", "name", "java-2");
        Vertex java3 = vertex("book", "name", "java-3");

        assertContains(edges, "created", james, java);
        assertContains(edges, "created", guido, python);
        assertContains(edges, "authored", james, java1);
        assertContains(edges, "authored", james, java2);
        assertContains(edges, "authored", james, java3);
    }

    @Test
    public void testQueryAllEdgesWithGraphAPI() {
        HugeGraph graph = graph();
        init18Edges();

        // All edges
        List<Edge> edges = ImmutableList.copyOf(graph.edges());

        Assert.assertEquals(18, edges.size());

        Vertex james = vertex("author", "id", 1);
        Vertex guido = vertex("author", "id", 2);

        Vertex java = vertex("language", "name", "java");
        Vertex python = vertex("language", "name", "python");

        Vertex java1 = vertex("book", "name", "java-1");
        Vertex java2 = vertex("book", "name", "java-2");
        Vertex java3 = vertex("book", "name", "java-3");

        assertContains(edges, "created", james, java);
        assertContains(edges, "created", guido, python);
        assertContains(edges, "authored", james, java1);
        assertContains(edges, "authored", james, java2);
        assertContains(edges, "authored", james, java3);
    }

    @Test
    public void testQueryEdgesWithOrderBy() {
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().V()
                                .hasLabel("person").has("name", "Louise")
                                .outE("look").order().by("time")
                                .toList();
        Assert.assertEquals(4, edges.size());
        Assert.assertEquals("2017-5-1", edges.get(0).value("time"));
        Assert.assertEquals("2017-5-1", edges.get(1).value("time"));
        Assert.assertEquals("2017-5-27", edges.get(2).value("time"));
        Assert.assertEquals("2017-5-27", edges.get(3).value("time"));
    }

    @Test
    public void testQueryEdgesWithOrderByAndIncidentToAdjacentStrategy() {
        HugeGraph graph = graph();
        init18Edges();

        List<Vertex> vertices = graph.traversal().V()
                                     .hasLabel("person").has("name", "Louise")
                                     .outE("look").inV().toList();
        /*
         * This should be 4 vertices, but the gremlin:
         * `.outE("look").inV()` will be replaced with `.out("look")`,
         * For more details see IncidentToAdjacentStrategy.
         */
        Assert.assertEquals(4, vertices.size());

        // This will call EdgeVertexStep when `.inV()`
        vertices = graph.traversal().V()
                        .hasLabel("person").has("name", "Louise")
                        .outE("look").order().by("time").inV().toList();
        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testQueryAllWithLimit() {
        HugeGraph graph = graph();
        init18Edges();

        // Query all with limit
        List<Edge> edges = graph.traversal().E().limit(10).toList();
        Assert.assertEquals(10, edges.size());

        edges = graph.traversal().E().limit(12).limit(10).toList();
        Assert.assertEquals(10, edges.size());

        edges = graph.traversal().E().limit(10).limit(12).toList();
        Assert.assertEquals(10, edges.size());
    }

    @Test
    public void testQueryAllWithLimitAfterUncommittedDelete() {
        HugeGraph graph = graph();
        init18Edges();

        // Query all with limit after delete
        graph.traversal().E().limit(14).drop().iterate();

        // Query count with uncommitted records
        Assert.assertEquals(4L, graph.traversal().E().count().next());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Query with limit
            graph.traversal().E().limit(3).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Query with offset
            graph.traversal().E().range(1, -1).toList();
        });

        // Query all with limit after delete once
        graph.tx().commit();
        Assert.assertEquals(4L, graph.traversal().E().count().next());
        List<Edge> edges = graph.traversal().E().limit(3).toList();
        Assert.assertEquals(3, edges.size());

        // Query all with limit after delete twice
        graph.traversal().E().limit(3).drop().iterate();
        graph.tx().commit();
        edges = graph.traversal().E().limit(3).toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryAllWithLimitAfterUncommittedInsert() {
        HugeGraph graph = graph();
        init18Edges();

        // Query all with limit after insert
        Vertex james = graph.addVertex(T.label, "author", "id", 3,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("authored", book,
                      "comment", "good book!",
                      "comment", "good book!",
                      "comment", "good book too!");

        // Query count with uncommitted records
        Assert.assertEquals(19L, graph.traversal().E().count().next());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Query with limit
            graph.traversal().E().limit(3).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Query with offset
            graph.traversal().E().range(1, -1).toList();
        });

        // Do commit
        graph.tx().commit();

        // Query count
        Assert.assertEquals(19L, graph.traversal().E().count().next());

        // Query with limit
        Assert.assertEquals(3L, graph.traversal().E().limit(3).toList().size());

        // Query with offset
        Assert.assertEquals(18L, graph.traversal().E().range(1, -1).toList().size());
    }

    @Test
    public void testQueryAllWithLimitByQueryEdges() {
        HugeGraph graph = graph();
        init18Edges();

        Query query = new Query(HugeType.EDGE);
        query.limit(1);
        Iterator<Edge> iter = graph.edges(query);
        List<Edge> edges = IteratorUtils.list(iter);
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryAllWithLimit0() {
        HugeGraph graph = graph();
        init18Edges();

        // Query all with limit 0
        List<Edge> edges = graph.traversal().E().limit(0).toList();

        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testQueryAllWithNoLimit() {
        HugeGraph graph = graph();
        init18Edges();

        // Query all with limit -1 (mean no-limit)
        List<Edge> edges = graph.traversal().E().limit(-1).toList();
        Assert.assertEquals(18, edges.size());
    }

    @Test
    public void testQueryAllWithIllegalLimit() {
        HugeGraph graph = graph();
        init18Edges();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().limit(-2).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().limit(-20).toList();
        });
    }

    @Test
    public void testQueryAllWithOffset() {
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().E().range(8, 100).toList();
        Assert.assertEquals(10, edges.size());

        List<Edge> edges2 = graph.traversal().E().range(8, -1).toList();
        Assert.assertEquals(edges, edges2);
    }

    @Test
    public void testQueryAllWithOffsetAndLimit() {
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().E().range(8, 9).toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().E().range(0, 4).toList();
        Assert.assertEquals(4, edges.size());

        edges = graph.traversal().E().range(-2, 4).toList();
        Assert.assertEquals(4, edges.size());

        edges = graph.traversal().E().range(18, -1).toList();
        Assert.assertEquals(0, edges.size());

        edges = graph.traversal().E().range(0, -1).toList();
        Assert.assertEquals(18, edges.size());

        edges = graph.traversal().E().range(-2, -1).toList();
        Assert.assertEquals(18, edges.size());
    }

    @Test
    public void testQueryAllWithOffsetAndLimitWithMultiTimes() {
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().E()
                                .range(1, 6)
                                .range(4, 8)
                                .toList();
        // [5, 6)
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().E()
                     .range(1, -1)
                     .range(6, 8)
                     .toList();
        // [7, 9)
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E()
                     .range(1, 6)
                     .range(6, 8)
                     .toList();
        // [7, 6) will be converted to NoneStep by EarlyLimitStrategy
        Assert.assertEquals(0, edges.size());

        edges = graph.traversal().E()
                     .range(1, 6)
                     .range(7, 8)
                     .toList();
        // [8, 6) will be converted to NoneStep by EarlyLimitStrategy
        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testQueryAllWithIllegalOffsetOrLimit() {
        HugeGraph graph = graph();
        init18Edges();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().range(8, 7).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().range(-1, -2).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().range(0, -2).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().range(-4, -2).toList();
        });
    }

    @Test
    public void testQueryEdgesWithLimitOnMultiLevel() {
        HugeGraph graph = graph();
        init18Edges();
        Vertex james = vertex("author", "id", 1);

        List<Edge> edges = graph.traversal().V()
                                .hasLabel("person").has("name", "Louise")
                                .outE("look").inV()
                                .inE("authored")
                                .toList();
        boolean supportIn = storeFeatures().supportsQueryWithInCondition();
        if (supportIn) {
            /*
             * Duplicated results of inE("authored") are removed by backend
             * store with IN Condition query
             */
            Assert.assertEquals(3, edges.size());
            Assert.assertEquals(james, edges.get(0).outVertex());
            Assert.assertEquals(james, edges.get(1).outVertex());
            Assert.assertEquals(james, edges.get(2).outVertex());
        } else {
            Assert.assertEquals(4, edges.size());
            Assert.assertEquals(james, edges.get(0).outVertex());
            Assert.assertEquals(james, edges.get(1).outVertex());
            Assert.assertEquals(james, edges.get(2).outVertex());
            Assert.assertEquals(james, edges.get(3).outVertex());
        }

        edges = graph.traversal().V()
                     .hasLabel("person").has("name", "Louise")
                     .outE("look").inV().dedup()
                     .inE("authored")
                     .toList();
        Assert.assertEquals(3, edges.size());

        edges = graph.traversal().V()
                     .hasLabel("person").has("name", "Louise")
                     .outE("look").limit(2).inV()
                     .inE("authored").limit(3)
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V()
                     .hasLabel("person").has("name", "Louise")
                     .outE("look").limit(3).inV()
                     .inE("authored").limit(2)
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V()
                     .hasLabel("person").has("name", "Louise")
                     .outE("look").limit(2).inV()
                     .inE("authored").limit(1)
                     .toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V()
                     .hasLabel("person").has("name", "Louise")
                     .outE("look").limit(1).inV()
                     .inE("authored").limit(2)
                     .toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryEdgesWithLimitOnMultiLevelAndFilterProp() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        for (int i = 0; i < 20; i++) {
            Vertex java = graph.addVertex(T.label, "book", "name", "java-" + i);
            james.addEdge("authored", java, "score", i % 2);
            james.addEdge("write", java, "time", "2020-6-" + i);
        }

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 62);
        Vertex java0 = graph.addVertex(T.label, "book", "name", "java-0");
        louise.addEdge("look", java0, "time", "2020-6-18", "score", 1);
        louise.addEdge("look", java0, "time", "2020-6-0", "score", 1);

        graph.tx().commit();

        // outE
        List<Edge> edges = graph.traversal().V()
                                .outE().has("score", 0)
                                .toList();
        Assert.assertEquals(10, edges.size());

        edges = graph.traversal().V()
                     .outE().has("score", 0)
                     .limit(11).toList();
        Assert.assertEquals(10, edges.size());

        edges = graph.traversal().V()
                     .outE().has("score", 0)
                     .limit(6).toList();
        Assert.assertEquals(6, edges.size());

        edges = graph.traversal().V()
                     .outE().has("score", 1)
                     .toList();
        Assert.assertEquals(12, edges.size());

        edges = graph.traversal().V()
                     .outE().has("score", 1)
                     .limit(13).toList();
        Assert.assertEquals(12, edges.size());

        edges = graph.traversal().V()
                     .outE().has("score", 1)
                     .limit(7).toList();
        Assert.assertEquals(7, edges.size());

        edges = graph.traversal().V()
                     .outE("authored").has("score", 1)
                     .toList();
        Assert.assertEquals(10, edges.size());

        edges = graph.traversal().V()
                     .outE("authored").has("score", 1)
                     .limit(11).toList();
        Assert.assertEquals(10, edges.size());

        edges = graph.traversal().V()
                     .outE("authored").has("score", 1)
                     .limit(5).toList();
        Assert.assertEquals(5, edges.size());

        edges = graph.traversal().V()
                     .outE().has("time", "2020-6-18")
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V()
                     .outE().has("time", "2020-6-18")
                     .limit(1).toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V()
                     .outE().has("time", "2020-6-0")
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V()
                     .outE().has("time", "2020-6-0")
                     .limit(1).toList();
        Assert.assertEquals(1, edges.size());

        // inE
        edges = graph.traversal().V()
                     .inE().has("score", 0)
                     .toList();
        Assert.assertEquals(10, edges.size());

        edges = graph.traversal().V()
                     .inE().has("score", 0)
                     .limit(11).toList();
        Assert.assertEquals(10, edges.size());

        edges = graph.traversal().V()
                     .inE().has("score", 0)
                     .limit(6).toList();
        Assert.assertEquals(6, edges.size());

        edges = graph.traversal().V()
                     .inE().has("score", 1)
                     .toList();
        Assert.assertEquals(12, edges.size());

        edges = graph.traversal().V()
                     .inE().has("score", 1)
                     .limit(13).toList();
        Assert.assertEquals(12, edges.size());

        edges = graph.traversal().V()
                     .inE().has("score", 1)
                     .limit(7).toList();
        Assert.assertEquals(7, edges.size());

        edges = graph.traversal().V()
                     .inE("authored").has("score", 1)
                     .toList();
        Assert.assertEquals(10, edges.size());

        edges = graph.traversal().V()
                     .inE("authored").has("score", 1)
                     .limit(11).toList();
        Assert.assertEquals(10, edges.size());

        edges = graph.traversal().V()
                     .inE("authored").has("score", 1)
                     .limit(5).toList();
        Assert.assertEquals(5, edges.size());

        edges = graph.traversal().V()
                     .inE().has("time", "2020-6-18")
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V()
                     .inE().has("time", "2020-6-18")
                     .limit(1).toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V()
                     .inE().has("time", "2020-6-0")
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V()
                     .inE().has("time", "2020-6-0")
                     .limit(1).toList();
        Assert.assertEquals(1, edges.size());

        // bothE
        edges = graph.traversal().V(java0)
                     .bothE().has("score", 1)
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V(java0)
                     .bothE().has("score", 1)
                     .limit(3).toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V(java0)
                     .bothE().has("score", 1)
                     .limit(1).toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(java0)
                     .bothE().has("time", "2020-6-0")
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V(java0)
                     .bothE().has("time", "2020-6-0")
                     .limit(3).toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V(java0)
                     .bothE().has("time", "2020-6-0")
                     .limit(1).toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryEdgesAdjacentVerticesWithLimitAndFilterProp() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido = graph.addVertex(T.label, "author", "id", 2,
                                       "name", "Guido van Rossum", "age", 62,
                                       "lived", "California");
        Vertex marko = graph.addVertex(T.label, "author", "id", 3,
                                       "name", "Marko", "age", 61,
                                       "lived", "California");
        guido.addEdge("know", james);
        guido.addEdge("know", marko);
        marko.addEdge("know", james);

        for (int i = 0; i < 20; i++) {
            Vertex java = graph.addVertex(T.label, "book", "name", "java-" + i);
            james.addEdge("authored", java, "score", i % 2);
            james.addEdge("write", java, "time", "2020-6-" + i);

            marko.addEdge("authored", java, "score", i % 2);
        }

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 62);
        Vertex java0 = graph.addVertex(T.label, "book", "name", "java-0");
        louise.addEdge("look", java0, "time", "2020-6-18", "score", 1);
        louise.addEdge("look", java0, "time", "2020-6-0", "score", 1);

        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                                      "city", "Beijing", "age", 62);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 61);

        louise.addEdge("friend", jeff);
        louise.addEdge("friend", sean);
        jeff.addEdge("friend", sean);
        jeff.addEdge("follow", marko);
        sean.addEdge("follow", marko);

        graph.tx().commit();

        // out
        List<Vertex> vertices = graph.traversal().V()
                                     .out().has("age", 62)
                                     .toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertEquals(2, new HashSet<>(vertices).size());
        Assert.assertTrue(vertices.contains(james));
        Assert.assertTrue(vertices.contains(jeff));

        vertices = graph.traversal().V()
                        .out().has("age", 62)
                        .limit(4).toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph.traversal().V()
                        .out().has("age", 62)
                        .limit(1).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V()
                        .out().has("name", "java-0")
                        .toList();
        Assert.assertEquals(5, vertices.size());

        vertices = graph.traversal().V()
                        .out().has("name", "java-0")
                        .limit(6).toList();
        Assert.assertEquals(5, vertices.size());

        vertices = graph.traversal().V()
                        .out().has("name", "java-0")
                        .limit(3).toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph.traversal().V()
                        .out("write", "look")
                        .has("name", Text.contains("java-1"))
                        .toList();
        Assert.assertEquals(11, vertices.size());

        vertices = graph.traversal().V()
                        .out("write", "look")
                        .has("name", Text.contains("java-1"))
                        .limit(12).toList();
        Assert.assertEquals(11, vertices.size());

        vertices = graph.traversal().V()
                        .out("write", "look")
                        .has("name", Text.contains("java-1"))
                        .limit(2).toList();
        Assert.assertEquals(2, vertices.size());

        boolean firstIsLook = graph.traversal().V()
                                   .outE("write", "look")
                                   .limit(1).label().is("look").hasNext();
        if (firstIsLook) {
            // query edges of louise if before james
            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(12)
                            .has("name", Text.contains("java-1"))
                            .limit(1).toList();
            Assert.assertEquals(1, vertices.size());
            Assert.assertEquals("java-1", vertices.get(0).value("name"));

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(12)
                            .has("name", Text.contains("java-1"))
                            .limit(11).toList();
            Assert.assertEquals(9, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(12)
                            .has("name", Text.contains("java-1"))
                            .limit(12).toList();
            Assert.assertEquals(9, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(13)
                            .has("name", Text.contains("java-1"))
                            .limit(12).toList();
            Assert.assertEquals(10, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(13)
                            .has("name", Text.contains("java-1"))
                            .limit(13).toList();
            Assert.assertEquals(10, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(14)
                            .has("name", Text.contains("java-1"))
                            .limit(12).toList();
            Assert.assertEquals(11, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(3)
                            .has("name", Text.contains("java-0"))
                            .limit(3).toList();
            Assert.assertEquals(3, vertices.size());
        } else {
            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(12)
                            .has("name", Text.contains("java-1"))
                            .limit(11).toList();
            Assert.assertEquals(11, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(12)
                            .has("name", Text.contains("java-1"))
                            .limit(12).toList();
            Assert.assertEquals(11, vertices.size());

            boolean firstLouise = graph.traversal().V(louise, james)
                                       .outE("write", "look").outV().next()
                                       .equals(louise);
            vertices = graph.traversal().V(louise, james)
                            .out("write", "look")
                            .limit(12)
                            .has("name", Text.contains("java-1"))
                            .limit(11).toList();
            // two look edges not matched
            Assert.assertEquals(firstLouise ? 9 : 11, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(3)
                            .has("name", Text.contains("java-0"))
                            .limit(3).toList();
            Assert.assertEquals(1, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(20)
                            .has("name", Text.contains("java-0"))
                            .limit(3).toList();
            Assert.assertEquals(1, vertices.size()); // skip java1~java19

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(21)
                            .has("name", Text.contains("java-0"))
                            .limit(3).toList();
            Assert.assertEquals(2, vertices.size());

            vertices = graph.traversal().V()
                            .out("write", "look")
                            .limit(22)
                            .has("name", Text.contains("java-0"))
                            .limit(3).toList();
            Assert.assertEquals(3, vertices.size());
        }

        // in
        vertices = graph.traversal().V(java0)
                        .in().has("age", 62)
                        .toList();
        Assert.assertEquals(4, vertices.size());
        Assert.assertEquals(2, new HashSet<>(vertices).size());
        Assert.assertTrue(vertices.contains(james));
        Assert.assertTrue(vertices.contains(louise));

        vertices = graph.traversal().V(java0)
                        .in().has("age", 62)
                        .limit(5).toList();
        Assert.assertEquals(4, vertices.size());

        vertices = graph.traversal().V(java0)
                        .in().has("age", 62)
                        .limit(3).toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph.traversal().V(java0)
                        .in().has("age", 62)
                        .limit(1).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V(java0)
                        .in().has("age", 62)
                        .dedup().limit(3).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V(java0)
                        .in().has("age", 62)
                        .dedup().limit(1).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V(james, jeff)
                        .in().has("age", 62)
                        .toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(guido));
        Assert.assertTrue(vertices.contains(louise));

        vertices = graph.traversal().V(james, jeff)
                        .in().has("age", 62)
                        .limit(3).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V(james, jeff)
                        .in().has("age", 62)
                        .limit(1).toList();
        Assert.assertEquals(1, vertices.size());

        // both
        vertices = graph.traversal().V(sean)
                        .both().has("age", 62)
                        .toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(louise));
        Assert.assertTrue(vertices.contains(jeff));

        vertices = graph.traversal().V(sean)
                        .both().has("age", 62)
                        .limit(3).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V(sean)
                        .both().has("age", 62)
                        .limit(1).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V(marko)
                        .both().has("age", 62)
                        .toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(guido));
        Assert.assertTrue(vertices.contains(jeff));

        vertices = graph.traversal().V(marko)
                        .both().has("age", 62)
                        .limit(4).toList();
        Assert.assertEquals(3, vertices.size());

        vertices = graph.traversal().V(marko)
                        .both().has("age", 62)
                        .limit(1).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph.traversal().V(jeff)
                        .both().has("age", 61)
                        .toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(sean));
        Assert.assertTrue(vertices.contains(marko));

        vertices = graph.traversal().V(jeff)
                        .both().has("age", 61)
                        .limit(3).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = graph.traversal().V(jeff)
                        .both().has("age", 61)
                        .limit(1).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testQueryEdgesWithLimitOnSuperVertexAndFilterProp() {
        HugeGraph graph = graph();

        int txCap = this.superNodeSize();
        for (int i = 0; i < txCap; i++) {
            Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                           "name", "James Gosling", "age", 62,
                                           "lived", "Canadian");
            Vertex java = graph.addVertex(T.label, "book", "name", "java-" + i);
            james.addEdge("authored", java, "score", i % 2);
            james.addEdge("write", java, "time", "2020-6-" + i);
            if (i % TX_BATCH == 0) {
                graph.tx().commit();
            }
        }
        graph.tx().commit();

        List<Edge> edges = graph.traversal().V()
                                .outE().has("score", 0)
                                .limit(10).toList();
        Assert.assertEquals(10, edges.size());
    }

    @Test
    public void testQueryEdgesWithLimitAndOrderBy() {
        Assume.assumeTrue("Not support order by",
                          storeFeatures().supportsQueryWithOrderBy());
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().V()
                                .hasLabel("person").has("name", "Louise")
                                .outE("look").order().by("time")
                                .limit(2).toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertEquals("2017-5-1", edges.get(0).value("time"));
        Assert.assertEquals("2017-5-1", edges.get(1).value("time"));

        edges = graph.traversal().V()
                     .hasLabel("person").has("name", "Louise")
                     .outE("look").order().by("time", Order.desc)
                     .limit(2).toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertEquals("2017-5-27", edges.get(0).value("time"));
        Assert.assertEquals("2017-5-27", edges.get(1).value("time"));

        edges = graph.traversal().V()
                     .hasLabel("person").has("name", "Louise")
                     .outE("look").limit(2)
                     .order().by("time", Order.desc)
                     .toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertEquals("2017-5-1", edges.get(0).value("time"));
        Assert.assertEquals("2017-5-1", edges.get(1).value("time"));
    }

    @Test
    public void testQueryEdgesById() {
        HugeGraph graph = graph();
        init18Edges();

        Object id = graph.traversal().E().toList().get(0).id();
        List<Edge> edges = graph.traversal().E(id).toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryEdgesByIdWithGraphAPI() {
        HugeGraph graph = graph();
        init18Edges();

        Object id = graph.traversal().E().toList().get(0).id();
        List<Edge> edges = ImmutableList.copyOf(graph.edges(id));
        Assert.assertEquals(1, edges.size());

        edges = ImmutableList.copyOf(graph.edges(id, id));
        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testQueryEdgesByIdWithGraphAPIAndNotCommittedUpdate() {
        HugeGraph graph = graph();
        init18Edges();

        Edge edge = graph.traversal().E().hasLabel("look").toList().get(0);
        Object id = edge.id();
        Assert.assertTrue(graph.edges(id).hasNext());
        Assert.assertTrue(graph.edges(id, id).hasNext());

        edge.property("score", 101);

        List<Edge> edges = ImmutableList.copyOf(graph.edges(id));
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(101, (int) edges.get(0).value("score"));

        edges = ImmutableList.copyOf(graph.edges(id, id));
        Assert.assertEquals(2, edges.size());
        Assert.assertEquals(101, (int) edges.get(1).value("score"));
    }

    @Test
    public void testQueryEdgesByIdWithGraphAPIAndNotCommittedRemoved() {
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().E().toList();

        Edge edge1 = edges.get(0);
        Edge edge2 = edges.get(1);
        Assert.assertTrue(graph.edges(edge1.id()).hasNext());
        Assert.assertTrue(graph.edges(edge2.id()).hasNext());

        edge1.remove();
        edge2.remove();
        Assert.assertFalse(graph.edges(edge1.id()).hasNext());
        Assert.assertFalse(graph.edges(edge2.id()).hasNext());
        Assert.assertFalse(graph.edges(edge1.id(), edge2.id()).hasNext());

        graph.tx().rollback();
        Assert.assertTrue(graph.edges(edge1.id()).hasNext());
        Assert.assertTrue(graph.edges(edge2.id()).hasNext());
        Assert.assertTrue(graph.edges(edge1.id(), edge2.id()).hasNext());
    }

    @Test
    public void testQueryEdgesByIdNotFound() {
        HugeGraph graph = graph();
        init18Edges();

        String id = graph.traversal().E().toList().get(0).id() + "-not-exist";
        Assert.assertTrue(graph.traversal().E(id).toList().isEmpty());
        Assert.assertThrows(NoSuchElementException.class, () -> {
            graph.traversal().E(id).next();
        });
    }

    @Test
    public void testQueryEdgesByInvalidId() {
        HugeGraph graph = graph();
        init18Edges();

        String id = "invalid-id";
        List<Edge> edges = graph.traversal().E(id).toList();
        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testQueryEdgesByInvalidSysprop() {
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().E().hasLabel("know").toList();
        Assert.assertEquals(1, edges.size());
        HugeEdge edge = (HugeEdge) edges.get(0);
        Id id = edge.id();
        Id know = edge.schemaLabel().id();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().hasLabel("know").has("ID", id).toList();
        }, e -> {
            Assert.assertContains("Undefined property key: 'ID'",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().hasLabel("know").has("NAME", "n1").toList();
        }, e -> {
            Assert.assertContains("Undefined property key: 'NAME'",
                                  e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            ConditionQuery query = new ConditionQuery(HugeType.EDGE);
            query.eq(HugeKeys.LABEL, know);
            query.query(id);
            graph.edges(query).hasNext();
        }, e -> {
            Assert.assertContains("Not supported querying by id and conditions",
                                  e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            ConditionQuery query = new ConditionQuery(HugeType.EDGE);
            query.eq(HugeKeys.LABEL, know);
            query.eq(HugeKeys.NAME, "n1");
            graph.edges(query).hasNext();
        }, e -> {
            Assert.assertContains("Not supported querying edges by",
                                  e.getMessage());
            Assert.assertContains("NAME == n1", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            ConditionQuery query = new ConditionQuery(HugeType.EDGE);
            query.eq(HugeKeys.LABEL, know);
            query.eq(HugeKeys.NAME, "n2");
            query.query(Condition.eq(IdGenerator.of("fake"), "n3"));
            graph.edges(query).hasNext();
        }, e -> {
            Assert.assertContains("Can't do index query with [",
                                  e.getMessage());
            Assert.assertContains("LABEL == ", e.getMessage());
            Assert.assertContains("NAME == n2", e.getMessage());
        });
    }

    @Test
    public void testQueryEdgesByLabel() {
        HugeGraph graph = graph();
        init18Edges();

        List<Edge> edges = graph.traversal().E().hasLabel("created").toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E().hasLabel("authored").toList();
        Assert.assertEquals(3, edges.size());

        edges = graph.traversal().E().hasLabel("look").toList();
        Assert.assertEquals(7, edges.size());

        edges = graph.traversal().E().hasLabel("friend").toList();
        Assert.assertEquals(4, edges.size());

        edges = graph.traversal().E().hasLabel("follow").toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().E().hasLabel("know").toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().E().hasLabel("created", "authored").toList();
        Assert.assertEquals(5, edges.size());
    }

    @Test
    public void testQueryEdgesByLabelWithLimit() {
        HugeGraph graph = graph();
        init18Edges();

        // Query by vertex label with limit
        List<Edge> edges = graph.traversal().E().hasLabel("look")
                                .limit(5).toList();
        Assert.assertEquals(5, edges.size());

        // Query by vertex label with limit
        graph.traversal().E().hasLabel("look").limit(5).drop().iterate();
        graph.tx().commit();

        edges = graph.traversal().E().hasLabel("look").limit(3).toList();
        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testQueryEdgesByDirection() {
        HugeGraph graph = graph();
        init18Edges();

        // Query vertex by condition (filter by Direction)
        ConditionQuery q = new ConditionQuery(HugeType.EDGE);
        q.eq(HugeKeys.DIRECTION, Direction.OUT);

        Assert.assertThrows(HugeException.class, () -> {
            graph.edges(q);
        });
    }

    @Test
    public void testQueryEdgesByHasKey() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support CONTAINS_KEY query",
                          storeFeatures().supportsQueryWithContainsKey());
        init18Edges();

        List<Edge> edges = graph.traversal().E()
                                .hasLabel("authored").hasKey("score")
                                .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(3, edges.get(0).value("score"));

        edges = graph.traversal().E().hasKey("score").toList();
        Assert.assertEquals(5, edges.size());
    }

    @Test
    public void testQueryEdgesByHasKeys() {
        HugeGraph graph = graph();
        init18Edges();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().hasLabel("authored")
                 .hasKey("score", "time").toList();
        });
    }

    @Test
    public void testQueryEdgesByHasValue() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support CONTAINS query",
                          storeFeatures().supportsQueryWithContains());
        init18Edges();

        List<Edge> edges = graph.traversal().E()
                                .hasLabel("look").hasValue(3)
                                .toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertEquals(3, edges.get(0).value("score"));
        Assert.assertEquals(3, edges.get(1).value("score"));

        // TODO: Seems Cassandra Bug if contains null value #862
        //edges = graph.traversal().E().hasValue(3).toList();
        //Assert.assertEquals(3, edges.size());
    }

    @Test
    public void testQueryEdgesByHasValues() {
        HugeGraph graph = graph();
        init18Edges();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph.traversal().E().hasLabel("look")
                 .hasValue(3, "2017-5-1").toList();
        });
    }

    @Test
    public void testQueryEdgesOfVertex() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex james = vertex("author", "id", 1);

        // Query BOTH edges of a vertex
        List<Edge> edges = graph.traversal().V(james.id()).bothE().toList();
        Assert.assertEquals(6, edges.size());

        edges = ImmutableList.copyOf(james.edges(Direction.BOTH));
        Assert.assertEquals(6, edges.size());

        // Query OUT edges of a vertex
        edges = graph.traversal().V(james.id()).outE().toList();
        Assert.assertEquals(4, edges.size());

        edges = ImmutableList.copyOf(james.edges(Direction.OUT));
        Assert.assertEquals(4, edges.size());

        // Query IN edges of a vertex
        edges = graph.traversal().V(james.id()).inE().toList();
        Assert.assertEquals(2, edges.size());

        edges = ImmutableList.copyOf(james.edges(Direction.IN));
        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testQueryEdgesOfVertexWithoutCommit() {
        HugeGraph graph = graph();
        init18Edges(false);

        HugeVertex james = (HugeVertex) vertex("author", "id", 1);

        List<Vertex> vertices = ImmutableList.copyOf(
                james.getVertices(Directions.BOTH));
        Assert.assertEquals(6, vertices.size());

        vertices = ImmutableList.copyOf(james.getVertices(Directions.OUT));
        Assert.assertEquals(4, vertices.size());

        vertices = ImmutableList.copyOf(james.getVertices(Directions.IN));
        Assert.assertEquals(2, vertices.size());

        vertices = ImmutableList.copyOf(james.getVertices(Directions.OUT,
                                                          "authored"));
        Assert.assertEquals(3, vertices.size());

        vertices = ImmutableList.copyOf(james.getVertices(Directions.OUT,
                                                          "authored",
                                                          "created"));
        Assert.assertEquals(4, vertices.size());

        vertices = ImmutableList.copyOf(james.getVertices(Directions.IN,
                                                          "know"));
        Assert.assertEquals(1, vertices.size());

        // Query BOTH edges of a vertex
        List<Edge> edges = graph.traversal().V(james.id()).bothE().toList();
        Assert.assertEquals(6, edges.size());

        edges = ImmutableList.copyOf(james.edges(Direction.BOTH));
        Assert.assertEquals(6, edges.size());

        // Query OUT edges of a vertex
        edges = graph.traversal().V(james.id()).outE().toList();
        Assert.assertEquals(4, edges.size());

        edges = ImmutableList.copyOf(james.edges(Direction.OUT));
        Assert.assertEquals(4, edges.size());

        // Query IN edges of a vertex
        edges = graph.traversal().V(james.id()).inE().toList();
        Assert.assertEquals(2, edges.size());

        edges = ImmutableList.copyOf(james.edges(Direction.IN));
        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testQueryEdgesOfVertexWithGraphAPI() {
        HugeGraph graph = graph();
        init18Edges();

        // Query edges of a vertex
        Vertex james = vertex("author", "id", 1);
        List<Edge> edges = ImmutableList.copyOf(
                graph.adjacentEdges((Id) james.id()));
        Assert.assertEquals(6, edges.size());

        List<Edge> edges2 = ImmutableList.copyOf(james.edges(Direction.BOTH));
        Assert.assertEquals(6, edges2.size());

        Assert.assertEquals(edges2, edges);
    }

    @Test
    public void testQueryEdgesOfVertexWithCustomizeId() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("author2")
              .properties("name", "age", "lived")
              .useCustomizeNumberId()
              .enableLabelIndex(false)
              .create();
        schema.vertexLabel("language2")
              .properties("name", "dynamic")
              .useCustomizeUuidId()
              .nullableKeys("dynamic")
              .enableLabelIndex(false)
              .create();
        schema.vertexLabel("book2")
              .properties("name")
              .useAutomaticId()
              .enableLabelIndex(false)
              .create();

        schema.edgeLabel("created2").singleTime()
              .link("author2", "language2")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("know2").singleTime()
              .link("author2", "author2")
              .enableLabelIndex(true)
              .create();
        schema.edgeLabel("authored2").singleTime()
              .properties("contribution", "comment", "score")
              .nullableKeys("score", "contribution", "comment")
              .link("author2", "book2")
              .enableLabelIndex(true)
              .create();

        Vertex james = graph.addVertex(T.label, "author2", T.id, 13579,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido = graph.addVertex(T.label, "author2", T.id, 24680,
                                       "name", "Guido van Rossum", "age", 61,
                                       "lived", "California");

        Vertex java = graph.addVertex(T.label, "language2", "name", "java",
                                      T.id, UUID.randomUUID());
        Vertex python = graph.addVertex(T.label, "language2",
                                        T.id, UUID.randomUUID(),
                                        "name", "python", "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book2", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book2", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book2", "name", "java-3");

        Edge e1 = james.addEdge("created2", java);
        Edge e2 = guido.addEdge("created2", python);

        Edge e3 = guido.addEdge("know2", james);

        Edge e4 = james.addEdge("authored2", java1);
        Edge e5 = james.addEdge("authored2", java2);
        Edge e6 = james.addEdge("authored2", java3, "score", 3);

        graph.tx().commit();

        // Query OUT edges of a vertex
        List<Edge> edges = graph.traversal().V(james.id()).outE().toList();
        Assert.assertEquals(4, edges.size());

        edges = ImmutableList.copyOf(james.edges(Direction.OUT));
        Assert.assertEquals(4, edges.size());

        // Query IN edges of a vertex
        edges = graph.traversal().V(james.id()).inE().toList();
        Assert.assertEquals(1, edges.size());

        edges = ImmutableList.copyOf(james.edges(Direction.IN));
        Assert.assertEquals(1, edges.size());

        // Query BOTH edges of a vertex
        edges = graph.traversal().V(james.id()).bothE().toList();
        Assert.assertEquals(5, edges.size());

        edges = ImmutableList.copyOf(james.edges(Direction.BOTH));
        Assert.assertEquals(5, edges.size());

        // Query by edge id
        Assert.assertEquals(1L, graph.traversal().E(e1.id()).count().next());
        Assert.assertEquals(1L, graph.traversal().E(e2.id()).count().next());
        Assert.assertEquals(1L, graph.traversal().E(e3.id()).count().next());
        Assert.assertEquals(1L, graph.traversal().E(e4.id()).count().next());
        Assert.assertEquals(1L, graph.traversal().E(e5.id()).count().next());
        Assert.assertEquals(1L, graph.traversal().E(e6.id()).count().next());
    }

    @Test
    public void testQueryAdjacentVerticesOfVertex() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex jeff = vertex("person", "name", "Jeff");

        // BOTH
        List<Vertex> vertices = graph.traversal().V(jeff.id())
                                     .both("friend").toList();
        Assert.assertEquals(2, vertices.size());

        vertices = ImmutableList.copyOf(
                jeff.vertices(Direction.BOTH, "friend"));
        Assert.assertEquals(2, vertices.size());

        // OUT
        vertices = graph.traversal().V(jeff.id()).out("look").toList();
        Assert.assertEquals(1, vertices.size());

        vertices = ImmutableList.copyOf(jeff.vertices(Direction.OUT, "look"));
        Assert.assertEquals(1, vertices.size());

        // IN
        vertices = graph.traversal().V(jeff.id()).in("follow").toList();
        Assert.assertEquals(0, vertices.size());

        vertices = ImmutableList.copyOf(jeff.vertices(Direction.IN, "follow"));
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testQueryAdjacentVerticesOfEdges() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex jeff = vertex("person", "name", "Jeff");
        Vertex java3 = vertex("book", "name", "java-3");

        // BOTH
        List<Vertex> vertices = graph.traversal().V(jeff.id())
                                     .bothE("friend").as("e").otherV()
                                     .toList();
        Assert.assertEquals(2, vertices.size());

        // OUT
        List<Edge> edges = graph.traversal().V(jeff.id())
                                .outE("look").toList();
        Assert.assertEquals(1, edges.size());

        HugeEdge edge = (HugeEdge) edges.get(0);
        Assert.assertEquals(jeff, edge.ownerVertex());
        Assert.assertEquals(jeff, edge.sourceVertex());
        Assert.assertEquals(java3, edge.otherVertex());
        Assert.assertEquals(java3, edge.targetVertex());
        Assert.assertEquals(jeff, edge.vertices(Direction.OUT).next());
        Assert.assertEquals(java3, edge.vertices(Direction.IN).next());

        // Fill edge properties
        Assert.assertEquals(2, edge.getProperties().size());
        Whitebox.setInternalState(edge, "propLoaded", false);
        Whitebox.setInternalState(edge, "properties",
                                  CollectionFactory.newIntObjectMap());
        Assert.assertEquals(0, edge.getProperties().size());
        Assert.assertEquals(2, edge.getFilledProperties().size());
        Assert.assertEquals(2, edge.getProperties().size());

        // Fill vertex properties
        Assert.assertTrue(edge.otherVertex().getProperties().isEmpty());
        Assert.assertEquals(1, edge.otherVertex().getFilledProperties().size());
        Assert.assertEquals(1, edge.otherVertex().getProperties().size());
    }

    @Test
    public void testQueryAdjacentVerticesOfEdgesWithoutVertex()
            throws InterruptedException, ExecutionException {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex java = new HugeVertex(graph, IdGenerator.of("java"),
                                     graph.vertexLabel("book"));

        james.addEdge("authored", java, "score", 3);
        graph.tx().commit();

        List<Edge> edges = graph.traversal().V(james.id()).outE().toList();
        Assert.assertEquals(1, edges.size());

        Assert.assertEquals(0, graph.traversal().V(java).toList().size());

        List<Vertex> vertices = graph.traversal().V(james.id()).out().toList();
        Assert.assertEquals(1, vertices.size());
        HugeVertex adjacent = (HugeVertex) vertices.get(0);
        Assert.assertFalse(adjacent.schemaLabel().undefined());
        adjacent.forceLoad(); // force load
        Assert.assertTrue("label: " + adjacent.schemaLabel(),
                          adjacent.schemaLabel().undefined());
        Assert.assertEquals("~undefined", adjacent.label());

        vertices = graph.traversal().V(james.id()).outE().otherV().toList();
        Assert.assertEquals(1, vertices.size());
        adjacent = (HugeVertex) vertices.get(0);
        Assert.assertTrue(adjacent.isPropLoaded());
        Assert.assertTrue(adjacent.schemaLabel().undefined());
        adjacent.forceLoad();
        Assert.assertTrue(adjacent.schemaLabel().undefined());
        Assert.assertEquals("~undefined", adjacent.label());

        params().graphEventHub().notify(Events.CACHE, "clear", null).get();
        vertices = graph.traversal().V(james.id()).outE().otherV().toList();
        Assert.assertEquals(1, vertices.size());
        adjacent = (HugeVertex) vertices.get(0);
        Assert.assertFalse(adjacent.isPropLoaded());
        Assert.assertFalse(adjacent.schemaLabel().undefined());
        adjacent.forceLoad();
        Assert.assertTrue(adjacent.schemaLabel().undefined());
        Assert.assertEquals("~undefined", adjacent.label());

        vertices = graph.traversal().V(james.id()).outE()
                        .has("score", 3).otherV().toList();
        Assert.assertEquals(1, vertices.size());
        adjacent = (HugeVertex) vertices.get(0);
        adjacent.forceLoad();
        // NOTE: if not commit, adjacent.label() will return 'book'
        Assert.assertTrue(adjacent.schemaLabel().undefined());
        Assert.assertEquals("~undefined", adjacent.label());
        Assert.assertFalse(adjacent.properties().hasNext());

        vertices = graph.traversal().V(james.id()).outE()
                        .has("score", 3).otherV().toList();
        Assert.assertEquals(1, vertices.size());
        adjacent = (HugeVertex) vertices.get(0);
        Assert.assertTrue(adjacent.schemaLabel().undefined());
        Assert.assertEquals("~undefined", adjacent.label());

        Whitebox.setInternalState(params().graphTransaction(),
                                  "checkAdjacentVertexExist", true);
        try {
            Assert.assertThrows(HugeException.class, () -> {
                // read from cache
                graph.traversal().V(james.id()).outE().has("score", 3)
                     .otherV().values().toList();
            }, e -> {
                Assert.assertContains("Vertex 'java' does not exist",
                                      e.getMessage());
            });
        } finally {
            Whitebox.setInternalState(params().graphTransaction(),
                                      "checkAdjacentVertexExist", false);
        }

        Whitebox.setInternalState(params().graphTransaction(),
                                  "checkAdjacentVertexExist", true);
        params().graphEventHub().notify(Events.CACHE, "clear", null).get();
        try {
            Assert.assertEquals(0, graph.traversal().V(java).toList().size());

            Assert.assertThrows(HugeException.class, () -> {
                graph.traversal().V(james.id()).out().values().toList();
            }, e -> {
                Assert.assertContains("Vertex 'java' does not exist",
                                      e.getMessage());
            });

            Assert.assertThrows(HugeException.class, () -> {
                graph.traversal().V(james.id()).outE().otherV()
                     .values().toList();
            }, e -> {
                Assert.assertContains("Vertex 'java' does not exist",
                                      e.getMessage());
            });

            Assert.assertThrows(HugeException.class, () -> {
                Vertex v = graph.traversal().V(james.id()).outE()
                                .has("score", 3).otherV().next();
                v.properties(); // throw
            }, e -> {
                Assert.assertContains("Vertex 'java' does not exist",
                                      e.getMessage());
            });

            Assert.assertThrows(HugeException.class, () -> {
                Vertex v = graph.traversal().V(james.id()).outE()
                                .has("score", 3).otherV().next();
                v.values(); // throw
            }, e -> {
                Assert.assertContains("Vertex 'java' does not exist",
                                      e.getMessage());
            });

            Assert.assertThrows(HugeException.class, () -> {
                Vertex v = graph.traversal().V(james.id()).outE()
                                .has("score", 3).otherV().next();
                ((HugeVertex) v).forceLoad(); // throw
            }, e -> {
                Assert.assertContains("Vertex 'java' does not exist",
                                      e.getMessage());
            });
        } finally {
            Whitebox.setInternalState(params().graphTransaction(),
                                      "checkAdjacentVertexExist", false);
        }
    }

    @Test
    public void testQueryAdjacentVerticesOfEdgesWithInvalidVertexLabel()
            throws InterruptedException, ExecutionException {
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
        schema.edgeLabel("call")
              .sourceLabel("designer").targetLabel("programmer")
              .create();

        Vertex v1 = graph.addVertex(T.label, "programmer", T.id, "123",
                                    "name", "marko", "age", 18,
                                    "city", "Beijing");
        Vertex v2 = graph.addVertex(T.label, "designer", T.id, "456",
                                    "name", "marko", "age", 19,
                                    "city", "Beijing");
        v2.addEdge("call", v1);
        graph.tx().commit();

        List<Vertex> vertices = graph.traversal().V(v1).both().toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v2, vertices.get(0));
        Assert.assertEquals(19, vertices.get(0).value("age"));
        Assert.assertEquals("designer", vertices.get(0).label());

        Assert.assertThrows(HugeException.class, () -> {
            // try to override vertex designer-456 with programmer-456
            graph.addVertex(T.label, "programmer", T.id, "456",
                            "name", "marko", "age", 20, "city", "Beijing");
            graph.tx().commit();
        }, e -> {
            String error = "The newly added vertex with id:'456' label:" +
                           "'programmer' is not allowed to insert, " +
                           "because already exist a vertex with same id and " +
                           "different label:'designer'";
            Assert.assertContains(error, e.getMessage());
        });

        Whitebox.setInternalState(params().graphTransaction(),
                                  "checkCustomVertexExist", false);
        params().graphEventHub().notify(Events.CACHE, "clear", null).get();
        try {
            // override vertex designer-456 with programmer-456
            graph.addVertex(T.label, "programmer", T.id, "456",
                            "name", "marko", "age", 21, "city", "Beijing");
            graph.tx().commit();
        } finally {
            Whitebox.setInternalState(params().graphTransaction(),
                                      "checkCustomVertexExist", true);
        }

        // query by id
        vertices = graph.traversal().V("456").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("456", vertices.get(0).id().toString());
        Assert.assertEquals(21, vertices.get(0).value("age"));
        Assert.assertEquals("programmer", vertices.get(0).label());

        // query by adjacent vertex
        vertices = graph.traversal().V("123").both().toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals("456", vertices.get(0).id().toString());
        Assert.assertEquals(false, vertices.get(0).property("age").isPresent());
        Assert.assertEquals("~undefined", vertices.get(0).label());
    }

    @Test
    public void testQueryAdjacentVerticesOfEdgesWithoutVertexAndNoLazyLoad()
            throws InterruptedException, ExecutionException {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex java = new HugeVertex(graph, IdGenerator.of("java"),
                                     graph.vertexLabel("book"));

        james.addEdge("authored", java, "score", 3);
        graph.tx().commit();

        Whitebox.setInternalState(params().graphTransaction(),
                                  "lazyLoadAdjacentVertex", false);
        try {
            List<Edge> edges = graph.traversal().V(james.id()).outE().toList();
            Assert.assertEquals(1, edges.size());

            Assert.assertEquals(0, graph.traversal().V(java).toList().size());

            List<Vertex> vertices = graph.traversal().V(james.id())
                                         .out().toList();
            Assert.assertEquals(1, vertices.size());
            HugeVertex adjacent = (HugeVertex) vertices.get(0);
            Assert.assertTrue("label: " + adjacent.schemaLabel(),
                              adjacent.schemaLabel().undefined());
            Assert.assertEquals("~undefined", adjacent.label());

            vertices = graph.traversal().V(james.id()).outE().otherV().toList();
            Assert.assertEquals(1, vertices.size());
            adjacent = (HugeVertex) vertices.get(0);
            Assert.assertTrue(adjacent.schemaLabel().undefined());
            Assert.assertEquals("~undefined", adjacent.label());

            vertices = graph.traversal().V(james.id()).outE()
                            .has("score", 3).otherV().toList();
            Assert.assertEquals(1, vertices.size());
            adjacent = (HugeVertex) vertices.get(0);
            // NOTE: if not load, adjacent.label() will return 'book'
            Assert.assertEquals("book", adjacent.label());
            adjacent.forceLoad();
            Assert.assertTrue(adjacent.schemaLabel().undefined());
            Assert.assertEquals("~undefined", adjacent.label());
            Assert.assertFalse(adjacent.properties().hasNext());

            vertices = graph.traversal().V(james.id()).outE()
                            .has("score", 3).otherV().toList();
            Assert.assertEquals(1, vertices.size());
            adjacent = (HugeVertex) vertices.get(0);
            Assert.assertTrue(adjacent.schemaLabel().undefined());
            Assert.assertEquals("~undefined", adjacent.label());
        } finally {
            Whitebox.setInternalState(params().graphTransaction(),
                                      "lazyLoadAdjacentVertex", true);
        }

        Whitebox.setInternalState(params().graphTransaction(),
                                  "lazyLoadAdjacentVertex", false);
        Whitebox.setInternalState(params().graphTransaction(),
                                  "checkAdjacentVertexExist", true);
        params().graphEventHub().notify(Events.CACHE, "clear", null).get();
        try {
            Assert.assertEquals(0, graph.traversal().V(java).toList().size());

            Assert.assertThrows(HugeException.class, () -> {
                graph.traversal().V(james.id()).out().toList();
            }, e -> {
                Assert.assertContains("Vertex 'java' does not exist",
                                      e.getMessage());
            });

            Assert.assertThrows(HugeException.class, () -> {
                graph.traversal().V(james.id()).outE().otherV().toList();
            }, e -> {
                Assert.assertContains("Vertex 'java' does not exist",
                                      e.getMessage());
            });

            Assert.assertThrows(HugeException.class, () -> {
                Vertex v = graph.traversal().V(james.id()).outE()
                                .has("score", 3).otherV().next();
                v.properties(); // throw
            }, e -> {
                Assert.assertContains("Vertex 'java' does not exist",
                                      e.getMessage());
            });

            Assert.assertThrows(HugeException.class, () -> {
                Vertex v = graph.traversal().V(james.id()).outE()
                                .has("score", 3).otherV().next();
                v.values(); // throw
            }, e -> {
                Assert.assertContains("Vertex 'java' does not exist",
                                      e.getMessage());
            });
        } finally {
            Whitebox.setInternalState(params().graphTransaction(),
                                      "lazyLoadAdjacentVertex", true);
            Whitebox.setInternalState(params().graphTransaction(),
                                      "checkAdjacentVertexExist", false);
        }
    }

    @Test
    public void testQueryOutEdgesOfVertex() {
        HugeGraph graph = graph();
        init18Edges();

        // Query OUT edges of a vertex
        Vertex james = vertex("author", "id", 1);
        List<Edge> edges = graph.traversal().V(james.id()).outE().toList();

        Assert.assertEquals(4, edges.size());
    }

    @Test
    public void testQueryOutVerticesOfVertex() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex james = vertex("author", "id", 1);
        List<Vertex> vertices = graph.traversal().V(james.id()).out().toList();

        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testQueryOutEdgesOfVertexBySortkey() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex louise = vertex("person", "name", "Louise");

        List<Edge> edges = graph.traversal().V(louise.id()).outE("look")
                                .has("time", "2017-5-1").toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V(louise.id())
                     .outE("look").has("time", "2017-5-27")
                     .toList();
        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testQueryOutEdgesOfVertexBySortkeyAndProps() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex louise = vertex("person", "name", "Louise");

        List<Edge> edges = graph.traversal().V(louise.id()).outE("look")
                                .has("time", "2017-5-1").has("score", 3)
                                .toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(louise.id()).outE("look")
                     .has("time", "2017-5-27").has("score", 3)
                     .toList();
        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testQueryOutEdgesOfVertexBySortkeyWithRange() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("no").asText().create();
        schema.propertyKey("calltime").asDate().create();
        schema.vertexLabel("phone")
              .properties("no")
              .primaryKeys("no")
              .enableLabelIndex(false)
              .create();
        schema.edgeLabel("call").multiTimes().properties("calltime")
              .sourceLabel("phone").targetLabel("phone")
              .sortKeys("calltime")
              .create();

        Vertex v1 = graph.addVertex(T.label, "phone", "no", "13812345678");
        Vertex v2 = graph.addVertex(T.label, "phone", "no", "13866668888");
        Vertex v10086 = graph.addVertex(T.label, "phone", "no", "10086");

        v1.addEdge("call", v2, "calltime", "2017-5-1 23:00:00");
        v1.addEdge("call", v2, "calltime", "2017-5-2 12:00:01");
        v1.addEdge("call", v2, "calltime", "2017-5-3 12:08:02");
        v1.addEdge("call", v2, "calltime", "2017-5-3 22:22:03");
        v1.addEdge("call", v2, "calltime", "2017-5-4 20:33:04");

        v1.addEdge("call", v10086, "calltime", "2017-5-2 15:30:05");
        v1.addEdge("call", v10086, "calltime", "2017-5-3 14:56:06");
        v2.addEdge("call", v10086, "calltime", "2017-5-3 17:28:07");

        graph.tx().commit();
        Assert.assertEquals(8, graph.traversal().E().toList().size());

        List<Edge> edges = graph.traversal().V(v1).outE("call")
                                .has("calltime", "2017-5-3 12:08:02")
                                .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(Utils.date("2017-5-3 12:08:02"),
                            edges.get(0).value("calltime"));

        edges = graph.traversal().V(v1).outE("call")
                     .has("calltime", P.lt("2017-5-2"))
                     .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(Utils.date("2017-5-1 23:00:00"),
                            edges.get(0).value("calltime"));

        edges = graph.traversal().V(v1).outE("call")
                     .has("calltime", P.gte("2017-5-3"))
                     .toList();
        Assert.assertEquals(4, edges.size());
        Assert.assertEquals(Utils.date("2017-5-3 12:08:02"),
                            edges.get(0).value("calltime"));
        Assert.assertEquals(Utils.date("2017-5-3 14:56:06"),
                            edges.get(1).value("calltime"));
        Assert.assertEquals(Utils.date("2017-5-3 22:22:03"),
                            edges.get(2).value("calltime"));
        Assert.assertEquals(Utils.date("2017-5-4 20:33:04"),
                            edges.get(3).value("calltime"));

        edges = graph.traversal().V(v1).outE("call")
                     .has("calltime", P.gte("2017-5-3"))
                     .where(__.otherV().hasId(v2.id()))
                     .toList();
        Assert.assertEquals(3, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("calltime", P.between("2017-5-2", "2017-5-4"))
                     .toList();
        Assert.assertEquals(5, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("calltime", P.between("2017-5-2", "2017-5-4"))
                     .where(__.not(__.otherV().hasId((v10086.id()))))
                     .toList();
        Assert.assertEquals(3, edges.size());
    }

    @Test
    public void testQueryOutVerticesOfVertexWithSortkey() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex louise = vertex("person", "name", "Louise");

        Assert.assertEquals(4, graph.traversal().V(louise.id())
                                    .out("look").count().next().longValue());
        List<Vertex> vertices = graph.traversal().V(louise.id())
                                     .out("look").toList();
        // Expect duplicated vertex "java-1"
        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testQueryOutEdgesOfVertexBySortkeyWithPrefix() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("no").asText().create();
        schema.propertyKey("callType").asText().create();
        schema.propertyKey("calltime").asDate().create();
        schema.vertexLabel("phone")
              .properties("no")
              .primaryKeys("no")
              .enableLabelIndex(false)
              .create();
        schema.edgeLabel("call").multiTimes()
              .properties("callType", "calltime")
              .sourceLabel("phone").targetLabel("phone")
              .sortKeys("callType", "calltime")
              .create();

        Vertex v1 = graph.addVertex(T.label, "phone", "no", "13812345678");
        Vertex v2 = graph.addVertex(T.label, "phone", "no", "13866668888");
        Vertex v10086 = graph.addVertex(T.label, "phone", "no", "10086");

        v1.addEdge("call", v2, "callType", "work",
                   "calltime", "2017-5-1 23:00:00");
        v1.addEdge("call", v2, "callType", "work",
                   "calltime", "2017-5-2 12:00:01");
        v1.addEdge("call", v2, "callType", "work",
                   "calltime", "2017-5-3 12:08:02");
        v1.addEdge("call", v2, "callType", "fun",
                   "calltime", "2017-5-3 22:22:03");
        v1.addEdge("call", v2, "callType", "fun",
                   "calltime", "2017-5-4 20:33:04");

        v1.addEdge("call", v10086, "callType", "work",
                   "calltime", "2017-5-2 15:30:05");
        v1.addEdge("call", v10086, "callType", "work",
                   "calltime", "2017-5-3 14:56:06");
        v2.addEdge("call", v10086, "callType", "fun",
                   "calltime", "2017-5-3 17:28:07");

        graph.tx().commit();
        Assert.assertEquals(8, graph.traversal().E().toList().size());

        List<Edge> edges = graph.traversal().V(v1).outE("call")
                                .has("callType", "work")
                                .toList();
        Assert.assertEquals(5, edges.size());

        edges = graph.traversal().V(v1).outE("call").has("callType", "work")
                     .has("calltime", "2017-5-1 23:00:00")
                     .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(Utils.date("2017-5-1 23:00:00"),
                            edges.get(0).value("calltime"));

        edges = graph.traversal().V(v1).outE("call").has("callType", "work")
                     .has("calltime", P.lt("2017-5-2"))
                     .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(Utils.date("2017-5-1 23:00:00"),
                            edges.get(0).value("calltime"));

        edges = graph.traversal().V(v1).outE("call").has("callType", "work")
                     .has("calltime", P.gte("2017-5-2"))
                     .toList();
        Assert.assertEquals(4, edges.size());
        Assert.assertEquals(Utils.date("2017-5-2 12:00:01"),
                            edges.get(0).value("calltime"));
        Assert.assertEquals(Utils.date("2017-5-2 15:30:05"),
                            edges.get(1).value("calltime"));
        Assert.assertEquals(Utils.date("2017-5-3 12:08:02"),
                            edges.get(2).value("calltime"));
        Assert.assertEquals(Utils.date("2017-5-3 14:56:06"),
                            edges.get(3).value("calltime"));

        edges = graph.traversal().V(v1).outE("call").has("callType", "work")
                     .has("calltime", P.gte("2017-5-2"))
                     .where(__.otherV().hasId(v2.id()))
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V(v1).outE("call").has("callType", "work")
                     .has("calltime", P.between("2017-5-2", "2017-5-4"))
                     .toList();
        Assert.assertEquals(4, edges.size());

        edges = graph.traversal().V(v1).outE("call").has("callType", "work")
                     .has("calltime", P.between("2017-5-2", "2017-5-4"))
                     .where(__.not(__.otherV().hasId((v10086.id()))))
                     .toList();
        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testQueryOutEdgesOfVertexBySortkeyWithPrefixInPage() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());
        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("no").asText().create();
        schema.propertyKey("callType").asText().create();
        schema.propertyKey("calltime").asDate().create();
        schema.vertexLabel("phone")
              .properties("no")
              .primaryKeys("no")
              .enableLabelIndex(false)
              .create();
        schema.edgeLabel("call").multiTimes()
              .properties("callType", "calltime")
              .sourceLabel("phone").targetLabel("phone")
              .sortKeys("callType", "calltime")
              .create();

        Vertex v1 = graph.addVertex(T.label, "phone", "no", "13812345678");
        Vertex v2 = graph.addVertex(T.label, "phone", "no", "13866668888");
        Vertex v10086 = graph.addVertex(T.label, "phone", "no", "10086");

        v1.addEdge("call", v2, "callType", "work",
                   "calltime", "2017-5-1 23:00:00");
        v1.addEdge("call", v2, "callType", "work",
                   "calltime", "2017-5-2 12:00:01");
        v1.addEdge("call", v2, "callType", "work",
                   "calltime", "2017-5-3 12:08:02");
        v1.addEdge("call", v2, "callType", "fun",
                   "calltime", "2017-5-3 22:22:03");
        v1.addEdge("call", v2, "callType", "fun",
                   "calltime", "2017-5-4 20:33:04");

        v1.addEdge("call", v10086, "callType", "work",
                   "calltime", "2017-5-2 15:30:05");
        v1.addEdge("call", v10086, "callType", "work",
                   "calltime", "2017-5-3 14:56:06");
        v2.addEdge("call", v10086, "callType", "fun",
                   "calltime", "2017-5-3 17:28:07");

        graph.tx().commit();
        Assert.assertEquals(8, graph.traversal().E().toList().size());

        List<Edge> edges = graph.traversal().V(v1).outE("call")
                                .has("callType", "work")
                                .toList();
        Assert.assertEquals(5, edges.size());

        Assert.assertEquals(5, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("callType", "work")
                        .has("~page", page).limit(1);
        }));

        Assert.assertEquals(2, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("callType", "fun")
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call").has("callType", "work")
                     .has("calltime", "2017-5-1 23:00:00")
                     .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(Utils.date("2017-5-1 23:00:00"),
                            edges.get(0).value("calltime"));

        edges = graph.traversal().V(v1).outE("call").has("callType", "work")
                     .has("calltime", P.lt("2017-5-2"))
                     .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(Utils.date("2017-5-1 23:00:00"),
                            edges.get(0).value("calltime"));

        edges = graph.traversal().V(v1).outE("call").has("callType", "work")
                     .has("calltime", P.gte("2017-5-2"))
                     .toList();
        Assert.assertEquals(4, edges.size());
        Assert.assertEquals(Utils.date("2017-5-2 12:00:01"),
                            edges.get(0).value("calltime"));
        Assert.assertEquals(Utils.date("2017-5-2 15:30:05"),
                            edges.get(1).value("calltime"));
        Assert.assertEquals(Utils.date("2017-5-3 12:08:02"),
                            edges.get(2).value("calltime"));
        Assert.assertEquals(Utils.date("2017-5-3 14:56:06"),
                            edges.get(3).value("calltime"));

        Assert.assertEquals(4, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("callType", "work")
                        .has("calltime", P.gte("2017-5-2"))
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("callType", "work")
                     .has("calltime", P.between("2017-5-2", "2017-5-4"))
                     .toList();
        Assert.assertEquals(4, edges.size());

        Assert.assertEquals(4, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("callType", "work")
                        .has("calltime", P.between("2017-5-2", "2017-5-4"))
                        .has("~page", page).limit(1);
        }));
    }

    @Test
    public void testQueryOutEdgesOfVertexBySortkeyWithMoreFields() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("no").asText().create();
        schema.propertyKey("location").asText().create();
        schema.propertyKey("callType").asText().create();
        schema.propertyKey("calltime").asDate().create();
        schema.propertyKey("duration").asInt().create();
        schema.vertexLabel("phone")
              .properties("no")
              .primaryKeys("no")
              .enableLabelIndex(false)
              .create();
        schema.edgeLabel("call").multiTimes()
              .properties("location", "callType", "duration", "calltime")
              .sourceLabel("phone").targetLabel("phone")
              .sortKeys("location", "callType", "duration", "calltime")
              .create();

        Vertex v1 = graph.addVertex(T.label, "phone", "no", "13812345678");
        Vertex v2 = graph.addVertex(T.label, "phone", "no", "13866668888");
        Vertex v10086 = graph.addVertex(T.label, "phone", "no", "10086");

        v1.addEdge("call", v2, "location", "Beijing", "callType", "work",
                   "duration", 3, "calltime", "2017-5-1 23:00:00");
        v1.addEdge("call", v2, "location", "Beijing", "callType", "work",
                   "duration", 3, "calltime", "2017-5-2 12:00:01");
        v1.addEdge("call", v2, "location", "Beijing", "callType", "work",
                   "duration", 3, "calltime", "2017-5-3 12:08:02");
        v1.addEdge("call", v2, "location", "Beijing", "callType", "work",
                   "duration", 8, "calltime", "2017-5-3 22:22:03");
        v1.addEdge("call", v2, "location", "Beijing", "callType", "fun",
                   "duration", 10, "calltime", "2017-5-4 20:33:04");

        v1.addEdge("call", v10086, "location", "Nanjing", "callType", "work",
                   "duration", 12, "calltime", "2017-5-2 15:30:05");
        v1.addEdge("call", v10086, "location", "Nanjing", "callType", "work",
                   "duration", 14, "calltime", "2017-5-3 14:56:06");
        v2.addEdge("call", v10086, "location", "Nanjing", "callType", "fun",
                   "duration", 15, "calltime", "2017-5-3 17:28:07");

        graph.tx().commit();
        Assert.assertEquals(8, graph.traversal().E().toList().size());

        // Query by sortkey prefix "location"
        List<Edge> edges = graph.traversal().V(v1).outE("call")
                                .has("location", "Beijing")
                                .toList();
        Assert.assertEquals(5, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Nanjing")
                     .toList();
        Assert.assertEquals(2, edges.size());

        // Query by sortkey prefix "location", "callType"
        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .toList();
        Assert.assertEquals(4, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "fun")
                     .toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Nanjing")
                     .has("callType", "work")
                     .toList();
        Assert.assertEquals(2, edges.size());

        // Query by sortkey prefix "location", "callType", "duration"
        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .toList();
        Assert.assertEquals(3, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 8)
                     .toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "fun")
                     .has("duration", 10)
                     .toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Nanjing")
                     .has("callType", "work")
                     .has("duration", 12)
                     .toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Nanjing")
                     .has("callType", "work")
                     .has("duration", 14)
                     .toList();
        Assert.assertEquals(1, edges.size());

        // Query by sortkey prefix "location", "callType" and range "duration"
        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", P.lt(8))
                     .toList();
        Assert.assertEquals(3, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", P.lte(8))
                     .toList();
        Assert.assertEquals(4, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", P.gt(3))
                     .toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", P.gte(3))
                     .toList();
        Assert.assertEquals(4, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", P.between(3, 9))
                     .toList();
        Assert.assertEquals(4, edges.size());

        // Query by sortkey prefix "location", "callType", "duration",
        // "callTime"
        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", "2017-5-1 23:00:00")
                     .toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", "2017-5-2 12:00:01")
                     .toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", "2017-5-3 12:08:02")
                     .toList();
        Assert.assertEquals(1, edges.size());

        // Query by sortkey prefix "location", "callType", "duration" and
        // range "callTime"
        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", P.lt("2017-5-2 12:00:01"))
                     .toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", P.lte("2017-5-2 12:00:01"))
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", P.gt("2017-5-2 12:00:01"))
                     .toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", P.gte("2017-5-2 12:00:01"))
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", P.between("2017-5-2", "2017-5-4"))
                     .toList();
        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testQueryOutEdgesOfVertexBySortkeyWithMoreFieldsInPage() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());
        HugeGraph graph = graph();

        SchemaManager schema = graph.schema();
        schema.propertyKey("no").asText().create();
        schema.propertyKey("location").asText().create();
        schema.propertyKey("callType").asText().create();
        schema.propertyKey("calltime").asDate().create();
        schema.propertyKey("duration").asInt().create();
        schema.vertexLabel("phone")
              .properties("no")
              .primaryKeys("no")
              .enableLabelIndex(false)
              .create();
        schema.edgeLabel("call").multiTimes()
              .properties("location", "callType", "duration", "calltime")
              .sourceLabel("phone").targetLabel("phone")
              .sortKeys("location", "callType", "duration", "calltime")
              .create();

        Vertex v1 = graph.addVertex(T.label, "phone", "no", "13812345678");
        Vertex v2 = graph.addVertex(T.label, "phone", "no", "13866668888");
        Vertex v10086 = graph.addVertex(T.label, "phone", "no", "10086");

        v1.addEdge("call", v2, "location", "Beijing", "callType", "work",
                   "duration", 3, "calltime", "2017-5-1 23:00:00");
        v1.addEdge("call", v2, "location", "Beijing", "callType", "work",
                   "duration", 3, "calltime", "2017-5-2 12:00:01");
        v1.addEdge("call", v2, "location", "Beijing", "callType", "work",
                   "duration", 3, "calltime", "2017-5-3 12:08:02");
        v1.addEdge("call", v2, "location", "Beijing", "callType", "work",
                   "duration", 8, "calltime", "2017-5-3 22:22:03");
        v1.addEdge("call", v2, "location", "Beijing", "callType", "fun",
                   "duration", 10, "calltime", "2017-5-4 20:33:04");

        v1.addEdge("call", v10086, "location", "Nanjing", "callType", "work",
                   "duration", 12, "calltime", "2017-5-2 15:30:05");
        v1.addEdge("call", v10086, "location", "Nanjing", "callType", "work",
                   "duration", 14, "calltime", "2017-5-3 14:56:06");
        v2.addEdge("call", v10086, "location", "Nanjing", "callType", "fun",
                   "duration", 15, "calltime", "2017-5-3 17:28:07");

        graph.tx().commit();
        Assert.assertEquals(8, graph.traversal().E().toList().size());

        // Query by sortkey prefix "location"
        List<Edge> edges = graph.traversal().V(v1).outE("call")
                                .has("location", "Beijing")
                                .toList();
        Assert.assertEquals(5, edges.size());

        Assert.assertEquals(5, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Nanjing")
                     .toList();
        Assert.assertEquals(2, edges.size());

        Assert.assertEquals(2, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Nanjing")
                        .has("~page", page).limit(1);
        }));

        // Query by sortkey prefix "location", "callType"
        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .toList();
        Assert.assertEquals(4, edges.size());

        Assert.assertEquals(4, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "fun")
                     .toList();
        Assert.assertEquals(1, edges.size());

        Assert.assertEquals(1, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "fun")
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Nanjing")
                     .has("callType", "work")
                     .toList();
        Assert.assertEquals(2, edges.size());

        Assert.assertEquals(2, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Nanjing")
                        .has("callType", "work")
                        .has("~page", page).limit(1);
        }));

        // Query by sortkey prefix "location", "callType", "duration"
        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .toList();
        Assert.assertEquals(3, edges.size());

        Assert.assertEquals(3, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", 3)
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 8)
                     .toList();
        Assert.assertEquals(1, edges.size());

        Assert.assertEquals(1, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", 8)
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "fun")
                     .has("duration", 10)
                     .toList();
        Assert.assertEquals(1, edges.size());

        Assert.assertEquals(1, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "fun")
                        .has("duration", 10)
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Nanjing")
                     .has("callType", "work")
                     .has("duration", 12)
                     .toList();
        Assert.assertEquals(1, edges.size());

        Assert.assertEquals(1, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Nanjing")
                        .has("callType", "work")
                        .has("duration", 12)
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Nanjing")
                     .has("callType", "work")
                     .has("duration", 14)
                     .toList();
        Assert.assertEquals(1, edges.size());

        Assert.assertEquals(1, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Nanjing")
                        .has("callType", "work")
                        .has("duration", 14)
                        .has("~page", page).limit(1);
        }));

        // Query by sortkey prefix "location", "callType" and range "duration"
        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", P.lt(8))
                     .toList();
        Assert.assertEquals(3, edges.size());

        Assert.assertEquals(3, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", P.lt(8))
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", P.lte(8))
                     .toList();
        Assert.assertEquals(4, edges.size());

        Assert.assertEquals(4, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", P.lte(8))
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", P.gt(3))
                     .toList();
        Assert.assertEquals(1, edges.size());

        Assert.assertEquals(1, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", P.gt(3))
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", P.gte(3))
                     .toList();
        Assert.assertEquals(4, edges.size());

        Assert.assertEquals(4, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", P.gte(3))
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", P.between(3, 9))
                     .toList();
        Assert.assertEquals(4, edges.size());

        Assert.assertEquals(4, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", P.between(3, 9))
                        .has("~page", page).limit(1);
        }));

        // Query by sortkey prefix "location", "callType", "duration",
        // "callTime"
        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", "2017-5-1 23:00:00")
                     .toList();
        Assert.assertEquals(1, edges.size());

        Assert.assertEquals(1, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", 3)
                        .has("calltime", "2017-5-1 23:00:00")
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", "2017-5-2 12:00:01")
                     .toList();
        Assert.assertEquals(1, edges.size());

        Assert.assertEquals(1, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", 3)
                        .has("calltime", "2017-5-2 12:00:01")
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", "2017-5-3 12:08:02")
                     .toList();
        Assert.assertEquals(1, edges.size());

        Assert.assertEquals(1, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", 3)
                        .has("calltime", "2017-5-3 12:08:02")
                        .has("~page", page).limit(1);
        }));

        // Query by sortkey prefix "location", "callType", "duration" and
        // range "callTime"
        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", P.lt("2017-5-2 12:00:01"))
                     .toList();
        Assert.assertEquals(1, edges.size());

        Assert.assertEquals(1, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", 3)
                        .has("calltime", P.lt("2017-5-2 12:00:01"))
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", P.lte("2017-5-2 12:00:01"))
                     .toList();
        Assert.assertEquals(2, edges.size());

        Assert.assertEquals(2, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", 3)
                        .has("calltime", P.lte("2017-5-2 12:00:01"))
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", P.gt("2017-5-2 12:00:01"))
                     .toList();
        Assert.assertEquals(1, edges.size());

        Assert.assertEquals(1, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", 3)
                        .has("calltime", P.gt("2017-5-2 12:00:01"))
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", P.gte("2017-5-2 12:00:01"))
                     .toList();
        Assert.assertEquals(2, edges.size());

        Assert.assertEquals(2, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", 3)
                        .has("calltime", P.gte("2017-5-2 12:00:01"))
                        .has("~page", page).limit(1);
        }));

        edges = graph.traversal().V(v1).outE("call")
                     .has("location", "Beijing")
                     .has("callType", "work")
                     .has("duration", 3)
                     .has("calltime", P.between("2017-5-2", "2017-5-4"))
                     .toList();
        Assert.assertEquals(2, edges.size());

        Assert.assertEquals(2, traverseInPage(page -> {
            return graph.traversal().V(v1).outE("call")
                        .has("location", "Beijing")
                        .has("callType", "work")
                        .has("duration", 3)
                        .has("calltime", P.between("2017-5-2", "2017-5-4"))
                        .has("~page", page).limit(1);
        }));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            traverseInPage(page -> {
                // no location
                return graph.traversal().V(v1).outE("call")
                            .has("callType", "work")
                            .has("duration", 3)
                            .has("calltime", P.between("2017-5-2", "2017-5-4"))
                            .has("~page", page).limit(1);
            });
        }, e -> {
            Assert.assertContains("Can't query by paging and filtering",
                                  e.getMessage());
        });
    }

    @Test
    public void testQueryOutEdgesOfVertexByRangeFilter() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("authoredByScore").onE("authored")
             .range().by("score").create();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book1 = graph.addVertex(T.label, "book", "name", "Test-Book-1");
        Vertex book2 = graph.addVertex(T.label, "book", "name", "Test-Book-2");
        Vertex book3 = graph.addVertex(T.label, "book", "name", "Test-Book-3");

        james.addEdge("authored", book1,
                      "contribution", "1991 3 1", "score", 5);
        james.addEdge("authored", book2,
                      "contribution", "1992 2 2", "score", 4);
        james.addEdge("authored", book3,
                      "contribution", "1993 3 2", "score", 3);

        graph.tx().commit();

        // Won't query by search index, just filter by property after outE()
        List<Edge> edges = graph.traversal().V(james).outE("authored")
                                .has("score", P.gte(4)).toList();
        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testQueryInEdgesOfVertexWithResultN() {
        HugeGraph graph = graph();
        init18Edges();

        // Query IN edges of a vertex
        Vertex james = vertex("author", "id", 1);
        List<Edge> edges = graph.traversal().V(james.id()).inE().toList();

        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testQueryInEdgesOfVertexWithResult1() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex java = vertex("language", "name", "java");
        List<Edge> edges = graph.traversal().V(java.id()).inE().toList();

        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryInEdgesOfVertexWithResult0() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex guido = vertex("author", "id", 2);
        List<Edge> edges = graph.traversal().V(guido.id()).inE().toList();

        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testQueryInEdgesOfVertexByLabel() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex java3 = vertex("book", "name", "java-3");

        List<Edge> edges = graph.traversal().V(java3.id()).inE().toList();
        Assert.assertEquals(5, edges.size());

        edges = graph.traversal().V(java3.id()).inE("look").toList();
        Assert.assertEquals(4, edges.size());
    }

    @Test
    public void testQueryInEdgesOfVertexByLabelAndFilter() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex java3 = vertex("book", "name", "java-3");

        List<Edge> edges = graph.traversal().V(java3.id())
                                .inE().has("score", 3).toList();
        Assert.assertEquals(3, edges.size());

        edges = graph.traversal().V(java3.id())
                     .inE("look").has("score", 3).toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().V(java3.id())
                     .inE("look").has("score", 4).toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(java3.id())
                     .inE("look").has("score", 0).toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryInEdgesOfVertexByLabels() {
        HugeGraph graph = graph();
        init18Edges();

        long size;
        size = graph.traversal().V().outE("created", "authored", "look")
                    .hasLabel("authored", "created")
                    .count().next();
        Assert.assertEquals(5L, size);

        size = graph.traversal().V().outE("created", "authored", "look")
                    .hasLabel("authored", "friend")
                    .count().next();
        Assert.assertEquals(3L, size);

        size = graph.traversal().V().outE("created")
                    .hasLabel("authored", "created")
                    .count().next();
        Assert.assertEquals(2L, size);

        size = graph.traversal().V().inE("created", "authored", "look")
                    .has("score", 3)
                    .count().next();
        Assert.assertEquals(3L, size);
    }

    @Test
    public void testQueryInEdgesOfVertexByConflictingLabels() {
        HugeGraph graph = graph();
        init18Edges();

        long direct = graph.traversal().V().inE("created")
                           .hasLabel("created", "look")
                           .hasLabel("authored")
                           .count().next();
        Assert.assertEquals(0L, direct);

        long matched = graph.traversal().V()
                            .match(__.as("start1")
                                     .inE("created")
                                     .as("m1"))
                            .select("m1")
                            .hasLabel("created", "look")
                            .hasLabel("authored")
                            .count().next();
        Assert.assertEquals(0L, matched);
        Assert.assertEquals(matched, direct);
    }

    @Test
    public void testQueryInEdgesOfVertexBySortkey() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex java3 = vertex("book", "name", "java-3");

        List<Edge> edges = graph.traversal().V(java3.id())
                                .inE("look").toList();
        Assert.assertEquals(4, edges.size());

        edges = graph.traversal().V(java3.id())
                     .inE("look").has("time", "2017-5-27").toList();
        Assert.assertEquals(3, edges.size());
    }

    @Test
    public void testQueryInVerticesOfVertex() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex java3 = vertex("book", "name", "java-3");

        List<Vertex> vertices = graph.traversal().V(java3.id())
                                     .in("look").toList();
        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testQueryInVerticesOfVertexAndFilter() {
        HugeGraph graph = graph();
        init18Edges();

        Vertex java3 = vertex("book", "name", "java-3");

        // NOTE: the has() just filter by vertex props
        List<Vertex> vertices = graph.traversal().V(java3.id())
                                     .in("look").has("age", P.gt(22))
                                     .toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testQueryByLongPropOfOverrideEdge() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        louise.addEdge("strike", sean, "id", 1, "timestamp", current,
                       "place", "park", "tool", "shovel", "reason", "jeer",
                       "arrested", false);
        louise.addEdge("strike", sean, "id", 1, "timestamp", current + 1,
                       "place", "park", "tool", "shovel", "reason", "jeer",
                       "arrested", false);
        List<Edge> edges = graph.traversal().E().has("timestamp", current)
                                .toList();
        Assert.assertEquals(0, edges.size());
        edges = graph.traversal().E().has("timestamp", current + 1).toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryByNegativeLongProperty() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.indexLabel("transferByTimestamp").onE("transfer").range()
              .by("timestamp").create();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        louise.addEdge("transfer", sean, "id", 1,
                       "amount", 500.00F, "timestamp", 1L,
                       "message", "Happy birthday!");
        louise.addEdge("transfer", sean, "id", 2,
                       "amount", -1234.56F, "timestamp", -100L,
                       "message", "Happy birthday!");

        graph.tx().commit();

        List<Edge> edges = graph.traversal().E()
                                .has("timestamp", -100L).toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(IdGenerator.of(2), edges.get(0).value("id"));

        edges = graph.traversal().E()
                     .has("timestamp", P.between(-101L, 0L))
                     .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(IdGenerator.of(2), edges.get(0).value("id"));

        edges = graph.traversal().E().has("timestamp", P.gt(-101L)).toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E().has("timestamp", P.gte(-100L)).toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E().has("timestamp", P.gt(-100L)).toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(IdGenerator.of(1), edges.get(0).value("id"));

        edges = graph.traversal().E().has("timestamp", P.gt(-99L)).toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(IdGenerator.of(1), edges.get(0).value("id"));

        edges = graph.traversal().E().has("timestamp", P.lt(-100L)).toList();
        Assert.assertEquals(0, edges.size());
        edges = graph.traversal().E().has("timestamp", P.lte(-100L)).toList();
        Assert.assertEquals(1, edges.size());
        edges = graph.traversal().E().has("timestamp", P.lt(0L)).toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryByNegativeFloatProperty() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.indexLabel("transferByAmount").onE("transfer").range()
              .by("amount").create();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        louise.addEdge("transfer", sean, "id", 1,
                       "amount", 500.00F, "timestamp", 1L,
                       "message", "Happy birthday!");
        louise.addEdge("transfer", sean, "id", 2,
                       "amount", -1234.56F, "timestamp", -100L,
                       "message", "Happy birthday!");

        graph.tx().commit();

        List<Edge> edges = graph.traversal().E()
                                .has("amount", -1234.56F).toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(IdGenerator.of(2), edges.get(0).value("id"));

        edges = graph.traversal().E()
                     .has("amount", P.between(-1235F, 0L))
                     .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(IdGenerator.of(2), edges.get(0).value("id"));

        edges = graph.traversal().E().has("amount", P.gt(-1235F)).toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E().has("amount", P.gte(-1234.56F)).toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E().has("amount", P.gt(-1234.56F)).toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(IdGenerator.of(1), edges.get(0).value("id"));

        edges = graph.traversal().E().has("amount", P.gt(-1234.56F)).toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(IdGenerator.of(1), edges.get(0).value("id"));

        edges = graph.traversal().E().has("amount", P.lt(-1234.56F)).toList();
        Assert.assertEquals(0, edges.size());
        edges = graph.traversal().E().has("amount", P.lte(-1234.56F)).toList();
        Assert.assertEquals(1, edges.size());
        edges = graph.traversal().E().has("amount", P.lt(0F)).toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryByDateProperty() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.edgeLabel("buy")
              .properties("place", "date")
              .link("person", "book")
              .create();
        schema.indexLabel("buyByDate").onE("buy").by("date").range().create();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                                      "city", "Beijing", "age", 22);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        Date[] dates = new Date[]{
                Utils.date("2012-01-01 00:00:00.000"),
                Utils.date("2013-01-01 00:00:00.000"),
                Utils.date("2014-01-01 00:00:00.000")
        };

        louise.addEdge("buy", java1, "place", "haidian", "date", dates[0]);
        jeff.addEdge("buy", java2, "place", "chaoyang", "date", dates[1]);
        sean.addEdge("buy", java3, "place", "chaoyang", "date", dates[2]);

        List<Edge> edges = graph.traversal().E().hasLabel("buy")
                                .has("date", dates[0])
                                .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(dates[0], edges.get(0).value("date"));

        edges = graph.traversal().E().hasLabel("buy")
                     .has("date", P.gt(dates[0]))
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E().hasLabel("buy")
                     .has("date", P.between(dates[1], dates[2]))
                     .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(dates[1], edges.get(0).value("date"));
    }

    @Test
    public void testQueryByDatePropertyInString() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.edgeLabel("buy")
              .properties("place", "date")
              .link("person", "book")
              .create();
        schema.indexLabel("buyByDate").onE("buy").by("date").range().create();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                                      "city", "Beijing", "age", 22);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        String[] dates = new String[]{
                "2012-01-01 00:00:00.000",
                "2013-01-01 00:00:00.000",
                "2014-01-01 00:00:00.000"
        };

        louise.addEdge("buy", java1, "place", "haidian", "date", dates[0]);
        jeff.addEdge("buy", java2, "place", "chaoyang", "date", dates[1]);
        sean.addEdge("buy", java3, "place", "chaoyang", "date", dates[2]);

        List<Edge> edges = graph.traversal().E().hasLabel("buy")
                                .has("date", dates[0])
                                .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(Utils.date(dates[0]), edges.get(0).value("date"));

        edges = graph.traversal().E().hasLabel("buy")
                     .has("date", P.gt(dates[0]))
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E().hasLabel("buy")
                     .has("date", P.between(dates[1], dates[2]))
                     .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(Utils.date(dates[1]), edges.get(0).value("date"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testQueryByUnionHasDate() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        GraphTraversalSource g = graph.traversal();

        schema.edgeLabel("buy")
              .properties("place", "date")
              .link("person", "book")
              .create();
        schema.indexLabel("buyByDate").onE("buy").by("date").range().create();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                                      "city", "Beijing", "age", 22);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        String[] dates = new String[]{
                "2012-01-01 00:00:00.000",
                "2013-01-01 00:00:00.000",
                "2014-01-01 00:00:00.000"
        };

        louise.addEdge("buy", java1, "place", "haidian", "date", dates[0]);
        jeff.addEdge("buy", java2, "place", "chaoyang", "date", dates[1]);
        sean.addEdge("buy", java3, "place", "chaoyang", "date", dates[2]);

        List<Edge> edges = g.E()
                            .hasLabel("buy")
                            .union(__.<Edge>has("date", dates[0]))
                            .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(Utils.date(dates[0]), edges.get(0).value("date"));

        edges = g.E()
                 .hasLabel("buy")
                 .union(__.<Edge>has("date", P.gt(dates[0])))
                 .toList();
        Assert.assertEquals(2, edges.size());

        edges = g.E()
                 .hasLabel("buy")
                 .union(__.<Edge>has("date", P.lt(dates[1])),
                        __.<Edge>has("date", P.gt(dates[1])))
                 .toList();
        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testQueryByDatePropertyInMultiFormatString() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.edgeLabel("buy")
              .properties("place", "date")
              .link("person", "book")
              .create();
        schema.indexLabel("buyByDate").onE("buy").by("date").range().create();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                                      "city", "Beijing", "age", 22);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        String[] dates = new String[]{
                "2012-01-01",
                "2013-01-01 00:00:00",
                "2014-01-01 00:00:00.000"
        };

        louise.addEdge("buy", java1, "place", "haidian", "date", dates[0]);
        jeff.addEdge("buy", java2, "place", "chaoyang", "date", dates[1]);
        sean.addEdge("buy", java3, "place", "chaoyang", "date", dates[2]);

        List<Edge> edges = graph.traversal().E().hasLabel("buy")
                                .has("date", dates[0])
                                .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(Utils.date(dates[0]), edges.get(0).value("date"));

        edges = graph.traversal().E().hasLabel("buy")
                     .has("date", P.gt(dates[0]))
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E().hasLabel("buy")
                     .has("date", P.between(dates[1], dates[2]))
                     .toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(Utils.date(dates[1]), edges.get(0).value("date"));
    }

    @Test
    public void testQueryByOutEWithDateProperty() {
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.edgeLabel("buy")
              .properties("place", "date")
              .link("person", "book")
              .create();
        schema.indexLabel("buyByDate").onE("buy").by("date").range().create();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                                      "city", "Beijing", "age", 22);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        String[] dates = new String[]{
                "2012-01-01 00:00:00.000",
                "2013-01-01 00:00:00.000",
                "2014-01-01 00:00:00.000"
        };

        louise.addEdge("buy", java1, "place", "haidian", "date", dates[0]);
        jeff.addEdge("buy", java2, "place", "chaoyang", "date", dates[1]);
        sean.addEdge("buy", java3, "place", "chaoyang", "date", dates[2]);

        List<Edge> edges = graph.traversal().V().outE()
                                .has("date", P.between(dates[0], dates[2]))
                                .toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertEquals(Utils.date(dates[0]), edges.get(0).value("date"));
        Assert.assertEquals(Utils.date(dates[1]), edges.get(1).value("date"));
    }

    @Test
    public void testQueryByTextContainsProperty() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("authoredByContribution").onE("authored")
             .search().by("contribution").create();
        graph.schema().indexLabel("authoredByScore").onE("authored")
             .range().by("score").create();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book1 = graph.addVertex(T.label, "book", "name", "Test-Book-1");
        Vertex book2 = graph.addVertex(T.label, "book", "name", "Test-Book-2");
        Vertex book3 = graph.addVertex(T.label, "book", "name", "Test-Book-3");

        james.addEdge("authored", book1,
                      "contribution", "1991 3 1", "score", 5);
        james.addEdge("authored", book2,
                      "contribution", "1992 2 2", "score", 4);
        james.addEdge("authored", book3,
                      "contribution", "1993 3 2", "score", 3);

        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().hasLabel("authored")
                                .has("contribution", Text.contains("1992"))
                                .toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "authored", james, book2,
                       "contribution", "1992 2 2", "score", 4);

        edges = graph.traversal().E().hasLabel("authored")
                     .has("contribution", Text.contains("2"))
                     .toList();
        Assert.assertEquals(2, edges.size());

        edges = graph.traversal().E().hasLabel("authored")
                     .has("score", P.gt(3))
                     .has("contribution", Text.contains("3"))
                     .toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "authored", james, book1,
                       "contribution", "1991 3 1", "score", 5);
    }

    @Test
    public void testQueryByTextContainsPropertyWithOutEdgeFilter() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("authoredByContribution").onE("authored")
             .search().by("contribution").create();
        graph.schema().indexLabel("authoredByScore").onE("authored")
             .range().by("score").create();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");

        Vertex book1 = graph.addVertex(T.label, "book", "name", "Test-Book-1");
        Vertex book2 = graph.addVertex(T.label, "book", "name", "Test-Book-2");
        Vertex book3 = graph.addVertex(T.label, "book", "name", "Test-Book-3");

        james.addEdge("authored", book1,
                      "contribution", "1991 3 1", "score", 5);
        james.addEdge("authored", book2,
                      "contribution", "1992 2 2", "score", 4);
        james.addEdge("authored", book3,
                      "contribution", "1993 3 2", "score", 3);

        graph.tx().commit();

        // Won't query by search index, just filter by property after outE()
        List<Edge> edges = graph.traversal().V(james).outE("authored")
                                .has("score", P.gte(4)).toList();
        Assert.assertEquals(2, edges.size());

        // Won't query by search index, just filter by property after outE()
        edges = graph.traversal().V(james).outE("authored")
                     .has("contribution", "1992 2 2")
                     .toList();
        Assert.assertEquals(1, edges.size());

        // Won't query by search index, just filter by property after outE()
        edges = graph.traversal().V(james).outE("authored")
                     .has("contribution", "1992 2 2")
                     .has("score", P.gte(4))
                     .toList();
        Assert.assertEquals(1, edges.size());

        // Won't query by search index, just filter by property after outE()
        edges = graph.traversal().V(james).outE("authored")
                     .has("contribution", "1992 2 2")
                     .has("score", P.gt(4))
                     .toList();
        Assert.assertEquals(0, edges.size());

        // Query by search index
        edges = graph.traversal().E()
                     .has("contribution", Text.contains("1992"))
                     .toList();
        Assert.assertEquals(1, edges.size());

        // Won't query by search index, just filter by property after outE()
        edges = graph.traversal().V(james).outE("authored")
                     .has("contribution", "1992")
                     .toList();
        Assert.assertEquals(0, edges.size()); // be careful!
    }

    @Test
    public void testScanEdge() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support scan",
                          storeFeatures().supportsScanToken() ||
                          storeFeatures().supportsScanKeyRange());
        init18Edges();

        Set<Edge> edges = new HashSet<>();

        long splitSize = 1L * 1024L * 1024L;
        List<Shard> splits = graph.metadata(HugeType.EDGE_OUT, "splits",
                                            splitSize);
        for (Shard split : splits) {
            ConditionQuery q = new ConditionQuery(HugeType.EDGE);
            q.scan(split.start(), split.end());
            edges.addAll(ImmutableList.copyOf(graph.edges(q)));
        }

        Assert.assertEquals(18, edges.size());
    }

    @Test
    public void testScanEdgeInPaging() {
        // FIXME: skip this test for hstore
        Assume.assumeTrue("skip this test for hstore",
                          Objects.equals("hstore", System.getProperty("backend")));

        HugeGraph graph = graph();
        Assume.assumeTrue("Not support scan",
                          storeFeatures().supportsScanToken() ||
                          storeFeatures().supportsScanKeyRange());
        init18Edges();

        List<Edge> edges = new LinkedList<>();

        ConditionQuery query = new ConditionQuery(HugeType.EDGE);

        String backend = graph.backend();
        if (backend.equals("cassandra") || backend.equals("scylladb")) {
            query.scan(String.valueOf(Long.MIN_VALUE),
                       String.valueOf(Long.MAX_VALUE));
        } else {
            // QUESTION: The query method may not be well adapted
            query.scan(BackendTable.ShardSplitter.START,
                       BackendTable.ShardSplitter.END);
        }

        query.limit(1);
        String page = PageInfo.PAGE_NONE;
        while (page != null) {
            query.page(page);
            Iterator<Edge> iterator = graph.edges(query);
            while (iterator.hasNext()) {
                edges.add(iterator.next());
            }
            page = PageInfo.pageInfo(iterator);
            CloseableIterator.closeIterator(iterator);
        }
        Assert.assertEquals(18, edges.size());
    }

    @Test
    public void testQueryBothEdgesOfVertexInPaging() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());
        init18Edges();
        Vertex james = graph.traversal().V().hasLabel("author")
                            .has("id", 1).next();
        int count = 0;
        String page = PageInfo.PAGE_NONE;
        while (page != null) {
            GraphTraversal<?, ?> iterator = graph.traversal().V(james).bothE()
                                                 .has("~page", page).limit(1);
            Long size = IteratorUtils.count(iterator);
            if (size == 0L) {
                // The last page is empty
                Assert.assertEquals(6, count);
            } else {
                Assert.assertEquals(1, size.intValue());
            }
            page = TraversalUtil.page(iterator);
            count += size.intValue();
        }
        Assert.assertEquals(6, count);
    }

    @Test
    public void testQueryOutEdgesOfVertexInPaging() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());
        init18Edges();
        Vertex james = graph.traversal().V().hasLabel("author")
                            .has("id", 1).next();
        int count = 0;
        String page = PageInfo.PAGE_NONE;
        while (page != null) {
            GraphTraversal<?, ?> iterator = graph.traversal().V(james).outE()
                                                 .has("~page", page).limit(1);
            Long size = IteratorUtils.count(iterator);
            if (size == 0L) {
                // The last page is empty
                Assert.assertEquals(4, count);
            } else {
                Assert.assertEquals(1, size.intValue());
            }
            page = TraversalUtil.page(iterator);
            count += size.intValue();
        }
        Assert.assertEquals(4, count);
    }

    @Test
    public void testQueryInEdgesOfVertexInPaging() {
        HugeGraph graph = graph();
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());
        init18Edges();
        Vertex james = graph.traversal().V().hasLabel("author")
                            .has("id", 1).next();
        int count = 0;
        String page = PageInfo.PAGE_NONE;
        while (page != null) {
            GraphTraversal<?, ?> iterator = graph.traversal().V(james).inE()
                                                 .has("~page", page).limit(1);
            Long size = IteratorUtils.count(iterator);
            if (size == 0L) {
                // The last page is empty
                Assert.assertEquals(2, count);
            } else {
                Assert.assertEquals(1, size.intValue());
            }
            page = TraversalUtil.page(iterator);
            count += size.intValue();
        }
        Assert.assertEquals(2, count);
    }

    @Test
    public void testQueryCount() {
        HugeGraph graph = graph();

        graph.schema().indexLabel("lookByTime").onE("look")
             .secondary().by("time").create();
        graph.schema().indexLabel("lookByScore").onE("look")
             .range().by("score").create();

        init18Edges();

        GraphTraversalSource g = graph.traversal();

        Assert.assertEquals(18L, g.E().count().next());

        Assert.assertEquals(2L, g.E().hasLabel("created").count().next());
        Assert.assertEquals(1L, g.E().hasLabel("know").count().next());
        Assert.assertEquals(3L, g.E().hasLabel("authored").count().next());
        Assert.assertEquals(7L, g.E().hasLabel("look").count().next());
        Assert.assertEquals(4L, g.E().hasLabel("friend").count().next());
        Assert.assertEquals(1L, g.E().hasLabel("follow").count().next());
        Assert.assertEquals(11L, g.E().hasLabel("look", "friend")
                                  .count().next());

        Assert.assertEquals(5L, g.E().hasLabel("look")
                                 .has("time", "2017-5-27").count().next());
        Assert.assertEquals(2L, g.E().hasLabel("look")
                                 .has("score", 3).count().next());
        Assert.assertEquals(3L, g.E().hasLabel("look")
                                 .has("score", P.gte(3)).count().next());
        Assert.assertEquals(1L, g.E().hasLabel("look")
                                 .has("score", P.lt(3)).count().next());

        Assert.assertEquals(1L, g.E().hasLabel("look")
                                 .has("time", "2017-5-27")
                                 .has("score", 3)
                                 .count().next());
        Assert.assertEquals(2L, g.E().hasLabel("look")
                                 .has("time", "2017-5-27")
                                 .has("score", P.gte(3))
                                 .count().next());
        Assert.assertEquals(2L, g.E().hasLabel("look")
                                 .has("time", "2017-5-27")
                                 .has("score", P.gt(0))
                                 .count().next());
        Assert.assertEquals(3L, g.E().hasLabel("look")
                                 .has("time", "2017-5-27")
                                 .has("score", P.gte(0))
                                 .count().next());
        Assert.assertEquals(0L, g.E().hasLabel("look")
                                 .has("time", "2017-5-27")
                                 .has("score", P.lt(0))
                                 .count().next());
        Assert.assertEquals(1L, g.E().hasLabel("look")
                                 .has("time", "2017-5-27")
                                 .has("score", P.lte(0))
                                 .count().next());
        Assert.assertEquals(2L, g.E().hasLabel("look")
                                 .has("time", "2017-5-27")
                                 .has("score", P.lte(3))
                                 .count().next());

        Assert.assertEquals(18L, g.E().count().min().next());
        Assert.assertEquals(7L, g.E().hasLabel("look").count().max().next());

        Assert.assertEquals(4L, g.E().hasLabel("look")
                                 .values("score").count().next());
        Assert.assertEquals(11L, g.E().hasLabel("look")
                                  .values().count().next());
    }

    @Test
    public void testQueryCountOfAdjacentEdges() {
        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        init18Edges();

        Vertex james = vertex("author", "id", 1);

        Assert.assertEquals(18L, g.E().count().next());

        Assert.assertEquals(6L, g.V(james).bothE().count().next());

        Assert.assertEquals(4L, g.V(james).outE().count().next());
        Assert.assertEquals(1L, g.V(james).outE("created").count().next());
        Assert.assertEquals(3L, g.V(james).outE("authored").count().next());
        Assert.assertEquals(1L, g.V(james).outE("authored")
                                 .has("score", 3).count().next());

        Assert.assertEquals(2L, g.V(james).inE().count().next());
        Assert.assertEquals(1L, g.V(james).inE("know").count().next());
        Assert.assertEquals(1L, g.V(james).inE("follow").count().next());
    }

    @Test
    public void testQueryCountAsCondition() {
        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();
        init18Edges();

        Vertex java1 = vertex("book", "name", "java-1");
        Vertex java3 = vertex("book", "name", "java-3");

        long paths;
        paths = g.V(java1)
                 .repeat(__.inE().outV().simplePath())
                 .until(__.or(__.inE().count().is(0), __.loops().is(3)))
                 .path().count().next();
        Assert.assertEquals(4L, paths);

        paths = g.V(java3)
                 .repeat(__.inE().outV().simplePath())
                 .until(__.or(__.inE().count().is(0), __.loops().is(3)))
                 .path().count().next();
        Assert.assertEquals(7L, paths);
    }

    @Test
    public void testRemoveEdge() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex java = graph.addVertex(T.label, "language", "name", "java");

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);

        Edge authored1 = james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3);

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(4, edges.size());

        authored1.remove();

        edges = graph.traversal().E().toList();
        Assert.assertEquals(3, edges.size());
        Assert.assertFalse(Utils.contains(edges,
                                          new FakeObjects.FakeEdge("authored", james, java1)));
    }

    @Test
    public void testRemoveEdgeById() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex java = graph.addVertex(T.label, "book", "name", "java");
        Edge write = james.addEdge("write", java, "time", "2017-6-7");
        Edge authored = james.addEdge("authored", java);
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(2, edges.size());

        // remove edge without label index
        graph.removeEdge(write.label(), write.id());
        graph.tx().commit();

        edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(authored, edges.get(0));

        // remove edge with label index
        graph.removeEdge(authored.label(), authored.id());
        graph.tx().commit();

        edges = graph.traversal().E().toList();
        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testRemoveEdgeNotExists() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Edge created = james.addEdge("created", java);
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());

        created.remove();
        graph.tx().commit();

        edges = graph.traversal().E().toList();
        Assert.assertEquals(0, edges.size());
        // Remove again
        created.remove();
    }

    @Test
    public void testRemoveEdgeOneByOne() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido = graph.addVertex(T.label, "author", "id", 2,
                                       "name", "Guido van Rossum", "age", 61,
                                       "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                                        "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3);

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(5, edges.size());

        for (int i = 0; i < edges.size(); i++) {
            edges.get(i).remove();
            Assert.assertEquals(4 - i, graph.traversal().E().toList().size());
        }
    }

    @Test
    public void testRemoveEdgesOfVertex() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido = graph.addVertex(T.label, "author", "id", 2,
                                       "name", "Guido van Rossum", "age", 61,
                                       "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                                        "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3);

        guido.addEdge("write", java1, "time", "2017-6-7");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(6, edges.size());

        // It will remove all edges of the vertex
        james.remove();

        edges = graph.traversal().E().toList();
        Assert.assertEquals(2, edges.size());
        assertContains(edges, "created", guido, python);
        assertContains(edges, "write", guido, java1,
                       "time", "2017-6-7");

        edges = graph.traversal().V(java1.id()).inE().toList();
        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().V(java2.id()).inE().toList();
        Assert.assertEquals(0, edges.size());

        edges = graph.traversal().V(java3.id()).inE().toList();
        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testRemoveEdgesOfSuperVertex() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido = graph.addVertex(T.label, "author", "id", 2,
                                       "name", "Guido van Rossum", "age", 61,
                                       "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                                        "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3);

        // Add some edges
        int txCap = TX_BATCH;
        for (int i = 0; i < txCap / TX_BATCH; i++) {
            for (int j = 0; j < TX_BATCH; j++) {
                int time = i * TX_BATCH + j;
                guido.addEdge("write", java1, "time", "time-" + time);
            }
            graph.tx().commit();
        }

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(txCap + 5, edges.size());

        // It will remove all edges of the vertex
        guido.remove();
        graph.tx().commit();

        edges = graph.traversal().E().toList();
        Assert.assertEquals(4, edges.size());
        assertContains(edges, "created", james, java);
        assertContains(edges, "authored", james, java1);
        assertContains(edges, "authored", james, java2);
        assertContains(edges, "authored", james, java3);

        // Add large amount of edges
        txCap = this.superNodeSize();
        assert txCap / TX_BATCH > 0 && txCap % TX_BATCH == 0;
        for (int i = 0; i < txCap / TX_BATCH; i++) {
            for (int j = 0; j < TX_BATCH; j++) {
                int time = i * TX_BATCH + j;
                guido.addEdge("write", java1, "time", "time-" + time);
            }
            graph.tx().commit();
        }

        guido.addEdge("created", python);

        edges = graph.traversal().E().toList();
        Assert.assertEquals(txCap + 5, edges.size());

        int old = Whitebox.getInternalState(params().graphTransaction(),
                                            "commitPartOfAdjacentEdges");
        Whitebox.setInternalState(params().graphTransaction(),
                                  "commitPartOfAdjacentEdges", 0);
        try {
            // It will try to remove all edges of the vertex, but with error
            guido.remove();

            Assert.assertThrows(LimitExceedException.class, () -> {
                graph.tx().commit();
            }, e -> {
                Assert.assertContains("Edges size has reached tx capacity",
                                      e.getMessage());
                graph.tx().rollback();
            });
        } finally {
            Whitebox.setInternalState(params().graphTransaction(),
                                      "commitPartOfAdjacentEdges", old);
        }

        // It will remove all edges of the vertex
        guido.remove();

        // Clear all
        try {
            graph.truncateBackend();
        } catch (UnsupportedOperationException e) {
            LOG.warn("Failed to truncate backend", e);
        }
    }

    @Test
    public void testRemoveEdgeAfterAddEdgeWithTx() {
        HugeGraph graph = graph();
        GraphTransaction tx = params().openTransaction();

        Vertex james = tx.addVertex(T.label, "author", "id", 1,
                                    "name", "James Gosling", "age", 62,
                                    "lived", "Canadian");

        Vertex java = tx.addVertex(T.label, "language", "name", "java");

        Edge created = james.addEdge("created", java);
        created.remove();

        try {
            tx.commit();
        } finally {
            tx.close();
        }

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testRemoveVertexAfterAddEdgesWithTx() {
        HugeGraph graph = graph();
        GraphTransaction tx = params().openTransaction();

        Vertex james = tx.addVertex(T.label, "author", "id", 1,
                                    "name", "James Gosling", "age", 62,
                                    "lived", "Canadian");
        Vertex guido = tx.addVertex(T.label, "author", "id", 2,
                                    "name", "Guido van Rossum", "age", 61,
                                    "lived", "California");

        Vertex java = tx.addVertex(T.label, "language", "name", "java");
        Vertex python = tx.addVertex(T.label, "language", "name", "python",
                                     "dynamic", true);

        Vertex java1 = tx.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = tx.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = tx.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3);

        james.remove();
        guido.remove();

        try {
            tx.commit();
        } finally {
            tx.close();
        }

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testAddEdgeProperty() {
        HugeGraph graph = graph();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("transfer", sean, "id", 1,
                                   "amount", 500.00F, "timestamp", current);
        graph.tx().commit();

        // Add property
        edge.property("message", "Happy birthday!");
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "transfer", louise, sean, "id", 1,
                       "amount", 500.00F, "timestamp", current,
                       "message", "Happy birthday!");
    }

    @Test
    public void testAddEdgePropertyNotInEdgeLabel() {
        Edge edge = initEdgeTransfer();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("age", 18);
        });
    }

    @Test
    public void testAddEdgePropertyWithNotExistPropKey() {
        Edge edge = initEdgeTransfer();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("prop-not-exist", "2017-1-1");
        });
    }

    @Test
    public void testAddEdgePropertyWithSpecialValueForSecondaryIndex() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        graph.tx().commit();

        long current = System.currentTimeMillis();
        louise.addEdge("strike", sean, "id", 1,
                       "timestamp", current, "place", "park",
                       "tool", "b\u0001", "reason", "jeer",
                       "arrested", false);
        louise.addEdge("strike", sean, "id", 2,
                       "timestamp", current, "place", "park",
                       "tool", "c\u0002", "reason", "jeer",
                       "arrested", false);
        louise.addEdge("strike", sean, "id", 3,
                       "timestamp", current, "place", "park",
                       "tool", "d\u0003", "reason", "jeer",
                       "arrested", false);
        graph.tx().commit();

        List<Edge> edges;

        edges = graph.traversal().E().has("tool", "b\u0001").toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(1, edges.get(0).value("id"));

        edges = graph.traversal().E().has("tool", "c\u0002").toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(2, edges.get(0).value("id"));

        edges = graph.traversal().E().has("tool", "d\u0003").toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(3, edges.get(0).value("id"));

        String backend = graph.backend();
        Set<String> nonZeroBackends = ImmutableSet.of("postgresql",
                                                      "rocksdb", "hbase", "hstore");
        if (nonZeroBackends.contains(backend)) {
            Assert.assertThrows(Exception.class, () -> {
                louise.addEdge("strike", sean, "id", 4,
                               "timestamp", current, "place", "park",
                               "tool", "a\u0000", "reason", "jeer",
                               "arrested", false);
                graph.tx().commit();
            }, e -> {
                if (e instanceof BackendException) {
                    Assert.assertContains("0x00", e.getCause().getMessage());
                } else {
                    Assert.assertContains("0x00", e.getMessage());
                }
            });
        } else {
            louise.addEdge("strike", sean, "id", 0,
                           "timestamp", current, "place", "park",
                           "tool", "a\u0000", "reason", "jeer",
                           "arrested", false);
            graph.tx().commit();

            edges = graph.traversal().E().has("tool", "a\u0000").toList();
            Assert.assertEquals(1, edges.size());
            Assert.assertEquals(0, edges.get(0).value("id"));
        }
    }

    @Test
    public void testAddEdgePropertyWithIllegalValueForSecondaryIndex() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        graph.tx().commit();

        long current = System.currentTimeMillis();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            louise.addEdge("strike", sean, "id", 4,
                           "timestamp", current, "place", "park",
                           "tool", "\u0000", "reason", "jeer",
                           "arrested", false);
            graph.tx().commit();
        }, e -> {
            Assert.assertContains("Illegal leading char '\\u0' " +
                                  "in index property:",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            louise.addEdge("strike", sean, "id", 4,
                           "timestamp", current, "place", "park",
                           "tool", "\u0001", "reason", "jeer",
                           "arrested", false);
            graph.tx().commit();
        }, e -> {
            Assert.assertContains("Illegal leading char '\\u1' in index",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            louise.addEdge("strike", sean, "id", 4,
                           "timestamp", current, "place", "park",
                           "tool", "\u0002", "reason", "jeer",
                           "arrested", false);
            graph.tx().commit();
        }, e -> {
            Assert.assertContains("Illegal leading char '\\u2' in index",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            louise.addEdge("strike", sean, "id", 4,
                           "timestamp", current, "place", "park",
                           "tool", "\u0003", "reason", "jeer",
                           "arrested", false);
            graph.tx().commit();
        }, e -> {
            Assert.assertContains("Illegal leading char '\\u3' in index",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            louise.addEdge("strike", sean, "id", 4,
                           "timestamp", current, "place", "park",
                           "tool", "\u0000a", "reason", "jeer",
                           "arrested", false);
            graph.tx().commit();
        }, e -> {
            Assert.assertContains("Illegal leading char '\\u0' in index",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            louise.addEdge("strike", sean, "id", 4,
                           "timestamp", current, "place", "park",
                           "tool", "\u0001a", "reason", "jeer",
                           "arrested", false);
            graph.tx().commit();
        }, e -> {
            Assert.assertContains("Illegal leading char '\\u1' in index",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            louise.addEdge("strike", sean, "id", 4,
                           "timestamp", current, "place", "park",
                           "tool", "\u0002a", "reason", "jeer",
                           "arrested", false);
            graph.tx().commit();
        }, e -> {
            Assert.assertContains("Illegal leading char '\\u2' in index",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            louise.addEdge("strike", sean, "id", 4,
                           "timestamp", current, "place", "park",
                           "tool", "\u0003a", "reason", "jeer",
                           "arrested", false);
            graph.tx().commit();
        }, e -> {
            Assert.assertContains("Illegal leading char '\\u3' in index",
                                  e.getMessage());
        });
    }

    @Test
    public void testUpdateEdgeProperty() {
        HugeGraph graph = graph();

        Edge edge = initEdgeTransfer();
        Assert.assertEquals(500.00F, edge.property("amount").value());

        // Update property
        edge.property("amount", 200.00F);
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        edge = edges.get(0);
        Assert.assertEquals(200.00F, edge.property("amount").value());
    }

    @Test
    public void testUpdateEdgePropertyWithRemoveAndSet() {
        HugeGraph graph = graph();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("transfer", sean, "id", 1,
                                   "amount", 500.00F, "timestamp", current,
                                   "message", "Happy birthday!");
        graph.tx().commit();

        edge.property("message").remove();
        edge.property("message", "Happy birthday ^-^");
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "transfer", louise, sean, "id", 1,
                       "amount", 500.00F, "timestamp", current,
                       "message", "Happy birthday ^-^");
    }

    @Test
    public void testUpdateEdgePropertyTwice() {
        HugeGraph graph = graph();

        Edge edge = initEdgeTransfer();
        Assert.assertEquals(500.00F, edge.property("amount").value());

        edge.property("amount", 100.00F);
        edge.property("amount", 200.00F);
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        edge = edges.get(0);
        Assert.assertEquals(200.00F, edge.property("amount").value());
    }

    @Test
    public void testUpdateEdgePropertyOfSortKey() {
        Edge edge = initEdgeTransfer();

        Assert.assertEquals(1, edge.property("id").value());
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Update sort key property
            edge.property("id", 2);
        });
    }

    @Test
    public void testUpdateEdgePropertyOfNewEdge() {
        HugeGraph graph = graph();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("transfer", sean, "id", 1,
                                   "amount", 500.00F, "timestamp", current,
                                   "message", "Happy birthday!");

        edge.property("amount", 200.00F);
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "transfer", louise, sean, "id", 1,
                       "amount", 200.00F, "timestamp", current,
                       "message", "Happy birthday!");
    }

    @Test
    public void testUpdateEdgePropertyOfAddingEdge() {
        Edge edge = initEdgeTransfer();

        Vertex louise = vertex("person", "name", "Louise");
        Vertex sean = vertex("person", "name", "Sean");

        louise.addEdge("transfer", sean, "id", 1, "amount", 500.00F,
                       "timestamp", edge.value("timestamp"),
                       "message", "Happy birthday!");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("message").remove();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("message", "*");
        });
    }

    @Test
    public void testUpdateEdgePropertyOfRemovingEdge() {
        Edge edge = initEdgeTransfer();

        edge.remove();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("message").remove();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("message", "*");
        });
    }

    @Test
    public void testUpdateEdgePropertyOfRemovingEdgeWithDrop() {
        HugeGraph graph = graph();
        Edge edge = initEdgeTransfer();

        graph.traversal().E(edge.id()).drop().iterate();

        // Update on dirty vertex
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("message").remove();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("message", "*");
        });
    }

    @Test
    public void testUpdatePropertyToValueOfRemovedEdgeWithUniqueIndex() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("weight").asDouble().ifNotExist().create();

        schema.vertexLabel("user")
              .properties("name")
              .primaryKeys("name")
              .ifNotExist()
              .create();

        schema.edgeLabel("like")
              .sourceLabel("user")
              .targetLabel("user")
              .properties("weight")
              .ifNotExist()
              .create();

        schema.indexLabel("likeByWeight")
              .onE("like")
              .by("weight")
              .unique()
              .ifNotExist()
              .create();

        Vertex marko = graph().addVertex(T.label, "user", "name", "marko");
        Vertex vadas = graph().addVertex(T.label, "user", "name", "vadas");
        Vertex josh = graph().addVertex(T.label, "user", "name", "josh");

        Edge edge1 = marko.addEdge("like", vadas, "weight", 0.5);
        Edge edge2 = marko.addEdge("like", josh, "weight", 0.8);
        Edge edge3 = vadas.addEdge("like", josh, "weight", 1.0);

        graph().tx().commit();

        edge1.remove();
        graph().tx().commit();
        edge2.property("weight", 0.5);
        graph().tx().commit();

        edge3.remove();
        edge2.property("weight", 1.0);
        graph().tx().commit();
    }

    @Test
    public void testQueryEdgeByUniqueIndex() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("weight").asDouble().ifNotExist().create();
        schema.vertexLabel("user")
              .properties("name")
              .primaryKeys("name")
              .ifNotExist()
              .create();
        schema.edgeLabel("like")
              .sourceLabel("user")
              .targetLabel("user")
              .properties("weight")
              .ifNotExist()
              .create();
        schema.indexLabel("likeByWeight")
              .onE("like")
              .by("weight")
              .unique()
              .ifNotExist()
              .create();

        Vertex marko = graph().addVertex(T.label, "user", "name", "marko");
        Vertex vadas = graph().addVertex(T.label, "user", "name", "vadas");
        marko.addEdge("like", vadas, "weight", 0.5);
        graph().tx().commit();

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().E().hasLabel("like").has("weight", 0.5).next();
        }, e -> {
            Assert.assertEquals("Don't accept query based on properties " +
                                "[weight] that are not indexed in label " +
                                "'like', may not match secondary condition",
                                e.getMessage());
        });
    }

    @Test
    public void testUpdateEdgePropertyOfAggregateType() {
        Assume.assumeTrue("Not support aggregate property",
                          storeFeatures().supportsAggregateProperty());

        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.propertyKey("startTime")
              .asDate().valueSingle().calcMin()
              .ifNotExist().create();
        schema.propertyKey("endTime")
              .asDate().valueSingle().calcMax()
              .ifNotExist().create();
        schema.propertyKey("times")
              .asLong().valueSingle().calcSum()
              .ifNotExist().create();
        schema.propertyKey("port")
              .asInt().valueSet().calcSet()
              .ifNotExist().create();
        schema.propertyKey("type")
              .asInt().valueList().calcList()
              .ifNotExist().create();

        schema.vertexLabel("ip").useCustomizeStringId().ifNotExist().create();

        schema.edgeLabel("attack").sourceLabel("ip").targetLabel("ip")
              .properties("startTime", "endTime", "times", "port", "type")
              .ifNotExist().create();

        Vertex ip1 = graph.addVertex(T.label, "ip", T.id, "10.0.0.1");
        Vertex ip2 = graph.addVertex(T.label, "ip", T.id, "10.0.0.2");

        ip1.addEdge("attack", ip2,
                    "startTime", "2019-1-1 00:00:30",
                    "endTime", "2019-1-1 00:01:00",
                    "times", 3, "port", 21, "type", 21);
        graph.tx().commit();

        Edge edge = graph.traversal().V("10.0.0.1").outE().next();
        Assert.assertEquals(Utils.date("2019-1-1 00:00:30"),
                            edge.value("startTime"));
        Assert.assertEquals(Utils.date("2019-1-1 00:01:00"),
                            edge.value("endTime"));
        Assert.assertEquals(3L, edge.value("times"));
        Assert.assertEquals(ImmutableSet.of(21), edge.value("port"));
        Assert.assertEquals(ImmutableList.of(21), edge.value("type"));

        edge = graph.traversal().V("10.0.0.2").inE().next();
        Assert.assertEquals(Utils.date("2019-1-1 00:00:30"),
                            edge.value("startTime"));
        Assert.assertEquals(Utils.date("2019-1-1 00:01:00"),
                            edge.value("endTime"));
        Assert.assertEquals(3L, edge.value("times"));
        Assert.assertEquals(ImmutableSet.of(21), edge.value("port"));
        Assert.assertEquals(ImmutableList.of(21), edge.value("type"));

        edge.property("startTime", "2019-1-1 00:04:00");
        edge.property("endTime", "2019-1-1 00:08:00");
        edge.property("times", 10);
        edge.property("port", 22);
        edge.property("type", 22);
        graph.tx().commit();

        edge = graph.traversal().V("10.0.0.1").outE().next();
        Assert.assertEquals(Utils.date("2019-1-1 00:00:30"),
                            edge.value("startTime"));
        Assert.assertEquals(Utils.date("2019-1-1 00:08:00"),
                            edge.value("endTime"));
        Assert.assertEquals(13L, edge.value("times"));
        Assert.assertEquals(ImmutableSet.of(21, 22), edge.value("port"));
        Assert.assertEquals(ImmutableList.of(21, 22), edge.value("type"));

        edge = graph.traversal().V("10.0.0.2").inE().next();
        Assert.assertEquals(Utils.date("2019-1-1 00:00:30"),
                            edge.value("startTime"));
        Assert.assertEquals(Utils.date("2019-1-1 00:08:00"),
                            edge.value("endTime"));
        Assert.assertEquals(13L, edge.value("times"));
        Assert.assertEquals(ImmutableSet.of(21, 22), edge.value("port"));
        Assert.assertEquals(ImmutableList.of(21, 22), edge.value("type"));

        edge.property("startTime", "2019-1-2 00:04:00");
        edge.property("endTime", "2019-1-2 00:08:00");
        edge.property("times", 7);
        edge.property("port", 23);
        edge.property("type", 23);
        graph.tx().commit();

        edge = graph.traversal().V("10.0.0.1").outE().next();
        Assert.assertEquals(Utils.date("2019-1-1 00:00:30"),
                            edge.value("startTime"));
        Assert.assertEquals(Utils.date("2019-1-2 00:08:00"),
                            edge.value("endTime"));
        Assert.assertEquals(20L, edge.value("times"));
        Assert.assertEquals(ImmutableSet.of(21, 22, 23),
                            edge.value("port"));
        Assert.assertEquals(ImmutableList.of(21, 22, 23),
                            edge.value("type"));

        edge = graph.traversal().V("10.0.0.2").inE().next();
        Assert.assertEquals(Utils.date("2019-1-1 00:00:30"),
                            edge.value("startTime"));
        Assert.assertEquals(Utils.date("2019-1-2 00:08:00"),
                            edge.value("endTime"));
        Assert.assertEquals(20L, edge.value("times"));
        Assert.assertEquals(ImmutableSet.of(21, 22, 23),
                            edge.value("port"));
        Assert.assertEquals(ImmutableList.of(21, 22, 23),
                            edge.value("type"));

        edge.property("startTime", "2019-1-1 00:00:00");
        edge.property("endTime", "2019-2-1 00:20:00");
        edge.property("times", 100);
        edge.property("port", 23);
        edge.property("type", 23);
        graph.tx().commit();

        edge = graph.traversal().V("10.0.0.1").outE().next();
        Assert.assertEquals(Utils.date("2019-1-1 00:00:00"),
                            edge.value("startTime"));
        Assert.assertEquals(Utils.date("2019-2-1 00:20:00"),
                            edge.value("endTime"));
        Assert.assertEquals(120L, edge.value("times"));
        Assert.assertEquals(ImmutableSet.of(21, 22, 23),
                            edge.value("port"));
        Assert.assertEquals(ImmutableList.of(21, 22, 23, 23),
                            edge.value("type"));

        edge = graph.traversal().V("10.0.0.2").inE().next();
        Assert.assertEquals(Utils.date("2019-1-1 00:00:00"),
                            edge.value("startTime"));
        Assert.assertEquals(Utils.date("2019-2-1 00:20:00"),
                            edge.value("endTime"));
        Assert.assertEquals(120L, edge.value("times"));
        Assert.assertEquals(ImmutableSet.of(21, 22, 23),
                            edge.value("port"));
        Assert.assertEquals(ImmutableList.of(21, 22, 23, 23),
                            edge.value("type"));
    }

    @Test
    public void testAddAndUpdateEdgePropertyOfAggregateType() {
        Assume.assumeTrue("Not support aggregate property",
                          storeFeatures().supportsAggregateProperty());

        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.propertyKey("startTime")
              .asDate().valueSingle().calcMin()
              .ifNotExist().create();
        schema.propertyKey("endTime")
              .asDate().valueSingle().calcMax()
              .ifNotExist().create();
        schema.propertyKey("times")
              .asLong().valueSingle().calcSum()
              .ifNotExist().create();
        schema.propertyKey("port")
              .asInt().valueSet().calcSet()
              .ifNotExist().create();
        schema.propertyKey("type")
              .asInt().valueList().calcList()
              .ifNotExist().create();

        schema.vertexLabel("ip").useCustomizeStringId().ifNotExist().create();

        schema.edgeLabel("attack").sourceLabel("ip").targetLabel("ip")
              .properties("startTime", "endTime", "times", "port", "type")
              .ifNotExist().create();

        Vertex ip1 = graph.addVertex(T.label, "ip", T.id, "10.0.0.1");
        Vertex ip2 = graph.addVertex(T.label, "ip", T.id, "10.0.0.2");

        Edge edge = ip1.addEdge("attack", ip2,
                                "startTime", "2019-1-1 00:00:30",
                                "endTime", "2019-1-1 00:01:00",
                                "times", 3, "port", 21, "type", 21);
        edge.property("startTime", "2019-1-1 00:04:00");
        edge.property("endTime", "2019-1-1 00:08:00");
        edge.property("times", 10L);
        edge.property("port", 21);
        edge.property("type", 21);

        Edge result = graph.traversal().V("10.0.0.1").outE().next();
        Assert.assertEquals(Utils.date("2019-1-1 00:04:00"),
                            result.value("startTime"));
        Assert.assertEquals(Utils.date("2019-1-1 00:08:00"),
                            result.value("endTime"));
        Assert.assertEquals(10L, result.value("times"));
        Assert.assertEquals(ImmutableSet.of(21), result.value("port"));
        Assert.assertEquals(ImmutableList.of(21, 21), result.value("type"));

        result = graph.traversal().V("10.0.0.2").inE().next();
        Assert.assertEquals(Utils.date("2019-1-1 00:04:00"),
                            result.value("startTime"));
        Assert.assertEquals(Utils.date("2019-1-1 00:08:00"),
                            result.value("endTime"));
        Assert.assertEquals(10L, result.value("times"));
        Assert.assertEquals(ImmutableSet.of(21), result.value("port"));
        Assert.assertEquals(ImmutableList.of(21, 21), result.value("type"));

        graph.tx().commit();

        result = graph.traversal().V("10.0.0.1").outE().next();
        Assert.assertEquals(Utils.date("2019-1-1 00:04:00"),
                            result.value("startTime"));
        Assert.assertEquals(Utils.date("2019-1-1 00:08:00"),
                            result.value("endTime"));
        Assert.assertEquals(10L, result.value("times"));
        Assert.assertEquals(ImmutableSet.of(21), result.value("port"));
        Assert.assertEquals(ImmutableList.of(21, 21), result.value("type"));

        result = graph.traversal().V("10.0.0.2").inE().next();
        Assert.assertEquals(Utils.date("2019-1-1 00:04:00"),
                            result.value("startTime"));
        Assert.assertEquals(Utils.date("2019-1-1 00:08:00"),
                            result.value("endTime"));
        Assert.assertEquals(10L, result.value("times"));
        Assert.assertEquals(ImmutableSet.of(21), result.value("port"));
        Assert.assertEquals(ImmutableList.of(21, 21), result.value("type"));

        edge = ip1.addEdge("attack", ip2,
                           "startTime", "2019-1-1 00:00:30",
                           "endTime", "2019-1-1 00:01:00",
                           "times", 3, "port", 22, "type", 22);

        edge.property("startTime", "2019-1-2 00:00:30");
        edge.property("endTime", "2019-1-2 00:01:00");
        edge.property("times", 2);
        edge.property("port", 23);
        edge.property("type", 23);

        Assert.assertEquals(Utils.date("2019-1-2 00:00:30"),
                            edge.value("startTime"));
        Assert.assertEquals(Utils.date("2019-1-2 00:01:00"),
                            edge.value("endTime"));
        Assert.assertEquals(2L, edge.value("times"));
        Assert.assertEquals(ImmutableSet.of(22, 23),
                            edge.value("port"));
        Assert.assertEquals(ImmutableList.of(22, 23),
                            edge.value("type"));

        Assert.assertEquals(Utils.date("2019-1-2 00:00:30"),
                            edge.property("startTime").value());
        Assert.assertEquals(Utils.date("2019-1-2 00:01:00"),
                            edge.property("endTime").value());
        Assert.assertEquals(2L, edge.property("times").value());
        Assert.assertEquals(ImmutableSet.of(22, 23),
                            edge.property("port").value());
        Assert.assertEquals(ImmutableList.of(22, 23),
                            edge.property("type").value());

        graph.tx().commit();
        result = graph.traversal().V("10.0.0.1").outE().next();
        Assert.assertEquals(Utils.date("2019-1-1 00:04:00"),
                            result.property("startTime").value());
        Assert.assertEquals(Utils.date("2019-1-2 00:01:00"),
                            result.property("endTime").value());
        Assert.assertEquals(12L, result.property("times").value());
        Assert.assertEquals(ImmutableSet.of(21, 22, 23),
                            result.property("port").value());
        Assert.assertEquals(ImmutableList.of(21, 21, 22, 23),
                            result.property("type").value());

        result = graph.traversal().V("10.0.0.2").inE().next();
        Assert.assertEquals(Utils.date("2019-1-1 00:04:00"),
                            result.property("startTime").value());
        Assert.assertEquals(Utils.date("2019-1-2 00:01:00"),
                            result.property("endTime").value());
        Assert.assertEquals(12L, result.property("times").value());
        Assert.assertEquals(ImmutableSet.of(21, 22, 23),
                            result.property("port").value());
        Assert.assertEquals(ImmutableList.of(21, 21, 22, 23),
                            result.property("type").value());
    }

    @Test
    public void testQueryEdgeByAggregateProperty() {
        Assume.assumeTrue("Not support aggregate property",
                          storeFeatures().supportsAggregateProperty());

        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.propertyKey("startTime")
              .asDate().valueSingle().calcMin()
              .ifNotExist().create();
        schema.propertyKey("endTime")
              .asDate().valueSingle().calcMax()
              .ifNotExist().create();
        schema.propertyKey("times")
              .asLong().valueSingle().calcSum()
              .ifNotExist().create();
        schema.propertyKey("firstTime")
              .asDate().valueSingle().calcOld()
              .ifNotExist().create();
        schema.propertyKey("port")
              .asInt().valueSet().calcSet()
              .ifNotExist().create();
        schema.propertyKey("type")
              .asInt().valueList().calcList()
              .ifNotExist().create();

        schema.vertexLabel("ip").useCustomizeStringId().ifNotExist().create();

        schema.edgeLabel("attack").sourceLabel("ip").targetLabel("ip")
              .properties("startTime", "endTime", "times",
                          "firstTime", "port", "type")
              .nullableKeys("port", "type")
              .ifNotExist().create();

        schema.indexLabel("attackByStartTime")
              .onE("attack").by("startTime").range().ifNotExist().create();
        schema.indexLabel("attackByendTime")
              .onE("attack").by("endTime").range().ifNotExist().create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("attackByTimes")
                  .onE("attack").by("times").range().ifNotExist().create();
        }, e -> {
            Assert.assertContains("The aggregate type SUM is not indexable",
                                  e.getMessage());
        });
        schema.indexLabel("attackByFirstTime")
              .onE("attack").by("firstTime").range().ifNotExist().create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("attackByPort")
                  .onE("attack").by("port").secondary().ifNotExist().create();
        }, e -> {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                    "The aggregate type SET is not indexable"));
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("attackByType")
                  .onE("attack").by("type").secondary().ifNotExist().create();
        }, e -> {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                    "The aggregate type LIST is not indexable"));
        });

        Vertex ip1 = graph.addVertex(T.label, "ip", T.id, "10.0.0.1");
        Vertex ip2 = graph.addVertex(T.label, "ip", T.id, "10.0.0.2");

        ip1.addEdge("attack", ip2,
                    "startTime", "2019-1-1 00:00:30",
                    "endTime", "2019-1-1 00:01:00",
                    "firstTime", "2019-5-5 12:00:00",
                    "times", 3);
        graph.tx().commit();

        List<Edge> edges = graph.traversal().V("10.0.0.1").outE().toList();
        Assert.assertEquals(1, edges.size());
        Edge edge = edges.get(0);
        Assert.assertEquals(Utils.date("2019-1-1 00:00:30"),
                            edge.value("startTime"));
        Assert.assertEquals(Utils.date("2019-1-1 00:01:00"),
                            edge.value("endTime"));
        Assert.assertEquals(Utils.date("2019-5-5 12:00:00"),
                            edge.value("firstTime"));
        Assert.assertEquals(3L, edge.value("times"));

        List<Edge> results = graph.traversal().E()
                                  .has("startTime", P.gt("2019-1-1 00:00:00"))
                                  .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("startTime", P.lt("2019-12-12 23:59:59"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("startTime", "2019-1-1 00:00:30")
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", P.gt("2019-1-1 00:00:00"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", P.lt("2019-12-12 23:59:59"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", "2019-1-1 00:01:00")
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("firstTime", "2019-5-5 12:00:00")
                       .toList();
        Assert.assertEquals(edges, results);

        edge.property("startTime", "2019-1-1 00:04:00");
        edge.property("endTime", "2019-1-1 00:08:00");
        edge.property("times", 10);
        graph.tx().commit();

        edges = graph.traversal().V("10.0.0.1").outE().toList();
        Assert.assertEquals(1, edges.size());
        edge = edges.get(0);
        Assert.assertEquals(Utils.date("2019-1-1 00:00:30"),
                            edge.value("startTime"));
        Assert.assertEquals(Utils.date("2019-1-1 00:08:00"),
                            edge.value("endTime"));
        Assert.assertEquals(Utils.date("2019-5-5 12:00:00"),
                            edge.value("firstTime"));
        Assert.assertEquals(13L, edge.value("times"));

        results = graph.traversal().E()
                       .has("startTime", P.gt("2019-1-1 00:00:00"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("startTime", P.lt("2019-12-12 23:59:59"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("startTime", "2019-1-1 00:00:30")
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", P.gt("2019-1-1 00:00:00"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", P.lt("2019-12-12 23:59:59"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", "2019-1-1 00:08:00")
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("firstTime", "2019-5-5 12:00:00")
                       .toList();
        Assert.assertEquals(edges, results);

        edge.property("startTime", "2019-1-2 00:04:00");
        edge.property("endTime", "2019-1-2 00:08:00");
        edge.property("times", 7);
        graph.tx().commit();

        edges = graph.traversal().V("10.0.0.1").outE().toList();
        Assert.assertEquals(1, edges.size());
        edge = edges.get(0);
        Assert.assertEquals(Utils.date("2019-1-1 00:00:30"),
                            edge.value("startTime"));
        Assert.assertEquals(Utils.date("2019-1-2 00:08:00"),
                            edge.value("endTime"));
        Assert.assertEquals(Utils.date("2019-5-5 12:00:00"),
                            edge.value("firstTime"));
        Assert.assertEquals(20L, edge.value("times"));

        results = graph.traversal().E()
                       .has("startTime", P.gt("2019-1-1 00:00:00"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("startTime", P.lt("2019-12-12 23:59:59"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("startTime", "2019-1-1 00:00:30")
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", P.gt("2019-1-1 00:00:00"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", P.lt("2019-12-12 23:59:59"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", "2019-1-2 00:08:00")
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("firstTime", "2019-5-5 12:00:00")
                       .toList();
        Assert.assertEquals(edges, results);

        edge.property("startTime", "2019-1-1 00:00:02");
        edge.property("endTime", "2019-2-1 00:20:00");
        edge.property("times", 100);
        graph.tx().commit();

        edges = graph.traversal().V("10.0.0.1").outE().toList();
        Assert.assertEquals(1, edges.size());
        edge = edges.get(0);
        Assert.assertEquals(Utils.date("2019-1-1 00:00:02"),
                            edge.value("startTime"));
        Assert.assertEquals(Utils.date("2019-2-1 00:20:00"),
                            edge.value("endTime"));
        Assert.assertEquals(Utils.date("2019-5-5 12:00:00"),
                            edge.value("firstTime"));
        Assert.assertEquals(120L, edge.value("times"));

        results = graph.traversal().E()
                       .has("startTime", P.gt("2019-1-1 00:00:00"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("startTime", P.lt("2019-12-12 23:59:59"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("startTime", "2019-1-1 00:00:02")
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", P.gt("2019-1-1 00:00:00"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", P.lt("2019-12-12 23:59:59"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", "2019-2-1 00:20:00")
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("firstTime", "2019-5-5 12:00:00")
                       .toList();
        Assert.assertEquals(edges, results);

        edge.property("startTime", "2019-1-1 00:00:01");
        edge.property("endTime", "2019-2-1 00:20:00");
        edge.property("times", 20);
        graph.tx().commit();

        edge.property("startTime", "2019-3-1 00:00:00");
        edge.property("endTime", "2019-5-1 00:20:00");
        edge.property("times", 25);
        graph.tx().commit();

        edge.property("startTime", "2019-1-1 00:30:00");
        edge.property("endTime", "2019-8-1 00:20:00");
        edge.property("times", 35);
        graph.tx().commit();

        edges = graph.traversal().V("10.0.0.1").outE().toList();
        Assert.assertEquals(1, edges.size());
        edge = edges.get(0);
        Assert.assertEquals(Utils.date("2019-1-1 00:00:01"),
                            edge.value("startTime"));
        Assert.assertEquals(Utils.date("2019-8-1 00:20:00"),
                            edge.value("endTime"));
        Assert.assertEquals(Utils.date("2019-5-5 12:00:00"),
                            edge.value("firstTime"));
        Assert.assertEquals(200L, edge.value("times"));

        results = graph.traversal().E()
                       .has("startTime", P.gt("2019-1-1 00:00:00"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("startTime", P.lt("2019-12-12 23:59:59"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("startTime", "2019-1-1 00:00:01")
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", P.gt("2019-1-1 00:00:00"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", P.lt("2019-12-12 23:59:59"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("endTime", "2019-8-1 00:20:00")
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("firstTime", P.gt("2019-5-1 12:00:00"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("firstTime", P.lt("2019-6-1 12:00:00"))
                       .toList();
        Assert.assertEquals(edges, results);

        results = graph.traversal().E()
                       .has("firstTime", "2019-5-5 12:00:00")
                       .toList();
        Assert.assertEquals(edges, results);
    }

    @Test
    public void testRemoveEdgeProperty() {
        HugeGraph graph = graph();

        Edge edge = initEdgeTransfer();
        Assert.assertEquals("Happy birthday!",
                            edge.property("message").value());

        // Remove property
        edge.property("message").remove();
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertFalse(edges.get(0).property("message").isPresent());
    }

    @Test
    public void testRemoveEdgePropertyTwice() {
        HugeGraph graph = graph();

        Edge edge = initEdgeTransfer();
        Assert.assertEquals(500.00F, edge.property("amount").value());
        Assert.assertEquals("Happy birthday!",
                            edge.property("message").value());

        // Remove property twice
        edge.property("message").remove();
        edge.property("message").remove();
        graph.tx().commit();

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertFalse(edges.get(0).property("message").isPresent());
    }

    @Test
    public void testRemoveEdgePropertyOfSortKey() {
        Edge edge = initEdgeTransfer();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("id").remove();
        });
    }

    @Test
    public void testRemoveEdgePropertyNullableWithIndex() {
        HugeGraph graph = graph();
        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("strike", sean, "id", 1,
                                   "timestamp", current, "place", "park",
                                   "tool", "shovel", "reason", "jeer",
                                   "arrested", false);
        edge.property("tool").remove();
        graph.tx().commit();
    }

    @Test
    public void testRemoveEdgePropertyNonNullWithIndex() {
        HugeGraph graph = graph();
        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("strike", sean, "id", 1,
                                   "timestamp", current, "place", "park",
                                   "tool", "shovel", "reason", "jeer",
                                   "arrested", false);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("timestamp").remove();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("place").remove();
        });
    }

    @Test
    public void testRemoveEdgePropertyNullableWithoutIndex() {
        HugeGraph graph = graph();
        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("strike", sean, "id", 1,
                                   "timestamp", current, "place", "park",
                                   "tool", "shovel", "reason", "jeer",
                                   "hurt", true, "arrested", false);
        edge.property("hurt").remove();
        graph.tx().commit();
    }

    @Test
    public void testRemoveEdgePropertyNonNullWithoutIndex() {
        HugeGraph graph = graph();
        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("strike", sean, "id", 1,
                                   "timestamp", current, "place", "park",
                                   "tool", "shovel", "reason", "jeer",
                                   "arrested", false);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            edge.property("arrested").remove();
        });
    }

    @Test
    public void testQueryEdgeByPropertyWithEmptyString() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        louise.addEdge("strike", sean, "id", 1,
                       "timestamp", current, "place", "park",
                       "tool", "", "reason", "jeer",
                       "arrested", false);
        Edge edge = graph.traversal().E().has("tool", "").next();
        Assert.assertEquals(1, (int) edge.value("id"));
        Assert.assertEquals("", edge.value("tool"));

        edge = graph.traversal().E().has("tool", "").has("place", "park")
                    .has("reason", "jeer").next();
        Assert.assertEquals(1, (int) edge.value("id"));
    }

    @Test
    public void testQueryEdgeByStringPropOfOverrideEdge() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        louise.addEdge("strike", sean, "id", 1, "timestamp", current,
                       "place", "park", "tool", "shovel", "reason", "jeer",
                       "arrested", false);
        louise.addEdge("strike", sean, "id", 1, "timestamp", current,
                       "place", "street", "tool", "shovel", "reason", "jeer",
                       "arrested", false);
        List<Edge> edges = graph.traversal().E().has("place", "park")
                                .toList();
        Assert.assertEquals(0, edges.size());
        edges = graph.traversal().E().has("place", "street").toList();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testQueryEdgeBeforeAfterUpdateMultiPropertyWithIndex() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("strike", sean, "id", 1,
                                   "timestamp", current, "place", "park",
                                   "tool", "shovel", "reason", "jeer",
                                   "arrested", false);

        List<Edge> vl = graph.traversal().E().has("tool", "shovel").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals(1, (int) vl.get(0).value("id"));
        vl = graph.traversal().E().has("timestamp", current).toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals(1, (int) vl.get(0).value("id"));
        vl = graph.traversal().E().has("tool", "knife").toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().E().has("timestamp", 666L).toList();
        Assert.assertEquals(0, vl.size());

        edge.property("tool", "knife");
        edge.property("timestamp", 666L);

        vl = graph.traversal().E().has("tool", "shovel").toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().E().has("timestamp", current).toList();
        Assert.assertEquals(0, vl.size());
        vl = graph.traversal().E().has("tool", "knife").toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals(1, (int) vl.get(0).value("id"));
        vl = graph.traversal().E().has("timestamp", 666L).toList();
        Assert.assertEquals(1, vl.size());
        Assert.assertEquals(1, (int) vl.get(0).value("id"));
    }

    @Test
    public void testQueryEdgeBeforeAfterUpdatePropertyWithSecondaryIndex() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("strike", sean, "id", 1,
                                   "timestamp", current, "place", "park",
                                   "tool", "shovel", "reason", "jeer",
                                   "arrested", false);

        List<Edge> el = graph.traversal().E().has("tool", "shovel")
                             .toList();
        Assert.assertEquals(1, el.size());
        Assert.assertEquals(1, (int) el.get(0).value("id"));
        el = graph.traversal().E().has("tool", "knife").toList();
        Assert.assertEquals(0, el.size());

        edge.property("tool", "knife");

        el = graph.traversal().E().has("tool", "shovel").toList();
        Assert.assertEquals(0, el.size());
        el = graph.traversal().E().has("tool", "knife").toList();
        Assert.assertEquals(1, el.size());
        Assert.assertEquals(1, (int) el.get(0).value("id"));
    }

    @Test
    public void testQueryEdgeBeforeAfterUpdatePropertyWithRangeIndex() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("strike", sean, "id", 1,
                                   "timestamp", current, "place", "park",
                                   "tool", "shovel", "reason", "jeer",
                                   "arrested", false);

        List<Edge> el = graph.traversal().E().has("timestamp", current)
                             .toList();
        Assert.assertEquals(1, el.size());
        Assert.assertEquals(1, (int) el.get(0).value("id"));
        el = graph.traversal().E().has("timestamp", 666L).toList();
        Assert.assertEquals(0, el.size());

        edge.property("timestamp", 666L);

        el = graph.traversal().E().has("timestamp", current).toList();
        Assert.assertEquals(0, el.size());
        el = graph.traversal().E().has("timestamp", 666L).toList();
        Assert.assertEquals(1, el.size());
        Assert.assertEquals(1, (int) el.get(0).value("id"));
    }

    @Test
    public void testQueryEdgeWithNullablePropertyInCompositeIndex() {
        HugeGraph graph = graph();
        initStrikeIndex();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        louise.addEdge("strike", sean, "id", 1,
                       "timestamp", current, "place", "park",
                       "tool", "shovel", "arrested", false);

        List<Edge> el = graph.traversal().E().has("place", "park")
                             .toList();
        Assert.assertEquals(1, el.size());
        Assert.assertEquals(1, (int) el.get(0).value("id"));
        el = graph.traversal().E().has("place", "park")
                  .has("tool", "shovel").toList();
        Assert.assertEquals(1, el.size());
        Assert.assertEquals(1, (int) el.get(0).value("id"));
    }

    @Test
    public void testQueryEdgeByBooleanRangePredicate() {
        HugeGraph graph = graph();
        initStrikeIndex();
        graph.schema().indexLabel("strikeByArrested").onE("strike").secondary()
             .by("arrested").create();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        louise.addEdge("strike", sean, "id", 1,
                       "timestamp", current, "place", "park",
                       "tool", "shovel", "reason", "jeer",
                       "arrested", false);
        louise.addEdge("strike", sean, "id", 2,
                       "timestamp", current + 1, "place", "street",
                       "tool", "shovel", "reason", "jeer",
                       "arrested", true);

        List<Edge> hasLtEdges = graph.traversal().E()
                                     .has("arrested", P.lt(true))
                                     .toList();
        Assert.assertEquals(1, hasLtEdges.size());
        Assert.assertEquals(1, (int) hasLtEdges.get(0).value("id"));

        List<Edge> whereEdges = graph.traversal().E()
                                     .where(__.has("arrested", P.lt(true)))
                                     .toList();
        Assert.assertEquals(1, whereEdges.size());
        Assert.assertEquals(1, (int) whereEdges.get(0).value("id"));

        List<Edge> matchEdges = graph.traversal().E()
                                     .match(__.as("start")
                                              .where(__.has("arrested",
                                                            P.lt(true)))
                                              .as("matched"))
                                     .<Edge>select("matched")
                                     .toList();
        Assert.assertEquals(1, matchEdges.size());
        Assert.assertEquals(1, (int) matchEdges.get(0).value("id"));

        List<Edge> hasNeqTrueEdges = graph.traversal().E()
                                          .has("arrested", P.neq(true))
                                          .toList();
        Assert.assertEquals(1, hasNeqTrueEdges.size());
        Assert.assertEquals(1, (int) hasNeqTrueEdges.get(0).value("id"));

        List<Edge> hasNeqFalseEdges = graph.traversal().E()
                                           .has("arrested", P.neq(false))
                                           .toList();
        Assert.assertEquals(1, hasNeqFalseEdges.size());
        Assert.assertEquals(2, (int) hasNeqFalseEdges.get(0).value("id"));

        List<Edge> hasLteFalseEdges = graph.traversal().E()
                                           .has("arrested", P.lte(false))
                                           .toList();
        Assert.assertEquals(1, hasLteFalseEdges.size());
        Assert.assertEquals(1, (int) hasLteFalseEdges.get(0).value("id"));

        List<Edge> hasGtFalseEdges = graph.traversal().E()
                                          .has("arrested", P.gt(false))
                                          .toList();
        Assert.assertEquals(1, hasGtFalseEdges.size());
        Assert.assertEquals(2, (int) hasGtFalseEdges.get(0).value("id"));

        List<Edge> hasGteTrueEdges = graph.traversal().E()
                                           .has("arrested", P.gte(true))
                                           .toList();
        Assert.assertEquals(1, hasGteTrueEdges.size());
        Assert.assertEquals(2, (int) hasGteTrueEdges.get(0).value("id"));

        List<Edge> hasGteFalseEdges = graph.traversal().E()
                                            .has("arrested", P.gte(false))
                                            .toList();
        Assert.assertEquals(2, hasGteFalseEdges.size());
        Set<Integer> gteFalseIds = new HashSet<>();
        for (Edge edge : hasGteFalseEdges) {
            gteFalseIds.add(edge.value("id"));
        }
        Assert.assertEquals(ImmutableSet.of(1, 2), gteFalseIds);

        List<Edge> hasLteTrueEdges = graph.traversal().E()
                                           .has("arrested", P.lte(true))
                                           .toList();
        Assert.assertEquals(2, hasLteTrueEdges.size());
        Set<Integer> lteTrueIds = new HashSet<>();
        for (Edge edge : hasLteTrueEdges) {
            lteTrueIds.add(edge.value("id"));
        }
        Assert.assertEquals(ImmutableSet.of(1, 2), lteTrueIds);

        Assert.assertEquals(0, graph.traversal().E()
                                    .has("arrested", P.lt(false))
                                    .toList().size());
        Assert.assertEquals(0, graph.traversal().E()
                                    .has("arrested", P.gt(true))
                                    .toList().size());
    }

    @Test
    public void testQueryEdgeByBooleanRangePredicateWithoutNullableProperty() {
        HugeGraph graph = graph();
        initStrikeIndex();
        graph.schema().indexLabel("strikeByHurt").onE("strike").secondary()
             .by("hurt").create();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        long current = System.currentTimeMillis();
        louise.addEdge("strike", sean, "id", 1,
                       "timestamp", current, "place", "park",
                       "tool", "shovel", "reason", "jeer",
                       "hurt", false, "arrested", false);
        louise.addEdge("strike", sean, "id", 2,
                       "timestamp", current + 1, "place", "street",
                       "tool", "shovel", "reason", "jeer",
                       "hurt", true, "arrested", true);
        louise.addEdge("strike", sean, "id", 3,
                       "timestamp", current + 2, "place", "mall",
                       "tool", "shovel", "reason", "jeer",
                       "arrested", false);

        List<Edge> gteFalseEdges = graph.traversal().E()
                                        .has("hurt", P.gte(false))
                                        .toList();
        Assert.assertEquals(2, gteFalseEdges.size());
        Set<Integer> gteFalseIds = new HashSet<>();
        for (Edge edge : gteFalseEdges) {
            gteFalseIds.add(edge.value("id"));
        }
        Assert.assertEquals(ImmutableSet.of(1, 2), gteFalseIds);

        List<Edge> lteTrueEdges = graph.traversal().E()
                                        .has("hurt", P.lte(true))
                                        .toList();
        Assert.assertEquals(2, lteTrueEdges.size());
        Set<Integer> lteTrueIds = new HashSet<>();
        for (Edge edge : lteTrueEdges) {
            lteTrueIds.add(edge.value("id"));
        }
        Assert.assertEquals(ImmutableSet.of(1, 2), lteTrueIds);

        List<Edge> lteFalseEdges = graph.traversal().E()
                                         .has("hurt", P.lte(false))
                                         .toList();
        Assert.assertEquals(1, lteFalseEdges.size());
        Assert.assertEquals(1, (int) lteFalseEdges.get(0).value("id"));

        List<Edge> neqTrueEdges = graph.traversal().E()
                                       .has("hurt", P.neq(true))
                                       .toList();
        Assert.assertEquals(1, neqTrueEdges.size());
        Assert.assertEquals(1, (int) neqTrueEdges.get(0).value("id"));
    }

    @Test
    public void testQueryEdgeByPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100LookEdges();

        GraphTraversal<Edge, Edge> iter = graph.traversal().E()
                                               .has("~page", "").limit(10);
        Assert.assertEquals(10, IteratorUtils.count(iter));
        String page = TraversalUtil.page(iter);

        List<Edge> edges;

        edges = graph.traversal().E()//.hasLabel("look")
                     .has("~page", page).limit(1)
                     .toList();
        Assert.assertEquals(1, edges.size());
        Edge edge1 = edges.get(0);

        edges = graph.traversal().E()
                     .has("~page", page).limit(33)
                     .toList();
        Assert.assertEquals(33, edges.size());
        Edge edge2 = edges.get(0);
        Assert.assertEquals(edge1.id(), edge2.id());
        Assert.assertEquals(edge1.label(), edge2.label());
        Assert.assertEquals(IteratorUtils.asList(edge1.properties()),
                            IteratorUtils.asList(edge2.properties()));

        edges = graph.traversal().E()
                     .has("~page", page).limit(89)
                     .toList();
        Assert.assertEquals(89, edges.size());
        Edge edge3 = edges.get(88);

        edges = graph.traversal().E()
                     .has("~page", page).limit(90)
                     .toList();
        Assert.assertEquals(90, edges.size());
        Edge edge4 = edges.get(88);
        Assert.assertEquals(edge3.id(), edge4.id());
        Assert.assertEquals(edge3.label(), edge4.label());
        Assert.assertEquals(IteratorUtils.asList(edge3.properties()),
                            IteratorUtils.asList(edge4.properties()));

        edges = graph.traversal().E()
                     .has("~page", page).limit(91)
                     .toList();
        Assert.assertEquals(90, edges.size());
    }

    @Test
    public void testQueryEdgeByPageResultsMatchedAll() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100LookEdges();

        List<Edge> all = graph.traversal().E().toList();

        GraphTraversal<Edge, Edge> iter;

        String page = PageInfo.PAGE_NONE;
        int size = 21;

        Set<Edge> pageAll = new HashSet<>();
        for (int i = 0; i < 100 / size; i++) {
            iter = graph.traversal().E()
                        .has("~page", page).limit(size);
            @SuppressWarnings("unchecked")
            List<Edge> edges = IteratorUtils.asList(iter);
            Assert.assertEquals(size, edges.size());

            pageAll.addAll(edges);

            page = TraversalUtil.page(iter);
        }

        iter = graph.traversal().E()
                    .has("~page", page).limit(size);
        @SuppressWarnings("unchecked")
        List<Edge> edges = IteratorUtils.asList(iter);
        Assert.assertEquals(16, edges.size());
        pageAll.addAll(edges);
        page = TraversalUtil.page(iter);

        Assert.assertEquals(100, pageAll.size());
        Assert.assertTrue(all.containsAll(pageAll));
        Assert.assertNull(page);
    }

    @Test
    public void testQueryEdgeByPageResultsMatchedAllWithFullPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100LookEdges();

        List<Edge> all = graph.traversal().E().toList();

        GraphTraversal<Edge, Edge> iter;

        String page = PageInfo.PAGE_NONE;
        int size = 20;

        Set<Edge> pageAll = new HashSet<>();
        for (int i = 0; i < 100 / size; i++) {
            iter = graph.traversal().E()
                        .has("~page", page).limit(size);
            @SuppressWarnings("unchecked")
            List<Edge> edges = IteratorUtils.asList(iter);
            Assert.assertEquals(size, edges.size());

            pageAll.addAll(edges);

            page = TraversalUtil.page(iter);
        }
        Assert.assertEquals(100, pageAll.size());
        Assert.assertTrue(all.containsAll(pageAll));

        if (page != null) {
            iter = graph.traversal().E().has("~page", page);
            long count = IteratorUtils.count(iter);
            Assert.assertEquals(0L, count);

            page = TraversalUtil.page(iter);
            CloseableIterator.closeIterator(iter);
        }
        Assert.assertNull(page);
    }

    @Test
    public void testQueryEdgeByPageWithInvalidPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100LookEdges();

        // Illegal base64 character
        Assert.assertThrows(BackendException.class, () -> {
            graph.traversal().E()
                 .has("~page", "!abc123#").limit(10)
                 .toList();
        });

        // Invalid page
        Assert.assertThrows(BackendException.class, () -> {
            graph.traversal().E()
                 .has("~page", "abc123").limit(10)
                 .toList();
        });
    }

    @Test
    public void testQueryEdgeByPageWithInvalidLimit() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100LookEdges();
        GraphTraversalSource g = graph.traversal();
        long bigLimit = Query.defaultCapacity() + 1L;

        Assert.assertThrows(IllegalStateException.class, () -> {
            g.E().has("~page", "").limit(0).toList();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            g.E().has("~page", "").limit(bigLimit).toList();
        });
    }

    @Test
    public void testQueryEdgeByPageWithOffset() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        init100LookEdges();

        Assert.assertThrows(IllegalStateException.class, () -> {
            graph.traversal().E()
                 .has("~page", "").range(2, 10)
                 .toList();
        });
    }

    @Test
    public void testQueryByHasIdEmptyList() {
        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();

        List<Edge> edges = g.E().hasId(Collections.EMPTY_LIST).toList();
        Assert.assertEquals(0, edges.size());
    }

    @Test
    public void testQueryByHasIdEmptyListInPage() {
        Assume.assumeTrue("Not support paging",
                          storeFeatures().supportsQueryByPage());

        HugeGraph graph = graph();
        GraphTraversalSource g = graph.traversal();

        GraphTraversal<Edge, Edge> iter = g.E()
                                           .hasId(Collections.EMPTY_LIST)
                                           .has("~page", "").limit(1);
        Assert.assertEquals(0, IteratorUtils.count(iter));

        String page = TraversalUtil.page(iter);
        Assert.assertNull(page);
    }

    @Test
    public void testAddEdgeWithCollectionIndex() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("tags").asText().valueSet().create();
        schema.propertyKey("category").asText().valueSet().create();
        schema.propertyKey("country").asText().create();

        schema.vertexLabel("soft")
              .properties("name", "tags", "country", "category")
              .primaryKeys("name").create();

        schema.edgeLabel("related")
              .sourceLabel("soft")
              .targetLabel("soft")
              .properties("tags").create();

        schema.indexLabel("edgeByTag").onE("related").secondary()
              .by("tags")
              .create();

        HugeGraph graph = graph();

        Vertex huge = graph.addVertex(
                T.label, "soft",
                "name", "hugegraph",
                "country", "china",
                "category", ImmutableList.of("graphdb", "db"),
                "tags", ImmutableList.of("graphdb", "gremlin"));

        Vertex neo4j = graph.addVertex(
                T.label, "soft", "name", "neo4j",
                "country", "usa",
                "category", ImmutableList.of("graphdb", "db"),
                "tags", ImmutableList.of("graphdb", "cypher"));

        Vertex janus = graph.addVertex(
                T.label, "soft", "name", "janusgraph",
                "country", "usa",
                "category", ImmutableList.of("graphdb", "db"),
                "tags", ImmutableList.of("graphdb", "gremlin"));

        huge.addEdge("related", neo4j, "tags", ImmutableList.of("graphdb"));

        Edge huge2janus = huge.addEdge("related", janus, "tags",
                                       ImmutableList.of("graphdb", "gremlin"));

        graph.tx().commit();

        Assert.assertThrows(IllegalStateException.class, () -> {
            graph.traversal().E().has("related", "tags",
                                      "gremlin").toList();
        });

        List<Edge> edges = graph.traversal().E()
                                .has("related", "tags",
                                     ConditionP.contains("gremlin"))
                                .toList();

        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().E()
                     .has("related",
                          "tags", ConditionP.contains("graphdb"))
                     .toList();

        Assert.assertEquals(2, edges.size());

        // append a new tag
        huge2janus.property("tags", ImmutableList.of("newTag"));
        graph.tx().commit();

        edges = graph.traversal().E()
                     .has("related",
                          "tags", ConditionP.contains("newTag"))
                     .toList();

        Assert.assertEquals(1, edges.size());

        edges = graph.traversal().E()
                     .has("related",
                          "tags", ConditionP.contains("graphdb"))
                     .toList();

        Assert.assertEquals(2, edges.size());

        // remove a edge
        edges.get(0).remove();
        graph.tx().commit();

        edges = graph.traversal().E()
                     .has("related",
                          "tags", ConditionP.contains("graphdb"))
                     .toList();
        Assert.assertEquals(1, edges.size());
    }

    private void init18Edges() {
        this.init18Edges(true);
    }

    private void init18Edges(boolean commit) {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                                       "name", "James Gosling", "age", 62,
                                       "lived", "Canadian");
        Vertex guido = graph.addVertex(T.label, "author", "id", 2,
                                       "name", "Guido van Rossum", "age", 61,
                                       "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                                        "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                                      "city", "Beijing", "age", 22);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);
        Vertex selina = graph.addVertex(T.label, "person", "name", "Selina",
                                        "city", "Beijing", "age", 24);

        james.addEdge("created", java);
        guido.addEdge("created", python);

        guido.addEdge("know", james);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3, "score", 3);

        louise.addEdge("look", java1, "time", "2017-5-1");
        louise.addEdge("look", java1, "time", "2017-5-27");
        louise.addEdge("look", java2, "time", "2017-5-27");
        louise.addEdge("look", java3, "time", "2017-5-1", "score", 3);
        jeff.addEdge("look", java3, "time", "2017-5-27", "score", 3);
        sean.addEdge("look", java3, "time", "2017-5-27", "score", 4);
        selina.addEdge("look", java3, "time", "2017-5-27", "score", 0);

        louise.addEdge("friend", jeff);
        louise.addEdge("friend", sean);
        louise.addEdge("friend", selina);
        jeff.addEdge("friend", sean);
        jeff.addEdge("follow", james);

        if (commit) {
            graph.tx().commit();
        }
    }

    private void init100LookEdges() {
        HugeGraph graph = graph();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex jeff = graph.addVertex(T.label, "person", "name", "Jeff",
                                      "city", "Beijing", "age", 22);

        Vertex java = graph.addVertex(T.label, "book", "name", "java-book");

        for (int i = 0; i < 50; i++) {
            louise.addEdge("look", java, "time", "time-" + i);
        }

        for (int i = 0; i < 50; i++) {
            jeff.addEdge("look", java, "time", "time-" + i);
        }

        graph.tx().commit();
    }

    private Edge initEdgeTransfer() {
        HugeGraph graph = graph();

        Vertex louise = graph.addVertex(T.label, "person", "name", "Louise",
                                        "city", "Beijing", "age", 21);
        Vertex sean = graph.addVertex(T.label, "person", "name", "Sean",
                                      "city", "Beijing", "age", 23);

        long current = System.currentTimeMillis();
        Edge edge = louise.addEdge("transfer", sean, "id", 1,
                                   "amount", 500.00F, "timestamp", current,
                                   "message", "Happy birthday!");

        graph.tx().commit();
        return edge;
    }

    private Vertex vertex(String label, String pkName, Object pkValue) {
        List<Vertex> vertices = graph().traversal().V()
                                       .hasLabel(label).has(pkName, pkValue)
                                       .toList();
        Assert.assertTrue(vertices.size() <= 1);
        return vertices.size() == 1 ? vertices.get(0) : null;
    }

    private static void assertContains(
            List<Edge> edges,
            String label,
            Vertex outVertex,
            Vertex inVertex,
            Object... kvs) {
        Assert.assertTrue(Utils.contains(edges,
                                         new FakeObjects.FakeEdge(label, outVertex, inVertex,
                                                                  kvs)));
    }

    private int traverseInPage(Function<String, GraphTraversal<?, ?>> fetcher) {
        String page = PageInfo.PAGE_NONE;
        int count = 0;
        while (page != null) {
            GraphTraversal<?, ?> iterator = fetcher.apply(page);
            Long size = IteratorUtils.count(iterator);
            Assert.assertLte(1L, size);
            page = TraversalUtil.page(iterator);
            count += size.intValue();
        }
        return count;
    }

    private int superNodeSize() {
        return params().configuration().get(CoreOptions.EDGE_TX_CAPACITY);
    }
}
