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

import java.util.Date;
import java.util.List;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.exception.ExistedException;
import org.apache.hugegraph.exception.NoIndexException;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.util.DateUtil;
import org.apache.hugegraph.util.Events;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class EdgeLabelCoreTest extends SchemaCoreTest {

    @Test
    public void testAddEdgeLabel() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        EdgeLabel look = schema.edgeLabel("look").multiTimes()
                               .properties("time")
                               .link("person", "book")
                               .sortKeys("time")
                               .create();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        assertVLEqual("person", look.sourceLabel());
        assertVLEqual("book", look.targetLabel());
        Assert.assertEquals(1, look.properties().size());
        assertContainsPk(look.properties(), "time");
        Assert.assertEquals(1, look.sortKeys().size());
        assertContainsPk(look.sortKeys(), "time");
        Assert.assertEquals(Frequency.MULTIPLE, look.frequency());
    }

    @Test
    public void testAddEdgeLabelWithIllegalName() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();

        // Empty string
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("").link("person", "author").create();
        });
        // One space
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel(" ").link("person", "author").create();
        });
        // Two spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("  ").link("person", "author").create();
        });
        // Multi spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("    ").link("person", "author").create();
        });
        // Start with '~'
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("~").link("person", "author").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("~ ").link("person", "author").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("~x").link("person", "author").create();
        });
    }

    @Test
    public void testAddEdgeLabelWithoutFrequency() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        EdgeLabel look = schema.edgeLabel("look").properties("time")
                               .link("person", "book")
                               .create();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        assertVLEqual("person", look.sourceLabel());
        assertVLEqual("book", look.targetLabel());
        Assert.assertEquals(1, look.properties().size());
        assertContainsPk(look.properties(), "time");
        Assert.assertEquals(0, look.sortKeys().size());
        Assert.assertEquals(Frequency.SINGLE, look.frequency());
    }

    @Test
    public void testAddEdgeLabelWithoutProperty() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        EdgeLabel look = schema.edgeLabel("look").singleTime()
                               .link("person", "book")
                               .create();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        assertVLEqual("person", look.sourceLabel());
        assertVLEqual("book", look.targetLabel());
        Assert.assertEquals(0, look.properties().size());
        Assert.assertEquals(0, look.sortKeys().size());
        Assert.assertEquals(Frequency.SINGLE, look.frequency());
    }

    @Test
    public void testAddEdgeLabelWithoutLink() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look").multiTimes()
                  .properties("time")
                  .sortKeys("time")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithNotExistProperty() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look").properties("date")
                  .link("person", "book")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithNotExistVertexLabel() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look").multiTimes().properties("time")
                  .link("reviewer", "book")
                  .sortKeys("time")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithSortKeyAssignedMultiTimes() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .multiTimes()
                  .properties("date")
                  .link("person", "book")
                  .sortKeys("date")
                  .sortKeys("date")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithSortKeyContainSameProp() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .multiTimes()
                  .properties("date")
                  .link("person", "book")
                  .sortKeys("date", "date")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelMultipleWithoutSortKey() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look").multiTimes().properties("date")
                  .link("person", "book")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelSortKeyNotInProperty() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look").multiTimes().properties("date")
                  .link("person", "book")
                  .sortKeys("time")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithNullableKeys() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        EdgeLabel look = schema.edgeLabel("look")
                               .properties("time", "weight")
                               .nullableKeys("weight")
                               .link("person", "book")
                               .create();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        assertVLEqual("person", look.sourceLabel());
        assertVLEqual("book", look.targetLabel());
        Assert.assertEquals(2, look.properties().size());
        assertContainsPk(look.properties(), "time", "weight");
        Assert.assertEquals(1, look.nullableKeys().size());
        assertContainsPk(look.nullableKeys(), "weight");
    }

    @Test
    public void testAddEdgeLabelWithUndefinedNullableKeys() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .properties("time", "weight")
                  .nullableKeys("undefined")
                  .link("person", "book")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithNullableKeysNotInProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .properties("time", "weight")
                  .nullableKeys("age")
                  .link("person", "book")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithNullableKeysIntersectSortkeys() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .multiTimes()
                  .properties("time", "weight")
                  .sortKeys("time")
                  .nullableKeys("time")
                  .link("person", "book")
                  .create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .multiTimes()
                  .properties("time", "weight")
                  .sortKeys("time", "weight")
                  .nullableKeys("time")
                  .link("person", "book")
                  .create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .multiTimes()
                  .properties("time", "weight")
                  .sortKeys("time")
                  .nullableKeys("time", "weight")
                  .link("person", "book")
                  .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithEnableLabelIndex() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        EdgeLabel write = schema.edgeLabel("write").link("person", "book")
                                .properties("time", "weight")
                                .enableLabelIndex(true)
                                .create();
        Assert.assertEquals(true, write.enableLabelIndex());

        Vertex marko = graph().addVertex(T.label, "person", "name", "marko",
                                         "age", 22);
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "java in action");
        Vertex hadoop = graph().addVertex(T.label, "book",
                                          "name", "hadoop mapreduce");

        marko.addEdge("write", java, "time", "2016-12-12", "weight", 0.3);
        marko.addEdge("write", hadoop, "time", "2014-2-28", "weight", 0.5);
        graph().tx().commit();

        List<Edge> edges = graph().traversal().E().hasLabel("write").toList();
        Assert.assertEquals(2, edges.size());
    }

    @Test
    public void testAddEdgeLabelWithDisableLabelIndex() {
        super.initPropertyKeys();
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        EdgeLabel write = schema.edgeLabel("write").link("person", "book")
                                .properties("time", "weight")
                                .enableLabelIndex(false)
                                .create();
        Assert.assertEquals(false, write.enableLabelIndex());

        Vertex marko = graph.addVertex(T.label, "person", "name", "marko",
                                       "age", 22);
        Vertex java = graph.addVertex(T.label, "book",
                                      "name", "java in action");
        Vertex hadoop = graph.addVertex(T.label, "book",
                                        "name", "hadoop mapreduce");

        marko.addEdge("write", java, "time", "2016-12-12", "weight", 0.3);
        marko.addEdge("write", hadoop, "time", "2014-2-28", "weight", 0.5);
        graph.tx().commit();

        if (!storeFeatures().supportsQueryByLabel()) {
            Assert.assertThrows(NoIndexException.class, () -> {
                graph.traversal().E().hasLabel("write").toList();
            });
        } else {
            List<Edge> edges = graph.traversal().E().hasLabel("write")
                                    .toList();
            Assert.assertEquals(2, edges.size());
        }
    }

    @Test
    public void testAddEdgeLabelWithTTL() {
        super.initPropertyKeys();

        SchemaManager schema = graph().schema();

        schema.propertyKey("date").asDate().ifNotExist().create();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("read").link("person", "book")
                  .properties("date", "weight")
                  .ttl(-86400L)
                  .create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("read").link("person", "book")
                  .properties("date", "weight")
                  .ttlStartTime("date")
                  .create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("read").link("person", "book")
                  .properties("date", "weight")
                  .ttlStartTime("name")
                  .create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("read").link("person", "book")
                  .properties("date", "weight")
                  .ttlStartTime("weight")
                  .create();
        });

        EdgeLabel read = schema.edgeLabel("read").link("person", "book")
                               .properties("date", "weight")
                               .ttl(86400L)
                               .create();

        Assert.assertNotNull(read);
        Assert.assertEquals("read", read.name());
        assertVLEqual("person", read.sourceLabel());
        assertVLEqual("book", read.targetLabel());
        Assert.assertEquals(2, read.properties().size());
        assertContainsPk(read.properties(), "date", "weight");
        Assert.assertEquals(0, read.sortKeys().size());
        Assert.assertEquals(Frequency.SINGLE, read.frequency());
        Assert.assertEquals(86400L, read.ttl());
        Assert.assertEquals(IdGenerator.ZERO, read.ttlStartTime());

        EdgeLabel write = schema.edgeLabel("write").link("person", "book")
                                .properties("date", "weight")
                                .ttl(86400L)
                                .ttlStartTime("date")
                                .create();

        Assert.assertNotNull(write);
        Assert.assertEquals("write", write.name());
        assertVLEqual("person", write.sourceLabel());
        assertVLEqual("book", write.targetLabel());
        Assert.assertEquals(2, write.properties().size());
        assertContainsPk(write.properties(), "date", "weight");
        Assert.assertEquals(0, write.sortKeys().size());
        Assert.assertEquals(Frequency.SINGLE, write.frequency());
        Assert.assertEquals(86400L, write.ttl());
        Assert.assertNotNull(write.ttlStartTime());
        assertContainsPk(ImmutableSet.of(write.ttlStartTime()), "date");
    }

    @Test
    public void testAppendEdgeLabelWithTTL() {
        super.initPropertyKeys();

        SchemaManager schema = graph().schema();

        schema.propertyKey("date").asDate().ifNotExist().create();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        // Create an edge label without TTL
        EdgeLabel read = schema.edgeLabel("read").link("person", "book")
                               .properties("date", "weight")
                               .create();

        Assert.assertNotNull(read);
        Assert.assertEquals("read", read.name());
        Assert.assertEquals(0L, read.ttl());

        // Update the edge label with TTL via append
        read = schema.edgeLabel("read")
                     .ttl(86400L)
                     .append();

        Assert.assertNotNull(read);
        Assert.assertEquals("read", read.name());
        Assert.assertEquals(86400L, read.ttl());
    }

    @Test
    public void testAppendEdgeLabelResetTTL() {
        super.initPropertyKeys();

        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        // Create an edge label with TTL
        EdgeLabel read = schema.edgeLabel("read").link("person", "book")
                               .properties("time", "weight")
                               .ttl(86400L)
                               .create();

        Assert.assertNotNull(read);
        Assert.assertEquals("read", read.name());
        Assert.assertEquals(86400L, read.ttl());

        // Reset TTL to 0 via append
        read = schema.edgeLabel("read")
                     .ttl(0L)
                     .append();

        Assert.assertNotNull(read);
        Assert.assertEquals("read", read.name());
        Assert.assertEquals(0L, read.ttl());
    }

    @Test
    public void testAppendEdgeLabelWithoutTTLShouldNotClearExistingTTL() {
        super.initPropertyKeys();

        SchemaManager schema = graph().schema();

        schema.propertyKey("date").asDate().ifNotExist().create();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        // Create edge label with TTL and ttlStartTime
        EdgeLabel read = schema.edgeLabel("read").link("person", "book")
                               .properties("date", "weight")
                               .ttl(86400L)
                               .ttlStartTime("date")
                               .create();

        Assert.assertNotNull(read);
        Assert.assertEquals(86400L, read.ttl());
        Assert.assertNotNull(read.ttlStartTime());
        assertContainsPk(ImmutableSet.of(read.ttlStartTime()), "date");

        // Append property WITHOUT specifying ttl
        read = schema.edgeLabel("read")
                     .nullableKeys("weight")
                     .append();

        // Both TTL and ttlStartTime should remain unchanged
        Assert.assertNotNull(read);
        Assert.assertEquals(86400L, read.ttl());
        Assert.assertNotNull(read.ttlStartTime());
        assertContainsPk(ImmutableSet.of(read.ttlStartTime()), "date");
    }

    @Test
    public void testAppendEdgeLabelWithUndefinedNullableKeys() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time", "weight")
              .nullableKeys("time")
              .link("person", "book")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look").nullableKeys("undefined").append();
        });
    }

    @Test
    public void testAppendEdgeLabelWithNullableKeysInOriginProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time", "weight")
              .link("person", "book")
              .create();

        EdgeLabel look = schema.edgeLabel("look")
                               .nullableKeys("weight")
                               .append();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        assertVLEqual("person", look.sourceLabel());
        assertVLEqual("book", look.targetLabel());
        Assert.assertEquals(2, look.properties().size());
        assertContainsPk(look.properties(), "time", "weight");
        Assert.assertEquals(1, look.nullableKeys().size());
        assertContainsPk(look.nullableKeys(), "weight");
    }

    @Test
    public void testAppendEdgeLabelWithNullableKeysInAppendProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time")
              .link("person", "book")
              .create();

        EdgeLabel look = schema.edgeLabel("look")
                               .properties("weight")
                               .nullableKeys("weight")
                               .append();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        assertVLEqual("person", look.sourceLabel());
        assertVLEqual("book", look.targetLabel());
        Assert.assertEquals(2, look.properties().size());
        assertContainsPk(look.properties(), "time", "weight");
        Assert.assertEquals(1, look.nullableKeys().size());
        assertContainsPk(look.nullableKeys(), "weight");
    }

    @Test
    public void testAppendEdgeLabelWithNullableKeysNotInProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time")
              .link("person", "book")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .nullableKeys("contribution")
                  .append();
        });
    }

    @Test
    public void testAppendEdgeLabelNewEdgeWithNullableKeysAbsent() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time", "weight")
              .nullableKeys("weight")
              .link("person", "book")
              .create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book", "id", 123456,
                                        "name", "Java in action");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Absent 'time'
            baby.addEdge("look", java, "weight", 0.5);
        });

        schema.edgeLabel("look")
              .nullableKeys("time")
              .append();
        // Absent 'time'
        Edge edge = baby.addEdge("look", java, "weight", 0.5);
        Assert.assertEquals("look", edge.label());
        Assert.assertEquals(0.5, edge.property("weight").value());
    }

    @Test
    public void testAppendEdgeLabelNewEdgeWithNonNullKeysAbsent() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time", "weight")
              .link("person", "book")
              .create();

        Vertex baby = graph().addVertex(T.label, "person", "name", "Baby",
                                        "age", 3, "city", "Beijing");
        Vertex java = graph().addVertex(T.label, "book", "id", 123456,
                                        "name", "Java in action");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Absent 'time'
            baby.addEdge("look", java, "weight", 0.5);
        });

        schema.edgeLabel("look")
              .nullableKeys("weight")
              .append();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            // Absent 'time'
            baby.addEdge("look", java, "weight", 0.5);
        });
    }

    @Test
    public void testAppendEdgeLabelWithNonNullProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time")
              .link("person", "book")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .properties("weight")
                  .append();
        });

    }

    @Test
    public void testAppendEdgeLabelWithSkAsNullableProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        schema.edgeLabel("look")
              .properties("time")
              .link("person", "book")
              .multiTimes().sortKeys("time")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.edgeLabel("look")
                  .properties("time", "weight")
                  .nullableKeys("time", "weight")
                  .append();
        });

    }

    @Test
    public void testRemoveEdgeLabel() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();

        schema.vertexLabel("book")
              .properties("name", "contribution")
              .primaryKeys("name")
              .create();

        schema.edgeLabel("look").link("person", "book")
              .properties("time", "city")
              .create();

        Assert.assertNotNull(schema.getEdgeLabel("look"));

        schema.edgeLabel("look").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getEdgeLabel("look");
        });
    }

    @Test
    public void testRemoveNotExistEdgeLabel() {
        SchemaManager schema = graph().schema();
        schema.edgeLabel("not-exist-el").remove();
    }

    @Test
    public void testRemoveEdgeLabelWithEdge() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        schema.edgeLabel("write").link("person", "book")
              .properties("time", "weight")
              .create();

        Vertex marko = graph().addVertex(T.label, "person", "name", "marko",
                                         "age", 22);
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "java in action");
        Vertex hadoop = graph().addVertex(T.label, "book",
                                          "name", "hadoop mapreduce");

        marko.addEdge("write", java, "time", "2016-12-12",
                      "weight", 0.3);
        marko.addEdge("write", hadoop, "time", "2014-2-28",
                      "weight", 0.5);
        graph().tx().commit();

        List<Edge> edges = graph().traversal().E().hasLabel("write").toList();
        Assert.assertNotNull(edges);
        Assert.assertEquals(2, edges.size());

        schema.edgeLabel("write").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getEdgeLabel("write");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().traversal().E().hasLabel("write").toList();
        });
    }

    @Test
    public void testRemoveEdgeLabelWithEdgeAndRangeIndex() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        schema.edgeLabel("write").link("person", "book")
              .properties("time", "weight")
              .create();

        schema.indexLabel("writeByWeight").onE("write").by("weight")
              .range()
              .create();

        Vertex marko = graph().addVertex(T.label, "person", "name", "marko",
                                         "age", 22);
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "java in action");
        Vertex hadoop = graph().addVertex(T.label, "book",
                                          "name", "hadoop mapreduce");

        marko.addEdge("write", java, "time", "2016-12-12",
                      "weight", 0.3);
        marko.addEdge("write", hadoop, "time", "2014-2-28",
                      "weight", 0.5);
        graph().tx().commit();

        List<Edge> edges = graph().traversal().E().hasLabel("write")
                                  .has("weight", 0.5).toList();
        Assert.assertNotNull(edges);
        Assert.assertEquals(1, edges.size());

        schema.edgeLabel("write").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getEdgeLabel("write");
        });

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("writeByWeight");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().traversal().E().hasLabel("write").toList();
        });
    }

    @Test
    public void testRemoveEdgeLabelWithEdgeAndSecondaryIndex() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        schema.edgeLabel("write").link("person", "book")
              .properties("time", "weight")
              .create();

        schema.indexLabel("writeByTime").onE("write").by("time")
              .secondary()
              .create();

        Vertex marko = graph().addVertex(T.label, "person", "name", "marko",
                                         "age", 22);
        Vertex java = graph().addVertex(T.label, "book",
                                        "name", "java in action");
        Vertex hadoop = graph().addVertex(T.label, "book",
                                          "name", "hadoop mapreduce");

        marko.addEdge("write", java, "time", "2016-12-12", "weight", 0.3);
        marko.addEdge("write", hadoop, "time", "2014-2-28", "weight", 0.5);
        graph().tx().commit();

        List<Edge> edges = graph().traversal().E().hasLabel("write")
                                  .has("time", "2016-12-12").toList();
        Assert.assertNotNull(edges);
        Assert.assertEquals(1, edges.size());

        schema.edgeLabel("write").remove();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getEdgeLabel("write");
        });

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("writeByTime");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().traversal().E().hasLabel("write").toList();
        });
    }

    @Test
    public void testRebuildIndexOfEdgeLabelWithoutLabelIndex() {
        Assume.assumeFalse("Support query by label",
                           storeFeatures().supportsQueryByLabel());

        initDataWithoutLabelIndex();

        // Not support query by label
        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().E().hasLabel("read").toList();
        }, e -> {
            Assert.assertTrue(
                    e.getMessage().startsWith("Don't accept query by label") &&
                    e.getMessage().endsWith("label index is disabled"));
        });

        // Query by property index is ok
        List<Edge> edges = graph().traversal().E()
                                  .has("date", P.lt("2019-12-30 13:00:00"))
                                  .toList();
        Assert.assertEquals(20, edges.size());

        graph().schema().edgeLabel("read").rebuildIndex();

        edges = graph().traversal().E()
                       .has("date", P.lt("2019-12-30 13:00:00")).toList();
        Assert.assertEquals(20, edges.size());
    }

    @Test
    public void testRemoveEdgeLabelWithoutLabelIndex() {
        Assume.assumeFalse("Support query by label",
                           storeFeatures().supportsQueryByLabel());

        initDataWithoutLabelIndex();

        // Not support query by label
        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().E().hasLabel("read").toList();
        }, e -> {
            Assert.assertTrue(
                    e.getMessage().startsWith("Don't accept query by label") &&
                    e.getMessage().endsWith("label index is disabled"));
        });

        // Query by property index is ok
        List<Edge> edges = graph().traversal().E()
                                  .has("date", P.lt("2019-12-30 13:00:00"))
                                  .toList();
        Assert.assertEquals(20, edges.size());

        graph().schema().edgeLabel("read").remove();

        Assert.assertThrows(NoIndexException.class, () ->
                graph().traversal().E()
                       .has("date", P.lt("2019-12-30 13:00:00")).toList()
        );
    }

    @Test
    public void testAddEdgeLabelWithUserdata() {
        super.initPropertyKeys();

        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        EdgeLabel father = schema.edgeLabel("father").link("person", "person")
                                 .properties("weight")
                                 .userdata("multiplicity", "one-to-many")
                                 .create();

        Assert.assertEquals(2, father.userdata().size());
        Assert.assertEquals("one-to-many",
                            father.userdata().get("multiplicity"));

        EdgeLabel write = schema.edgeLabel("write").link("person", "book")
                                .properties("time", "weight")
                                .userdata("multiplicity", "one-to-many")
                                .userdata("multiplicity", "many-to-many")
                                .create();
        // The same key user data will be overwritten
        Assert.assertEquals(2, write.userdata().size());
        Assert.assertEquals("many-to-many",
                            write.userdata().get("multiplicity"));
    }

    @Test
    public void testAppendEdgeLabelWithUserdata() {
        super.initPropertyKeys();

        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        EdgeLabel write = schema.edgeLabel("write").link("person", "book")
                                .properties("time", "weight")
                                .userdata("multiplicity", "one-to-many")
                                .create();
        // The same key user data will be overwritten
        Assert.assertEquals(2, write.userdata().size());
        Assert.assertEquals("one-to-many",
                            write.userdata().get("multiplicity"));

        write = schema.edgeLabel("write")
                      .userdata("icon", "picture2")
                      .append();
        Assert.assertEquals(3, write.userdata().size());
        Assert.assertEquals("one-to-many",
                            write.userdata().get("multiplicity"));
        Assert.assertEquals("picture2", write.userdata().get("icon"));
    }

    @Test
    public void testEliminateEdgeLabelWithUserdata() {
        super.initPropertyKeys();

        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        EdgeLabel write = schema.edgeLabel("write").link("person", "book")
                                .properties("time", "weight")
                                .userdata("multiplicity", "one-to-many")
                                .userdata("icon", "picture2")
                                .create();
        Assert.assertEquals(3, write.userdata().size());
        Assert.assertEquals("one-to-many",
                            write.userdata().get("multiplicity"));
        Assert.assertEquals("picture2", write.userdata().get("icon"));

        write = schema.edgeLabel("write")
                      .userdata("icon", "")
                      .eliminate();
        Assert.assertEquals(2, write.userdata().size());
        Assert.assertEquals("one-to-many",
                            write.userdata().get("multiplicity"));
    }

    @Test
    public void testEliminateVertexLabelWithoutUserdata() {
        super.initPropertyKeys();

        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("person2")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .nullableKeys("city")
              .create();

        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .create();

        schema.edgeLabel("write")
              .link("person", "book")
              .multiTimes()
              .properties("time", "weight")
              .sortKeys("time")
              .nullableKeys("weight")
              .userdata("multiplicity", "one-to-many")
              .create();

        Assert.assertThrows(HugeException.class, () -> {
            schema.edgeLabel("write").sourceLabel("person2").eliminate();
        });

        Assert.assertThrows(HugeException.class, () -> {
            schema.edgeLabel("write").targetLabel("person2").eliminate();
        });

        Assert.assertThrows(HugeException.class, () -> {
            schema.edgeLabel("write").singleTime().eliminate();
        });

        Assert.assertThrows(HugeException.class, () -> {
            schema.edgeLabel("write").sortKeys("time").eliminate();
        });

        Assert.assertThrows(HugeException.class, () -> {
            schema.edgeLabel("write").enableLabelIndex(false).eliminate();
        });

        Assert.assertThrows(HugeException.class, () -> {
            schema.edgeLabel("write").properties("weight").eliminate();
        });

        Assert.assertThrows(HugeException.class, () -> {
            schema.edgeLabel("write").nullableKeys("weight").eliminate();
        });
    }

    @Test
    public void testListEdgeLabels() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        EdgeLabel look = schema.edgeLabel("look").multiTimes()
                               .properties("time")
                               .link("person", "book")
                               .sortKeys("time")
                               .create();
        EdgeLabel write = schema.edgeLabel("write").multiTimes()
                                .properties("time")
                                .link("author", "book")
                                .sortKeys("time")
                                .create();

        List<EdgeLabel> edgeLabels = schema.getEdgeLabels();
        Assert.assertEquals(2, edgeLabels.size());
        Assert.assertTrue(edgeLabels.contains(look));
        Assert.assertTrue(edgeLabels.contains(write));

        // clear cache
        params().schemaEventHub().call(Events.CACHE, "clear", null);

        Assert.assertEquals(look, schema.getEdgeLabel("look"));

        edgeLabels = schema.getEdgeLabels();
        Assert.assertEquals(2, edgeLabels.size());
        Assert.assertTrue(edgeLabels.contains(look));
        Assert.assertTrue(edgeLabels.contains(write));
    }

    @Test
    public void testCreateTime() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("book").properties("id", "name")
              .primaryKeys("id").create();
        EdgeLabel look = schema.edgeLabel("look").multiTimes()
                               .properties("time")
                               .link("person", "book")
                               .sortKeys("time")
                               .create();

        Date createTime = (Date) look.userdata().get(Userdata.CREATE_TIME);
        Date now = DateUtil.now();
        Assert.assertFalse(createTime.after(now));

        look = schema.getEdgeLabel("look");
        createTime = (Date) look.userdata().get(Userdata.CREATE_TIME);
        Assert.assertFalse(createTime.after(now));
    }

    @Test
    public void testDuplicateEdgeLabelWithIdentityProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .ifNotExist()
              .create();
        schema.vertexLabel("book")
              .properties("id", "name")
              .primaryKeys("id")
              .ifNotExist()
              .create();

        schema.edgeLabel("friend")
              .sourceLabel("person").targetLabel("person")
              .ifNotExist().create();
        schema.edgeLabel("friend")
              .sourceLabel("person").targetLabel("person")
              .ifNotExist().create();

        schema.edgeLabel("follow")
              .sourceLabel("person").targetLabel("person")
              .properties("time", "weight")
              .ifNotExist().create();
        schema.edgeLabel("follow")
              .properties("weight", "time")
              .sourceLabel("person").targetLabel("person")
              .ifNotExist().create();

        schema.edgeLabel("looks")
              .multiTimes()
              .properties("time")
              .link("person", "book")
              .sortKeys("time")
              .ifNotExist()
              .create();
        schema.edgeLabel("looks")
              .multiTimes()
              .properties("time")
              .link("person", "book")
              .sortKeys("time")
              .checkExist(false)
              .create();
    }

    @Test
    public void testDuplicateEdgeLabelWithDifferentProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .ifNotExist()
              .create();
        schema.vertexLabel("book")
              .properties("id", "name")
              .primaryKeys("id")
              .ifNotExist().create();

        schema.edgeLabel("friend")
              .link("person", "person")
              .properties("time")
              .ifNotExist()
              .create();
        Assert.assertThrows(ExistedException.class, () -> {
            schema.edgeLabel("friend")
                  .link("person", "book")
                  .properties() // no time property
                  .ifNotExist()
                  .create();
        });

        schema.edgeLabel("looks")
              .multiTimes()
              .properties("time")
              .link("person", "book")
              .sortKeys("time")
              .ifNotExist()
              .create();
        Assert.assertThrows(ExistedException.class, () -> {
            schema.edgeLabel("looks").multiTimes()
                  .properties("time", "city") // add city
                  .link("person", "book")
                  .sortKeys("time")
                  .checkExist(false)
                  .create();
        });
        Assert.assertThrows(ExistedException.class, () -> {
            schema.edgeLabel("looks").multiTimes()
                  .properties("time", "city")
                  .link("person", "person") // not person->book
                  .sortKeys("time")
                  .checkExist(false)
                  .create();
        });
        Assert.assertThrows(ExistedException.class, () -> {
            schema.edgeLabel("looks").multiTimes()
                  .properties("time", "city")
                  .link("person", "person") // no sortKeys
                  .checkExist(false)
                  .create();
        });
    }
}
