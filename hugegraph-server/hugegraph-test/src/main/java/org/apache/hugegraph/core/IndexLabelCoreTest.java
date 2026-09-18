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
import org.apache.hugegraph.exception.ExistedException;
import org.apache.hugegraph.exception.NoIndexException;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.IndexType;
import org.apache.hugegraph.type.define.WriteType;
import org.apache.hugegraph.util.DateUtil;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class IndexLabelCoreTest extends SchemaCoreTest {

    @Test
    public void testAddIndexLabelOfVertex() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.propertyKey("born").asDate().ifNotExist().create();
        schema.propertyKey("fans").asLong().ifNotExist().create();
        schema.propertyKey("height").asFloat().ifNotExist().create();
        schema.propertyKey("idNo").asText().ifNotExist().create();
        schema.propertyKey("category").asText().valueSet().ifNotExist()
              .create();
        schema.propertyKey("score").asInt().valueSet().ifNotExist()
              .create();
        schema.vertexLabel("person")
              .properties("id", "name", "age", "city", "born", "tags",
                          "category", "score",
                          "fans", "height", "weight", "idNo")
              .primaryKeys("id").create();
        schema.indexLabel("personByName").onV("person").secondary()
              .by("name").create();
        schema.indexLabel("personByCity").onV("person").search()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        schema.indexLabel("personByBorn").onV("person").range()
              .by("born").create();
        schema.indexLabel("personByFans").onV("person").range()
              .by("fans").create();
        schema.indexLabel("personByHeight").onV("person").range()
              .by("height").create();
        schema.indexLabel("personByWeight").onV("person").range()
              .by("weight").create();
        schema.indexLabel("personByIdNo").onV("person").unique()
              .by("idNo").create();
        schema.indexLabel("personByTags").onV("person").secondary()
              .by("tags").create();
        schema.indexLabel("personByCategory").onV("person").search()
              .by("category").create();
        schema.indexLabel("personByScore").onV("person").secondary()
              .by("score").create();

        VertexLabel person = schema.getVertexLabel("person");
        IndexLabel personByName = schema.getIndexLabel("personByName");
        IndexLabel personByCity = schema.getIndexLabel("personByCity");
        IndexLabel personByAge = schema.getIndexLabel("personByAge");
        IndexLabel personByBorn = schema.getIndexLabel("personByBorn");
        IndexLabel personByFans = schema.getIndexLabel("personByFans");
        IndexLabel personByHeight = schema.getIndexLabel("personByHeight");
        IndexLabel personByWeight = schema.getIndexLabel("personByWeight");
        IndexLabel personByIdNo = schema.getIndexLabel("personByIdNo");
        IndexLabel personByTags = schema.getIndexLabel("personByTags");
        IndexLabel personByCategory = schema.getIndexLabel("personByCategory");
        IndexLabel personByScore = schema.getIndexLabel("personByScore");

        Assert.assertNotNull(personByName);
        Assert.assertNotNull(personByCity);
        Assert.assertNotNull(personByAge);
        Assert.assertNotNull(personByBorn);
        Assert.assertNotNull(personByFans);
        Assert.assertNotNull(personByHeight);
        Assert.assertNotNull(personByWeight);
        Assert.assertNotNull(personByIdNo);
        Assert.assertNotNull(personByTags);
        Assert.assertNotNull(personByCategory);

        Assert.assertEquals(11, person.indexLabels().size());
        assertContainsIl(person.indexLabels(),
                         "personByName", "personByCity", "personByAge",
                         "personByBorn", "personByFans", "personByHeight",
                         "personByWeight", "personByIdNo", "personByTags",
                         "personByCategory", "personByScore");

        Assert.assertEquals(HugeType.VERTEX_LABEL, personByName.baseType());
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByCity.baseType());
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByAge.baseType());
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByBorn.baseType());
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByFans.baseType());
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByHeight.baseType());
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByWeight.baseType());
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByIdNo.baseType());
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByTags.baseType());
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByCategory.baseType());
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByScore.baseType());

        assertVLEqual("person", personByName.baseValue());
        assertVLEqual("person", personByCity.baseValue());
        assertVLEqual("person", personByAge.baseValue());
        assertVLEqual("person", personByBorn.baseValue());
        assertVLEqual("person", personByFans.baseValue());
        assertVLEqual("person", personByHeight.baseValue());
        assertVLEqual("person", personByWeight.baseValue());
        assertVLEqual("person", personByIdNo.baseValue());
        assertVLEqual("person", personByTags.baseValue());
        assertVLEqual("person", personByCategory.baseValue());
        assertVLEqual("person", personByScore.baseValue());

        Assert.assertEquals(IndexType.SECONDARY, personByName.indexType());
        Assert.assertEquals(IndexType.SEARCH, personByCity.indexType());
        Assert.assertEquals(IndexType.RANGE_INT, personByAge.indexType());
        Assert.assertEquals(IndexType.RANGE_LONG, personByBorn.indexType());
        Assert.assertEquals(IndexType.RANGE_LONG, personByFans.indexType());
        Assert.assertEquals(IndexType.RANGE_FLOAT, personByHeight.indexType());
        Assert.assertEquals(IndexType.RANGE_DOUBLE, personByWeight.indexType());
        Assert.assertEquals(IndexType.UNIQUE, personByIdNo.indexType());
        Assert.assertEquals(IndexType.SECONDARY, personByTags.indexType());
        Assert.assertEquals(IndexType.SEARCH, personByCategory.indexType());
        Assert.assertEquals(IndexType.SECONDARY, personByScore.indexType());
    }

    @Test
    public void testAddIndexLabelWithIllegalName() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        // Empty string
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("").onV("person").by("name").create();
        });
        // One space
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel(" ").onV("person").by("name").create();
        });
        // Two spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("  ").onV("person").by("name").create();
        });
        // Multi spaces
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("    ").onV("person").by("name").create();
        });
        // Start with '~'
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("~").onV("person").by("name").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("~ ").onV("person").by("name").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("~x").onV("person").by("name").create();
        });
    }

    @Test
    public void testAddIndexLabelOfEdge() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime()
              .link("author", "book")
              .properties("contribution", "tags")
              .create();

        schema.indexLabel("authoredByContri").onE("authored").secondary()
              .by("contribution").create();
        schema.indexLabel("authoredByTags").onE("authored").secondary()
              .by("tags").create();

        EdgeLabel authored = schema.getEdgeLabel("authored");
        IndexLabel authoredByContri = schema.getIndexLabel("authoredByContri");
        IndexLabel authoredByTags = schema.getIndexLabel("authoredByTags");

        Assert.assertNotNull(authoredByContri);
        Assert.assertEquals(2, authored.indexLabels().size());
        assertContainsIl(authored.indexLabels(), "authoredByContri",
                         "authoredByTags");

        Assert.assertEquals(HugeType.EDGE_LABEL, authoredByContri.baseType());
        Assert.assertEquals(HugeType.EDGE_LABEL, authoredByTags.baseType());

        assertELEqual("authored", authoredByContri.baseValue());
        assertELEqual("authored", authoredByTags.baseValue());

        Assert.assertEquals(IndexType.SECONDARY, authoredByContri.indexType());
        Assert.assertEquals(IndexType.SECONDARY, authoredByTags.indexType());
    }

    @Test
    public void testAddIndexLabelOfVertexWithVertexExist() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        graph().addVertex(T.label, "person", "name", "Baby",
                          "city", "Hongkong", "age", 3);
        graph().tx().commit();

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V().hasLabel("person")
                   .has("city", "Hongkong").next();
        });

        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();

        Vertex vertex = graph().traversal().V().hasLabel("person")
                               .has("city", "Hongkong").next();
        Assert.assertNotNull(vertex);

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V().hasLabel("person")
                   .has("age", P.inside(2, 4)).next();
        });
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();

        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);
    }

    @Test
    public void testAddIndexLabelOfEdgeWithEdgeExist() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime().link("author", "book")
              .properties("contribution").create();

        Vertex james = graph().addVertex(T.label, "author", "id", 1,
                                         "name", "James Gosling");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "java-1");

        james.addEdge("authored", java1, "contribution", "test");

        graph().tx().commit();

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().E().hasLabel("authored")
                   .has("contribution", "test").next();
        });

        schema.indexLabel("authoredByContri").onE("authored")
              .secondary().by("contribution").create();

        Edge edge = graph().traversal().E().hasLabel("authored")
                           .has("contribution", "test").next();
        Assert.assertNotNull(edge);
    }

    @Test
    public void testAddIndexLabelOnUndefinedSchemaLabel() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authorByName").onV("undefined-vertex-label")
                  .by("name").secondary().create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authoredByContri").onE("undefined-edge-label")
                  .by("contribution").secondary().create();
        });
    }

    @Test
    public void testAddIndexLabelByUndefinedProperty() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime().link("author", "book")
              .properties("contribution").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authorByData").onV("author")
                  .by("undefined-property").secondary().create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authoredByData").onE("authored")
                  .by("undefined-property").secondary().create();
        });
    }

    @Test
    public void testAddIndexLabelByNotBelongedProperty() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime().link("author", "book")
              .properties("contribution").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("bookById").onV("book")
                  .by("id").secondary().create();
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authoredByName").onE("authored")
                  .by("name").secondary().create();
        });
    }

    @Test
    public void testAddIndexLabelWithInvalidFields() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("author").properties("id", "name", "age")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.vertexLabel("soft").properties("name", "tags", "score")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime().link("author", "book")
              .properties("contribution", "age", "weight").create();

        // Invalid range-index
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authorByName").onV("author")
                  .by("name").range().create();
        }, e -> {
            Assert.assertContains("Range index can only build on numeric",
                                  e.getMessage());
        });

        // Collection index not support union index
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("softByNameAndTags").onV("soft")
                  .by("name", "tags").secondary().create();
        }, e -> {
            Assert.assertContains("Not allowed to build union index",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("softByScore").onV("soft")
                  .by("score").range().create();
        }, e -> {
            Assert.assertContains("Not allowed to build range index",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authoredByAgeAndWeight").onE("authored")
                  .by("age", "weight").range().create();
        }, e -> {
            Assert.assertContains("Range index can only build on one field",
                                  e.getMessage());
        });

        // Invalid search-index
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authorByAge").onV("author")
                  .by("age").search().create();
        }, e -> {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                    "Search index can only build on text"));
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authorByNameAndAge").onV("author")
                  .by("name", "age").search().create();
        }, e -> {
            Assert.assertContains("Search index can only build on one field",
                                  e.getMessage());
        });
    }

    @Test
    public void testAddIndexLabelWithInvalidFieldsForAggregateProperty() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.propertyKey("sumProp").asLong().valueSingle().calcSum()
              .ifNotExist().create();
        schema.vertexLabel("author")
              .properties("id", "name", "age", "sumProp")
              .primaryKeys("id").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authorBySumProp")
                  .onV("author").by("sumProp").secondary()
                  .ifNotExist().create();
        }, e -> {
            Assert.assertContains("The aggregate type SUM is not indexable",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authorBySumProp")
                  .onV("author").by("sumProp").range()
                  .ifNotExist().create();
        }, e -> {
            Assert.assertContains("The aggregate type SUM is not indexable",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("authorBySumProp")
                  .onV("author").by("sumProp").shard()
                  .ifNotExist().create();
        }, e -> {
            Assert.assertContains("The aggregate type SUM is not indexable",
                                  e.getMessage());
        });
    }

    @Test
    public void testAddIndexLabelWithFieldsAssignedMultiTimes() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByCity").onV("person").secondary()
                  .by("city").by("city").create();
        });
    }

    @Test
    public void testAddIndexLabelWithFieldsContainSameProp() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAgeAndCity").onV("person").secondary()
                  .by("age", "city", "age").create();
        });
    }

    @Test
    public void testAddIndexLabelWithSameFieldsBetweenRangeSecondary() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAge2").onV("person").secondary()
                  .by("age").create();
        });
    }

    @Test
    public void testAddIndexLabelWithSameFieldsBetweenSearchSecondary() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        schema.indexLabel("personByCity").onV("person").search()
              .by("city").create();
        schema.indexLabel("personByCity2").onV("person").secondary()
              .by("city").create();
    }

    @Test
    public void testAddIndexLabelWithSameFieldsAndSameIndexType() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByCity1").onV("person").secondary()
                  .by("city").create();
        });

        schema.indexLabel("personByCitySearch").onV("person").search()
              .by("city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByCitySearch1").onV("person").search()
                  .by("city").create();
        });

        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAge1").onV("person").range()
                  .by("age").create();
        });

        schema.indexLabel("personByAgeAndCity").onV("person").secondary()
              .by("age", "city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAgeAndCity1").onV("person").secondary()
                  .by("age", "city").create();
        });
    }

    @Test
    public void testAddIndexLabelRangePrefixOfExistedSecondary() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        schema.indexLabel("personByAgeAndCity").onV("person").secondary()
              .by("age", "city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
    }

    @Test
    public void testAddIndexLabelSecondaryPrefixOfExistedSecondary() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        schema.indexLabel("personByAgeAndCity").onV("person").secondary()
              .by("age", "city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAge").onV("person").secondary()
                  .by("age").create();
        });
    }

    @Test
    public void testAddIndexLabelSecondaryPrefixWithExistedSecondary() {
        super.initPropertyKeys();
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        graph.addVertex(T.label, "person", "name", "Baby",
                        "city", "Hongkong", "age", 3);
        graph.tx().commit();

        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAgeSecondary").onV("person").secondary()
                  .by("age").create();
        });

        List<Vertex> vertices;
        vertices = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V().has("city", "Hongkong")
                          .has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());

        schema.indexLabel("personByCityAndAge").onV("person").secondary()
              .by("city", "age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByCity");
        });
        schema.getIndexLabel("personByAge");

        vertices = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph.traversal().V().has("city", "Hongkong")
                        .has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testAddIndexLabelSecondaryPrefixWithExistedRange() {
        super.initPropertyKeys();
        HugeGraph graph = graph();
        SchemaManager schema = graph.schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        graph.addVertex(T.label, "person", "name", "Baby",
                        "city", "Hongkong", "age", 3);
        graph.tx().commit();

        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAgeSecondary").onV("person").secondary()
                  .by("age").create();
        });

        List<Vertex> vertices;
        vertices = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());

        vertices = graph().traversal().V().has("city", "Hongkong")
                          .has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());

        schema.indexLabel("personByAgeAndCity").onV("person").secondary()
              .by("age", "city").create();
        schema.getIndexLabel("personByAge");

        vertices = graph.traversal().V().has("city", "Hongkong").toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph.traversal().V().has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = graph.traversal().V().has("city", "Hongkong")
                        .has("age", 3).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testAddIndexLabelOnPrimaryKeyProps() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByName").onV("person").secondary()
                  .by("name").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByNameAge").onV("person").secondary()
                  .by("name", "age").create();
        });

        schema.vertexLabel("person1").properties("name", "age", "city")
              .primaryKeys("age").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person1ByAge").onV("person1").secondary()
                  .by("age").create();
        });
        schema.indexLabel("person1ByAge").onV("person").range()
              .by("age").create();

        schema.vertexLabel("student").properties("name", "age", "city")
              .primaryKeys("name", "age").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("studentByNameAge").onV("student").secondary()
                  .by("name", "age").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("studentByAgeName").onV("student").secondary()
                  .by("age", "name").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("studentByNameAgeCity").onV("student").secondary()
                  .by("name", "age", "city").create();
        });
        schema.indexLabel("studentByName").onV("student").secondary()
              .by("name").create();

        schema.indexLabel("studentByNameAge").onV("student").shard()
              .by("name", "age").create();
        schema.indexLabel("studentByAgeName").onV("student").shard()
              .by("age", "name").create();
        schema.indexLabel("studentByNameAgeCity").onV("student").shard()
              .by("name", "age", "city").create();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("studentByName");
        });
    }

    @Test
    public void testAddShardIndexLabel() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city", "weight")
              .primaryKeys("name").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByName").onV("person").shard()
                  .by("name").create();
        });
        schema.indexLabel("personByCity").onV("person").shard()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").shard()
              .by("age").create();
        schema.indexLabel("personByWeight").onV("person").shard()
              .by("weight").create();
        schema.getIndexLabel("personByCity");
        schema.getIndexLabel("personByAge");
        schema.getIndexLabel("personByWeight");

        schema.indexLabel("personByNameAndAge").onV("person").shard()
              .by("name", "age").create();

        schema.indexLabel("personByCityAndAge").onV("person").shard()
              .by("city", "age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByCity");
        });
        schema.getIndexLabel("personByAge");
        schema.getIndexLabel("personByWeight");

        schema.indexLabel("personByAgeAndCity").onV("person").shard()
              .by("age", "city").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByCity");
        });
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByAge");
        });
        schema.getIndexLabel("personByWeight");

        schema.indexLabel("personByNameAgeCityWeight").onV("person").shard()
              .by("name", "age", "city", "weight").create();
    }

    @Test
    public void testAddUniqueIndexLabel() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city", "weight")
              .primaryKeys("name").create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByName").onV("person").unique()
                  .by("name").create();
        });
        schema.indexLabel("personByCity").onV("person").unique()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").unique()
              .by("age").create();
        schema.indexLabel("personByWeight").onV("person").unique()
              .by("weight").create();
        schema.getIndexLabel("personByCity");
        schema.getIndexLabel("personByAge");
        schema.getIndexLabel("personByWeight");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByNameAndAge").onV("person").unique()
                  .by("name", "age").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByCityAndAge").onV("person").unique()
                  .by("city", "age").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAgeAndCity").onV("person").unique()
                  .by("age", "city").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByNameAgeCityWeight").onV("person")
                  .unique().by("name", "age", "city", "weight").create();
        });
    }

    @Test
    public void testAddIndexLabelWithRepeatIndex() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city", "weight")
              .create();
        // Repeat index tests for existed range index
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAge1").onV("person").range()
                  .by("age").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAge2").onV("person").secondary()
                  .by("age").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByAge3").onV("person").shard()
                  .by("age").create();
        });
        schema.indexLabel("personByAge4").onV("person").unique()
              .by("age").create();
        schema.getIndexLabel("personByAge");
        schema.getIndexLabel("personByAge4");

        // Repeat index tests for existed secondary index(number)
        schema.vertexLabel("person1")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person1ByAge").onV("person1").secondary()
              .by("age").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person1ByAge1").onV("person1").secondary()
                  .by("age").create();
        });
        schema.indexLabel("person1ByAge2").onV("person1").shard()
              .by("age").create();
        schema.indexLabel("person1ByAge3").onV("person1").range()
              .by("age").create();
        schema.indexLabel("person1ByAge4").onV("person1").unique()
              .by("age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person1ByAge");
        });
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person1ByAge2");
        });
        schema.getIndexLabel("person1ByAge3");
        schema.getIndexLabel("person1ByAge4");
        // Repeat index tests for existed secondary index(string)
        schema.vertexLabel("person2")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person2ByCity").onV("person2").secondary()
              .by("city").create();
        schema.indexLabel("person2ByCity1").onV("person2").search()
              .by("city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person2ByCity2").onV("person2").secondary()
                  .by("city").create();
        });
        schema.indexLabel("person2ByCity3").onV("person2").unique()
              .by("city").create();
        schema.getIndexLabel("person2ByCity");
        schema.getIndexLabel("person2ByCity1");
        schema.getIndexLabel("person2ByCity3");

        // Repeat index tests for existed shard index
        schema.vertexLabel("person3")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person3ByAge").onV("person3").shard()
              .by("age").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person3ByAge1").onV("person3").secondary()
                  .by("age").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person3ByAge2").onV("person3").shard()
                  .by("age").create();
        });
        schema.indexLabel("person3ByAge3").onV("person3").range()
              .by("age").create();
        schema.indexLabel("person3ByAge4").onV("person3").unique()
              .by("age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person3ByAge");
        });
        schema.getIndexLabel("person3ByAge3");
        schema.getIndexLabel("person3ByAge4");

        // Repeat index tests for existed search index
        schema.vertexLabel("person4")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person4ByCity").onV("person4").search()
              .by("city").create();
        schema.indexLabel("person4ByCity1").onV("person4").secondary()
              .by("city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person4ByCity2").onV("person4").search()
                  .by("city").create();
        });
        schema.indexLabel("person4ByCity3").onV("person4").unique()
              .by("city").create();
        schema.getIndexLabel("person4ByCity");
        schema.getIndexLabel("person4ByCity1");
        schema.getIndexLabel("person4ByCity3");

        // Repeat index tests for existed composite secondary index
        schema.vertexLabel("person5")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person5ByCityAndName").onV("person5").secondary()
              .by("city", "name").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person5ByCity1").onV("person5").secondary()
                  .by("city").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person5ByCity2").onV("person5").shard()
                  .by("city").create();
        });
        schema.indexLabel("person5ByCity3").onV("person5").search()
              .by("city").create();
        schema.indexLabel("person5ByCity4").onV("person5").unique()
              .by("city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person5ByCityAndName1").onV("person5")
                  .secondary().by("city", "name").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person5ByCityAndName2").onV("person5").shard()
                  .by("city", "name").create();
        });
        schema.getIndexLabel("person5ByCity3");
        schema.getIndexLabel("person5ByCity4");
        schema.indexLabel("person5ByCity4").remove();
        schema.indexLabel("person5ByCityAndName3").onV("person5").unique()
              .by("city", "name").create();
        schema.getIndexLabel("person5ByCityAndName3");

        // Repeat index tests for existed composite shard index
        schema.vertexLabel("person6")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person6ByCityAndName").onV("person6").shard()
              .by("city", "name").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person6ByCity1").onV("person6").secondary()
                  .by("city").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person6ByCity2").onV("person6").shard()
                  .by("city").create();
        });
        schema.indexLabel("person6ByCity3").onV("person6").search()
              .by("city").create();
        schema.indexLabel("person6ByCity4").onV("person6").unique()
              .by("city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person6ByCityAndName1").onV("person6")
                  .secondary().by("city", "name").create();
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person6ByCityAndName2").onV("person6").shard()
                  .by("city").create();
        });
        schema.getIndexLabel("person6ByCity3");
        schema.getIndexLabel("person6ByCity4");
        schema.indexLabel("person6ByCity4").remove();
        schema.indexLabel("person6ByCityAndName3").onV("person6").unique()
              .by("city", "name").create();
        schema.getIndexLabel("person6ByCityAndName3");

        // Repeat index tests for existed unique index
        schema.vertexLabel("person7")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person7ByCity").onV("person7").unique()
              .by("city").create();
        schema.indexLabel("person7ByCity1").onV("person7").secondary()
              .by("city").create();
        schema.indexLabel("person7ByCity1").remove();
        schema.indexLabel("person7ByCity2").onV("person7").shard()
              .by("city").create();
        schema.indexLabel("person7ByCity3").onV("person7").search()
              .by("city").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person7ByCity4").onV("person7").unique()
                  .by("city").create();
        });
        schema.indexLabel("person7ByCityAndName1").onV("person7")
              .secondary().by("city", "name").create();
        schema.indexLabel("person7ByCityAndName1").remove();
        schema.indexLabel("person7ByCityAndName2").onV("person7").shard()
              .by("city", "name").create();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("person7ByCityAndName3").onV("person7")
                  .unique().by("city", "name").create();
        });
        schema.getIndexLabel("person5ByCity3");
        schema.getIndexLabel("person7ByCityAndName2");
    }

    @Test
    public void testAddIndexLabelWithSubIndex() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        // Composite secondary index override prefix secondary index
        schema.vertexLabel("person")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.getIndexLabel("personByCity");
        schema.indexLabel("personByCityAndName").onV("person").secondary()
              .by("city", "name").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByCity");
        });

        schema.vertexLabel("person1")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person1ByCity").onV("person1").secondary()
              .by("city").create();
        schema.getIndexLabel("person1ByCity");
        schema.indexLabel("person1ByCityAndAge").onV("person1").secondary()
              .by("city", "age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person1ByCity");
        });

        // Composite shard index override prefix shard index
        schema.vertexLabel("person2")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person2ByCity").onV("person2").shard()
              .by("city").create();
        schema.getIndexLabel("person2ByCity");
        schema.indexLabel("person2ByCityAndName").onV("person2").shard()
              .by("city", "name").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person2ByCity");
        });

        schema.vertexLabel("person3")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person3ByCity").onV("person3").shard()
              .by("city").create();
        schema.getIndexLabel("person3ByCity");
        schema.indexLabel("person3ByCityAndAge").onV("person3").shard()
              .by("city", "age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person3ByCity");
        });

        // Composite shard index override prefix secondary index
        schema.vertexLabel("person4")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person4ByCity").onV("person4").secondary()
              .by("city").create();
        schema.getIndexLabel("person4ByCity");
        schema.indexLabel("person4ByCityAndName").onV("person4").shard()
              .by("city", "name").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person4ByCity");
        });

        // Composite secondary index override prefix all string shard index
        schema.vertexLabel("person5")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person5ByCity").onV("person5").shard()
              .by("city").create();
        schema.getIndexLabel("person5ByCity");
        schema.indexLabel("person5ByCityAndAge").onV("person5").secondary()
              .by("city", "age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person5ByCity");
        });

        // Range index override secondary index
        schema.vertexLabel("person6")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person6ByAge").onV("person6").secondary()
              .by("age").create();
        schema.getIndexLabel("person6ByAge");
        schema.indexLabel("person6ByAge1").onV("person6").range()
              .by("age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person6ByAge");
        });

        // Range index override shard index
        schema.vertexLabel("person7")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person7ByAge").onV("person7").shard()
              .by("age").create();
        schema.getIndexLabel("person7ByAge");
        schema.indexLabel("person7ByAge1").onV("person7").range()
              .by("age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person7ByAge");
        });

        // Unique index override more fields unique index
        schema.vertexLabel("person8")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person8ByCityAndAge").onV("person8").unique()
              .by("city", "age").create();
        schema.getIndexLabel("person8ByCityAndAge");
        schema.indexLabel("person8ByAge").onV("person8").unique()
              .by("age").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person8ByCityAndAge");
        });
        schema.getIndexLabel("person8ByAge");

        schema.vertexLabel("person9")
              .properties("name", "age", "city", "weight")
              .create();
        schema.indexLabel("person9ByCityAndAge").onV("person9").unique()
              .by("city", "age").create();
        schema.getIndexLabel("person9ByCityAndAge");
        schema.indexLabel("person9ByCity").onV("person9").unique()
              .by("city").create();
        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("person9ByCityAndAge");
        });
        schema.getIndexLabel("person9ByCity");
    }

    @Test
    public void testAddShardIndexLabelWithPrefixSecondaryIndex() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();

        schema.getIndexLabel("personByCity");

        schema.indexLabel("personByCityAndAge").onV("person").shard()
              .by("city", "age").create();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByCity");
        });
    }

    @Test
    public void testAddIndexLabelWithOlapPropertyKey() {
        Assume.assumeTrue("Not support olap properties",
                          storeFeatures().supportsOlapProperties());

        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();

        schema.propertyKey("pagerank")
              .asDouble().valueSingle()
              .writeType(WriteType.OLAP_RANGE)
              .ifNotExist().create();
        schema.propertyKey("wcc")
              .asText().valueSingle()
              .writeType(WriteType.OLAP_SECONDARY)
              .ifNotExist().create();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByPagerankAndWcc").onV("person")
                  .secondary().by("pagerank", "wcc").ifNotExist().create();
        }, e -> {
            Assert.assertContains("Can't build index on multiple olap " +
                                  "properties,", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByPagerankAndCity").onV("person")
                  .secondary().by("pagerank", "city").ifNotExist().create();
        }, e -> {
            Assert.assertContains("Can't build index on olap properties and " +
                                  "oltp properties in one index label,",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByWcc").onV("person")
                  .search().by("wcc").ifNotExist().create();
        }, e -> {
            Assert.assertContains("Only secondary and range index can be " +
                                  "built on olap property,", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("personByPagerank").onV("person")
                  .shard().by("pagerank").ifNotExist().create();
        }, e -> {
            Assert.assertContains("Only secondary and range index can be " +
                                  "built on olap property,", e.getMessage());
        });
    }

    @Test
    public void testRemoveIndexLabelOfVertex() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        VertexLabel person = schema.getVertexLabel("person");

        Assert.assertEquals(2, person.indexLabels().size());
        assertContainsIl(person.indexLabels(), "personByCity", "personByAge");

        graph().addVertex(T.label, "person", "name", "Baby",
                          "city", "Hongkong", "age", 3);
        graph().tx().commit();

        Vertex vertex = graph().traversal().V().hasLabel("person")
                               .has("city", "Hongkong").next();
        Assert.assertNotNull(vertex);
        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);

        schema.indexLabel("personByCity").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByCity");
        });

        person = schema.getVertexLabel("person");
        Assert.assertEquals(1, person.indexLabels().size());
        assertNotContainsIl(person.indexLabels(), "personByCity");
        assertContainsIl(person.indexLabels(), "personByAge");

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V().hasLabel("person")
                   .has("city", "Hongkong").next();
        });
        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);

        schema.indexLabel("personByAge").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("personByAge");
        });

        person = schema.getVertexLabel("person");
        Assert.assertEquals(0, person.indexLabels().size());

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V().hasLabel("person")
                   .has("age", P.inside(2, 4)).next();
        });
    }

    @Test
    public void testRemoveIndexLabelOfEdge() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime()
              .link("author", "book")
              .properties("contribution")
              .create();

        Vertex james = graph().addVertex(T.label, "author", "id", 1,
                                         "name", "James Gosling");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "java-1");

        schema.indexLabel("authoredByContri").onE("authored").secondary()
              .by("contribution").create();

        EdgeLabel authored = schema.getEdgeLabel("authored");

        Assert.assertEquals(1, authored.indexLabels().size());
        assertContainsIl(authored.indexLabels(), "authoredByContri");

        james.addEdge("authored", java1, "contribution", "test");
        graph().tx().commit();

        Edge edge = graph().traversal().E().hasLabel("authored")
                           .has("contribution", "test").next();
        Assert.assertNotNull(edge);

        schema.indexLabel("authoredByContri").remove();

        Assert.assertThrows(NotFoundException.class, () -> {
            schema.getIndexLabel("authoredByContri");
        });

        /*
         * Should not expect that schemalabel previously constructed can be
         * dynamically modified with index label operation
         */
        authored = schema.getEdgeLabel("authored");
        Assert.assertEquals(0, authored.indexLabels().size());

        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().E().hasLabel("authored")
                   .has("contribution", "test").next();
        });
    }

    @Test
    public void testRemoveNotExistIndexLabel() {
        SchemaManager schema = graph().schema();
        schema.indexLabel("not-exist-il").remove();
    }

    @Test
    public void testRebuildIndexLabelOfVertex() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();
        VertexLabel person = schema.getVertexLabel("person");
        Assert.assertEquals(2, person.indexLabels().size());
        assertContainsIl(person.indexLabels(), "personByCity", "personByAge");

        graph().addVertex(T.label, "person", "name", "Baby",
                          "city", "Hongkong", "age", 3);
        graph().tx().commit();

        Vertex vertex = graph().traversal().V().hasLabel("person")
                               .has("city", "Hongkong").next();
        Assert.assertNotNull(vertex);
        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);

        schema.indexLabel("personByCity").rebuild();
        vertex = graph().traversal().V().hasLabel("person")
                        .has("city", "Hongkong").next();
        Assert.assertNotNull(vertex);

        schema.indexLabel("personByAge").rebuild();
        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);
    }

    @Test
    public void testRebuildIndexLabelOfVertexLabel() {
        Assume.assumeTrue("Not support range condition query",
                          storeFeatures().supportsQueryWithRangeCondition());
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person").properties("name", "age", "city")
              .primaryKeys("name").create();
        schema.indexLabel("personByCity").onV("person").secondary()
              .by("city").create();
        schema.indexLabel("personByAge").onV("person").range()
              .by("age").create();

        VertexLabel person = schema.getVertexLabel("person");
        Assert.assertEquals(2, person.indexLabels().size());
        assertContainsIl(person.indexLabels(), "personByCity", "personByAge");

        graph().addVertex(T.label, "person", "name", "Baby",
                          "city", "Hongkong", "age", 3);
        graph().tx().commit();

        Vertex vertex = graph().traversal().V().hasLabel("person")
                               .has("city", "Hongkong").next();
        Assert.assertNotNull(vertex);
        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);

        schema.vertexLabel("person").rebuildIndex();
        vertex = graph().traversal().V().hasLabel("person")
                        .has("city", "Hongkong").next();
        Assert.assertNotNull(vertex);
        vertex = graph().traversal().V().hasLabel("person")
                        .has("age", P.inside(2, 4)).next();
        Assert.assertNotNull(vertex);
    }

    @Test
    public void testRebuildIndexLabelOfEdgeLabel() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime()
              .link("author", "book")
              .properties("contribution")
              .create();

        Vertex james = graph().addVertex(T.label, "author", "id", 1,
                                         "name", "James Gosling");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "java-1");

        schema.indexLabel("authoredByContri").onE("authored").secondary()
              .by("contribution").create();

        EdgeLabel authored = schema.getEdgeLabel("authored");

        Assert.assertEquals(1, authored.indexLabels().size());
        assertContainsIl(authored.indexLabels(), "authoredByContri");

        james.addEdge("authored", java1, "contribution", "test");
        graph().tx().commit();

        Edge edge = graph().traversal().E().hasLabel("authored")
                           .has("contribution", "test").next();
        Assert.assertNotNull(edge);

        schema.indexLabel("authoredByContri").rebuild();
        Assert.assertEquals(1, authored.indexLabels().size());
        assertContainsIl(authored.indexLabels(), "authoredByContri");

        edge = graph().traversal().E().hasLabel("authored")
                      .has("contribution", "test").next();
        Assert.assertNotNull(edge);
    }

    @Test
    public void testRebuildIndexLabelOfEdge() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("author").properties("id", "name")
              .primaryKeys("id").create();
        schema.vertexLabel("book").properties("name")
              .primaryKeys("name").create();
        schema.edgeLabel("authored").singleTime()
              .link("author", "book")
              .properties("contribution")
              .create();

        Vertex james = graph().addVertex(T.label, "author", "id", 1,
                                         "name", "James Gosling");
        Vertex java1 = graph().addVertex(T.label, "book", "name", "java-1");

        schema.indexLabel("authoredByContri").onE("authored")
              .secondary().by("contribution").create();

        EdgeLabel authored = schema.getEdgeLabel("authored");

        Assert.assertEquals(1, authored.indexLabels().size());
        assertContainsIl(authored.indexLabels(), "authoredByContri");

        james.addEdge("authored", java1, "contribution", "test");
        graph().tx().commit();

        Edge edge = graph().traversal().E().hasLabel("authored")
                           .has("contribution", "test").next();
        Assert.assertNotNull(edge);

        schema.edgeLabel("authored").rebuildIndex();
        Assert.assertEquals(1, authored.indexLabels().size());
        assertContainsIl(authored.indexLabels(), "authoredByContri");
        edge = graph().traversal().E().hasLabel("authored")
                      .has("contribution", "test").next();
        Assert.assertNotNull(edge);
    }

    @Test
    public void testRebuildIndexOfVertexWithoutLabelIndex() {
        Assume.assumeFalse("Support query by label",
                           storeFeatures().supportsQueryByLabel());

        initDataWithoutLabelIndex();

        // Not support query by label
        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V().hasLabel("reader").toList();
        }, e -> {
            Assert.assertTrue(
                    e.getMessage().startsWith("Don't accept query by label") &&
                    e.getMessage().endsWith("label index is disabled"));
        });

        // Query by property index is ok
        List<Vertex> vertices = graph().traversal().V()
                                       .has("city", "Shanghai").toList();
        Assert.assertEquals(10, vertices.size());

        graph().schema().indexLabel("readerByCity").rebuild();

        vertices = graph().traversal().V()
                          .has("city", "Shanghai").toList();
        Assert.assertEquals(10, vertices.size());
    }

    @Test
    public void testRebuildIndexOfEdgeWithoutLabelIndex() {
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

        graph().schema().indexLabel("readByDate").rebuild();

        edges = graph().traversal().E()
                       .has("date", P.lt("2019-12-30 13:00:00")).toList();
        Assert.assertEquals(20, edges.size());
    }

    @Test
    public void testRemoveIndexLabelOfVertexWithoutLabelIndex() {
        Assume.assumeFalse("Support query by label",
                           storeFeatures().supportsQueryByLabel());

        initDataWithoutLabelIndex();

        // Not support query by label
        Assert.assertThrows(NoIndexException.class, () -> {
            graph().traversal().V().hasLabel("reader").toList();
        }, e -> {
            Assert.assertTrue(
                    e.getMessage().startsWith("Don't accept query by label") &&
                    e.getMessage().endsWith("label index is disabled"));
        });

        // Query by property index is ok
        List<Vertex> vertices = graph().traversal().V()
                                       .has("city", "Shanghai").toList();
        Assert.assertEquals(10, vertices.size());

        graph().schema().indexLabel("readerByCity").remove();

        Assert.assertThrows(NoIndexException.class, () ->
                graph().traversal().V().has("city", "Shanghai").toList()
        );
    }

    @Test
    public void testRemoveIndexLabelOfEdgeWithoutLabelIndex() {
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

        graph().schema().indexLabel("readByDate").remove();

        Assert.assertThrows(NoIndexException.class, () ->
                graph().traversal().E()
                       .has("date", P.lt("2019-12-30 13:00:00")).toList()
        );
    }

    @Test
    public void testAddIndexLabelWithUserdata() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("id", "name", "age", "city")
              .primaryKeys("id")
              .create();

        IndexLabel personByName = schema.indexLabel("personByName")
                                        .onV("person").secondary().by("name")
                                        .userdata("min", 0)
                                        .userdata("max", 100)
                                        .create();
        Assert.assertEquals(3, personByName.userdata().size());
        Assert.assertEquals(0, personByName.userdata().get("min"));
        Assert.assertEquals(100, personByName.userdata().get("max"));

        IndexLabel personByAge = schema.indexLabel("personByAge")
                                       .onV("person").range().by("age")
                                       .userdata("length", 15)
                                       .userdata("length", 18)
                                       .create();
        // The same key user data will be overwritten
        Assert.assertEquals(2, personByAge.userdata().size());
        Assert.assertEquals(18, personByAge.userdata().get("length"));

        List<Object> datas = ImmutableList.of("Beijing", "Shanghai");
        IndexLabel personByCity = schema.indexLabel("personByCity")
                                        .onV("person").secondary().by("city")
                                        .userdata("range", datas)
                                        .create();
        Assert.assertEquals(2, personByCity.userdata().size());
        Assert.assertEquals(datas, personByCity.userdata().get("range"));
    }

    @Test
    public void testAppendIndexLabelWithUserdata() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("id", "name", "age", "city")
              .primaryKeys("id")
              .create();

        IndexLabel personByName = schema.indexLabel("personByName")
                                        .onV("person").secondary().by("name")
                                        .userdata("min", 0)
                                        .create();
        Assert.assertEquals(2, personByName.userdata().size());
        Assert.assertEquals(0, personByName.userdata().get("min"));

        personByName = schema.indexLabel("personByName")
                             .userdata("min", 1)
                             .userdata("max", 100)
                             .append();
        Assert.assertEquals(3, personByName.userdata().size());
        Assert.assertEquals(1, personByName.userdata().get("min"));
        Assert.assertEquals(100, personByName.userdata().get("max"));
    }

    @Test
    public void testEliminateIndexLabelWithUserdata() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("id", "name", "age", "city")
              .primaryKeys("id")
              .create();

        IndexLabel personByName = schema.indexLabel("personByName")
                                        .onV("person").secondary().by("name")
                                        .userdata("min", 0)
                                        .userdata("max", 100)
                                        .create();
        Assert.assertEquals(3, personByName.userdata().size());
        Assert.assertEquals(0, personByName.userdata().get("min"));
        Assert.assertEquals(100, personByName.userdata().get("max"));

        personByName = schema.indexLabel("personByName")
                             .userdata("max", "")
                             .eliminate();
        Assert.assertEquals(2, personByName.userdata().size());
        Assert.assertEquals(0, personByName.userdata().get("min"));
    }

    @Test
    public void testUpdateIndexLabelWithoutUserdata() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("id", "name", "age", "city")
              .primaryKeys("id")
              .create();
        schema.vertexLabel("software")
              .properties("id", "name")
              .primaryKeys("id")
              .create();

        schema.indexLabel("personByName")
              .onV("person").secondary().by("name")
              .userdata("min", 0)
              .userdata("max", 100)
              .create();

        Assert.assertThrows(HugeException.class, () -> {
            schema.indexLabel("personByName").onV("software").append();
        });
        Assert.assertThrows(HugeException.class, () -> {
            schema.indexLabel("personByName").range().append();
        });
        Assert.assertThrows(HugeException.class, () -> {
            schema.indexLabel("personByName").by("age").append();
        });

        Assert.assertThrows(HugeException.class, () -> {
            schema.indexLabel("personByName").onV("software").eliminate();
        });
        Assert.assertThrows(HugeException.class, () -> {
            schema.indexLabel("personByName").range().eliminate();
        });
        Assert.assertThrows(HugeException.class, () -> {
            schema.indexLabel("personByName").by("age").eliminate();
        });
    }

    @Test
    public void testCreateTime() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("id", "name", "age", "city")
              .primaryKeys("id")
              .create();

        IndexLabel personByName = schema.indexLabel("personByName")
                                        .onV("person").secondary().by("name")
                                        .create();

        Date createTime = (Date) personByName.userdata()
                                             .get(Userdata.CREATE_TIME);
        Date now = DateUtil.now();
        Assert.assertFalse(createTime.after(now));

        personByName = schema.getIndexLabel("personByName");
        createTime = (Date) personByName.userdata().get(Userdata.CREATE_TIME);
        Assert.assertFalse(createTime.after(now));
    }

    @Test
    public void testDuplicateIndexLabelWithIdentityProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();

        schema.indexLabel("index4Person")
              .onV("person")
              .by("age", "city")
              .secondary()
              .ifNotExist()
              .create();
        schema.indexLabel("index4Person")
              .onV("person")
              .by("age", "city")
              .secondary()
              .checkExist(false)
              .create();
        schema.indexLabel("index4Person")
              .onV("person")
              .by("age", "city")
              .ifNotExist()
              .create();

        schema.indexLabel("ageIndex4Person")
              .onV("person")
              .by("age")
              .range()
              .ifNotExist()
              .create();
        schema.indexLabel("ageIndex4Person")
              .onV("person")
              .by("age")
              .range()
              .checkExist(false)
              .create();
    }

    @Test
    public void testDuplicateIndexLabelWithDifferentProperties() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();
        schema.edgeLabel("friend")
              .link("person", "person")
              .properties("time")
              .ifNotExist()
              .create();

        schema.indexLabel("index4Person")
              .onV("person")
              .by("age", "city")
              .secondary()
              .ifNotExist()
              .create();
        Assert.assertThrows(ExistedException.class, () -> {
            schema.indexLabel("index4Person")
                  .onV("person")
                  .by("age") // remove city
                  .secondary()
                  .checkExist(false)
                  .create();
        });
        Assert.assertThrows(ExistedException.class, () -> {
            schema.indexLabel("index4Person")
                  .onE("friend") // not on person
                  .by("age")
                  .secondary()
                  .checkExist(false)
                  .create();
        });

        schema.indexLabel("index4Friend")
              .onE("friend")
              .search()
              .by("time")
              .ifNotExist()
              .create();
        Assert.assertThrows(ExistedException.class, () -> {
            schema.indexLabel("index4Friend")
                  .onE("friend")
                  .by("time")
                  .checkExist(false)
                  .create();
        });

        schema.indexLabel("ageIndex4Person")
              .onV("person")
              .by("age")
              .range()
              .ifNotExist()
              .create();
        Assert.assertThrows(ExistedException.class, () -> {
            schema.indexLabel("ageIndex4Person")
                  .onV("person")
                  .by("age")
                  .secondary()  // different index type
                  .ifNotExist()
                  .create();
        });
    }

    @Test
    public void testAddIndexLabelOnDecimalProperty() {
        super.initPropertyKeys();
        SchemaManager schema = graph().schema();
        schema.propertyKey("balance").asDecimal().create();
        schema.vertexLabel("account")
              .properties("name", "balance")
              .primaryKeys("name")
              .create();

        // no byte-order encoding exists for decimals: no index of any type
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("accountByBalance").onV("account")
                  .by("balance").secondary().create();
        }, e -> {
            Assert.assertContains("data type is decimal", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("accountByBalanceRange").onV("account")
                  .by("balance").range().create();
        }, e -> {
            Assert.assertContains("data type is decimal", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("accountByNameBalance").onV("account")
                  .by("name", "balance").shard().create();
        }, e -> {
            Assert.assertContains("data type is decimal", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            schema.indexLabel("accountByBalanceUnique").onV("account")
                  .by("balance").unique().create();
        }, e -> {
            Assert.assertContains("data type is decimal", e.getMessage());
        });
    }
}
