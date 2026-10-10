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

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.traversal.optimize.HugeCountStepStrategy;
import org.apache.hugegraph.traversal.optimize.HugeCountStrategy;
import org.apache.hugegraph.traversal.optimize.HugeGraphStepStrategy;
import org.apache.hugegraph.traversal.optimize.HugeVertexStepStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.DT;
import org.apache.tinkerpop.gremlin.process.traversal.GType;
import org.apache.tinkerpop.gremlin.process.traversal.NotP;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.FailStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.junit.Test;

public class TinkerPop37StepsCoreTest extends BaseCoreTest {

    @Test
    public void testStringManipulationSteps() {
        Assert.assertEquals("123", graph().traversal().inject(123)
                                         .asString().next());
        Assert.assertEquals(5, graph().traversal().inject("marko")
                                      .length().next());
        Assert.assertEquals("marko", graph().traversal().inject("MARKO")
                                         .toLower().next());
        Assert.assertEquals("MARKO", graph().traversal().inject("marko")
                                         .toUpper().next());
        Assert.assertEquals("marko", graph().traversal().inject("  marko  ")
                                         .trim().next());
        Assert.assertEquals("marko  ", graph().traversal().inject("  marko  ")
                                           .lTrim().next());
        Assert.assertEquals("  marko", graph().traversal().inject("  marko  ")
                                           .rTrim().next());
        Assert.assertEquals("huge-graph", graph().traversal().inject("huge_graph")
                                              .replace("_", "-").next());
        Assert.assertEquals("hugegraph", graph().traversal().inject("huge")
                                             .concat("graph").next());
        Assert.assertEquals("eguh", graph().traversal().inject("huge")
                                        .reverse().next());
        Assert.assertEquals(Arrays.asList("huge", "graph"),
                            graph().traversal().inject("huge-graph")
                                   .split("-").next());
        Assert.assertEquals("graph", graph().traversal().inject("hugegraph")
                                        .substring(4).next());
        Assert.assertEquals("huge", graph().traversal().inject("hugegraph")
                                       .substring(0, 4).next());

        Map<String, Object> values = new HashMap<>();
        values.put("name", "marko");
        values.put("age", 29);
        Assert.assertEquals("marko is 29 years old",
                            graph().traversal().inject(values)
                                   .format("%{name} is %{age} years old")
                                   .next());
    }

    @Test
    public void testListManipulationSteps() {
        List<Integer> values = Arrays.asList(1, 2);
        List<Integer> other = Arrays.asList(2, 3);

        Assert.assertEquals(Arrays.asList(1, 2, 2, 3),
                            graph().traversal().inject(values)
                                   .combine(other).next());
        Assert.assertEquals(setOf(1, 2, 3),
                            asSet(graph().traversal().inject(values)
                                         .merge(other).next()));
        Assert.assertEquals(setOf(2),
                            asSet(graph().traversal().inject(values)
                                         .intersect(other).next()));
        Assert.assertEquals(setOf(1),
                            asSet(graph().traversal().inject(values)
                                         .difference(other).next()));
        Assert.assertEquals(setOf(1, 3),
                            asSet(graph().traversal().inject(values)
                                         .disjunct(other).next()));
        Assert.assertEquals(Arrays.asList(Arrays.asList(1, 2),
                                          Arrays.asList(1, 3),
                                          Arrays.asList(2, 2),
                                          Arrays.asList(2, 3)),
                            graph().traversal().inject(values)
                                   .product(other).next());
        Assert.assertEquals(Arrays.asList(3, 2, 1),
                            graph().traversal().inject(Arrays.asList(1, 2, 3))
                                   .reverse().next());
        Assert.assertEquals("huge-graph",
                            graph().traversal()
                                   .inject(Arrays.asList("huge", "graph"))
                                   .conjoin("-").next());
        Assert.assertEquals(Arrays.asList(1, 2, 3),
                            graph().traversal()
                                   .inject(Arrays.asList(1, 2, 3))
                                   .all(P.gt(0)).next());
        Assert.assertEquals(Arrays.asList(1, 2, 3),
                            graph().traversal()
                                   .inject(Arrays.asList(1, 2, 3))
                                   .any(P.eq(2)).next());
    }

    @Test
    public void testOffsetDateTimeQueryOnExistingDateSchema() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("birthday").asDate().create();
        schema.vertexLabel("dated").properties("birthday").create();
        schema.indexLabel("datedByBirthday").onV("dated")
              .by("birthday").range().create();
        OffsetDateTime time = OffsetDateTime.parse("2023-08-02T08:00:00.123456789+08:00");
        Date date = Date.from(time.toInstant());
        Vertex first = graph().addVertex(T.label, "dated", "birthday", date);
        Vertex later = graph().addVertex(T.label, "dated", "birthday",
                                         Date.from(time.plusDays(1).toInstant()));
        commitTx();
        Assert.assertEquals(Collections.singletonList(first.id()),
                            graph().traversal().V().hasLabel("dated")
                                   .has("birthday", date).id().toList());
        Assert.assertEquals(Collections.singletonList(later.id()),
                            graph().traversal().V().hasLabel("dated")
                                   .has("birthday", P.gt(date)).id().toList());
        Assert.assertEquals(Collections.singletonList(first.id()),
                            graph().traversal().V().hasLabel("dated")
                                   .has("birthday", time).id().toList());
        Assert.assertEquals(Collections.singletonList(later.id()),
                            graph().traversal().V().hasLabel("dated")
                                   .has("birthday", P.gt(time)).id().toList());
    }

    @Test
    public void testOffsetDateTimeWithDateSchemaAndRangeIndex() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("birthday").asDate().create();
        schema.vertexLabel("dated").properties("birthday").create();
        schema.indexLabel("datedByBirthday").onV("dated")
              .by("birthday").range().create();

        OffsetDateTime time = graph().traversal()
                                     .inject("2023-08-02T08:00:00.123456789+08:00")
                                     .asDate().next();
        Date expected = Date.from(time.toInstant());
        Vertex legacy = graph().addVertex(T.label, "dated", "birthday", expected);
        commitTx();
        Assert.assertEquals(expected, graph().traversal().V(legacy.id())
                                            .values("birthday").next());
        Assert.assertEquals(Collections.singletonList(legacy.id()),
                            graph().traversal().V().hasLabel("dated")
                                   .has("birthday", expected).id().toList());

        Vertex modern = graph().addVertex(T.label, "dated", "birthday", time);
        Vertex later = graph().traversal().addV("dated")
                              .property("birthday", time.plusDays(1)).next();
        commitTx();
        Assert.assertEquals(expected, graph().traversal().V(modern.id())
                                            .values("birthday").next());
        Assert.assertEquals(Date.from(time.plusDays(1).toInstant()),
                            graph().traversal().V(later.id()).values("birthday").next());
        Assert.assertEquals(setOf(legacy.id(), modern.id()),
                            asSet(graph().traversal().V().hasLabel("dated")
                                         .has("birthday", time).id().toList()));
        Assert.assertEquals(setOf(legacy.id(), modern.id()),
                            asSet(graph().traversal().V().hasLabel("dated")
                                         .has("birthday", time.withOffsetSameInstant(
                                                 ZoneOffset.UTC)).id().toList()));
        Assert.assertEquals(Collections.singletonList(later.id()),
                            graph().traversal().V().hasLabel("dated")
                                   .has("birthday", P.gt(time)).id().toList());
        Assert.assertEquals(setOf(legacy.id(), modern.id()),
                            asSet(graph().traversal().V().hasLabel("dated")
                                         .has("birthday", P.gte(time).and(
                                                 P.lt(time.plusDays(1)))).id().toList()));
        Assert.assertEquals(Collections.singletonList(later.id()),
                            graph().traversal().V().hasLabel("dated")
                                   .has("birthday", P.gt(expected)).id().toList());
    }

    @Test
    public void testDateManipulationSteps() {
        OffsetDateTime start = OffsetDateTime.parse("2023-08-02T00:00:00Z");
        OffsetDateTime expected = OffsetDateTime.parse("2023-08-09T00:00:00Z");

        OffsetDateTime actual = graph().traversal()
                                       .inject("2023-08-02T00:00:00Z")
                                       .asDate().dateAdd(DT.day, 7).next();
        long milliseconds = graph().traversal()
                                   .inject("2023-08-02T00:00:00Z")
                                   .asDate().dateAdd(DT.day, 7)
                                   .dateDiff(start).next();

        Assert.assertEquals(expected, actual);
        Assert.assertEquals(604800000L, milliseconds);
    }

    @Test
    public void testAddVertexKeepsPropertiesFoldedByPrimaryKeyStrategy() {
        this.initMutationSchema();

        GraphTraversal<Vertex, Vertex> traversal = graph().traversal()
                                                          .addV("person")
                                                          .property(
                                                               Cardinality.single,
                                                               "name",
                                                               __.constant("marko"))
                                                          .property(
                                                               Cardinality.single,
                                                               "status", "active");
        Assert.assertTrue(traversal.asAdmin().getSteps().stream().anyMatch(
                          step -> step instanceof AddPropertyStepContract));

        Vertex vertex = traversal.next();
        commitTx();

        Vertex stored = graph().traversal().V(vertex.id()).next();
        Assert.assertEquals("marko", stored.value("name"));
        Assert.assertEquals("active", stored.value("status"));
    }

    @Test
    public void testMergeVertexWithHugeGraphIds() {
        this.initMutationSchema();
        Map<Object, Object> search = map(T.label, "person",
                                         "name", "marko");

        Vertex created = graph().traversal().mergeV(search)
                                .option(Merge.onCreate,
                                        map("status", "created"))
                                .next();
        commitTx();
        Vertex matched = graph().traversal().mergeV(search)
                                .option(Merge.onMatch,
                                        map("status", "matched"))
                                .next();
        commitTx();

        Assert.assertEquals(created.id(), matched.id());
        Assert.assertEquals("matched", matched.value("status"));
        Assert.assertEquals(1L, graph().traversal().V()
                                      .hasLabel("person")
                                      .has("name", "marko")
                                      .count().next());
    }

    @Test
    public void testMergeEdgeWithHugeGraphIds() {
        this.initMutationSchema();
        Vertex marko = graph().addVertex(T.label, "person",
                                         "name", "marko");
        Vertex vadas = graph().addVertex(T.label, "person",
                                         "name", "vadas");
        commitTx();
        Map<Object, Object> search = map(T.label, "knows",
                                         Direction.OUT, marko.id(),
                                         Direction.IN, vadas.id());

        Edge created = graph().traversal().mergeE(search)
                              .option(Merge.onCreate,
                                      map("status", "created",
                                          "weight", 0.5D))
                              .next();
        commitTx();
        Edge matched = graph().traversal().mergeE(search)
                              .option(Merge.onMatch,
                                      map("status", "matched"))
                              .next();
        commitTx();

        Assert.assertEquals(created.id(), matched.id());
        Assert.assertEquals("matched", matched.value("status"));
        Assert.assertEquals(1L, graph().traversal().E()
                                      .hasLabel("knows").count().next());
    }

    @Test
    public void testMergeOnCreateValidation() {
        this.initMutationSchema();
        Map<Object, Object> search = map(T.label, "person",
                                         "name", "marko");
        Map<Object, Object> invalid = map(T.label, "person",
                                          "name", "vadas");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            graph().traversal().mergeV(search)
                   .option(Merge.onCreate, invalid);
        });
    }

    @Test
    public void testPropertyMapAndMidTraversalE() {
        this.initMutationSchema();
        Vertex marko = graph().addVertex(T.label, "person",
                                         "name", "marko");
        Vertex vadas = graph().addVertex(T.label, "person",
                                         "name", "vadas");
        Edge edge = marko.addEdge("knows", vadas);
        commitTx();

        graph().traversal().V(marko.id())
               .property(map("status", "active"))
               .iterate();
        commitTx();

        Assert.assertEquals("active", graph().traversal().V(marko.id())
                                                .values("status").next());
        Assert.assertEquals(edge.id(), graph().traversal().inject(1)
                                         .E(edge.id()).next().id());
    }

    @Test
    public void testUnproductiveByFiltersMissingGroupKey() {
        this.initTextData();
        graph().traversal().V()
               .hasLabel("person")
               .has("name", "marko")
               .property("status", "active")
               .iterate();
        commitTx();

        Map<Object, Object> grouped = graph().traversal().V()
                                             .group()
                                             .by("status")
                                             .by("name")
                                             .next();

        Assert.assertEquals(1, grouped.size());
        Assert.assertEquals(Collections.singletonList("marko"),
                            grouped.get("active"));
        Assert.assertFalse(grouped.containsKey(null));
    }

    @Test
    public void testUnproductiveByOmitsProjectKey() {
        this.initTextData();

        Map<String, Object> projected = graph().traversal().V()
                                               .hasLabel("person")
                                               .has("name", "vadas")
                                               .project("name", "status")
                                               .by("name")
                                               .by("status")
                                               .next();

        Assert.assertEquals("vadas", projected.get("name"));
        Assert.assertFalse(projected.containsKey("status"));
    }

    @Test
    public void testMissingByValueCanUseExplicitFallback() {
        this.initTextData();
        graph().traversal().V()
               .hasLabel("person")
               .has("name", "marko")
               .property("status", "active")
               .iterate();
        commitTx();

        Map<Object, Object> grouped = graph().traversal().V()
                                             .group()
                                             .by(__.coalesce(
                                                  __.values("status"),
                                                  __.constant("missing")))
                                             .by("name")
                                             .next();

        Assert.assertEquals(Collections.singletonList("marko"),
                            grouped.get("active"));
        Assert.assertEquals(setOf("lop", "vadas"),
                            asSet(grouped.get("missing")));
    }

    @Test
    public void testFailStep() {
        Assert.assertThrows(FailStep.FailException.class, () -> {
            graph().traversal().inject(1).fail("expected failure").iterate();
        });
    }

    @Test
    public void testTextPContaining() {
        this.initTextData();
        Assert.assertEquals(Arrays.asList("marko"),
                            this.names(TextP.containing("ark")));
    }

    @Test
    public void testTextPStartingWith() {
        this.initTextData();
        Assert.assertEquals(Arrays.asList("marko"),
                            this.names(TextP.startingWith("mar")));
    }

    @Test
    public void testTextPEndingWith() {
        this.initTextData();
        Assert.assertEquals(Arrays.asList("vadas"),
                            this.names(TextP.endingWith("das")));
    }

    @Test
    public void testTextPRegex() {
        this.initTextData();
        Assert.assertEquals(Arrays.asList("marko"),
                            this.names(TextP.regex("^mar")));
    }

    @Test
    public void testTextPNegations() {
        this.initTextData();
        Assert.assertEquals(Arrays.asList("lop", "vadas"),
                            this.names(TextP.notContaining("ar")));
        Assert.assertEquals(Arrays.asList("lop", "vadas"),
                            this.names(TextP.notStartingWith("mar")));
        Assert.assertEquals(Arrays.asList("lop", "marko"),
                            this.names(TextP.notEndingWith("das")));
        Assert.assertEquals(Arrays.asList("lop", "vadas"),
                            this.names(TextP.notRegex("^mar")));
    }

    @Test
    public void testTextPWithLocalFilter() {
        this.initTextData();

        Assert.assertEquals(Arrays.asList("marko"),
                            this.namesWithLocalFilter(
                                 TextP.containing("ark")));
        Assert.assertEquals(Arrays.asList("marko"),
                            this.namesWithLocalFilter(
                                 TextP.startingWith("mar")));
        Assert.assertEquals(Arrays.asList("vadas"),
                            this.namesWithLocalFilter(
                                 TextP.endingWith("das")));
        Assert.assertEquals(Arrays.asList("marko"),
                            this.namesWithLocalFilter(TextP.regex("^mar")));
        Assert.assertEquals(Arrays.asList("lop", "vadas"),
                            this.namesWithLocalFilter(
                                 TextP.notContaining("ar")));
        Assert.assertEquals(Arrays.asList("lop", "vadas"),
                            this.namesWithLocalFilter(
                                 TextP.notStartingWith("mar")));
        Assert.assertEquals(Arrays.asList("lop", "marko"),
                            this.namesWithLocalFilter(
                                 TextP.notEndingWith("das")));
        Assert.assertEquals(Arrays.asList("lop", "vadas"),
                            this.namesWithLocalFilter(
                                 TextP.notRegex("^mar")));
    }

    @Test
    public void testTypeOfPredicatesStayLocal() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("age").asInt().create();
        schema.vertexLabel("person").properties("name", "age")
              .primaryKeys("name").nullableKeys("age").create();
        graph().addVertex(T.label, "person", "name", "marko", "age", 29);
        graph().addVertex(T.label, "person", "name", "vadas", "age", 27);
        graph().addVertex(T.label, "person", "name", "lop");
        commitTx();

        this.assertTypeFilter(T.label, P.typeOf(GType.STRING),
                              "lop", "marko", "vadas");
        this.assertTypeFilter(T.label, P.typeOf(GType.INT));
        this.assertTypeFilter("name", P.typeOf(GType.STRING),
                              "lop", "marko", "vadas");
        this.assertTypeFilter("name", P.typeOf(GType.INT));
        this.assertTypeFilter("age", P.typeOf(GType.INT), "marko", "vadas");
        this.assertTypeFilter("age", P.typeOf(GType.STRING));
        this.assertTypeFilter("age", P.typeOf(Integer.class), "marko", "vadas");
        this.assertTypeFilter("age", P.typeOf("Integer"), "marko", "vadas");
        this.assertTypeFilter("age", P.not(P.typeOf(GType.INT)));
        this.assertTypeFilter("age", P.not(P.typeOf(GType.STRING)), "marko", "vadas");
        this.assertTypeFilter("age", P.typeOf(GType.INT).and(P.gt(28)), "marko");
        this.assertTypeFilter("age", P.typeOf(GType.STRING).or(P.eq(27)), "vadas");
        this.assertTypeFilter("age", P.not(P.typeOf(GType.STRING)).and(P.gt(28)),
                              "marko");
        this.assertTypeFilter("age", new NotP<>(P.typeOf(GType.INT).and(P.gt(28))),
                              "vadas");
    }

    private void assertTypeFilter(Object key, P<?> predicate, String... expected) {
        for (boolean optimized : new boolean[]{true, false}) {
            GraphTraversalSource source = graph().traversal();
            if (!optimized) {
                source = source.withoutStrategies(HugeGraphStepStrategy.class,
                                                  HugeVertexStepStrategy.class,
                                                  HugeCountStepStrategy.class,
                                                  HugeCountStrategy.class);
            }
            GraphTraversal<Vertex, Vertex> traversal = source.V();
            if (key instanceof T) {
                traversal = traversal.has((T) key, predicate.clone());
            } else {
                traversal = traversal.has((String) key, predicate.clone());
            }
            Assert.assertEquals(Arrays.asList(expected),
                                traversal.values("name").order().toList());
            GraphTraversal<Vertex, Vertex> counted = source.V();
            if (key instanceof T) {
                counted = counted.has((T) key, predicate.clone());
            } else {
                counted = counted.has((String) key, predicate.clone());
            }
            Assert.assertEquals((long) expected.length, counted.count().next().longValue());
        }
    }

    private void initMutationSchema() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("status").asText().create();
        schema.propertyKey("weight").asDouble().create();
        schema.vertexLabel("person")
              .properties("name", "status")
              .primaryKeys("name")
              .nullableKeys("status")
              .create();
        schema.edgeLabel("knows")
              .link("person", "person")
              .properties("status", "weight")
              .nullableKeys("status", "weight")
              .create();
    }

    private void initTextData() {
        this.initMutationSchema();
        graph().addVertex(T.label, "person", "name", "marko");
        graph().addVertex(T.label, "person", "name", "vadas");
        graph().addVertex(T.label, "person", "name", "lop");
        commitTx();
    }

    private List<Object> names(TextP predicate) {
        return graph().traversal().V()
                      .hasLabel("person")
                      .has("name", predicate)
                      .values("name")
                      .order()
                      .toList();
    }

    private List<Object> namesWithLocalFilter(TextP predicate) {
        return graph().traversal().V()
                      .hasLabel("person")
                      .filter(__.values("name").is(predicate))
                      .values("name")
                      .order()
                      .toList();
    }

    private static Map<Object, Object> map(Object... keyValues) {
        Map<Object, Object> result = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            result.put(keyValues[i], keyValues[i + 1]);
        }
        return result;
    }

    private static Set<Object> setOf(Object... values) {
        return new HashSet<>(Arrays.asList(values));
    }

    private static Set<Object> asSet(Object values) {
        Assert.assertInstanceOf(Iterable.class, values);
        List<Object> list = new ArrayList<>();
        for (Object value : (Iterable<?>) values) {
            list.add(value);
        }
        return new HashSet<>(list);
    }
}
