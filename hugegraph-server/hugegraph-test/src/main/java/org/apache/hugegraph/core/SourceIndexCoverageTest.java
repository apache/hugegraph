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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.backend.serializer.AbstractSerializer;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.tx.GraphIndexTransaction;
import org.apache.hugegraph.exception.NoIndexException;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.SchemaLabel;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.tinkerpop.TestGraph;
import org.apache.hugegraph.traversal.optimize.HugeGraphStep;
import org.apache.hugegraph.traversal.optimize.Text;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.GraphReadMode;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.type.define.WriteType;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class SourceIndexCoverageTest extends BaseCoreTest {

    @Before
    public void initSchema() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("coverageName").asText().create();
        schema.propertyKey("coverageScore").asInt().create();
        schema.propertyKey("coverageBody").asText().create();
        schema.vertexLabel("coverageA")
              .properties("coverageName", "coverageScore", "coverageBody")
              .useAutomaticId().create();
        schema.vertexLabel("coverageB")
              .properties("coverageName", "coverageScore", "coverageBody")
              .enableLabelIndex(false).useAutomaticId().create();
        schema.vertexLabel("unrelated").properties("coverageBody").useAutomaticId().create();
    }

    private Set<Vertex> addVertices() {
        Vertex a = graph().addVertex(T.label, "coverageA", "coverageName", "same",
                                     "coverageScore", 10, "coverageBody", "gold silver");
        Vertex b = graph().addVertex(T.label, "coverageB", "coverageName", "same",
                                     "coverageScore", 20, "coverageBody", "gold bronze");
        this.commitTx();
        return ImmutableSet.of(a, b);
    }

    private void nameIndex(String label) {
        graph().schema().indexLabel(label + "ByName").onV(label)
               .by("coverageName").secondary().create();
    }

    @Test
    public void testCompleteCandidatesReturnAllResults() {
        this.nameIndex("coverageA");
        this.nameIndex("coverageB");
        Set<Vertex> expected = this.addVertices();
        Assert.assertEquals(expected,
                            graph().traversal().V().has("coverageName", "same").toSet());
    }

    @Test
    public void testTinkerPopModernSchemaCoversGlobalPropertyQueries() {
        new TestGraph(graph()).initModernSchema(IdStrategy.AUTOMATIC);
        Vertex person = graph().addVertex(T.label, "person", "name", "marko", "age", 29);
        Vertex software = graph().addVertex(T.label, "software", "name", "lop");
        Edge created = person.addEdge("created", software, "weight", 0.4d);
        this.commitTx();

        Assert.assertEquals(ImmutableSet.of(person),
                            graph().traversal().V().has("name", "marko").toSet());
        Assert.assertEquals(ImmutableSet.of(person),
                            graph().traversal().V().has("age", 29).toSet());
        Assert.assertEquals(ImmutableSet.of(created),
                            graph().traversal().E().has("weight", 0.4d).toSet());
    }

    @Test
    public void testTinkerPopBasicSchemaCoversSharedProperties() {
        new TestGraph(graph()).initBasicSchema(IdStrategy.AUTOMATIC, TestGraph.DEFAULT_VL);
        Vertex vertex = graph().addVertex(T.label, TestGraph.DEFAULT_VL, "name", "shared");
        Vertex person = graph().addVertex(T.label, "person", "name", "shared");
        Vertex software = graph().addVertex(T.label, "software", "name", "shared");
        Edge self = vertex.addEdge("self", vertex, "name", "shared");
        Edge friend = vertex.addEdge("friend", vertex, "name", "shared");
        Edge link = vertex.addEdge("l", vertex, "name", "shared");
        Edge partition = vertex.addEdge("aTOa", vertex,
                                        "gremlin.partitionGraphStrategy.partition", "shared");
        this.commitTx();

        Assert.assertEquals(ImmutableSet.of(vertex, person, software),
                            graph().traversal().V().has("name", "shared").toSet());
        Assert.assertEquals(ImmutableSet.of(self, friend, link),
                            graph().traversal().E().has("name", "shared").toSet());
        Assert.assertEquals(ImmutableSet.of(partition), graph().traversal().E()
                .has("gremlin.partitionGraphStrategy.partition", "shared").toSet());
    }

    @Test
    public void testTinkerPopSinkSchemaCoversName() {
        new TestGraph(graph()).initSinkSchema();
        Vertex message = graph().addVertex(T.label, "message", "name", "ping");
        this.commitTx();

        Assert.assertEquals(ImmutableSet.of(message),
                            graph().traversal().V().has("name", "ping").toSet());
    }

    @Test
    public void testPartialCoverageRejectsIncompleteSource() {
        this.nameIndex("coverageA");
        Set<Vertex> expected = this.addVertices();
        Assert.assertThrows(NoIndexException.class, () ->
                graph().traversal().V().has("coverageName", "same").toSet(), error -> {
                    Assert.assertContains("Incomplete index coverage", error.getMessage());
                    Assert.assertFalse(error.getMessage().contains("coverageB"));
                });
        Assert.assertEquals(1, graph().traversal().V().hasLabel("coverageA")
                                    .has("coverageName", "same").toList().size());
        this.nameIndex("coverageB");
        Assert.assertEquals(expected,
                            graph().traversal().V().has("coverageName", "same").toSet());
    }

    @Test
    public void testOrCoverageFailureClosesEarlierBranchCursors() {
        this.prepareOrIndexes(false);
        this.addVertices();
        try (IndexCursorTracker tracker = new IndexCursorTracker(null)) {
            NoIndexException failure = Assert.assertThrows(NoIndexException.class,
                    () -> graph().vertices(this.orQuery()));
            Assert.assertContains("Incomplete index coverage", failure.getMessage());
            tracker.assertCounts(2, 1);
            Assert.assertEquals(0, failure.getSuppressed().length);
            // Closing must happen before rollback or test-fixture cleanup.
            graph().tx().rollback();
            tracker.assertCounts(2, 1);
        }
    }

    @Test
    public void testOrCoverageFailurePreservesOriginalErrorWhenCloseFails() {
        this.prepareOrIndexes(false);
        this.addVertices();
        RuntimeException closeFailure = new IllegalStateException("index cursor close failed");
        try (IndexCursorTracker tracker = new IndexCursorTracker(closeFailure)) {
            NoIndexException failure = Assert.assertThrows(NoIndexException.class,
                    () -> graph().vertices(this.orQuery()));
            Assert.assertContains("Incomplete index coverage", failure.getMessage());
            // A failed close on the first cursor must not skip the second one.
            tracker.assertCounts(2, 1);
            Assert.assertEquals(1, failure.getSuppressed().length);
            Assert.assertSame(closeFailure, Throwables.getRootCause(failure.getSuppressed()[0]));
        }
    }

    @Test
    public void testCompleteOrPlanKeepsCursorsUntilResultClose() {
        this.prepareOrIndexes(true);
        Set<Vertex> expected = this.addVertices();
        try (IndexCursorTracker tracker = new IndexCursorTracker(null)) {
            Iterator<Vertex> results = graph().vertices(this.orQuery());
            try {
                tracker.assertCounts(4, 0);
                Assert.assertTrue(results.hasNext());
                Assert.assertTrue(expected.contains(results.next()));
            } finally {
                CloseableIterator.closeIterator(results);
            }
            // Includes the later OR branch whose results were never requested.
            tracker.assertCounts(4, 1);
        }
    }

    @Test
    public void testLabeledSubEdgeUsesParentIndex() {
        Assume.assumeTrue(storeFeatures().supportsFatherAndSubEdgeLabel());
        SchemaManager schema = graph().schema();
        List<Edge> edges = this.addSubEdges();
        Edge expected = edges.get(0);
        Edge sibling = edges.get(1);
        schema.indexLabel("parentEdgeByName").onE("parentEdge")
              .by("coverageName").secondary().create();
        Assert.assertEquals(2,
                            graph().traversal().E().has("coverageName", "same").toList().size());
        Assert.assertEquals(ImmutableList.of(sibling), graph().traversal().E()
                .hasLabel("subB").has("coverageName", "same").limit(1).toList());
        Assert.assertEquals(ImmutableSet.of(expected), graph().traversal().E()
                .hasLabel("subA").has("coverageName", "same").toSet());
        Assert.assertEquals(ImmutableList.of(expected), graph().traversal().E()
                .hasLabel("subA").has("coverageName", "same").limit(1).toList());
        if (storeFeatures().supportsQueryByPage()) {
            for (Edge edge : edges) {
                Set<Edge> actual = new HashSet<>();
                String page = "";
                int pages = 0;
                do {
                    ConditionQuery query = this.nameQuery(HugeType.EDGE);
                    query.eq(HugeKeys.LABEL, graph().edgeLabel(edge.label()).id());
                    query.limit(1);
                    query.page(page);
                    Iterator<Edge> results = graph().edges(query);
                    try {
                        results.forEachRemaining(actual::add);
                        page = PageInfo.pageInfo(results);
                    } finally {
                        CloseableIterator.closeIterator(results);
                    }
                    Assert.assertTrue(++pages < 10);
                } while (page != null && !page.isEmpty());
                Assert.assertEquals(ImmutableSet.of(edge), actual);
            }
        }

        SchemaLabel parentLabel = schema.getEdgeLabel("parentEdge");
        try {
            this.params().schemaTransaction().updateSchemaStatus(parentLabel, SchemaStatus.DELETING);
            Assert.assertThrows(NoIndexException.class, () -> graph().traversal().E()
                    .hasLabel("subA").has("coverageName", "same").toSet());
            ConditionQuery query = this.nameQuery(HugeType.EDGE);
            query.eq(HugeKeys.LABEL, graph().edgeLabel("subA").id());
            query.showDeleting(true);
            Assert.assertEquals(ImmutableSet.of(expected), ImmutableSet.copyOf(graph().edges(query)));
        } finally {
            this.params().schemaTransaction().updateSchemaStatus(parentLabel, SchemaStatus.CREATED);
        }

        IndexLabel parent = schema.getIndexLabel("parentEdgeByName");
        try {
            this.params().schemaTransaction().updateSchemaStatus(parent, SchemaStatus.REBUILDING);
            try (IndexCursorTracker tracker = new IndexCursorTracker(null)) {
                Assert.assertThrows(IllegalArgumentException.class, () -> graph().traversal().E()
                        .hasLabel("subA").has("coverageName", "same").toSet(), error ->
                        Assert.assertContains("REBUILDING", error.getMessage()));
                tracker.assertCounts(0, 0);
            }
            schema.indexLabel("subAByName").onE("subA").by("coverageName").secondary().create();
            // A compatible direct index is preferred over the parent fallback.
            try (IndexCursorTracker tracker = new IndexCursorTracker(null)) {
                Assert.assertEquals(ImmutableSet.of(expected), graph().traversal().E()
                        .hasLabel("subA").has("coverageName", "same").toSet());
                tracker.assertCounts(1, 1);
            }
        } finally {
            this.params().schemaTransaction().updateSchemaStatus(parent, SchemaStatus.CREATED);
        }
    }

    @Test
    public void testLabeledSubEdgeUsesParentRangeJointAndSearchIndexes() {
        Assume.assumeTrue(storeFeatures().supportsFatherAndSubEdgeLabel());
        SchemaManager schema = graph().schema();
        Edge expected = this.addSubEdges().get(1);
        schema.indexLabel("parentByScore").onE("parentEdge").by("coverageScore").range().create();
        Assert.assertEquals(ImmutableList.of(expected), graph().traversal().E()
                .hasLabel("subB").has("coverageScore", P.gte(1)).limit(1).toList());
        schema.indexLabel("parentByName").onE("parentEdge").by("coverageName").secondary().create();
        Assert.assertEquals(ImmutableList.of(expected), graph().traversal().E()
                .hasLabel("subB").has("coverageName", "same")
                .has("coverageScore", P.gte(1)).limit(1).toList());
        schema.indexLabel("parentByName").remove();
        schema.indexLabel("parentSearchByName").onE("parentEdge").by("coverageName").search().create();
        Assert.assertEquals(ImmutableList.of(expected), graph().traversal().E()
                .hasLabel("subB").has("coverageName", Text.contains("same")).limit(1).toList());
    }

    private List<Edge> addSubEdges() {
        SchemaManager schema = graph().schema();
        schema.edgeLabel("parentEdge").asBase().properties("coverageName", "coverageScore")
              .multiTimes().sortKeys("coverageScore").create();
        for (String label : new String[]{"subA", "subB"}) {
            schema.edgeLabel(label).withBase("parentEdge").link("coverageA", "coverageB")
                  .properties("coverageName", "coverageScore")
                  .multiTimes().sortKeys("coverageScore").create();
        }
        Set<Vertex> vertices = this.addVertices();
        Vertex a = vertices.stream().filter(v -> v.label().equals("coverageA")).findFirst().get();
        Vertex b = vertices.stream().filter(v -> v.label().equals("coverageB")).findFirst().get();
        Edge first = a.addEdge("subA", b, "coverageName", "same", "coverageScore", 1);
        Edge second = a.addEdge("subB", b, "coverageName", "same", "coverageScore", 2);
        this.commitTx();
        return ImmutableList.of(first, second);
    }

    @Test
    public void testSingleBranchFailureClosesPartialCursors() {
        this.nameIndex("coverageA");
        this.nameIndex("coverageB");
        this.addVertices();
        RuntimeException injected = new IllegalStateException("second index query failed");
        try (IndexCursorTracker tracker = new IndexCursorTracker(null, injected, 2)) {
            RuntimeException failure = Assert.assertThrows(RuntimeException.class,
                    () -> graph().vertices(this.nameQuery(HugeType.VERTEX)));
            Assert.assertSame(injected, Throwables.getRootCause(failure));
            tracker.assertCounts(1, 1);
        }
    }

    @Test
    public void testJointBranchFailureClosesPartialCursors() {
        this.prepareJointIndexes();
        ConditionQuery query = this.jointQuery();
        RuntimeException injected = new IllegalStateException("second joint index query failed");
        try (IndexCursorTracker tracker = new IndexCursorTracker(null, injected, 2)) {
            RuntimeException failure = Assert.assertThrows(RuntimeException.class,
                    () -> graph().vertices(query));
            Assert.assertSame(injected, Throwables.getRootCause(failure));
            tracker.assertCounts(1, 1);
        }
    }

    @Test
    public void testSingleBranchFailurePreservesErrorAndClosesAllCursors() {
        this.nameIndex("coverageA");
        this.nameIndex("coverageB");
        graph().schema().vertexLabel("coverageC").properties("coverageName")
               .useAutomaticId().create();
        this.nameIndex("coverageC");
        this.addVertices();
        RuntimeException injected = new IllegalStateException("third index query failed");
        RuntimeException closing = new IllegalStateException("first index cursor close failed");
        try (IndexCursorTracker tracker = new IndexCursorTracker(closing, injected, 3)) {
            RuntimeException failure = Assert.assertThrows(RuntimeException.class,
                    () -> graph().vertices(this.nameQuery(HugeType.VERTEX)));
            Assert.assertSame(injected, Throwables.getRootCause(failure));
            tracker.assertCounts(2, 1);
            Assert.assertEquals(1, injected.getSuppressed().length);
            Assert.assertSame(closing, Throwables.getRootCause(injected.getSuppressed()[0]));
        }
    }

    @Test
    public void testJointPlanClosesUnselectedCursorsAndTransfersSelected() {
        Set<Vertex> expected = this.prepareJointIndexes();
        ConditionQuery query = this.jointQuery();
        try (IndexCursorTracker tracker = new IndexCursorTracker(null)) {
            Iterator<Vertex> results = graph().vertices(query);
            try {
                Assert.assertEquals(2, tracker.cursors.size());
                Assert.assertEquals(1, tracker.cursors.stream().mapToInt(cursor -> cursor.closes).sum());
                Assert.assertEquals(expected, ImmutableSet.copyOf(results));
            } finally {
                CloseableIterator.closeIterator(results);
            }
            tracker.assertCounts(2, 1);
        }
    }

    @Test
    public void testJointReadFailureClosesAllCursorsAndPreservesError() {
        this.prepareJointIndexes();
        RuntimeException injected = new IllegalStateException("second joint cursor read failed");
        RuntimeException closing = new IllegalStateException("first joint cursor close failed");
        try (IndexCursorTracker tracker = new IndexCursorTracker(closing)) {
            tracker.failOnRead = 2;
            tracker.readFailure = injected;
            RuntimeException failure = Assert.assertThrows(RuntimeException.class,
                    () -> graph().vertices(this.jointQuery()));
            Assert.assertSame(injected, Throwables.getRootCause(failure));
            tracker.assertCounts(2, 1);
            Assert.assertEquals(1, injected.getSuppressed().length);
            Assert.assertSame(closing, Throwables.getRootCause(injected.getSuppressed()[0]));
        }
    }

    @Test
    public void testSearchReadFailureClosesCursorAndPreservesError() {
        graph().schema().indexLabel("coverageASearch").onV("coverageA")
               .by("coverageBody").search().create();
        this.addVertices();
        RuntimeException injected = new IllegalStateException("search index cursor read failed");
        RuntimeException closing = new IllegalStateException("search index cursor close failed");
        try (IndexCursorTracker tracker = new IndexCursorTracker(closing)) {
            tracker.failOnRead = 1;
            tracker.readFailure = injected;
            RuntimeException failure = Assert.assertThrows(RuntimeException.class,
                    () -> graph().traversal().V().hasLabel("coverageA")
                            .has("coverageBody", Text.contains("gold")).toSet());
            Assert.assertSame(injected, Throwables.getRootCause(failure));
            tracker.assertCounts(1, 1);
            Assert.assertEquals(1, injected.getSuppressed().length);
            Assert.assertSame(closing, Throwables.getRootCause(injected.getSuppressed()[0]));
        }
    }

    @Test
    public void testSearchDecodeFailureClosesCursorAndPreservesError() {
        graph().schema().indexLabel("coverageASearch").onV("coverageA")
               .by("coverageBody").search().create();
        this.addVertices();
        RuntimeException injected = new IllegalStateException("search index decode failed");
        RuntimeException closing = new IllegalStateException("search index cursor close failed");
        GraphIndexTransaction indexTx = Whitebox.getInternalState(params().graphTransaction(), "indexTx");
        AbstractSerializer original = Whitebox.getInternalState(indexTx, "serializer");
        AbstractSerializer observed = Mockito.mock(AbstractSerializer.class,
                                                    AdditionalAnswers.delegatesTo(original));
        Mockito.doAnswer(invocation -> {
            // Decode a real backend entry before failing outside the iterator wrapper.
            original.readIndex(invocation.getArgument(0), invocation.getArgument(1),
                               invocation.getArgument(2));
            throw injected;
        }).when(observed).readIndex(Mockito.any(), Mockito.any(), Mockito.any());
        Whitebox.setInternalState(indexTx, "serializer", observed);
        try (IndexCursorTracker tracker = new IndexCursorTracker(closing)) {
            RuntimeException failure = Assert.assertThrows(RuntimeException.class,
                    () -> graph().traversal().V().hasLabel("coverageA")
                            .has("coverageBody", Text.contains("gold")).toSet());
            Assert.assertSame(injected, Throwables.getRootCause(failure));
            tracker.assertCounts(1, 1);
            Assert.assertEquals(1, injected.getSuppressed().length);
            Assert.assertSame(closing, Throwables.getRootCause(injected.getSuppressed()[0]));
        } finally {
            Whitebox.setInternalState(indexTx, "serializer", original);
        }
    }

    private Set<Vertex> prepareJointIndexes() {
        this.nameIndex("coverageA");
        graph().schema().indexLabel("coverageAByScore").onV("coverageA")
               .by("coverageScore").secondary().create();
        // Exceed the fixture's intersection threshold of 2 to retain lazy cursors.
        Set<Vertex> expected = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            expected.add(graph().addVertex(T.label, "coverageA", "coverageName", "same",
                                          "coverageScore", 10, "coverageBody", "gold"));
        }
        this.commitTx();
        return expected;
    }

    private ConditionQuery jointQuery() {
        ConditionQuery query = this.nameQuery(HugeType.VERTEX);
        query.eq(HugeKeys.LABEL, graph().vertexLabel("coverageA").id());
        query.query(Condition.eq(graph().propertyKey("coverageScore").id(), 10));
        return query;
    }

    private void prepareOrIndexes(boolean complete) {
        this.nameIndex("coverageA");
        this.nameIndex("coverageB");
        graph().schema().indexLabel("coverageAByScore").onV("coverageA")
               .by("coverageScore").secondary().create();
        if (complete) {
            graph().schema().indexLabel("coverageBByScore").onV("coverageB")
                   .by("coverageScore").secondary().create();
        }
    }

    private ConditionQuery orQuery() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(Condition.or(
                Condition.eq(graph().propertyKey("coverageName").id(), "same"),
                Condition.eq(graph().propertyKey("coverageScore").id(), 10)));
        return query;
    }

    private final class IndexCursorTracker implements AutoCloseable {

        private final GraphIndexTransaction indexTx;
        private final BackendStore original;
        private final List<CountingIndexCursor> cursors = new ArrayList<>();
        private int failOnRead;
        private RuntimeException readFailure;

        private IndexCursorTracker(RuntimeException closeFailure) {
            this(closeFailure, null, 0);
        }

        private IndexCursorTracker(RuntimeException closeFailure, RuntimeException queryFailure,
                                   int failOnQuery) {
            this.indexTx = Whitebox.getInternalState(params().graphTransaction(), "indexTx");
            this.original = Whitebox.getInternalState(this.indexTx, "store");
            BackendStore observed = Mockito.mock(BackendStore.class,
                                                 AdditionalAnswers.delegatesTo(this.original));
            Mockito.doAnswer(invocation -> {
                if (queryFailure != null && this.cursors.size() + 1 == failOnQuery) {
                    throw queryFailure;
                }
                // Keep real index matching, validation, serialization and backend queries.
                Iterator<BackendEntry> entries = this.original.query((Query) invocation.getArgument(0));
                CountingIndexCursor cursor = new CountingIndexCursor(entries,
                        this.cursors.isEmpty() ? closeFailure : null);
                this.cursors.add(cursor);
                if (this.cursors.size() == this.failOnRead) {
                    cursor.readFailure = this.readFailure;
                }
                return cursor;
            }).when(observed).query(Mockito.any(Query.class));
            Whitebox.setInternalState(this.indexTx, "store", observed);
        }

        private void assertCounts(int opened, int closesPerCursor) {
            Assert.assertEquals(opened, this.cursors.size());
            for (CountingIndexCursor cursor : this.cursors) {
                Assert.assertEquals(closesPerCursor, cursor.closes);
            }
        }

        @Override
        public void close() {
            Whitebox.setInternalState(this.indexTx, "store", this.original);
            // Release leaks even when the regression assertion fails on old code.
            for (CountingIndexCursor cursor : this.cursors) {
                if (cursor.closes == 0) {
                    CloseableIterator.closeIterator(cursor.entries);
                }
            }
        }
    }

    private static final class CountingIndexCursor implements CloseableIterator<BackendEntry> {

        private final Iterator<BackendEntry> entries;
        private final RuntimeException closeFailure;
        private int closes;
        private RuntimeException readFailure;

        private CountingIndexCursor(Iterator<BackendEntry> entries, RuntimeException closeFailure) {
            this.entries = entries;
            this.closeFailure = closeFailure;
        }

        @Override
        public boolean hasNext() {
            if (this.readFailure != null) {
                throw this.readFailure;
            }
            return this.entries.hasNext();
        }

        @Override
        public BackendEntry next() {
            return this.entries.next();
        }

        @Override
        public void close() {
            this.closes++;
            CloseableIterator.closeIterator(this.entries);
            if (this.closeFailure != null) {
                throw this.closeFailure;
            }
        }
    }

    @Test
    public void testNegativeRootLabelKeepsPropertyFilterLocal() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("name").asText().create();
        for (String label : new String[]{"indexed", "unindexed"}) {
            schema.vertexLabel(label).properties("name").useAutomaticId().create();
        }
        schema.indexLabel("indexedByName").onV("indexed").by("name").secondary().create();
        Vertex allowed = graph().addVertex(T.label, "indexed", "name", "alice");
        graph().addVertex(T.label, "unindexed", "name", "alice");
        graph().addVertex(T.label, "indexed", "name", "bob");
        this.commitTx();

        // Control: the unqualified indexed query really has incomplete coverage.
        Assert.assertThrows(NoIndexException.class, () -> graph().traversal().V()
                .has("name", "alice").toSet(), error ->
                Assert.assertContains("Incomplete index coverage", error.getMessage()));

        try (IndexCursorTracker tracker = new IndexCursorTracker(null)) {
            GraphTraversal<Vertex, Vertex> query = graph().traversal().V()
                    .hasLabel(P.neq("unindexed")).has("name", "alice");
            query.asAdmin().applyStrategies();
            HugeGraphStep<?, ?> source = (HugeGraphStep<?, ?>) query.asAdmin().getStartStep();
            // The negative label keeps both predicates local, so the source
            // scans vertices without running property-index coverage checks.
            Assert.assertTrue(source.getHasContainers().isEmpty());
            Assert.assertEquals(ImmutableSet.of(allowed), query.toSet());
            tracker.assertCounts(0, 0);
        }
    }

    @Test
    public void testInvalidValuePrecedesPartialCoverageError() {
        this.addVertices();
        ConditionQuery invalid = new ConditionQuery(HugeType.VERTEX);
        invalid.query(Condition.eq(graph().propertyKey("coverageScore").id(), "not-an-int"));
        Assert.assertThrows(NoIndexException.class, () -> graph().vertices(invalid).hasNext());

        graph().schema().indexLabel("coverageAByScore").onV("coverageA")
               .by("coverageScore").secondary().create();
        Assert.assertEquals(ImmutableList.of(), ImmutableList.copyOf(graph().vertices(invalid)));

        ConditionQuery valid = new ConditionQuery(HugeType.VERTEX);
        valid.query(Condition.eq(graph().propertyKey("coverageScore").id(), 10));
        Assert.assertThrows(NoIndexException.class, () -> graph().vertices(valid).hasNext());
    }

    @Test
    public void testOlapOnlyQueryAcrossLabels() {
        // The existing OLAP secondary-property query also returns no results
        // on HStore master; its backend coverage is tracked in #3090.
        Assume.assumeFalse("HStore OLAP secondary query is tracked in #3090",
                           "hstore".equals(graph().backend()));
        Assume.assumeTrue("Not support olap properties",
                          storeFeatures().supportsOlapProperties());
        graph().schema().propertyKey("coverageOlap").asText()
               .writeType(WriteType.OLAP_SECONDARY).create();
        Set<Vertex> expected = this.addVertices();
        for (Vertex vertex : expected) {
            graph().addVertex(T.id, vertex.id(), "coverageOlap", "same");
        }
        this.commitTx();

        GraphReadMode previous = graph().readMode();
        graph().readMode(GraphReadMode.ALL);
        try {
            Assert.assertEquals(expected,
                                graph().traversal().V().has("coverageOlap", "same").toSet());
        } finally {
            graph().readMode(previous);
        }
    }

    @Test
    public void testEmptyCandidateLabelStillRequiresCoverage() {
        this.nameIndex("coverageA");
        graph().addVertex(T.label, "coverageA", "coverageName", "same",
                          "coverageScore", 10, "coverageBody", "gold");
        this.commitTx();
        Assert.assertThrows(NoIndexException.class, () ->
                graph().traversal().V().has("coverageName", "same").toList());
    }

    @Test
    public void testNoCoverageAndExplicitIds() {
        Set<Vertex> expected = this.addVertices();
        Assert.assertThrows(NoIndexException.class, () ->
                graph().traversal().V().has("coverageName", "same").toList());
        Object[] ids = expected.stream().map(Vertex::id).toArray();
        Assert.assertEquals(expected,
                            graph().traversal().V(ids).has("coverageName", "same").toSet());
        graph().schema().edgeLabel("coverageLink").link("coverageA", "coverageB").create();
        Vertex a = expected.stream().filter(v -> v.label().equals("coverageA")).findFirst().get();
        Vertex b = expected.stream().filter(v -> v.label().equals("coverageB")).findFirst().get();
        a.addEdge("coverageLink", b);
        this.commitTx();
        Assert.assertEquals(ImmutableSet.of(b), graph().traversal().V(a.id())
                .out("coverageLink").has("coverageName", "same").toSet());
    }

    @Test
    public void testDownstreamStepsCannotHidePartialCoverage() {
        this.nameIndex("coverageA");
        this.addVertices();
        for (GraphTraversal<?, ?> query : new GraphTraversal<?, ?>[]{
                graph().traversal().V().has("coverageName", "same").limit(1),
                graph().traversal().V().has("coverageName", "same").count(),
                graph().traversal().V().has("coverageName", "same").out(),
                graph().traversal().V().has("coverageName", "same").where(__.out()),
                graph().traversal().V().has("coverageName", "same").barrier()}) {
            Assert.assertThrows(NoIndexException.class, query::toList);
        }
    }

    @Test
    public void testPreparedTraversalChecksCurrentSchema() {
        this.nameIndex("coverageA");
        this.nameIndex("coverageB");
        this.addVertices();
        GraphTraversal<?, ?> query = graph().traversal().V().has("coverageName", "same");
        query.asAdmin().applyStrategies();
        graph().schema().vertexLabel("coverageLater").properties("coverageName")
               .useAutomaticId().create();
        graph().addVertex(T.label, "coverageLater", "coverageName", "same");
        this.commitTx();
        Assert.assertThrows(NoIndexException.class, query::toList);
        this.nameIndex("coverageLater");
        Assert.assertEquals(3, graph().traversal().V().has("coverageName", "same").toList().size());
        graph().schema().indexLabel("coverageBByName").remove();
        Assert.assertThrows(NoIndexException.class, () ->
                graph().traversal().V().has("coverageName", "same").toList());
    }

    @Test
    public void testRangeRequiresCompatibleCoverage() {
        SchemaManager schema = graph().schema();
        schema.indexLabel("coverageAByScore").onV("coverageA").by("coverageScore").range().create();
        schema.indexLabel("coverageBByScore").onV("coverageB")
              .by("coverageScore").secondary().create();
        Set<Vertex> expected = this.addVertices();
        Assert.assertThrows(NoIndexException.class, () ->
                graph().traversal().V().has("coverageScore", P.gte(10)).toList());
        // Equality is supported by both RANGE and SECONDARY indexes.
        Assert.assertEquals(1, graph().traversal().V().has("coverageScore", 10).toList().size());
        schema.indexLabel("coverageBByScore").remove();
        schema.indexLabel("coverageBRange").onV("coverageB").by("coverageScore").range().create();
        Assert.assertEquals(expected,
                            graph().traversal().V().has("coverageScore", P.gte(10)).toSet());
    }

    @Test
    public void testSearchRequiresCompatibleCoverage() {
        SchemaManager schema = graph().schema();
        schema.indexLabel("coverageASearch").onV("coverageA").by("coverageBody").search().create();
        schema.indexLabel("coverageBSecondary").onV("coverageB")
              .by("coverageBody").secondary().create();
        this.addVertices();
        Assert.assertThrows(NoIndexException.class, () ->
                graph().traversal().V().has("coverageBody", Text.contains("(gold)")).toList());
        schema.indexLabel("coverageBSearch").onV("coverageB").by("coverageBody").search().create();
        schema.indexLabel("unrelatedSearch").onV("unrelated").by("coverageBody").search().create();
        Assert.assertEquals(2, graph().traversal().V()
                                    .has("coverageBody", Text.contains("(gold)")).toList().size());
    }

    @Test
    public void testCompositeCoverage() {
        SchemaManager schema = graph().schema();
        schema.indexLabel("coverageAComposite").onV("coverageA")
              .by("coverageName", "coverageScore").secondary().create();
        this.nameIndex("coverageB");
        this.addVertices();
        Assert.assertThrows(NoIndexException.class, () -> graph().traversal().V()
                .has("coverageName", "same").has("coverageScore", 10).toList());
        schema.indexLabel("coverageBComposite").onV("coverageB")
              .by("coverageName", "coverageScore").secondary().create();
        Assert.assertEquals(1, graph().traversal().V().has("coverageName", "same")
                                    .has("coverageScore", 10).toList().size());
    }

    @Test
    public void testJointIndexCoverage() {
        this.nameIndex("coverageA");
        this.nameIndex("coverageB");
        SchemaManager schema = graph().schema();
        schema.indexLabel("coverageARange").onV("coverageA")
              .by("coverageScore").range().create();
        Set<Vertex> vertices = this.addVertices();
        Assert.assertThrows(NoIndexException.class, () -> graph().traversal().V()
                .has("coverageName", "same").has("coverageScore", P.gte(15)).toList());
        schema.indexLabel("coverageBRange").onV("coverageB")
              .by("coverageScore").range().create();
        Vertex expected = vertices.stream().filter(v -> v.label().equals("coverageB"))
                                  .findFirst().get();
        Assert.assertEquals(ImmutableSet.of(expected), graph().traversal().V()
                .has("coverageName", "same").has("coverageScore", P.gte(15)).toSet());
    }

    @Test
    public void testUnlabeledMixedCompositeAndJointIndexes() {
        SchemaManager schema = graph().schema();
        schema.indexLabel("coverageAComposite").onV("coverageA")
              .by("coverageName", "coverageScore").secondary().create();
        this.nameIndex("coverageB");
        schema.indexLabel("coverageBRange").onV("coverageB")
              .by("coverageScore").range().create();
        Set<Vertex> expected = this.addVertices();
        for (Vertex vertex : expected) {
            vertex.property("coverageScore", 10);
        }
        // Each individual index also matches a vertex outside the intersection.
        graph().addVertex(T.label, "coverageB", "coverageName", "other",
                          "coverageScore", 10, "coverageBody", "gold");
        graph().addVertex(T.label, "coverageB", "coverageName", "same",
                          "coverageScore", 30, "coverageBody", "gold");
        this.commitTx();
        Assert.assertEquals(expected, graph().traversal().V()
                .has("coverageName", "same").has("coverageScore", 10).toSet());

        IndexLabel index = schema.getIndexLabel("coverageBRange");
        try {
            this.params().schemaTransaction().updateSchemaStatus(index, SchemaStatus.REBUILDING);
            Assert.assertThrows(IllegalArgumentException.class, () -> graph().traversal().V()
                    .has("coverageName", "same").has("coverageScore", 10).toSet());
        } finally {
            this.params().schemaTransaction().updateSchemaStatus(index, SchemaStatus.CREATED);
        }
        Assert.assertEquals(expected, graph().traversal().V()
                .has("coverageName", "same").has("coverageScore", 10).toSet());
    }

    @Test
    public void testAllVertexCandidatesDeletingReturnEmpty() {
        this.nameIndex("coverageA");
        this.nameIndex("coverageB");
        Set<Vertex> expected = this.addVertices();
        SchemaLabel a = graph().schema().getVertexLabel("coverageA");
        SchemaLabel b = graph().schema().getVertexLabel("coverageB");
        try {
            for (SchemaStatus status : new SchemaStatus[]{SchemaStatus.DELETING,
                                                         SchemaStatus.UNDELETED}) {
                this.params().schemaTransaction().updateSchemaStatus(a, status);
                this.params().schemaTransaction().updateSchemaStatus(b, status);
                Assert.assertEquals(ImmutableSet.of(), graph().traversal().V()
                        .has("coverageName", "same").toSet());
                Assert.assertEquals(ImmutableSet.of(), graph().traversal().V()
                        .hasLabel("coverageA").has("coverageName", "same").toSet());
                this.assertEmptyPage(this.nameQuery(HugeType.VERTEX));
                ConditionQuery query = this.nameQuery(HugeType.VERTEX);
                query.showDeleting(true);
                Assert.assertEquals(expected, ImmutableSet.copyOf(graph().vertices(query)));
                ConditionQuery child = query.copy();
                child.showDeleting(false);
                child.setOriginQuery(query);
                Assert.assertEquals(expected, ImmutableSet.copyOf(graph().vertices(child)));
            }
            graph().schema().indexLabel("coverageAByName").remove();
            graph().schema().indexLabel("coverageBByName").remove();
            Assert.assertEquals(ImmutableSet.of(), graph().traversal().V()
                    .has("coverageName", "same").toSet());
            ConditionQuery query = this.nameQuery(HugeType.VERTEX);
            query.showDeleting(true);
            Assert.assertThrows(NoIndexException.class, () -> graph().vertices(query).hasNext());
            // A visible, unindexed candidate must still fail next to an invisible one.
            this.params().schemaTransaction().updateSchemaStatus(a, SchemaStatus.CREATED);
            Assert.assertThrows(NoIndexException.class, () -> graph().traversal().V()
                    .has("coverageName", "same").toSet());
        } finally {
            this.params().schemaTransaction().updateSchemaStatus(a, SchemaStatus.CREATED);
            this.params().schemaTransaction().updateSchemaStatus(b, SchemaStatus.CREATED);
        }
    }

    @Test
    public void testOnlyEdgeCandidateDeletingReturnsEmpty() {
        SchemaManager schema = graph().schema();
        schema.edgeLabel("deletingEdge").link("coverageA", "coverageB")
              .properties("coverageName").create();
        schema.indexLabel("deletingEdgeByName").onE("deletingEdge")
              .by("coverageName").secondary().create();
        Set<Vertex> vertices = this.addVertices();
        Vertex a = vertices.stream().filter(v -> v.label().equals("coverageA")).findFirst().get();
        Vertex b = vertices.stream().filter(v -> v.label().equals("coverageB")).findFirst().get();
        Edge edge = a.addEdge("deletingEdge", b, "coverageName", "same");
        this.commitTx();
        SchemaLabel label = schema.getEdgeLabel("deletingEdge");
        try {
            this.params().schemaTransaction().updateSchemaStatus(label, SchemaStatus.DELETING);
            Assert.assertEquals(ImmutableSet.of(), graph().traversal().E()
                    .has("coverageName", "same").toSet());
            this.assertEmptyPage(this.nameQuery(HugeType.EDGE));
            ConditionQuery query = this.nameQuery(HugeType.EDGE);
            query.showDeleting(true);
            Assert.assertEquals(ImmutableSet.of(edge), ImmutableSet.copyOf(graph().edges(query)));
            schema.indexLabel("deletingEdgeByName").remove();
            Assert.assertEquals(ImmutableSet.of(), graph().traversal().E()
                    .has("coverageName", "same").toSet());
            Assert.assertThrows(NoIndexException.class, () -> graph().edges(query).hasNext());
        } finally {
            this.params().schemaTransaction().updateSchemaStatus(label, SchemaStatus.CREATED);
        }
        Assert.assertThrows(NoIndexException.class, () -> graph().traversal().E()
                .has("coverageName", "same").toSet());
    }

    @Test
    public void testOnlyHiddenCandidateReturnsEmpty() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("hiddenName").asText().create();
        VertexLabel hidden = schema.vertexLabel("~coverageHidden")
                                   .properties("hiddenName").useAutomaticId().build();
        this.params().schemaTransaction().addVertexLabel(hidden);
        try {
            schema.indexLabel("hiddenByName").onV("~coverageHidden")
                  .by("hiddenName").secondary().create();
            Vertex vertex = this.params().graphTransaction().addVertex(
                    this.params().graphTransaction().constructVertex(
                            false, T.label, "~coverageHidden", "hiddenName", "same"));
            this.commitTx();
            Assert.assertEquals(ImmutableSet.of(), graph().traversal().V()
                    .has("hiddenName", "same").toSet());
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            query.query(Condition.eq(graph().propertyKey("hiddenName").id(), "same"));
            this.assertEmptyPage(query.copy());
            query.showHidden(true);
            Assert.assertEquals(ImmutableSet.of(vertex), ImmutableSet.copyOf(graph().vertices(query)));
            schema.indexLabel("hiddenByName").remove();
            Assert.assertEquals(ImmutableSet.of(), graph().traversal().V()
                    .has("hiddenName", "same").toSet());
            Assert.assertThrows(NoIndexException.class, () -> graph().vertices(query).hasNext());
        } finally {
            // System schema cannot be deleted individually. Reset the test
            // graph so the hidden label cannot retain this test's property key.
            CoreTestSuite.clear();
        }
    }

    @Test
    public void testIndexStatusAndRebuild() {
        this.nameIndex("coverageA");
        this.nameIndex("coverageB");
        Set<Vertex> expected = this.addVertices();
        graph().schema().indexLabel("coverageBByName").rebuild();
        Assert.assertEquals(expected,
                            graph().traversal().V().has("coverageName", "same").toSet());
        IndexLabel index = graph().schema().getIndexLabel("coverageBByName");
        try {
            for (SchemaStatus status : new SchemaStatus[]{SchemaStatus.CREATING,
                                                         SchemaStatus.REBUILDING,
                                                         SchemaStatus.DELETING}) {
                this.params().schemaTransaction().updateSchemaStatus(index, status);
                Assert.assertThrows(IllegalArgumentException.class, () ->
                        graph().traversal().V().has("coverageName", "same").toList());
            }
        } finally {
            this.params().schemaTransaction().updateSchemaStatus(index, SchemaStatus.CREATED);
        }
        Assert.assertEquals(expected,
                            graph().traversal().V().has("coverageName", "same").toSet());
    }

    @Test
    public void testDeletingVertexLabelUsesQueryVisibility() {
        this.nameIndex("coverageA");
        this.nameIndex("coverageB");
        Set<Vertex> expected = this.addVertices();
        Vertex visible = expected.stream().filter(v -> v.label().equals("coverageA"))
                                 .findFirst().get();
        SchemaLabel deleting = graph().schema().getVertexLabel("coverageB");
        try {
            this.params().schemaTransaction().updateSchemaStatus(deleting, SchemaStatus.DELETING);
            IndexLabel index = graph().schema().getIndexLabel("coverageBByName");
            this.params().schemaTransaction().updateSchemaStatus(index, SchemaStatus.DELETING);
            // A hidden label's unavailable index must not be validated either.
            Assert.assertEquals(ImmutableSet.of(visible), graph().traversal().V()
                    .has("coverageName", "same").toSet());
            this.params().schemaTransaction().updateSchemaStatus(index, SchemaStatus.CREATED);
            ConditionQuery includeDeleting = this.nameQuery(HugeType.VERTEX);
            includeDeleting.showDeleting(true);
            Assert.assertEquals(expected, ImmutableSet.copyOf(graph().vertices(includeDeleting)));
            graph().schema().indexLabel("coverageBByName").remove();

            for (SchemaStatus status : new SchemaStatus[]{SchemaStatus.DELETING,
                                                         SchemaStatus.UNDELETED}) {
                this.params().schemaTransaction().updateSchemaStatus(deleting, status);
                Assert.assertEquals(ImmutableSet.of(visible), graph().traversal().V()
                        .has("coverageName", "same").toSet());
                ConditionQuery query = this.nameQuery(HugeType.VERTEX);
                query.showDeleting(true);
                Assert.assertThrows(NoIndexException.class,
                                    () -> graph().vertices(query).hasNext());
                // Flattened queries inherit visibility from the outer request.
                ConditionQuery child = query.copy();
                child.showDeleting(false);
                child.setOriginQuery(query);
                Assert.assertThrows(NoIndexException.class,
                                    () -> graph().vertices(child).hasNext());
            }
        } finally {
            this.params().schemaTransaction().updateSchemaStatus(deleting, SchemaStatus.CREATED);
        }
        Assert.assertThrows(NoIndexException.class, () -> graph().traversal().V()
                .has("coverageName", "same").toSet());
        this.nameIndex("coverageB");
        Assert.assertEquals(expected,
                            graph().traversal().V().has("coverageName", "same").toSet());
    }

    @Test
    public void testDeletingEdgeLabelUsesQueryVisibility() {
        SchemaManager schema = graph().schema();
        for (String label : new String[]{"visibleEdge", "deletingEdge"}) {
            schema.edgeLabel(label).link("coverageA", "coverageB")
                  .properties("coverageName").create();
            schema.indexLabel(label + "ByName").onE(label)
                  .by("coverageName").secondary().create();
        }
        Set<Vertex> vertices = this.addVertices();
        Vertex a = vertices.stream().filter(v -> v.label().equals("coverageA")).findFirst().get();
        Vertex b = vertices.stream().filter(v -> v.label().equals("coverageB")).findFirst().get();
        Edge visible = a.addEdge("visibleEdge", b, "coverageName", "same");
        Edge removed = a.addEdge("deletingEdge", b, "coverageName", "same");
        this.commitTx();
        SchemaLabel deleting = schema.getEdgeLabel("deletingEdge");
        try {
            this.params().schemaTransaction().updateSchemaStatus(deleting, SchemaStatus.DELETING);
            ConditionQuery includeDeleting = this.nameQuery(HugeType.EDGE);
            includeDeleting.showDeleting(true);
            Assert.assertEquals(ImmutableSet.of(visible, removed),
                                ImmutableSet.copyOf(graph().edges(includeDeleting)));
            schema.indexLabel("deletingEdgeByName").remove();
            for (SchemaStatus status : new SchemaStatus[]{SchemaStatus.DELETING,
                                                         SchemaStatus.UNDELETED}) {
                this.params().schemaTransaction().updateSchemaStatus(deleting, status);
                Assert.assertEquals(ImmutableSet.of(visible), graph().traversal().E()
                        .has("coverageName", "same").toSet());
                ConditionQuery query = this.nameQuery(HugeType.EDGE);
                query.showDeleting(true);
                Assert.assertThrows(NoIndexException.class, () -> graph().edges(query).hasNext());
            }
        } finally {
            this.params().schemaTransaction().updateSchemaStatus(deleting, SchemaStatus.CREATED);
        }
        Assert.assertThrows(NoIndexException.class, () -> graph().traversal().E()
                .has("coverageName", "same").toSet());
        schema.indexLabel("deletingEdgeByName").onE("deletingEdge")
              .by("coverageName").secondary().create();
        Assert.assertEquals(ImmutableSet.of(visible, removed), graph().traversal().E()
                .has("coverageName", "same").toSet());
    }

    @Test
    public void testPagingWhileVertexLabelIsDeleting() {
        Assume.assumeTrue(storeFeatures().supportsQueryByPage());
        this.nameIndex("coverageA");
        Set<Vertex> vertices = this.addVertices();
        Vertex a = vertices.stream().filter(v -> v.label().equals("coverageA")).findFirst().get();
        Vertex another = graph().addVertex(T.label, "coverageA", "coverageName", "same",
                                           "coverageScore", 30, "coverageBody", "gold");
        this.commitTx();
        SchemaLabel deleting = graph().schema().getVertexLabel("coverageB");
        try {
            this.params().schemaTransaction().updateSchemaStatus(deleting, SchemaStatus.DELETING);
            Set<Vertex> actual = new HashSet<>();
            String page = "";
            int requests = 0;
            do {
                Iterator<Vertex> iterator = graph().vertices(this.pageQuery(page));
                try {
                    while (iterator.hasNext()) {
                        Assert.assertTrue(actual.add(iterator.next()));
                    }
                    page = PageInfo.pageInfo(iterator);
                } finally {
                    CloseableIterator.closeIterator(iterator);
                }
                this.params().schemaTransaction().updateSchemaStatus(deleting,
                                                                    SchemaStatus.UNDELETED);
                Assert.assertTrue(++requests < 10);
            } while (page != null && !page.isEmpty());
            Assert.assertEquals(ImmutableSet.of(a, another), actual);
            Assert.assertTrue(requests > 1);
        } finally {
            this.params().schemaTransaction().updateSchemaStatus(deleting, SchemaStatus.CREATED);
        }
        Assert.assertThrows(NoIndexException.class, () -> this.page(""));
    }

    private ConditionQuery nameQuery(HugeType type) {
        ConditionQuery query = new ConditionQuery(type);
        query.query(Condition.eq(graph().propertyKey("coverageName").id(), "same"));
        return query;
    }

    @Test
    public void testGlobalEdgeCoverage() {
        SchemaManager schema = graph().schema();
        schema.edgeLabel("edgeA").link("coverageA", "coverageB")
              .properties("coverageName").create();
        schema.edgeLabel("edgeB").link("coverageA", "coverageB")
              .properties("coverageName").enableLabelIndex(false).create();
        schema.indexLabel("edgeAByName").onE("edgeA").by("coverageName").secondary().create();
        this.addVertices();
        Iterator<Vertex> vertices = graph().traversal().V().toList().iterator();
        Vertex a = vertices.next();
        Vertex b = vertices.next();
        if (a.label().equals("coverageB")) {
            Vertex swap = a;
            a = b;
            b = swap;
        }
        a.addEdge("edgeA", b, "coverageName", "same");
        a.addEdge("edgeB", b, "coverageName", "same");
        this.commitTx();
        Assert.assertThrows(NoIndexException.class, () ->
                graph().traversal().E().has("coverageName", "same").toList());
        schema.indexLabel("edgeBByName").onE("edgeB").by("coverageName").secondary().create();
        Assert.assertEquals(2,
                            graph().traversal().E().has("coverageName", "same").toList().size());
    }

    @Test
    public void testParentEdgeIndexCoversSubLabels() {
        Assume.assumeTrue(storeFeatures().supportsFatherAndSubEdgeLabel());
        SchemaManager schema = graph().schema();
        schema.edgeLabel("parentEdge").asBase().properties("coverageName", "coverageScore")
              .multiTimes().sortKeys("coverageScore").create();
        for (String label : new String[]{"subA", "subB"}) {
            schema.edgeLabel(label).withBase("parentEdge").link("coverageA", "coverageB")
                  .properties("coverageName", "coverageScore")
                  .multiTimes().sortKeys("coverageScore").create();
        }
        schema.indexLabel("parentByName").onE("parentEdge")
              .by("coverageName").secondary().create();
        Set<Vertex> vertices = this.addVertices();
        Vertex a = vertices.stream().filter(v -> v.label().equals("coverageA")).findFirst().get();
        Vertex b = vertices.stream().filter(v -> v.label().equals("coverageB")).findFirst().get();
        a.addEdge("subA", b, "coverageName", "same", "coverageScore", 1);
        a.addEdge("subB", b, "coverageName", "same", "coverageScore", 2);
        this.commitTx();
        Assert.assertEquals(2,
                            graph().traversal().E().has("coverageName", "same").toList().size());
        schema.indexLabel("parentByName").remove();
        schema.indexLabel("subAByName").onE("subA").by("coverageName").secondary().create();
        Assert.assertThrows(NoIndexException.class, () ->
                graph().traversal().E().has("coverageName", "same").toList());
        schema.indexLabel("subBByName").onE("subB").by("coverageName").secondary().create();
        Assert.assertEquals(2,
                            graph().traversal().E().has("coverageName", "same").toList().size());

        SchemaLabel deleting = schema.getEdgeLabel("subB");
        try {
            this.params().schemaTransaction().updateSchemaStatus(deleting, SchemaStatus.DELETING);
            schema.indexLabel("subBByName").remove();
            Assert.assertEquals(ImmutableList.of("subA"), graph().traversal().E()
                    .has("coverageName", "same").label().toList());
            ConditionQuery query = this.nameQuery(HugeType.EDGE);
            query.showDeleting(true);
            Assert.assertThrows(NoIndexException.class, () -> graph().edges(query).hasNext());
        } finally {
            this.params().schemaTransaction().updateSchemaStatus(deleting, SchemaStatus.CREATED);
        }
        Assert.assertThrows(NoIndexException.class, () -> graph().traversal().E()
                .has("coverageName", "same").toList());
        schema.indexLabel("subBByName").onE("subB").by("coverageName").secondary().create();
        Assert.assertEquals(2,
                            graph().traversal().E().has("coverageName", "same").toList().size());

        SchemaLabel subA = schema.getEdgeLabel("subA");
        try {
            this.params().schemaTransaction().updateSchemaStatus(subA, SchemaStatus.DELETING);
            this.params().schemaTransaction().updateSchemaStatus(deleting, SchemaStatus.DELETING);
            schema.indexLabel("subAByName").remove();
            schema.indexLabel("subBByName").remove();
            // The visible base label has no direct records of its own.
            Assert.assertEquals(ImmutableSet.of(), graph().traversal().E()
                    .has("coverageName", "same").toSet());
        } finally {
            this.params().schemaTransaction().updateSchemaStatus(subA, SchemaStatus.CREATED);
            this.params().schemaTransaction().updateSchemaStatus(deleting, SchemaStatus.CREATED);
        }
    }

    @Test
    public void testPagingRequiresCoverageOnEveryRequest() {
        Assume.assumeTrue(storeFeatures().supportsQueryByPage());
        this.nameIndex("coverageA");
        Set<Vertex> expected = this.addVertices();
        Assert.assertThrows(NoIndexException.class, () -> this.page(""));
        this.nameIndex("coverageB");
        Set<Vertex> actual = new HashSet<>();
        String page = "";
        String resume = null;
        int pages = 0;
        do {
            ConditionQuery query = this.pageQuery(page);
            Iterator<Vertex> iterator = graph().vertices(query);
            try {
                iterator.forEachRemaining(actual::add);
                page = PageInfo.pageInfo(iterator);
                if (resume == null) {
                    resume = page;
                }
            } finally {
                CloseableIterator.closeIterator(iterator);
            }
            Assert.assertTrue(++pages < 10);
        } while (page != null && !page.isEmpty());
        Assert.assertEquals(expected, actual);
        Assert.assertNotNull(resume);
        String continuation = resume;
        graph().schema().indexLabel("coverageBByName").remove();
        Assert.assertThrows(NoIndexException.class, () -> this.page(continuation));
    }

    private void assertEmptyPage(ConditionQuery query) {
        if (!storeFeatures().supportsQueryByPage()) {
            return;
        }
        query.limit(1);
        query.page("");
        Iterator<?> iterator = query.resultType().isVertex() ? graph().vertices(query) :
                               graph().edges(query);
        try {
            Assert.assertFalse(iterator.hasNext());
            Assert.assertNull(PageInfo.pageInfo(iterator));
        } finally {
            CloseableIterator.closeIterator(iterator);
        }
    }

    private ConditionQuery pageQuery(String page) {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(Condition.eq(graph().propertyKey("coverageName").id(), "same"));
        query.limit(1);
        query.page(page);
        return query;
    }

    private void page(String page) {
        Iterator<Vertex> iterator = graph().vertices(this.pageQuery(page));
        try {
            while (iterator.hasNext()) {
                iterator.next();
            }
        } finally {
            CloseableIterator.closeIterator(iterator);
        }
    }
}
