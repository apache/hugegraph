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

package org.apache.hugegraph.backend.tx;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.CachedGraphTransaction;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery.OptimizedType;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.query.QueryResultContext;
import org.apache.hugegraph.backend.tx.GraphIndexTransaction.RemoveLeftIndexJob;
import org.apache.hugegraph.job.EphemeralJob;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.mockito.Mockito;

public class GraphTransactionTest {

    @Test
    public void testBatchDecisionsRemainFixedAfterOriginChanges() {
        ConditionQuery root = new ConditionQuery(HugeType.VERTEX);
        root.optimized(OptimizedType.PRIMARY_KEY);
        root.showHidden(true);
        root.showDeleting(true);
        root.showExpired(true);
        IdQuery query = new IdQuery(root, IdGenerator.of(1L));
        QueryResultContext context = new QueryResultContext(query);
        root.optimized(OptimizedType.INDEX_FILTER);
        root.showHidden(false);
        root.showDeleting(false);
        root.showExpired(false);
        query.resetIds();
        query.mustSortByInput(false);
        Assert.assertEquals(OptimizedType.PRIMARY_KEY, context.optimizedType());
        Assert.assertTrue(context.conditionFilterRequired());
        Assert.assertTrue(context.showHidden());
        Assert.assertTrue(context.showDeleting());
        Assert.assertTrue(context.showExpired());
        Assert.assertTrue(context.mustSortByInputIds());
        Assert.assertEquals(IdGenerator.of(1L), context.inputIds().get(0));
        Assert.assertEquals(1, context.inputIds().size());
        Assert.assertSame(root, context.matchQuery());
    }

    @Test
    public void testSiblingOptimizationDoesNotReplaceBatchDecision() {
        ConditionQuery root = new ConditionQuery(HugeType.EDGE);
        ConditionQuery first = root.copy();
        first.optimized(OptimizedType.INDEX_FILTER);
        ConditionQuery second = root.copy();
        second.optimized(OptimizedType.INDEX);
        Assert.assertEquals(OptimizedType.INDEX_FILTER, root.optimized());
        QueryResultContext context = new QueryResultContext(
                new IdQuery(second, IdGenerator.of(1L)));
        Assert.assertEquals(OptimizedType.INDEX, context.optimizedType());
        Assert.assertSame(root, context.matchQuery());
    }

    @Test
    public void testDirectIdsHaveNoConditionFilter() {
        IdQuery query = new IdQuery(HugeType.EDGE, IdGenerator.of(1L));
        QueryResultContext context = new QueryResultContext(query);
        Assert.assertNull(context.matchQuery());
        Assert.assertFalse(context.conditionFilterRequired());
        Assert.assertNull(context.resultsFilter());
        Assert.assertFalse(new QueryResultContext(query, true).mustSortByInputIds());
    }
    @Test
    public void testInvisibleCandidatesDoNotScheduleIndexCleanup() throws Exception {
        try (FilterFixture fixture = new FilterFixture()) {
            for (boolean hidden : new boolean[]{true, false}) {
                VertexLabel label = new VertexLabel(fixture.graph, fixture.vertex.schemaLabel().id(),
                                                    hidden ? "~hidden" : "person");
                if (!hidden) {
                    label.status(SchemaStatus.DELETING);
                }
                HugeVertex candidate = new HugeVertex(fixture.graph, fixture.vertex.id(), label);
                candidate.addProperty(fixture.graph.propertyKey("name"), "actual");
                fixture.vertices.update(candidate.id(), candidate);
                ConditionQuery query = fixture.query("absent", OptimizedType.INDEX);
                Assert.assertTrue(fixture.fetch(query).isEmpty());
                Assert.assertTrue(fixture.jobs.isEmpty());

                // Making the same candidate visible must reach residual filtering and cleanup.
                query.showHidden(hidden);
                query.showDeleting(!hidden);
                Assert.assertTrue(fixture.fetch(query).isEmpty());
                fixture.assertCleanup(query);
                fixture.jobs.clear();
            }
        }
    }

    @Test
    public void testResidualMismatchCleansOnlyIndexOptimizedQueries() throws Exception {
        try (FilterFixture fixture = new FilterFixture()) {
            for (OptimizedType type : Arrays.asList(OptimizedType.PRIMARY_KEY,
                                                    OptimizedType.INDEX_FILTER,
                                                    OptimizedType.INDEX)) {
                ConditionQuery query = fixture.query("absent", type);
                Assert.assertTrue(fixture.fetch(query).isEmpty());
                if (type == OptimizedType.INDEX) {
                    fixture.assertCleanup(query);
                } else {
                    Assert.assertTrue(fixture.jobs.isEmpty());
                }
                fixture.jobs.clear();
            }
        }
    }

    @Test
    public void testMatchingCandidateStillSchedulesItsStaleIndexCleanup() throws Exception {
        try (FilterFixture fixture = new FilterFixture()) {
            ConditionQuery query = fixture.query("actual", OptimizedType.INDEX);
            Id name = fixture.graph.propertyKey("name").id();
            query.recordIndexValue(name, fixture.vertex.id(), "actual");
            query.recordIndexValue(name, fixture.vertex.id(), "stale");
            query.selectedIndexField(name);
            List<Vertex> found = fixture.fetch(query);
            Assert.assertEquals(1, found.size());
            Assert.assertEquals(fixture.vertex.id(), found.get(0).id());
            Assert.assertTrue(query.existLeftIndex(fixture.vertex.id()));
            fixture.assertCleanup(query);
        }
    }

    private static final class FilterFixture implements AutoCloseable {

        private final HugeGraph graph;
        private final CachedGraphTransaction transaction;
        private final HugeVertex vertex;
        private final Cache<Id, Object> vertices;
        private final List<EphemeralJob<?>> jobs;

        private FilterFixture() {
            this.graph = HugeFactory.open(FakeObjects.newConfig());
            this.graph.schema().propertyKey("name").asText().create();
            this.graph.schema().vertexLabel("person").useCustomizeNumberId()
                      .properties("name").create();
            this.vertex = (HugeVertex) this.graph.addVertex(T.label, "person", T.id, 1L,
                                                           "name", "actual");
            this.graph.tx().commit();
            HugeGraphParams params = Whitebox.getInternalState(this.graph, "params");
            HugeGraphParams observed = Mockito.spy(params);
            this.jobs = new ArrayList<>();
            Mockito.doAnswer(invocation -> {
                this.jobs.add(invocation.getArgument(0));
                return null;
            }).when(observed).submitEphemeralJob(Mockito.any());
            this.transaction = new CachedGraphTransaction(observed, params.loadGraphStore());
            this.transaction.clearCache(null, false);
            this.vertices = Whitebox.getInternalState(this.transaction, "verticesCache");
        }

        private ConditionQuery query(String value, OptimizedType type) {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            query.query(Condition.eq(this.graph.propertyKey("name").id(), value));
            query.optimized(type);
            return query;
        }

        private List<Vertex> fetch(ConditionQuery query) {
            List<Vertex> found = new ArrayList<>();
            this.transaction.queryVertices(new IdQuery(query, this.vertex.id()))
                            .forEachRemaining(found::add);
            return found;
        }

        private void assertCleanup(ConditionQuery query) {
            Assert.assertEquals(1, this.jobs.size());
            Assert.assertTrue(this.jobs.get(0) instanceof RemoveLeftIndexJob);
            Assert.assertSame(query, Whitebox.getInternalState(this.jobs.get(0), "query"));
            HugeVertex cleaned = Whitebox.getInternalState(this.jobs.get(0), "element");
            Assert.assertEquals(this.vertex.id(), cleaned.id());
        }

        @Override
        public void close() throws Exception {
            try {
                this.transaction.close();
            } finally {
                this.graph.clearBackend();
                this.graph.close();
            }
        }
    }

}
