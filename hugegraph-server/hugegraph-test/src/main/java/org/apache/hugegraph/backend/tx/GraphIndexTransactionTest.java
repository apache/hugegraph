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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.analyzer.Analyzer;
import org.apache.hugegraph.backend.serializer.AbstractSerializer;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.structure.builder.IndexBuilder;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.type.define.IndexType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.EdgeLabelType;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.type.define.WriteType;
import org.junit.Test;
import org.mockito.Mockito;

public class GraphIndexTransactionTest {

    @Test
    public void testKeepBackendIndexOrderOnlyForOrderedHstoreRangeQuery() {
        ConditionQuery query = new ConditionQuery(HugeType.RANGE_INT_INDEX);

        Assert.assertFalse(GraphIndexTransaction.keepBackendIndexOrder(
                true, IndexType.RANGE_INT, query));

        query.limit(10L);
        Assert.assertTrue(GraphIndexTransaction.keepBackendIndexOrder(
                true, IndexType.RANGE_INT, query));
        Assert.assertFalse(GraphIndexTransaction.keepBackendIndexOrder(
                false, IndexType.RANGE_INT, query));
        Assert.assertFalse(GraphIndexTransaction.keepBackendIndexOrder(
                true, IndexType.SECONDARY, query));

        query.limit(Query.NO_LIMIT);
        query.page("");
        Assert.assertTrue(GraphIndexTransaction.keepBackendIndexOrder(
                true, IndexType.RANGE_INT, query));
    }

    @Test
    public void testEngineTransactionAndStoreBuilderShareExpansionForEveryIndexKind() {
        FakeObjects objects = new FakeObjects();
        HugeGraph graph = objects.graph();
        PropertyKey text = objects.newPropertyKey(IdGenerator.of(1), "text", DataType.TEXT);
        PropertyKey number = objects.newPropertyKey(IdGenerator.of(2), "number", DataType.INT);
        VertexLabel vertexLabel = objects.newVertexLabel(IdGenerator.of(4), "vertex",
                                                         IdStrategy.CUSTOMIZE_NUMBER, text.id(), number.id());
        HugeVertex vertex = new HugeVertex(graph, IdGenerator.of(1), vertexLabel);
        vertex.addProperty(text, "alpha beta");
        vertex.addProperty(number, 7);
        vertex.expiredTime(123L);
        Analyzer analyzer = value -> Set.of("alpha", "beta");
        int value = 10;
        for (IndexType type : List.of(IndexType.SECONDARY, IndexType.SEARCH, IndexType.SHARD,
                                       IndexType.UNIQUE, IndexType.RANGE_INT, IndexType.RANGE_FLOAT,
                                       IndexType.RANGE_LONG, IndexType.RANGE_DOUBLE)) {
            PropertyKey field = type.isRange() ? number : text;
            IndexLabel label = objects.newIndexLabel(IdGenerator.of(value++), type.name(),
                                                     HugeType.VERTEX_LABEL, vertexLabel.id(), type, field.id());
            List<Index> expected = new IndexBuilder(graph, analyzer).buildIndex(vertex.element(), label);
            List<Index> actual = captureEngineExpansion(objects, vertex, label, analyzer);
            Assert.assertEquals(type.name(), expected.size(), actual.size());
            for (int i = 0; i < expected.size(); i++) {
                Assert.assertEquals(type.name(), expected.get(i).fieldValues(), actual.get(i).fieldValues());
                Assert.assertEquals(vertex.id(), actual.get(i).elementId());
                Assert.assertEquals(123L, actual.get(i).expiredTime());
            }
            if (type == IndexType.SEARCH) {
                Set<Object> values = new HashSet<>();
                actual.forEach(index -> values.add(index.fieldValues()));
                Assert.assertEquals(Set.of("alpha", "beta", "alpha beta"), values);
            }
        }
    }

    @Test
    public void testEngineNullableCompositePrefixAndCollectionExpansionRemainUnloaded() {
        FakeObjects objects = new FakeObjects();
        PropertyKey first = objects.newPropertyKey(IdGenerator.of(1), "first", DataType.TEXT);
        PropertyKey missing = objects.newPropertyKey(IdGenerator.of(2), "missing", DataType.TEXT);
        PropertyKey last = objects.newPropertyKey(IdGenerator.of(3), "last", DataType.TEXT);
        VertexLabel label = objects.newVertexLabel(IdGenerator.of(4), "vertex", IdStrategy.CUSTOMIZE_NUMBER,
                                                   first.id(), missing.id(), last.id());
        label.nullableKeys(missing.id());
        HugeVertex vertex = new HugeVertex(objects.graph(), IdGenerator.of(1), label);
        vertex.addProperty(first, "first");
        vertex.addProperty(last, "last");
        vertex.propNotLoaded();
        IndexLabel index = objects.newIndexLabel(IdGenerator.of(5), "composite", HugeType.VERTEX_LABEL,
                                                 label.id(), IndexType.SECONDARY,
                                                 first.id(), missing.id(), last.id());
        List<Index> actual = captureEngineExpansion(objects, vertex, index, text -> Set.of(text));
        Assert.assertEquals(1, actual.size());
        Assert.assertEquals("first", actual.get(0).fieldValues());
        Assert.assertFalse(vertex.isPropLoaded());

        PropertyKey list = objects.newPropertyKey(IdGenerator.of(6), "list", DataType.INT, Cardinality.LIST);
        label.properties(list.id());
        vertex.addProperty(list, List.of(7, -1, 7));
        IndexLabel collection = objects.newIndexLabel(IdGenerator.of(7), "collection", HugeType.VERTEX_LABEL,
                                                      label.id(), IndexType.SECONDARY, list.id());
        actual = captureEngineExpansion(objects, vertex, collection, text -> Set.of(text));
        Assert.assertEquals(3, actual.size());
        Assert.assertEquals(actual.get(0).fieldValues(), actual.get(2).fieldValues());
    }

    @Test
    public void testEngineAndStoreSelectInheritedEdgeIndexesThroughSharedHelper() {
        FakeObjects objects = new FakeObjects();
        PropertyKey inherited = objects.newPropertyKey(IdGenerator.of(1), "inherited", DataType.TEXT);
        PropertyKey own = objects.newPropertyKey(IdGenerator.of(2), "own", DataType.TEXT);
        EdgeLabel parent = objects.newEdgeLabel(IdGenerator.of(3), "parent", Frequency.MULTIPLE,
                                                IdGenerator.of(5), IdGenerator.of(5), inherited.id());
        EdgeLabel child = objects.newEdgeLabel(IdGenerator.of(4), "child", Frequency.MULTIPLE,
                                               IdGenerator.of(5), IdGenerator.of(5), inherited.id(), own.id());
        child.edgeLabelType(EdgeLabelType.SUB);
        child.fatherId(parent.id());
        parent.enableLabelIndex(false);
        child.enableLabelIndex(false);
        IndexLabel parentIndex = objects.newIndexLabel(IdGenerator.of(11), "parentIndex", HugeType.EDGE_LABEL,
                                                       parent.id(), IndexType.SECONDARY, inherited.id());
        IndexLabel childIndex = objects.newIndexLabel(IdGenerator.of(12), "childIndex", HugeType.EDGE_LABEL,
                                                      child.id(), IndexType.SECONDARY, own.id());
        parent.indexLabels(parentIndex.id());
        child.indexLabels(childIndex.id());
        HugeEdge edge = new HugeEdge(objects.graph(), new EdgeId(IdGenerator.of(1), Directions.OUT,
                parent.id(), child.id(), "", IdGenerator.of(2)), child);
        edge.addProperty(inherited, "parent value");
        edge.addProperty(own, "child value");
        Analyzer analyzer = text -> Set.of(text);
        List<Index> store = new IndexBuilder(objects.graph(), analyzer).buildEdgeIndex(edge.element());
        List<Index> engine = captureEngineIndexes(objects, List.of(parentIndex, childIndex), analyzer,
                                                  tx -> tx.updateEdgeIndex(edge, true));
        Assert.assertEquals(2, engine.size());
        Assert.assertEquals(childIndex.id(), engine.get(0).indexLabelId());
        Assert.assertEquals(parentIndex.id(), engine.get(1).indexLabelId());
        for (int i = 0; i < engine.size(); i++) {
            Assert.assertEquals(store.get(i).indexLabelId(), engine.get(i).indexLabelId());
            Assert.assertEquals(store.get(i).fieldValues(), engine.get(i).fieldValues());
        }
    }

    @Test
    public void testEngineOlapAdapterSelectsTransactionCandidatesAndPreservesSingletonGuard() {
        FakeObjects objects = new FakeObjects();
        PropertyKey property = objects.newPropertyKey(IdGenerator.of(1), "olap", DataType.INT);
        property.writeType(WriteType.OLAP_COMMON);
        PropertyKey unrelated = objects.newPropertyKey(IdGenerator.of(2), "other", DataType.INT);
        HugeVertex vertex = new HugeVertex(objects.graph(), IdGenerator.of(1), VertexLabel.OLAP_VL);
        vertex.addProperty(property, 42);
        IndexLabel selected = objects.newIndexLabel(IdGenerator.of(11), "selected", HugeType.VERTEX_LABEL,
                                                    VertexLabel.OLAP_VL.id(), IndexType.RANGE_INT, property.id());
        IndexLabel other = objects.newIndexLabel(IdGenerator.of(12), "other", HugeType.VERTEX_LABEL,
                                                 VertexLabel.OLAP_VL.id(), IndexType.RANGE_INT, unrelated.id());
        List<Index> actual = captureEngineIndexes(objects, List.of(other, selected), text -> Set.of(text),
                                                  tx -> tx.updateVertexIndex(vertex, true));
        Assert.assertEquals(1, actual.size());
        Assert.assertEquals(selected.id(), actual.get(0).indexLabelId());
        Assert.assertEquals(42, actual.get(0).fieldValues());
        for (HugeVertex malformed : List.of(new HugeVertex(objects.graph(), IdGenerator.of(2), VertexLabel.OLAP_VL),
                                            new HugeVertex(objects.graph(), IdGenerator.of(3), VertexLabel.OLAP_VL))) {
            if (malformed.id().equals(IdGenerator.of(3))) {
                malformed.addProperty(property, 42);
                malformed.addProperty(unrelated, 43);
            }
            try {
                captureEngineIndexes(objects, List.of(selected), text -> Set.of(text),
                                     tx -> tx.updateVertexIndex(malformed, true));
                Assert.fail("Malformed olap vertex must fail the shared singleton guard");
            } catch (IllegalArgumentException expected) {
                Assert.assertTrue(expected.getMessage().contains("Expect only 1 property"));
            }
        }
    }

    private static List<Index> captureEngineExpansion(FakeObjects objects, HugeVertex vertex,
                                                       IndexLabel label, Analyzer analyzer) {
        return captureEngineIndexes(objects, List.of(label), analyzer,
                                    tx -> tx.updateIndex(label.id(), vertex, true));
    }

    private static List<Index> captureEngineIndexes(FakeObjects objects, List<IndexLabel> labels,
                                                    Analyzer analyzer, Consumer<GraphIndexTransaction> action) {
        HugeGraphParams params = Mockito.mock(HugeGraphParams.class);
        ISchemaTransaction schema = Mockito.mock(ISchemaTransaction.class);
        AbstractSerializer serializer = Mockito.mock(AbstractSerializer.class);
        BackendStore store = Mockito.mock(BackendStore.class);
        Mockito.when(params.graph()).thenReturn(objects.graph());
        Mockito.doReturn(objects.graph().configuration()).when(params).configuration();
        Mockito.when(params.serializer()).thenReturn(serializer);
        Mockito.when(params.schemaTransaction()).thenReturn(schema);
        Mockito.when(schema.getIndexLabels()).thenReturn(labels);
        Mockito.when(params.analyzer()).thenReturn(analyzer);
        for (IndexLabel label : labels) {
            Mockito.when(schema.getIndexLabel(label.id())).thenReturn(label);
        }
        List<Index> indexes = new ArrayList<>();
        Mockito.when(serializer.writeIndex(Mockito.any(Index.class))).thenAnswer(call -> {
            indexes.add(call.getArgument(0));
            return Mockito.mock(BackendEntry.class);
        });
        GraphIndexTransaction tx = new GraphIndexTransaction(params, store) {
            @Override
            public void doEliminate(BackendEntry entry) {
                // Capture pure expansion without touching a real backend.
            }
        };
        action.accept(tx);
        return indexes;
    }

}
