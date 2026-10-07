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

package org.apache.hugegraph.structure.builder;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.query.MatchedIndex;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.structure.BaseEdge;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.IndexType;
import org.apache.hugegraph.type.define.WriteType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.EdgeLabelType;
import org.junit.Assert;
import org.junit.Test;

public class IndexBuilderTest {

    @Test
    public void testSearchIncludesOriginalTextAndIgnoresReservedSymbols() {
        IndexBuilder builder = new IndexBuilder(supplier(), text -> Set.of("alpha", "beta", "\u0001", "\u0002"));
        Assert.assertEquals(Set.of("alpha", "beta", "alpha beta"), builder.segmentWords("alpha beta"));
    }

    @Test
    public void testExplicitSearchWordsBypassAnalyzer() {
        AtomicInteger calls = new AtomicInteger();
        IndexBuilder builder = new IndexBuilder(supplier(), text -> {
            calls.incrementAndGet();
            return Set.of("analyzed");
        });
        Assert.assertEquals(Set.of("alpha", "beta"), builder.segmentWords("(alpha|beta|alpha)"));
        Assert.assertEquals(Set.of("alpha"), builder.segmentWords("(alpha)"));
        Assert.assertEquals(Set.of(""), builder.segmentWords("()"));
        Assert.assertEquals(0, calls.get());
    }

    @Test
    public void testSearchExpansionForCollectionIncludesJoinedText() {
        HugeGraphSupplier graph = supplier();
        PropertyKey property = property(graph, 1, DataType.TEXT, Cardinality.LIST);
        BaseVertex vertex = vertex(graph, property);
        vertex.addProperty(property, List.of("alpha", "beta"));
        IndexLabel label = index(graph, 1, IndexType.SEARCH, property);
        IndexBuilder builder = new IndexBuilder(graph, text -> Set.of("alpha", "beta"));
        Set<Object> words = new HashSet<>();
        builder.forEachIndex(vertex, label, index -> words.add(index.fieldValues()));
        Assert.assertEquals(Set.of("alpha", "beta", "alpha beta"), words);
        Assert.assertEquals("alpha beta", IndexBuilder.propertyValueToString(List.of("alpha", "beta")));
    }

    @Test
    public void testNullablePrefixAndUniqueNullExpansion() {
        HugeGraphSupplier graph = supplier();
        PropertyKey first = property(graph, 1, DataType.TEXT, Cardinality.SINGLE);
        PropertyKey missing = property(graph, 2, DataType.TEXT, Cardinality.SINGLE);
        PropertyKey last = property(graph, 3, DataType.TEXT, Cardinality.SINGLE);
        BaseVertex vertex = vertex(graph, first, missing, last);
        vertex.schemaLabel().nullableKeys(missing.id());
        vertex.addProperty(first, "first");
        vertex.addProperty(last, "last");
        IndexBuilder builder = new IndexBuilder(graph, text -> Set.of(text));
        IndexLabel secondary = index(graph, 1, IndexType.SECONDARY, first, missing, last);
        List<Index> indexes = builder.buildIndex(vertex, secondary);
        Assert.assertEquals(1, indexes.size());
        Assert.assertEquals("first", indexes.get(0).fieldValues());
        IndexLabel unique = index(graph, 2, IndexType.UNIQUE, first, missing, last);
        Assert.assertEquals("first!\u0001!last", builder.buildIndex(vertex, unique).get(0).fieldValues());
    }

    @Test
    public void testCollectionSecondaryExpansionPreservesDuplicateValuesAndExpiry() {
        HugeGraphSupplier graph = supplier();
        PropertyKey property = property(graph, 1, DataType.INT, Cardinality.LIST);
        BaseVertex vertex = vertex(graph, property);
        vertex.addProperty(property, List.of(7, -1, 7));
        vertex.expiredTime(123L);
        IndexLabel label = index(graph, 1, IndexType.SECONDARY, property);
        List<Index> indexes = new ArrayList<>();
        new IndexBuilder(graph, text -> Set.of(text)).forEachIndex(vertex, label, indexes::add);
        Assert.assertEquals(3, indexes.size());
        Assert.assertEquals(indexes.get(0).fieldValues(), indexes.get(2).fieldValues());
        Assert.assertNotEquals(indexes.get(0).fieldValues(), indexes.get(1).fieldValues());
        for (Index index : indexes) {
            Assert.assertEquals(vertex.id(), index.elementId());
            Assert.assertEquals(123L, index.expiredTime());
        }
    }

    @Test
    public void testStoreRebuildIncludesParentIndexesAndPreservesSelectorOrderAndDuplicates() {
        Map<Id, EdgeLabel> labels = new HashMap<>();
        Map<Id, IndexLabel> indexes = new HashMap<>();
        HugeGraphSupplier graph = supplier(labels, indexes);
        PropertyKey inherited = property(graph, 1, DataType.TEXT, Cardinality.SINGLE);
        PropertyKey own = property(graph, 2, DataType.TEXT, Cardinality.SINGLE);
        EdgeLabel parent = new EdgeLabel(graph, IdGenerator.of(3), "parent");
        parent.enableLabelIndex(false);
        EdgeLabel child = new EdgeLabel(graph, IdGenerator.of(4), "child");
        child.edgeLabelType(EdgeLabelType.SUB);
        child.fatherId(parent.id());
        child.enableLabelIndex(false);
        labels.put(parent.id(), parent);
        IndexLabel parentIndex = index(graph, 11, IndexType.SECONDARY, inherited);
        parentIndex.baseType(HugeType.EDGE_LABEL);
        parentIndex.baseValue(parent.id());
        IndexLabel childIndex = index(graph, 12, IndexType.SECONDARY, own);
        childIndex.baseType(HugeType.EDGE_LABEL);
        childIndex.baseValue(child.id());
        indexes.put(parentIndex.id(), parentIndex);
        indexes.put(childIndex.id(), childIndex);
        parent.indexLabels(parentIndex.id());
        child.indexLabels(childIndex.id());
        BaseEdge edge = new BaseEdge(new EdgeId(IdGenerator.of(1), Directions.OUT, parent.id(), child.id(),
                                                "", IdGenerator.of(2)), child);
        edge.isOutEdge(true);
        edge.addProperty(inherited, "parent value");
        edge.addProperty(own, "child value");
        IndexBuilder builder = new IndexBuilder(graph, text -> Set.of(text));
        List<Index> rebuilt = builder.buildEdgeIndex(edge);
        Assert.assertEquals(2, rebuilt.size());
        Assert.assertEquals(childIndex.id(), rebuilt.get(0).indexLabelId());
        Assert.assertEquals("child value", rebuilt.get(0).fieldValues());
        Assert.assertEquals(parentIndex.id(), rebuilt.get(1).indexLabelId());
        Assert.assertEquals("parent value", rebuilt.get(1).fieldValues());

        child.indexLabels(parentIndex.id());
        List<Id> selected = new ArrayList<>();
        IndexBuilder.forEachIndexLabelId(edge, labels::get, selected::add);
        List<Id> expected = new ArrayList<>(child.indexLabels());
        expected.addAll(parent.indexLabels());
        Assert.assertEquals(expected, selected);
        Assert.assertEquals(2L, selected.stream().filter(parentIndex.id()::equals).count());
        Assert.assertEquals(3, builder.buildEdgeIndex(edge).size());
    }

    @Test
    public void testOlapSelectorUsesProvidedStagedCandidates() {
        HugeGraphSupplier graph = supplier();
        PropertyKey property = property(graph, 1, DataType.INT, Cardinality.SINGLE);
        property.writeType(WriteType.OLAP_COMMON);
        PropertyKey unrelated = property(graph, 2, DataType.INT, Cardinality.SINGLE);
        BaseVertex vertex = new BaseVertex(IdGenerator.of(1), VertexLabel.OLAP_VL);
        vertex.addProperty(property, 42);
        IndexLabel staged = index(graph, 11, IndexType.RANGE_INT, property);
        IndexLabel other = index(graph, 12, IndexType.RANGE_INT, unrelated);
        List<Id> selected = new ArrayList<>();
        // The supplier has no index list; the caller's transaction candidates are authoritative.
        IndexBuilder.forEachOlapIndexLabelId(vertex, List.of(other, staged), selected::add);
        Assert.assertEquals(List.of(staged.id()), selected);
    }

    @Test
    public void testOlapSelectorRejectsZeroOrMultiplePropertiesBeforeCandidateTraversal() {
        HugeGraphSupplier graph = supplier();
        BaseVertex vertex = new BaseVertex(IdGenerator.of(1), VertexLabel.OLAP_VL);
        AtomicInteger traversals = new AtomicInteger();
        Iterable<IndexLabel> candidates = () -> {
            traversals.incrementAndGet();
            return List.<IndexLabel>of().iterator();
        };
        assertInvalidOlap(vertex, candidates);
        vertex.addProperty(property(graph, 1, DataType.INT, Cardinality.SINGLE), 42);
        vertex.addProperty(property(graph, 2, DataType.INT, Cardinality.SINGLE), 43);
        assertInvalidOlap(vertex, candidates);
        Assert.assertEquals(0, traversals.get());
    }

    private static void assertInvalidOlap(BaseVertex vertex, Iterable<IndexLabel> candidates) {
        try {
            IndexBuilder.forEachOlapIndexLabelId(vertex, candidates, id -> Assert.fail("Unexpected selected index"));
            Assert.fail("Olap selector must require exactly one property");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("Expect only 1 property"));
        }
    }

    @Test
    public void testMatchedIndexRetainsEngineEqualityAndReadOnlyLabels() {
        HugeGraphSupplier graph = supplier();
        VertexLabel first = new VertexLabel(graph, IdGenerator.of(1), "first");
        VertexLabel second = new VertexLabel(graph, IdGenerator.of(2), "second");
        PropertyKey property = property(graph, 1, DataType.TEXT, Cardinality.SINGLE);
        IndexLabel search = index(graph, 1, IndexType.SEARCH, property);
        MatchedIndex match = new MatchedIndex(first, Set.of(search));
        MatchedIndex equal = new MatchedIndex(second, Set.of(search));
        Assert.assertEquals(match, equal);
        Assert.assertEquals(match.hashCode(), equal.hashCode());
        Assert.assertTrue(match.containsSearchIndex());
        try {
            match.indexLabels().clear();
            Assert.fail("Matched labels must remain read-only");
        } catch (UnsupportedOperationException expected) {
            Assert.assertEquals(Set.of(search), match.indexLabels());
        }
    }

    private static HugeGraphSupplier supplier() {
        return supplier(Map.of(), Map.of());
    }

    private static HugeGraphSupplier supplier(Map<Id, EdgeLabel> labels, Map<Id, IndexLabel> indexes) {
        return (HugeGraphSupplier) Proxy.newProxyInstance(HugeGraphSupplier.class.getClassLoader(),
                new Class<?>[]{HugeGraphSupplier.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "name":
                        case "toString":
                            return "index-builder-test";
                        case "now":
                            return 0L;
                        case "edgeLabel":
                            return labels.get(args[0]);
                        case "indexLabel":
                            return indexes.get(args[0]);
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            return null;
                    }
                });
    }

    private static PropertyKey property(HugeGraphSupplier graph, int value, DataType type, Cardinality cardinality) {
        PropertyKey property = new PropertyKey(graph, IdGenerator.of(value), "property" + value);
        property.dataType(type);
        property.cardinality(cardinality);
        return property;
    }

    private static BaseVertex vertex(HugeGraphSupplier graph, PropertyKey... properties) {
        VertexLabel label = new VertexLabel(graph, IdGenerator.of(1), "vertex");
        for (PropertyKey property : properties) {
            label.properties(Set.of(property.id()));
        }
        return new BaseVertex(IdGenerator.of(1), label);
    }

    private static IndexLabel index(HugeGraphSupplier graph, int value, IndexType type, PropertyKey... fields) {
        IndexLabel label = new IndexLabel(graph, IdGenerator.of(value), "index" + value);
        label.baseType(HugeType.VERTEX_LABEL);
        label.baseValue(IdGenerator.of(1));
        label.indexType(type);
        for (PropertyKey property : fields) {
            label.indexField(property.id());
        }
        return label;
    }
}
