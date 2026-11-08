/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.unit.core;

import java.util.Collections;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.builder.EdgeLabelBuilder;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.type.define.EdgeLabelType;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.util.LockUtil;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class GraphSchemaCloneTest {

    private static final String PARENT_COPY_GRAPH = "DEFAULT-parent-copy";

    @Test
    public void testEdgeLabelsAreRejectedBeforeTargetCreation() {
        GraphManager manager = Mockito.mock(GraphManager.class, Mockito.CALLS_REAL_METHODS);
        HugeGraph source = Mockito.mock(HugeGraph.class);
        SchemaManager sourceSchema = Mockito.mock(SchemaManager.class);
        Mockito.doReturn(source).when(manager).graph("DEFAULT-source");
        Mockito.when(source.schema()).thenReturn(sourceSchema);
        Mockito.when(sourceSchema.getEdgeLabels())
               .thenReturn(ImmutableList.of(Mockito.mock(EdgeLabel.class)));
        IllegalArgumentException failure = Assert.assertThrows(IllegalArgumentException.class,
                () -> manager.cloneGraphSchema("DEFAULT", "source", "target", "copy", "admin"));
        Assert.assertTrue(failure.getMessage().contains("edge labels"));
        Mockito.verify(manager, Mockito.never()).cloneGraph(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyMap());
    }

    @Test
    public void testEdgeAddedAfterPreflightIsNotCopied() {
        GraphManager manager = Mockito.mock(GraphManager.class, Mockito.CALLS_REAL_METHODS);
        HugeGraph source = Mockito.mock(HugeGraph.class);
        HugeGraph target = Mockito.mock(HugeGraph.class);
        SchemaManager sourceSchema = Mockito.mock(SchemaManager.class);
        SchemaManager targetSchema = new SchemaManager(Mockito.mock(ISchemaTransaction.class), target);
        Mockito.doReturn(source).when(manager).graph("DEFAULT-source");
        Mockito.doReturn(target).when(manager).cloneGraph(Mockito.eq("DEFAULT"),
                          Mockito.eq("source"), Mockito.eq("target"), Mockito.anyMap());
        Mockito.when(source.schema()).thenReturn(sourceSchema);
        Mockito.when(target.schema()).thenReturn(targetSchema);
        Mockito.when(sourceSchema.getEdgeLabels()).thenReturn(ImmutableList.of(),
                                     ImmutableList.of(Mockito.mock(EdgeLabel.class)));

        HugeException failure = Assert.assertThrows(HugeException.class,
                () -> manager.cloneGraphSchema("DEFAULT", "source", "target", "copy", "admin"));
        Assert.assertTrue(failure.getMessage().contains("partially created"));
        Mockito.verify(sourceSchema, Mockito.never()).getPropertyKeys();
    }

    @Test
    public void testEdgeAddedDuringCopyDoesNotReturnSuccess() {
        GraphManager manager = Mockito.mock(GraphManager.class, Mockito.CALLS_REAL_METHODS);
        HugeGraph source = Mockito.mock(HugeGraph.class);
        HugeGraph target = Mockito.mock(HugeGraph.class);
        SchemaManager sourceSchema = Mockito.mock(SchemaManager.class);
        SchemaManager targetSchema = new SchemaManager(Mockito.mock(ISchemaTransaction.class), target);
        Mockito.doReturn(source).when(manager).graph("DEFAULT-source");
        Mockito.doReturn(target).when(manager).cloneGraph(Mockito.eq("DEFAULT"),
                          Mockito.eq("source"), Mockito.eq("target"), Mockito.anyMap());
        Mockito.when(source.schema()).thenReturn(sourceSchema);
        Mockito.when(target.schema()).thenReturn(targetSchema);
        Mockito.when(sourceSchema.getEdgeLabels()).thenReturn(ImmutableList.of(), ImmutableList.of(),
                                     ImmutableList.of(Mockito.mock(EdgeLabel.class)));

        HugeException failure = Assert.assertThrows(HugeException.class,
                () -> manager.cloneGraphSchema("DEFAULT", "source", "target", "copy", "admin"));
        Assert.assertTrue(failure.getMessage().contains("partially created"));
    }

    @Test
    public void testCopyFailureReportsPartialTargetWithoutDeletingIt() {
        GraphManager manager = Mockito.mock(GraphManager.class, Mockito.CALLS_REAL_METHODS);
        HugeGraph source = Mockito.mock(HugeGraph.class);
        HugeGraph target = Mockito.mock(HugeGraph.class);
        SchemaManager sourceSchema = Mockito.mock(SchemaManager.class);
        SchemaManager targetSchema = Mockito.mock(SchemaManager.class);
        Mockito.doReturn(source).when(manager).graph("DEFAULT-source");
        Mockito.doReturn(target).when(manager).cloneGraph(Mockito.eq("DEFAULT"),
                          Mockito.eq("source"), Mockito.eq("target"), Mockito.anyMap());
        Mockito.when(source.schema()).thenReturn(sourceSchema);
        Mockito.when(target.schema()).thenReturn(targetSchema);
        Mockito.doThrow(new IllegalStateException("copy failed"))
               .when(targetSchema).copyFromWithoutEdges(sourceSchema);

        HugeException failure = Assert.assertThrows(HugeException.class,
                () -> manager.cloneGraphSchema("DEFAULT", "source", "target", "copy", "admin"));
        Assert.assertTrue(failure.getMessage().contains("partially created"));
        Mockito.verify(manager, Mockito.never()).dropGraph(Mockito.anyString(),
                                                          Mockito.anyString(), Mockito.anyBoolean());
    }

    @Test
    public void testFailedIndexCannotProduceSuccessfulClone() {
        GraphManager manager = Mockito.mock(GraphManager.class, Mockito.CALLS_REAL_METHODS);
        HugeGraph source = Mockito.mock(HugeGraph.class);
        HugeGraph target = Mockito.mock(HugeGraph.class);
        SchemaManager targetSchema = Mockito.mock(SchemaManager.class);
        IndexLabel index = Mockito.mock(IndexLabel.class);
        Mockito.doReturn(source).when(manager).graph("DEFAULT-source");
        Mockito.doReturn(target).when(manager).cloneGraph(Mockito.eq("DEFAULT"),
                          Mockito.eq("source"), Mockito.eq("target"), Mockito.anyMap());
        Mockito.when(source.schema()).thenReturn(Mockito.mock(SchemaManager.class));
        Mockito.when(target.schema()).thenReturn(targetSchema);
        Mockito.when(targetSchema.getIndexLabels()).thenReturn(ImmutableList.of(index));
        Mockito.when(index.status()).thenReturn(SchemaStatus.INVALID);

        HugeException failure = Assert.assertThrows(HugeException.class,
                () -> manager.cloneGraphSchema("DEFAULT", "source", "target", "copy", "admin"));
        Assert.assertTrue(failure.getMessage().contains("partially created"));
        Assert.assertNull(failure.getCause());
    }

    @Test
    public void testSingleParentCreateIgnoresDerivedFields() {
        ISchemaTransaction transaction = Mockito.mock(ISchemaTransaction.class);
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.spaceGraphName()).thenReturn(PARENT_COPY_GRAPH);

        Id sourceId = IdGenerator.of(1L);
        Id targetId = IdGenerator.of(2L);
        Id sortKeyId = IdGenerator.of(3L);
        VertexLabel source = Mockito.mock(VertexLabel.class);
        VertexLabel target = Mockito.mock(VertexLabel.class);
        Mockito.when(source.name()).thenReturn("person");
        Mockito.when(target.name()).thenReturn("software");
        Mockito.when(graph.vertexLabel(sourceId)).thenReturn(source);
        Mockito.when(graph.vertexLabel(targetId)).thenReturn(target);

        EdgeLabel existed = Mockito.mock(EdgeLabel.class);
        Mockito.when(existed.edgeLabelType()).thenReturn(EdgeLabelType.PARENT);
        Mockito.when(existed.links())
               .thenReturn(Collections.singleton(Pair.of(sourceId, targetId)));
        Mockito.when(existed.frequency()).thenReturn(Frequency.SINGLE);
        Mockito.when(existed.enableLabelIndex()).thenReturn(true);
        Mockito.when(existed.properties()).thenReturn(Collections.emptySet());
        Mockito.when(existed.sortKeys())
               .thenReturn(Collections.singletonList(sortKeyId));
        Mockito.when(existed.nullableKeys()).thenReturn(Collections.emptySet());
        Mockito.when(transaction.getEdgeLabel("singleParent")).thenReturn(existed);

        EdgeLabelBuilder builder = new EdgeLabelBuilder(transaction, graph,
                                                        "singleParent");
        builder.asBase();
        builder.singleTime();
        builder.ifNotExist();

        Assert.assertSame(existed, createWithLocks(builder));
    }

    @Test
    public void testMultiParentCreateIgnoresOnlyDerivedLinks() {
        ISchemaTransaction transaction = Mockito.mock(ISchemaTransaction.class);
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.spaceGraphName()).thenReturn(PARENT_COPY_GRAPH);

        Id sourceId = IdGenerator.of(1L);
        Id targetId = IdGenerator.of(2L);
        Id sortKeyId = IdGenerator.of(3L);
        VertexLabel source = Mockito.mock(VertexLabel.class);
        VertexLabel target = Mockito.mock(VertexLabel.class);
        PropertyKey sortKey = Mockito.mock(PropertyKey.class);
        Mockito.when(source.name()).thenReturn("person");
        Mockito.when(target.name()).thenReturn("software");
        Mockito.when(sortKey.id()).thenReturn(sortKeyId);
        Mockito.when(graph.vertexLabel(sourceId)).thenReturn(source);
        Mockito.when(graph.vertexLabel(targetId)).thenReturn(target);
        Mockito.when(graph.propertyKey("time")).thenReturn(sortKey);

        EdgeLabel existed = Mockito.mock(EdgeLabel.class);
        Mockito.when(existed.edgeLabelType()).thenReturn(EdgeLabelType.PARENT);
        Mockito.when(existed.links())
               .thenReturn(Collections.singleton(Pair.of(sourceId, targetId)));
        Mockito.when(existed.frequency()).thenReturn(Frequency.MULTIPLE);
        Mockito.when(existed.enableLabelIndex()).thenReturn(true);
        Mockito.when(existed.properties())
               .thenReturn(Collections.singleton(sortKeyId));
        Mockito.when(existed.sortKeys())
               .thenReturn(Collections.singletonList(sortKeyId));
        Mockito.when(existed.nullableKeys()).thenReturn(Collections.emptySet());
        Mockito.when(transaction.getEdgeLabel("multiParent")).thenReturn(existed);

        EdgeLabelBuilder builder = new EdgeLabelBuilder(transaction, graph,
                                                        "multiParent");
        builder.asBase();
        builder.multiTimes();
        builder.properties("time");
        builder.sortKeys("time");
        builder.ifNotExist();

        Assert.assertSame(existed, createWithLocks(builder));
    }

    private static EdgeLabel createWithLocks(EdgeLabelBuilder builder) {
        LockUtil.init(PARENT_COPY_GRAPH);
        try {
            return builder.create();
        } finally {
            LockUtil.destroy(PARENT_COPY_GRAPH);
        }
    }
}
