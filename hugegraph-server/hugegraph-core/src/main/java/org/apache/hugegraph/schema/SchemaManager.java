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

package org.apache.hugegraph.schema;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.schema.builder.EdgeLabelBuilder;
import org.apache.hugegraph.schema.builder.IndexLabelBuilder;
import org.apache.hugegraph.schema.builder.PropertyKeyBuilder;
import org.apache.hugegraph.schema.builder.VertexLabelBuilder;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.SchemaElement;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class SchemaManager {

    private final ISchemaTransaction transaction;
    private HugeGraph graph;

    public SchemaManager(ISchemaTransaction transaction, HugeGraph graph) {
        E.checkNotNull(transaction, "transaction");
        E.checkNotNull(graph, "graph");
        this.transaction = transaction;
        this.graph = graph;
    }

    public HugeGraph proxy(HugeGraph graph) {
        E.checkNotNull(graph, "graph");
        HugeGraph old = this.graph;
        this.graph = graph;
        return old;
    }

    public PropertyKey.Builder propertyKey(String name) {
        return new PropertyKeyBuilder(this.transaction, this.graph, name);
    }

    public VertexLabel.Builder vertexLabel(String name) {
        return new VertexLabelBuilder(this.transaction, this.graph, name);
    }

    public EdgeLabel.Builder edgeLabel(String name) {
        return new EdgeLabelBuilder(this.transaction, this.graph, name);
    }

    public IndexLabel.Builder indexLabel(String name) {
        return new IndexLabelBuilder(this.transaction, this.graph, name);
    }

    public PropertyKey getPropertyKey(String name) {
        E.checkArgumentNotNull(name, "The name parameter can't be null");
        PropertyKey propertyKey = this.transaction.getPropertyKey(name);
        checkExists(HugeType.PROPERTY_KEY, propertyKey, name);
        return propertyKey;
    }

    public VertexLabel getVertexLabel(String name) {
        E.checkArgumentNotNull(name, "The name parameter can't be null");
        VertexLabel vertexLabel = this.transaction.getVertexLabel(name);
        checkExists(HugeType.VERTEX_LABEL, vertexLabel, name);
        return vertexLabel;
    }

    public EdgeLabel getEdgeLabel(String name) {
        E.checkArgumentNotNull(name, "The name parameter can't be null");
        EdgeLabel edgeLabel = this.transaction.getEdgeLabel(name);
        checkExists(HugeType.EDGE_LABEL, edgeLabel, name);
        return edgeLabel;
    }

    public IndexLabel getIndexLabel(String name) {
        E.checkArgumentNotNull(name, "The name parameter can't be null");
        IndexLabel indexLabel = this.transaction.getIndexLabel(name);
        checkExists(HugeType.INDEX_LABEL, indexLabel, name);
        return indexLabel;
    }

    public List<PropertyKey> getPropertyKeys() {
        return this.graph.propertyKeys().stream()
                         .filter(pk -> !Graph.Hidden.isHidden(pk.name()))
                         .collect(Collectors.toList());
    }

    public List<VertexLabel> getVertexLabels() {
        return this.graph.vertexLabels().stream()
                         .filter(vl -> !Graph.Hidden.isHidden(vl.name()))
                         .collect(Collectors.toList());
    }

    public List<EdgeLabel> getEdgeLabels() {
        return this.graph.edgeLabels().stream()
                         .filter(el -> !Graph.Hidden.isHidden(el.name()))
                         .collect(Collectors.toList());
    }

    public List<IndexLabel> getIndexLabels() {
        return this.graph.indexLabels().stream()
                         .filter(il -> !Graph.Hidden.isHidden(il.name()))
                         .collect(Collectors.toList());
    }

    public void copyFrom(SchemaManager schema) {
        for (PropertyKey pk : schema.getPropertyKeys()) {
            new PropertyKeyBuilder(this.transaction, this.graph, pk).create();
        }
        for (VertexLabel vl : schema.getVertexLabels()) {
            new VertexLabelBuilder(this.transaction, this.graph, vl).create();
        }
        for (EdgeLabel el : schema.getEdgeLabels()) {
            new EdgeLabelBuilder(this.transaction, this.graph, el).create();
        }
        for (IndexLabel il : schema.getIndexLabels()) {
            new IndexLabelBuilder(this.transaction, this.graph, il).create();
        }
    }

    private static void checkExists(HugeType type, Object object, String name) {
        if (object == null) {
            throw new NotFoundException("%s with name '%s' does not exist",
                                        type.readableName(), name);
        }
    }
}
