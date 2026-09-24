/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeIndex;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class TaskTransaction extends GraphTransaction {

    public static final String TASK = HugeTask.P.TASK;

    public TaskTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);
        this.autoCommit(true);
    }

    public HugeVertex constructVertex(HugeTask<?> task) {
        if (!this.graph().existsVertexLabel(TASK)) {
            throw new HugeException("Schema is missing for task(%s) '%s'",
                                    task.id(), task.name());
        }
        return this.constructVertex(false, task.asArray());
    }

    public void deleteIndex(HugeVertex vertex) {
        // Delete the old record if exist
        Iterator<Vertex> old = this.queryTaskInfos(vertex.id());
        HugeVertex oldV = (HugeVertex) QueryResults.one(old);
        if (oldV == null) {
            return;
        }
        this.deleteIndexIfNeeded(oldV, vertex);
    }

    private boolean deleteIndexIfNeeded(HugeVertex oldV, HugeVertex newV) {
        if (!oldV.value(HugeTask.P.STATUS).equals(newV.value(HugeTask.P.STATUS))) {
            // Only delete vertex if index value changed else override it
            this.updateIndex(this.indexLabel(HugeTask.P.STATUS).id(), oldV, true);
            return true;
        }
        return false;
    }

    public void removeTaskVertex(HugeVertex vertex) {
        this.checkOwnerThread();

        this.beforeWrite();

        this.doRemove(this.serializer.writeVertex(vertex.prepareRemoved()));
        if (TASK.equals(vertex.schemaLabel().name())) {
            this.updateIndex(this.indexLabel(HugeTask.P.STATUS).id(),
                             vertex, true);
        }
        this.removeLabelIndex(vertex);

        this.afterWrite();
    }

    private void removeLabelIndex(HugeVertex vertex) {
        if (this.store().features().supportsQueryByLabel() ||
            !vertex.schemaLabel().enableLabelIndex()) {
            return;
        }

        HugeIndex index = new HugeIndex(this.graph(),
                                        IndexLabel.label(vertex.type()));
        index.fieldValues(vertex.schemaLabel().id());
        index.elementIds(vertex.id(), vertex.expiredTime());
        this.doEliminate(this.serializer.writeIndex(index));
    }

    public void initSchema() {
        if (this.existVertexLabel(TASK)) {
            return;
        }

        HugeGraph graph = this.graph();
        String[] properties = this.initProperties();

        // Create vertex label '~task'
        VertexLabel label = graph.schema().vertexLabel(TASK)
                                 .properties(properties)
                                 .useCustomizeNumberId()
                                 .nullableKeys(HugeTask.P.DESCRIPTION, HugeTask.P.CONTEXT,
                                               HugeTask.P.UPDATE, HugeTask.P.INPUT,
                                               HugeTask.P.RESULT,
                                               HugeTask.P.DEPENDENCIES, HugeTask.P.SERVER)
                                 .enableLabelIndex(true)
                                 .build();
        this.params().schemaTransaction().addVertexLabel(label);

        // Create index
        this.createIndexLabel(label, HugeTask.P.STATUS);
    }

    private boolean existVertexLabel(String label) {
        return this.params().schemaTransaction()
                   .getVertexLabel(label) != null;
    }

    private String[] initProperties() {
        List<String> props = new ArrayList<>();

        props.add(createPropertyKey(HugeTask.P.TYPE));
        props.add(createPropertyKey(HugeTask.P.NAME));
        props.add(createPropertyKey(HugeTask.P.CALLABLE));
        props.add(createPropertyKey(HugeTask.P.DESCRIPTION));
        props.add(createPropertyKey(HugeTask.P.CONTEXT));
        props.add(createPropertyKey(HugeTask.P.STATUS, DataType.BYTE));
        props.add(createPropertyKey(HugeTask.P.PROGRESS, DataType.INT));
        props.add(createPropertyKey(HugeTask.P.CREATE, DataType.DATE));
        props.add(createPropertyKey(HugeTask.P.UPDATE, DataType.DATE));
        props.add(createPropertyKey(HugeTask.P.RETRIES, DataType.INT));
        props.add(createPropertyKey(HugeTask.P.INPUT, DataType.BLOB));
        props.add(createPropertyKey(HugeTask.P.RESULT, DataType.BLOB));
        props.add(createPropertyKey(HugeTask.P.DEPENDENCIES, DataType.LONG,
                                    Cardinality.SET));
        props.add(createPropertyKey(HugeTask.P.SERVER));

        return props.toArray(new String[0]);
    }

    private String createPropertyKey(String name) {
        return this.createPropertyKey(name, DataType.TEXT);
    }

    private String createPropertyKey(String name, DataType dataType) {
        return this.createPropertyKey(name, dataType, Cardinality.SINGLE);
    }

    private String createPropertyKey(String name, DataType dataType,
                                     Cardinality cardinality) {
        HugeGraph graph = this.graph();
        SchemaManager schema = graph.schema();
        PropertyKey propertyKey = schema.propertyKey(name)
                                        .dataType(dataType)
                                        .cardinality(cardinality)
                                        .build();
        this.params().schemaTransaction().addPropertyKey(propertyKey);
        return name;
    }

    private IndexLabel createIndexLabel(VertexLabel label, String field) {
        HugeGraph graph = this.graph();
        SchemaManager schema = graph.schema();
        String name = Graph.Hidden.hide("task-index-by-" + field);
        IndexLabel indexLabel = schema.indexLabel(name)
                                      .on(HugeType.VERTEX_LABEL, TASK)
                                      .by(field)
                                      .build();
        this.params().schemaTransaction().addIndexLabel(label, indexLabel);
        return indexLabel;
    }

    private IndexLabel indexLabel(String field) {
        String name = Graph.Hidden.hide("task-index-by-" + field);
        HugeGraph graph = this.graph();
        return graph.indexLabel(name);
    }
}
