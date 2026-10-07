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
import java.util.List;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;

public class TaskAndResultTransaction extends TaskTransaction {

    public static final String TASKRESULT = HugeTaskResult.P.TASKRESULT;

    /**
     * Task transactions, for persistence
     */
    protected volatile TaskAndResultTransaction taskTx = null;

    public TaskAndResultTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);
        this.autoCommit(true);
    }

    public HugeVertex constructTaskVertex(HugeTask<?> task) {
        if (!this.graph().existsVertexLabel(TASK)) {
            throw new HugeException("Schema is missing for task(%s) '%s'",
                                    task.id(), task.name());
        }

        return this.constructVertex(false, task.asArrayWithoutResult());
    }

    public HugeVertex constructTaskResultVertex(HugeTaskResult taskResult) {
        if (!this.graph().existsVertexLabel(TASKRESULT)) {
            throw new HugeException("Schema is missing for task result");
        }

        return this.constructVertex(false, taskResult.asArray());
    }

    @Override
    public void initSchema() {
        super.initSchema();

        if (this.graph().existsVertexLabel(TASKRESULT)) {
            return;
        }

        HugeGraph graph = this.graph();
        String[] properties = this.initTaskResultProperties();

        // Create vertex label '~taskresult'
        VertexLabel label =
            graph.schema().vertexLabel(HugeTaskResult.P.TASKRESULT).properties(properties)
                 .nullableKeys(HugeTaskResult.P.RESULT)
                 .useCustomizeStringId().enableLabelIndex(true).build();

        graph.addVertexLabel(label);
    }

    private String[] initTaskResultProperties() {
        List<String> props = new ArrayList<>();
        props.add(createPropertyKey(HugeTaskResult.P.RESULT, DataType.BLOB));

        return props.toArray(new String[0]);
    }

    private String createPropertyKey(String name, DataType dataType) {
        return createPropertyKey(name, dataType, Cardinality.SINGLE);
    }

    private String createPropertyKey(String name, DataType dataType, Cardinality cardinality) {
        SchemaManager schema = this.graph().schema();
        PropertyKey propertyKey =
            schema.propertyKey(name).dataType(dataType).cardinality(cardinality).build();
        this.graph().addPropertyKey(propertyKey);
        return name;
    }
}
