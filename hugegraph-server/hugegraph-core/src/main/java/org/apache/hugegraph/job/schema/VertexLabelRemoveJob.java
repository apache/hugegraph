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

package org.apache.hugegraph.job.schema;

import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.util.LockUtil;

import com.google.common.collect.ImmutableSet;

public class VertexLabelRemoveJob extends SchemaJob {

    @Override
    public String type() {
        return REMOVE_SCHEMA;
    }

    @Override
    public Object execute() {
        removeVertexLabel(this.params(), this.schemaId());
        return null;
    }

    private static void removeVertexLabel(HugeGraphParams graph, Id id) {
        GraphTransaction graphTx = graph.graphTransaction();
        ISchemaTransaction schemaTx = graph.schemaTransaction();
        VertexLabel vertexLabel = schemaTx.getVertexLabel(id);
        // If the vertex label does not exist, return directly
        if (vertexLabel == null) {
            return;
        }
        if (vertexLabel.status().deleting()) {
            LOG.info("The vertex label '{}' has been in {} status, " +
                     "please check if it's expected to delete it again",
                     vertexLabel, vertexLabel.status());
        }

        // Check no edge label use the vertex label
        List<EdgeLabel> edgeLabels = schemaTx.getEdgeLabels();
        for (EdgeLabel edgeLabel : edgeLabels) {
            if (edgeLabel.linkWithLabel(id)) {
                throw new HugeException(
                        "Not allowed to remove vertex label '%s' " +
                        "because the edge label '%s' still link with it",
                        vertexLabel.name(), edgeLabel.name());
            }
        }

        /*
         * Copy index label ids because removeIndexLabel will mutate
         * vertexLabel.indexLabels()
         */
        Set<Id> indexLabelIds = ImmutableSet.copyOf(vertexLabel.indexLabels());
        LockUtil.Locks locks = new LockUtil.Locks(graph.graph().spaceGraphName());
        try {
            locks.lockWrites(LockUtil.VERTEX_LABEL_DELETE, id);
            schemaTx.updateSchemaStatus(vertexLabel, SchemaStatus.DELETING);
            try {
                for (Id ilId : indexLabelIds) {
                    IndexLabelRemoveJob.removeIndexLabel(graph, ilId);
                }
                // TODO: use event to replace direct call
                // Deleting a vertex will automatically deletes the held edge
                graphTx.removeVertices(vertexLabel);
                /*
                 * Should commit changes to backend store before release
                 * delete lock
                 */
                graph.graph().tx().commit();
                // Remove vertex label
                removeSchema(schemaTx, vertexLabel);
            } catch (Throwable e) {
                schemaTx.updateSchemaStatus(vertexLabel,
                                            SchemaStatus.UNDELETED);
                throw e;
            }
        } finally {
            locks.unlock();
        }
    }
}
