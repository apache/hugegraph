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

import java.util.Set;

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.util.LockUtil;

import com.google.common.collect.ImmutableSet;

public class EdgeLabelRemoveJob extends SchemaJob {

    @Override
    public String type() {
        return REMOVE_SCHEMA;
    }

    @Override
    public Object execute() {
        removeEdgeLabel(this.params(), this.schemaId());
        return null;
    }

    private static void removeEdgeLabel(HugeGraphParams graph, Id id) {
        GraphTransaction graphTx = graph.graphTransaction();
        ISchemaTransaction schemaTx = graph.schemaTransaction();
        EdgeLabel edgeLabel = schemaTx.getEdgeLabel(id);
        // If the edge label does not exist, return directly
        if (edgeLabel == null) {
            return;
        }
        if (edgeLabel.status().deleting()) {
            LOG.info("The edge label '{}' has been in {} status, " +
                     "please check if it's expected to delete it again",
                     edgeLabel, edgeLabel.status());
        }
        // Remove index related data(include schema) of this edge label
        Set<Id> indexIds = ImmutableSet.copyOf(edgeLabel.indexLabels());
        LockUtil.Locks locks = new LockUtil.Locks(graph.graph().spaceGraphName());
        try {
            locks.lockWrites(LockUtil.EDGE_LABEL_DELETE, id);
            schemaTx.updateSchemaStatus(edgeLabel, SchemaStatus.DELETING);
            try {
                for (Id indexId : indexIds) {
                    IndexLabelRemoveJob.removeIndexLabel(graph, indexId);
                }
                // Remove all edges which has matched label
                // TODO: use event to replace direct call
                graphTx.removeEdges(edgeLabel);
                /*
                 * Should commit changes to backend store before release
                 * delete lock
                 */
                graph.graph().tx().commit();
                // Remove edge label
                removeSchema(schemaTx, edgeLabel);
            } catch (Throwable e) {
                schemaTx.updateSchemaStatus(edgeLabel, SchemaStatus.UNDELETED);
                throw e;
            }
        } finally {
            locks.unlock();
        }
    }
}
