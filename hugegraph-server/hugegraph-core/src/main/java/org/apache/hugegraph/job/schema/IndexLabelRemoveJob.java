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

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.util.LockUtil;

public class IndexLabelRemoveJob extends SchemaJob {

    @Override
    public String type() {
        return REMOVE_SCHEMA;
    }

    @Override
    public Object execute() {
        removeIndexLabel(this.params(), this.schemaId());
        return null;
    }

    protected static void removeIndexLabel(HugeGraphParams graph, Id id) {
        GraphTransaction graphTx = graph.graphTransaction();
        ISchemaTransaction schemaTx = graph.schemaTransaction();
        IndexLabel indexLabel = schemaTx.getIndexLabel(id);
        // If the index label does not exist, return directly
        if (indexLabel == null) {
            return;
        }
        if (indexLabel.status().deleting()) {
            LOG.info("The index label '{}' has been in {} status, " +
                     "please check if it's expected to delete it again",
                     indexLabel, indexLabel.status());
        }
        LockUtil.Locks locks = new LockUtil.Locks(graph.graph().spaceGraphName());
        try {
            locks.lockWrites(LockUtil.INDEX_LABEL_DELETE, id);
            // TODO add update lock
            // Set index label to "deleting" status
            schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.DELETING);
            try {
                // Remove indexLabel from indexLabels of vertex/edge label
                schemaTx.removeIndexLabelFromBaseLabel(indexLabel);

                // Remove index data
                // TODO: use event to replace direct call
                graphTx.removeIndex(indexLabel);
                /*
                 * Should commit changes to backend store before release
                 * delete lock
                 */
                graph.graph().tx().commit();
                // Remove index label
                removeSchema(schemaTx, indexLabel);
            } catch (Throwable e) {
                schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.UNDELETED);
                throw e;
            }
        } finally {
            locks.unlock();
        }
    }
}
