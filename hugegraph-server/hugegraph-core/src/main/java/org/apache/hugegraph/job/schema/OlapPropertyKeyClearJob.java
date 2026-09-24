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

public class OlapPropertyKeyClearJob extends IndexLabelRemoveJob {

    @Override
    public String type() {
        return CLEAR_OLAP;
    }

    @Override
    public Object execute() {
        Id olap = this.schemaId();

        // Clear olap data table
        this.params().graphTransaction().clearOlapPk(olap);

        // Clear corresponding index data
        clearIndexLabel(this.params(), olap);
        return null;
    }

    protected static void clearIndexLabel(HugeGraphParams graph, Id id) {
        Id olapIndexLabel = findOlapIndexLabel(graph, id);
        if (olapIndexLabel == null) {
            return;
        }
        GraphTransaction graphTx = graph.graphTransaction();
        ISchemaTransaction schemaTx = graph.schemaTransaction();
        IndexLabel indexLabel = schemaTx.getIndexLabel(olapIndexLabel);
        // If the index label does not exist, return directly
        if (indexLabel == null) {
            return;
        }
        LockUtil.Locks locks = new LockUtil.Locks(graph.graph().spaceGraphName());
        try {
            locks.lockWrites(LockUtil.INDEX_LABEL_DELETE, olapIndexLabel);
            // Set index label to "rebuilding" status
            schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.REBUILDING);
            try {
                // Remove index data
                graphTx.removeIndex(indexLabel);
                /*
                 * Should commit changes to backend store before release
                 * delete lock
                 */
                graph.graph().tx().commit();
                schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.CREATED);
            } catch (Throwable e) {
                schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.INVALID);
                throw e;
            }
        } finally {
            locks.unlock();
        }
    }

    protected static Id findOlapIndexLabel(HugeGraphParams graph, Id olap) {
        ISchemaTransaction schemaTx = graph.schemaTransaction();
        for (IndexLabel indexLabel : schemaTx.getIndexLabels()) {
            if (indexLabel.indexFields().contains(olap)) {
                return indexLabel.id();
            }
        }
        return null;
    }
}
