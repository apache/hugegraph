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

import java.util.Collection;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.backend.tx.ISchemaTransaction;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.SchemaLabel;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.util.LockUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.collect.ImmutableSet;

public class IndexLabelRebuildJob extends SchemaJob {

    @Override
    public String type() {
        return REBUILD_INDEX;
    }

    @Override
    public Object execute() {
        SchemaElement schema = this.schemaElement();
        // If the schema does not exist, ignore it
        if (schema != null) {
            this.rebuildIndex(schema);
        }
        return null;
    }

    private void rebuildIndex(SchemaElement schema) {
        switch (schema.type()) {
            case INDEX_LABEL:
                IndexLabel indexLabel = (IndexLabel) schema;
                SchemaLabel label;
                if (indexLabel.baseType() == HugeType.VERTEX_LABEL) {
                    label = this.graph().vertexLabel(indexLabel.baseValue());
                } else {
                    assert indexLabel.baseType() == HugeType.EDGE_LABEL;
                    label = this.graph().edgeLabel(indexLabel.baseValue());
                }
                assert label != null;
                this.rebuildIndex(label, ImmutableSet.of(indexLabel.id()));
                break;
            case VERTEX_LABEL:
            case EDGE_LABEL:
                label = (SchemaLabel) schema;
                this.rebuildIndex(label, label.indexLabels());
                break;
            default:
                assert schema.type() == HugeType.PROPERTY_KEY;
                throw new AssertionError(String.format(
                        "The %s can't rebuild index", schema.type()));
        }
    }

    private void rebuildIndex(SchemaLabel label, Collection<Id> indexLabelIds) {
        ISchemaTransaction schemaTx = this.params().schemaTransaction();
        GraphTransaction graphTx = this.params().graphTransaction();

        Consumer<?> indexUpdater = (elem) -> {
            for (Id id : indexLabelIds) {
                graphTx.updateIndex(id, (HugeElement) elem, false);
            }
        };

        LockUtil.Locks locks = new LockUtil.Locks(schemaTx.spaceGraphName());
        try {
            locks.lockWrites(LockUtil.INDEX_LABEL_REBUILD, indexLabelIds);

            Set<IndexLabel> ils = indexLabelIds.stream()
                                               .map(this.graph()::indexLabel)
                                               .collect(Collectors.toSet());
            for (IndexLabel il : ils) {
                if (il.status() == SchemaStatus.CREATING) {
                    continue;
                }
                schemaTx.updateSchemaStatus(il, SchemaStatus.REBUILDING);
            }

            this.removeIndex(indexLabelIds);
            /*
             * Note: Here must commit index transaction firstly.
             * Because remove index convert to (id like <?>:personByCity):
             * `delete from index table where label = ?`,
             * But append index will convert to (id like Beijing:personByCity):
             * `update index element_ids += xxx where field_value = ?
             * and index_label_name = ?`,
             * They have different id lead to it can't compare and optimize
             */
            graphTx.commit();

            try {
                if (label.type() == HugeType.VERTEX_LABEL) {
                    @SuppressWarnings("unchecked")
                    Consumer<Vertex> consumer = (Consumer<Vertex>) indexUpdater;
                    graphTx.traverseVerticesByLabel((VertexLabel) label,
                                                    consumer, false);
                } else {
                    assert label.type() == HugeType.EDGE_LABEL;
                    @SuppressWarnings("unchecked")
                    Consumer<Edge> consumer = (Consumer<Edge>) indexUpdater;
                    graphTx.traverseEdgesByLabel((EdgeLabel) label,
                                                 consumer, false);
                }
                graphTx.commit();
            } catch (Throwable e) {
                for (IndexLabel il : ils) {
                    schemaTx.updateSchemaStatus(il, SchemaStatus.INVALID);
                }
                throw e;
            }

            for (IndexLabel il : ils) {
                schemaTx.updateSchemaStatus(il, SchemaStatus.CREATED);
            }
        } finally {
            locks.unlock();
        }
    }

    private void removeIndex(Collection<Id> indexLabelIds) {
        ISchemaTransaction schemaTx = this.params().schemaTransaction();
        GraphTransaction graphTx = this.params().graphTransaction();

        for (Id id : indexLabelIds) {
            IndexLabel il = schemaTx.getIndexLabel(id);
            if (il == null || il.status() == SchemaStatus.CREATING) {
                /*
                 * TODO: How to deal with non-existent index name:
                 * continue or throw exception?
                 */
                continue;
            }
            LockUtil.Locks locks = new LockUtil.Locks(schemaTx.spaceGraphName());
            try {
                locks.lockWrites(LockUtil.INDEX_LABEL_DELETE, indexLabelIds);
                graphTx.removeIndex(il);
            } catch (Throwable e) {
                schemaTx.updateSchemaStatus(il, SchemaStatus.INVALID);
                throw e;
            } finally {
                locks.unlock();
            }
        }
    }

    private SchemaElement schemaElement() {
        HugeType type = this.schemaType();
        Id id = this.schemaId();
        switch (type) {
            case VERTEX_LABEL:
                return this.graph().vertexLabel(id);
            case EDGE_LABEL:
                return this.graph().edgeLabel(id);
            case INDEX_LABEL:
                return this.graph().indexLabel(id);
            default:
                throw new AssertionError(String.format(
                        "Invalid HugeType '%s' for rebuild", type));
        }
    }
}
