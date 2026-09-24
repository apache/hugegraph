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

package org.apache.hugegraph.backend.tx;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.exception.NotAllowException;
import org.apache.hugegraph.job.JobBuilder;
import org.apache.hugegraph.job.schema.EdgeLabelRemoveJob;
import org.apache.hugegraph.job.schema.IndexLabelRebuildJob;
import org.apache.hugegraph.job.schema.IndexLabelRemoveJob;
import org.apache.hugegraph.job.schema.OlapPropertyKeyClearJob;
import org.apache.hugegraph.job.schema.OlapPropertyKeyCreateJob;
import org.apache.hugegraph.job.schema.OlapPropertyKeyRemoveJob;
import org.apache.hugegraph.job.schema.SchemaJob;
import org.apache.hugegraph.job.schema.VertexLabelRemoveJob;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.PdMetaDriver;
import org.apache.hugegraph.meta.managers.SchemaMetaManager;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.SchemaLabel;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.type.define.WriteType;
import org.apache.hugegraph.util.DateUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.LockUtil;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableSet;

public class SchemaTransactionV2 implements ISchemaTransaction {

    protected static final Logger LOG = Log.logger(SchemaTransaction.class);

    private final String graphSpace;
    private final String graph;
    private final HugeGraphParams graphParams;
    private final IdCounter idCounter;
    private final SchemaMetaManager schemaMetaManager;

    public SchemaTransactionV2(MetaDriver metaDriver,
                               String cluster,
                               HugeGraphParams graphParams) {
        E.checkNotNull(graphParams, "graphParams");
        this.graphParams = graphParams;
        this.graphSpace = graphParams.graph().graphSpace();
        this.graph = graphParams.name();
        this.schemaMetaManager =
                new SchemaMetaManager(metaDriver, cluster, this.graph());
        this.idCounter = new IdCounter(((PdMetaDriver) metaDriver).pdClient(),
                                       idKeyName(this.graphSpace, this.graph));
    }

    private static void setCreateTimeIfNeeded(SchemaElement schema) {
        if (!schema.userdata().containsKey(Userdata.CREATE_TIME)) {
            schema.userdata(Userdata.CREATE_TIME, DateUtil.now());
        }
    }

    /**
     * Asynchronous Task Series
     */
    private static Id asyncRun(HugeGraph graph, SchemaElement schema,
                               SchemaJob job) {
        return asyncRun(graph, schema, job, ImmutableSet.of());
    }

    @Watched(prefix = "schema")
    private static Id asyncRun(HugeGraph graph, SchemaElement schema,
                               SchemaJob job, Set<Id> dependencies) {
        E.checkArgument(schema != null, "Schema can't be null");
        String name = SchemaJob.formatTaskName(schema.type(),
                                               schema.id(),
                                               schema.name());

        JobBuilder<Object> builder = JobBuilder.of(graph).name(name)
                                               .job(job)
                                               .dependencies(dependencies);
        HugeTask<?> task = builder.schedule();
        // If TASK_SYNC_DELETION is true, wait async thread done before
        // continue. This is used when running tests.
        if (graph.option(CoreOptions.TASK_SYNC_DELETION)) {
            task.syncWait();
        }
        return task.id();
    }

    public String idKeyName(String graphSpace, String graph) {
        // {graphSpace}/{graph}/m    "m" means "schema"
        return String.join("/", graphSpace, graph, "m");
    }

    @Watched(prefix = "schema")
    public List<PropertyKey> getPropertyKeys(boolean cache) {
        return this.getAllSchema(HugeType.PROPERTY_KEY);
    }

    @Watched(prefix = "schema")
    public List<PropertyKey> getPropertyKeys() {
        return this.getAllSchema(HugeType.PROPERTY_KEY);
    }

    @Watched(prefix = "schema")
    public List<VertexLabel> getVertexLabels() {
        return this.getAllSchema(HugeType.VERTEX_LABEL);
    }

    @Watched(prefix = "schema")
    public List<EdgeLabel> getEdgeLabels() {
        return this.getAllSchema(HugeType.EDGE_LABEL);
    }

    @Watched(prefix = "schema")
    public List<IndexLabel> getIndexLabels() {
        return this.getAllSchema(HugeType.INDEX_LABEL);
    }

    @Watched(prefix = "schema")
    public Id addPropertyKey(PropertyKey propertyKey) {
        this.addSchema(propertyKey);
        if (!propertyKey.olap()) {
            return IdGenerator.ZERO;
        }
        return this.createOlapPk(propertyKey);
    }

    @Watched(prefix = "schema")
    public void updatePropertyKey(PropertyKey propertyKey) {
        this.updateSchema(propertyKey, null);
    }

    public void updatePropertyKey(PropertyKey old, PropertyKey update) {
        this.removePropertyKey(old.id());
        this.addPropertyKey(update);
    }

    @Watched(prefix = "schema")
    public PropertyKey getPropertyKey(Id id) {
        E.checkArgumentNotNull(id, "Property key id can't be null");
        return this.getSchema(HugeType.PROPERTY_KEY, id);
    }

    @Watched(prefix = "schema")
    public PropertyKey getPropertyKey(String name) {
        E.checkArgumentNotNull(name, "Property key name can't be null");
        E.checkArgument(!name.isEmpty(), "Property key name can't be empty");
        return this.getSchema(HugeType.PROPERTY_KEY, name);
    }

    @Watched(prefix = "schema")
    public Id removePropertyKey(Id id) {
        LOG.debug("SchemaTransaction remove property key '{}'", id);
        PropertyKey propertyKey = this.getPropertyKey(id);
        // If the property key does not exist, return directly
        if (propertyKey == null) {
            return null;
        }

        List<VertexLabel> vertexLabels = this.getVertexLabels();
        for (VertexLabel vertexLabel : vertexLabels) {
            if (vertexLabel.properties().contains(id)) {
                throw new NotAllowException(
                        "Not allowed to remove property key: '%s' " +
                        "because the vertex label '%s' is still using it.",
                        propertyKey, vertexLabel.name());
            }
        }

        List<EdgeLabel> edgeLabels = this.getEdgeLabels();
        for (EdgeLabel edgeLabel : edgeLabels) {
            if (edgeLabel.properties().contains(id)) {
                throw new NotAllowException(
                        "Not allowed to remove property key: '%s' " +
                        "because the edge label '%s' is still using it.",
                        propertyKey, edgeLabel.name());
            }
        }
        if (propertyKey.oltp()) {
            this.removeSchema(propertyKey);
            return IdGenerator.ZERO;
        } else {
            return this.removeOlapPk(propertyKey);
        }
    }

    @Watched(prefix = "schema")
    public void addVertexLabel(VertexLabel vertexLabel) {
        this.addSchema(vertexLabel);
    }

    @Watched(prefix = "schema")
    public void updateVertexLabel(VertexLabel vertexLabel) {
        this.updateSchema(vertexLabel, null);
    }

    @Watched(prefix = "schema")
    public VertexLabel getVertexLabel(Id id) {
        E.checkArgumentNotNull(id, "Vertex label id can't be null");
        if (SchemaElement.OLAP_ID.equals(id)) {
            return VertexLabel.OLAP_VL;
        }
        return this.getSchema(HugeType.VERTEX_LABEL, id);
    }

    @Watched(prefix = "schema")
    public VertexLabel getVertexLabel(String name) {
        E.checkArgumentNotNull(name, "Vertex label name can't be null");
        E.checkArgument(!name.isEmpty(), "Vertex label name can't be empty");
        if (SchemaElement.OLAP.equals(name)) {
            return VertexLabel.OLAP_VL;
        }
        return this.getSchema(HugeType.VERTEX_LABEL, name);
    }

    @Watched(prefix = "schema")
    public Id removeVertexLabel(Id id) {
        LOG.debug("SchemaTransaction remove vertex label '{}'", id);
        SchemaJob job = new VertexLabelRemoveJob();
        VertexLabel schema = this.getVertexLabel(id);
        return asyncRun(this.graph(), schema, job);
    }

    @Watched(prefix = "schema")
    public void addEdgeLabel(EdgeLabel edgeLabel) {
        this.addSchema(edgeLabel);
    }

    @Watched(prefix = "schema")
    public void updateEdgeLabel(EdgeLabel edgeLabel) {
        this.updateSchema(edgeLabel, null);
    }

    @Watched(prefix = "schema")
    public EdgeLabel getEdgeLabel(Id id) {
        E.checkArgumentNotNull(id, "Edge label id can't be null");
        return this.getSchema(HugeType.EDGE_LABEL, id);
    }

    @Watched(prefix = "schema")
    public EdgeLabel getEdgeLabel(String name) {
        E.checkArgumentNotNull(name, "Edge label name can't be null");
        E.checkArgument(!name.isEmpty(), "Edge label name can't be empty");
        return this.getSchema(HugeType.EDGE_LABEL, name);
    }

    @Watched(prefix = "schema")
    public Id removeEdgeLabel(Id id) {
        /*
         * Call an asynchronous task and call back the corresponding
         * removeSchema() method after the task ends to complete the delete
         * schema operation
         */
        LOG.debug("SchemaTransaction remove edge label '{}'", id);
        EdgeLabel schema = this.getEdgeLabel(id);
        if (schema.edgeLabelType().parent()) {
            List<EdgeLabel> edgeLabels = this.getEdgeLabels();
            for (EdgeLabel edgeLabel : edgeLabels) {
                if (edgeLabel.edgeLabelType().sub() &&
                    edgeLabel.fatherId() == id) {
                    throw new NotAllowException(
                            "Not allowed to remove a parent edge label: '%s' " +
                            "because the sub edge label '%s' is still existing",
                            schema.name(), edgeLabel.name());
                }
            }
        }
        SchemaJob job = new EdgeLabelRemoveJob();
        return asyncRun(this.graph(), schema, job);
    }

    @Watched(prefix = "schema")
    public void addIndexLabel(SchemaLabel baseLabel, IndexLabel indexLabel) {
        /*
         * Create index and update index name in base-label(VL/EL)
         * TODO: should wrap update base-label and create index in one tx.
         */
        this.addSchema(indexLabel);

        if (baseLabel.equals(VertexLabel.OLAP_VL)) {
            return;
        }

        this.updateSchema(baseLabel, schema -> {
            // NOTE: Do schema update in the lock block
            baseLabel.addIndexLabel(indexLabel.id());
        });
    }

    @Watched(prefix = "schema")
    public void updateIndexLabel(IndexLabel indexLabel) {
        this.updateSchema(indexLabel, null);
    }

    @Watched(prefix = "schema")
    public IndexLabel getIndexLabel(Id id) {
        E.checkArgumentNotNull(id, "Index label id can't be null");
        return this.getSchema(HugeType.INDEX_LABEL, id);
    }

    @Watched(prefix = "schema")
    public IndexLabel getIndexLabel(String name) {
        E.checkArgumentNotNull(name, "Index label name can't be null");
        E.checkArgument(!name.isEmpty(), "Index label name can't be empty");
        return this.getSchema(HugeType.INDEX_LABEL, name);
    }

    @Override
    public void close() {

    }

    @Watched(prefix = "schema")
    public Id removeIndexLabel(Id id) {
        LOG.debug("SchemaTransaction remove index label '{}'", id);
        SchemaJob job = new IndexLabelRemoveJob();
        IndexLabel schema = this.getIndexLabel(id);
        return asyncRun(this.graph(), schema, job);
    }

    // Generality of schema processing functions
    @Watched(prefix = "schema")
    public void updateSchemaStatus(SchemaElement schema, SchemaStatus status) {
        if (!this.existsSchemaId(schema.type(), schema.id())) {
            LOG.warn("Can't update schema '{}', it may be deleted", schema);
            return;
        }

        this.updateSchema(schema, schemaToUpdate -> {
            // NOTE: Do schema update in the lock block
            schema.status(status);
        });
    }

    @Watched(prefix = "schema")
    public boolean existsSchemaId(HugeType type, Id id) {
        return this.getSchema(type, id) != null;
    }

    @Override
    public void removeIndexLabelFromBaseLabel(IndexLabel indexLabel) {
        HugeType baseType = indexLabel.baseType();
        Id baseValue = indexLabel.baseValue();
        SchemaLabel baseLabel;
        if (baseType == HugeType.VERTEX_LABEL) {
            baseLabel = this.getVertexLabel(baseValue);
        } else {
            assert baseType == HugeType.EDGE_LABEL;
            baseLabel = this.getEdgeLabel(baseValue);
        }

        if (baseLabel == null) {
            LOG.info("The base label '{}' of index label '{}' " +
                     "may be deleted before", baseValue, indexLabel);
            return;
        }
        if (baseLabel.equals(VertexLabel.OLAP_VL)) {
            return;
        }

        this.updateSchema(baseLabel, schema -> {
            // NOTE: Do schema update in the lock block
            baseLabel.removeIndexLabel(indexLabel.id());
        });
    }

    protected void updateSchema(SchemaElement schema,
                                Consumer<SchemaElement> updateCallback) {
        LOG.debug("SchemaTransaction update {} with id '{}'",
                  schema.type(), schema.id());
        this.saveSchema(schema, true, updateCallback);
    }

    protected void addSchema(SchemaElement schema) {
        LOG.debug("SchemaTransaction add {} with id '{}'",
                  schema.type(), schema.id());
        setCreateTimeIfNeeded(schema);
        this.saveSchema(schema, false, null);
    }

    @SuppressWarnings("unchecked")
    private void saveSchema(SchemaElement schema, boolean update,
                            Consumer<SchemaElement> updateCallback) {
        // Lock for schema update
        String spaceGraph = this.graphParams()
                                .graph().spaceGraphName();
        LockUtil.Locks locks = new LockUtil.Locks(spaceGraph);
        try {
            locks.lockWrites(LockUtil.hugeType2Group(schema.type()), schema.id());

            if (updateCallback != null) {
                // NOTE: Do schema update in the lock block
                updateCallback.accept(schema);
            }
            // Call the corresponding method
            switch (schema.type()) {
                case PROPERTY_KEY:
                    this.schemaMetaManager.addPropertyKey(this.graphSpace,
                                                          this.graph,
                                                          (PropertyKey) schema);
                    break;
                case VERTEX_LABEL:
                    this.schemaMetaManager.addVertexLabel(this.graphSpace,
                                                          this.graph,
                                                          (VertexLabel) schema);
                    // Point's label changes, clear the corresponding graph's point cache
                    // information
                    MetaManager.instance().notifyGraphVertexCacheClear(this.graphSpace, this.graph);
                    break;
                case EDGE_LABEL:
                    this.schemaMetaManager.addEdgeLabel(this.graphSpace,
                                                        this.graph,
                                                        (EdgeLabel) schema);
                    // Side label changes, clear the corresponding edge cache information of the
                    // graph.
                    MetaManager.instance().notifyGraphEdgeCacheClear(this.graphSpace, this.graph);
                    break;
                case INDEX_LABEL:
                    this.schemaMetaManager.addIndexLabel(this.graphSpace,
                                                         this.graph,
                                                         (IndexLabel) schema);
                    break;
                default:
                    throw new AssertionError(String.format(
                            "Invalid key '%s' for saveSchema", schema.type()));
            }
        } finally {
            locks.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    protected <T extends SchemaElement> T getSchema(HugeType type, Id id) {
        LOG.debug("SchemaTransaction get {} by id '{}'",
                  type.readableName(), id);
        switch (type) {
            case PROPERTY_KEY:
                return (T) this.schemaMetaManager.getPropertyKey(this.graphSpace,
                                                                 this.graph, id);
            case VERTEX_LABEL:
                return (T) this.schemaMetaManager.getVertexLabel(this.graphSpace,
                                                                 this.graph, id);
            case EDGE_LABEL:
                return (T) this.schemaMetaManager.getEdgeLabel(this.graphSpace,
                                                               this.graph, id);
            case INDEX_LABEL:
                return (T) this.schemaMetaManager.getIndexLabel(this.graphSpace,
                                                                this.graph, id);
            default:
                throw new AssertionError(String.format(
                        "Invalid type '%s' for getSchema", type));
        }
    }

    /**
     * Currently doesn't allow to exist schema with the same name
     *
     * @param type the query schema type
     * @param name the query schema name
     * @param <T>  SubClass of SchemaElement
     * @return the queried schema object
     */
    @SuppressWarnings("unchecked")
    protected <T extends SchemaElement> T getSchema(HugeType type, String name) {
        LOG.debug("SchemaTransaction get {} by name '{}'",
                  type.readableName(), name);
        switch (type) {
            case PROPERTY_KEY:
                return (T) this.schemaMetaManager.getPropertyKey(this.graphSpace,
                                                                 this.graph, name);
            case VERTEX_LABEL:
                return (T) this.schemaMetaManager.getVertexLabel(this.graphSpace,
                                                                 this.graph, name);
            case EDGE_LABEL:
                return (T) this.schemaMetaManager.getEdgeLabel(this.graphSpace,
                                                               this.graph, name);
            case INDEX_LABEL:
                return (T) this.schemaMetaManager.getIndexLabel(this.graphSpace,
                                                                this.graph, name);
            default:
                throw new AssertionError(String.format(
                        "Invalid type '%s' for getSchema", type));
        }
    }

    @SuppressWarnings("unchecked")
    protected <T extends SchemaElement> List<T> getAllSchema(HugeType type) {
        LOG.debug("SchemaTransaction getAllSchema {}", type.readableName());
        switch (type) {
            case PROPERTY_KEY:
                return (List<T>) this.schemaMetaManager.getPropertyKeys(this.graphSpace,
                                                                        this.graph);
            case VERTEX_LABEL:
                return (List<T>) this.schemaMetaManager.getVertexLabels(this.graphSpace,
                                                                        this.graph);
            case EDGE_LABEL:
                return (List<T>) this.schemaMetaManager.getEdgeLabels(this.graphSpace, this.graph);
            case INDEX_LABEL:
                return (List<T>) this.schemaMetaManager.getIndexLabels(this.graphSpace, this.graph);
            default:
                throw new AssertionError(String.format(
                        "Invalid type '%s' for getSchema", type));
        }
    }

    @Override
    public void removeSchema(SchemaElement schema) {
        LOG.debug("SchemaTransaction remove {} by id '{}'",
                  schema.type(), schema.id());
        String spaceGraph = this.graphParams()
                                .graph().spaceGraphName();
        LockUtil.Locks locks = new LockUtil.Locks(spaceGraph);
        try {
            locks.lockWrites(LockUtil.hugeType2Group(schema.type()),
                             schema.id());
            switch (schema.type()) {
                case PROPERTY_KEY:
                    this.schemaMetaManager.removePropertyKey(this.graphSpace, this.graph,
                                                             schema.id());
                    break;
                case VERTEX_LABEL:
                    this.schemaMetaManager.removeVertexLabel(this.graphSpace, this.graph,
                                                             schema.id());
                    break;
                case EDGE_LABEL:
                    this.schemaMetaManager.removeEdgeLabel(this.graphSpace, this.graph,
                                                           schema.id());
                    break;
                case INDEX_LABEL:
                    this.schemaMetaManager.removeIndexLabel(this.graphSpace, this.graph,
                                                            schema.id());
                    break;
                default:
                    throw new AssertionError(String.format(
                            "Invalid key '%s' for saveSchema", schema.type()));
            }
        } finally {
            locks.unlock();
        }
    }

    // OLAP related methods
    public void createIndexLabelForOlapPk(PropertyKey propertyKey) {
        WriteType writeType = propertyKey.writeType();
        if (writeType == WriteType.OLTP ||
            writeType == WriteType.OLAP_COMMON) {
            return;
        }

        String indexName = SchemaElement.OLAP + "_by_" + propertyKey.name();
        IndexLabel.Builder builder = this.graph().schema()
                                         .indexLabel(indexName)
                                         .onV(SchemaElement.OLAP)
                                         .by(propertyKey.name());
        if (propertyKey.writeType() == WriteType.OLAP_SECONDARY) {
            builder.secondary();
        } else {
            assert propertyKey.writeType() == WriteType.OLAP_RANGE;
            builder.range();
        }
        builder.build();
        this.graph().addIndexLabel(VertexLabel.OLAP_VL, builder.build());
    }

    public Id removeOlapPk(PropertyKey propertyKey) {
        LOG.debug("SchemaTransaction remove olap property key {} with id '{}'",
                  propertyKey.name(), propertyKey.id());
        SchemaJob job = new OlapPropertyKeyRemoveJob();
        return asyncRun(this.graph(), propertyKey, job);
    }

    public void removeOlapPk(Id id) {
        this.graphParams().loadGraphStore().removeOlapTable(id);
    }

    public Id clearOlapPk(PropertyKey propertyKey) {
        LOG.debug("SchemaTransaction clear olap property key {} with id '{}'",
                  propertyKey.name(), propertyKey.id());
        SchemaJob job = new OlapPropertyKeyClearJob();
        return asyncRun(this.graph(), propertyKey, job);
    }

    public void clearOlapPk(Id id) {
        this.graphParams().loadGraphStore().clearOlapTable(id);
    }

    public Id createOlapPk(PropertyKey propertyKey) {
        LOG.debug("SchemaTransaction create olap property key {} with id '{}'",
                  propertyKey.name(), propertyKey.id());
        SchemaJob job = new OlapPropertyKeyCreateJob();
        return asyncRun(this.graph(), propertyKey, job);
    }

    // -- store related methods, divided into two categories:
    // 1. olap table related 2. ID generation strategy
    // - 1. olap table related
    public void createOlapPk(Id id) {
        this.graphParams().loadGraphStore().createOlapTable(id);
    }

    public boolean existOlapTable(Id id) {
        return this.graphParams().loadGraphStore().existOlapTable(id);
    }

    public void initAndRegisterOlapTables() {
        for (PropertyKey pk : this.getPropertyKeys()) {
            if (pk.olap()) {
                this.graphParams().loadGraphStore().checkAndRegisterOlapTable(pk.id());
            }
        }
    }

    // - 2. ID generation strategy
    @Watched(prefix = "schema")
    public Id getNextId(HugeType type) {
        LOG.debug("SchemaTransaction get next id for {}", type);
        return this.idCounter.nextId(type);
    }

    @Watched(prefix = "schema")
    public void setNextIdLowest(HugeType type, long lowest) {
        LOG.debug("SchemaTransaction set next id to {} for {}", lowest, type);
        this.idCounter.setCounterLowest(type, lowest);
    }

    @Watched(prefix = "schema")
    public Id getNextSystemId() {
        LOG.debug("SchemaTransaction get next system id");
        Id id = this.idCounter.nextId(HugeType.SYS_SCHEMA);
        return IdGenerator.of(-id.asLong());
    }

    @Watched(prefix = "schema")
    public Id validOrGenerateId(HugeType type, Id id, String name) {
        boolean forSystem = Graph.Hidden.isHidden(name);
        if (id != null) {
            this.checkIdAndUpdateNextId(type, id, name, forSystem);
        } else {
            if (forSystem) {
                id = this.getNextSystemId();
            } else {
                id = this.getNextId(type);
            }
        }
        return id;
    }

    private void checkIdAndUpdateNextId(HugeType type, Id id,
                                        String name, boolean forSystem) {
        if (forSystem) {
            if (id.number() && id.asLong() < 0) {
                return;
            }
            throw new IllegalStateException(String.format(
                    "Invalid system id '%s'", id));
        }
        E.checkState(id.number() && id.asLong() > 0L,
                     "Schema id must be number and >0, but got '%s'", id);
        GraphMode mode = this.graphMode();
        E.checkState(mode == GraphMode.RESTORING,
                     "Can't build schema with provided id '%s' " +
                     "when graph '%s' in mode '%s'", id, this.graph, mode);
        this.setNextIdLowest(type, id.asLong());
    }

    // Functional functions
    public void checkSchemaName(String name) {
        String illegalReg = this.graphParams().configuration()
                                .get(CoreOptions.SCHEMA_ILLEGAL_NAME_REGEX);
        E.checkNotNull(name, "name");
        E.checkArgument(!name.isEmpty(), "The name can't be empty.");
        E.checkArgument(name.length() < 256,
                        "The length of name must less than 256 bytes.");
        E.checkArgument(!name.matches(illegalReg),
                        "Illegal schema name '%s'", name);

        final char[] filters = {'#', '>', ':', '!'};
        for (char c : filters) {
            E.checkArgument(name.indexOf(c) == -1,
                            "The name can't contain character '%s'.", c);
        }
    }

    @Override
    public String graphName() {
        return this.graph;
    }

    @Override
    public String spaceGraphName() {
        return this.graph().spaceGraphName();
    }

    protected HugeGraphParams graphParams() {
        return this.graphParams;
    }

    public GraphMode graphMode() {
        return this.graphParams().mode();
    }

    // Get field method
    public HugeGraph graph() {
        return this.graphParams.graph();
    }

    // Rebuild index
    @Watched(prefix = "schema")
    public Id rebuildIndex(SchemaElement schema) {
        return this.rebuildIndex(schema, ImmutableSet.of());
    }

    @Watched(prefix = "schema")
    public Id rebuildIndex(SchemaElement schema, Set<Id> dependencies) {
        LOG.debug("SchemaTransaction rebuild index for {} with id '{}'",
                  schema.type(), schema.id());
        SchemaJob job = new IndexLabelRebuildJob();
        return asyncRun(this.graph(), schema, job, dependencies);
    }

    /**
     * Clear all schema information
     */
    public void clear() {
        this.schemaMetaManager.clearAllSchema(this.graphSpace, graph);
    }
}
