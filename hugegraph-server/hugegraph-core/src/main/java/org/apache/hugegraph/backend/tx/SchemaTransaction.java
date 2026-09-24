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

package org.apache.hugegraph.backend.tx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.LocalCounter;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.SystemSchemaStore;
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
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.type.define.WriteType;
import org.apache.hugegraph.util.DateUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.LockUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.google.common.collect.ImmutableSet;

public class SchemaTransaction extends IndexableTransaction implements ISchemaTransaction {

    private final SchemaIndexTransaction indexTx;
    private final SystemSchemaStore systemSchemaStore;
    // TODO: move LocalCounter counter define into SystemSchemaStore class
    private final LocalCounter counter;

    public SchemaTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);
        this.autoCommit(true);

        this.indexTx = new SchemaIndexTransaction(graph, store);
        this.systemSchemaStore = store.systemSchemaStore();
        this.counter = graph.counter();
    }

    private static void setCreateTimeIfNeeded(SchemaElement schema) {
        if (!schema.userdata().containsKey(Userdata.CREATE_TIME)) {
            schema.userdata(Userdata.CREATE_TIME, DateUtil.now());
        }
    }

    private static Id asyncRun(HugeGraph graph, SchemaElement schema,
                               SchemaJob callable) {
        return asyncRun(graph, schema, callable, ImmutableSet.of());
    }

    @Watched(prefix = "schema")
    private static Id asyncRun(HugeGraph graph, SchemaElement schema,
                               SchemaJob callable, Set<Id> dependencies) {
        E.checkArgument(schema != null, "Schema can't be null");
        String name = SchemaJob.formatTaskName(schema.type(),
                                               schema.id(),
                                               schema.name());

        JobBuilder<Object> builder = JobBuilder.of(graph).name(name)
                                               .job(callable)
                                               .dependencies(dependencies);
        HugeTask<?> task = builder.schedule();

        // If TASK_SYNC_DELETION is true, wait async thread done before
        // continue. This is used when running tests.
        if (graph.option(CoreOptions.TASK_SYNC_DELETION)) {
            task.syncWait();
        }
        return task.id();
    }

    @Override
    protected AbstractTransaction indexTransaction() {
        return this.indexTx;
    }

    @Override
    protected void beforeRead() {
        /*
         * NOTE: each schema operation will be auto committed,
         * we expect the tx is clean when query.
         */
        if (this.hasUpdate()) {
            throw new BackendException("There are still dirty changes");
        }
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
        if (VertexLabel.OLAP_VL.id().equals(id)) {
            return VertexLabel.OLAP_VL;
        }
        return this.getSchema(HugeType.VERTEX_LABEL, id);
    }

    @Watched(prefix = "schema")
    public VertexLabel getVertexLabel(String name) {
        E.checkArgumentNotNull(name, "Vertex label name can't be null");
        E.checkArgument(!name.isEmpty(), "Vertex label name can't be empty");
        if (VertexLabel.OLAP_VL.name().equals(name)) {
            return VertexLabel.OLAP_VL;
        }
        return this.getSchema(HugeType.VERTEX_LABEL, name);
    }

    @Watched(prefix = "schema")
    public Id removeVertexLabel(Id id) {
        LOG.debug("SchemaTransaction remove vertex label '{}'", id);
        SchemaJob callable = new VertexLabelRemoveJob();
        VertexLabel schema = this.getVertexLabel(id);
        return asyncRun(this.graph(), schema, callable);
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

    @Watched(prefix = "schema")
    public Id removeIndexLabel(Id id) {
        LOG.debug("SchemaTransaction remove index label '{}'", id);
        SchemaJob callable = new IndexLabelRemoveJob();
        IndexLabel schema = this.getIndexLabel(id);
        return asyncRun(this.graph(), schema, callable);
    }

    @Watched(prefix = "schema")
    public Id rebuildIndex(SchemaElement schema) {
        return this.rebuildIndex(schema, ImmutableSet.of());
    }

    @Watched(prefix = "schema")
    public Id rebuildIndex(SchemaElement schema, Set<Id> dependencies) {
        LOG.debug("SchemaTransaction rebuild index for {} with id '{}'",
                  schema.type(), schema.id());
        SchemaJob callable = new IndexLabelRebuildJob();
        return asyncRun(this.graph(), schema, callable, dependencies);
    }

    public void createIndexLabelForOlapPk(PropertyKey propertyKey) {
        WriteType writeType = propertyKey.writeType();
        if (writeType == WriteType.OLTP ||
            writeType == WriteType.OLAP_COMMON) {
            return;
        }

        String indexName = VertexLabel.OLAP_VL.name() + "_by_" +
                           propertyKey.name();
        IndexLabel.Builder builder = this.graph().schema()
                                         .indexLabel(indexName)
                                         .onV(VertexLabel.OLAP_VL.name())
                                         .by(propertyKey.name());
        if (propertyKey.writeType() == WriteType.OLAP_SECONDARY) {
            builder.secondary();
        } else {
            assert propertyKey.writeType() == WriteType.OLAP_RANGE;
            builder.range();
        }
        this.graph().addIndexLabel(VertexLabel.OLAP_VL, builder.build());
    }

    public Id createOlapPk(PropertyKey propertyKey) {
        LOG.debug("SchemaTransaction create olap property key {} with id '{}'",
                  propertyKey.name(), propertyKey.id());
        SchemaJob callable = new OlapPropertyKeyCreateJob();
        return asyncRun(this.graph(), propertyKey, callable);
    }

    public Id clearOlapPk(PropertyKey propertyKey) {
        LOG.debug("SchemaTransaction clear olap property key {} with id '{}'",
                  propertyKey.name(), propertyKey.id());
        SchemaJob callable = new OlapPropertyKeyClearJob();
        return asyncRun(this.graph(), propertyKey, callable);
    }

    public Id removeOlapPk(PropertyKey propertyKey) {
        LOG.debug("SchemaTransaction remove olap property key {} with id '{}'",
                  propertyKey.name(), propertyKey.id());
        SchemaJob callable = new OlapPropertyKeyRemoveJob();
        return asyncRun(this.graph(), propertyKey, callable);
    }

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

    private void saveSchema(SchemaElement schema, boolean update,
                            Consumer<SchemaElement> updateCallback) {
        // Lock for schema update
        LockUtil.Locks locks = new LockUtil.Locks(this.params().graph().spaceGraphName());
        try {
            locks.lockWrites(LockUtil.hugeType2Group(schema.type()), schema.id());

            if (updateCallback != null) {
                // NOTE: Do schema update in the lock block
                updateCallback.accept(schema);
            }

            // System schema just put into SystemSchemaStore in memory
            if (schema.longId() < 0L) {
                this.systemSchemaStore.add(schema);
                return;
            }

            BackendEntry entry = this.serialize(schema);

            this.beforeWrite();

            if (update) {
                this.doUpdateIfPresent(entry);
                // TODO: also support updateIfPresent for index-update
                this.indexTx.updateNameIndex(schema, false);
            } else {
                // TODO: support updateIfAbsentProperty (property: label name)
                this.doUpdateIfAbsent(entry);
                this.indexTx.updateNameIndex(schema, false);
            }

            this.afterWrite();
        } finally {
            locks.unlock();
        }
    }

    protected <T extends SchemaElement> T getSchema(HugeType type, Id id) {
        LOG.debug("SchemaTransaction get {} by id '{}'",
                  type.readableName(), id);
        // System schema just get from SystemSchemaStore in memory
        if (id.asLong() < 0L) {
            return this.systemSchemaStore.get(id);
        }

        this.beforeRead();
        BackendEntry entry = this.query(type, id);
        if (entry == null) {
            return null;
        }
        T schema = this.deserialize(entry, type);
        this.afterRead();
        return schema;
    }

    /**
     * Currently doesn't allow to exist schema with the same name
     *
     * @param type the query schema type
     * @param name the query schema name
     * @param <T>  SubClass of SchemaElement
     * @return the queried schema object
     */
    protected <T extends SchemaElement> T getSchema(HugeType type,
                                                    String name) {
        LOG.debug("SchemaTransaction get {} by name '{}'",
                  type.readableName(), name);
        // System schema just get from SystemSchemaStore in memory
        if (Graph.Hidden.isHidden(name)) {
            return this.systemSchemaStore.get(name);
        }

        this.beforeRead();

        ConditionQuery query = new ConditionQuery(type);
        query.eq(HugeKeys.NAME, name);
        QueryResults<BackendEntry> results = this.indexTx.query(query);

        this.afterRead();

        // Should not exist schema with same name
        BackendEntry entry = results.one();
        if (entry == null) {
            return null;
        }
        return this.deserialize(entry, type);
    }

    protected <T extends SchemaElement> List<T> getAllSchema(HugeType type) {
        List<T> results = new ArrayList<>();
        Query query = new Query(type);
        Iterator<BackendEntry> entries = this.query(query).iterator();
        /*
         * Can use MapperIterator instead if don't need to debug:
         * new MapperIterator<>(entries, entry -> this.deserialize(entry, type))
         * QueryResults.fillList(iter, results);
         */
        try {
            while (entries.hasNext()) {
                BackendEntry entry = entries.next();
                if (entry == null) {
                    continue;
                }
                results.add(this.deserialize(entry, type));
                Query.checkForceCapacity(results.size());
            }
        } finally {
            CloseableIterator.closeIterator(entries);
        }
        return results;
    }

    @Override
    public void removeSchema(SchemaElement schema) {
        LOG.debug("SchemaTransaction remove {} by id '{}'",
                  schema.type(), schema.id());
        // System schema just remove from SystemSchemaStore in memory
        if (schema.longId() < 0L) {
            throw new IllegalStateException("Deletion of system metadata " +
                                            "should not occur");
        }

        LockUtil.Locks locks = new LockUtil.Locks(this.graph().spaceGraphName());
        try {
            locks.lockWrites(LockUtil.hugeType2Group(schema.type()),
                             schema.id());
            this.beforeWrite();
            this.indexTx.updateNameIndex(schema, true);
            BackendEntry e = this.serializer.writeId(schema.type(), schema.id());
            this.doRemove(e);
            this.afterWrite();
        } finally {
            locks.unlock();
        }
    }

    private BackendEntry serialize(SchemaElement schema) {
        switch (schema.type()) {
            case PROPERTY_KEY:
                return this.serializer.writePropertyKey((PropertyKey) schema);
            case VERTEX_LABEL:
                return this.serializer.writeVertexLabel((VertexLabel) schema);
            case EDGE_LABEL:
                return this.serializer.writeEdgeLabel((EdgeLabel) schema);
            case INDEX_LABEL:
                return this.serializer.writeIndexLabel((IndexLabel) schema);
            default:
                throw new AssertionError(String.format(
                        "Unknown schema type '%s'", schema.type()));
        }
    }

    @SuppressWarnings({"unchecked"})
    private <T> T deserialize(BackendEntry entry, HugeType type) {
        switch (type) {
            case PROPERTY_KEY:
                return (T) this.serializer.readPropertyKey(this.graph(), entry);
            case VERTEX_LABEL:
                return (T) this.serializer.readVertexLabel(this.graph(), entry);
            case EDGE_LABEL:
                return (T) this.serializer.readEdgeLabel(this.graph(), entry);
            case INDEX_LABEL:
                return (T) this.serializer.readIndexLabel(this.graph(), entry);
            default:
                throw new AssertionError(String.format(
                        "Unknown schema type '%s'", type));
        }
    }

    public void checkSchemaName(String name) {
        String illegalReg = this.params().configuration()
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
    public String spaceGraphName() {
        return this.graph().spaceGraphName();
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
                     "when graph '%s' in mode '%s'",
                     id, this.graphName(), mode);
        this.setNextIdLowest(type, id.asLong());
    }

    @Watched(prefix = "schema")
    public Id getNextId(HugeType type) {
        LOG.debug("SchemaTransaction get next id for {}", type);
        return this.store().nextId(type);
    }

    @Watched(prefix = "schema")
    public void setNextIdLowest(HugeType type, long lowest) {
        LOG.debug("SchemaTransaction set next id to {} for {}", lowest, type);
        this.store().setCounterLowest(type, lowest);
    }

    @Watched(prefix = "schema")
    public Id getNextSystemId() {
        LOG.debug("SchemaTransaction get next system id");
        Id id = this.counter.nextId(HugeType.SYS_SCHEMA);
        return IdGenerator.of(-id.asLong());
    }
}
