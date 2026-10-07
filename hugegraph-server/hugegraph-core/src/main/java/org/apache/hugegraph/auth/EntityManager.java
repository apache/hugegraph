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

package org.apache.hugegraph.auth;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.auth.SchemaDefine.Entity;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.collect.ImmutableMap;

public class EntityManager<T extends Entity> {

    private final HugeGraphParams graph;
    private final String label;
    private final Function<Vertex, T> deser;
    private final ThreadLocal<Boolean> autoCommit = new ThreadLocal<>();

    private static final long NO_LIMIT = -1L;

    public EntityManager(HugeGraphParams graph, String label,
                         Function<Vertex, T> deser) {
        E.checkNotNull(graph, "graph");

        this.graph = graph;
        this.label = label;
        this.deser = deser;
    }

    private GraphTransaction tx() {
        return this.graph.systemTransaction();
    }

    private HugeGraph graph() {
        return this.graph.graph();
    }

    private String unhideLabel() {
        return Hidden.unHide(this.label);
    }

    public Id add(T entity) {
        E.checkArgumentNotNull(entity, "Entity can't be null");
        return this.save(entity, false);
    }

    public Id update(T entity) {
        E.checkArgumentNotNull(entity, "Entity can't be null");
        entity.onUpdate();
        return this.save(entity, true);
    }

    public T delete(Id id) {
        T entity = null;
        Iterator<Vertex> vertices = this.tx().queryVertices(id);
        if (vertices.hasNext()) {
            HugeVertex vertex = (HugeVertex) vertices.next();
            entity = this.deser.apply(vertex);
            this.tx().removeVertex(vertex);
            this.commitOrRollback();
            assert !vertices.hasNext();
        }
        return entity;
    }

    public T get(Id id) {
        T entity = null;
        Iterator<Vertex> vertices = this.tx().queryVertices(id);
        if (vertices.hasNext()) {
            entity = this.deser.apply(vertices.next());
            assert !vertices.hasNext();
        }
        if (entity == null) {
            throw new NotFoundException("Can't find %s with id '%s'",
                                        this.unhideLabel(), id);
        }
        return entity;
    }

    public boolean exists(Id id) {
        Iterator<Vertex> vertices = this.tx().queryVertices(id);
        if (vertices.hasNext()) {
            Vertex vertex = vertices.next();
            return this.label.equals(vertex.label());
        }
        return false;
    }

    public List<T> list(List<Id> ids) {
        return toList(this.queryById(ids));
    }

    public List<T> list(long limit) {
        return toList(this.queryEntity(this.label, ImmutableMap.of(), limit));
    }

    protected List<T> query(String key, Object value, long limit) {
        Map<String, Object> conditions = ImmutableMap.of(key, value);
        return toList(this.queryEntity(this.label, conditions, limit));
    }

    protected List<T> toList(Iterator<Vertex> vertices) {
        Iterator<T> iter = new MapperIterator<>(vertices, this.deser);
        // Convert iterator to list to avoid across thread tx accessed
        return (List<T>) QueryResults.toList(iter).list();
    }

    private Iterator<Vertex> queryById(List<Id> ids) {
        Object[] idArray = ids.toArray(new Id[0]);
        return this.tx().queryVertices(idArray);
    }

    private Iterator<Vertex> queryEntity(String label,
                                         Map<String, Object> conditions,
                                         long limit) {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        VertexLabel vl = this.graph().vertexLabel(label);
        query.eq(HugeKeys.LABEL, vl.id());
        for (Map.Entry<String, Object> entry : conditions.entrySet()) {
            PropertyKey pkey = this.graph().propertyKey(entry.getKey());
            query.query(Condition.eq(pkey.id(), entry.getValue()));
        }
        query.showHidden(true);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        return this.tx().queryVertices(query);
    }

    private Id save(T entity, boolean expectExists) {
        // Construct vertex from task
        HugeVertex vertex = this.constructVertex(entity);
        E.checkArgument(this.exists(vertex.id()) == expectExists,
                        "Can't save %s '%s' that %s exists",
                        this.unhideLabel(), vertex.id(),
                        expectExists ? "not" : "already");

        // Add or update user in backend store, stale index might exist
        vertex = this.tx().addVertex(vertex);
        this.commitOrRollback();
        return vertex.id();
    }

    private HugeVertex constructVertex(Entity entity) {
        if (!this.graph().existsVertexLabel(entity.label())) {
            throw new HugeException("Schema is missing for %s '%s'",
                                    entity.label(), entity.id());
        }
        return this.tx().constructVertex(false, entity.asArray());
    }

    private void commitOrRollback() {
        Boolean autoCommit = this.autoCommit.get();
        if (autoCommit != null && !autoCommit) {
            return;
        }
        this.tx().commitOrRollback();
    }

    public void autoCommit(boolean value) {
        this.autoCommit.set(value);
    }
}
