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
import org.apache.hugegraph.auth.SchemaDefine.Relationship;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;

import com.google.common.collect.ImmutableMap;

public class RelationshipManager<T extends Relationship> {

    private final HugeGraphParams graph;
    private final String label;
    private final Function<Edge, T> deser;
    private final ThreadLocal<Boolean> autoCommit = new ThreadLocal<>();

    private static final long NO_LIMIT = -1L;

    public RelationshipManager(HugeGraphParams graph, String label,
                               Function<Edge, T> deser) {
        E.checkNotNull(graph, "graph");

        this.graph = graph;
        this.label = label;
        this.deser = deser;
        this.autoCommit.set(true);
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

    public Id add(T relationship) {
        E.checkArgumentNotNull(relationship, "Relationship can't be null");
        return this.save(relationship, false);
    }

    public Id update(T relationship) {
        E.checkArgumentNotNull(relationship, "Relationship can't be null");
        relationship.onUpdate();
        return this.save(relationship, true);
    }

    public T delete(Id id) {
        T relationship = null;
        Iterator<Edge> edges = this.tx().queryEdges(id);
        if (edges.hasNext()) {
            HugeEdge edge = (HugeEdge) edges.next();
            relationship = this.deser.apply(edge);
            this.tx().removeEdge(edge);
            this.commitOrRollback();
            assert !edges.hasNext();
        }
        return relationship;
    }

    public T get(Id id) {
        T relationship = null;
        Iterator<Edge> edges = this.tx().queryEdges(id);
        if (edges.hasNext()) {
            relationship = this.deser.apply(edges.next());
            assert !edges.hasNext();
        }
        if (relationship == null) {
            throw new NotFoundException("Can't find %s with id '%s'",
                                        this.unhideLabel(), id);
        }
        return relationship;
    }

    public boolean exists(Id id) {
        Iterator<Edge> edges = this.tx().queryEdges(id);
        if (edges.hasNext()) {
            Edge edge = edges.next();
            return this.label.equals(edge.label());
        }
        return false;
    }

    public List<T> list(List<Id> ids) {
        return toList(this.queryById(ids));
    }

    public List<T> list(long limit) {
        Iterator<Edge> edges = this.queryRelationship(null, null, this.label,
                                                      ImmutableMap.of(), limit);
        return toList(edges);
    }

    public List<T> list(Id source, Directions direction,
                        String label, long limit) {
        Iterator<Edge> edges = this.queryRelationship(source, direction, label,
                                                      ImmutableMap.of(), limit);
        return toList(edges);
    }

    public List<T> list(Id source, Directions direction, String label,
                        String key, Object value, long limit) {
        Map<String, Object> conditions = ImmutableMap.of(key, value);
        Iterator<Edge> edges = this.queryRelationship(source, direction, label,
                                                      conditions, limit);
        return toList(edges);
    }

    protected List<T> toList(Iterator<Edge> edges) {
        Iterator<T> iter = new MapperIterator<>(edges, this.deser);
        // Convert iterator to list to avoid across thread tx accessed
        return (List<T>) QueryResults.toList(iter).list();
    }

    private Iterator<Edge> queryById(List<Id> ids) {
        Object[] idArray = ids.toArray(new Id[0]);
        return this.tx().queryEdges(idArray);
    }

    private Iterator<Edge> queryRelationship(Id source,
                                             Directions direction,
                                             String label,
                                             Map<String, Object> conditions,
                                             long limit) {
        ConditionQuery query = new ConditionQuery(HugeType.EDGE);
        EdgeLabel el = this.graph().edgeLabel(label);
        if (direction == null) {
            direction = Directions.OUT;
        }
        if (source != null) {
            query.eq(HugeKeys.OWNER_VERTEX, source);
            query.eq(HugeKeys.DIRECTION, direction);
        }
        if (label != null) {
            query.eq(HugeKeys.LABEL, el.id());
        }
        for (Map.Entry<String, Object> entry : conditions.entrySet()) {
            PropertyKey pk = this.graph().propertyKey(entry.getKey());
            query.query(Condition.eq(pk.id(), entry.getValue()));
        }
        query.showHidden(true);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        Iterator<Edge> edges = this.tx().queryEdges(query);
        if (limit == NO_LIMIT) {
            return edges;
        }
        long[] size = new long[1];
        return new MapperIterator<>(edges, edge -> {
            if (++size[0] > limit) {
                return null;
            }
            return edge;
        });
    }

    private Id save(T relationship, boolean expectExists) {
        if (!this.graph().existsEdgeLabel(relationship.label())) {
            throw new HugeException("Schema is missing for %s '%s'",
                                    relationship.label(),
                                    relationship.source());
        }
        HugeVertex source = this.newVertex(relationship.source(),
                                           relationship.sourceLabel());
        HugeVertex target = this.newVertex(relationship.target(),
                                           relationship.targetLabel());
        HugeEdge edge = source.constructEdge(relationship.label(), target,
                                             relationship.asArray());
        E.checkArgument(this.exists(edge.id()) == expectExists,
                        "Can't save %s '%s' that %s exists",
                        this.unhideLabel(), edge.id(),
                        expectExists ? "not" : "already");

        this.tx().addEdge(edge);
        this.commitOrRollback();
        return edge.id();
    }

    private HugeVertex newVertex(Object id, String label) {
        VertexLabel vl = this.graph().vertexLabel(label);
        Id idValue = HugeVertex.getIdValue(id);
        return HugeVertex.create(this.tx(), idValue, vl);
    }

    private void commitOrRollback() {
        Boolean autoCommit = this.autoCommit.get();
        if (autoCommit != null && !autoCommit) {
            return;
        }
        this.tx().commitOrRollback();
    }

    public void autoCommit(boolean value) {
        autoCommit.set(value);
    }
}
