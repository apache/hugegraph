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

package org.apache.hugegraph.variables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.SplicingIdGenerator;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.iterator.FilterIterator;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;

public class HugeVariables implements Graph.Variables {

    private static final Logger LOG = Log.logger(HugeVariables.class);

    // Variables vertex label
    private static final String VARIABLES = "variables";

    // Variables properties
    private static final String VARIABLE_KEY = "varKey";
    private static final String VARIABLE_TYPE = "varType";

    private static final String BYTE_VALUE = "B";
    private static final String BOOLEAN_VALUE = "Z";
    private static final String INTEGER_VALUE = "I";
    private static final String LONG_VALUE = "L";
    private static final String FLOAT_VALUE = "F";
    private static final String DOUBLE_VALUE = "D";
    private static final String STRING_VALUE = "S";

    // Variables properties suffix
    private static final String LIST = "L";
    private static final String SET = "S";

    private static final String[] TYPES = {
            Hidden.hide(BYTE_VALUE),
            Hidden.hide(BOOLEAN_VALUE),
            Hidden.hide(INTEGER_VALUE),
            Hidden.hide(LONG_VALUE),
            Hidden.hide(FLOAT_VALUE),
            Hidden.hide(DOUBLE_VALUE),
            Hidden.hide(STRING_VALUE),
            Hidden.hide(BYTE_VALUE + LIST),
            Hidden.hide(BOOLEAN_VALUE + LIST),
            Hidden.hide(INTEGER_VALUE + LIST),
            Hidden.hide(LONG_VALUE + LIST),
            Hidden.hide(FLOAT_VALUE + LIST),
            Hidden.hide(DOUBLE_VALUE + LIST),
            Hidden.hide(STRING_VALUE + LIST),
            Hidden.hide(BYTE_VALUE + SET),
            Hidden.hide(BOOLEAN_VALUE + SET),
            Hidden.hide(INTEGER_VALUE + SET),
            Hidden.hide(LONG_VALUE + SET),
            Hidden.hide(FLOAT_VALUE + SET),
            Hidden.hide(DOUBLE_VALUE + SET),
            Hidden.hide(STRING_VALUE + SET)
    };

    private final HugeGraphParams params;

    public HugeVariables(HugeGraphParams params) {
        this.params = params;
    }

    private static Object extractSingleObject(Object value) {
        if (value instanceof List || value instanceof Set) {
            Collection<?> collection = (Collection<?>) value;
            if (collection.isEmpty()) {
                return null;
            }
            value = collection.iterator().next();
        }
        return value;
    }

    public synchronized void initSchemaIfNeeded() {
        if (this.params.graph().existsVertexLabel(Hidden.hide(VARIABLES))) {
            // Ignore if exist
            return;
        }

        createPropertyKey(Hidden.hide(VARIABLE_KEY), DataType.TEXT,
                          Cardinality.SINGLE);
        createPropertyKey(Hidden.hide(VARIABLE_TYPE), DataType.TEXT,
                          Cardinality.SINGLE);
        createPropertyKey(Hidden.hide(BYTE_VALUE), DataType.BYTE,
                          Cardinality.SINGLE);
        createPropertyKey(Hidden.hide(BOOLEAN_VALUE), DataType.BOOLEAN,
                          Cardinality.SINGLE);
        createPropertyKey(Hidden.hide(INTEGER_VALUE), DataType.INT,
                          Cardinality.SINGLE);
        createPropertyKey(Hidden.hide(LONG_VALUE), DataType.LONG,
                          Cardinality.SINGLE);
        createPropertyKey(Hidden.hide(FLOAT_VALUE), DataType.FLOAT,
                          Cardinality.SINGLE);
        createPropertyKey(Hidden.hide(DOUBLE_VALUE), DataType.DOUBLE,
                          Cardinality.SINGLE);
        createPropertyKey(Hidden.hide(STRING_VALUE), DataType.TEXT,
                          Cardinality.SINGLE);
        createPropertyKey(Hidden.hide(BYTE_VALUE + LIST),
                          DataType.BYTE, Cardinality.LIST);
        createPropertyKey(Hidden.hide(BOOLEAN_VALUE + LIST),
                          DataType.BOOLEAN, Cardinality.LIST);
        createPropertyKey(Hidden.hide(INTEGER_VALUE + LIST),
                          DataType.INT, Cardinality.LIST);
        createPropertyKey(Hidden.hide(LONG_VALUE + LIST),
                          DataType.LONG, Cardinality.LIST);
        createPropertyKey(Hidden.hide(FLOAT_VALUE + LIST),
                          DataType.FLOAT, Cardinality.LIST);
        createPropertyKey(Hidden.hide(DOUBLE_VALUE + LIST),
                          DataType.DOUBLE, Cardinality.LIST);
        createPropertyKey(Hidden.hide(STRING_VALUE + LIST),
                          DataType.TEXT, Cardinality.LIST);
        createPropertyKey(Hidden.hide(BYTE_VALUE + SET),
                          DataType.BYTE, Cardinality.SET);
        createPropertyKey(Hidden.hide(BOOLEAN_VALUE + SET),
                          DataType.BOOLEAN, Cardinality.SET);
        createPropertyKey(Hidden.hide(INTEGER_VALUE + SET),
                          DataType.INT, Cardinality.SET);
        createPropertyKey(Hidden.hide(LONG_VALUE + SET),
                          DataType.LONG, Cardinality.SET);
        createPropertyKey(Hidden.hide(FLOAT_VALUE + SET),
                          DataType.FLOAT, Cardinality.SET);
        createPropertyKey(Hidden.hide(DOUBLE_VALUE + SET),
                          DataType.DOUBLE, Cardinality.SET);
        createPropertyKey(Hidden.hide(STRING_VALUE + SET),
                          DataType.TEXT, Cardinality.SET);

        String[] properties = {Hidden.hide(VARIABLE_KEY),
                               Hidden.hide(VARIABLE_TYPE)};
        properties = ArrayUtils.addAll(properties, TYPES);

        SchemaManager schema = this.params.graph().schema();
        VertexLabel variables = schema.vertexLabel(Hidden.hide(VARIABLES))
                                      .properties(properties)
                                      .usePrimaryKeyId()
                                      .primaryKeys(Hidden.hide(VARIABLE_KEY))
                                      .nullableKeys(TYPES)
                                      .build();
        this.params.schemaTransaction().addVertexLabel(variables);

        LOG.debug("Variables schema created");
    }

    private void createPropertyKey(String name, DataType dataType,
                                   Cardinality cardinality) {
        SchemaManager schema = this.params.graph().schema();
        PropertyKey propertyKey = schema.propertyKey(name)
                                        .dataType(dataType)
                                        .cardinality(cardinality)
                                        .build();
        this.params.schemaTransaction().addPropertyKey(propertyKey);
    }

    @Override
    public Set<String> keys() {
        Iterator<Vertex> vertices = this.queryAllVariableVertices();
        try {
            Set<String> keys = new HashSet<>();
            while (vertices.hasNext()) {
                keys.add(vertices.next().value(Hidden.hide(VARIABLE_KEY)));
                Query.checkForceCapacity(keys.size());
            }
            return keys;
        } finally {
            CloseableIterator.closeIterator(vertices);
        }
    }

    @Override
    public <R> Optional<R> get(String key) {
        if (key == null) {
            throw Graph.Variables.Exceptions.variableKeyCanNotBeNull();
        }
        if (key.isEmpty()) {
            throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty();
        }

        Vertex vertex = this.queryVariableVertex(key);
        if (vertex == null) {
            return Optional.empty();
        }

        String type = vertex.value(Hidden.hide(VARIABLE_TYPE));
        if (!Arrays.asList(TYPES).contains(Hidden.hide(type))) {
            throw Graph.Variables.Exceptions
                    .dataTypeOfVariableValueNotSupported(type);
        }
        // The value of key VARIABLE_TYPE is the name of variable value
        return Optional.of(vertex.value(Hidden.hide(type)));
    }

    @Override
    public void set(String key, Object value) {
        if (key == null) {
            throw Graph.Variables.Exceptions.variableKeyCanNotBeNull();
        }
        if (key.isEmpty()) {
            throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty();
        }
        if (value == null) {
            throw Graph.Variables.Exceptions.variableValueCanNotBeNull();
        }

        this.createVariableVertex(key, value);
    }

    @Override
    public void remove(String key) {
        if (key == null) {
            throw Graph.Variables.Exceptions.variableKeyCanNotBeNull();
        }
        if (key.isEmpty()) {
            throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty();
        }
        HugeVertex vertex = this.queryVariableVertex(key);
        if (vertex != null) {
            this.removeVariableVertex(vertex);
        }
    }

    @Override
    public Map<String, Object> asMap() {
        Iterator<Vertex> vertices = this.queryAllVariableVertices();
        try {
            Map<String, Object> variables = new HashMap<>();
            while (vertices.hasNext()) {
                Vertex vertex = vertices.next();
                String key = vertex.value(Hidden.hide(VARIABLE_KEY));
                String type = vertex.value(Hidden.hide(VARIABLE_TYPE));
                if (!Arrays.asList(TYPES).contains(Hidden.hide(type))) {
                    throw Graph.Variables.Exceptions
                            .dataTypeOfVariableValueNotSupported(type);
                }
                Object value = vertex.value(Hidden.hide(type));
                variables.put(key, value);
                Query.checkForceCapacity(variables.size());
            }
            return Collections.unmodifiableMap(variables);
        } finally {
            CloseableIterator.closeIterator(vertices);
        }
    }

    public <R> List<Optional<R>> mget(String... keys) {
        Query.checkForceCapacity(keys.length);
        List<String> nameList = new ArrayList<>();

        for (String key : keys) {
            if (!StringUtils.isEmpty(key)) {
                nameList.add(key);
            }
        }

        Map<String, Optional<R>> map = new HashMap<>();
        Iterator<Vertex> vertices = EmptyIterator.INSTANCE;
        try {
            vertices = this.batchQueryVariableVertices(nameList);
            while (vertices.hasNext()) {
                Vertex v = vertices.next();
                String type = v.value(Hidden.hide(VARIABLE_TYPE));
                map.put(v.value(Hidden.hide(VARIABLE_KEY)),
                        Optional.of(v.value(Hidden.hide(type))));
            }
        } finally {
            CloseableIterator.closeIterator(vertices);
        }

        List<Optional<R>> list = new ArrayList<>();
        for (String key : keys) {
            Optional<R> value = map.get(key);
            if (value == null) {
                list.add(Optional.empty());
            } else {
                list.add(value);
            }
        }
        return list;
    }

    public Number count() {
        ConditionQuery cq = new ConditionQuery(HugeType.TASK);
        cq.eq(HugeKeys.LABEL, this.variableVertexLabel().id());
        return this.params.graph().queryNumber(cq);
    }

    public Iterator<Vertex> queryVariablesByShard(String start, String end, String page,
                                                  long pageLimit) {
        ConditionQuery query = this.createVariableShardQuery(start, end, page, pageLimit);
        GraphTransaction tx = this.params.graphTransaction();
        Iterator<Vertex> vertices = EmptyIterator.INSTANCE;
        Iterator filter = EmptyIterator.INSTANCE;
        try {
            vertices = tx.queryVertices(query);
            VertexLabel vl = this.variableVertexLabel();
            filter = new FilterIterator(vertices, (v) -> {
                if (((HugeVertex) v).schemaLabel().id() == vl.id()) {
                    return true;
                }
                return false;
            });

            return filter;
        } catch (Exception e) {
            LOG.error("Failed to query variables by shard", e);
            throw e;
        } finally {
            CloseableIterator.closeIterator(vertices);
            CloseableIterator.closeIterator(filter);
        }
    }

    private ConditionQuery createVariableShardQuery(String start, String end, String page,
                                                    long pageLimit) {
        ConditionQuery query = new ConditionQuery(HugeType.TASK);
        query.scan(start, end);
        query.page(page);
        if (query.paging()) {
            query.limit(pageLimit);
        }
        query.showHidden(true);
        return query;
    }

    private Iterator<Vertex> batchQueryVariableVertices(List<String> nameList) {
        GraphTransaction tx = this.params.graphTransaction();
        List<Id> query = this.constructId(nameList);
        Iterator<Vertex> vertices = tx.queryTaskInfos(query.toArray());
        return vertices;
    }

    private List<Id> constructId(List<String> nameList) {
        VertexLabel vl = this.variableVertexLabel();
        List<Id> queryIdList = new ArrayList<>();
        for (String name : nameList) {
            queryIdList.add(
                    SplicingIdGenerator.splicing(vl.id().asString(), name));
        }
        return queryIdList;
    }

    @Override
    public String toString() {
        return StringFactory.graphVariablesString(this);
    }

    private void setProperty(HugeVertex vertex, String key, Object value) {
        String suffix;
        if (value instanceof List) {
            suffix = LIST;
        } else if (value instanceof Set) {
            suffix = SET;
        } else {
            suffix = "";
        }

        vertex.property(Hidden.hide(VARIABLE_KEY), key);
        Object object = HugeVariables.extractSingleObject(value);
        if (object == null) {
            vertex.property(Hidden.hide(STRING_VALUE + suffix), value);
            vertex.property(Hidden.hide(VARIABLE_TYPE), STRING_VALUE + suffix);
            return;
        }

        if (object instanceof Byte) {
            vertex.property(Hidden.hide(BYTE_VALUE + suffix), value);
            vertex.property(Hidden.hide(VARIABLE_TYPE), BYTE_VALUE + suffix);
        } else if (object instanceof Boolean) {
            vertex.property(Hidden.hide(BOOLEAN_VALUE + suffix), value);
            vertex.property(Hidden.hide(VARIABLE_TYPE),
                            BOOLEAN_VALUE + suffix);
        } else if (object instanceof Integer) {
            vertex.property(Hidden.hide(INTEGER_VALUE + suffix), value);
            vertex.property(Hidden.hide(VARIABLE_TYPE),
                            INTEGER_VALUE + suffix);
        } else if (object instanceof Long) {
            vertex.property(Hidden.hide(LONG_VALUE + suffix), value);
            vertex.property(Hidden.hide(VARIABLE_TYPE), LONG_VALUE + suffix);
        } else if (object instanceof Float) {
            vertex.property(Hidden.hide(FLOAT_VALUE + suffix), value);
            vertex.property(Hidden.hide(VARIABLE_TYPE), FLOAT_VALUE + suffix);
        } else if (object instanceof Double) {
            vertex.property(Hidden.hide(DOUBLE_VALUE + suffix), value);
            vertex.property(Hidden.hide(VARIABLE_TYPE), DOUBLE_VALUE + suffix);
        } else if (object instanceof String) {
            vertex.property(Hidden.hide(STRING_VALUE + suffix), value);
            vertex.property(Hidden.hide(VARIABLE_TYPE), STRING_VALUE + suffix);
        } else {
            throw Graph.Variables.Exceptions
                    .dataTypeOfVariableValueNotSupported(value);
        }
    }

    private void createVariableVertex(String key, Object value) {
        VertexLabel vl = this.variableVertexLabel();
        GraphTransaction tx = this.params.graphTransaction();

        HugeVertex vertex = HugeVertex.create(tx, null, vl);
        try {
            this.setProperty(vertex, key, value);
        } catch (IllegalArgumentException e) {
            throw Graph.Variables.Exceptions
                    .dataTypeOfVariableValueNotSupported(value, e);
        }
        // PrimaryKey id
        vertex.assignId(null);

        tx.addVertex(vertex);
    }

    private void removeVariableVertex(HugeVertex vertex) {
        this.params.graphTransaction().removeVertex(vertex);
    }

    private HugeVertex queryVariableVertex(String key) {
        GraphTransaction tx = this.params.graphTransaction();
        Query query = this.createVariableQuery(key);
        Iterator<Vertex> vertices = tx.queryVertices(query);
        return (HugeVertex) QueryResults.one(vertices);
    }

    private Iterator<Vertex> queryAllVariableVertices() {
        GraphTransaction tx = this.params.graphTransaction();
        Query query = this.createVariableQuery(null);
        Iterator<Vertex> vertices = tx.queryVertices(query);
        return vertices;
    }

    private ConditionQuery createVariableQuery(String name) {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        VertexLabel vl = this.variableVertexLabel();
        query.eq(HugeKeys.LABEL, vl.id());
        if (name != null) {
            PropertyKey pkey = this.params.graph().propertyKey(
                    Hidden.hide(VARIABLE_KEY));
            query.query(Condition.eq(pkey.id(), name));
        }
        query.showHidden(true);
        return query;
    }

    private VertexLabel variableVertexLabel() {
        return this.params.graph().vertexLabel(Hidden.hide(VARIABLES));
    }
}
