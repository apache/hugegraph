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

package org.apache.hugegraph.backend.store.mysql;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.serializer.TableBackendEntry;
import org.apache.hugegraph.backend.serializer.TableSerializerV2;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.structure.HugeProperty;
import org.apache.hugegraph.structure.HugeVectorIndexMap;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.JsonUtil;

public class MysqlSerializer extends TableSerializerV2 {

    public MysqlSerializer(HugeConfig config) {
        super(config);
    }

    @Override
    public MysqlBackendEntry newBackendEntry(HugeType type, Id id) {
        return new MysqlBackendEntry(type, id);
    }

    @Override
    protected TableBackendEntry newBackendEntry(TableBackendEntry.Row row) {
        return new MysqlBackendEntry(row);
    }

    @Override
    protected MysqlBackendEntry convertEntry(BackendEntry backendEntry) {
        if (!(backendEntry instanceof MysqlBackendEntry)) {
            throw new BackendException("Not supported by MysqlSerializer");
        }
        return (MysqlBackendEntry) backendEntry;
    }

    @Override
    protected Set<Object> parseIndexElemIds(TableBackendEntry entry) {
        Set<Object> elemIds = InsertionOrderUtil.newSet();
        elemIds.add(entry.column(HugeKeys.ELEMENT_IDS));
        for (TableBackendEntry.Row row : entry.subRows()) {
            elemIds.add(row.column(HugeKeys.ELEMENT_IDS));
        }
        return elemIds;
    }

    @Override
    protected Id toId(Number number) {
        return IdGenerator.of(number.longValue());
    }

    @Override
    protected Id[] toIdArray(Object object) {
        assert object instanceof String;
        String value = (String) object;
        Number[] values = JsonUtil.fromJson(value, Number[].class);
        Id[] ids = new Id[values.length];
        int i = 0;
        for (Number number : values) {
            ids[i++] = IdGenerator.of(number.longValue());
        }
        return ids;
    }

    @Override
    protected Object toLongSet(Collection<Id> ids) {
        return this.toLongList(ids);
    }

    @Override
    protected Object toLongList(Collection<Id> ids) {
        long[] values = new long[ids.size()];
        int i = 0;
        for (Id id : ids) {
            values[i++] = id.asLong();
        }
        return JsonUtil.toJson(values);
    }

    @Override
    protected void formatProperty(HugeProperty<?> prop,
                                  TableBackendEntry.Row row) {
        throw new BackendException("Not support updating single property " +
                                   "by MySQL");
    }

    @Override
    protected void formatProperties(HugeElement element,
                                    TableBackendEntry.Row row) {
        Map<Number, Object> properties = new HashMap<>();
        // Add all properties of a Vertex
        for (HugeProperty<?> prop : element.getProperties()) {
            Number key = prop.propertyKey().id().asLong();
            Object val = prop.value();
            properties.put(key, val);
        }
        row.column(HugeKeys.PROPERTIES, JsonUtil.toJson(properties));
    }

    @Override
    protected void parseProperties(HugeElement element,
                                   TableBackendEntry.Row row) {
        String properties = row.column(HugeKeys.PROPERTIES);
        // Query edge will wrapped by a vertex, whose properties is empty
        if (properties.isEmpty()) {
            return;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> props = JsonUtil.fromJson(properties, Map.class);
        for (Map.Entry<String, Object> prop : props.entrySet()) {
            /*
             * The key is string instead of int, because the key in json
             * must be string
             */
            Id pkeyId = this.toId(Long.valueOf(prop.getKey()));
            String colJson = JsonUtil.toJson(prop.getValue());
            this.parseProperty(pkeyId, colJson, element);
        }
    }

    @Override
    protected void writeUserdata(SchemaElement schema,
                                 TableBackendEntry entry) {
        assert entry instanceof MysqlBackendEntry;
        entry.column(HugeKeys.USER_DATA, JsonUtil.toJson(schema.userdata()));
    }

    @Override
    protected void readUserdata(SchemaElement schema,
                                TableBackendEntry entry) {
        assert entry instanceof MysqlBackendEntry;
        // Parse all user data of a schema element
        String json = entry.column(HugeKeys.USER_DATA);
        @SuppressWarnings("unchecked")
        Map<String, Object> userdata = JsonUtil.fromJson(json, Map.class);
        for (Map.Entry<String, Object> e : userdata.entrySet()) {
            schema.userdata(e.getKey(), e.getValue());
        }
    }

    @Override
    public BackendEntry writeOlapVertex(HugeVertex vertex) {
        throw new NotImplementedException("Unsupported writeOlapVertex()");
    }

    @Override
    public BackendEntry writeVectorSequence(HugeVectorIndexMap indexMap) {
        throw new NotImplementedException("Unsupported writeVectorSequence()");
    }

    @Override
    public HugeVectorIndexMap readVectorSequence(HugeGraph graph, ConditionQuery query,
                                                 BackendEntry entry) {
        throw new NotImplementedException("Unsupported readVectorSequence()");
    }
}
