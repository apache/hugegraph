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

package org.apache.hugegraph.util;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.backend.Shard;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.id.IdUtil;
import org.apache.hugegraph.id.SplicingIdGenerator;
import org.apache.hugegraph.query.Aggregate;
import org.apache.hugegraph.query.BatchConditionQuery;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.query.IdPrefixQuery;
import org.apache.hugegraph.query.IdQuery;
import org.apache.hugegraph.query.IdRangeQuery;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.query.serializer.AbstractSerializerAdapter;
import org.apache.hugegraph.query.serializer.QueryAdapter;
import org.apache.hugegraph.query.serializer.QueryIdAdapter;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.SchemaElement;
import org.apache.hugegraph.struct.schema.SchemaLabel;
import org.apache.hugegraph.struct.schema.Userdata;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.struct.schema.builder.SchemaBuilder;
import org.apache.hugegraph.type.GraphType;
import org.apache.hugegraph.type.Indexfiable;
import org.apache.hugegraph.type.Namifiable;
import org.apache.hugegraph.type.Propfiable;
import org.apache.hugegraph.type.Typifiable;

/** Exact relocated class descriptors accepted by legacy JSON and Kryo decoders. */
public final class LegacyClassNames {

    private static final Map<String, Class<?>> LEGACY_CLASSES = legacyClasses();

    private LegacyClassNames() {
    }

    /** Return an exact migrated type, or null so the caller retains its normal loader. */
    public static Class<?> lookup(String name) {
        Class<?> type = LEGACY_CLASSES.get(name);
        if (type != null) {
            return type;
        }
        // Object arrays use JVM descriptors; keep dimensions and remap
        // only an exact, known component name. Primitive arrays use the
        // unchanged default resolver.
        int dimensions = 0;
        while (dimensions < name.length() && name.charAt(dimensions) == '[') {
            dimensions++;
            if (dimensions > 255) {
                return null;
            }
        }
        if (dimensions > 0 && dimensions < name.length() &&
            name.charAt(dimensions) == 'L' && name.endsWith(";")) {
            type = LEGACY_CLASSES.get(name.substring(dimensions + 1, name.length() - 1));
            if (type != null) {
                return Array.newInstance(type, new int[dimensions]).getClass();
            }
        }
        return null;
    }

    private static Class<?> declaredClass(Class<?> owner, String name) {
        for (Class<?> type : owner.getDeclaredClasses()) {
            if (type.getSimpleName().equals(name)) {
                return type;
            }
        }
        throw new IllegalStateException("Missing legacy target: " + owner.getName() + "$" + name);
    }

    private static Map<String, Class<?>> legacyClasses() {
        Map<String, Class<?>> classes = new HashMap<>();
        classes.put("org.apache.hugegraph.backend.id.Id", Id.class);
        classes.put("org.apache.hugegraph.backend.id.IdGenerator", IdGenerator.class);
        classes.put("org.apache.hugegraph.backend.id.IdUtil", IdUtil.class);
        classes.put("org.apache.hugegraph.backend.id.SplicingIdGenerator", SplicingIdGenerator.class);
        classes.put("org.apache.hugegraph.backend.serializer.BytesBuffer", BytesBuffer.class);
        classes.put("org.apache.hugegraph.type.Typeable", Typifiable.class);
        classes.put("org.apache.hugegraph.type.Nameable", Namifiable.class);
        classes.put("org.apache.hugegraph.type.Propertiable", Propfiable.class);
        classes.put("org.apache.hugegraph.type.Indexable", Indexfiable.class);
        classes.put("org.apache.hugegraph.backend.id.Id$IdType", Id.IdType.class);
        classes.put("org.apache.hugegraph.backend.id.IdGenerator$StringId", IdGenerator.StringId.class);
        classes.put("org.apache.hugegraph.backend.id.IdGenerator$LongId", IdGenerator.LongId.class);
        classes.put("org.apache.hugegraph.backend.id.IdGenerator$UuidId", IdGenerator.UuidId.class);
        classes.put("org.apache.hugegraph.backend.id.IdGenerator$ObjectId", IdGenerator.ObjectId.class);
        classes.put("org.apache.hugegraph.backend.id.EdgeId", EdgeId.class);
        classes.put("org.apache.hugegraph.backend.serializer.BinaryBackendEntry$BinaryId", BinaryId.class);
        classes.put("org.apache.hugegraph.backend.store.BackendEntry$BackendColumn", BackendColumn.class);
        classes.put("org.apache.hugegraph.backend.store.Shard", Shard.class);
        classes.put("org.apache.hugegraph.HugeException", HugeException.class);
        classes.put("org.apache.hugegraph.backend.BackendException", BackendException.class);
        classes.put("org.apache.hugegraph.structure.GraphType", GraphType.class);
        classes.put("org.apache.hugegraph.schema.SchemaElement", SchemaElement.class);
        classes.put("org.apache.hugegraph.schema.SchemaLabel", SchemaLabel.class);
        classes.put("org.apache.hugegraph.schema.PropertyKey", PropertyKey.class);
        classes.put("org.apache.hugegraph.schema.VertexLabel", VertexLabel.class);
        classes.put("org.apache.hugegraph.schema.EdgeLabel", EdgeLabel.class);
        classes.put("org.apache.hugegraph.schema.IndexLabel", IndexLabel.class);
        classes.put("org.apache.hugegraph.schema.Userdata", Userdata.class);
        classes.put("org.apache.hugegraph.schema.SchemaElement$TaskWithSchema", SchemaElement.TaskWithSchema.class);
        classes.put("org.apache.hugegraph.schema.SchemaElement$P", SchemaElement.P.class);
        classes.put("org.apache.hugegraph.schema.PropertyKey$P", declaredClass(PropertyKey.class, "P"));
        classes.put("org.apache.hugegraph.schema.VertexLabel$P", declaredClass(VertexLabel.class, "P"));
        classes.put("org.apache.hugegraph.schema.EdgeLabel$P", declaredClass(EdgeLabel.class, "P"));
        classes.put("org.apache.hugegraph.schema.IndexLabel$P", declaredClass(IndexLabel.class, "P"));
        classes.put("org.apache.hugegraph.schema.PropertyKey$Builder", PropertyKey.Builder.class);
        classes.put("org.apache.hugegraph.schema.VertexLabel$Builder", VertexLabel.Builder.class);
        classes.put("org.apache.hugegraph.schema.EdgeLabel$Builder", EdgeLabel.Builder.class);
        classes.put("org.apache.hugegraph.schema.IndexLabel$Builder", IndexLabel.Builder.class);
        classes.put("org.apache.hugegraph.structure.HugeIndex", Index.class);
        classes.put("org.apache.hugegraph.structure.HugeIndex$IdWithExpiredTime", Index.IdWithExpiredTime.class);
        classes.put("org.apache.hugegraph.schema.builder.SchemaBuilder", SchemaBuilder.class);
        classes.put("org.apache.hugegraph.backend.query.serializer.AbstractSerializerAdapter",
                    AbstractSerializerAdapter.class);
        classes.put("org.apache.hugegraph.backend.query.serializer.QueryAdapter", QueryAdapter.class);
        classes.put("org.apache.hugegraph.backend.query.serializer.QueryIdAdapter", QueryIdAdapter.class);
        classes.put("org.apache.hugegraph.backend.query.Query", Query.class);
        classes.put("org.apache.hugegraph.backend.query.Query$Order", Query.Order.class);
        classes.put("org.apache.hugegraph.backend.query.Query$OrderType", Query.OrderType.class);
        classes.put("org.apache.hugegraph.backend.query.IdQuery", IdQuery.class);
        classes.put("org.apache.hugegraph.backend.query.IdQuery$OneIdQuery", IdQuery.OneIdQuery.class);
        classes.put("org.apache.hugegraph.backend.query.IdPrefixQuery", IdPrefixQuery.class);
        classes.put("org.apache.hugegraph.backend.query.IdRangeQuery", IdRangeQuery.class);
        classes.put("org.apache.hugegraph.backend.query.ConditionQuery", ConditionQuery.class);
        classes.put("org.apache.hugegraph.backend.query.ConditionQuery$OptimizedType",
                    ConditionQuery.OptimizedType.class);
        classes.put("org.apache.hugegraph.backend.query.ConditionQuery$Element2IndexValueMap",
                    ConditionQuery.Element2IndexValueMap.class);
        classes.put("org.apache.hugegraph.backend.query.ConditionQuery$LeftIndex", ConditionQuery.LeftIndex.class);
        classes.put("org.apache.hugegraph.backend.query.BatchConditionQuery", BatchConditionQuery.class);
        classes.put("org.apache.hugegraph.backend.query.Aggregate", Aggregate.class);
        classes.put("org.apache.hugegraph.backend.query.Aggregate$AggregateFunc", Aggregate.AggregateFunc.class);
        classes.put("org.apache.hugegraph.backend.query.Condition", Condition.class);
        classes.put("org.apache.hugegraph.backend.query.Condition$ConditionType", Condition.ConditionType.class);
        classes.put("org.apache.hugegraph.backend.query.Condition$RelationType", Condition.RelationType.class);
        classes.put("org.apache.hugegraph.backend.query.Condition$BinCondition", Condition.BinCondition.class);
        classes.put("org.apache.hugegraph.backend.query.Condition$And", Condition.And.class);
        classes.put("org.apache.hugegraph.backend.query.Condition$Or", Condition.Or.class);
        classes.put("org.apache.hugegraph.backend.query.Condition$Not", Condition.Not.class);
        classes.put("org.apache.hugegraph.backend.query.Condition$Relation", Condition.Relation.class);
        classes.put("org.apache.hugegraph.backend.query.Condition$SyspropRelation", Condition.SyspropRelation.class);
        classes.put("org.apache.hugegraph.backend.query.Condition$FlattenSyspropRelation",
                    Condition.FlattenSyspropRelation.class);
        classes.put("org.apache.hugegraph.backend.query.Condition$UserpropRelation", Condition.UserpropRelation.class);
        classes.put("org.apache.hugegraph.backend.query.Condition$RangeConditions", Condition.RangeConditions.class);
        return Collections.unmodifiableMap(classes);
    }

}
