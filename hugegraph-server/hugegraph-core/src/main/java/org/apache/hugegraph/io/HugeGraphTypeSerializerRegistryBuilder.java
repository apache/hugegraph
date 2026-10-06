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

package org.apache.hugegraph.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.store.Shard;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.Blob;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.SimpleTypeSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.TransformSerializer;

import groovy.lang.GString;

public final class HugeGraphTypeSerializerRegistryBuilder
        extends TypeSerializerRegistry.Builder {

    private static final TypeSerializer<Id> ID_TRANSFORM_SERIALIZER =
            new WireTransformSerializer<>(Id::asObject);
    private static final TypeSerializer<Blob> BLOB_TRANSFORM_SERIALIZER =
            new WireTransformSerializer<>(blob -> ByteBuffer.wrap(blob.bytes()));
    private static final TypeSerializer<Shard> SHARD_TRANSFORM_SERIALIZER =
            new WireTransformSerializer<>(HugeGraphTypeSerializerRegistryBuilder::shardMap);
    private static final TypeSerializer<SchemaElement> SCHEMA_TRANSFORM_SERIALIZER =
            new WireTransformSerializer<>(HugeGraphTypeSerializerRegistryBuilder::schemaMap);
    private static final TypeSerializer<SchemaManager> SCHEMA_MANAGER_TRANSFORM_SERIALIZER =
            new WireTransformSerializer<>(HugeGraphTypeSerializerRegistryBuilder::schemaManagerMap);
    private static final TypeSerializer<Enum<?>> ENUM_TRANSFORM_SERIALIZER =
            new WireTransformSerializer<>(Enum::name);

    private static final TypeSerializer<Optional<?>> OPTIONAL_TRANSFORM_SERIALIZER =
            new WireTransformSerializer<>(optional -> optional.orElse(null));
    private static final TypeSerializer<File> FILE_TRANSFORM_SERIALIZER =
            new WireTransformSerializer<>(file -> Map.of("file", file.getName()));

    private static final TypeSerializer<GString> GSTRING_TRANSFORM_SERIALIZER =
            new WireTransformSerializer<>(GString::toString);

    public HugeGraphTypeSerializerRegistryBuilder() {
        this.withFallbackResolver(type -> {
            if (Id.class.isAssignableFrom(type)) {
                return ID_TRANSFORM_SERIALIZER;
            }
            if (Blob.class.isAssignableFrom(type)) {
                return BLOB_TRANSFORM_SERIALIZER;
            }
            if (Shard.class.isAssignableFrom(type)) {
                return SHARD_TRANSFORM_SERIALIZER;
            }
            if (SchemaElement.class.isAssignableFrom(type)) {
                return SCHEMA_TRANSFORM_SERIALIZER;
            }
            if (SchemaManager.class.isAssignableFrom(type)) {
                return SCHEMA_MANAGER_TRANSFORM_SERIALIZER;
            }
            if (isHugeGraphEnum(type)) {
                return ENUM_TRANSFORM_SERIALIZER;
            }
            if (Optional.class.isAssignableFrom(type)) {
                return OPTIONAL_TRANSFORM_SERIALIZER;
            }
            if (File.class.isAssignableFrom(type)) {
                return FILE_TRANSFORM_SERIALIZER;
            }
            if (GString.class.isAssignableFrom(type)) {
                return GSTRING_TRANSFORM_SERIALIZER;
            }
            return null;
        });
    }

    private static boolean isHugeGraphEnum(Class<?> type) {
        // Scope the fallback to HugeGraph; native TinkerPop enums keep their serializers.
        return Enum.class.isAssignableFrom(type) &&
               type.getName().startsWith("org.apache.hugegraph.");
    }

    private static Map<String, Object> shardMap(Shard shard) {
        // Match the existing GraphSON contract, including open range boundaries.
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("start", shard.start());
        result.put("end", shard.end());
        result.put("length", shard.length());
        return result;
    }

    private static Map<String, Object> schemaManagerMap(SchemaManager schema) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("propertykeys", schemaValue(schema.getPropertyKeys()));
        result.put("vertexlabels", schemaValue(schema.getVertexLabels()));
        result.put("edgelabels", schemaValue(schema.getEdgeLabels()));
        result.put("indexlabels", schemaValue(schema.getIndexLabels()));
        return result;
    }

    private static Map<String, Object> schemaMap(SchemaElement schema) {
        // Use the same field names and values as the existing GraphSON schema API.
        GraphSONSchemaSerializer serializer = new GraphSONSchemaSerializer();
        Map<HugeKeys, Object> fields;
        if (schema instanceof PropertyKey) {
            fields = serializer.writePropertyKey((PropertyKey) schema);
        } else if (schema instanceof VertexLabel) {
            fields = serializer.writeVertexLabel((VertexLabel) schema);
        } else if (schema instanceof EdgeLabel) {
            fields = serializer.writeEdgeLabel((EdgeLabel) schema);
        } else if (schema instanceof IndexLabel) {
            fields = serializer.writeIndexLabel((IndexLabel) schema);
        } else {
            throw new IllegalArgumentException("Unsupported schema type: " + schema.getClass());
        }
        Map<String, Object> result = new LinkedHashMap<>();
        fields.forEach((key, value) -> result.put(key.string(), schemaValue(value)));
        return result;
    }

    private static Object schemaValue(Object value) {
        if (value instanceof Enum && isHugeGraphEnum(value.getClass())) {
            return ((Enum<?>) value).name();
        }
        if (value instanceof SchemaElement) {
            return schemaMap((SchemaElement) value);
        }
        if (value instanceof SchemaManager) {
            return schemaManagerMap((SchemaManager) value);
        }
        if (value instanceof Map) {
            Map<Object, Object> result = new LinkedHashMap<>();
            ((Map<?, ?>) value).forEach((key, item) -> result.put(schemaValue(key), schemaValue(item)));
            return result;
        }
        if (value instanceof Collection) {
            List<Object> result = new ArrayList<>(((Collection<?>) value).size());
            for (Object item : (Collection<?>) value) {
                result.add(schemaValue(item));
            }
            return result;
        }
        return value;
    }

    private static final class WireTransformSerializer<T>
            extends SimpleTypeSerializer<T>
            implements TransformSerializer<T> {

        private final Function<T, Object> transform;

        private WireTransformSerializer(Function<T, Object> transform) {
            super(null);
            this.transform = transform;
        }

        @Override
        protected T readValue(Buffer buffer, GraphBinaryReader context)
                throws IOException {
            throw new IOException("HugeGraph value is written as a standard wire value");
        }

        @Override
        protected void writeValue(T value, Buffer buffer,
                                  GraphBinaryWriter context)
                throws IOException {
            throw new IOException("HugeGraph value is written as a standard wire value");
        }

        @Override
        public Object transform(T value) {
            return this.transform.apply(value);
        }
    }
}
