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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.Id.IdType;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.StringEncoding;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

public class HugeGryoModule {

    private static GraphSONSchemaSerializer schemaSerializer =
            new GraphSONSchemaSerializer();

    public static void register(HugeGraphIoRegistry io) {
        io.register(GryoIo.class, Optional.class, new OptionalSerializer());

        // HugeGraph id serializer
        io.register(GryoIo.class, IdGenerator.StringId.class,
                    new IdSerializer());
        io.register(GryoIo.class, IdGenerator.LongId.class,
                    new IdSerializer());
        io.register(GryoIo.class, EdgeId.class, new EdgeIdSerializer());

        // HugeGraph schema serializer
        io.register(GryoIo.class, PropertyKey.class,
                    new PropertyKeyKryoSerializer());
        io.register(GryoIo.class, VertexLabel.class,
                    new VertexLabelKryoSerializer());
        io.register(GryoIo.class, EdgeLabel.class,
                    new EdgeLabelKryoSerializer());
        io.register(GryoIo.class, IndexLabel.class,
                    new IndexLabelKryoSerializer());
    }

    static class OptionalSerializer extends Serializer<Optional<?>> {

        @Override
        public void write(Kryo kryo, Output output, Optional<?> optional) {
            if (optional.isPresent()) {
                kryo.writeClassAndObject(output, optional.get());
            } else {
                kryo.writeObject(output, null);
            }
        }

        @Override
        public Optional<?> read(Kryo kryo, Input input, Class<Optional<?>> c) {
            Object value = kryo.readClassAndObject(input);
            return value == null ? Optional.empty() : Optional.of(value);
        }
    }

    static class IdSerializer extends Serializer<Id> {

        @Override
        public void write(Kryo kryo, Output output, Id id) {
            output.writeByte(id.type().ordinal());
            byte[] idBytes = id.asBytes();
            output.write(idBytes.length);
            output.writeBytes(id.asBytes());
        }

        @Override
        public Id read(Kryo kryo, Input input, Class<Id> clazz) {
            int type = input.readByteUnsigned();
            int length = input.read();
            byte[] idBytes = input.readBytes(length);
            return IdGenerator.of(idBytes, IdType.values()[type]);
        }
    }

    static class EdgeIdSerializer extends Serializer<EdgeId> {

        @Override
        public void write(Kryo kryo, Output output, EdgeId edgeId) {
            byte[] idBytes = edgeId.asBytes();
            output.write(idBytes.length);
            output.writeBytes(edgeId.asBytes());
        }

        @Override
        public EdgeId read(Kryo kryo, Input input, Class<EdgeId> clazz) {
            int length = input.read();
            byte[] idBytes = input.readBytes(length);
            return EdgeId.parse(StringEncoding.decode(idBytes));
        }
    }

    private static void writeEntry(Kryo kryo,
                                   Output output,
                                   Map<HugeKeys, Object> schema) {
        /* Write columns size and data */
        output.writeInt(schema.keySet().size());
        for (Map.Entry<HugeKeys, Object> entry : schema.entrySet()) {
            kryo.writeObject(output, entry.getKey());
            kryo.writeClassAndObject(output, entry.getValue());
        }
    }

    @SuppressWarnings("unused")
    private static Map<HugeKeys, Object> readEntry(Kryo kryo, Input input) {
        int columnSize = input.readInt();
        Map<HugeKeys, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < columnSize; i++) {
            HugeKeys key = kryo.readObject(input, HugeKeys.class);
            Object val = kryo.readClassAndObject(input);
            map.put(key, val);
        }
        return map;
    }

    static class PropertyKeyKryoSerializer extends Serializer<PropertyKey> {

        @Override
        public void write(Kryo kryo, Output output, PropertyKey pk) {
            writeEntry(kryo, output, schemaSerializer.writePropertyKey(pk));
        }

        @Override
        public PropertyKey read(Kryo kryo, Input input, Class<PropertyKey> c) {
            // TODO
            return null;
        }
    }

    static class VertexLabelKryoSerializer extends Serializer<VertexLabel> {

        @Override
        public void write(Kryo kryo, Output output, VertexLabel vl) {
            writeEntry(kryo, output, schemaSerializer.writeVertexLabel(vl));
        }

        @Override
        public VertexLabel read(Kryo kryo, Input input, Class<VertexLabel> c) {
            // TODO
            return null;
        }
    }

    static class EdgeLabelKryoSerializer extends Serializer<EdgeLabel> {

        @Override
        public void write(Kryo kryo, Output output, EdgeLabel el) {
            writeEntry(kryo, output, schemaSerializer.writeEdgeLabel(el));
        }

        @Override
        public EdgeLabel read(Kryo kryo, Input input, Class<EdgeLabel> clazz) {
            // TODO
            return null;
        }
    }

    static class IndexLabelKryoSerializer extends Serializer<IndexLabel> {

        @Override
        public void write(Kryo kryo, Output output, IndexLabel il) {
            writeEntry(kryo, output, schemaSerializer.writeIndexLabel(il));
        }

        @Override
        public IndexLabel read(Kryo kryo, Input input, Class<IndexLabel> c) {
            // TODO
            return null;
        }
    }
}
