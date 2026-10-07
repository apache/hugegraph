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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.query.Condition.RelationType;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.KryoException;
import org.apache.tinkerpop.shaded.kryo.Registration;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.apache.tinkerpop.shaded.kryo.util.DefaultClassResolver;
import org.apache.tinkerpop.shaded.kryo.util.IntMap;
import org.apache.tinkerpop.shaded.kryo.util.MapReferenceResolver;
import org.apache.tinkerpop.shaded.kryo.util.ObjectMap;

public final class KryoUtil {

    private static final ThreadLocal<Kryo> KRYOS = new ThreadLocal<>();

    public static Kryo kryo() {
        Kryo kryo = KRYOS.get();
        if (kryo != null) {
            return kryo;
        }

        kryo = new Kryo(new LegacyClassResolver(), new MapReferenceResolver());
        registerSerializers(kryo);
        KRYOS.set(kryo);
        return kryo;
    }

    public static byte[] toKryo(Object value) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             Output output = new Output(bos, 256)) {
            kryo().writeObject(output, value);
            output.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new BackendException("Failed to serialize: %s", e, value);
        }
    }

    public static <T> T fromKryo(byte[] value, Class<T> clazz) {
        E.checkState(value != null,
                     "Kryo value can't be null for '%s'",
                     clazz.getSimpleName());
        return kryo().readObject(new Input(value), clazz);
    }

    public static byte[] toKryoWithType(Object value) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             Output output = new Output(bos, 256)) {
            kryo().writeClassAndObject(output, value);
            output.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new BackendException("Failed to serialize: %s", e, value);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T fromKryoWithType(byte[] value) {
        E.checkState(value != null, "Kryo value can't be null for object");
        return (T) kryo().readClassAndObject(new Input(value));
    }

    /** Resolve relocated names only when Kryo decodes a class descriptor. */
    private static class LegacyClassResolver extends DefaultClassResolver {

        private static final String LEGACY_RELATION =
                "org.apache.hugegraph.backend.query.Condition$RelationType";
        private final Map<Integer, Registration> legacyRelations = new HashMap<>();

        @Override
        protected Registration readName(Input input) {
            // Kryo shaded by Gremlin 3.5.1 names contain a varint ID and, on first use, a string.
            // Consume once: streamed Input may refill, making position rewind unsafe.
            int nameId = input.readVarInt(true);
            if (this.nameIdToClass == null) {
                this.nameIdToClass = new IntMap<>();
            }
            Class<?> type = this.nameIdToClass.get(nameId);
            String name = null;
            if (type == null) {
                name = input.readString();
                type = this.getTypeByName(name);
                if (type == null) {
                    try {
                        type = Class.forName(name, false, this.kryo.getClassLoader());
                    } catch (ClassNotFoundException e) {
                        throw new KryoException("Unable to find class: " + name, e);
                    }
                    if (this.nameToClass == null) {
                        this.nameToClass = new ObjectMap<>();
                    }
                    this.nameToClass.put(name, type);
                }
                this.nameIdToClass.put(nameId, type);
            }
            Registration registration = this.kryo.getRegistration(type);
            Registration legacy = this.legacyRelations.get(nameId);
            if (legacy == null && name != null) {
                Serializer<?> serializer = null;
                if (LEGACY_RELATION.equals(name)) {
                    serializer = new LegacyRelationSerializer();
                } else if (name.startsWith("[") && name.endsWith("L" + LEGACY_RELATION + ";")) {
                    serializer = new LegacyRelationArraySerializer(registration.getType());
                }
                if (serializer != null) {
                    // Never register this globally: canonical descriptors use other ordinals.
                    legacy = new Registration(registration.getType(), serializer, NAME);
                    this.legacyRelations.put(nameId, legacy);
                }
            }
            return legacy != null ? legacy : registration;
        }

        @Override
        public void reset() {
            super.reset();
            this.legacyRelations.clear();
        }

        @Override
        protected Class<?> getTypeByName(String name) {
            Class<?> type = LegacyClassNames.lookup(name);
            return type != null ? type : super.getTypeByName(name);
        }
    }

    /** The frozen core enum order differs from the existing shared enum order. */
    private static class LegacyRelationSerializer extends Serializer<RelationType> {

        private static final RelationType[] VALUES = {
                RelationType.EQ, RelationType.GT, RelationType.GTE, RelationType.LT,
                RelationType.LTE, RelationType.NEQ, RelationType.IN, RelationType.NOT_IN,
                RelationType.PREFIX, RelationType.TEXT_CONTAINS, RelationType.TEXT_CONTAINS_ANY,
                RelationType.CONTAINS, RelationType.CONTAINS_VALUE, RelationType.CONTAINS_KEY,
                RelationType.SCAN
        };

        private LegacyRelationSerializer() {
            this.setAcceptsNull(true);
            this.setImmutable(true);
        }

        @Override
        public RelationType read(Kryo kryo, Input input, Class<RelationType> type) {
            int ordinal = input.readVarInt(true);
            if (ordinal == 0) {
                return null;
            }
            if (ordinal < 0 || ordinal > VALUES.length) {
                throw new KryoException("Invalid legacy relation ordinal: " + ordinal);
            }
            return VALUES[ordinal - 1];
        }

        @Override
        public void write(Kryo kryo, Output output, RelationType value) {
            kryo.getSerializer(RelationType.class).write(kryo, output, value);
        }
    }

    /** Typed enum arrays omit element descriptors, so retain the enclosing descriptor's codec. */
    private static class LegacyRelationArraySerializer extends Serializer<Object[]> {

        private final Serializer<?> componentSerializer;

        private LegacyRelationArraySerializer(Class<?> type) {
            this.setAcceptsNull(true);
            Class<?> component = type.getComponentType();
            this.componentSerializer = component == RelationType.class ? new LegacyRelationSerializer() :
                                       new LegacyRelationArraySerializer(component);
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Object[] read(Kryo kryo, Input input, Class<Object[]> type) {
            int length = input.readVarInt(true);
            if (length == 0) {
                return null;
            }
            Class<?> component = type.getComponentType();
            Object[] values = (Object[]) Array.newInstance(component, length - 1);
            kryo.reference(values);
            for (int i = 0; i < values.length; i++) {
                values[i] = kryo.readObjectOrNull(input, component, (Serializer) this.componentSerializer);
            }
            return values;
        }

        @Override
        public void write(Kryo kryo, Output output, Object[] values) {
            kryo.getSerializer(values.getClass()).write(kryo, output, values);
        }
    }

    private static void registerSerializers(Kryo kryo) {
        kryo.addDefaultSerializer(UUID.class, new Serializer<UUID>() {

            @Override
            public UUID read(Kryo kryo, Input input, Class<UUID> c) {
                return new UUID(input.readLong(), input.readLong());
            }

            @Override
            public void write(Kryo kryo, Output output, UUID uuid) {
                output.writeLong(uuid.getMostSignificantBits());
                output.writeLong(uuid.getLeastSignificantBits());
            }
        });
    }
}
