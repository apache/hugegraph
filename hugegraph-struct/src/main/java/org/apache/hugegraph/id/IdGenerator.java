/*
 * Copyright 2017 HugeGraph Authors
 *
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

package org.apache.hugegraph.id;

import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.util.StringEncoding;
import com.google.common.primitives.Longs;

import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.LongEncoding;
import org.apache.hugegraph.util.NumericUtil;

import java.nio.charset.Charset;
import java.util.Objects;
import java.util.UUID;

public abstract class IdGenerator {

    public static final Id ZERO = IdGenerator.of(0L);

    public abstract Id generate(BaseVertex vertex);

    public final static Id of(String id) {
        return new StringId(id);
    }

    public final static Id of(UUID id) {
        return new UuidId(id);
    }

    public final static Id of(String id, boolean uuid) {
        return uuid ? new UuidId(id) : new StringId(id);
    }

    public final static Id of(long id) {
        return new LongId(id);
    }

    public static Id of(Object id) {
        if (id instanceof Id) {
            return (Id) id;
        } else if (id instanceof String) {
            return of((String) id);
        } else if (id instanceof Number) {
            return of(((Number) id).longValue());
        } else if (id instanceof UUID) {
            return of((UUID) id);
        }
        return new ObjectId(id);
    }

    public final static Id of(byte[] bytes, Id.IdType type) {
        switch (type) {
            case LONG:
                return new LongId(bytes);
            case UUID:
                return new UuidId(bytes);
            case STRING:
                return new StringId(bytes);
            default:
                throw new AssertionError("Invalid id type " + type);
        }
    }

    public final static Id ofStoredString(String id, Id.IdType type) {
        switch (type) {
            case LONG:
                return of(LongEncoding.decodeSignedB64(id));
            case UUID:
                byte[] bytes = StringEncoding.decodeBase64(id);
                return of(bytes, Id.IdType.UUID);
            case STRING:
                return of(id);
            default:
                throw new AssertionError("Invalid id type " + type);
        }
    }

    public final static String asStoredString(Id id) {
        switch (id.type()) {
            case LONG:
                return LongEncoding.encodeSignedB64(id.asLong());
            case UUID:
                return StringEncoding.encodeBase64(id.asBytes());
            case STRING:
                return id.asString();
            default:
                throw new AssertionError("Invalid id type " + id.type());
        }
    }

    public final static Id.IdType idType(Id id) {
        if (id instanceof LongId) {
            return Id.IdType.LONG;
        }
        if (id instanceof UuidId) {
            return Id.IdType.UUID;
        }
        if (id instanceof StringId) {
            return Id.IdType.STRING;
        }
        if (id instanceof EdgeId) {
            return Id.IdType.EDGE;
        }
        return Id.IdType.UNKNOWN;
    }

    private final static int compareType(Id id1, Id id2) {
        return idType(id1).ordinal() - idType(id2).ordinal();
    }

    /****************************** id defines ******************************/

    public static final class StringId implements Id {

        private final String id;
        private static final Charset CHARSET = Charset.forName("UTF-8");

        public StringId(String id) {
            E.checkArgument(!id.isEmpty(), "The id can't be empty");
            this.id = id;
        }

        public StringId(byte[] bytes) {
            this.id = StringEncoding.decode(bytes);
        }

        @Override
        public IdType type() {
            return IdType.STRING;
        }

        @Override
        public Object asObject() {
            return this.id;
        }

        @Override
        public String asString() {
            return this.id;
        }

        @Override
        public long asLong() {
            return Long.parseLong(this.id);
        }

        @Override
        public byte[] asBytes() {
            return this.id.getBytes(CHARSET);
        }

        @Override
        public int length() {
            return this.id.length();
        }

        @Override
        public int compareTo(Id other) {
            int cmp = compareType(this, other);
            if (cmp != 0) {
                return cmp;
            }
            return this.id.compareTo(other.asString());
        }

        @Override
        public int hashCode() {
            return this.id.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof StringId)) {
                return false;
            }
            return this.id.equals(((StringId) other).id);
        }

        @Override
        public String toString() {
            return this.id;
        }
    }

    public static final class LongId extends Number implements Id {

        private static final long serialVersionUID = -7732461469037400190L;

        private final long id;

        public LongId(long id) {
            this.id = id;
        }

        public LongId(byte[] bytes) {
            this.id = NumericUtil.bytesToLong(bytes);
        }

        @Override
        public IdType type() {
            return IdType.LONG;
        }

        @Override
        public Object asObject() {
            return this.id;
        }

        @Override
        public String asString() {
            // TODO: encode with base64
            return Long.toString(this.id);
        }

        @Override
        public long asLong() {
            return this.id;
        }

        @Override
        public byte[] asBytes() {
            return Longs.toByteArray(this.id);
            // return NumericUtil.longToBytes(this.id);
        }

        @Override
        public int length() {
            return Long.BYTES;
        }

        @Override
        public int compareTo(Id other) {
            int cmp = compareType(this, other);
            if (cmp != 0) {
                return cmp;
            }
            return Long.compare(this.id, other.asLong());
        }

        @Override
        public int hashCode() {
            return Long.hashCode(this.id);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Number)) {
                if (idDigitalObject(other)) {
                    return this.id == (long) Double.parseDouble(other.toString());
                }
                return false;
            }
            return this.id == ((Number) other).longValue();
        }

        private static boolean idDigitalObject(Object object) {
            String string = object.toString();
            for (int i = string.length(); --i >= 0; ) {
                char c = string.charAt(i);
                if (!Character.isDigit(c) &&
                    '.' != c) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            return String.valueOf(this.id);
        }

        @Override
        public int intValue() {
            return (int) this.id;
        }

        @Override
        public long longValue() {
            return this.id;
        }

        @Override
        public float floatValue() {
            return this.id;
        }

        @Override
        public double doubleValue() {
            return this.id;
        }
    }

    public static final class UuidId implements Id {

        private final UUID uuid;

        public UuidId(String string) {
            this(StringEncoding.uuid(string));
        }

        public UuidId(byte[] bytes) {
            this(fromBytes(bytes));
        }

        public UuidId(UUID uuid) {
            E.checkArgument(uuid != null, "The uuid can't be null");
            this.uuid = uuid;
        }

        @Override
        public IdType type() {
            return IdType.UUID;
        }

        @Override
        public Object asObject() {
            return this.uuid;
        }

        @Override
        public String asString() {
            return this.uuid.toString();
        }

        @Override
        public long asLong() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] asBytes() {
            BytesBuffer buffer = BytesBuffer.allocate(16);
            buffer.writeLong(this.uuid.getMostSignificantBits());
            buffer.writeLong(this.uuid.getLeastSignificantBits());
            return buffer.bytes();
        }

        private static UUID fromBytes(byte[] bytes) {
            E.checkArgument(bytes != null, "The UUID can't be null");
            BytesBuffer buffer = BytesBuffer.wrap(bytes);
            long high = buffer.readLong();
            long low = buffer.readLong();
            return new UUID(high, low);
        }

        @Override
        public int length() {
            return UUID_LENGTH;
        }

        @Override
        public int compareTo(Id other) {
            E.checkNotNull(other, "compare id");
            int cmp = compareType(this, other);
            if (cmp != 0) {
                return cmp;
            }
            return this.uuid.compareTo(((UuidId) other).uuid);
        }

        @Override
        public int hashCode() {
            return this.uuid.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof UuidId)) {
                return false;
            }
            return this.uuid.equals(((UuidId) other).uuid);
        }

        @Override
        public String toString() {
            return this.uuid.toString();
        }
    }

    /**
     * This class is just used by backend store for wrapper object as Id
     */
    public static final class ObjectId implements Id {

        private final Object object;

        public ObjectId(Object object) {
            E.checkNotNull(object, "object");
            this.object = object;
        }

        @Override
        public IdType type() {
            return IdType.UNKNOWN;
        }

        @Override
        public Object asObject() {
            return this.object;
        }

        @Override
        public String asString() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long asLong() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] asBytes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int length() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(Id o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int hashCode() {
            return this.object.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ObjectId)) {
                return false;
            }
            return Objects.equals(this.object, ((ObjectId) other).object);
        }

        @Override
        public String toString() {
            return this.object.toString();
        }
    }
}
