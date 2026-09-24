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

package org.apache.hugegraph.backend.id;

import java.util.Objects;
import java.util.UUID;

import org.apache.hugegraph.backend.id.Id.IdType;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.LongEncoding;
import org.apache.hugegraph.util.NumericUtil;
import org.apache.hugegraph.util.StringEncoding;

public abstract class IdGenerator {

    public static final Id ZERO = IdGenerator.of(0L);

    public abstract Id generate(HugeVertex vertex);

    public static Id of(String id) {
        return new StringId(id);
    }

    public static Id of(UUID id) {
        return new UuidId(id);
    }

    public static Id of(String id, boolean uuid) {
        return uuid ? new UuidId(id) : new StringId(id);
    }

    public static Id of(long id) {
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

    public static Id of(byte[] bytes, IdType type) {
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

    public static Id ofStoredString(String id, IdType type) {
        switch (type) {
            case LONG:
                return of(LongEncoding.decodeSignedB64(id));
            case UUID:
                byte[] bytes = StringEncoding.decodeBase64(id);
                return of(bytes, IdType.UUID);
            case STRING:
                return of(id);
            default:
                throw new AssertionError("Invalid id type " + type);
        }
    }

    public static String asStoredString(Id id) {
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

    public static IdType idType(Id id) {
        if (id instanceof LongId) {
            return IdType.LONG;
        }
        if (id instanceof UuidId) {
            return IdType.UUID;
        }
        if (id instanceof StringId) {
            return IdType.STRING;
        }
        if (id instanceof EdgeId) {
            return IdType.EDGE;
        }
        return IdType.UNKNOWN;
    }

    public static int compareType(Id id1, Id id2) {
        return idType(id1).ordinal() - idType(id2).ordinal();
    }

    /****************************** id defines ******************************/

    public static class StringId implements Id {

        protected String id;
        protected byte[] bytes;

        public StringId(String id) {
            E.checkArgument(id != null && !id.isEmpty(),
                            "The id can't be null or empty");
            this.id = id;
            this.bytes = null;
        }

        public StringId(byte[] bytes) {
            E.checkArgument(bytes != null && bytes.length > 0,
                            "The id bytes can't be null or empty");
            this.bytes = bytes;
            this.id = null;
        }

        @Override
        public IdType type() {
            return IdType.STRING;
        }

        @Override
        public Object asObject() {
            return this.asString();
        }

        @Override
        public String asString() {
            if (this.id == null) {
                assert this.bytes != null;
                this.id = StringEncoding.decode(this.bytes);
            }
            return this.id;
        }

        @Override
        public long asLong() {
            return Long.parseLong(this.asString());
        }

        @Override
        public byte[] asBytes() {
            if (this.bytes == null) {
                assert this.id != null;
                this.bytes = StringEncoding.encode(this.id);
            }
            return this.bytes;
        }

        @Override
        public int length() {
            return this.asString().length();
        }

        @Override
        public int compareTo(Id other) {
            int cmp = compareType(this, other);
            if (cmp != 0) {
                return cmp;
            }
            if (this.id != null) {
                return this.id.compareTo(other.asString());
            } else {
                return Bytes.compare(this.bytes, other.asBytes());
            }
        }

        @Override
        public int hashCode() {
            return this.asString().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof StringId)) {
                return false;
            }
            StringId other = (StringId) obj;
            if (this.id != null) {
                return this.id.equals(other.asString());
            } else if (other.bytes == null) {
                return this.asString().equals(other.asString());
            } else {
                assert this.bytes != null;
                return Bytes.equals(this.bytes, other.asBytes());
            }
        }

        @Override
        public String toString() {
            return this.asString();
        }
    }

    public static class LongId extends Number implements Id {

        private static final long serialVersionUID = -7732461469037400190L;

        protected Long id;

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
            return NumericUtil.longToBytes(this.id);
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
                return false;
            }
            return this.id == ((Number) other).longValue();
        }

        @Override
        public String toString() {
            return String.valueOf(this.id);
        }

        @Override
        public int intValue() {
            return this.id.intValue();
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

    public static class UuidId implements Id {

        protected UUID uuid;

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
    public static class ObjectId implements Id {

        protected Object object;

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
