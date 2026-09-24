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

package org.apache.hugegraph.serializer;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.UUID;

import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.Id.IdType;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.SerialEnum;
import org.apache.hugegraph.util.Blob;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.StringEncoding;

/**
 * class BytesBuffer is a util for read/write binary
 */
public class BytesBuffer extends OutputStream {

    public static final int BYTE_LEN = Byte.BYTES;
    public static final int SHORT_LEN = Short.BYTES;
    public static final int INT_LEN = Integer.BYTES;
    public static final int LONG_LEN = Long.BYTES;
    public static final int CHAR_LEN = Character.BYTES;
    public static final int FLOAT_LEN = Float.BYTES;
    public static final int DOUBLE_LEN = Double.BYTES;
    public static final int BLOB_LEN = 4;

    public static final int UINT8_MAX = ((byte) -1) & 0xff;
    public static final int UINT16_MAX = ((short) -1) & 0xffff;
    public static final long UINT32_MAX = (-1) & 0xffffffffL;
    public static final long WRITE_BYTES_MAX_LENGTH = 10 * Bytes.MB;

    // NOTE: +1 to let code 0 represent length 1
    public static final int ID_LEN_MAX = 0x7fff + 1;
    public static final int BIG_ID_LEN_MAX = 0xfffff + 1;

    public static final byte STRING_ENDING_BYTE = (byte) 0x00;
    public static final byte STRING_ENDING_BYTE_FF = (byte) 0xff;
    public static final int STRING_LEN_MAX = UINT16_MAX;
    public static final long BLOB_LEN_MAX = Bytes.GB;

    // The value must be in range [8, ID_LEN_MAX]
    public static final int INDEX_HASH_ID_THRESHOLD = 32;

    public static final int DEFAULT_CAPACITY = 64;
    public static final int MAX_BUFFER_CAPACITY = 128 * 1024 * 1024; // 128M

    public static final int BUF_EDGE_ID = 128;
    public static final int BUF_PROPERTY = 64;

    private ByteBuffer buffer;
    private final boolean resize;

    public BytesBuffer() {
        this(DEFAULT_CAPACITY);
    }

    public BytesBuffer(int capacity) {
        E.checkArgument(capacity <= MAX_BUFFER_CAPACITY,
                        "Capacity exceeds max buffer capacity: %s",
                        MAX_BUFFER_CAPACITY);
        this.buffer = ByteBuffer.allocate(capacity);
        this.resize = true;
    }

    public BytesBuffer(ByteBuffer buffer) {
        E.checkNotNull(buffer, "buffer");
        this.buffer = buffer;
        this.resize = false;
    }

    public static BytesBuffer allocate(int capacity) {
        return new BytesBuffer(capacity);
    }

    public static BytesBuffer wrap(ByteBuffer buffer) {
        return new BytesBuffer(buffer);
    }

    public static BytesBuffer wrap(byte[] array) {
        return new BytesBuffer(ByteBuffer.wrap(array));
    }

    public static BytesBuffer wrap(byte[] array, int offset, int length) {
        return new BytesBuffer(ByteBuffer.wrap(array, offset, length));
    }

    public ByteBuffer asByteBuffer() {
        return this.buffer;
    }

    public BytesBuffer forReadWritten() {
        this.buffer.flip();
        return this;
    }

    public BytesBuffer forReadAll() {
        this.buffer.position(this.buffer.limit());
        return this;
    }

    public byte[] array() {
        return this.buffer.array();
    }

    public byte[] bytes() {
        byte[] bytes = this.buffer.array();
        int position = this.buffer.position();
        if (position == bytes.length) {
            return bytes;
        } else {
            return Arrays.copyOf(bytes, position);
        }
    }

    public int position() {
        return this.buffer.position();
    }

    public BytesBuffer copyFrom(BytesBuffer other) {
        this.write(other.bytes());
        return this;
    }

    public int remaining() {
        return this.buffer.remaining();
    }

    private void require(int size) {
        // Does need to resize?
        if (this.buffer.limit() - this.buffer.position() >= size) {
            return;
        }
        // Can't resize for wrapped buffer since will change the origin ref
        E.checkState(this.resize, "Can't resize for wrapped buffer");

        // Extra capacity as buffer
        int newcapacity = size + this.buffer.limit() + DEFAULT_CAPACITY;
        E.checkArgument(newcapacity <= MAX_BUFFER_CAPACITY,
                        "Capacity exceeds max buffer capacity: %s",
                        MAX_BUFFER_CAPACITY);
        ByteBuffer newBuffer = ByteBuffer.allocate(newcapacity);
        this.buffer.flip();
        newBuffer.put(this.buffer);
        this.buffer = newBuffer;
    }

    public BytesBuffer write(byte val) {
        require(BYTE_LEN);
        this.buffer.put(val);
        return this;
    }

    @Override
    public void write(int val) {
        assert val <= UINT8_MAX;
        require(BYTE_LEN);
        this.buffer.put((byte) val);
    }

    @Override
    public void write(byte[] val) {
        require(BYTE_LEN * val.length);
        this.buffer.put(val);
    }

    @Override
    public void write(byte[] val, int offset, int length) {
        require(BYTE_LEN * length);
        this.buffer.put(val, offset, length);
    }

    public BytesBuffer writeBoolean(boolean val) {
        this.write(val ? 1 : 0);
        return this;
    }

    public BytesBuffer writeChar(char val) {
        require(CHAR_LEN);
        this.buffer.putChar(val);
        return this;
    }

    public BytesBuffer writeShort(short val) {
        require(SHORT_LEN);
        this.buffer.putShort(val);
        return this;
    }

    public BytesBuffer writeInt(int val) {
        require(INT_LEN);
        this.buffer.putInt(val);
        return this;
    }

    public BytesBuffer writeLong(long val) {
        require(LONG_LEN);
        this.buffer.putLong(val);
        return this;
    }

    public BytesBuffer writeFloat(float val) {
        require(FLOAT_LEN);
        this.buffer.putFloat(val);
        return this;
    }

    public BytesBuffer writeDouble(double val) {
        require(DOUBLE_LEN);
        this.buffer.putDouble(val);
        return this;
    }

    public byte peek() {
        return this.buffer.get(this.buffer.position());
    }

    public byte peekLast() {
        return this.buffer.get(this.buffer.capacity() - 1);
    }

    public byte read() {
        return this.buffer.get();
    }

    public byte[] read(int length) {
        byte[] bytes = new byte[length];
        this.buffer.get(bytes);
        return bytes;
    }

    public byte[] readToEnd() {
        byte[] bytes = new byte[this.remaining()];
        this.buffer.get(bytes);
        return bytes;
    }

    public boolean readBoolean() {
        return this.buffer.get() != 0;
    }

    public char readChar() {
        return this.buffer.getChar();
    }

    public short readShort() {
        return this.buffer.getShort();
    }

    public int readInt() {
        return this.buffer.getInt();
    }

    public long readLong() {
        return this.buffer.getLong();
    }

    public float readFloat() {
        return this.buffer.getFloat();
    }

    public double readDouble() {
        return this.buffer.getDouble();
    }

    public BytesBuffer writeBytes(byte[] bytes) {
        // Original limit as above, consider this limit may be due to performance considerations when multiple storage backends are used.
        // The above limit will cause errors when writing value to property exceeds the limit. So adjust size to 5M
        E.checkArgument(bytes.length <= WRITE_BYTES_MAX_LENGTH,
                        "The max length of bytes is %s, but got %s",
                        WRITE_BYTES_MAX_LENGTH, bytes.length);
        require(SHORT_LEN + bytes.length);
        this.writeVInt(bytes.length);
        this.write(bytes);
        return this;
    }

    public byte[] readBytes() {
        int length = this.readVInt();
        assert length >= 0;
        byte[] bytes = this.read(length);
        return bytes;
    }

    public BytesBuffer writeBigBytes(byte[] bytes) {
        E.checkArgument(bytes.length <= BLOB_LEN_MAX,
                        "The max length of bytes is %s, but got %s",
                        BLOB_LEN_MAX, bytes.length);
        require(BLOB_LEN + bytes.length);
        this.writeVInt(bytes.length);
        this.write(bytes);
        return this;
    }

    public byte[] readBigBytes() {
        int length = this.readVInt();
        assert length >= 0;
        byte[] bytes = this.read(length);
        return bytes;
    }

    public BytesBuffer writeStringRaw(String val) {
        this.write(StringEncoding.encode(val));
        return this;
    }

    public BytesBuffer writeString(String val) {
        byte[] bytes = StringEncoding.encode(val);
        this.writeBytes(bytes);
        return this;
    }

    public String readString() {
        return StringEncoding.decode(this.readBytes());
    }

    public BytesBuffer writeStringWithEnding(String value) {
        if (!value.isEmpty()) {
            byte[] bytes = StringEncoding.encode(value);
            /*
             * assert '0x00'/'0xFF' not exist in string index id
             * NOTE:
             *   0x00 is NULL in UTF8(or ASCII) bytes
             *   0xFF is not a valid byte in UTF8 bytes
             */
            assert !Bytes.contains(bytes, STRING_ENDING_BYTE_FF) :
                   "Invalid UTF8 bytes: " + value;
            if (Bytes.contains(bytes, STRING_ENDING_BYTE)) {
                E.checkArgument(false,
                                "Can't contains byte '0x00' in string: '%s'",
                                value);
            }
            this.write(bytes);
        }
        /*
         * Choose 0x00 as ending symbol (see #1057)
         * The following is out of date:
         * A reasonable ending symbol should be 0x00(to ensure order), but
         * considering that some backends like PG do not support 0x00 string,
         * so choose 0xFF currently.
         */
        this.write(STRING_ENDING_BYTE);
        return this;
    }

    public String readStringWithEnding() {
        return StringEncoding.decode(this.readBytesWithEnding());
    }
    public String skipBytesWithEnding(){
        boolean foundEnding = false;
        while (this.remaining() > 0) {
            byte current = this.read();
            if (current == STRING_ENDING_BYTE) {
                foundEnding = true;
                break;
            }
        }
        return "";
    }

    public BytesBuffer writeStringToRemaining(String value) {
        byte[] bytes = StringEncoding.encode(value);
        this.write(bytes);
        return this;
    }

    public String readStringFromRemaining() {
        byte[] bytes = new byte[this.buffer.remaining()];
        this.buffer.get(bytes);
        return StringEncoding.decode(bytes);
    }

    public BytesBuffer writeUInt8(int val) {
        assert val <= UINT8_MAX;
        this.write(val);
        return this;
    }

    public int readUInt8() {
        return this.read() & 0x000000ff;
    }

    public BytesBuffer writeUInt16(int val) {
        assert val <= UINT16_MAX;
        this.writeShort((short) val);
        return this;
    }

    public int readUInt16() {
        return this.readShort() & 0x0000ffff;
    }

    public BytesBuffer writeUInt32(long val) {
        assert val <= UINT32_MAX;
        this.writeInt((int) val);
        return this;
    }

    public long readUInt32() {
        return this.readInt() & 0xffffffffL;
    }

    public BytesBuffer writeVInt(int value) {
        // NOTE: negative numbers are not compressed
        if (value > 0x0fffffff || value < 0) {
            this.write(0x80 | ((value >>> 28) & 0x7f));
        }
        if (value > 0x1fffff || value < 0) {
            this.write(0x80 | ((value >>> 21) & 0x7f));
        }
        if (value > 0x3fff || value < 0) {
            this.write(0x80 | ((value >>> 14) & 0x7f));
        }
        if (value > 0x7f || value < 0) {
            this.write(0x80 | ((value >>>  7) & 0x7f));
        }
        this.write(value & 0x7f);

        return this;
    }

    public int readVInt() {
        byte leading = this.read();
        int value = leading & 0x7f;
        if (leading >= 0) {
            assert (leading & 0x80) == 0;
            return value;
        }

        int i = 1;
        for (; i < 5; i++) {
            byte b = this.read();
            if (b >= 0) {
                value = b | (value << 7);
                break;
            } else {
                value = (b & 0x7f) | (value << 7);
            }
        }

        return value;
    }

    public BytesBuffer writeVLong(long value) {
        if (value < 0) {
            this.write((byte) 0x81);
        }
        if (value > 0xffffffffffffffL || value < 0L) {
            this.write(0x80 | ((int) (value >>> 56) & 0x7f));
        }
        if (value > 0x1ffffffffffffL || value < 0L) {
            this.write(0x80 | ((int) (value >>> 49) & 0x7f));
        }
        if (value > 0x3ffffffffffL || value < 0L) {
            this.write(0x80 | ((int) (value >>> 42) & 0x7f));
        }
        if (value > 0x7ffffffffL || value < 0L) {
            this.write(0x80 | ((int) (value >>> 35) & 0x7f));
        }
        if (value > 0xfffffffL || value < 0L) {
            this.write(0x80 | ((int) (value >>> 28) & 0x7f));
        }
        if (value > 0x1fffffL || value < 0L) {
            this.write(0x80 | ((int) (value >>> 21) & 0x7f));
        }
        if (value > 0x3fffL || value < 0L) {
            this.write(0x80 | ((int) (value >>> 14) & 0x7f));
        }
        if (value > 0x7fL || value < 0L) {
            this.write(0x80 | ((int) (value >>>  7) & 0x7f));
        }
        this.write((int) value & 0x7f);

        return this;
    }

    public long readVLong() {
        byte leading = this.read();
        E.checkArgument(leading != 0x80,
                        "Unexpected varlong with leading byte '0x%s'",
                        Bytes.toHex(leading));
        long value = leading & 0x7fL;
        if (leading >= 0) {
            assert (leading & 0x80) == 0;
            return value;
        }

        int i = 1;
        for (; i < 10; i++) {
            byte b = this.read();
            if (b >= 0) {
                value = b | (value << 7);
                break;
            } else {
                value = (b & 0x7f) | (value << 7);
            }
        }

        E.checkArgument(i < 10,
                        "Unexpected varlong %s with too many bytes(%s)",
                        value, i + 1);
        E.checkArgument(i < 9 || (leading & 0x7e) == 0,
                        "Unexpected varlong %s with leading byte '0x%s'",
                        value, Bytes.toHex(leading));
        return value;
    }

    public <T> T newValue(Cardinality cardinality) {
        switch (cardinality) {
            case SET:
                return (T) new LinkedHashSet<>();
            case LIST:
                return (T) new ArrayList<>();
            default:
                // pass
                break;
        }
        return null;
    }

    private byte getCardinalityAndType(int cardinality, int type){
        return  (byte) ((cardinality << 6) | type);
    }

    public static byte getCardinality(int value){
        return (byte) ((value & 0xc0) >> 6);
    }

    public static byte getType(int value){
        return (byte) (value & 0x3f);
    }

    public BytesBuffer writeProperty(PropertyKey pkey, Object value) {
        return writeProperty(pkey.cardinality(), pkey.dataType(), value);
    }

    public BytesBuffer writeProperty(Cardinality cardinality, DataType dataType, Object value) {
        this.write(getCardinalityAndType(cardinality.code(),dataType.code()));
        if (cardinality == Cardinality.SINGLE) {
            this.writeProperty(dataType, value);
            return this;
        }
        assert cardinality == Cardinality.LIST ||
               cardinality == Cardinality.SET;
        Collection<?> values = (Collection<?>) value;
        this.writeVInt(values.size());
        for (Object o : values) {
            this.writeProperty(dataType, o);
        }
        return this;
    }

    public Object readProperty(PropertyKey propertyKey) {
        byte cardinalityAndType = this.read();
        Cardinality cardinality;
        DataType type;
        cardinality = SerialEnum.fromCode(Cardinality.class,
                getCardinality(cardinalityAndType));

        type = SerialEnum.fromCode(DataType.class, getType(cardinalityAndType));
        propertyKey.cardinality(cardinality);
        propertyKey.dataType(type);
        if (cardinality == Cardinality.SINGLE) {
            Object value = this.readProperty(type);
            return value;
        }
        Collection<Object> values = this.newValue(cardinality);
        assert cardinality == Cardinality.LIST ||
                cardinality == Cardinality.SET;
        int size = this.readVInt();
        for (int i = 0; i < size; i++) {
            values.add(this.readProperty(type));
        }
        return values;
    }

    public void writeProperty(DataType dataType, Object value) {
        switch (dataType) {
            case BOOLEAN:
                this.writeVInt(((Boolean) value) ? 1 : 0);
                break;
            case BYTE:
                this.writeVInt((Byte) value);
                break;
            case INT:
                this.writeVInt((Integer) value);
                break;
            case FLOAT:
                this.writeFloat((Float) value);
                break;
            case LONG:
                this.writeVLong((Long) value);
                break;
            case DATE:
                this.writeVLong(((Date) value).getTime());
                break;
            case DOUBLE:
                this.writeDouble((Double) value);
                break;
            case TEXT:
                this.writeString((String) value);
                break;
            case BLOB:
                byte[] bytes = value instanceof byte[] ?
                               (byte[]) value : ((Blob) value).bytes();
                this.writeBigBytes(bytes);
                break;
            case UUID:
                UUID uuid = (UUID) value;
                // Generally writeVLong(uuid) can't save space
                this.writeLong(uuid.getMostSignificantBits());
                this.writeLong(uuid.getLeastSignificantBits());
                break;
            default:
                throw new IllegalArgumentException("Unsupported data type " + dataType);
        }
    }

    public Object readProperty(DataType dataType) {
        switch (dataType) {
            case BOOLEAN:
                return this.readVInt() == 1;
            case BYTE:
                return (byte) this.readVInt();
            case INT:
                return this.readVInt();
            case FLOAT:
                return this.readFloat();
            case LONG:
                return this.readVLong();
            case DATE:
                return new Date(this.readVLong());
            case DOUBLE:
                return this.readDouble();
            case TEXT:
                return this.readString();
            case BLOB:
                return Blob.wrap(this.readBigBytes());
            case UUID:
                return new UUID(this.readLong(), this.readLong());
            default:
                throw new IllegalArgumentException("Unsupported data type " + dataType);
        }
    }

    public BytesBuffer writeId(Id id) {
        return this.writeId(id, false);
    }

    public BytesBuffer writeId(Id id, boolean big) {
        switch (id.type()) {
            case LONG:
                // Number Id
                long value = id.asLong();
                this.writeNumber(value);
                break;
            case UUID:
                // UUID Id
                byte[] bytes = id.asBytes();
                assert bytes.length == Id.UUID_LENGTH;
                this.writeUInt8(0x7f); // 0b01111111 means UUID
                this.write(bytes);
                break;
            case EDGE:
                // Edge Id
                this.writeUInt8(0x7e); // 0b01111110 means EdgeId
                this.writeEdgeId(id);
                break;
            default:
                // String Id
                bytes = id.asBytes();
                int len = bytes.length;
                E.checkArgument(len > 0, "Can't write empty id");
                E.checkArgument(len <= 16384,
                                "Big id max length is %s, but got %s {%s}",
                                16384, len, id);
                len -= 1;
                if (len <= 63) {
                    this.writeUInt8(len | 0x80);
                } else {
                    int high = len >> 8;
                    int low = len & 0xff;
                    this.writeUInt8(high | 0xc0);
                    this.writeUInt8(low);
                }

                this.write(bytes);
                break;
        }
        return this;
    }

    public Id readId() {
        return this.readId(false);
    }

    public Id readId(boolean big) {
        byte b = this.read();
        boolean number = (b & 0x80) == 0;
        if (number) {
            if (b == 0x7f) {
                // UUID Id
                return IdGenerator.of(this.read(Id.UUID_LENGTH), IdType.UUID);
            } else if (b == 0x7e) {
                // Edge Id
                return this.readEdgeId();
            } else {
                // Number Id
                return IdGenerator.of(this.readNumber(b));
            }
        } else {
            // String Id
            int len = b & 0x3f;
            if ((b & 0x40) != 0) {
                int high = len << 8;
                int low = this.readUInt8();
                len = high + low;
            }
            len += 1;
            byte[] id = this.read(len);
            return IdGenerator.of(id, IdType.STRING);
        }
    }

    public BytesBuffer writeEdgeId(Id id) {
        EdgeId edge = (EdgeId) id;
        this.writeId(edge.ownerVertexId());
        this.write(edge.directionCode());
        this.writeId(edge.edgeLabelId());
        this.writeId(edge.subLabelId());
        this.writeStringWithEnding(edge.sortValues());
        this.writeId(edge.otherVertexId());
        return this;
    }

    public Id readEdgeId() {
        return new EdgeId(this.readId(), EdgeId.directionFromCode(this.read()),
                          this.readId(), this.readId(),
                          this.readStringWithEnding(), this.readId());
    }

    public Id readEdgeIdSkipSortValues() {
        return new EdgeId(this.readId(), EdgeId.directionFromCode(this.read()),
                          this.readId(), this.readId(),
                          this.skipBytesWithEnding(),
                          this.readId());
    }


    public BytesBuffer writeIndexId(Id id, HugeType type) {
        return this.writeIndexId(id, type, true);
    }

    public BytesBuffer writeIndexId(Id id, HugeType type, boolean withEnding) {
        byte[] bytes = id.asBytes();
        int len = bytes.length;
        E.checkArgument(len > 0, "Can't write empty id");

        this.write(bytes);
        if (type.isStringIndex()) {
            if (Bytes.contains(bytes, STRING_ENDING_BYTE)) {
                // Not allow STRING_ENDING_BYTE exist in string index id
                E.checkArgument(false,
                                "The %s type index id can't contains " +
                                "byte '0x%s', but got: 0x%s", type,
                                Bytes.toHex(STRING_ENDING_BYTE),
                                Bytes.toHex(bytes));
            }
            if (withEnding) {
                this.writeStringWithEnding("");
            }
        }
        return this;
    }

    public BinaryId readIndexId(HugeType type) {
        byte[] id;
        if (type.isRange4Index()) {
            // HugeCodeType 1 bytes + IndexLabel 4 bytes + fieldValue 4 bytes
            id = this.read(9);
        } else if (type.isRange8Index()) {
            // HugeCodeType 1 bytes + IndexLabel 4 bytes + fieldValue 8 bytes
            id = this.read(13);
        } else {
            assert type.isStringIndex();
            id = this.readBytesWithEnding();
        }
        return new BinaryId(id, IdGenerator.of(id, IdType.STRING));
    }

    public BinaryId asId() {
        return new BinaryId(this.bytes(), null);
    }

    public BinaryId parseId(HugeType type) {
        if (type.isIndex()) {
            return this.readIndexId(type);
        }
        // Parse id from bytes
        int start = this.buffer.position();
        /*
         * Since edge id in edges table doesn't prefix with leading 0x7e,
         * so readId() will return the source vertex id instead of edge id,
         * can't call: type.isEdge() ? this.readEdgeId() : this.readId();
         */
        Id id = this.readId();
        int end = this.buffer.position();
        int len = end - start;
        byte[] bytes = new byte[len];
        System.arraycopy(this.array(), start, bytes, 0, len);
        return new BinaryId(bytes, id);
    }

    /**
     * Parse OLAP id
     * @param type
     * @param isOlap
     * @return
     */
    public BinaryId parseOlapId(HugeType type, boolean isOlap) {
        if (type.isIndex()) {
            return this.readIndexId(type);
        }
        // Parse id from bytes
        int start = this.buffer.position();
        /**
         * OLAP
         * {PropertyKey}{VertexId}
         */
        if (isOlap) {
            // First read OLAP property id
            Id pkId = this.readId();
        }
        Id id = this.readId();
        int end = this.buffer.position();
        int len = end - start;
        byte[] bytes = new byte[len];
        System.arraycopy(this.array(), start, bytes, 0, len);
        return new BinaryId(bytes, id);
    }

    private void writeNumber(long val) {
        /*
         * 8 kinds of number, 2 ~ 9 bytes number:
         * 0b 0kkksxxx X...
         * 0(1 bit) + kind(3 bits) + signed(1 bit) + number(n bits)
         *
         * 2 byte : 0b 0000 1xxx X(8 bits)                  [0, 2047]
         *          0b 0000 0xxx X(8 bits)                  [-2048, -1]
         * 3 bytes: 0b 0001 1xxx X X                        [0, 524287]
         *          0b 0001 0xxx X X                        [-524288, -1]
         * 4 bytes: 0b 0010 1xxx X X X                      [0, 134217727]
         *          0b 0010 0xxx X X X                      [-134217728, -1]
         * 5 bytes: 0b 0011 1xxx X X X X                    [0, 2^35 - 1]
         *          0b 0011 0xxx X X X X                    [-2^35, -1]
         * 6 bytes: 0b 0100 1xxx X X X X X                  [0, 2^43 - 1]
         *          0b 0100 0xxx X X X X X                  [-2^43, -1]
         * 7 bytes: 0b 0101 1xxx X X X X X X                [0, 2^51 - 1]
         *          0b 0101 0xxx X X X X X X                [-2^51, -1]
         * 8 bytes: 0b 0110 1xxx X X X X X X X              [0, 2^59 - 1]
         *          0b 0110 0xxx X X X X X X X              [-2^59, -1]
         * 9 bytes: 0b 0111 1000 X X X X X X X X            [0, 2^64 - 1]
         *          0b 0111 0000 X X X X X X X X            [-2^64, -1]
         *
         * NOTE:    0b 0111 1111 is used by 128 bits UUID
         *          0b 0111 1110 is used by EdgeId
         */
        int positive = val >= 0 ? 0x08 : 0x00;
        if (~0x7ffL <= val && val <= 0x7ffL) {
            int high3bits = (int) (val >> 8) & 0x07;
            this.writeUInt8(0x00 | positive | high3bits);
            this.writeUInt8((byte) val);
        } else if (~0x7ffffL <= val && val <= 0x7ffffL) {
            int high3bits = (int) (val >> 16) & 0x07;
            this.writeUInt8(0x10 | positive | high3bits);
            this.writeShort((short) val);
        } else if (~0x7ffffffL <= val && val <= 0x7ffffffL) {
            int high3bits = (int) (val >> 24 & 0x07);
            this.writeUInt8(0x20 | positive | high3bits);
            this.write((byte) (val >> 16));
            this.writeShort((short) val);
        } else if (~0x7ffffffffL <= val && val <= 0x7ffffffffL) {
            int high3bits = (int) (val >> 32) & 0x07;
            this.writeUInt8(0x30 | positive | high3bits);
            this.writeInt((int) val);
        } else if (~0x7ffffffffffL <= val && val <= 0x7ffffffffffL) {
            int high3bits = (int) (val >> 40) & 0x07;
            this.writeUInt8(0x40 | positive | high3bits);
            this.write((byte) (val >> 32));
            this.writeInt((int) val);
        } else if (~0x7ffffffffffffL <= val && val <= 0x7ffffffffffffL) {
            int high3bits = (int) (val >> 48) & 0x07;
            this.writeUInt8(0x50 | positive | high3bits);
            this.writeShort((short) (val >> 32));
            this.writeInt((int) val);
        } else if (~0x7ffffffffffffffL <= val && val <= 0x7ffffffffffffffL) {
            int high3bits = (int) (val >> 56) & 0x07;
            this.writeUInt8(0x60 | positive | high3bits);
            this.write((byte) (val >> 48));
            this.writeShort((short) (val >> 32));
            this.writeInt((int) val);
        } else {
            // high3bits is always 0b000 for 9 bytes number
            this.writeUInt8(0x70 | positive);
            this.writeLong(val);
        }
    }

    private long readNumber(byte b) {
        // Parse the kind from byte 0kkksxxx
        int kind = b >>> 4;
        boolean positive = (b & 0x08) > 0;
        long high3bits = b & 0x07;
        long value = high3bits << ((kind + 1) * 8);
        switch (kind) {
            case 0:
                value |= this.readUInt8();
                break;
            case 1:
                value |= this.readUInt16();
                break;
            case 2:
                value |= (long) this.readUInt8() << 16 | this.readUInt16();
                break;
            case 3:
                value |= this.readUInt32();
                break;
            case 4:
                value |= (long) this.readUInt8() << 32 | this.readUInt32();
                break;
            case 5:
                value |= (long) this.readUInt16() << 32 | this.readUInt32();
                break;
            case 6:
                value |= (long) this.readUInt8() << 48 |
                         (long) this.readUInt16() << 32 |
                         this.readUInt32();
                break;
            case 7:
                assert high3bits == 0L;
                value |= this.readLong();
                break;
            default:
                throw new AssertionError("Invalid length of number: " + kind);
        }
        if (!positive && kind < 7) {
            // Restore the bits of the original negative number
            long mask = Long.MIN_VALUE >> (52 - kind * 8);
            value |= mask;
        }
        return value;
    }

    private byte[] readBytesWithEnding() {
        int start = this.buffer.position();
        boolean foundEnding = false;
        while (this.remaining() > 0) {
            byte current = this.read();
            if (current == STRING_ENDING_BYTE) {
                foundEnding = true;
                break;
            }
        }
        E.checkArgument(foundEnding, "Not found ending '0x%s'",
                        Bytes.toHex(STRING_ENDING_BYTE));
        int end = this.buffer.position() - 1;
        int len = end - start;
        byte[] bytes = new byte[len];
        System.arraycopy(this.array(), start, bytes, 0, len);
        return bytes;
    }

    public byte[] remainingBytes(){
        int length = this.remaining();
        int start = this.position();
        byte[] bytes = new byte[length];
        System.arraycopy(this.array(), start, bytes, 0, length);
        return bytes;
    }
}
