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

package org.apache.hugegraph.store.business.itrv2.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.serializer.BinaryElementSerializer;
import org.apache.hugegraph.store.query.KvSerializer;
import org.apache.hugegraph.store.query.Tuple2;
import org.apache.hugegraph.store.util.MultiKv;
import org.apache.hugegraph.structure.BaseEdge;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.structure.BaseVertex;

import lombok.extern.slf4j.Slf4j;

/**
 * support backend column, Multi kv, BaseElement
 * format : object | object | object
 *  todo: need write object type header ?
 *
 * @param <T> object type
 */
@Slf4j
public class SortShuffleSerializer<T> {

    private static final byte TYPE_HEADER_MULTI_KV = 1;
    private static final byte TYPE_HEADER_BACKEND_COLUMN = 2;
    private static final byte TYPE_HEADER_BASE_ELEMENT = 3;

    private static final SortShuffleSerializer<RocksDBSession.BackendColumn> backendSerializer =
            new SortShuffleSerializer<>(new BackendColumnSerializer());

    private static final SortShuffleSerializer<MultiKv> mkv =
            new SortShuffleSerializer<>(new MultiKvSerializer());

    private static final SortShuffleSerializer<BaseElement> element =
            new SortShuffleSerializer<>(new BaseElementSerializer());

    private final ObjectSerializer<T> serializer;

    private SortShuffleSerializer(ObjectSerializer<T> serializer) {
        this.serializer = serializer;
    }

    public static SortShuffleSerializer<RocksDBSession.BackendColumn> ofBackendColumnSerializer() {
        return backendSerializer;
    }

    public static SortShuffleSerializer<MultiKv> ofMultiKvSerializer() {
        return mkv;
    }

    public static SortShuffleSerializer<BaseElement> ofBaseElementSerializer() {
        return element;
    }

    public static byte[] toByte(int i) {
        byte[] result = new byte[4];
        result[0] = (byte) ((i >> 24) & 0xff);
        result[1] = (byte) ((i >> 16) & 0xff);
        result[2] = (byte) ((i >> 8) & 0xff);
        result[3] = (byte) (i & 0xff);
        return result;
    }

    public static int toInt(byte[] b) {
        assert b.length == 4;
        int value = 0;
        for (int i = 0; i < 4; i++) {
            int shift = (3 - i) * 8;
            value += (b[i] & 0xff) << shift;
        }
        return value;
    }

    private static byte[] kvBytesToByte(byte[] key, byte[] value) {

        int len = (key == null ? 0 : key.length) + (value == null ? 0 : value.length) + 8;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.putInt(key == null ? 0 : key.length);
        if (key != null) {
            buffer.put(key);
        }
        buffer.putInt(value == null ? 0 : value.length);
        if (value != null) {
            buffer.put(value);
        }
        return buffer.array();
    }

    private static Tuple2<byte[], byte[]> fromKvBytes(byte[] bytes) {
        assert bytes != null;
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int nameLen = buffer.getInt();
        byte[] name = null;
        if (nameLen != 0) {
            name = new byte[nameLen];
            buffer.get(name);
        }

        int valueLen = buffer.getInt();
        byte[] value = null;
        if (valueLen != 0) {
            value = new byte[valueLen];
            buffer.get(value);
        }

        return Tuple2.of(name, value);
    }

    public void write(OutputStream output, T data) throws IOException {
        // input.write(serializer.getTypeHeader());
        var b = serializer.getBytes(data);
        output.write(toByte(b.length));
        output.write(b);
    }

    public T read(InputStream input) {
        try {
            var bytes = input.readNBytes(4);

            if (bytes.length == 0) {
                return null;
            }

            int sz = toInt(bytes);
            return serializer.fromBytes(input.readNBytes(sz));
        } catch (IOException e) {
            log.debug("error: {}", e.getMessage());
            return null;
        }
    }

    private abstract static class ObjectSerializer<T> {

        public abstract T fromBytes(byte[] bytes);

        public abstract byte[] getBytes(T t);

        public abstract byte getTypeHeader();
    }

    /**
     * format :
     * key bytes len| key | value bytes len | value bytes
     */

    private static class MultiKvSerializer extends ObjectSerializer<MultiKv> {

        @Override
        public MultiKv fromBytes(byte[] bytes) {
            var tuple = fromKvBytes(bytes);
            return MultiKv.of(KvSerializer.fromObjectBytes(tuple.getV1()),
                              KvSerializer.fromObjectBytes(tuple.getV2()));
        }

        @Override
        public byte[] getBytes(MultiKv multiKv) {
            return kvBytesToByte(KvSerializer.toBytes(multiKv.getKeys()),
                                 KvSerializer.toBytes(multiKv.getValues()));
        }

        @Override
        public byte getTypeHeader() {
            return TYPE_HEADER_MULTI_KV;
        }
    }

    /**
     * format:
     * name.len | name | value.len | value
     */
    private static class BackendColumnSerializer extends
                                                 ObjectSerializer<RocksDBSession.BackendColumn> {

        @Override
        public RocksDBSession.BackendColumn fromBytes(byte[] bytes) {
            var tuple = fromKvBytes(bytes);
            return RocksDBSession.BackendColumn.of(tuple.getV1(), tuple.getV2());
        }

        @Override
        public byte[] getBytes(RocksDBSession.BackendColumn column) {
            return kvBytesToByte(column.name, column.value);
        }

        @Override
        public byte getTypeHeader() {
            return TYPE_HEADER_BACKEND_COLUMN;
        }
    }

    /**
     * format:
     * vertex/edge | name.len | name | value.len | value
     */
    private static class BaseElementSerializer extends ObjectSerializer<BaseElement> {

        private final BinaryElementSerializer serializer = new BinaryElementSerializer();

        @Override
        public BaseElement fromBytes(byte[] bytes) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            boolean isVertex = buffer.get() == 0;

            int nameLen = buffer.getInt();
            byte[] name = new byte[nameLen];
            buffer.get(name);
            int valueLen = buffer.getInt();
            byte[] value = new byte[valueLen];
            buffer.get(value);

            if (isVertex) {
                return serializer.parseVertex(null, BackendColumn.of(name, value), null);
            }
            return serializer.parseEdge(null, BackendColumn.of(name, value), null, true);
        }

        @Override
        public byte[] getBytes(BaseElement element) {
            assert element != null;

            BackendColumn column;
            boolean isVertex = false;
            if (element instanceof BaseVertex) {
                column = serializer.writeVertex((BaseVertex) element);
                isVertex = true;
            } else {
                column = serializer.writeEdge((BaseEdge) element);
            }

            ByteBuffer buffer = ByteBuffer.allocate(column.name.length + column.value.length + 9);
            if (isVertex) {
                buffer.put((byte) 0);
            } else {
                buffer.put((byte) 1);
            }

            buffer.putInt(column.name.length);
            buffer.put(column.name);
            buffer.putInt(column.value.length);
            buffer.put(column.value);
            return buffer.array();
        }

        @Override
        public byte getTypeHeader() {
            return TYPE_HEADER_BASE_ELEMENT;
        }
    }

}
