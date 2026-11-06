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

package org.apache.hugegraph.pd.raft;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Requires;
import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;

import lombok.Data;

import org.apache.hugegraph.pd.raft.serializer.HugegraphHessianSerializerFactory;

@Data
public class KVOperation {

    /**
     * Put operation
     */
    public static final byte PUT = 0x01;
    /**
     * Get operation
     */
    public static final byte GET = 0x02;
    public static final byte DEL = 0x03;
    public static final byte REMOVE_BY_PREFIX = 0x04;
    public static final byte REMOVE = 0x05;
    public static final byte PUT_WITH_TTL = 0x06;
    public static final byte CLEAR = 0x07;
    public static final byte PUT_WITH_TTL_UNIT = 0x08;
    public static final byte REMOVE_WITH_TTL = 0x09;
    /**
     * Atomic multi-key write, the value holds a serialized kv.TxnEntry: the request with its
     * keys encoded by the proposing node
     */
    public static final byte TXN = 0x0A;
    /**
     * Snapshot operation
     */
    public static final byte SAVE_SNAPSHOT = 0x10;
    public static final byte LOAD_SNAPSHOT = 0x11;

    private byte[] key;
    private byte[] value;
    // Raw object, used for native processing, reducing the number of deserialization
    // operations
    private Object attach;
    private Object arg;
    private byte op;
    // Raft log index of the entry, set on apply on every node and never serialized
    private transient long index;
    // Raft term of the entry, set with the index
    private transient long term;

    public KVOperation() {

    }

    public KVOperation(byte[] key, byte[] value, Object attach, byte op) {
        this.key = key;
        this.value = value;
        this.attach = attach;
        this.op = op;
    }

    public KVOperation(byte[] key, byte[] value, Object attach, byte op, Object arg) {
        this.key = key;
        this.value = value;
        this.attach = attach;
        this.op = op;
        this.arg = arg;
    }

    public static KVOperation fromByteArray(byte[] value) throws IOException {

        try (ByteArrayInputStream bis = new ByteArrayInputStream(value, 1, value.length - 1)) {
            Hessian2Input input = new Hessian2Input(bis);
            input.setSerializerFactory(HugegraphHessianSerializerFactory.getInstance());
            KVOperation op = new KVOperation();
            op.op = value[0];
            op.key = input.readBytes();
            op.value = input.readBytes();
            op.arg = input.readObject();
            input.close();
            return op;
        }
    }

    public static KVOperation createPut(final byte[] key, final byte[] value) {
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        return new KVOperation(key, value, null, PUT);
    }

    public static KVOperation createGet(final byte[] key) {
        Requires.requireNonNull(key, "key");
        return new KVOperation(key, BytesUtil.EMPTY_BYTES, null, GET);
    }

    public static KVOperation createPutWithTTL(byte[] key, byte[] value, long ttl) {
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        return new KVOperation(key, value, value, PUT_WITH_TTL,
                               ttl);
    }

    public static KVOperation createPutWithTTL(byte[] key, byte[] value, long ttl,
                                               TimeUnit timeUnit) {
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        return new KVOperation(key, value, value, PUT_WITH_TTL_UNIT,
                               new Object[]{ttl, timeUnit});
    }

    public static KVOperation createRemoveWithTTL(byte[] key) {
        Requires.requireNonNull(key, "key");
        return new KVOperation(key, key, null, REMOVE_WITH_TTL);
    }

    public static KVOperation createRemoveByPrefix(byte[] key) {
        Requires.requireNonNull(key, "key");
        return new KVOperation(key, key, null, REMOVE_BY_PREFIX);
    }

    public static KVOperation createRemove(byte[] key) {
        Requires.requireNonNull(key, "key");
        return new KVOperation(key, key, null, REMOVE);
    }

    public static KVOperation createTxn(byte[] request) {
        Requires.requireNonNull(request, "request");
        return new KVOperation(null, request, null, TXN);
    }

    public static KVOperation createClear() {
        return new KVOperation(null, null, null, CLEAR);
    }

    public static KVOperation createSaveSnapshot(String snapshotPath) {
        Requires.requireNonNull(snapshotPath, "snapshotPath");
        return new KVOperation(null, null, snapshotPath, SAVE_SNAPSHOT);
    }

    public static KVOperation createLoadSnapshot(String snapshotPath) {
        Requires.requireNonNull(snapshotPath, "snapshotPath");
        return new KVOperation(null, null, snapshotPath, LOAD_SNAPSHOT);
    }

    public byte[] toByteArray() throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            bos.write(op);
            Hessian2Output output = new Hessian2Output(bos);
            output.writeObject(key);
            output.writeObject(value);
            output.writeObject(arg);
            output.flush();
            return bos.toByteArray();
        }
    }
}
