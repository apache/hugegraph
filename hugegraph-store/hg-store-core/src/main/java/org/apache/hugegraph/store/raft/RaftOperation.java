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

package org.apache.hugegraph.store.raft;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.google.protobuf.CodedOutputStream;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class RaftOperation {

    public static final byte SYNC_PARTITION_TASK = 0x60;
    public static final byte SYNC_PARTITION = 0x61;
    public static final byte BLANK_TASK = 0x62;
    public static final byte DO_SNAPSHOT = 0x63;
    // Cluster internal data migration operation
    public static final byte IN_WRITE_OP = 0x64;
    public static final byte IN_CLEAN_OP = 0x65;
    public static final byte RAFT_UPDATE_PARTITION = 0x66;
    public static final byte DB_COMPACTION = 0x67;
    public static final byte DO_SYNC_SNAPSHOT = 0x68;
    public static final byte SYNC_BLANK_TASK = 0x69;

    final static byte[] EMPTY_Bytes = new byte[0];
    private static final Logger LOG = LoggerFactory.getLogger(RaftOperation.class);
    private byte[] values;     // req serialized result, used for transmitting to other raft nodes
    private Object req;
    // Original object, used for native processing, reducing one deserialization operation
    private byte op;         // operation type

    public static RaftOperation create(final byte op) {
        try {
            RaftOperation operation = new RaftOperation();
            operation.setOp(op);
            operation.setReq(null);
            operation.setValues(toByteArray(op));
            return operation;
        } catch (Exception e) {
            LOG.error("create error", e);
            return null;
        }
    }

    public static RaftOperation create(final byte op, final byte[] values, final Object req) {
        RaftOperation operation = new RaftOperation();
        operation.setOp(op);
        operation.setReq(req);
        operation.setValues(values);
        return operation;
    }

    public static RaftOperation create(final byte op, final Object req) {
        try {
            RaftOperation operation = new RaftOperation();
            operation.setOp(op);
            operation.setReq(req);
            operation.setValues(toByteArray(op, req));
            return operation;
        } catch (Exception e) {
            log.error("exception ", e);
        }
        return null;
    }

    public static RaftOperation create(final byte op,
                                       final com.google.protobuf.GeneratedMessageV3 req) throws
                                                                                         IOException {
        // Serialization
        final byte[] buffer = new byte[req.getSerializedSize() + 1];
        final CodedOutputStream output = CodedOutputStream.newInstance(buffer);
        output.write(op);
        req.writeTo(output);
        output.checkNoSpaceLeft();
        output.flush();
        return create(op, buffer, req);
    }

    public static byte[] toByteArray(final byte op) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            bos.write(op);
            bos.flush();
            return bos.toByteArray();
        }
    }

    public static byte[] toByteArray(final byte op, final Object obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            bos.write(op);
            Hessian2Output output = new Hessian2Output(bos);
            output.writeObject(obj);
            output.flush();
            return bos.toByteArray();
        }
    }

    public static Object toObject(final byte[] bytes, int offset) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes, offset + 1,
                                                                 bytes.length - offset)) {
            Hessian2Input input = new Hessian2Input(bis);
            Object obj = input.readObject();
            input.close();
            return obj;
        }
    }
}
