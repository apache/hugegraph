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

import java.util.Arrays;
import java.util.Base64;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.google.common.primitives.Longs;

public class DirectBinarySerializer {

    protected static final Logger LOG = Log.logger(DirectBinarySerializer.class);

    public static class DirectHugeElement {
        private final Id id;
        private final long expiredTime;

        public DirectHugeElement(Id id, long expiredTime) {
            this.id = id;
            this.expiredTime = expiredTime;
        }

        public Id id() {
            return id;
        }

        public long expiredTime() {
            return expiredTime;
        }
    }

    public DirectHugeElement parseIndex(byte[] key, byte[] value) {
        long expiredTime = 0L;

        if (value.length > 0) {
            // Get delimiter address
            int delimiterIndex =
                    Bytes.indexOf(value, BytesBuffer.STRING_ENDING_BYTE);

            if (delimiterIndex >= 0) {
                // Delimiter is in the data, need to parse from data
                // Parse expiration time
                byte[] expiredTimeBytes =
                        Arrays.copyOfRange(value, delimiterIndex + 1,
                                           value.length);

                if (expiredTimeBytes.length > 0) {
                    byte[] rawBytes =
                            Base64.getDecoder().decode(expiredTimeBytes);
                    if (rawBytes.length >= Longs.BYTES) {
                        expiredTime = Longs.fromByteArray(rawBytes);
                    }
                }
            }
        }

        return new DirectHugeElement(IdGenerator.of(key), expiredTime);
    }

    public DirectHugeElement parseVertex(byte[] key, byte[] value) {
        long expiredTime = 0L;

        BytesBuffer buffer = BytesBuffer.wrap(value);
        // read schema label id
        buffer.readId();
        // Skip edge properties
        this.skipProperties(buffer);
        // Parse edge expired time if needed
        if (buffer.remaining() > 0) {
            expiredTime = buffer.readVLong();
        }

        return new DirectHugeElement(IdGenerator.of(key), expiredTime);
    }

    public DirectHugeElement parseEdge(byte[] key, byte[] value) {
        long expiredTime = 0L;

        BytesBuffer buffer = BytesBuffer.wrap(value);
        // Skip edge properties
        this.skipProperties(buffer);
        // Parse edge expired time if needed
        if (buffer.remaining() > 0) {
            expiredTime = buffer.readVLong();
        }

        return new DirectHugeElement(IdGenerator.of(key), expiredTime);
    }

    public DirectHugeElement parseSchemaIndex(HugeGraphSupplier graph, byte[] key, byte[] value) {
        Index index = BinaryElementSerializer.getInstance().parseSchemaIndex(
                graph, BackendColumn.of(key, value));
        return new DirectHugeElement(IdGenerator.of(key), index.expiredTime());
    }

    /** Read TTL from an untagged persisted Server vertex row. */
    public DirectHugeElement parseSchemaVertex(HugeGraphSupplier graph, byte[] key, byte[] value) {
        E.checkArgumentNotNull(graph, "Schema vertex decoding requires a schema supplier");
        BytesBuffer buffer = BytesBuffer.wrap(value);
        Id labelId = buffer.readId();
        this.skipProperties(buffer, graph);
        long expiredTime = graph.vertexLabelOrNone(labelId).ttl() > 0 ? buffer.readVLong() : 0L;
        return new DirectHugeElement(IdGenerator.of(key), expiredTime);
    }

    /** Read TTL from an untagged persisted Server edge row. */
    public DirectHugeElement parseSchemaEdge(HugeGraphSupplier graph, byte[] key, byte[] value) {
        E.checkArgumentNotNull(graph, "Schema edge decoding requires a schema supplier");
        BytesBuffer name = BytesBuffer.wrap(key);
        name.readId();
        byte type = name.read();
        E.checkState(type == HugeType.EDGE_IN.code() || type == HugeType.EDGE_OUT.code(),
                     "Invalid edge column type: %s", type & 0xff);
        name.readId();
        Id subLabelId = name.readId();
        BytesBuffer buffer = BytesBuffer.wrap(value);
        this.skipProperties(buffer, graph);
        long expiredTime = graph.edgeLabelOrNone(subLabelId).ttl() > 0 ? buffer.readVLong() : 0L;
        return new DirectHugeElement(IdGenerator.of(key), expiredTime);
    }

    private void skipProperties(BytesBuffer buffer) {
        this.skipProperties(buffer, null);
    }

    private void skipProperties(BytesBuffer buffer, HugeGraphSupplier graph) {
        int size = buffer.readVInt();
        assert size >= 0;
        for (int i = 0; i < size; i++) {
            Id pkeyId = IdGenerator.of(buffer.readVInt());
            if (graph == null) {
                this.skipProperty(pkeyId, buffer);
            } else {
                buffer.skipSchemaProperty(graph.propertyKey(pkeyId));
            }
        }
    }

    protected void skipProperty(Id pkeyId, BytesBuffer buffer) {
        buffer.skipProperty();
    }
}
