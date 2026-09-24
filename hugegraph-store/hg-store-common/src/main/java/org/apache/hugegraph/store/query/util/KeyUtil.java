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

package org.apache.hugegraph.store.query.util;

import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdUtil;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.store.constant.HugeServerTables;

public class KeyUtil {

    private static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * Using algorithm from huge-server
     *
     * @param key   original key
     * @param table looking up table
     * @return
     */
    public static byte[] getOwnerKey(String table, byte[] key) {
        if (key == null || key.length == 0) {
            return EMPTY_BYTES;
        }

        if (HugeServerTables.isEdgeTable(table)) {
            var id = (EdgeId) IdUtil.fromBytes(key);
            return idToBytes(id.ownerVertexId());
        }

        return key;
    }

    public static byte[] getOwnerId(Id id) {
        if (id instanceof BinaryId) {
            id = ((BinaryId) id).origin();
        }
        if (id != null && id.edge()) {
            id = ((EdgeId) id).ownerVertexId();
        }
        return id != null ? id.asBytes() : EMPTY_BYTES;

    }

    public static byte[] idToBytes(Id id) {
        BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
        buffer.writeId(id);
        return buffer.bytes();
    }

}
