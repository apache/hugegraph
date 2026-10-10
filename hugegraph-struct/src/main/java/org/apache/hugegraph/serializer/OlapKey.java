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

package org.apache.hugegraph.serializer;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.struct.schema.SchemaElement;

/** Physical keys for the merged HStore OLAP table, independent of the value codec. */
public final class OlapKey {

    private OlapKey() {
    }

    public static byte[] format(Id propertyId, Id vertexId) {
        return BytesBuffer.allocate(propertyId.length() + vertexId.length() + 18)
                          .writeId(propertyId).writeId(vertexId).bytes();
    }

    public static boolean matchesProperty(byte[] value, Id propertyId) {
        if (value == null || value.length == 0) {
            return false;
        }
        try {
            return BytesBuffer.wrap(value).readVInt() == SchemaElement.schemaId(propertyId);
        } catch (IllegalArgumentException | java.nio.BufferUnderflowException e) {
            return false;
        }
    }
}
