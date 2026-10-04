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

package org.apache.hugegraph.io;

import java.io.IOException;

import org.apache.hugegraph.backend.id.Id;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.SimpleTypeSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.TransformSerializer;

public final class HugeGraphTypeSerializerRegistryBuilder
        extends TypeSerializerRegistry.Builder {

    private static final TypeSerializer<Id> ID_TRANSFORM_SERIALIZER =
            new IdTransformSerializer();

    public HugeGraphTypeSerializerRegistryBuilder() {
        this.withFallbackResolver(type -> {
            if (Id.class.isAssignableFrom(type)) {
                return ID_TRANSFORM_SERIALIZER;
            }
            return null;
        });
    }

    private static final class IdTransformSerializer
            extends SimpleTypeSerializer<Id>
            implements TransformSerializer<Id> {

        private IdTransformSerializer() {
            super(null);
        }

        @Override
        protected Id readValue(Buffer buffer, GraphBinaryReader context)
                throws IOException {
            throw new IOException("HugeGraph Id is written as a wire primitive");
        }

        @Override
        protected void writeValue(Id value, Buffer buffer,
                                  GraphBinaryWriter context)
                throws IOException {
            throw new IOException("HugeGraph Id is written as a wire primitive");
        }

        @Override
        public Object transform(Id value) {
            return value.asObject();
        }
    }
}
