/*
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

package org.apache.hugegraph.query.serializer;

import java.lang.reflect.Type;
import java.util.Map;

import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;

import com.google.common.collect.ImmutableMap;

public class QueryIdAdapter extends AbstractSerializerAdapter<Id> {

    static ImmutableMap<String, Type> cls =
            ImmutableMap.<String, Type>builder()
                        .put("E", EdgeId.class)
                        .put("S", IdGenerator.StringId.class)
                        .put("L", IdGenerator.LongId.class)
                        .put("U", IdGenerator.UuidId.class)
                        .put("O", IdGenerator.ObjectId.class)
                        .put("B", BinaryId.class)
                        .build();

    @Override
    public Map<String, Type> validType() {
        return cls;
    }
}
