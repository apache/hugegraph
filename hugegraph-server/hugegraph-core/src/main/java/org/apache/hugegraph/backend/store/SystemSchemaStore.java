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

package org.apache.hugegraph.backend.store;

import java.util.Map;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.struct.schema.SchemaElement;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.util.collection.CollectionFactory;

/**
 * The system schema will be initialized when server started, and the
 * initialization process is thread-safe, so it's unnecessary to lock it.
 */
public class SystemSchemaStore {

    private static final int SYSTEM_SCHEMA_MAX_NUMS = 128;

    private SchemaElement[] storeByIds;
    private final Map<String, SchemaElement> storeByNames;

    public SystemSchemaStore() {
        this.storeByIds = new SchemaElement[SYSTEM_SCHEMA_MAX_NUMS];
        this.storeByNames = CollectionFactory.newMap(CollectionType.EC,
                                                     SYSTEM_SCHEMA_MAX_NUMS);
    }

    public void add(SchemaElement schema) {
        long idValue = schema.id().asLong();
        assert idValue < 0L;
        int index = (int) Math.abs(idValue);
        if (index >= this.storeByIds.length) {
            this.expandCapacity();
        }
        this.storeByIds[index] = schema;
        this.storeByNames.put(schema.name(), schema);
    }

    @SuppressWarnings("unchecked")
    public <T extends SchemaElement> T get(Id id) {
        long idValue = id.asLong();
        assert idValue < 0L;
        int index = (int) Math.abs(idValue);
        return (T) this.storeByIds[index];
    }

    @SuppressWarnings("unchecked")
    public <T extends SchemaElement> T get(String name) {
        return (T) this.storeByNames.get(name);
    }

    private void expandCapacity() {
        int newLength = this.storeByIds.length << 1;
        SchemaElement[] newStoreByIds = new SchemaElement[newLength];
        System.arraycopy(this.storeByIds, 0, newStoreByIds, 0,
                         this.storeByIds.length);
        this.storeByIds = newStoreByIds;
    }
}
