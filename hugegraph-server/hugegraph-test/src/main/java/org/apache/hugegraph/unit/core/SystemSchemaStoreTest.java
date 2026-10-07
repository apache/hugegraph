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

package org.apache.hugegraph.unit.core;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.backend.store.SystemSchemaStore;
import org.apache.hugegraph.struct.schema.SchemaElement;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Test;
import org.mockito.Mockito;

public class SystemSchemaStoreTest {

    @Test
    public void testExpandCapacity() {
        SystemSchemaStore store = new SystemSchemaStore();
        SchemaElement[] storeByIds = Whitebox.getInternalState(store,
                                                               "storeByIds");
        int initCapacity = storeByIds.length;

        int num = initCapacity + 1;
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        for (int i = 1; i <= num; i++) {
            Id id = IdGenerator.of(-i);
            String name = "name-" + i;
            store.add(new VertexLabel(graph, id, name));
        }

        for (int i = 1; i <= num; i++) {
            Id id = IdGenerator.of(-i);
            String name = "name-" + i;
            VertexLabel vlById = store.get(id);
            VertexLabel vlByName = store.get(name);
            Assert.assertEquals(vlById, vlByName);
        }
    }
}
