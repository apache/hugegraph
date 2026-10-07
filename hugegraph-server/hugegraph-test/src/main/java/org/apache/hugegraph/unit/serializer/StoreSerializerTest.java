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

package org.apache.hugegraph.unit.serializer;

import java.util.Iterator;

import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.backend.store.BackendAction;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.raft.StoreCommand;
import org.apache.hugegraph.backend.store.raft.StoreSerializer;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreAction;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreType;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Action;
import org.junit.Test;

public class StoreSerializerTest {

    @Test
    public void testSerializeBackendMutation() {
        BinaryBackendEntry entry = new BinaryBackendEntry(HugeType.VERTEX,
                                                          new byte[]{1, 2});
        entry.column(new byte[]{1}, new byte[]{1});
        entry.column(new byte[]{2}, new byte[]{2});
        entry.column(new byte[]{127}, new byte[]{127});
        BackendMutation origin = new BackendMutation();
        origin.add(entry, Action.INSERT);
        byte[] bytes = StoreSerializer.writeMutation(origin);

        BytesBuffer buffer = BytesBuffer.wrap(bytes);
        BackendMutation actual = StoreSerializer.readMutation(buffer);
        Assert.assertEquals(1, actual.size());
        Iterator<BackendAction> iter = actual.mutation();
        while (iter.hasNext()) {
            BackendAction item = iter.next();
            Assert.assertEquals(Action.INSERT, item.action());
            BackendEntry e = item.entry();
            Assert.assertEquals(entry.type(), e.type());
            Assert.assertEquals(entry.id(), e.id());
            Assert.assertEquals(entry.subId(), e.subId());
            Assert.assertEquals(entry.ttl(), e.ttl());
            Assert.assertEquals(entry.columnsSize(), e.columnsSize());
            Assert.assertEquals(entry.columns(), e.columns());
        }
    }

    @Test
    public void testSerializeStoreCommand() {
        BinaryBackendEntry entry = new BinaryBackendEntry(HugeType.VERTEX,
                                                          new byte[]{1, 2});
        entry.column(new byte[]{1}, new byte[]{1});
        entry.column(new byte[]{2}, new byte[]{2});
        entry.column(new byte[]{127}, new byte[]{127});
        BackendMutation origin = new BackendMutation();
        origin.add(entry, Action.INSERT);
        byte[] mutationBytes = StoreSerializer.writeMutation(origin);

        StoreCommand command = new StoreCommand(StoreType.GRAPH,
                                                StoreAction.MUTATE,
                                                mutationBytes);
        Assert.assertEquals(StoreAction.MUTATE, command.action());
        Assert.assertArrayEquals(mutationBytes, command.data());

        byte[] commandBytes = command.data();
        StoreCommand actual = StoreCommand.fromBytes(commandBytes);
        Assert.assertEquals(StoreType.GRAPH, command.type());
        Assert.assertEquals(command.action(), actual.action());
        Assert.assertArrayEquals(command.data(), actual.data());
    }
}
