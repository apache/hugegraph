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

import java.math.BigDecimal;
import java.util.Iterator;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
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

import com.google.common.collect.ImmutableList;

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
    public void testConditionQueryBytesKeepBigDecimal() {
        // A DECIMAL filter travels to the store as a BigDecimal, not as an
        // untyped JSON number that Gson would read back as a double
        Id key = IdGenerator.of(7L);
        BigDecimal exact = new BigDecimal("12345678901234567890.123456789012345678");
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(Condition.eq(key, exact));
        query.query(Condition.gte(IdGenerator.of(8L), new BigDecimal("0.000000000000000001")));
        query.query(Condition.in(IdGenerator.of(9L),
                                 ImmutableList.of(new BigDecimal("1.10"),
                                                  new BigDecimal("2.20"))));

        ConditionQuery copy = ConditionQuery.fromBytes(query.bytes());
        Object value = copy.userpropValue(key);
        Assert.assertEquals(BigDecimal.class, value.getClass());
        Assert.assertEquals(exact, value);
        Object low = copy.userpropValue(IdGenerator.of(8L));
        Assert.assertEquals(new BigDecimal("0.000000000000000001"), low);
        Object list = copy.userpropValue(IdGenerator.of(9L));
        Assert.assertEquals(ImmutableList.of(new BigDecimal("1.10"),
                                             new BigDecimal("2.20")), list);
        // a plain double condition is unchanged
        query = new ConditionQuery(HugeType.VERTEX);
        query.query(Condition.eq(key, 1.5d));
        Assert.assertEquals(1.5d, ConditionQuery.fromBytes(query.bytes())
                                                .userpropValue(key));
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
