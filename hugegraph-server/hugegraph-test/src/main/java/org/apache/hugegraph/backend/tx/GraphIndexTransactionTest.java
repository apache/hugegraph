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

package org.apache.hugegraph.backend.tx;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.page.PageIds;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class GraphIndexTransactionTest {

    @Test
    public void testSortedRangeBatchHolderKeepsPeekedBatch() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        Id id1 = IdGenerator.of(1);
        Id id2 = IdGenerator.of(2);
        Id id3 = IdGenerator.of(3);
        Set<Id> ids = InsertionOrderUtil.newSet();
        ids.add(id1);
        ids.add(id2);
        ids.add(id3);

        GraphIndexTransaction.SortedRangeBatchIdHolder holder =
                new GraphIndexTransaction.SortedRangeBatchIdHolder(query, ids);

        Assert.assertTrue(holder.keepOrder());

        PageIds peeked = holder.peekNext(2);
        Assert.assertEquals(ImmutableList.of(id1, id2), asList(peeked.ids()));

        PageIds firstBatch = holder.fetchNext(null, 2);
        Assert.assertEquals(ImmutableList.of(id1, id2),
                            asList(firstBatch.ids()));

        PageIds secondBatch = holder.fetchNext(null, 2);
        Assert.assertEquals(ImmutableList.of(id3), asList(secondBatch.ids()));
        Assert.assertFalse(holder.hasNext());
    }

    @Test
    public void testSortedRangeBatchHolderClosesOnZeroBatch() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        Set<Id> ids = InsertionOrderUtil.newSet();
        ids.add(IdGenerator.of(1));

        GraphIndexTransaction.SortedRangeBatchIdHolder holder =
                new GraphIndexTransaction.SortedRangeBatchIdHolder(query, ids);

        PageIds batch = holder.fetchNext(null, 0);
        Assert.assertTrue(batch.empty());
        Assert.assertFalse(holder.hasNext());
    }

    private static List<Id> asList(Set<Id> ids) {
        return new ArrayList<>(ids);
    }
}
