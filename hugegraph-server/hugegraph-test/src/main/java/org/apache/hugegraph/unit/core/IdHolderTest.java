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

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.page.IdHolder.BatchIdHolder;
import org.apache.hugegraph.backend.page.IdHolder.PagingIdHolder;
import org.apache.hugegraph.backend.page.IdHolderList;
import org.apache.hugegraph.backend.page.PageIds;
import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.backend.page.QueryList;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.junit.Test;

public class IdHolderTest {

    @Test
    public void testBatchIndexQueryKeepsHolderOrder() {
        ConditionQuery parent = new ConditionQuery(HugeType.VERTEX);
        ConditionQuery indexQuery =
                new ConditionQuery(HugeType.RANGE_INT_INDEX);
        Set<Id> ids = ids(IdGenerator.of(2L), IdGenerator.of(1L));
        Iterator<BackendEntry> entries =
                Collections.<BackendEntry>singletonList(null).iterator();
        BatchIdHolder holder = new BatchIdHolder(indexQuery, entries,
                                                  batch -> ids, true);

        IdQuery idQuery = fetchIdQuery(parent, holder, 2);

        Assert.assertTrue(holder.keepOrder());
        Assert.assertTrue(idQuery.mustSortByInput());
    }

    @Test
    public void testPagingIndexQueryKeepsHolderOrder() {
        ConditionQuery parent = new ConditionQuery(HugeType.VERTEX);
        parent.page("");
        ConditionQuery indexQuery =
                new ConditionQuery(HugeType.RANGE_INT_INDEX);
        indexQuery.page("");
        Set<Id> ids = ids(IdGenerator.of(2L), IdGenerator.of(1L));
        PagingIdHolder holder = new PagingIdHolder(
                indexQuery,
                query -> new PageIds(ids, PageState.EMPTY), true);

        IdQuery idQuery = fetchIdQuery(parent, holder, 2);

        Assert.assertTrue(holder.keepOrder());
        Assert.assertTrue(idQuery.mustSortByInput());
    }

    private static IdQuery fetchIdQuery(ConditionQuery parent,
                                        org.apache.hugegraph.backend.page.IdHolder holder,
                                        int pageSize) {
        AtomicReference<IdQuery> captured = new AtomicReference<>();
        QueryList<Id> queries = new QueryList<>(parent, query -> {
            Assert.assertTrue(query instanceof IdQuery);
            IdQuery idQuery = (IdQuery) query;
            captured.set(idQuery);
            return new QueryResults<>(idQuery.ids().iterator(), idQuery);
        });
        IdHolderList holders = new IdHolderList(holder.paging());
        holders.add(holder);
        queries.add(holders, Query.QUERY_BATCH);

        Iterator<Id> results = queries.fetch(pageSize).iterator();
        Assert.assertTrue(results.hasNext());
        Assert.assertNotNull(captured.get());
        return captured.get();
    }

    private static Set<Id> ids(Id... ids) {
        Set<Id> result = InsertionOrderUtil.newSet();
        Collections.addAll(result, ids);
        return result;
    }
}
