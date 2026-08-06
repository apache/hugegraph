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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.Idfiable;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class QueryResultsTest {

    @Test
    public void testKeepInputOrderForPagingIdQuery() {
        Id id1 = IdGenerator.of(1L);
        Id id2 = IdGenerator.of(2L);
        Query pagingQuery = new Query(HugeType.VERTEX);
        pagingQuery.page("page-1");
        pagingQuery.limit(2L);

        Set<Id> ids = InsertionOrderUtil.newSet();
        ids.add(id2);
        ids.add(id1);

        IdQuery idQuery = new IdQuery(pagingQuery, ids);
        idQuery.mustSortByInput(true);
        QueryResults<TestIdfiable> results = new QueryResults<>(
                Arrays.asList(new TestIdfiable(id1),
                              new TestIdfiable(id2)).iterator(),
                idQuery);

        List<Id> orderedIds = new ArrayList<>();
        results.keepInputOrderIfNeeded(
                Arrays.asList(new TestIdfiable(id1),
                              new TestIdfiable(id2)).iterator())
               .forEachRemaining(item -> orderedIds.add(item.id()));

        Assert.assertEquals(ImmutableList.of(id2, id1), orderedIds);
    }

    private static final class TestIdfiable implements Idfiable {

        private final Id id;

        private TestIdfiable(Id id) {
            this.id = id;
        }

        @Override
        public Id id() {
            return this.id;
        }
    }
}
