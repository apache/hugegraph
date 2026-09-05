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

import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.ConditionQuery.OptimizedType;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.query.QueryResultContext;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.junit.Test;

public class GraphTransactionTest {

    @Test
    public void testBatchDecisionsRemainFixedAfterOriginChanges() {
        ConditionQuery root = new ConditionQuery(HugeType.VERTEX);
        root.optimized(OptimizedType.PRIMARY_KEY);
        root.showHidden(true);
        root.showDeleting(true);
        root.showExpired(true);
        IdQuery query = new IdQuery(root, IdGenerator.of(1L));
        QueryResultContext context = new QueryResultContext(query);
        root.optimized(OptimizedType.INDEX_FILTER);
        root.showHidden(false);
        root.showDeleting(false);
        root.showExpired(false);
        query.resetIds();
        query.mustSortByInput(false);
        Assert.assertEquals(OptimizedType.PRIMARY_KEY, context.optimizedType());
        Assert.assertTrue(context.conditionFilterRequired());
        Assert.assertTrue(context.showHidden());
        Assert.assertTrue(context.showDeleting());
        Assert.assertTrue(context.showExpired());
        Assert.assertTrue(context.mustSortByInputIds());
        Assert.assertEquals(IdGenerator.of(1L), context.inputIds().get(0));
        Assert.assertEquals(1, context.inputIds().size());
        Assert.assertSame(root, context.matchQuery());
    }

    @Test
    public void testSiblingOptimizationDoesNotReplaceBatchDecision() {
        ConditionQuery root = new ConditionQuery(HugeType.EDGE);
        ConditionQuery first = root.copy();
        first.optimized(OptimizedType.INDEX_FILTER);
        ConditionQuery second = root.copy();
        second.optimized(OptimizedType.INDEX);
        Assert.assertEquals(OptimizedType.INDEX_FILTER, root.optimized());
        QueryResultContext context = new QueryResultContext(
                new IdQuery(second, IdGenerator.of(1L)));
        Assert.assertEquals(OptimizedType.INDEX, context.optimizedType());
        Assert.assertSame(root, context.matchQuery());
    }

    @Test
    public void testDirectIdsHaveNoConditionFilter() {
        IdQuery query = new IdQuery(HugeType.EDGE, IdGenerator.of(1L));
        QueryResultContext context = new QueryResultContext(query);
        Assert.assertNull(context.matchQuery());
        Assert.assertFalse(context.conditionFilterRequired());
        Assert.assertNull(context.resultsFilter());
        Assert.assertFalse(new QueryResultContext(query, true).mustSortByInputIds());
    }
}
