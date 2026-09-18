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

import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.IndexType;
import org.junit.Test;

public class GraphIndexTransactionTest {

    @Test
    public void testKeepBackendIndexOrderOnlyForOrderedHstoreRangeQuery() {
        ConditionQuery query = new ConditionQuery(HugeType.RANGE_INT_INDEX);

        Assert.assertFalse(GraphIndexTransaction.keepBackendIndexOrder(
                true, IndexType.RANGE_INT, query));

        query.limit(10L);
        Assert.assertTrue(GraphIndexTransaction.keepBackendIndexOrder(
                true, IndexType.RANGE_INT, query));
        Assert.assertFalse(GraphIndexTransaction.keepBackendIndexOrder(
                false, IndexType.RANGE_INT, query));
        Assert.assertFalse(GraphIndexTransaction.keepBackendIndexOrder(
                true, IndexType.SECONDARY, query));

        query.limit(Query.NO_LIMIT);
        query.page("");
        Assert.assertTrue(GraphIndexTransaction.keepBackendIndexOrder(
                true, IndexType.RANGE_INT, query));
    }
}
