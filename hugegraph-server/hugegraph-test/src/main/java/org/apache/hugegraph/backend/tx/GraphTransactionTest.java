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

import java.util.Collections;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.ConditionQuery.OptimizedType;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.junit.Test;

public class GraphTransactionTest {

    @Test
    public void testQueryNeedsPostFilter() {
        Id key = IdGenerator.of(1);
        ConditionQuery search = new ConditionQuery(HugeType.EDGE);
        search.query(Condition.textContains(key, "word"));

        Assert.assertTrue(GraphTransaction.queryNeedsPostFilter(search));
        IdQuery searchIds = new IdQuery(search, IdGenerator.of(2));
        Assert.assertTrue(GraphTransaction.queryNeedsPostFilter(searchIds));

        ConditionQuery searchAny = new ConditionQuery(HugeType.EDGE);
        searchAny.query(Condition.textContainsAny(
                        key, Collections.singleton("word")));
        Assert.assertTrue(GraphTransaction.queryNeedsPostFilter(searchAny));

        ConditionQuery exact = new ConditionQuery(HugeType.EDGE);
        exact.query(Condition.eq(key, "word"));
        Assert.assertFalse(GraphTransaction.queryNeedsPostFilter(exact));
        exact.optimized(OptimizedType.INDEX_FILTER);
        Assert.assertTrue(GraphTransaction.queryNeedsPostFilter(exact));

        ConditionQuery index = new ConditionQuery(HugeType.EDGE);
        index.query(Condition.eq(key, "word"));
        index.optimized(OptimizedType.INDEX);
        Assert.assertTrue(GraphTransaction.queryNeedsPostFilter(index));

        ConditionQuery labelIndex = new ConditionQuery(HugeType.EDGE);
        labelIndex.query(Condition.eq(HugeKeys.LABEL, IdGenerator.of(2)));
        labelIndex.query(Condition.eq(key, "word"));
        labelIndex.optimized(OptimizedType.INDEX);
        Assert.assertFalse(GraphTransaction.queryNeedsPostFilter(labelIndex));
        IdQuery labelIndexIds = new IdQuery(labelIndex, IdGenerator.of(2));
        Assert.assertFalse(GraphTransaction.queryNeedsPostFilter(labelIndexIds));

        ConditionQuery vertexLabelIndex =
                new ConditionQuery(HugeType.VERTEX);
        vertexLabelIndex.query(Condition.eq(HugeKeys.LABEL,
                                            IdGenerator.of(2)));
        vertexLabelIndex.query(Condition.eq(key, "word"));
        vertexLabelIndex.optimized(OptimizedType.INDEX);
        Assert.assertTrue(GraphTransaction.queryNeedsPostFilter(
                          vertexLabelIndex));

        ConditionQuery primaryKey = new ConditionQuery(HugeType.VERTEX);
        primaryKey.optimized(OptimizedType.PRIMARY_KEY);
        Assert.assertTrue(GraphTransaction.queryNeedsPostFilter(primaryKey));

        ConditionQuery sortKeys = new ConditionQuery(HugeType.EDGE);
        sortKeys.query(Condition.eq(key, "word"));
        sortKeys.optimized(OptimizedType.SORT_KEYS);
        Assert.assertTrue(GraphTransaction.queryNeedsPostFilter(sortKeys));
        Assert.assertFalse(GraphTransaction.queryNeedsPostFilter(
                           new Query(HugeType.EDGE)));
    }
}
