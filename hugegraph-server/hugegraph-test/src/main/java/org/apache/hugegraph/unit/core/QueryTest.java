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

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.Aggregate.AggregateFunc;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.IdPrefixQuery;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.query.IdRangeQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.Query.Order;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class QueryTest {

    @Test
    public void testOrderBy() {
        Query query = new Query(HugeType.VERTEX);
        Assert.assertTrue(query.orders().isEmpty());

        query.order(HugeKeys.NAME, Order.ASC);
        Assert.assertEquals(ImmutableMap.of(HugeKeys.NAME, Order.ASC),
                            query.orders());
    }

    @Test
    public void testConditionWithEqAndIn() {
        Id label1 = IdGenerator.of(1);
        Id label2 = IdGenerator.of(2);

        ConditionQuery query = new ConditionQuery(HugeType.EDGE);
        query.eq(HugeKeys.LABEL, label1);
        query.query(Condition.in(HugeKeys.LABEL,
                                 ImmutableList.of(label1, label2)));

        Assert.assertEquals(label1, query.condition(HugeKeys.LABEL));
    }

    @Test
    public void testConditionWithConflictingEqAndIn() {
        Id label1 = IdGenerator.of(1);
        Id label2 = IdGenerator.of(2);
        Id label3 = IdGenerator.of(3);

        ConditionQuery query = new ConditionQuery(HugeType.EDGE);
        query.eq(HugeKeys.LABEL, label1);
        query.eq(HugeKeys.LABEL, label2);
        query.query(Condition.in(HugeKeys.LABEL,
                                 ImmutableList.of(label1, label3)));

        Assert.assertNull(query.condition(HugeKeys.LABEL));
    }

    @Test
    public void testConditionWithMultipleMatchedInValues() {
        Id label1 = IdGenerator.of(1);
        Id label2 = IdGenerator.of(2);
        Id label3 = IdGenerator.of(3);
        Id label4 = IdGenerator.of(4);

        ConditionQuery query = new ConditionQuery(HugeType.EDGE);
        query.query(Condition.in(HugeKeys.LABEL,
                                 ImmutableList.of(label1, label2, label3)));
        query.query(Condition.in(HugeKeys.LABEL,
                                 ImmutableList.of(label1, label2, label4)));

        Assert.assertThrows(IllegalStateException.class,
                            () -> query.condition(HugeKeys.LABEL),
                            e -> Assert.assertContains("Illegal key 'LABEL'",
                                                       e.getMessage()));
    }

    @Test
    public void testToString() {
        Query query = new Query(HugeType.VERTEX);
        Assert.assertEquals("`Query * from VERTEX`", query.toString());

        query.page("p1");
        Assert.assertEquals("`Query * from VERTEX page 'p1'`",
                            query.toString());

        query = new Query(HugeType.VERTEX);
        query.limit(10L);
        Assert.assertEquals("`Query * from VERTEX limit 10`", query.toString());

        query = new Query(HugeType.VERTEX);
        query.aggregate(AggregateFunc.COUNT, null);
        query.limit(10L);
        Assert.assertEquals("`Query count(*) from VERTEX limit 10`",
                            query.toString());

        query = new Query(HugeType.VERTEX);
        query.aggregate(AggregateFunc.MAX, "age");
        query.limit(10L);
        Assert.assertEquals("`Query max(age) from VERTEX limit 10`",
                            query.toString());

        query = new Query(HugeType.VERTEX);
        query.page("p2");
        query.limit(10L);
        Assert.assertEquals("`Query * from VERTEX page 'p2', limit 10`",
                            query.toString());

        query = new Query(HugeType.VERTEX);
        query.page("p3");
        query.offset(100L);
        query.limit(10L);
        Assert.assertEquals("`Query * from VERTEX page 'p3', offset 100, " +
                            "limit 10`", query.toString());

        query = new Query(HugeType.VERTEX);
        query.page("");
        query.offset(100L);
        query.limit(10L);
        query.order(HugeKeys.NAME, Order.ASC);
        query.order(HugeKeys.FIELDS, Order.DESC);
        Assert.assertEquals("`Query * from VERTEX page '', offset 100, " +
                            "limit 10, order by {NAME=ASC, FIELDS=DESC}`",
                            query.toString());

        IdQuery query2 = new IdQuery(HugeType.VERTEX, IdGenerator.of(1));
        query2.query(IdGenerator.of(3));
        query2.limit(10L);
        Assert.assertEquals("`Query * from VERTEX limit 10 where id in [1, 3]`",
                            query2.toString());

        ConditionQuery query3 = new ConditionQuery(HugeType.EDGE);
        query3.eq(HugeKeys.LABEL, 3);
        query3.gt(HugeKeys.PROPERTIES, 10);
        query3.lt(HugeKeys.PROPERTIES, 18);
        query3.limit(10L);
        String qStr = query3.toString();
        Assert.assertTrue(qStr.contains("`Query * from EDGE limit 10 where ["));
        Assert.assertTrue(qStr.contains("LABEL == 3"));
        Assert.assertTrue(qStr.contains("PROPERTIES > 10"));
        Assert.assertTrue(qStr.contains("PROPERTIES < 18"));

        ConditionQuery query4 = new ConditionQuery(HugeType.EDGE);
        query4.query(ImmutableSet.of(IdGenerator.of(1), IdGenerator.of(3)));
        query4.eq(HugeKeys.LABEL, 3);
        query4.lt(HugeKeys.PROPERTIES, 18);
        query4.limit(10L);
        qStr = query4.toString();
        Assert.assertTrue(qStr.contains("`Query * from EDGE limit 10 " +
                                        "where id in [1, 3] and ["));
        Assert.assertTrue(qStr.contains("LABEL == 3"));
        Assert.assertTrue(qStr.contains("PROPERTIES < 18"));
    }

    @Test
    public void testToStringOfIdRangeQuery() {
        IdRangeQuery query = new IdRangeQuery(HugeType.EDGE,
                                              IdGenerator.of(1),
                                              IdGenerator.of(3));
        query.limit(5L);
        Assert.assertEquals("`Query * from EDGE limit 5 where id in range " +
                            "[1, 3)`", query.toString());

        query = new IdRangeQuery(HugeType.EDGE, query,
                                 IdGenerator.of(1), true,
                                 IdGenerator.of(3), true);
        Assert.assertEquals("`Query * from EDGE limit 5 where id in range " +
                            "[1, 3]`", query.toString());

        query = new IdRangeQuery(HugeType.EDGE, null,
                                 IdGenerator.of(1), false,
                                 IdGenerator.of(3), true);
        Assert.assertEquals("`Query * from EDGE where id in range (1, 3]`",
                            query.toString());
    }

    @Test
    public void testToStringOfIdPrefixQuery() {
        IdPrefixQuery query = new IdPrefixQuery(HugeType.EDGE,
                                                IdGenerator.of(1));
        query.limit(5L);
        Assert.assertEquals("`Query * from EDGE limit 5 where id prefix " +
                            "with 1`", query.toString());

        query = new IdPrefixQuery(query,
                                  IdGenerator.of(12),
                                  IdGenerator.of(1));
        Assert.assertEquals("`Query * from EDGE limit 5 where id prefix " +
                            "with 1 and start with 12(inclusive)`",
                            query.toString());

        query = new IdPrefixQuery(query,
                                  IdGenerator.of(12), false,
                                  IdGenerator.of(1));
        Assert.assertEquals("`Query * from EDGE limit 5 where id prefix " +
                            "with 1 and start with 12(exclusive)`",
                            query.toString());
    }
}
