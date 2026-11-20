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

package org.apache.hugegraph.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.junit.Assert;
import org.junit.Test;

public class ConditionQueryCodecTest {

    private static final Id NAME = IdGenerator.of(1L);

    @Test
    public void testUserpropStringValueOutStaysString() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(Condition.eq(NAME, "OUT"));

        ConditionQuery decoded = ConditionQuery.fromBytes(query.bytes());

        Object value = decoded.relations().iterator().next().value();
        Assert.assertEquals("OUT", value);
    }

    @Test
    public void testUserpropTextContainsInStillTestsAfterRoundTrip() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(Condition.textContains(NAME, "IN"));

        ConditionQuery decoded = ConditionQuery.fromBytes(query.bytes());

        Condition.Relation relation = decoded.relations().iterator().next();
        Assert.assertEquals("IN", relation.value());
        Assert.assertTrue(relation.test("LOGIN"));
    }

    @Test
    public void testSyspropNameValueInStaysString() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX_LABEL);
        query.query(Condition.eq(HugeKeys.NAME, "IN"));

        ConditionQuery decoded = ConditionQuery.fromBytes(query.bytes());

        Assert.assertEquals("IN", decoded.condition(HugeKeys.NAME));
    }

    @Test
    public void testDirectionValueStillDecodesToDirections() {
        ConditionQuery query = new ConditionQuery(HugeType.EDGE);
        query.query(Condition.eq(HugeKeys.DIRECTION, Directions.OUT));

        ConditionQuery decoded = ConditionQuery.fromBytes(query.bytes());

        Assert.assertEquals(Directions.OUT, decoded.condition(HugeKeys.DIRECTION));

        query = new ConditionQuery(HugeType.EDGE);
        query.query(Condition.eq(HugeKeys.DIRECTION, Directions.IN));
        decoded = ConditionQuery.fromBytes(query.bytes());
        Assert.assertEquals(Directions.IN, decoded.condition(HugeKeys.DIRECTION));
    }

    @Test
    public void testEmptyInListRoundTrips() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(Condition.in(NAME, new ArrayList<>()));

        ConditionQuery decoded = ConditionQuery.fromBytes(query.bytes());

        Object value = decoded.relations().iterator().next().value();
        Assert.assertTrue(value instanceof List);
        Assert.assertTrue(((List<?>) value).isEmpty());
    }

    @Test
    public void testUnsetConditionOnEmptyQueryIsNoop() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.unsetCondition(HugeKeys.LABEL);
        Assert.assertTrue(query.conditions().isEmpty());

        query.query(Condition.eq(HugeKeys.LABEL, 1));
        Assert.assertEquals(1, query.conditions().size());
    }

    @Test
    public void testResetUserpropConditionsOnEmptyQueryIsNoop() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.resetUserpropConditions();
        Assert.assertTrue(query.conditions().isEmpty());
    }

    @Test
    public void testRemoveConditionOnEmptyQueryIsNoop() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.removeCondition(Condition.eq(HugeKeys.LABEL, 1));
        Assert.assertTrue(query.conditions().isEmpty());
    }
}
