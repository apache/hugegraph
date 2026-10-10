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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.query.Aggregate.AggregateFunc;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.junit.Assert;
import org.junit.Test;

public class SharedQueryTest {

    @Test
    public void testOffsetAdvancesOnlyByCurrentBatchSize() {
        Query query = new Query(HugeType.VERTEX);
        query.offset(5L);
        query.limit(2L);
        Assert.assertTrue(query.skipOffsetIfNeeded(
                new LinkedHashSet<>(Arrays.asList(1, 2))).isEmpty());
        Assert.assertEquals(2L, query.actualOffset());
        Assert.assertTrue(query.skipOffsetIfNeeded(
                new LinkedHashSet<>(Arrays.asList(3, 4))).isEmpty());
        Assert.assertEquals(4L, query.actualOffset());
        Assert.assertEquals(new LinkedHashSet<>(Arrays.asList(6, 7)),
                            query.skipOffsetIfNeeded(
                                    new LinkedHashSet<>(Arrays.asList(5, 6, 7))));
        Assert.assertEquals(5L, query.actualOffset());
    }

    @Test
    public void testContradictoryIntersectionsStayEmpty() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(Condition.eq(HugeKeys.LABEL, 1));
        query.query(Condition.eq(HugeKeys.LABEL, 2));
        query.query(Condition.in(HugeKeys.LABEL, Arrays.asList(2, 3)));
        Assert.assertNull(query.condition(HugeKeys.LABEL));

        ConditionQuery emptyFirst = new ConditionQuery(HugeType.VERTEX);
        emptyFirst.query(Condition.in(HugeKeys.LABEL, Collections.emptyList()));
        emptyFirst.query(Condition.in(HugeKeys.LABEL, Arrays.asList(2, 3)));
        Assert.assertNull(emptyFirst.condition(HugeKeys.LABEL));
    }

    @Test
    public void testExplicitConditionResolutionPreservesLegacyInList() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        Assert.assertFalse(query.containsConditionValues(HugeKeys.LABEL));
        Assert.assertTrue(query.conditionValues(HugeKeys.LABEL).isEmpty());
        Assert.assertNull(query.conditionValue(HugeKeys.LABEL));

        List<Integer> labels = Arrays.asList(1, 2, 2);
        query.query(Condition.in(HugeKeys.LABEL, labels));
        Assert.assertSame(labels, query.condition(HugeKeys.LABEL));
        Assert.assertEquals(new LinkedHashSet<>(labels), query.conditionValues(HugeKeys.LABEL));
        Assert.assertNull(query.singleConditionValueOrNull(HugeKeys.LABEL));
        Assert.assertThrows(IllegalStateException.class, () -> query.conditionValue(HugeKeys.LABEL));

        query.eq(HugeKeys.LABEL, 2);
        Assert.assertEquals(Integer.valueOf(2), query.conditionValue(HugeKeys.LABEL));
        Assert.assertEquals(Integer.valueOf(2), query.singleConditionValueOrNull(HugeKeys.LABEL));
        query.eq(HugeKeys.LABEL, 3);
        Assert.assertTrue(query.containsConditionValues(HugeKeys.LABEL));
        Assert.assertTrue(query.conditionValues(HugeKeys.LABEL).isEmpty());
        Assert.assertNull(query.conditionValue(HugeKeys.LABEL));
    }

    @Test
    public void testEmptyInAndNegativeConditionAreDistinct() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.neq(HugeKeys.LABEL, 1);
        Assert.assertTrue(query.containsCondition(HugeKeys.LABEL));
        Assert.assertFalse(query.containsConditionValues(HugeKeys.LABEL));
        query.query(Condition.in(HugeKeys.LABEL, Collections.emptyList()));
        Assert.assertTrue(query.containsConditionValues(HugeKeys.LABEL));
        Assert.assertEquals(Collections.emptyList(), query.condition(HugeKeys.LABEL));
        Assert.assertNull(query.singleConditionValueOrNull(HugeKeys.LABEL));
    }

    @Test
    public void testSingleIdAndCopy() {
        Id id = IdGenerator.of(123L);
        IdQuery.OneIdQuery query = new IdQuery.OneIdQuery(HugeType.VERTEX, id);
        BaseVertex vertex = new BaseVertex(id);
        Assert.assertTrue(query.test(vertex));
        Assert.assertFalse(query.test(new BaseVertex(IdGenerator.of(124L))));
        IdQuery.OneIdQuery copy = (IdQuery.OneIdQuery) query.copy();
        query.resetIds();
        Assert.assertEquals(0, query.idsSize());
        Assert.assertTrue(query.test(vertex));
        Assert.assertEquals(id, copy.id());
        Assert.assertEquals(1, copy.idsSize());
    }

    @Test
    public void testRangeIndexValueConsumedOncePerProperty() {
        Id key = IdGenerator.of(10L);
        Id id = IdGenerator.of(123L);
        PropertyKey property = new PropertyKey(null, key, "age");
        property.dataType(DataType.INT);
        BaseVertex vertex = new BaseVertex(id);
        vertex.addProperty(property, 25);
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.query(Condition.gte(key, 20));
        query.query(Condition.lte(key, 30));
        query.recordIndexValue(key, id, 25);
        query.recordIndexValue(key, id, 17);
        query.selectedIndexField(key);
        Assert.assertTrue(query.test(vertex));
        Assert.assertEquals(1, query.getLeftIndexOfElement(id).size());
        Assert.assertEquals(Collections.singleton(17),
                            query.getLeftIndexOfElement(id).iterator().next()
                                 .indexFieldValues());
    }

    @Test
    public void testExplicitFilterAndFreshElements() {
        BaseVertex vertex = new BaseVertex(IdGenerator.of(123L));
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        AtomicInteger calls = new AtomicInteger();
        query.registerResultsFilter(element -> {
            calls.incrementAndGet();
            return false;
        });
        Assert.assertFalse(query.test(vertex));
        Assert.assertEquals(1, calls.get());
        vertex.fresh(true);
        Assert.assertTrue(query.test(vertex));
        Assert.assertEquals(1, calls.get());
        vertex.fresh(false);
        Assert.assertTrue(query.test(vertex, element -> true));
        Assert.assertEquals(1, calls.get());
    }

    @Test
    public void testAggregateDescriptorRetainsNumericStrategy() {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.aggregate(AggregateFunc.AVG, "age");
        byte[] encoded = query.bytes();
        Assert.assertTrue(new String(encoded, StandardCharsets.UTF_8)
                          .contains("\"aggregate\":{\"func\":\"AVG\",\"column\":\"age\"}"));
        ConditionQuery decoded = ConditionQuery.fromBytes(encoded);
        Assert.assertEquals(AggregateFunc.AVG, decoded.aggregateNotNull().func());
        Assert.assertEquals("age", decoded.aggregateNotNull().column());
        Assert.assertEquals(25D, decoded.aggregateNotNull()
                                        .reduce(Arrays.<Number>asList(20, 30).iterator())
                                        .doubleValue(), 0D);
        query.aggregate(AggregateFunc.COUNT, null);
        decoded = ConditionQuery.fromBytes(query.bytes());
        Assert.assertTrue(decoded.aggregateNotNull().countAll());
        Assert.assertEquals(0L, decoded.aggregateNotNull().defaultValue());
    }
}
