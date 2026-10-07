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
import java.util.List;

import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.unit.FakeObjects;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class HugePrimaryKeyTest {

    @Test
    public void testEmptyPrimaryKeyStillRejected() {
        Fixture fixture = new Fixture(true, "");
        IllegalArgumentException error = Assert.assertThrows(IllegalArgumentException.class,
                                                            () -> fixture.vertex.assignId(null));
        Assert.assertTrue(error.getMessage().contains("primary key can't be empty"));
    }

    @Test
    public void testReservedLeadingSymbolsStillRejected() {
        for (char value = 0; value <= 3; value++) {
            Fixture fixture = new Fixture(true, value + "value");
            IllegalArgumentException error = Assert.assertThrows(IllegalArgumentException.class,
                                                                () -> fixture.vertex.assignId(null));
            Assert.assertTrue(error.getMessage().contains("Illegal leading char"));
        }
    }

    @Test
    public void testEnginePrimaryIdMatchesActualQueryOptimizer() {
        Fixture escaped = new Fixture(true, "a!b", "left>right:tail`x");
        escaped.vertex.assignId(null);
        Assert.assertEquals(escaped.vertex.id(), escaped.optimizedQueryId(false));
        Fixture numeric = new Fixture(true, 42);
        numeric.vertex.assignId(null);
        Assert.assertEquals(numeric.vertex.id(), numeric.optimizedQueryId(false));
    }

    @Test
    public void testCompositeEmptyFieldRetainsLegacyEncodedName() {
        Fixture fixture = new Fixture(true, "first", "", "a!b");
        fixture.vertex.assignId(null);
        Assert.assertEquals("1:first!!a`!b", fixture.vertex.id().asString());
        Assert.assertEquals(fixture.vertex.id(), fixture.optimizedQueryId(true));
    }

    @Test
    public void testNumberEncodingOptionRetainsPlainNumberMode() {
        Fixture plain = new Fixture(false, 42);
        plain.vertex.assignId(null);
        Assert.assertEquals("1:42", plain.vertex.id().asString());
        Assert.assertEquals(plain.vertex.id(), plain.optimizedQueryId(true));
        Fixture encoded = new Fixture(true, 42);
        encoded.vertex.assignId(null);
        Assert.assertNotEquals(plain.vertex.id(), encoded.vertex.id());
    }

    private static final class Fixture {

        private final FakeObjects objects = new FakeObjects();
        private final List<PropertyKey> keys = new ArrayList<>();
        private final Object[] values;
        private final boolean encodeNumber;
        private final VertexLabel label;
        private final HugeVertex vertex;

        private Fixture(boolean encodeNumber, Object... values) {
            this.values = values;
            this.encodeNumber = encodeNumber;
            Mockito.doReturn(encodeNumber).when(this.objects.graph()).option(CoreOptions.VERTEX_ENCODE_PK_NUMBER);
            for (int i = 0; i < values.length; i++) {
                this.keys.add(this.objects.newPropertyKey(IdGenerator.of(i + 1), "key" + i,
                                                          values[i] instanceof Number ? DataType.INT : DataType.TEXT));
            }
            Id[] ids = this.keys.stream().map(PropertyKey::id).toArray(Id[]::new);
            this.label = this.objects.newVertexLabel(IdGenerator.of(1), "person", IdStrategy.PRIMARY_KEY, ids);
            this.label.primaryKeys(ids);
            this.vertex = new HugeVertex(this.objects.graph(), null, this.label);
            for (int i = 0; i < values.length; i++) {
                this.vertex.addProperty(this.keys.get(i), values[i]);
            }
        }

        private Id optimizedQueryId(boolean serializedValues) {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            query.eq(HugeKeys.LABEL, this.label.id());
            for (int i = 0; i < this.values.length; i++) {
                Condition.Relation relation = Condition.eq(this.keys.get(i).id(), this.values[i]);
                if (serializedValues) {
                    Object value = this.keys.get(i).serialValue(this.values[i], this.encodeNumber);
                    relation.serialValue("".equals(value) ? ConditionQuery.INDEX_VALUE_EMPTY : value);
                }
                query.query(relation);
            }
            GraphTransaction tx = Mockito.mock(GraphTransaction.class, Mockito.CALLS_REAL_METHODS);
            Mockito.doReturn(this.objects.graph()).when(tx).graph();
            Query optimized = Whitebox.invoke(GraphTransaction.class, new Class[]{ConditionQuery.class},
                                               "optimizeQuery", tx, query);
            return optimized.ids().iterator().next();
        }
    }
}
