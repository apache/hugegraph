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

package org.apache.hugegraph.id;

import java.util.UUID;

import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.StringEncoding;
import org.junit.Assert;
import org.junit.Test;

public class IdCompatibilityTest {

    @Test
    public void testNumericIdsRejectTextAndNullWithoutParsing() {
        Id id = IdGenerator.of(42L);
        Assert.assertFalse(id.equals(null));
        Assert.assertFalse(id.equals("42"));
        Assert.assertFalse(id.equals("42.0"));
        Assert.assertFalse(id.equals("not-a-number"));
        Assert.assertFalse(id.equals(IdGenerator.of("42")));
        Id same = IdGenerator.of(new byte[]{0, 0, 0, 0, 0, 0, 0, 42}, Id.IdType.LONG);
        Assert.assertEquals(id, same);
        Assert.assertEquals(id.hashCode(), same.hashCode());
    }

    @Test
    public void testUtf8IdsAgreeAcrossStringAndByteRepresentations() {
        String text = "北京`>vertex";
        Id fromString = IdGenerator.of(text);
        Id fromBytes = IdGenerator.of(StringEncoding.encode(text), Id.IdType.STRING);
        Assert.assertEquals(fromString, fromBytes);
        Assert.assertEquals(fromBytes, fromString);
        Assert.assertEquals(fromString.hashCode(), fromBytes.hashCode());
        Assert.assertEquals(0, fromString.compareTo(fromBytes));
        Assert.assertEquals(0, fromBytes.compareTo(fromString));
        Assert.assertEquals(text, fromBytes.asObject());
        Assert.assertArrayEquals(StringEncoding.encode(text), fromString.asBytes());
    }

    @Test
    public void testStoredIdsPreserveTypesAndEscapedEdgeParts() {
        Id[] ids = {
                IdGenerator.of(Long.MIN_VALUE),
                IdGenerator.of("北京`>vertex"),
                IdGenerator.of(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff")),
                new EdgeId(IdGenerator.of("owner>vertex"), Directions.OUT,
                           IdGenerator.of(1), IdGenerator.of(2), "value>with`escape",
                           IdGenerator.of(42))
        };
        for (Id id : ids) {
            Id stored = IdUtil.readStoredString(IdUtil.writeStoredString(id));
            Assert.assertEquals(id.type(), stored.type());
            Assert.assertEquals(id, stored);
            Assert.assertEquals(id.hashCode(), stored.hashCode());
        }
        Assert.assertEquals(Directions.OUT, EdgeId.directionFromCode(HugeType.EDGE_OUT.code()));
        Assert.assertEquals(Directions.IN, EdgeId.directionFromCode(HugeType.EDGE_IN.code()));
    }
}
