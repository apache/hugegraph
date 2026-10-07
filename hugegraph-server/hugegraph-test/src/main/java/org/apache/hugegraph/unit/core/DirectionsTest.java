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

import org.apache.hugegraph.util.TinkerPopUtil;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.Test;

public class DirectionsTest {

    @Test
    public void testString() {
        Assert.assertEquals("out", Directions.OUT.string());
        Assert.assertEquals("in", Directions.IN.string());
        Assert.assertEquals("both", Directions.BOTH.string());
    }

    @Test
    public void testType() {
        Assert.assertEquals(HugeType.EDGE_OUT, Directions.OUT.type());
        Assert.assertEquals(HugeType.EDGE_IN, Directions.IN.type());
        Assert.assertThrows(IllegalArgumentException.class, Directions.BOTH::type);
    }

    @Test
    public void testFromHugeType() {
        Assert.assertEquals(Directions.OUT,
                            Directions.convert(HugeType.EDGE_OUT));
        Assert.assertEquals(Directions.IN,
                            Directions.convert(HugeType.EDGE_IN));
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Directions.convert(HugeType.EDGE);
        });
    }

    @Test
    public void testOpposite() {
        Assert.assertEquals(Directions.IN, Directions.OUT.opposite());
        Assert.assertEquals(Directions.OUT, Directions.IN.opposite());
        Assert.assertEquals(Directions.BOTH, Directions.BOTH.opposite());
    }

    @Test
    public void testToDirection() {
        Assert.assertEquals(Direction.OUT, TinkerPopUtil.direction(Directions.OUT));
        Assert.assertEquals(Direction.IN, TinkerPopUtil.direction(Directions.IN));
        Assert.assertEquals(Direction.BOTH, TinkerPopUtil.direction(Directions.BOTH));
    }

    @Test
    public void testFromDirection() {
        Assert.assertEquals(Directions.OUT, TinkerPopUtil.direction(Direction.OUT));
        Assert.assertEquals(Directions.IN, TinkerPopUtil.direction(Direction.IN));
        Assert.assertEquals(Directions.BOTH,
                            TinkerPopUtil.direction(Direction.BOTH));
    }
}
