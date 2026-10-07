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

package org.apache.hugegraph.unit.id;

import java.util.Set;

import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class EdgeIdTest extends BaseUnitTest {

    @Before
    public void setup() {
        // pass
    }

    @After
    public void teardown() {
        // pass
    }

    @Test
    public void testEdgeIdEqual() {
        EdgeId edgeId1 = new EdgeId(IdGenerator.of("1:marko"), Directions.OUT,
                                    IdGenerator.of(1),
                                    IdGenerator.of(1), "",
                                    IdGenerator.of("1:josh"));
        EdgeId edgeId2 = new EdgeId(IdGenerator.of("1:marko"), Directions.OUT,
                                    IdGenerator.of(1),
                                    IdGenerator.of(1), "",
                                    IdGenerator.of("1:josh"));
        EdgeId edgeId3 = new EdgeId(IdGenerator.of("1:josh"), Directions.IN,
                                    IdGenerator.of(1),
                                    IdGenerator.of(1), "",
                                    IdGenerator.of("1:marko"));
        Assert.assertTrue(edgeId1.equals(edgeId2));
        Assert.assertTrue(edgeId2.equals(edgeId1));
        Assert.assertTrue(edgeId1.equals(edgeId3));
        Assert.assertTrue(edgeId3.equals(edgeId1));
    }

    @Test
    public void testEdgeIdEqualWithDirection() {
        EdgeId edgeId1 = new EdgeId(IdGenerator.of("1:marko"), Directions.OUT,
                                    IdGenerator.of(1),
                                    IdGenerator.of(1), "",
                                    IdGenerator.of("1:josh"), true);
        EdgeId edgeId2 = new EdgeId(IdGenerator.of("1:marko"), Directions.OUT,
                                    IdGenerator.of(1),
                                    IdGenerator.of(1), "",
                                    IdGenerator.of("1:josh"), true);
        EdgeId edgeId3 = new EdgeId(IdGenerator.of("1:josh"), Directions.IN,
                                    IdGenerator.of(1),
                                    IdGenerator.of(1), "",
                                    IdGenerator.of("1:marko"), true);
        Assert.assertTrue(edgeId1.equals(edgeId2));
        Assert.assertTrue(edgeId2.equals(edgeId1));
        Assert.assertFalse(edgeId1.equals(edgeId3));
        Assert.assertFalse(edgeId3.equals(edgeId1));
    }

    @Test
    public void testCollectionContainsEdgeId() {
        EdgeId edgeId1 = new EdgeId(IdGenerator.of("1:marko"), Directions.OUT,
                                    IdGenerator.of(1),
                                    IdGenerator.of(1), "",
                                    IdGenerator.of("1:josh"));
        Set<Id> edgeIds = ImmutableSet.of(edgeId1);
        Assert.assertTrue(edgeIds.contains(edgeId1));

        EdgeId edgeId2 = new EdgeId(IdGenerator.of("1:marko"), Directions.OUT,
                                    IdGenerator.of(1),
                                    IdGenerator.of(1), "",
                                    IdGenerator.of("1:josh"));
        Assert.assertTrue(edgeIds.contains(edgeId2));

        EdgeId edgeId3 = new EdgeId(IdGenerator.of("1:josh"), Directions.IN,
                                    IdGenerator.of(1),
                                    IdGenerator.of(1), "",
                                    IdGenerator.of("1:marko"));
        Assert.assertTrue(edgeIds.contains(edgeId3));
    }

    @Test
    public void testCollectionContainsEdgeIdWithDirection() {
        EdgeId edgeId1 = new EdgeId(IdGenerator.of("1:marko"), Directions.OUT,
                                    IdGenerator.of(1),
                                    IdGenerator.of(1), "",
                                    IdGenerator.of("1:josh"), true);
        Set<Id> edgeIds = ImmutableSet.of(edgeId1);
        Assert.assertTrue(edgeIds.contains(edgeId1));

        EdgeId edgeId2 = new EdgeId(IdGenerator.of("1:marko"), Directions.OUT,
                                    IdGenerator.of(1),
                                    IdGenerator.of(1), "",
                                    IdGenerator.of("1:josh"), true);
        Assert.assertTrue(edgeIds.contains(edgeId2));

        EdgeId edgeId3 = new EdgeId(IdGenerator.of("1:josh"), Directions.IN,
                                    IdGenerator.of(1),
                                    IdGenerator.of(1), "",
                                    IdGenerator.of("1:marko"), true);
        Assert.assertFalse(edgeIds.contains(edgeId3));
    }
}
