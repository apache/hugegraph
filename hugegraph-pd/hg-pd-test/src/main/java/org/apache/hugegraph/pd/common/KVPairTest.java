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

package org.apache.hugegraph.pd.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KVPairTest {

    KVPair<String, Integer> pair;

    @Before
    public void init() {
        this.pair = new KVPair<>("key", 1);
    }

    @Test
    public void testGetKey() {
        assertEquals(this.pair.getKey(), "key");
    }

    @Test
    public void testSetKey() {
        this.pair.setKey("key2");
        assertEquals(this.pair.getKey(), "key2");
    }

    @Test
    public void testGetValue() {
        assertEquals(1, this.pair.getValue());
    }

    @Test
    public void testSetValue() {
        this.pair.setValue(2);
        assertEquals(2, this.pair.getValue());
    }

    @Test
    public void testToString() {

    }

    @Test
    public void testHashCode() {
        Assert.assertEquals("key".hashCode() * 13 + 1, this.pair.hashCode());
        Assert.assertEquals(new KVPair<>("key", 1).hashCode(), this.pair.hashCode());

        KVPair<String, Integer> nullKey = new KVPair<>(null, 1);
        Assert.assertEquals(1, nullKey.hashCode());
        Assert.assertEquals(new KVPair<>(null, 1), nullKey);
        Assert.assertEquals(new KVPair<String, Integer>(null, 1).hashCode(),
                            nullKey.hashCode());
    }

    @Test
    public void testEquals() {
        var pair2 = new KVPair<>("key", 1);
        Assert.assertEquals(pair2, this.pair);
    }
}
