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

package org.apache.hugegraph.store.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.hugegraph.store.term.Bits;
import org.apache.hugegraph.store.term.HgPair;
import org.apache.hugegraph.store.term.HgTriple;
import org.junit.Before;
import org.junit.Test;

public class HgPairTest {

    private HgPair<String, String> pair;
    private HgTriple<String, String, String> triple;

    @Before
    public void setUp() {
        pair = new HgPair<>("key", "value");
        triple = new HgTriple<>("x", "y", "z");
    }

    @Test
    public void testPair() {
        int hashCode = pair.hashCode();
        pair.toString();
        pair.setKey("key1");
        pair.setValue("value1");
        pair.getKey();
        pair.getValue();
        assertEquals(new HgPair<>("key1", "value1"), pair);
        var pair2 = new HgPair<>();
        pair2.setKey("key1");
        pair2.hashCode();
        assertNotEquals(pair2, pair);
        triple.getZ();
        triple.getX();
        triple.getY();
        triple.toString();
        triple.hashCode();
        triple.hashCode();
        assertEquals(triple, new HgTriple<>("x", "y", "z"));
        assertNotEquals(pair2, triple);
    }

    @Test
    public void testHashCodeWithNullKey() {
        HgPair<String, String> nullKey = new HgPair<>(null, "value");
        assertEquals(new HgPair<>(null, "value"), nullKey);
        assertEquals("value".hashCode(), nullKey.hashCode());
        assertEquals(new HgPair<>(null, "value").hashCode(), nullKey.hashCode());
        assertEquals("key".hashCode() * 13 + "value".hashCode(), pair.hashCode());
    }

    @Test
    public void testBits() {
        byte[] buf = new byte[4];
        Bits.putInt(buf, 0, 3);
        int i = Bits.getInt(buf, 0);
        assertEquals(3, i);
        buf = new byte[2];
        Bits.putShort(buf, 0, 2);
        int s = Bits.getShort(buf, 0);
        assertEquals(2, s);
        buf = new byte[4];
        Bits.put(buf, 0, new byte[]{0, 0, 0, 66});
        int toInt = Bits.toInt(buf);
        assertEquals(66, toInt);
    }
}
