/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit.perf;

import org.junit.Test;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.perf.LightStopwatch;
import org.apache.hugegraph.perf.NormalStopwatch;
import org.apache.hugegraph.perf.PerfUtil.FastMap;
import org.apache.hugegraph.perf.Stopwatch;
import org.apache.hugegraph.perf.Stopwatch.Path;
import org.apache.hugegraph.unit.BaseUnitTest;

public class StopwatchTest extends BaseUnitTest {

    @Test
    public void testNormalStopwatchChild() {
        Stopwatch watch1 = new NormalStopwatch("w1", Path.EMPTY);

        Stopwatch watch2 = new NormalStopwatch("w2", watch1);
        Stopwatch watch3 = new NormalStopwatch("w3", watch1);
        Stopwatch watch4 = new NormalStopwatch("w4", watch1);
        Stopwatch watch5 = new NormalStopwatch("w5", watch1);

        Assert.assertEquals(watch2, watch1.child("w2"));
        Assert.assertEquals(watch3, watch1.child("w3"));
        Assert.assertEquals(watch4, watch1.child("w4"));
        Assert.assertEquals(watch5, watch1.child("w5"));

        Assert.assertEquals(watch2, watch1.child("w2", null));
        Assert.assertEquals(watch3, watch1.child("w3", null));
        Assert.assertEquals(watch4, watch1.child("w4", null));
        Assert.assertEquals(watch5, watch1.child("w5", null));

        Assert.assertNull(watch1.child("w2"));
        Assert.assertNull(watch1.child("w3"));
        Assert.assertNull(watch1.child("w4"));
        Assert.assertNull(watch1.child("w5"));

        Assert.assertNull(watch1.child("w2", watch2));
        Assert.assertNull(watch1.child("w3", watch3));
        Assert.assertNull(watch1.child("w4", watch4));
        Assert.assertNull(watch1.child("w5", watch5));

        watch1.clear();
        Assert.assertNull(watch1.child("w2"));
        Assert.assertNull(watch1.child("w3"));
        Assert.assertNull(watch1.child("w4"));
        Assert.assertNull(watch1.child("w5"));
    }

    @Test
    public void testLightStopwatchChild() {
        Stopwatch watch1 = new LightStopwatch("w1", Path.EMPTY);

        Stopwatch watch2 = new LightStopwatch("w2", watch1);
        Stopwatch watch3 = new LightStopwatch("w3", watch1);
        Stopwatch watch4 = new LightStopwatch("w4", watch1);
        Stopwatch watch5 = new LightStopwatch("w5", watch1);

        Assert.assertEquals(watch2, watch1.child("w2"));
        Assert.assertEquals(watch3, watch1.child("w3"));
        Assert.assertEquals(watch4, watch1.child("w4"));
        Assert.assertEquals(watch5, watch1.child("w5"));

        Assert.assertEquals(watch2, watch1.child("w2", null));
        Assert.assertEquals(watch3, watch1.child("w3", null));
        Assert.assertEquals(watch4, watch1.child("w4", null));
        Assert.assertEquals(watch5, watch1.child("w5", null));

        Assert.assertNull(watch1.child("w2"));
        Assert.assertNull(watch1.child("w3"));
        Assert.assertNull(watch1.child("w4"));
        Assert.assertNull(watch1.child("w5"));

        Assert.assertNull(watch1.child("w2", watch2));
        Assert.assertNull(watch1.child("w3", watch3));
        Assert.assertNull(watch1.child("w4", watch4));
        Assert.assertNull(watch1.child("w5", watch5));

        watch1.clear();
        Assert.assertNull(watch1.child("w2"));
        Assert.assertNull(watch1.child("w3"));
        Assert.assertNull(watch1.child("w4"));
        Assert.assertNull(watch1.child("w5"));
    }

    @Test
    public void testChildReplacedUnderSameNameIsReturned() {
        Stopwatch parent = new NormalStopwatch("w1", Path.EMPTY);
        Stopwatch first = new NormalStopwatch("w2", parent);
        Assert.assertSame(first, parent.child("w2"));

        Stopwatch second = new NormalStopwatch("w2", parent);
        Assert.assertSame(second, parent.child("w2"));

        Stopwatch other = new NormalStopwatch("w3", parent);
        Assert.assertSame(other, parent.child("w3"));
        Assert.assertSame(second, parent.child("w2"));
    }

    @Test
    public void testPathEqualsNull() {
        Path path = new Path("a");
        Assert.assertFalse(path.equals(null));
        Assert.assertFalse(path.equals("a"));
        Assert.assertTrue(path.equals(new Path("a")));
    }

    @Test
    public void testFastMapReplaceEqualKey() {
        FastMap<String, String> map = new FastMap<>();
        String[] keys = {new String("a"), new String("b"),
                         new String("c"), new String("d")};
        for (String key : keys) {
            map.put(key, "first");
        }
        for (String key : keys) {
            String equal = new String(key);
            Assert.assertEquals("first", map.put(equal, "second"));
            Assert.assertEquals("second", map.get(key));
            Assert.assertEquals("second", map.get(equal));
        }
        Assert.assertEquals(4, map.size());
    }

    @Test
    public void testFastMapRemoveEqualKey() {
        FastMap<String, String> map = new FastMap<>();
        String[] keys = {new String("a"), new String("b"),
                         new String("c"), new String("d")};
        for (String key : keys) {
            map.put(key, "first");
        }
        for (String key : keys) {
            Assert.assertEquals("first", map.remove(new String(key)));
            Assert.assertFalse(map.containsKey(key));
            Assert.assertNull(map.get(key));
        }
        map.put("other", "second");
        Assert.assertEquals("second", map.get("other"));
    }

    @Test
    public void testFastMapNullKey() {
        FastMap<String, String> map = new FastMap<>();
        map.put(null, "null-value");
        map.put("first", "first-value");
        Assert.assertEquals("null-value", map.get(null));
        Assert.assertEquals("null-value", map.put(null, "updated"));
        Assert.assertEquals("updated", map.remove(null));
        Assert.assertNull(map.get(null));
        Assert.assertEquals("first-value", map.get("first"));
    }
}
