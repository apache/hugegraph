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

import java.util.List;

import org.apache.hugegraph.backend.store.BackendTable.ShardSplitter.Range;
import org.apache.hugegraph.backend.Shard;
import org.apache.hugegraph.testutil.Assert;
import org.apache.logging.log4j.util.Strings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RangeTest {

    private static final byte[] START = new byte[]{0x0};
    private static final byte[] END = new byte[]{
            -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1
    };

    @Before
    public void setup() {
        // pass
    }

    @After
    public void teardown() {
        // pass
    }

    @Test
    public void testRangeOfOnlyOneRegion() {
        // The startKey and endKey is "", if it's the only region of table
        Range range = new Range(START, END);

        List<Shard> shards = range.splitEven(0);
        Assert.assertEquals(1, shards.size());
        Assert.assertEquals(Strings.EMPTY, shards.get(0).start());
        Assert.assertEquals(Strings.EMPTY, shards.get(0).end());

        shards = range.splitEven(1);
        Assert.assertEquals(1, shards.size());
        Assert.assertEquals(Strings.EMPTY, shards.get(0).start());
        Assert.assertEquals(Strings.EMPTY, shards.get(0).end());

        shards = range.splitEven(3);
        Assert.assertEquals(3, shards.size());
        Assert.assertEquals(Strings.EMPTY, shards.get(0).start());
        Assert.assertEquals("VVVVVVVVVVVVVVVVVVVVVQ==", shards.get(0).end());
        Assert.assertEquals("VVVVVVVVVVVVVVVVVVVVVQ==", shards.get(1).start());
        Assert.assertEquals("qqqqqqqqqqqqqqqqqqqqqg==", shards.get(1).end());
        Assert.assertEquals("qqqqqqqqqqqqqqqqqqqqqg==", shards.get(2).start());
        Assert.assertEquals(Strings.EMPTY, shards.get(2).end());

        for (int i = 4; i < 100; i++) {
            range.splitEven(i);
        }
    }

    @Test
    public void testRangeOfRegionWithEndKey() {
        byte[] end = new byte[]{-3, 0x31, 0x30, 0x30, 0x30, 0x77,
                                0x20, 0x09, 0x38, 0x31, 0x33, 0x32, 0x35};
        Range range = new Range(START, end);

        List<Shard> shards = range.splitEven(0);
        Assert.assertEquals(1, shards.size());
        Assert.assertEquals(Strings.EMPTY, shards.get(0).start());
        Assert.assertEquals("/TEwMDB3IAk4MTMyNQ==", shards.get(0).end());

        shards = range.splitEven(1);
        Assert.assertEquals(1, shards.size());
        Assert.assertEquals(Strings.EMPTY, shards.get(0).start());
        Assert.assertEquals("/TEwMDB3IAk4MTMyNQ==", shards.get(0).end());

        shards = range.splitEven(2);
        Assert.assertEquals(3, shards.size());
        Assert.assertEquals(Strings.EMPTY, shards.get(0).start());
        Assert.assertEquals("fpiYGBg7kAScGJmZGg==", shards.get(0).end());
        Assert.assertEquals("fpiYGBg7kAScGJmZGg==", shards.get(1).start());
        Assert.assertEquals("/TEwMDB3IAk4MTMyNA==", shards.get(1).end());
        Assert.assertEquals("/TEwMDB3IAk4MTMyNA==", shards.get(2).start());
        Assert.assertEquals("/TEwMDB3IAk4MTMyNQ==", shards.get(2).end());

        for (int i = 3; i < 100; i++) {
            range.splitEven(i);
        }
    }

    @Test
    public void testRangeOfRegionWithStartKey() {
        byte[] start = new byte[]{-3, 0x35, 0x30, 0x30,
                                  0x30, 0x77, 0x4e, -37,
                                  0x31, 0x31, 0x30, 0x30,
                                  0x30, 0x37, 0x36, 0x33};
        Range range = new Range(start, END);

        List<Shard> shards = range.splitEven(0);
        Assert.assertEquals(1, shards.size());
        Assert.assertEquals("/TUwMDB3TtsxMTAwMDc2Mw==", shards.get(0).start());
        Assert.assertEquals(Strings.EMPTY, shards.get(0).end());

        shards = range.splitEven(1);
        Assert.assertEquals(1, shards.size());
        Assert.assertEquals("/TUwMDB3TtsxMTAwMDc2Mw==", shards.get(0).start());
        Assert.assertEquals(Strings.EMPTY, shards.get(0).end());

        shards = range.splitEven(2);
        Assert.assertEquals(2, shards.size());
        Assert.assertEquals("/TUwMDB3TtsxMTAwMDc2Mw==", shards.get(0).start());
        Assert.assertEquals("/pqYGBg7p22YmJgYGBubGQ==", shards.get(0).end());
        Assert.assertEquals("/pqYGBg7p22YmJgYGBubGQ==", shards.get(1).start());
        Assert.assertEquals(Strings.EMPTY, shards.get(1).end());

        for (int i = 3; i < 100; i++) {
            range.splitEven(i);
        }
    }

    @Test
    public void testRangeOfRegionWithStartKeyAndEndKey() {
        byte[] start = new byte[]{-3, 0x31, 0x30, 0x30, 0x30, 0x77,
                                  0x20, 0x09, 0x38, 0x31, 0x33, 0x32, 0x35};
        byte[] end = new byte[]{-3, 0x31, 0x33, 0x35, 0x33, 0x32,
                                0x37, 0x34, 0x31, 0x35, 0x32};

        Range range = new Range(start, end);

        List<Shard> shards = range.splitEven(0);
        Assert.assertEquals(1, shards.size());
        Assert.assertEquals("/TEwMDB3IAk4MTMyNQ==", shards.get(0).start());
        Assert.assertEquals("/TEzNTMyNzQxNTI=", shards.get(0).end());

        shards = range.splitEven(1);
        Assert.assertEquals(1, shards.size());
        Assert.assertEquals("/TEwMDB3IAk4MTMyNQ==", shards.get(0).start());
        Assert.assertEquals("/TEzNTMyNzQxNTI=", shards.get(0).end());

        shards = range.splitEven(2);
        Assert.assertEquals(3, shards.size());
        Assert.assertEquals("/TEwMDB3IAk4MTMyNQ==", shards.get(0).start());
        Assert.assertEquals("/TExsrHUq560szKZGg==", shards.get(0).end());
        Assert.assertEquals("/TExsrHUq560szKZGg==", shards.get(1).start());
        Assert.assertEquals("/TEzNTMyNzQxNTH//w==", shards.get(1).end());
        Assert.assertEquals("/TEzNTMyNzQxNTH//w==", shards.get(2).start());
        Assert.assertEquals("/TEzNTMyNzQxNTI=", shards.get(2).end());

        for (int i = 3; i < 100; i++) {
            range.splitEven(i);
        }
    }
}
