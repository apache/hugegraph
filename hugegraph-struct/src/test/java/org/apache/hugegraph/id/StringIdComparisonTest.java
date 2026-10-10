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

import org.apache.hugegraph.util.StringEncoding;
import org.junit.Assert;
import org.junit.Test;

public class StringIdComparisonTest {

    @Test
    public void testUtf16OrderAcrossRepresentations() {
        String[] values = {"a", "aa", "北京", "\uD7FF", "\uD800\uDC00", "\uE000", "\uFFFF"};
        for (String left : values) {
            for (String right : values) {
                int expected = Integer.signum(left.compareTo(right));
                for (boolean leftBytes : new boolean[]{false, true}) {
                    for (boolean rightBytes : new boolean[]{false, true}) {
                        // Fresh IDs keep both directions independent of lazy decoding.
                        int forward = compare(left, leftBytes, right, rightBytes);
                        int reverse = compare(right, rightBytes, left, leftBytes);
                        Assert.assertEquals(expected, forward);
                        Assert.assertEquals(-forward, reverse);
                    }
                }
            }
        }
    }

    @Test
    public void testOrderDoesNotChangeAfterLazyConversion() {
        Id supplementary = newId("\uD800\uDC00", true);
        Id privateUse = newId("\uE000", true);
        int before = Integer.signum(supplementary.compareTo(privateUse));
        Assert.assertEquals(-1, before);

        supplementary.asString();
        Assert.assertEquals(before, Integer.signum(supplementary.compareTo(privateUse)));
        privateUse.asString();
        Assert.assertEquals(before, Integer.signum(supplementary.compareTo(privateUse)));
        supplementary.asBytes();
        privateUse.asBytes();
        Assert.assertEquals(before, Integer.signum(supplementary.compareTo(privateUse)));
        Assert.assertEquals(-before, Integer.signum(privateUse.compareTo(supplementary)));
    }

    @Test
    public void testMixedUnicodeOrderIsTransitive() {
        String beforeSurrogates = "\uD7FF";
        String supplementary = "\uD800\uDC00";
        String privateUse = "\uE000";
        for (boolean firstBytes : new boolean[]{false, true}) {
            for (boolean secondBytes : new boolean[]{false, true}) {
                for (boolean thirdBytes : new boolean[]{false, true}) {
                    Assert.assertTrue(compare(beforeSurrogates, firstBytes,
                                              supplementary, secondBytes) < 0);
                    Assert.assertTrue(compare(supplementary, secondBytes,
                                              privateUse, thirdBytes) < 0);
                    Assert.assertTrue(compare(beforeSurrogates, firstBytes,
                                              privateUse, thirdBytes) < 0);
                }
            }
        }
    }

    private static int compare(String left, boolean leftBytes, String right, boolean rightBytes) {
        return Integer.signum(newId(left, leftBytes).compareTo(newId(right, rightBytes)));
    }

    private static Id newId(String value, boolean bytes) {
        return bytes ? IdGenerator.of(StringEncoding.encode(value), Id.IdType.STRING) :
               IdGenerator.of(value);
    }
}
