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

package org.apache.hugegraph.unit.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.StringUtil;
import org.apache.hugegraph.util.StringUtil.Chars;
import org.junit.Test;

import com.google.common.base.Splitter;

public class StringUtilTest {

    @Test
    public void testSplit() {
        Assert.assertArrayEquals(new String[]{"1", "2", "3"},
                                 StringUtil.split("1, 2, 3", ", "));
        Assert.assertArrayEquals(new String[]{"1", "1", "1"},
                                 StringUtil.split("1 1 1", " "));
        Assert.assertArrayEquals(new String[]{"", "", ""},
                                 StringUtil.split("111", "1"));

        Assert.assertEquals(guavaSplit("123", " "),
                            toStringList(StringUtil.split("123", " ")));
        Assert.assertEquals(guavaSplit("1 2 3", " "),
                            toStringList(StringUtil.split("1 2 3", " ")));
        Assert.assertEquals(guavaSplit("1:2:3", ":"),
                            toStringList(StringUtil.split("1:2:3", ":")));
        Assert.assertEquals(guavaSplit("1::2:3", ":"),
                            toStringList(StringUtil.split("1::2:3", ":")));
        Assert.assertEquals(guavaSplit("1::2::3", ":"),
                            toStringList(StringUtil.split("1::2::3", ":")));
        Assert.assertEquals(guavaSplit("1::2::3", "::"),
                            toStringList(StringUtil.split("1::2::3", "::")));
        Assert.assertEquals(guavaSplit("1:|2|:3", "|"),
                            toStringList(StringUtil.split("1:|2|:3", "|")));
        Assert.assertEquals(guavaSplit("1\t2\t3", "\t"),
                            toStringList(StringUtil.split("1\t2\t3", "\t")));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            StringUtil.split("123", "");
        });
    }

    @Test
    public void testSplitToCharsArray() {
        Assert.assertArrayEquals(Chars.of("1", "2", "3"),
                                 StringUtil.splitToCharsArray("1, 2, 3", ", "));
        Assert.assertArrayEquals(Chars.of("1", "1", "1"),
                                 StringUtil.splitToCharsArray("1 1 1", " "));
        Assert.assertArrayEquals(Chars.of("", "", ""),
                                 StringUtil.splitToCharsArray("111", "1"));

        Assert.assertArrayEquals(new Chars[]{Chars.of("123")},
                                 StringUtil.splitToCharsArray("123", " "));
        Assert.assertArrayEquals(Chars.of("1", "", "2", "3"),
                                 StringUtil.splitToCharsArray("1::2:3", ":"));
        Assert.assertArrayEquals(Chars.of("1", "", "2", "", "3"),
                                 StringUtil.splitToCharsArray("1::2::3", ":"));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            StringUtil.splitToCharsArray("123", "");
        });
    }

    @Test
    public void testCharsCharAt() {
        Chars chars = Chars.of("123");
        Assert.assertEquals('1', chars.charAt(0));
        Assert.assertEquals('2', chars.charAt(1));
        Assert.assertEquals('3', chars.charAt(2));

        Assert.assertThrows(ArrayIndexOutOfBoundsException.class, () -> {
            chars.charAt(3);
        });
        Assert.assertThrows(ArrayIndexOutOfBoundsException.class, () -> {
            chars.charAt(-1);
        });
    }

    @Test
    public void testCharsSubSequence() {
        Chars chars = Chars.of("123");
        Assert.assertEquals(Chars.of("1"), chars.subSequence(0, 1));
        Assert.assertEquals(Chars.of("12"), chars.subSequence(0, 2));
        Assert.assertEquals(Chars.of("2"), chars.subSequence(1, 2));
        Assert.assertEquals(Chars.of("23"), chars.subSequence(1, 3));
        Assert.assertEquals(Chars.of("123"), chars.subSequence(0, 3));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            chars.subSequence(2, 1);
        }, e -> {
            Assert.assertContains("Invalid end parameter 1", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            chars.subSequence(-1, 2);
        }, e -> {
            Assert.assertContains("Invalid start parameter -1", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            chars.subSequence(1, -1);
        }, e -> {
            Assert.assertContains("Invalid end parameter -1", e.getMessage());
        });
    }

    @Test
    public void testCharsEquals() {
        Chars chars1 = Chars.of("123");
        Chars chars2 = Chars.of("123");
        Chars chars3 = Chars.of("12");

        Assert.assertEquals(chars1, chars2);
        Assert.assertNotEquals(chars1, chars3);
        Assert.assertNotEquals(chars1, "123");
        Assert.assertNotEquals(chars1, null);
    }

    @Test
    public void testCharsHashCode() {
        Chars chars1 = Chars.of("123");
        Chars chars2 = Chars.of("123");
        Chars chars3 = Chars.of("12");

        Assert.assertEquals(chars1.hashCode(), chars2.hashCode());
        Assert.assertEquals(chars1.hashCode(), "123".hashCode());
        Assert.assertNotEquals(chars1.hashCode(), chars3.hashCode());
    }

    private static List<String> guavaSplit(String line, String delimiter) {
        return Splitter.on(delimiter).splitToList(line);
    }

    private static List<String> toStringList(String[] stringArray) {
        List<String> results = new ArrayList<>(stringArray.length);
        Collections.addAll(results, stringArray);
        return results;
    }

    @Test
    public void testCharsEmptySliceAtEnd() {
        Chars chars = Chars.of("123");
        CharSequence tail = chars.subSequence(3, 3);
        Assert.assertEquals(0, tail.length());
        Assert.assertEquals("", tail.toString());

        Assert.assertEquals("", Chars.of("12").subSequence(1, 2).subSequence(1, 1).toString());
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            chars.subSequence(4, 4);
        });
    }
}
