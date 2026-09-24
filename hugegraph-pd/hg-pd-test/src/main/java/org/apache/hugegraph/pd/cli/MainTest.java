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

package org.apache.hugegraph.pd.cli;

import lombok.extern.slf4j.Slf4j;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class MainTest extends BaseCliToolsTest {

    public static boolean test2sup(List<Integer> arrays, int tail, int res) {
        System.out.println(String.format("%d %d", tail, res));
        if (tail == 0) {
            System.out.println(String.format("a = %d %d", tail, res));
            return false;
        } else if (tail == 1) {
            System.out.println(String.format("b = %d %d", arrays.get(0), res));
            return (arrays.get(0) == res);
        } else if (tail == 2) {
            System.out.println(String.format("c = %d %d %d", arrays.get(0), arrays.get(1), res));
            return (arrays.get(0) + arrays.get(1) == Math.abs(res)) ||
                   (Math.abs(arrays.get(0) - arrays.get(1)) == Math.abs(res));
        } else {
            return test2sup(arrays, tail - 1, res + arrays.get(tail - 1)) ||
                   test2sup(arrays, tail - 1, res - arrays.get(tail - 1));
        }
    }

    @Test
    public void test2() {
        Integer[] a = new Integer[]{1, 0, 3, 2};
        List<Integer> aa = Arrays.asList(a);
        System.out.printf(test2sup(aa, aa.size(), 0) ? "TRUE" : "FALSE");
    }

}
