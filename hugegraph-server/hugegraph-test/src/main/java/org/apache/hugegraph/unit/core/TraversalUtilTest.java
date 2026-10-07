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

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.hugegraph.util.DateUtil;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Test;

public class TraversalUtilTest {

    @Test
    public void testParsePredicate() {
        Assert.assertEquals(P.eq(0),
                            TraversalUtil.parsePredicate("P.eq(0)"));
        Assert.assertEquals(P.eq(12),
                            TraversalUtil.parsePredicate("P.eq(12 345)"));
        Assert.assertEquals(P.eq(-1),
                            TraversalUtil.parsePredicate("P.eq( -1 )"));
        Assert.assertEquals(P.eq(18),
                            TraversalUtil.parsePredicate("P.eq(\"18\")"));

        Assert.assertEquals(P.neq(0),
                            TraversalUtil.parsePredicate("P.neq(0)"));
        Assert.assertEquals(P.neq(-2),
                            TraversalUtil.parsePredicate("P.neq(\"-2\")"));

        Assert.assertEquals(P.lt(-1),
                            TraversalUtil.parsePredicate("P.lt(-1)"));
        Assert.assertEquals(P.lt(-1),
                            TraversalUtil.parsePredicate("P.lt(\"-1\")"));
        Assert.assertEquals(P.lte(-123.45),
                            TraversalUtil.parsePredicate("P.lte(-123.45)"));
        Assert.assertEquals(P.lte(3.14),
                            TraversalUtil.parsePredicate("P.lte(\"3.14\")"));

        Assert.assertEquals(P.gt(18),
                            TraversalUtil.parsePredicate("P.gt(18)"));
        Assert.assertEquals(P.gt(18),
                            TraversalUtil.parsePredicate("P.gt(\"18\")"));
        Assert.assertEquals(P.gte(3.14),
                            TraversalUtil.parsePredicate("P.gte(3.14)"));
        Assert.assertEquals(P.gte(3.14),
                            TraversalUtil.parsePredicate("P.gte(\"3.14\")"));

        Assert.assertEquals(P.between(1, 100),
                            TraversalUtil.parsePredicate("P.between(1, 100)"));
        Assert.assertEquals(P.between(1, 1.2),
                            TraversalUtil.parsePredicate("P.between(1, 1.2)"));
        Assert.assertEquals(P.between(1, 2),
                            TraversalUtil.parsePredicate("P.between(\"1\", 2)"));

        Assert.assertEquals(P.inside(1, 100),
                            TraversalUtil.parsePredicate("P.inside(1, 100)"));
        Assert.assertEquals(P.inside(0.28, 1),
                            TraversalUtil.parsePredicate("P.inside(0.28, 1)"));
        Assert.assertEquals(P.inside(1, 2),
                            TraversalUtil.parsePredicate("P.inside(\"1\", 2)"));

        Assert.assertEquals(P.outside(1, 100),
                            TraversalUtil.parsePredicate("P.outside(1, 100)"));
        Assert.assertEquals(P.outside(1, 1.5),
                            TraversalUtil.parsePredicate("P.outside(1, 1.5)"));
        Assert.assertEquals(P.outside(1, 2),
                            TraversalUtil.parsePredicate("P.outside(\"1\", 2)"));

        Assert.assertEquals(P.within(1, 3, 5),
                            TraversalUtil.parsePredicate("P.within(1, 3, 5)"));
        Assert.assertEquals(P.within("abc", "hello", (Object) 123, 3.14),
                            TraversalUtil.parsePredicate(
                                    "P.within(\"abc\", \"hello\", 123, 3.14)"));
    }

    @Test
    public void testParsePredicateWithTime() {
        Assert.assertEquals(P.eq(DateUtil.parse("2018-8-8").getTime()),
                            TraversalUtil.parsePredicate("P.eq(\"2018-8-8\")"));

        Assert.assertEquals(P.eq(DateUtil.parse("2018-8-8 23:08:59").getTime()),
                            TraversalUtil.parsePredicate(
                                    "P.eq(\"2018-8-8 23:08:59\")"));

        Assert.assertEquals(P.gt(DateUtil.parse("2018-8-8").getTime()),
                            TraversalUtil.parsePredicate("P.gt(\"2018-8-8\")"));

        Assert.assertEquals(P.gte(DateUtil.parse("2018-8-8").getTime()),
                            TraversalUtil.parsePredicate("P.gte(\"2018-8-8\")"));

        Assert.assertEquals(P.lt(DateUtil.parse("2018-8-8").getTime()),
                            TraversalUtil.parsePredicate("P.lt(\"2018-8-8\")"));

        Assert.assertEquals(P.lte(DateUtil.parse("2018-8-8").getTime()),
                            TraversalUtil.parsePredicate("P.lte(\"2018-8-8\")"));

        Assert.assertEquals(P.between(DateUtil.parse("2018-8-8").getTime(),
                                      DateUtil.parse("2019-12-8").getTime()),
                            TraversalUtil.parsePredicate(
                                    "P.between(\"2018-8-8\", \"2019-12-8\")"));

        Assert.assertEquals(P.inside(DateUtil.parse("2018-8-8").getTime(),
                                     DateUtil.parse("2019-12-8").getTime()),
                            TraversalUtil.parsePredicate(
                                    "P.inside(\"2018-8-8\", \"2019-12-8\")"));

        Assert.assertEquals(P.outside(DateUtil.parse("2018-8-8").getTime(),
                                      DateUtil.parse("2019-12-8").getTime()),
                            TraversalUtil.parsePredicate(
                                    "P.outside(\"2018-8-8\", \"2019-12-8\")"));

        Assert.assertEquals(P.within("2018-8-8", "2019-12-8"),
                            TraversalUtil.parsePredicate(
                                    "P.within(\"2018-8-8\", \"2019-12-8\")"));

        // special time
        Assert.assertEquals(P.eq(DateUtil.parse("2018-8-8").getTime()),
                            TraversalUtil.parsePredicate("P.eq(2018-8-8)"));
        Assert.assertEquals(P.gt(DateUtil.parse("2018-8-8").getTime()),
                            TraversalUtil.parsePredicate("P.gt(2018-8-8)"));
        Assert.assertEquals(P.gte(DateUtil.parse("2018-8-8").getTime()),
                            TraversalUtil.parsePredicate("P.gte(2018-8-8)"));
        Assert.assertEquals(P.lt(DateUtil.parse("2018-8-8").getTime()),
                            TraversalUtil.parsePredicate("P.lt(2018-8-8)"));
        Assert.assertEquals(P.lte(DateUtil.parse("2018-8-8").getTime()),
                            TraversalUtil.parsePredicate("P.lte(2018-8-8)"));
        Assert.assertEquals(P.lte(DateUtil.parse("2018-8-8").getTime()),
                            TraversalUtil.parsePredicate("P.lte(2018-8-8)"));

        // invalid time
        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.eq(2018-9-90)");
        }, e -> {
            Assert.assertEquals("Invalid value '2018-9-90', " +
                                "expect a number", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.eq(2018-9-9 10)");
        }, e -> {
            Assert.assertEquals("Invalid value '2018-9-9 10', " +
                                "expect a number", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.eq(2018-9-9 10:123:59)");
        }, e -> {
            Assert.assertEquals("Invalid value '2018-9-9 10:123:59', " +
                                "expect a number", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.eq(2018/9/9)");
        }, e -> {
            Assert.assertEquals("Invalid value '2018/9/9', " +
                                "expect a number", e.getMessage());
        });
    }

    @Test
    public void testParsePredicateWithInvalidString() {
        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("what(123)");
        }, e -> {
            Assert.assertEquals("Invalid predicate: what(123)",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.what(123)");
        }, e -> {
            Assert.assertEquals("Not support predicate 'what'",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P .eq(123)");
        }, e -> {
            Assert.assertEquals("Invalid predicate: P .eq(123)",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P. eq(123)");
        }, e -> {
            Assert.assertEquals("Invalid predicate: P. eq(123)",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.eq (123)");
        }, e -> {
            Assert.assertEquals("Invalid predicate: P.eq (123)",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.eq(18m)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m', expect a number",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.neq(18m)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m', expect a number",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.lt(18m)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m', expect a number",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.lte(18m)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m', expect a number",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.gt(18m)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m', expect a number",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.gte(18m)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m', expect a number",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.between(18m, 20)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m, 20', " +
                                "expect a list", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.between(18, 20m)");
        }, e -> {
            Assert.assertEquals("Invalid value '18, 20m', " +
                                "expect a list", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.between(18)");
        }, e -> {
            Assert.assertEquals("Invalid numbers size 1, expect 2",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.between(18, 20, 30)");
        }, e -> {
            Assert.assertEquals("Invalid numbers size 3, expect 2",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.between(\"18m\", 20)");
        }, e -> {
            Assert.assertEquals("Invalid value '\"18m\", 20', " +
                                "expect a list of number", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.inside(18m, 20)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m, 20', " +
                                "expect a list", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.inside(\"18m\", 20)");
        }, e -> {
            Assert.assertEquals("Invalid value '\"18m\", 20', " +
                                "expect a list of number", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.inside(18)");
        }, e -> {
            Assert.assertEquals("Invalid numbers size 1, expect 2",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.outside(18m, 20)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m, 20', " +
                                "expect a list", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.outside(\"18m\", 20)");
        }, e -> {
            Assert.assertEquals("Invalid value '\"18m\", 20', " +
                                "expect a list of number", e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.outside(18)");
        }, e -> {
            Assert.assertEquals("Invalid numbers size 1, expect 2",
                                e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            TraversalUtil.parsePredicate("P.within(18m, 20)");
        }, e -> {
            Assert.assertEquals("Invalid value '18m, 20', " +
                                "expect a list", e.getMessage());
        });
    }
}
