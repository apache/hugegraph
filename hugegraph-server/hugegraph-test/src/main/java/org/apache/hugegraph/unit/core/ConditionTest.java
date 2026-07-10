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

import java.util.Date;

import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.Condition.Relation;
import org.apache.hugegraph.backend.query.Condition.RelationType;
import org.apache.hugegraph.backend.query.Condition.SyspropRelation;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class ConditionTest extends BaseUnitTest {

    @Test
    public void testConditionEqWithSysprop() {
        Condition c1 = Condition.eq(HugeKeys.ID, "123");

        Assert.assertEquals("ID == 123", c1.toString());

        Assert.assertTrue(c1.isRelation());
        Assert.assertTrue(c1.isSysprop());
        Assert.assertTrue(c1.isFlattened());
        Assert.assertFalse(c1.isLogic());
        Assert.assertTrue(c1.test("123"));
        Assert.assertFalse(c1.test("1234"));
        Assert.assertFalse(c1.test(123));
        Assert.assertFalse(c1.test(new Date(123)));
        Assert.assertFalse(c1.test((Object) null));

        Relation r1 = (Relation) c1;
        Assert.assertEquals(HugeKeys.ID, r1.key());
        Assert.assertEquals("123", r1.value());
        Assert.assertEquals("123", r1.serialValue());
        Assert.assertEquals(RelationType.EQ, r1.relation());
        Assert.assertTrue(r1.test("123"));

        Relation r2 = (Relation) c1.copy();
        Assert.assertEquals(r1, r2);
        Assert.assertEquals(HugeKeys.ID, r2.key());
        Assert.assertEquals("123", r2.value());
        Assert.assertEquals("123", r2.serialValue());
        Assert.assertEquals(RelationType.EQ, r2.relation());
        Assert.assertTrue(r2.test("123"));

        r2.serialValue("1234");
        Assert.assertEquals("1234", r2.serialValue());
        Assert.assertEquals("123", r1.serialValue());
        Assert.assertTrue(r2.test("123"));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.eq(HugeKeys.ID, null).test("any");
        }, e -> {
            Assert.assertEquals("Can't test null value for `==`",
                                e.getMessage());
        });
    }

    @Test
    public void testConditionEqWithUserprop() {
        Condition c1 = Condition.eq(IdGenerator.of("1"), "123");

        Assert.assertEquals("1 == 123", c1.toString());

        Assert.assertTrue(c1.isRelation());
        Assert.assertFalse(c1.isSysprop());
        Assert.assertTrue(c1.isFlattened());
        Assert.assertFalse(c1.isLogic());
        Assert.assertTrue(c1.test("123"));
        Assert.assertFalse(c1.test("1234"));
        Assert.assertFalse(c1.test(123));
        Assert.assertFalse(c1.test(new Date(123)));
        Assert.assertFalse(c1.test((Object) null));

        Relation r1 = (Relation) c1;
        Assert.assertEquals(IdGenerator.of("1"), r1.key());
        Assert.assertEquals("123", r1.value());
        Assert.assertEquals("123", r1.serialValue());
        Assert.assertEquals(RelationType.EQ, r1.relation());
        Assert.assertTrue(r1.test("123"));

        Relation r2 = (Relation) c1.copy();
        Assert.assertEquals(r1, r2);
        Assert.assertEquals(IdGenerator.of("1"), r2.key());
        Assert.assertEquals("123", r2.value());
        Assert.assertEquals("123", r2.serialValue());
        Assert.assertEquals(RelationType.EQ, r2.relation());
        Assert.assertTrue(r2.test("123"));

        r2.serialValue("1234");
        Assert.assertEquals("1234", r2.serialValue());
        Assert.assertEquals("123", r1.serialValue());
        Assert.assertTrue(r2.test("123"));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.eq(IdGenerator.of("1"), null).test("any");
        }, e -> {
            Assert.assertEquals("Can't test null value for `==`",
                                e.getMessage());
        });
    }

    @Test
    public void testConditionEq() {
        Condition c1 = Condition.eq(HugeKeys.ID, 123);
        Assert.assertTrue(c1.test(123));
        Assert.assertTrue(c1.test(123.0));
        Assert.assertFalse(c1.test(122));
        Assert.assertFalse(c1.test(123.01));
        Assert.assertFalse(c1.test(120));
        Assert.assertFalse(c1.test(20));
        Assert.assertTrue(c1.test("123"));
        Assert.assertTrue(c1.test("123.0"));
        Assert.assertFalse(c1.test("123.01"));
        Assert.assertFalse(c1.test("200"));
        Assert.assertFalse(c1.test((Object) null)); // null means 0

        Condition c2 = Condition.eq(HugeKeys.ID, 0);
        Assert.assertTrue(c2.test((Object) null));

        Condition c3 = Condition.eq(HugeKeys.ID, -1);
        Assert.assertFalse(c3.test((Object) null));

        Condition c4 = Condition.eq(HugeKeys.ID, "123");
        Assert.assertFalse(c4.test(123));
        Assert.assertFalse(c4.test(new Date(0L)));
    }

    @Test
    public void testConditionGt() {
        Condition c1 = Condition.gt(HugeKeys.ID, 123);
        Assert.assertTrue(c1.test(124));
        Assert.assertTrue(c1.test(200));
        Assert.assertFalse(c1.test(123));
        Assert.assertFalse(c1.test(123.0));
        Assert.assertFalse(c1.test(120));
        Assert.assertFalse(c1.test(20));
        Assert.assertFalse(c1.test("123"));
        Assert.assertFalse(c1.test("123.0"));
        Assert.assertTrue(c1.test("123.01"));
        Assert.assertTrue(c1.test("200"));
        Assert.assertFalse(c1.test((Object) null)); // null means 0

        Condition c2 = Condition.gt(HugeKeys.ID, 0);
        Assert.assertFalse(c2.test((Object) null));

        Condition c3 = Condition.gt(HugeKeys.ID, -1);
        Assert.assertTrue(c3.test((Object) null));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.gt(HugeKeys.ID, "123").test(123);
        }, e -> {
            String err = "Can't compare between 123(Integer) and 123(String)";
            Assert.assertEquals(err, e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.gt(HugeKeys.ID, "123").test(new Date(0L));
        }, e -> {
            String err = String.format("Can't compare between %s(Date) " +
                                       "and 123(String)", new Date(0L));
            Assert.assertEquals(err, e.getMessage());
        });
    }

    @Test
    public void testConditionGte() {
        Condition c1 = Condition.gte(HugeKeys.ID, 123);
        Assert.assertTrue(c1.test(124));
        Assert.assertTrue(c1.test(200));
        Assert.assertTrue(c1.test(123));
        Assert.assertTrue(c1.test(123.0));
        Assert.assertFalse(c1.test(122));
        Assert.assertFalse(c1.test(20));
        Assert.assertTrue(c1.test("123"));
        Assert.assertTrue(c1.test("123.0"));
        Assert.assertTrue(c1.test("123.01"));
        Assert.assertTrue(c1.test("200"));
        Assert.assertFalse(c1.test((Object) null)); // null means 0

        Condition c2 = Condition.gte(HugeKeys.ID, 0);
        Assert.assertTrue(c2.test((Object) null));

        Condition c3 = Condition.gte(HugeKeys.ID, -1);
        Assert.assertTrue(c3.test((Object) null));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.gte(HugeKeys.ID, "123").test(123);
        }, e -> {
            String err = "Can't compare between 123(Integer) and 123(String)";
            Assert.assertEquals(err, e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.gte(HugeKeys.ID, "123").test(new Date(0L));
        }, e -> {
            String err = String.format("Can't compare between %s(Date) " +
                                       "and 123(String)", new Date(0L));
            Assert.assertEquals(err, e.getMessage());
        });
    }

    @Test
    public void testConditionLt() {
        Condition c1 = Condition.lt(HugeKeys.ID, 123);
        Assert.assertTrue(c1.test(122));
        Assert.assertTrue(c1.test(120));
        Assert.assertTrue(c1.test(20));
        Assert.assertFalse(c1.test(124));
        Assert.assertFalse(c1.test(200));
        Assert.assertFalse(c1.test(123));
        Assert.assertFalse(c1.test(123.0));
        Assert.assertFalse(c1.test("123"));
        Assert.assertFalse(c1.test("123.0"));
        Assert.assertFalse(c1.test("123.01"));
        Assert.assertFalse(c1.test("200"));
        Assert.assertTrue(c1.test("122.99"));
        Assert.assertTrue(c1.test((Object) null)); // null means 0

        Condition c2 = Condition.lt(HugeKeys.ID, 0.1);
        Assert.assertTrue(c2.test((Object) null));

        Condition c3 = Condition.lt(HugeKeys.ID, 0);
        Assert.assertFalse(c3.test((Object) null));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.lt(HugeKeys.ID, "123").test(123);
        }, e -> {
            String err = "Can't compare between 123(Integer) and 123(String)";
            Assert.assertEquals(err, e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.lt(HugeKeys.ID, "123").test(new Date(0L));
        }, e -> {
            String err = String.format("Can't compare between %s(Date) " +
                                       "and 123(String)", new Date(0L));
            Assert.assertEquals(err, e.getMessage());
        });
    }

    @Test
    public void testConditionLte() {
        Condition c1 = Condition.lte(HugeKeys.ID, 123);
        Assert.assertTrue(c1.test(123));
        Assert.assertTrue(c1.test(123.0));
        Assert.assertTrue(c1.test(120));
        Assert.assertTrue(c1.test(20));
        Assert.assertTrue(c1.test("123"));
        Assert.assertTrue(c1.test("123.0"));
        Assert.assertFalse(c1.test("123.01"));
        Assert.assertTrue(c1.test("20"));
        Assert.assertTrue(c1.test((Object) null)); // null means 0

        Condition c2 = Condition.lte(HugeKeys.ID, 0);
        Assert.assertTrue(c2.test((Object) null));

        Condition c3 = Condition.lte(HugeKeys.ID, -1);
        Assert.assertFalse(c3.test((Object) null));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.lte(HugeKeys.ID, "123").test(123);
        }, e -> {
            String err = "Can't compare between 123(Integer) and 123(String)";
            Assert.assertEquals(err, e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.lte(HugeKeys.ID, "123").test(new Date(0L));
        }, e -> {
            String err = String.format("Can't compare between %s(Date) " +
                                       "and 123(String)", new Date(0L));
            Assert.assertEquals(err, e.getMessage());
        });
    }

    @Test
    public void testConditionBooleanRange() {
        Condition lt = Condition.lt(HugeKeys.ID, true);
        Assert.assertTrue(lt.test(false));
        Assert.assertFalse(lt.test(true));

        Condition lte = Condition.lte(HugeKeys.ID, false);
        Assert.assertTrue(lte.test(false));
        Assert.assertFalse(lte.test(true));

        Condition gt = Condition.gt(HugeKeys.ID, false);
        Assert.assertTrue(gt.test(true));
        Assert.assertFalse(gt.test(false));

        Condition gte = Condition.gte(HugeKeys.ID, true);
        Assert.assertTrue(gte.test(true));
        Assert.assertFalse(gte.test(false));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.lt(HugeKeys.ID, true).test(1);
        }, e -> {
            String err = "Can't compare between 1(Integer) and true(Boolean)";
            Assert.assertEquals(err, e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.lt(HugeKeys.ID, true).test((Object) null);
        }, e -> {
            String err = "Can't compare between null(null) and true(Boolean)";
            Assert.assertEquals(err, e.getMessage());
        });
    }

    @Test
    public void testConditionNeq() {
        Condition c1 = Condition.neq(HugeKeys.ID, 123);
        Assert.assertTrue(c1.test(124));
        Assert.assertTrue(c1.test(122.9));
        Assert.assertTrue(c1.test(20));
        Assert.assertFalse(c1.test(123));
        Assert.assertFalse(c1.test(123.0));
        Assert.assertFalse(c1.test("123"));
        Assert.assertFalse(c1.test("123.0"));
        Assert.assertTrue(c1.test("123.01"));
        Assert.assertTrue(c1.test("20"));
        Assert.assertTrue(c1.test((Object) null)); // null means 0

        Condition c2 = Condition.neq(HugeKeys.ID, 0);
        Assert.assertFalse(c2.test((Object) null));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.neq(HugeKeys.ID, "123").test(123);
        }, e -> {
            String err = "Can't compare between 123(Integer) and 123(String)";
            Assert.assertEquals(err, e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.neq(HugeKeys.ID, "123").test(new Date(0L));
        }, e -> {
            String err = String.format("Can't compare between %s(Date) " +
                                       "and 123(String)", new Date(0L));
            Assert.assertEquals(err, e.getMessage());
        });
    }

    @Test
    public void testConditionIn() {
        Condition c1 = Condition.in(HugeKeys.ID, ImmutableList.of(1, 2, "3"));
        Assert.assertTrue(c1.test(1));
        Assert.assertTrue(c1.test(2));
        Assert.assertTrue(c1.test("3"));
        Assert.assertFalse(c1.test(4));
        Assert.assertFalse(c1.test(1.0));
        Assert.assertFalse(c1.test(2.0));
        Assert.assertFalse(c1.test(3));
        Assert.assertFalse(c1.test(123));
        Assert.assertFalse(c1.test(1.01));
        Assert.assertFalse(c1.test("123"));
        Assert.assertFalse(c1.test("123.0"));
        Assert.assertFalse(c1.test("1"));
        Assert.assertFalse(c1.test("2"));
        Assert.assertFalse(c1.test("3.0"));
        Assert.assertFalse(c1.test("1.0"));
        Assert.assertFalse(c1.test((Object) null));
        Assert.assertFalse(c1.test(ImmutableList.of(1, 2)));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.in(HugeKeys.ID, null).test("1");
        }, e -> {
            Assert.assertEquals("Can't test null value for `in`",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition c2 = new SyspropRelation(HugeKeys.ID, RelationType.IN,
                                               "single-value");
            c2.test("singlevalue");
        }, e -> {
            Assert.assertEquals("Can't test 'single-value'(String) for `in`, " +
                                "expect Collection", e.getMessage());
        });
    }

    @Test
    public void testConditionNotIn() {
        Condition c1 = Condition.nin(HugeKeys.ID, ImmutableList.of(1, 2, "3"));
        Assert.assertFalse(c1.test(1));
        Assert.assertFalse(c1.test(2));
        Assert.assertFalse(c1.test("3"));
        Assert.assertTrue(c1.test(4));
        Assert.assertTrue(c1.test(1.0));
        Assert.assertTrue(c1.test(2.0));
        Assert.assertTrue(c1.test(3));
        Assert.assertTrue(c1.test(123));
        Assert.assertTrue(c1.test(1.01));
        Assert.assertTrue(c1.test("123"));
        Assert.assertTrue(c1.test("123.0"));
        Assert.assertTrue(c1.test("1"));
        Assert.assertTrue(c1.test("2"));
        Assert.assertTrue(c1.test("3.0"));
        Assert.assertTrue(c1.test("1.0"));
        Assert.assertTrue(c1.test((Object) null));
        Assert.assertTrue(c1.test(ImmutableList.of(4)));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition.nin(HugeKeys.ID, null).test("1");
        }, e -> {
            Assert.assertEquals("Can't test null value for `notin`",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition c2 = new SyspropRelation(HugeKeys.ID, RelationType.NOT_IN,
                                               "single-value");
            c2.test("singlevalue");
        }, e -> {
            Assert.assertEquals("Can't test 'single-value'(String) " +
                                "for `notin`, expect Collection",
                                e.getMessage());
        });
    }

    @Test
    public void testConditionPrefix() {
        Condition c1 = Condition.prefix(HugeKeys.ID, IdGenerator.of("abc"));
        Assert.assertTrue(c1.test(IdGenerator.of("a")));
        Assert.assertTrue(c1.test(IdGenerator.of("ab")));
        Assert.assertTrue(c1.test(IdGenerator.of("abc")));
        Assert.assertFalse(c1.test(IdGenerator.of("abcd")));
        Assert.assertFalse(c1.test(IdGenerator.of("b")));
        Assert.assertFalse(c1.test(IdGenerator.of("bc")));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test((Object) null);
        }, e -> {
            Assert.assertEquals("Can't execute `prefix` on type null, " +
                                "expect Id", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test(123);
        }, e -> {
            Assert.assertEquals("Can't execute `prefix` on type Integer, " +
                                "expect Id", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition c2 = new SyspropRelation(HugeKeys.ID, RelationType.PREFIX,
                                               "abc");
            c2.test(IdGenerator.of("ab"));
        }, e -> {
            Assert.assertEquals("Can't test 'abc'(String) for `prefix`, " +
                                "expect Id", e.getMessage());
        });
    }

    @Test
    public void testConditionContains() {
        Condition c1 = Condition.contains(HugeKeys.ID, "v1");
        Assert.assertTrue(c1.test(ImmutableList.of("v1", "v2")));
        Assert.assertTrue(c1.test(ImmutableList.of("v1", "v3")));
        Assert.assertFalse(c1.test(ImmutableList.of("v3", "v4")));

        Condition c2 = Condition.contains(HugeKeys.ID,
                                          ImmutableList.of("v1", "v2"));
        Assert.assertFalse(c2.test(ImmutableList.of("v1", "v2", "v3")));
        Assert.assertTrue(c2.test(ImmutableList.of(ImmutableList.of("v1", "v2"),
                                                   "v3")));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test((Object) null);
        }, e -> {
            Assert.assertEquals("Can't execute `contains` on type null, " +
                                "expect Collection", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test("v1");
        }, e -> {
            Assert.assertEquals("Can't execute `contains` on type String, " +
                                "expect Collection", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test(123);
        }, e -> {
            Assert.assertEquals("Can't execute `contains` on type Integer, " +
                                "expect Collection", e.getMessage());
        });
    }

    @Test
    public void testConditionContainsKey() {
        Condition c1 = Condition.containsKey(HugeKeys.ID, "k1");
        Assert.assertTrue(c1.test(ImmutableMap.of("k1", "abc")));
        Assert.assertTrue(c1.test(ImmutableMap.of("k1", "abc", "k2", "123")));
        Assert.assertFalse(c1.test(ImmutableMap.of("k3", "ab")));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test((Object) null);
        }, e -> {
            Assert.assertEquals("Can't execute `containsk` on type null, " +
                                "expect Map", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test("v1");
        }, e -> {
            Assert.assertEquals("Can't execute `containsk` on type String, " +
                                "expect Map", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test(123f);
        }, e -> {
            Assert.assertEquals("Can't execute `containsk` on type Float, " +
                                "expect Map", e.getMessage());
        });
    }

    @Test
    public void testConditionContainsValue() {
        Condition c1 = Condition.containsValue(HugeKeys.ID, "abc");
        Assert.assertTrue(c1.test(ImmutableMap.of("k1", "abc")));
        Assert.assertTrue(c1.test(ImmutableMap.of("k1", "abc", "k2", "123")));
        Assert.assertFalse(c1.test(ImmutableMap.of("k1", "ab")));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test((Object) null);
        }, e -> {
            Assert.assertEquals("Can't execute `containsv` on type null, " +
                                "expect Map", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test("v1");
        }, e -> {
            Assert.assertEquals("Can't execute `containsv` on type String, " +
                                "expect Map", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test(123d);
        }, e -> {
            Assert.assertEquals("Can't execute `containsv` on type Double, " +
                                "expect Map", e.getMessage());
        });
    }

    @Test
    public void testConditionScan() {
        Condition c1 = Condition.scan("abc", "axy");
        Assert.assertTrue(c1.test("abc"));
        Assert.assertTrue(c1.test("abd"));
        Assert.assertTrue(c1.test("abz"));
        Assert.assertTrue(c1.test("axx"));
        // test of scan will return true for any case
        Assert.assertTrue(c1.test("abb"));
        Assert.assertTrue(c1.test("axy"));
        Assert.assertTrue(c1.test("axz"));
        Assert.assertTrue(c1.test((Object) null));
    }

    @Test
    public void testConditionTextContains() {
        Condition c1 = Condition.textContains(IdGenerator.of("1"), "tom");
        Assert.assertTrue(c1.test("tom"));
        Assert.assertTrue(c1.test("tomcat"));
        Assert.assertFalse(c1.test("cat"));
        Assert.assertFalse(c1.test("text"));
        Assert.assertFalse(c1.test("abc"));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test((Object) null);
        }, e -> {
            Assert.assertEquals("Can't execute `textcontains` on type null, " +
                                "expect String", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test(123L);
        }, e -> {
            Assert.assertEquals("Can't execute `textcontains` on type Long, " +
                                "expect String", e.getMessage());
        });
    }

    @Test
    public void testConditionTextContainsAny() {
        Condition c1 = Condition.textContainsAny(IdGenerator.of("1"),
                                                 ImmutableSet.of("tom", "cat"));
        Assert.assertTrue(c1.test("tom"));
        Assert.assertTrue(c1.test("tomcat"));
        Assert.assertTrue(c1.test("tomcat2"));
        Assert.assertTrue(c1.test("cat"));
        Assert.assertFalse(c1.test("text"));
        Assert.assertFalse(c1.test("abc"));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test((Object) null);
        }, e -> {
            Assert.assertEquals("Can't execute `textcontainsany` " +
                                "on type null, expect String", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test(123L);
        }, e -> {
            Assert.assertEquals("Can't execute `textcontainsany` " +
                                "on type Long, expect String", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            c1.test(ImmutableList.of("1"));
        }, e -> {
            Assert.assertEquals("Can't execute `textcontainsany` " +
                                "on type SingletonImmutableList, expect String",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Condition c2 = new SyspropRelation(HugeKeys.ID,
                                               RelationType.TEXT_CONTAINS_ANY,
                                               "abc");
            c2.test("abc");
        }, e -> {
            Assert.assertEquals("Can't test 'abc'(String) for " +
                                "`textcontainsany`, expect Collection",
                                e.getMessage());
        });
    }

    @Test
    public void testConditionAnd() {
        Condition c1 = Condition.gt(HugeKeys.ID, 18);
        Condition c2 = Condition.lt(HugeKeys.ID, 30);
        Condition c3 = c1.and(c2);
        Assert.assertEquals(Condition.and(c1, c2), c3);
        Assert.assertTrue(c3.test(19));
        Assert.assertTrue(c3.test(20));
        Assert.assertTrue(c3.test(29));
        Assert.assertFalse(c3.test(17));
        Assert.assertFalse(c3.test(18));
        Assert.assertFalse(c3.test(30));
        Assert.assertFalse(c3.test(31));
        Assert.assertFalse(c3.test((Object) null)); // null means 0
    }

    @Test
    public void testConditionOr() {
        Condition c1 = Condition.lt(HugeKeys.ID, 18);
        Condition c2 = Condition.gt(HugeKeys.ID, 30);
        Condition c3 = c1.or(c2);
        Assert.assertEquals(Condition.or(c1, c2), c3);
        Assert.assertFalse(c3.test(18));
        Assert.assertFalse(c3.test(19));
        Assert.assertFalse(c3.test(29));
        Assert.assertFalse(c3.test(30));
        Assert.assertTrue(c3.test(17));
        Assert.assertTrue(c3.test(31));
        Assert.assertTrue(c3.test((Object) null)); // null means 0
    }
}
