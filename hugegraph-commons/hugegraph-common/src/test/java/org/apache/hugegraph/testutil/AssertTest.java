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

package org.apache.hugegraph.testutil;

import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

public class AssertTest extends BaseUnitTest {

    @Test
    public void testAssertEquals() {
        Assert.assertEquals((byte) 1, Byte.valueOf("1"));
        Assert.assertEquals((short) 1, Short.valueOf("1"));
        Assert.assertEquals('1', Character.valueOf('1'));
        Assert.assertEquals(1, Integer.valueOf("1"));
        Assert.assertEquals(1L, Long.valueOf("1"));
        Assert.assertEquals(1f, Float.valueOf("1"));
        Assert.assertEquals(1d, Double.valueOf("1"));
    }

    @Test
    public void testAssertEqualsWithError() {
        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals((byte) 1, "1");
        }, e -> {
            Assert.assertContains("expected: java.lang.Byte",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals((short) 1, "1");
        }, e -> {
            Assert.assertContains("expected: java.lang.Short",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals('1', "1");
        }, e -> {
            Assert.assertContains("expected: java.lang.Character",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1, "1");
        }, e -> {
            Assert.assertContains("expected: java.lang.Integer",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1L, "1");
        }, e -> {
            Assert.assertContains("expected: java.lang.Long",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1f, "1.0");
        }, e -> {
            Assert.assertContains("expected: java.lang.Float",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1d, "1.0");
        }, e -> {
            Assert.assertContains("expected: java.lang.Double",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1f, "1");
        }, e -> {
            Assert.assertContains("expected:<1.0> but was:<1>",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1d, "1");
        }, e -> {
            Assert.assertContains("expected:<1.0> but was:<1>",
                                  e.getMessage());
        });
    }

    @Test
    public void testAssertEqualsOfIntWithError() {
        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1, (Byte) (byte) 1);
        }, e -> {
            Assert.assertContains("expected: java.lang.Integer",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1, (Short) (short) 1);
        }, e -> {
            Assert.assertContains("expected: java.lang.Integer",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1, (Character) '1');
        }, e -> {
            Assert.assertContains("expected: java.lang.Integer",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1, (Long) 1L);
        }, e -> {
            Assert.assertContains("expected: java.lang.Integer",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1, (Float) 1f);
        }, e -> {
            Assert.assertContains("expected:<1> but was:<1.0>",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1, (Double) 1d);
        }, e -> {
            Assert.assertContains("expected:<1> but was:<1.0>",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1, "1.0");
        }, e -> {
            Assert.assertContains("expected:<1> but was:<1.0>",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1, (Byte) (byte) 2);
        }, e -> {
            Assert.assertContains("expected:<1> but was:<2>",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertEquals(1, null);
        }, e -> {
            Assert.assertContains("expected:<1> but was:<null>",
                                  e.getMessage());
        });
    }

    @Test
    public void testAssertThrows() {
        Assert.assertThrows(NullPointerException.class, () -> {
            throw new NullPointerException();
        });
        Assert.assertThrows(RuntimeException.class, () -> {
            throw new RuntimeException();
        });

        Throwable exception = Assert.assertThrows(RuntimeException.class, () -> {
            throw new RuntimeException("fake-error");
        });
        Assert.assertInstanceOf(RuntimeException.class, exception);
        Assert.assertEquals("fake-error", exception.getMessage());

        Assert.assertThrows(RuntimeException.class, () -> {
            throw new RuntimeException("fake-error");
        }, e -> {
            Assert.assertEquals("fake-error", e.getMessage());
        });
    }

    @Test
    public void testAssertThrowsWithTypeError() {
        try {
            Assert.assertThrows(NullPointerException.class, () -> {
                // pass
            });
            Assert.fail("Expect error");
        } catch (AssertionError e) {
            Assert.assertContains("java.lang.NullPointerException", e.getMessage());
        }

        try {
            Assert.assertThrows(NullPointerException.class, () -> {
                throw new RuntimeException();
            });
            Assert.fail("Expect error");
        } catch (AssertionError e) {
            Assert.assertContains("java.lang.NullPointerException", e.getMessage());
            Assert.assertContains("java.lang.RuntimeException", e.getMessage());
        }
    }

    @Test
    public void testAssertThrowsWithMessageError() {
        try {
            Assert.assertThrows(RuntimeException.class, () -> {
                throw new RuntimeException("fake-error");
            }, e -> {
                Assert.assertEquals("fake-error-typo", e.getMessage());
            });
            Assert.fail("Expect error");
        } catch (AssertionError e) {
            Assert.assertContains("expected:<fake-error[-typo]> but was:<fake-error[]>",
                                  e.getMessage());
        }
    }

    @Test
    public void testAssertGt() {
        Assert.assertGt((byte) 1, Byte.valueOf("2"));
        Assert.assertGt((short) 1, Short.valueOf("2"));
        Assert.assertGt(1, Integer.valueOf("2"));
        Assert.assertGt(1L, Long.valueOf("2"));
        Assert.assertGt(1f, Float.valueOf("1.01"));
        Assert.assertGt(1d, Double.valueOf("1.01"));

        Assert.assertGt((byte) 1, (byte) 2);
        Assert.assertGt((short) 1, (short) 2);
        Assert.assertGt(1, 2);
        Assert.assertGt(1L, 2L);
        Assert.assertGt(1f, 1.01f);
        Assert.assertGt(1d, 1.01d);

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertGt(1, 0);
        }, e -> {
            Assert.assertContains("Expected: a number > 1", e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertGt(1, null);
        }, e -> {
            Assert.assertContains("Expected: an instance of java.lang.Integer",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertGt(1, (byte) 2);
        }, e -> {
            Assert.assertContains("Expected: an instance of java.lang.Integer",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertGt(1, 1.1);
        }, e -> {
            Assert.assertContains("Expected: an instance of java.lang.Integer",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertGt(1, '2');
        }, e -> {
            Assert.assertContains("Expected: an instance of java.lang.Integer",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertGt(0.9, 1);
        }, e -> {
            Assert.assertContains("Expected: an instance of java.lang.Double",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertGt(0.9d, 0.98f);
        }, e -> {
            Assert.assertContains("Expected: an instance of java.lang.Double",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertGt(0.9f, 0.98d);
        }, e -> {
            Assert.assertContains("Expected: an instance of java.lang.Float",
                                  e.getMessage());
        });
    }

    @Test
    public void testAssertGte() {
        Assert.assertGte((byte) 1, Byte.valueOf("2"));
        Assert.assertGte((short) 1, Short.valueOf("2"));
        Assert.assertGte(1, Integer.valueOf("2"));
        Assert.assertGte(1L, Long.valueOf("2"));
        Assert.assertGte(1f, Float.valueOf("1.01"));
        Assert.assertGte(1d, Double.valueOf("1.01"));

        Assert.assertGte((byte) 1, Byte.valueOf("1"));
        Assert.assertGte((short) 1, Short.valueOf("1"));
        Assert.assertGte(1, Integer.valueOf("1"));
        Assert.assertGte(1L, Long.valueOf("1"));
        Assert.assertGte(1f, Float.valueOf("1"));
        Assert.assertGte(1d, Double.valueOf("1"));

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertGte(1, 0);
        }, e -> {
            Assert.assertContains("Expected: a number >= 1", e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertGte(1, 1.1);
        }, e -> {
            Assert.assertContains("Expected: an instance of java.lang.Integer",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertGte(1, '2');
        }, e -> {
            Assert.assertContains("Expected: an instance of java.lang.Integer",
                                  e.getMessage());
        });
    }

    @Test
    public void testAssertLt() {
        Assert.assertLt((byte) 1, Byte.valueOf("0"));
        Assert.assertLt((short) 1, Short.valueOf("0"));
        Assert.assertLt(1, Integer.valueOf("0"));
        Assert.assertLt(1L, Long.valueOf("0"));
        Assert.assertLt(1f, Float.valueOf("0.99"));
        Assert.assertLt(1d, Double.valueOf("0.99"));

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertLt(1, 2);
        }, e -> {
            Assert.assertContains("Expected: a number < 1", e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertGt(1, 0.9);
        }, e -> {
            Assert.assertContains("Expected: an instance of java.lang.Integer",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertGt(1, '0');
        }, e -> {
            Assert.assertContains("Expected: an instance of java.lang.Integer",
                                  e.getMessage());
        });
    }

    @Test
    public void testAssertLte() {
        Assert.assertLte((byte) 1, Byte.valueOf("0"));
        Assert.assertLte((short) 1, Short.valueOf("0"));
        Assert.assertLte(1, Integer.valueOf("0"));
        Assert.assertLte(1L, Long.valueOf("0"));
        Assert.assertLte(1f, Float.valueOf("0.99"));
        Assert.assertLte(1d, Double.valueOf("0.99"));

        Assert.assertLte((byte) 1, Byte.valueOf("1"));
        Assert.assertLte((short) 1, Short.valueOf("1"));
        Assert.assertLte(1, Integer.valueOf("1"));
        Assert.assertLte(1L, Long.valueOf("1"));
        Assert.assertLte(1f, Float.valueOf("1"));
        Assert.assertLte(1d, Double.valueOf("1"));

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertLte(1, 2);
        }, e -> {
            Assert.assertContains("Expected: a number <= 1", e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertLte(1, 0.9);
        }, e -> {
            Assert.assertContains("Expected: an instance of java.lang.Integer",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertLte(1, '0');
        }, e -> {
            Assert.assertContains("Expected: an instance of java.lang.Integer",
                                  e.getMessage());
        });
    }

    @Test
    public void testAssertContains() {
        Assert.assertContains("test", "test");
        Assert.assertContains("test", "hellotest");
        Assert.assertContains("test", "test123");

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertContains("test123", "test");
        }, e -> {
            Assert.assertContains("Expected: a string containing",
                                  e.getMessage());
        });

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertContains("null", null);
        }, e -> {
            Assert.assertContains("Expected: a string containing",
                                  e.getMessage());
        });

        Assert.assertThrows(NullPointerException.class, () -> {
            Assert.assertContains(null, "null");
        });
    }

    @Test
    public void testAssertInstanceOf() {
        Assert.assertInstanceOf(Integer.class, 1);
        Assert.assertInstanceOf(Double.class, 1.0);
        Assert.assertInstanceOf(String.class, "1.0");
        Assert.assertInstanceOf(BaseUnitTest.class, this);

        Assert.assertThrows(AssertionError.class, () -> {
            Assert.assertInstanceOf(Float.class, 1);
        }, e -> {
            Assert.assertContains("Expected: an instance of java.lang.Float",
                                  e.getMessage());
        });
    }
}
