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

package org.apache.hugegraph.unit.config;

import static org.apache.hugegraph.config.OptionChecker.allowValues;
import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;
import static org.apache.hugegraph.config.OptionChecker.inValues;
import static org.apache.hugegraph.config.OptionChecker.nonNegativeInt;
import static org.apache.hugegraph.config.OptionChecker.positiveInt;
import static org.apache.hugegraph.config.OptionChecker.rangeDouble;
import static org.apache.hugegraph.config.OptionChecker.rangeInt;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.config.ConfigConvOption;
import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.config.ConfigListConvOption;
import org.apache.hugegraph.config.ConfigListOption;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

public class TypedOptionTest {

    @Test
    public void testParseConvertNumbersAndBooleans() {
        ConfigOption<Integer> intOption = new ConfigOption<>(
                "typed.int", "int option", rangeInt(1, 100), 10);
        Assert.assertEquals(42, intOption.parseConvert("42"));
        Assert.assertEquals(1, intOption.parseConvert("1"));
        Assert.assertEquals(100, intOption.parseConvert("100"));

        Assert.assertThrows(ConfigException.class, () -> {
            intOption.parseConvert("forty-two");
        }, e -> {
            Assert.assertContains("Invalid type of value 'forty-two' for option 'typed.int'",
                                  e.getMessage());
            Assert.assertContains("expect 'Integer' type", e.getMessage());
        });
        Assert.assertThrows(ConfigException.class, () -> {
            intOption.parseConvert("101");
        }, e -> {
            Assert.assertContains("Invalid option value for 'typed.int': 101", e.getMessage());
        });
        Assert.assertThrows(ConfigException.class, () -> intOption.parseConvert("0"));

        ConfigOption<Long> longOption = new ConfigOption<>(
                "typed.long", "long option", null, 1L);
        Assert.assertEquals(Long.MAX_VALUE, longOption.parseConvert("9223372036854775807"));
        Assert.assertThrows(ConfigException.class, () -> {
            longOption.parseConvert("9223372036854775808");
        });

        ConfigOption<Double> doubleOption = new ConfigOption<>(
                "typed.double", "double option", rangeDouble(0D, 1D), 0.5D);
        Assert.assertEquals(0.25D, doubleOption.parseConvert("0.25"));
        Assert.assertThrows(ConfigException.class, () -> doubleOption.parseConvert("1.01"));
        Assert.assertThrows(ConfigException.class, () -> doubleOption.parseConvert("half"));

        ConfigOption<Boolean> boolOption = new ConfigOption<>(
                "typed.bool", "bool option", disallowEmpty(), false);
        Assert.assertEquals(true, boolOption.parseConvert("true"));
        Assert.assertEquals(false, boolOption.parseConvert("false"));
        Assert.assertThrows(ConfigException.class, () -> {
            boolOption.parseConvert("maybe");
        }, e -> {
            Assert.assertContains("expect 'Boolean' type", e.getMessage());
        });
    }

    @Test
    public void testParseConvertStringWithChecker() {
        ConfigOption<String> option = new ConfigOption<>(
                "typed.choice", "choice option",
                allowValues("rocksdb", "hstore"), "rocksdb");
        Assert.assertEquals("hstore", option.parseConvert("hstore"));
        // Values are matched exactly, no case folding or trimming
        Assert.assertThrows(ConfigException.class, () -> {
            option.parseConvert("RocksDB");
        }, e -> {
            Assert.assertContains("Invalid option value for 'typed.choice': RocksDB",
                                  e.getMessage());
        });
        Assert.assertThrows(ConfigException.class, () -> option.parseConvert(" hstore"));

        ConfigOption<String> nonEmpty = new ConfigOption<>(
                "typed.non_empty", "non-empty option", disallowEmpty(), "value");
        Assert.assertEquals(" value ", nonEmpty.parseConvert(" value "));
        Assert.assertThrows(ConfigException.class, () -> nonEmpty.parseConvert(""));
        Assert.assertThrows(ConfigException.class, () -> nonEmpty.parseConvert("   "));
    }

    @Test
    public void testParseConvertClass() {
        ConfigOption<Class> option = new ConfigOption<>(
                "typed.class", "class option", null, Object.class);
        Assert.assertEquals(String.class, option.parseConvert("java.lang.String"));
        // Accepts the format produced by Class.toString()
        Assert.assertEquals(String.class, option.parseConvert(String.class.toString()));

        Assert.assertThrows(ConfigException.class, () -> {
            option.parseConvert("org.apache.hugegraph.NoSuchClass");
        }, e -> {
            Assert.assertContains("Failed to parse Class from String " +
                                  "'org.apache.hugegraph.NoSuchClass'", e.getMessage());
            Assert.assertInstanceOf(ClassNotFoundException.class, e.getCause());
        });
    }

    @Test
    public void testConstructWithInvalidTypeOrDefault() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new ConfigOption<>("typed.map", "map option", new HashMap<String, String>());
        }, e -> {
            Assert.assertContains("doesn't belong to acceptable type set", e.getMessage());
        });

        Assert.assertThrows(NullPointerException.class, () -> {
            new ConfigOption<>("typed.null", false, "null default", null, String.class, null);
        }, e -> {
            Assert.assertContains("'value' of 'typed.null' can't be null", e.getMessage());
        });

        Assert.assertThrows(NullPointerException.class, () -> {
            new ConfigOption<>(null, false, "no name", null, String.class, "v");
        });
    }

    @Test
    public void testListOptionParse() {
        ConfigListOption<Integer> option = new ConfigListOption<>(
                "typed.list", "int list", disallowEmpty(), 1, 2);
        Assert.assertEquals(ImmutableList.of(1, 2), option.defaultValue());
        Assert.assertEquals(ImmutableList.of(3, 4, 5), option.parseConvert("[3, 4 ,5]"));
        Assert.assertEquals(ImmutableList.of(7), option.parseConvert("7"));

        Assert.assertThrows(ConfigException.class, () -> {
            option.parseConvert("[1, two]");
        }, e -> {
            Assert.assertContains("Invalid type of value 'two'", e.getMessage());
        });
        // An empty list literal yields one blank element, which isn't a valid integer
        Assert.assertThrows(ConfigException.class, () -> option.parseConvert("[]"));
    }

    @Test
    public void testListOptionWithInValuesChecker() {
        ConfigListOption<String> option = new ConfigListOption<>(
                "typed.list_in", "string list", inValues("a", "b", "c"), "a");
        Assert.assertEquals(ImmutableList.of("c", "a"), option.parseConvert("[c, a]"));
        Assert.assertThrows(ConfigException.class, () -> {
            option.parseConvert("[a, d]");
        }, e -> {
            Assert.assertContains("Invalid option value for 'typed.list_in'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new ConfigListOption<String>("typed.list_empty", false, "empty list",
                                         null, null, Collections.emptyList());
        }, e -> {
            Assert.assertContains("Element class can't be null", e.getMessage());
        });
    }

    @Test
    public void testConvOptionChecksRawValueBeforeConvert() {
        ConfigConvOption<String, TimeUnit> option = new ConfigConvOption<>(
                "typed.unit", "time unit", allowValues("SECONDS", "MINUTES"),
                TimeUnit::valueOf, "SECONDS");
        Assert.assertEquals(TimeUnit.SECONDS, option.defaultValue());
        Assert.assertEquals(TimeUnit.MINUTES, option.parseConvert("MINUTES"));
        // HOURS is a valid TimeUnit but is rejected by the raw value checker
        Assert.assertThrows(ConfigException.class, () -> option.parseConvert("HOURS"));

        ConfigConvOption<String, TimeUnit> unchecked = new ConfigConvOption<>(
                "typed.unit_unchecked", "time unit", null, TimeUnit::valueOf, "SECONDS");
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            unchecked.parseConvert("WEEKS");
        });

        Assert.assertThrows(NullPointerException.class, () -> {
            new ConfigConvOption<String, TimeUnit>("typed.unit_null", "time unit",
                                                   null, null, "SECONDS");
        }, e -> {
            Assert.assertContains("'convert' can't be null", e.getMessage());
        });
    }

    @Test
    public void testListConvOptionParse() {
        ConfigListConvOption<String, TimeUnit> option = new ConfigListConvOption<>(
                "typed.units", "time units", disallowEmpty(), TimeUnit::valueOf,
                "SECONDS", "DAYS");
        Assert.assertEquals(ImmutableList.of(TimeUnit.SECONDS, TimeUnit.DAYS),
                            option.defaultValue());
        Assert.assertEquals(ImmutableList.of(TimeUnit.HOURS, TimeUnit.MINUTES),
                            option.parseConvert("[HOURS, MINUTES]"));
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            option.parseConvert("[HOURS, FORTNIGHTS]");
        });
    }

    @Test
    public void testOptionCheckerBoundariesAndNulls() {
        Predicate<Integer> range = rangeInt(-1, 1);
        Assert.assertTrue(range.apply(-1));
        Assert.assertTrue(range.apply(1));
        Assert.assertFalse(range.apply(-2));
        Assert.assertFalse(range.apply(2));
        Assert.assertFalse(range.apply(null));

        Predicate<Double> doubles = rangeDouble(0.1D, 0.2D);
        Assert.assertTrue(doubles.apply(0.1D));
        Assert.assertTrue(doubles.apply(0.2D));
        Assert.assertFalse(doubles.apply(0.20001D));
        Assert.assertFalse(doubles.apply(null));

        Predicate<Long> positive = positiveInt();
        Assert.assertTrue(positive.apply(1L));
        Assert.assertFalse(positive.apply(0L));
        Assert.assertFalse(positive.apply(null));

        Predicate<Long> nonNegative = nonNegativeInt();
        Assert.assertTrue(nonNegative.apply(0L));
        Assert.assertFalse(nonNegative.apply(-1L));
        Assert.assertFalse(nonNegative.apply(null));

        Predicate<String> allowed = allowValues("x", "y");
        Assert.assertTrue(allowed.apply("y"));
        Assert.assertFalse(allowed.apply("z"));
        Assert.assertFalse(allowed.apply(null));

        Predicate<List<String>> in = inValues("x", "y");
        Assert.assertTrue(in.apply(ImmutableList.of()));
        Assert.assertTrue(in.apply(ImmutableList.of("x", "x", "y")));
        Assert.assertFalse(in.apply(ImmutableList.of("x", "z")));
        Assert.assertFalse(in.apply(null));
    }

    @Test
    public void testDisallowEmptyChecker() {
        Predicate<Object> nonEmpty = disallowEmpty();
        Assert.assertFalse(nonEmpty.apply(null));
        Assert.assertFalse(nonEmpty.apply(""));
        Assert.assertFalse(nonEmpty.apply(" \t"));
        Assert.assertFalse(nonEmpty.apply(new String[0]));
        Assert.assertFalse(nonEmpty.apply(new int[0]));
        Assert.assertFalse(nonEmpty.apply(Collections.emptyList()));
        Assert.assertFalse(nonEmpty.apply(Collections.emptySet()));

        Assert.assertTrue(nonEmpty.apply("a"));
        Assert.assertTrue(nonEmpty.apply(new String[]{""}));
        Assert.assertTrue(nonEmpty.apply(ImmutableList.of("")));
        Assert.assertTrue(nonEmpty.apply(0));
        Assert.assertTrue(nonEmpty.apply(false));
    }
}
