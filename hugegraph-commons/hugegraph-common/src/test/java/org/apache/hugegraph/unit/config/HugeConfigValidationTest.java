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

import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;
import static org.apache.hugegraph.config.OptionChecker.rangeInt;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.config.ConfigListOption;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionHolder;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class HugeConfigValidationTest extends BaseUnitTest {

    @BeforeClass
    public static void init() {
        OptionSpace.register("validation-test", ValidationOptions.class.getName());
    }

    @Test
    public void testUrlOptionAddsMissingScheme() {
        HugeConfig config = configOf(ValidationOptions.httpUrl.name(), "127.0.0.1:8080");
        Assert.assertEquals("http://127.0.0.1:8080", config.get(ValidationOptions.httpUrl));

        // Surrounding whitespace is trimmed before the scheme is prefixed
        config = configOf(ValidationOptions.httpUrl.name(), "  graph.local:8080/api  ");
        Assert.assertEquals("http://graph.local:8080/api", config.get(ValidationOptions.httpUrl));

        // A configured scheme which already ends with "://" isn't doubled
        config = configOf(ValidationOptions.httpsUrl.name(), "graph.local");
        Assert.assertEquals("https://graph.local", config.get(ValidationOptions.httpsUrl));
    }

    @Test
    public void testUrlOptionKeepsExistingScheme() {
        HugeConfig config = configOf(ValidationOptions.httpUrl.name(), "https://graph.local:8443");
        Assert.assertEquals("https://graph.local:8443", config.get(ValidationOptions.httpUrl));

        // Only the scheme is lower-cased, host and path keep their case
        config = configOf(ValidationOptions.httpUrl.name(), "HTTPS://Graph.Local/Path");
        Assert.assertEquals("https://Graph.Local/Path", config.get(ValidationOptions.httpUrl));
    }

    @Test
    public void testUrlNormalizationSkipsDefaultAndPlainOptions() {
        HugeConfig config = configOf(ValidationOptions.plainUrl.name(), "127.0.0.1:8080");
        Assert.assertEquals("127.0.0.1:8080", config.get(ValidationOptions.plainUrl));
        // Default values are returned as declared and never rewritten
        Assert.assertEquals("localhost:8080", config.get(ValidationOptions.httpUrl));
        Assert.assertTrue(ValidationOptions.httpUrl.needsUrlNormalization());
        Assert.assertEquals("http", ValidationOptions.httpUrl.getDefaultScheme());
        Assert.assertFalse(ValidationOptions.plainUrl.needsUrlNormalization());
        Assert.assertNull(ValidationOptions.plainUrl.getDefaultScheme());
    }

    @Test
    public void testConstructWithInvalidSource() {
        Assert.assertThrows(ConfigException.class, () -> {
            new HugeConfig((Map<String, Object>) null);
        }, e -> {
            Assert.assertContains("The property map is null", e.getMessage());
        });
        Assert.assertThrows(ConfigException.class, () -> {
            new HugeConfig((Configuration) null);
        }, e -> {
            Assert.assertContains("The config object is null", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new HugeConfig("");
        }, e -> {
            Assert.assertContains("The config path can't be empty", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new HugeConfig("target/not-exist-" + System.nanoTime() + ".properties");
        }, e -> {
            Assert.assertContains("Please specify a proper config file", e.getMessage());
        });
        // A directory is not a config file either
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new HugeConfig("src/test");
        });
    }

    @Test
    public void testStringValuesAreParsedAndChecked() {
        HugeConfig config = configOf(ValidationOptions.port.name(), "8182");
        Assert.assertEquals(8182, config.get(ValidationOptions.port));

        Assert.assertThrows(ConfigException.class, () -> {
            configOf(ValidationOptions.port.name(), "70000");
        }, e -> {
            Assert.assertContains("Invalid option value for 'validation.port': 70000",
                                  e.getMessage());
        });
        Assert.assertThrows(ConfigException.class, () -> {
            configOf(ValidationOptions.port.name(), "http");
        }, e -> {
            Assert.assertContains("expect 'Integer' type", e.getMessage());
        });
        Assert.assertThrows(ConfigException.class, () -> {
            configOf(ValidationOptions.name.name(), "  ");
        });
    }

    @Test
    public void testNonStringValueWithWrongType() {
        Map<String, Object> options = new HashMap<>();
        options.put(ValidationOptions.port.name(), 8182L);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new HugeConfig(options);
        }, e -> {
            Assert.assertContains("Invalid value for key 'validation.port': '8182'",
                                  e.getMessage());
        });

        options.put(ValidationOptions.port.name(), 8182);
        Assert.assertEquals(8182, new HugeConfig(options).get(ValidationOptions.port));
    }

    @Test
    public void testUnregisteredOptionIsKeptAsRawValue() {
        HugeConfig config = configOf("validation.unregistered", "raw-value");
        Assert.assertEquals("raw-value", config.getProperty("validation.unregistered"));
        Assert.assertEquals("default-name", config.get(ValidationOptions.name));
    }

    @Test
    public void testGetMapWithInvalidEntry() {
        HugeConfig config = configOf(ValidationOptions.pairs.name(), "[k1: v1, k2:a:b]");
        Assert.assertEquals(ImmutableMap.of("k1", "v1", "k2", "a:b"),
                            config.getMap(ValidationOptions.pairs));

        HugeConfig invalid = configOf(ValidationOptions.pairs.name(), "[k1:v1, k2]");
        Assert.assertThrows(IllegalStateException.class, () -> {
            invalid.getMap(ValidationOptions.pairs);
        }, e -> {
            Assert.assertContains("Invalid option format for 'validation.pairs': k2",
                                  e.getMessage());
            Assert.assertContains("expect KEY:VALUE", e.getMessage());
        });
    }

    private static HugeConfig configOf(String key, Object value) {
        Map<String, Object> options = new HashMap<>();
        options.put(key, value);
        return new HugeConfig(options);
    }

    public static class ValidationOptions extends OptionHolder {

        private static volatile ValidationOptions instance;

        public static synchronized ValidationOptions instance() {
            if (instance == null) {
                instance = new ValidationOptions();
                instance.registerOptions();
            }
            return instance;
        }

        public static final ConfigOption<String> httpUrl =
                new ConfigOption<>(
                        "validation.http_url",
                        "url option normalized with http scheme",
                        disallowEmpty(),
                        "localhost:8080"
                ).withUrlNormalization("http");

        public static final ConfigOption<String> httpsUrl =
                new ConfigOption<>(
                        "validation.https_url",
                        "url option normalized with https scheme",
                        disallowEmpty(),
                        "localhost:8443"
                ).withUrlNormalization("https://");

        public static final ConfigOption<String> plainUrl =
                new ConfigOption<>(
                        "validation.plain_url",
                        "url option without normalization",
                        disallowEmpty(),
                        "localhost:8080"
                );

        public static final ConfigOption<Integer> port =
                new ConfigOption<>(
                        "validation.port",
                        "port option",
                        rangeInt(1, 65535),
                        8080
                );

        public static final ConfigOption<String> name =
                new ConfigOption<>(
                        "validation.name",
                        "non-empty name option",
                        disallowEmpty(),
                        "default-name"
                );

        public static final ConfigListOption<String> pairs =
                new ConfigListOption<>(
                        "validation.pairs",
                        false,
                        "key value pairs",
                        null,
                        String.class,
                        ImmutableList.of()
                );
    }
}
