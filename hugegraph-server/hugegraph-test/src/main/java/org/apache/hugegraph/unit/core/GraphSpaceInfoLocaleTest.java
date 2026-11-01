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

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.util.JsonUtil;
import org.junit.Test;

/**
 * GraphSpace.info() formats the storage percentage and parses it back;
 * that must not depend on the JVM's default locale (a decimal comma on
 * pl_PL, de_DE, fr_FR, ... used to fail with NumberFormatException).
 */
public class GraphSpaceInfoLocaleTest extends BaseUnitTest {

    private static float storagePercent(Locale locale) {
        Locale savedFormat = Locale.getDefault(Locale.Category.FORMAT);
        Locale savedDisplay = Locale.getDefault(Locale.Category.DISPLAY);
        Locale.setDefault(locale);
        try {
            GraphSpace space = new GraphSpace("gs_locale");
            space.storageLimit(100);
            space.setStorageUsed(33);
            Map<String, Object> info = space.info();
            return (Float) info.get("storage_percent");
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, savedFormat);
            Locale.setDefault(Locale.Category.DISPLAY, savedDisplay);
        }
    }

    @Test
    public void testUsageBelowOneGbRetainsPrecision() {
        GraphSpace space = new GraphSpace("small");
        space.storageLimit(100);
        space.setStorageUsedKb(512 * 1024);
        Map<String, Object> info = space.info();
        Assert.assertEquals(0, info.get("storage_used"));
        Assert.assertEquals(512L * 1024L, info.get("storage_used_kb"));
        Assert.assertEquals(0.005f, (Float) info.get("storage_percent"), 0.000001f);
        GraphSpace restored = JsonUtil.fromJson(JsonUtil.toJson(space), GraphSpace.class);
        Assert.assertEquals(512L * 1024L, restored.info().get("storage_used_kb"));
        space.setStorageUsedKb(0);
        Assert.assertEquals(0L, space.info().get("storage_used_kb"));
    }

    @Test
    public void testLegacyAndCurrentCodecsRoundTripBothDirections() throws IOException {
        GraphSpace upgraded = JsonUtil.fromJson(LegacyGraphSpaceCodec.create("legacy", 3), GraphSpace.class);
        assertLegacyRoundTrip(upgraded, 3L * 1024L * 1024L);

        GraphSpace current = new GraphSpace("current");
        current.configs(Map.of("user_config", "preserved"));
        current.setStorageUsedKb(512L * 1024L);
        assertLegacyRoundTrip(current, 512L * 1024L);
    }

    private static void assertLegacyRoundTrip(GraphSpace space, long usedKb) throws IOException {
        String rewritten = LegacyGraphSpaceCodec.rewrite(JsonUtil.toJson(space));
        Map<?, ?> legacyInfo = JsonUtil.fromJson(rewritten, Map.class);
        Assert.assertEquals(Long.toString(usedKb), legacyInfo.get("storage_used_kb"));
        GraphSpace restored = JsonUtil.fromJson(rewritten, GraphSpace.class);
        Assert.assertEquals(usedKb, restored.info().get("storage_used_kb"));
        Assert.assertEquals((int) (usedKb / (1024L * 1024L)), restored.info().get("storage_used"));
        Assert.assertEquals("preserved", restored.configs().get("user_config"));
        Assert.assertEquals(usedKb, JsonUtil.fromJson(JsonUtil.toJson(restored), GraphSpace.class)
                                          .info().get("storage_used_kb"));
    }

    @Test
    public void testMissingOrInvalidPreciseUsagePreservesLegacyGbUsage() {
        GraphSpace legacy = new GraphSpace("legacy");
        legacy.setStorageUsed(7);
        Map<String, Object> info = legacy.info();
        info.remove("storage_used_kb");
        Assert.assertEquals(7L * 1024L * 1024L,
                            JsonUtil.fromJson(JsonUtil.toJson(info), GraphSpace.class)
                                    .info().get("storage_used_kb"));
        info.remove("creator");
        info.put("storage_used_kb", null);
        info.put("creator", "after_precision");
        for (Object invalid : Arrays.asList(null, -1, "-1", "bad", "1.5", 1.5d, true,
                                            new BigInteger("9223372036854775808"),
                                            "9223372036854775808", Map.of("nested", 1), List.of(1))) {
            info.put("storage_used_kb", invalid);
            GraphSpace restored = JsonUtil.fromJson(JsonUtil.toJson(info), GraphSpace.class);
            Assert.assertEquals(String.valueOf(invalid), 7L * 1024L * 1024L,
                                restored.info().get("storage_used_kb"));
            Assert.assertEquals(7, restored.info().get("storage_used"));
            // Malformed nested values must not terminate the outer object early.
            Assert.assertEquals("after_precision", restored.creator());
        }
        for (Object valid : List.of(0L, "0", 512L * 1024L, " 524288 ")) {
            info.put("storage_used_kb", valid);
            long expected = Long.parseLong(valid.toString().trim());
            Assert.assertEquals(expected, JsonUtil.fromJson(JsonUtil.toJson(info), GraphSpace.class)
                                                 .info().get("storage_used_kb"));
        }
    }

    @Test
    public void testInfoWithDecimalCommaLocales() {
        for (Locale locale : new Locale[]{new Locale("pl", "PL"),
                                          Locale.GERMANY, Locale.FRANCE,
                                          new Locale("ru", "RU")}) {
            Assert.assertEquals(locale.toString(), 0.33f,
                                storagePercent(locale), 0.0001f);
        }
    }

    @Test
    public void testInfoWithDecimalPointLocales() {
        Assert.assertEquals(0.33f, storagePercent(Locale.ROOT), 0.0001f);
        Assert.assertEquals(0.33f, storagePercent(Locale.US), 0.0001f);
    }
}
