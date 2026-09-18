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

import java.util.Locale;
import java.util.Map;

import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
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
