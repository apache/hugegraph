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

package org.apache.hugegraph.bootstrap;

import org.junit.Assert;
import org.junit.Test;

public class HugeGraphServerBootstrapTest {

    @Test
    public void testValidateDnsCacheTtlAcceptsFinitePositiveValue() {
        HugeGraphServerBootstrap.validateDnsCacheTtl("1");
        HugeGraphServerBootstrap.validateDnsCacheTtl("30");
        HugeGraphServerBootstrap.validateDnsCacheTtl("2147483647");
    }

    @Test
    public void testValidateDnsCacheTtlRejectsInvalidValues() {
        assertInvalidDnsCacheTtl(null);
        assertInvalidDnsCacheTtl("");
        assertInvalidDnsCacheTtl("0");
        assertInvalidDnsCacheTtl("-1");
        assertInvalidDnsCacheTtl("2147483648");
        assertInvalidDnsCacheTtl("invalid");
    }

    private static void assertInvalidDnsCacheTtl(String value) {
        try {
            HugeGraphServerBootstrap.validateDnsCacheTtl(value);
            Assert.fail("Expected invalid DNS cache TTL: " + value);
        } catch (RuntimeException ignored) {
            // Expected.
        }
    }
}
