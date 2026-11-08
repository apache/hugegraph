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

package org.apache.hugegraph.unit.util;

import java.util.Map;

import org.apache.hugegraph.api.profile.VersionAPI;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.VersionUtil;
import org.apache.hugegraph.version.ApiVersion;
import org.apache.hugegraph.version.CoreVersion;
import org.junit.Test;

public class VersionTest {

    @Test
    public void testCoreVersionGet() {
        String pomVersion = VersionUtil.getPomVersion();
        Assert.assertNotNull(pomVersion);
        String version = CoreVersion.VERSION.get();
        Assert.assertNotNull(version);
        Assert.assertEquals(pomVersion, version);
    }

    @Test
    public void testCoreVersionCheck() {
        // Expect not throw exception
        CoreVersion.check();
    }

    @Test
    public void testApiVersionCheck() {
        // Expect not throw exception
        ApiVersion.check();
    }

    @Test
    public void testSchemaCloneApiVersionIsAdvertised() {
        Map<?, ?> versions = (Map<?, ?>) ((Map<?, ?>) new VersionAPI().list()).get("versions");
        String api = (String) versions.get("api");
        Assert.assertTrue(VersionUtil.gte(api, "0.73"));
        Assert.assertEquals("0.73", VersionUtil.getApiVersionFromProperties());
    }
}
