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

package org.apache.hugegraph.meta.managers;

import java.util.Map;

import org.apache.hugegraph.auth.HugeTarget;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.JsonUtil;
import org.junit.Test;
import org.mockito.Mockito;

public class AuthMetaManagerTest {

    @Test
    public void testGetTargetRestoresLegacyGraphSpaceFromNamespace()
            throws Exception {
        MetaDriver driver = Mockito.mock(MetaDriver.class);
        Map<String, Object> legacy = target("DEFAULT").asMap();
        legacy.remove("graphspace");
        Mockito.when(driver.get(Mockito.anyString()))
               .thenReturn(JsonUtil.toJson(legacy));
        AuthMetaManager manager = new AuthMetaManager(driver, "cluster");

        HugeTarget restored = manager.getTarget(
                              "SPACE_A", IdGenerator.of("target"));

        Assert.assertEquals("SPACE_A", restored.graphSpace());
    }

    @Test
    public void testGetTargetRejectsExplicitGraphSpaceMismatch() {
        MetaDriver driver = Mockito.mock(MetaDriver.class);
        Mockito.when(driver.get(Mockito.anyString()))
               .thenReturn(JsonUtil.toJson(target("SPACE_B").asMap()));
        AuthMetaManager manager = new AuthMetaManager(driver, "cluster");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.getTarget("SPACE_A", IdGenerator.of("target"));
        });
    }

    @Test
    public void testDeleteTargetRejectsMismatchBeforeMutation() {
        MetaDriver driver = Mockito.mock(MetaDriver.class);
        Mockito.when(driver.get(Mockito.anyString()))
               .thenReturn(JsonUtil.toJson(target("SPACE_B").asMap()));
        AuthMetaManager manager = new AuthMetaManager(driver, "cluster");

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.deleteTarget("SPACE_A", IdGenerator.of("target"));
        });
        Mockito.verify(driver, Mockito.never()).delete(Mockito.anyString());
    }

    private static HugeTarget target(String graphSpace) {
        HugeTarget target = new HugeTarget("target", "hugegraph", "url");
        target.graphSpace(graphSpace);
        target.creator("admin");
        return target;
    }
}
