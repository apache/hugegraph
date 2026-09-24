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

package org.apache.hugegraph.pd.service;

import org.apache.hugegraph.pd.KvService;
import org.apache.hugegraph.pd.config.PDConfig;
import org.junit.Assert;
import org.junit.Test;

public class KvServiceTest {

    @Test
    public void testKv() {
        try {
            PDConfig pdConfig = BaseServerTest.getConfig();
            KvService service = new KvService(pdConfig);
            String key = "kvTest";
            String kvTest = service.get(key);
            Assert.assertEquals(kvTest, "");
            service.put(key, "kvTestValue");
            kvTest = service.get(key);
            Assert.assertEquals(kvTest, "kvTestValue");
            service.scanWithPrefix(key);
            service.delete(key);
            service.put(key, "kvTestValue");
            service.deleteWithPrefix(key);
            service.put(key, "kvTestValue", 1000L);
            service.keepAlive(key);
        } catch (Exception e) {

        }
    }

    @Test
    public void testMember() {
        try {
            PDConfig pdConfig = BaseServerTest.getConfig();
            KvService service = new KvService(pdConfig);
            service.setPdConfig(pdConfig);
            PDConfig config = service.getPdConfig();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
