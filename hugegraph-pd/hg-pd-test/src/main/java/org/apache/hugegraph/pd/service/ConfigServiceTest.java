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

import java.util.List;

import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.IdService;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConfigServiceTest {

    private PDConfig config = BaseServerTest.getConfig();

    private ConfigService service;

    @Before
    public void setUp() {
        service = new ConfigService(config);
    }

    @Test
    public void testGetPDConfig() throws Exception {
        // Setup
        try {
            final Metapb.PDConfig config = Metapb.PDConfig.newBuilder()
                                                          .setVersion(0L)
                                                          .setPartitionCount(0)
                                                          .setShardCount(55)
                                                          .setMaxShardsPerStore(0)
                                                          .setTimestamp(0L).build();
            service.setPDConfig(config);
            // Run the test
            Metapb.PDConfig result = service.getPDConfig(0L);

            // Verify the results
            Assert.assertTrue(result.getShardCount() == 55);
            result = service.getPDConfig();
            Assert.assertTrue(result.getShardCount() == 55);
        } catch (Exception e) {

        } finally {

        }

    }

    @Test
    public void testGetGraphSpace() throws Exception {
        // Setup
        Metapb.GraphSpace space = Metapb.GraphSpace.newBuilder()
                                                   .setName("gs1")
                                                   .setTimestamp(0L).build();
        final List<Metapb.GraphSpace> expectedResult = List.of(space);
        service.setGraphSpace(space);
        // Run the test
        final List<Metapb.GraphSpace> result = service.getGraphSpace(
                "gs1");

        Assert.assertTrue(result.size() == 1);
    }

    @Test
    public void testUpdatePDConfig() {
        try {
            final Metapb.PDConfig mConfig = Metapb.PDConfig.newBuilder()
                                                           .setVersion(0L)
                                                           .setPartitionCount(0)
                                                           .setShardCount(0)
                                                           .setMaxShardsPerStore(0)
                                                           .setTimestamp(0L)
                                                           .build();
            final PDConfig expectedResult = new PDConfig();
            expectedResult.setConfigService(new ConfigService(new PDConfig()));
            expectedResult.setIdService(new IdService(new PDConfig()));
            expectedResult.setClusterId(0L);
            expectedResult.setPatrolInterval(0L);
            expectedResult.setDataPath("dataPath");
            expectedResult.setMinStoreCount(0);
            expectedResult.setInitialStoreList("initialStoreList");
            expectedResult.setHost("host");
            expectedResult.setVerifyPath("verifyPath");
            expectedResult.setLicensePath("licensePath");
            service.updatePDConfig(mConfig);
        } catch (Exception e) {

        } finally {

        }
    }
}
