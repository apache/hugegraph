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

package org.apache.hugegraph.pd.client;

import org.apache.hugegraph.pd.BaseTest;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class DiscoveryClientTest extends BaseTest {

    private DiscoveryClientImpl client;

    @Before
    public void setUp() {
        client = getClient("appName", "localhost:8654", new HashMap());
    }

    @Test
    public void testGetRegisterNode() {
        // Setup
        try {
            Consumer result = client.getRegisterConsumer();
            final NodeInfo expectedResult = NodeInfo.newBuilder()
                                                    .setAppName("appName")
                                                    .build();

            Thread.sleep(3000);
            Query query = Query.newBuilder().setAppName("appName")
                               .setVersion("0.13.0").build();

            // Run the test
            client.getNodeInfos(query);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }

    }

    private DiscoveryClientImpl getClient(String appName, String address,
                                          Map labels) {
        DiscoveryClientImpl discoveryClient = null;
        try {
            discoveryClient =
                    DiscoveryClientImpl.newBuilder().setCenterAddress(pdGrpcAddr)
                                       .setAddress(address)
                                       .setAppName(appName)
                                       .setDelay(2000)
                                       .setVersion("0.13.0")
                                       .setId("0").setLabels(labels)
                                       .setPdConfig(getPdConfig())
                                       .build();
            discoveryClient.scheduleTask();
        } catch (Exception e) {
            throw new AssertionError("Failed to build DiscoveryClient", e);
        }

        return discoveryClient;
    }
}
