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

import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.junit.Assert;
// import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class DiscoveryClientImplTest {

    String address = "localhost:80";
    int delay = 1000;
    int wait = delay * 3 + 500;

    // @Test
    public void registerStore() throws InterruptedException {

        HashMap<String, String> labels = new HashMap<>();

        labels.put("metrics", "/actuator/prometheus");
        labels.put("target", "10.81.116.77:8520");
        labels.put("scheme", "http");
        labels.put("__relabeling", "http");
        labels.put("no_relabeling", "http");
        getClient("store", "address1", labels);

        labels.put("metrics", "/actuator/prometheus");
        labels.put("target", "10.81.116.78:8520");
        labels.put("scheme", "http");
        getClient("store", "address2", labels);

        labels.put("metrics", "/actuator/prometheus");
        labels.put("target", "10.81.116.79:8520");
        labels.put("scheme", "http");
        getClient("store", "address3", labels);

        labels.put("metrics", "/actuator/prometheus");
        labels.put("target", "10.81.116.78:8620");
        labels.put("scheme", "http");
        getClient("pd", "address1", labels);

        labels.put("metrics", "/graph/metrics");
        labels.put("target", "10.37.1.1:9200");
        labels.put("scheme", "https");
        getClient("hugegraph", "address1", labels);
    }

    // @Test
    public void testNodes() throws InterruptedException {
        String appName = "hugegraph";
        register(appName, address);
    }

    // @Test
    public void testMultiNode() throws InterruptedException {
        for (int i = 0; i < 2; i++) {
            register("app" + String.valueOf(i), address + i);
        }
    }

    // @Test
    public void testParallelMultiNode() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(30);
        Vector<Exception> exceptions = new Vector<>();
        for (int i = 0; i < 30; i++) {
            int finalI = i;
            new Thread(() -> {
                try {
                    for (int j = 0; j < 3; j++) {
                        register("app" + finalI, address + j);
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        latch.await();
        Assert.assertTrue(exceptions.size() == 0);
    }

    private static AtomicLong label = new AtomicLong();

    private void register(String appName, String address) throws InterruptedException {

        HashMap<String, String> labels = new HashMap<>();
        String labelValue = String.valueOf(label.incrementAndGet());
        labels.put("address", labelValue);
        labels.put("address1", labelValue);
        Query query = Query.newBuilder().setAppName(
                appName).setVersion("0.13.0").putAllLabels(labels).build();
        DiscoveryClientImpl discoveryClient = getClient(appName, address, labels);
        Thread.sleep(10000);
        NodeInfos nodeInfos1 = discoveryClient.getNodeInfos(query);
        Assert.assertTrue(nodeInfos1.getInfoCount() == 1);
        DiscoveryClientImpl discoveryClient1 = getClient(appName, address + 0, labels);
        Thread.sleep(10000);
        Assert.assertTrue(
                discoveryClient.getNodeInfos(query).getInfoCount() == 2);
        Query query1 = Query.newBuilder().setAppName(
                appName).setVersion("0.12.0").putAllLabels(labels).build();
        Assert.assertTrue(
                discoveryClient.getNodeInfos(query1).getInfoCount() == 0);
        discoveryClient.cancelTask();
        discoveryClient1.cancelTask();
        Thread.sleep(wait);
        NodeInfos nodeInfos = discoveryClient.getNodeInfos(query);
        System.out.println(nodeInfos);
        Assert.assertTrue(nodeInfos.getInfoCount() == 0);
        discoveryClient.close();
        discoveryClient1.close();
    }

    private DiscoveryClientImpl getClient(String appName, String address, Map labels) {
        DiscoveryClientImpl discoveryClient = null;
        try {
            discoveryClient = DiscoveryClientImpl.newBuilder().setCenterAddress(
                    "localhost:8687,localhost:8686,localhost:8688").setAddress(address).setAppName(
                    appName).setDelay(delay).setVersion("0.13.0").setId(
                    "0").setLabels(labels).build();
            discoveryClient.scheduleTask();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return discoveryClient;
    }
}
