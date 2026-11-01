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

package org.apache.hugegraph.unit.core;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.lock.LockResult;
import org.apache.hugegraph.meta.managers.ServiceMetaManager;
import org.apache.hugegraph.pd.client.DiscoveryClientImpl;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class GraphManagerServiceNamesTest {

    @Test
    public void testServiceUrlsPreservesRawIdentityAndRegisterType() throws Exception {
        DiscoveryClientImpl client = Mockito.mock(DiscoveryClientImpl.class);
        Mockito.when(client.getNodeInfos(Mockito.any(Query.class)))
               .thenReturn(NodeInfos.newBuilder()
                                    .addInfo(NodeInfo.newBuilder().setAddress("http://server:8080"))
                                    .build());
        GraphManager manager = new GraphManager(
                new HugeConfig(new PropertiesConfiguration()),
                new EventHub("service-query-test"));
        try {
            setField(manager, "pdClient", client);
            Class<?> registerType = Class.forName(GraphManager.class.getName() + "$PdRegisterType");
            Object nodePort = Whitebox.getInternalState(registerType, "NODE_PORT");
            Set<String> urls = Whitebox.invoke(GraphManager.class,
                    new Class<?>[]{String.class, String.class, registerType},
                    "getServiceUrls", manager, "team_a", "team/Analytics-prod", nodePort);

            ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
            Mockito.verify(client).getNodeInfos(query.capture());
            Assert.assertEquals(Map.of("GRAPHSPACE", "team_a",
                                       "SERVICE_NAME", "team/Analytics-prod",
                                       "REGISTER_TYPE", "NODE_PORT"), query.getValue().getLabelsMap());
            Assert.assertEquals(Set.of("http://server:8080"), urls);
        } finally {
            manager.close();
        }
    }

    @Test
    public void testReloadPreservesCompleteServiceNames() {
        MetaDriver driver = storageDriver();
        ServiceMetaManager first = new ServiceMetaManager(driver, "cluster");
        for (String name : new String[]{"analytics", "team/analytics",
                                       "analytics-prod", "team/analytics-prod"}) {
            first.addServiceConfig("team_a", service(name));
        }
        first.addServiceConfig("other", service("analytics"));

        ServiceMetaManager reloaded = new ServiceMetaManager(driver, "cluster");
        Map<String, Service> services = reloaded.serviceConfigs("team_a");
        Assert.assertEquals(4, services.size());
        for (String name : new String[]{"analytics", "team/analytics",
                                       "analytics-prod", "team/analytics-prod"}) {
            Assert.assertEquals(name, services.get(name).name());
            Assert.assertEquals(name, reloaded.service("team_a", name).name());
        }
        Assert.assertEquals(1, reloaded.serviceConfigs("other").size());

        reloaded.removeServiceConfig("team_a", "team/analytics");
        Assert.assertNull(reloaded.service("team_a", "team/analytics"));
        Assert.assertNotNull(reloaded.service("team_a", "analytics"));
        Assert.assertEquals(3, reloaded.serviceConfigs("team_a").size());
    }

    @Test
    public void testClearSpaceDropsFullServiceNames() throws Exception {
        MetaDriver driver = storageDriver();
        Constructor<MetaManager> constructor = MetaManager.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        MetaManager metadata = constructor.newInstance();
        setField(metadata, "metaDriver", driver);
        setField(metadata, "cluster", "cluster");
        Whitebox.invoke(MetaManager.class, new Class<?>[]{String.class},
                        "initManagers", metadata, "cluster");
        GraphManager manager = new GraphManager(
                new HugeConfig(new PropertiesConfiguration()),
                new EventHub("service-name-test"));
        try {
            setField(manager, "metaManager", metadata);
            // Exercise the metadata-backed clear path without bootstrapping
            // a PD/HStore cluster; the real managers use a storage-only seam.
            setField(manager, "PDExist", true);
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            spaces.put("team_a", new GraphSpace("team_a"));
            spaces.put("other", new GraphSpace("other"));
            Map<String, Service> cache = Whitebox.getInternalState(manager, "services");
            for (String name : new String[]{"analytics", "analytics-prod",
                                           "team/analytics-prod"}) {
                Service service = service(name);
                metadata.addServiceConfig("team_a", service);
                String key = Whitebox.invoke(GraphManager.class,
                        new Class<?>[]{String.class, String.class}, "serviceName",
                        null, "team_a", name);
                cache.put(key, service);
            }
            metadata.addServiceConfig("other", service("analytics"));
            cache.put("other-analytics", service("analytics"));

            manager.clearGraphSpace("team_a");

            Assert.assertTrue(metadata.serviceConfigs("team_a").isEmpty());
            Assert.assertEquals(1, metadata.serviceConfigs("other").size());
            Assert.assertEquals(1, cache.size());
            Assert.assertTrue(cache.containsKey("other-analytics"));
        } finally {
            manager.close();
        }
    }

    private static Service service(String name) {
        return new Service(name, "admin", Service.ServiceType.OLTP,
                           Service.DeploymentType.MANUAL);
    }

    private static void setField(Object target, String name, Object value)
            throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static MetaDriver storageDriver() {
        Map<String, String> storage = new LinkedHashMap<>();
        MetaDriver driver = Mockito.mock(MetaDriver.class);
        Mockito.doAnswer(call -> {
            storage.put(call.getArgument(0), call.getArgument(1));
            return null;
        }).when(driver).put(Mockito.anyString(), Mockito.anyString());
        Mockito.when(driver.get(Mockito.anyString()))
               .thenAnswer(call -> storage.get(call.<String>getArgument(0)));
        Mockito.doAnswer(call -> {
            storage.remove(call.<String>getArgument(0));
            return null;
        }).when(driver).delete(Mockito.anyString());
        Mockito.doAnswer(call -> {
            storage.keySet().removeIf(key -> key.startsWith(call.getArgument(0)));
            return null;
        }).when(driver).deleteWithPrefix(Mockito.anyString());
        Mockito.when(driver.scanWithPrefix(Mockito.anyString())).thenAnswer(call ->
                storage.entrySet().stream()
                       .filter(entry -> entry.getKey().startsWith(call.getArgument(0)))
                       .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        Mockito.when(driver.tryLock(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong()))
               .thenAnswer(call -> {
                   LockResult lock = new LockResult();
                   lock.lockSuccess(true);
                   return lock;
               });
        return driver;
    }
}
