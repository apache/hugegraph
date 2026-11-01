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

package org.apache.hugegraph.rpc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import org.apache.hugegraph.util.E;

public class RpcProviderConfig implements RpcServiceConfig4Server {

    private final Map<String, ProviderConfig<?>> configs = new ConcurrentHashMap<>();

    @Override
    public <T, S extends T> String addService(Class<T> clazz, S serviceImpl) {
        return this.addService(null, clazz.getName(), serviceImpl);
    }

    @Override
    public <T, S extends T> String addService(String graph,
                                              Class<T> clazz,
                                              S serviceImpl) {
        return this.addService(graph, clazz.getName(), serviceImpl);
    }

    private <T, S extends T> String addService(String graph,
                                               String interfaceId,
                                               S serviceImpl) {
        ProviderConfig<T> providerConfig = new ProviderConfig<>();
        String serviceId;
        if (graph != null) {
            serviceId = interfaceId + ":" + graph;
            providerConfig.setId(serviceId).setUniqueId(graph);
        } else {
            serviceId = interfaceId;
        }

        providerConfig.setInterfaceId(interfaceId)
                      .setRef(serviceImpl);

        E.checkArgument(this.configs.putIfAbsent(serviceId, providerConfig) == null,
                        "Not allowed to add service already exist: '%s'",
                        serviceId);
        return serviceId;
    }

    @Override
    public void removeService(String serviceId) {
        boolean[] removed = {false};
        this.configs.computeIfPresent(serviceId, (key, config) -> {
            config.unExport();
            removed[0] = true;
            return null;
        });
        E.checkArgument(removed[0], "The service '%s' doesn't exist", serviceId);
    }

    @Override
    public boolean removeService(String serviceId, Object serviceImpl) {
        boolean[] removed = {false};
        this.configs.computeIfPresent(serviceId, (key, config) -> {
            if (config.getRef() != serviceImpl) {
                return config;
            }
            config.unExport();
            removed[0] = true;
            return null;
        });
        return removed[0];
    }

    @Override
    public void removeAllService() {
        for (String serviceId : this.configs.keySet()) {
            this.configs.computeIfPresent(serviceId, (key, config) -> {
                config.unExport();
                return null;
            });
        }
    }

    void exportAll(ServerConfig server, int timeout) {
        for (String serviceId : this.configs.keySet()) {
            this.configs.computeIfPresent(serviceId, (key, config) -> {
                // Sofa appends a single server; retain other bindings without adding duplicates.
                if (config.getServer() == null || config.getServer().stream().noneMatch(bound -> bound == server)) {
                    config.setServer(server);
                }
                config.setTimeout(timeout).export();
                return config;
            });
        }
    }

    public Map<String, ProviderConfig<?>> configs() {
        return this.configs;
    }
}
