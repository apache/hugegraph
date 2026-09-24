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

package org.apache.hugegraph.space.register.registerImpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.hugegraph.constant.ServiceConstant;
import org.apache.hugegraph.pd.client.DiscoveryClient;
import org.apache.hugegraph.pd.client.DiscoveryClientImpl;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.space.register.IServiceRegister;
import org.apache.hugegraph.space.register.RegisterConfig;
import org.apache.hugegraph.space.register.dto.PortDTO;
import org.apache.hugegraph.space.register.dto.ServiceDTO;

import com.google.gson.Gson;

public class SampleRegister implements IServiceRegister {

    private DiscoveryClient client = null;

    private RegisterConfig decodeConfigMap(String configMap) {
        RegisterConfig config = new RegisterConfig();
        Gson gson = new Gson();
        ServiceDTO serviceDTO = gson.fromJson(configMap, ServiceDTO.class);
        config.setNodePort(serviceDTO.getSpec().getPorts().get(0).getNodePort().toString());
        config.setNodeName(serviceDTO.getSpec().getClusterIP());
        config.setPodIp("127.0.0.1");
        config.setPodPort("8080");
        return config;
    }

    public String init(String appName) throws Exception {
        File file = new File("/home/scorpiour/HugeGraph/hugegraph-plugin/example/k8s-service.json");

        try (FileInputStream input = new FileInputStream(file)) {
            System.out.printf("load file: %s%n", file.toPath());
            long fileLength = file.length();
            byte[] bytes = new byte[(int) fileLength];
            input.read(bytes);
            String configMap = new String(bytes);
            RegisterConfig config = this.decodeConfigMap(configMap);
            config.setGrpcAddress("127.0.0.1:8686");
            config.setAppName(appName);
            System.out.printf("load file: %s%n", file.toPath());
            return this.registerService(config);
        } catch (IOException ignored) {
        }

        return "";
    }

    public String registerService(RegisterConfig config) {
        if (null != this.client) {
            this.client.cancelTask();
        }

        System.out.println("going to attach client");
        String address = config.getNodeName() + ":" + config.getNodePort();
        String clientId = config.getAppName() + ":" + address;

        try {
            PDConfig pdConfig = PDConfig.of(config.getGrpcAddress());
            pdConfig.setAuthority(ServiceConstant.SERVICE_NAME,
                                  ServiceConstant.AUTHORITY);
            DiscoveryClient client = DiscoveryClientImpl.newBuilder().setPdConfig(pdConfig)
                                                        .setCenterAddress(config.getGrpcAddress())
                                                        .setAddress(address)
                                                        .setAppName(config.getAppName())
                                                        .setDelay(config.getInterval())
                                                        .setVersion(config.getVersion())
                                                        .setId(clientId)
                                                        .setLabels(config.getLabelMap()).build();
            this.client = client;
            client.scheduleTask();
            System.out.println("going to schedule client");
            return clientId;
        } catch (Exception var6) {
            return "";
        }
    }

    public void unregister(RegisterConfig config) {
        this.unregisterAll();
    }

    public void unregister(String id) {
        this.unregisterAll();
    }

    public void unregisterAll() {
        if (null != this.client) {
            synchronized (this.client) {
                this.client.cancelTask();
            }
        }

    }

    public Map<String, NodeInfos> getServiceInfo(String serviceId) {
        return null;
    }

    public void close() {
    }
}
