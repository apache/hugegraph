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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.hugegraph.constant.ServiceConstant;
import org.apache.hugegraph.pd.client.DiscoveryClient;
import org.apache.hugegraph.pd.client.DiscoveryClientImpl;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.space.register.IServiceRegister;
import org.apache.hugegraph.space.register.RegisterConfig;
import org.apache.hugegraph.space.register.dto.ApplicationDTO;
import org.apache.hugegraph.space.register.dto.EurekaDTO;
import org.apache.hugegraph.space.register.dto.EurekaInstanceDTO;
import org.apache.hugegraph.space.register.dto.ServiceDTO;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

public class PdRegister implements IServiceRegister {

    private static final Object MTX = new Object();
    private static PdRegister instance = null;
    private final String service;
    private final String token;
    private final Map<String, Set<DiscoveryClient>> clientMap = new ConcurrentHashMap();
    private final Map<String, RegisterConfig> configMap = new HashMap();
    private final Map<String, EurekaDTO> ddsMap = new ConcurrentHashMap();
    private HttpClient httpClient;
    private HttpClient ddsClient;
    private ScheduledExecutorService pool;

    private PdRegister(String service, String token) {
        this.service = service;
        this.token = token;
    }

    public static PdRegister getInstance() {
        return getInstance(ServiceConstant.SERVICE_NAME, ServiceConstant.AUTHORITY);
    }

    //FIXME: pd auth:use this method to replace getInstance()
    public static PdRegister getInstance(String service, String token) {
        synchronized (MTX) {
            if (null == instance) {
                instance = new PdRegister(service, token);
            }

            return instance;
        }
    }

    private String generateServiceId(RegisterConfig config) {
        byte[] md5 = null;
        String origin = config.getAppName() + config.getPodIp() + config.getNodeName();

        try {
            md5 = MessageDigest.getInstance("md5").digest(origin.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException var7) {
        }

        String md5code = (new BigInteger(1, md5)).toString(16);
        String prefix = "";

        for (int i = 0; i < 32 - md5code.length(); ++i) {
            prefix = prefix + "0";
        }

        return prefix + md5code;
    }

    private String loadConfigMap() throws Exception {
        this.initHttpClient();
        String host = this.getServiceHost();
        String namespace = this.getNamespace();
        String appName = this.getAppName();
        String url = String.format("https://%s/api/v1/namespaces/%s/services/%s", host, namespace,
                                   appName);
        HttpGet get = new HttpGet(url);
        String token = this.getKubeToken();
        get.setHeader("Authorization", "Bearer " + token);
        get.setHeader("Content-Type", "application/json");
        HttpResponse response = this.httpClient.execute(get);
        String configMap = EntityUtils.toString(response.getEntity());
        return configMap;
    }

    private RegisterConfig decodeConfigMap(String configMap) {
        RegisterConfig config = new RegisterConfig();
        Gson gson = new Gson();
        ServiceDTO serviceDTO = gson.fromJson(configMap, ServiceDTO.class);
        config.setNodePort(
                serviceDTO.getSpec().getPorts().get(0).getNodePort().toString());
        config.setNodeName(serviceDTO.getSpec().getClusterIP());
        return config;
    }

    private void initHttpClient() throws Exception {
        if (this.httpClient == null) {
            File certFile = new File("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt");
            SSLContext ssl = SSLContexts.custom().loadTrustMaterial(certFile).build();
            SSLConnectionSocketFactory sslsf =
                    new SSLConnectionSocketFactory(ssl, new String[]{"TLSv1", "TLSv1.1", "TLSv1.2"},
                                                   null, NoopHostnameVerifier.INSTANCE);
            HttpClient client = HttpClients.custom().setSSLSocketFactory(sslsf).build();
            this.httpClient = client;
        }
    }

    public String init(String appName) throws Exception {
        this.initHttpClient();
        String rawConfig = this.loadConfigMap();
        RegisterConfig config = this.decodeConfigMap(rawConfig);
        config.setAppName(appName);
        return this.registerService(config);
    }

    private String getKubeToken() {
        String path = "/var/run/secrets/kubernetes.io/serviceaccount/token";
        File file = new File(path);
        String result = "";

        try {
            if (file.canRead()) {
                FileReader reader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(reader);
                String namespace = bufferedReader.readLine();
                namespace = namespace.trim();
                result = namespace;
                bufferedReader.close();
            } else {
                System.out.println("Cannot read namespace file");
            }
        } catch (Throwable var10) {
        }

        return result;
    }

    private String getAppName() {
        String appName = System.getenv("APP_NAME");
        return Strings.isNullOrEmpty(appName) ? "kuboard" : appName;
    }

    private String getNamespace() {
        String path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
        File file = new File(path);
        String result = "";

        try {
            if (file.canRead()) {
                FileReader reader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(reader);
                String namespace = bufferedReader.readLine();
                namespace = namespace.trim();
                result = namespace;
                bufferedReader.close();
            } else {
                System.out.println("Cannot read namespace file");
            }
        } catch (Throwable var10) {
        }

        return result;
    }

    private String getServiceHost() {
        String host = System.getenv("KUBERNETES_SERVICE_HOST");
        return host;
    }

    public String registerService(RegisterConfig config) {
        try {
            String serviceId = this.registerClient(config);
            this.registerDDS(config);
            return serviceId;
        } catch (Throwable e) {
            System.out.println(e);
            return null;
        }
    }

    private void initDDSClient() {
        HttpClient client = HttpClients.custom().build();
        this.ddsClient = client;
    }

    private EurekaInstanceDTO buildEurekaInstanceDTO(String serviceName, String host,
                                                     Integer port) {
        String url = host.trim() + (null != port && port > 0 ? ":" + port : "");
        EurekaInstanceDTO instance =
                (new EurekaInstanceDTO()).setInstanceId(url).setHostName(host).setApp(serviceName)
                                         .setIpAddr(host)
                                         .setPort(ImmutableMap.of("$", port, "@enabled", true))
                                         .setMetadata(
                                                 ImmutableMap.of("zone", "A", "ddsServiceGroup",
                                                                 "DFS-TEST")).setStatus("UP")
                                         .setDataCenterInfo(ImmutableMap.of("@class",
                                                                            "com.netflix.appinfo" +
                                                                            ".InstanceInfo$DefaultDataCenterInfo",
                                                                            "name", "MyOwn"))
                                         .setHealthCheckUrl("").setSecureViaAddress(serviceName)
                                         .setVipAddress(serviceName).setSecurePort(
                                                 ImmutableMap.of("$", 443, "@enabled", false)).setHomePageUrl("")
                                         .setStatusPageUrl("");
        return instance;
    }

    private List<EurekaDTO> buildEurekaDTO(String serviceName, RegisterConfig config) {
        List<EurekaDTO> dtoList = new ArrayList();
        if (null != config.getUrls()) {
            config.getUrls().forEach((url) -> {
                try {
                    EurekaDTO dto = new EurekaDTO();
                    URL info = new URL(url);
                    EurekaInstanceDTO instance =
                            this.buildEurekaInstanceDTO(serviceName, info.getHost(),
                                                        info.getPort());
                    dto.setInstance(instance);
                    dtoList.add(dto);
                } catch (Throwable var7) {
                }

            });
        }

        if (null != config.getNodeName() && null != config.getNodePort()) {
            try {
                EurekaDTO dto = new EurekaDTO();
                EurekaInstanceDTO instance =
                        this.buildEurekaInstanceDTO(serviceName, config.getNodeName(),
                                                    Integer.parseInt(config.getNodePort()));
                dto.setInstance(instance);
                dtoList.add(dto);
            } catch (Throwable var7) {
            }
        }

        if (null != config.getPodIp() && null != config.getPodPort()) {
            try {
                EurekaDTO dto = new EurekaDTO();
                EurekaInstanceDTO instance =
                        this.buildEurekaInstanceDTO(serviceName, config.getPodIp(),
                                                    Integer.parseInt(config.getPodPort()));
                dto.setInstance(instance);
                dtoList.add(dto);
            } catch (Throwable var6) {
            }
        }

        return dtoList;
    }

    private boolean examGetResponse(HttpResponse response, String ipAddress) {
        HttpEntity respBody = response.getEntity();
        if (null != respBody) {
            try {
                InputStream content = respBody.getContent();
                Scanner sc = new Scanner(content);
                byte[] data = sc.next().getBytes();
                String contentStr = new String(data);
                sc.close();
                Gson gson = new Gson();
                ApplicationDTO app =
                        gson.fromJson(contentStr, ApplicationDTO.class);
                boolean hasOther = app.getApplication().getInstance().stream().anyMatch(
                        (instance) -> !instance.getIpAddr().equals(ipAddress) &&
                                      instance.getStatus().equals("UP"));
                return !hasOther;
            } catch (IOException var11) {
                return false;
            } catch (Exception var12) {
                return false;
            }
        } else {
            return true;
        }
    }

    private void registerDDS(RegisterConfig config) {
        if (!Strings.isNullOrEmpty(config.getDdsHost())) {
            synchronized (MTX) {
                if (null == this.pool) {
                    this.pool = new ScheduledThreadPoolExecutor(1);
                }

                if (null == this.ddsClient) {
                    this.initDDSClient();
                }
            }

            String serviceName = config.getLabelMap().get("SERVICE_NAME");
            List<EurekaDTO> eurekaDTOList = this.buildEurekaDTO(serviceName, config);
            eurekaDTOList.forEach(
                    (dto) -> this.ddsMap.put(serviceName + dto.getInstance().getInstanceId(), dto));
            this.pool.scheduleAtFixedRate(() -> {
                String contentType = "application/json";

                try {
                    String url = String.format("http://%s/eureka/apps/%s", config.getDdsHost(),
                                               serviceName);

                    for (Map.Entry<String, EurekaDTO> entry : this.ddsMap.entrySet()) {
                        try {
                            boolean ddsPost = true;
                            EurekaDTO dto = entry.getValue();
                            if (config.getDdsSlave()) {
                                HttpGet get = new HttpGet(url);
                                get.setHeader("Content-Type", contentType);
                                get.setHeader("Accept", contentType);
                                HttpResponse getResp = this.ddsClient.execute(get);
                                ddsPost = this.examGetResponse(getResp,
                                                               dto.getInstance().getIpAddr());
                            }

                            dto.getInstance().setStatus(ddsPost ? "UP" : "DOWN");
                            HttpPost post = new HttpPost(url);
                            post.setHeader("Content-Type", contentType);
                            String json = (new Gson()).toJson(dto);
                            StringEntity entity = new StringEntity(json, "UTF-8");
                            post.setEntity(entity);
                            this.ddsClient.execute(post);
                        } catch (Throwable var12) {
                        }
                    }
                } catch (Throwable var13) {
                }

            }, 1L, 20L, TimeUnit.SECONDS);
        }
    }

    public void unregister(RegisterConfig config) {
        String serviceId = this.generateServiceId(config);
        this.unregister(serviceId);
    }

    public void unregister(String serviceId) {
        Set<DiscoveryClient> clients = this.clientMap.get(serviceId);
        if (null != clients) {
            for (DiscoveryClient client : clients) {
                synchronized (MTX) {
                    client.cancelTask();
                }
            }
        }

        this.clientMap.remove(serviceId);
    }

    public Map<String, NodeInfos> getServiceInfo(String serviceId) {
        Set<DiscoveryClient> clients = this.clientMap.get(serviceId);
        if (null != clients && clients.size() > 0) {
            Map<String, NodeInfos> response = new HashMap();

            for (DiscoveryClient client : clients) {
                if (null != client) {
                    RegisterConfig config = this.configMap.get(serviceId);
                    Query query =
                            Query.newBuilder().setAppName(config.getAppName())
                                 .setVersion(config.getVersion()).build();
                    NodeInfos nodeInfos = client.getNodeInfos(query);
                    response.put(serviceId, nodeInfos);
                }
            }

            return response;
        } else {
            return Collections.emptyMap();
        }
    }

    private String registerClient(RegisterConfig config) throws Exception {
        String serviceId = this.generateServiceId(config);
        Boolean hasRegistered = false;
        if (!Strings.isNullOrEmpty(config.getNodePort()) &&
            !Strings.isNullOrEmpty(config.getNodeName())) {
            String address = config.getNodeName() + ":" + config.getNodePort();
            String clientId = serviceId + ":" + address;
            PDConfig pdConfig = PDConfig.of(config.getGrpcAddress());
            pdConfig.setAuthority(this.service, this.token);
            DiscoveryClient client = DiscoveryClientImpl.newBuilder().setPdConfig(pdConfig)
                                                        .setCenterAddress(config.getGrpcAddress())
                                                        .setAddress(address)
                                                        .setAppName(config.getAppName())
                                                        .setDelay(config.getInterval())
                                                        .setVersion(config.getVersion())
                                                        .setId(clientId)
                                                        .setLabels(config.getLabelMap())
                                                        .setRegisterConsumer(config.getConsumer())
                                                        .build();
            client.scheduleTask();
            this.clientMap.computeIfAbsent(serviceId, (v) -> new HashSet()).add(client);
            hasRegistered = true;
        }

        if (!Strings.isNullOrEmpty(config.getPodIp()) &&
            !Strings.isNullOrEmpty(config.getPodPort())) {
            String address = config.getPodIp() + ":" + config.getPodPort();
            String clientId = serviceId + ":" + address;
            PDConfig pdConfig = PDConfig.of(config.getGrpcAddress());
            pdConfig.setAuthority(this.service, this.token);
            DiscoveryClient client = DiscoveryClientImpl.newBuilder().setPdConfig(pdConfig)
                                                        .setCenterAddress(config.getGrpcAddress())
                                                        .setAddress(address)
                                                        .setAppName(config.getAppName())
                                                        .setDelay(config.getInterval())
                                                        .setVersion(config.getVersion())
                                                        .setId(clientId)
                                                        .setLabels(config.getLabelMap())
                                                        .setRegisterConsumer(config.getConsumer())
                                                        .build();
            client.scheduleTask();
            this.clientMap.computeIfAbsent(serviceId, (v) -> new HashSet()).add(client);
            hasRegistered = true;
        }

        if (null != config.getUrls()) {
            for (String address : config.getUrls()) {
                String clientId = serviceId + ":" + address;
                PDConfig pdConfig = PDConfig.of(config.getGrpcAddress());
                pdConfig.setAuthority(this.service, this.token);
                DiscoveryClient client = DiscoveryClientImpl.newBuilder().setPdConfig(pdConfig)
                                                            .setCenterAddress(
                                                                    config.getGrpcAddress())
                                                            .setAddress(address)
                                                            .setAppName(config.getAppName())
                                                            .setDelay(config.getInterval())
                                                            .setVersion(config.getVersion())
                                                            .setId(clientId)
                                                            .setLabels(config.getLabelMap())
                                                            .setRegisterConsumer(
                                                                    config.getConsumer()).build();
                client.scheduleTask();
                this.clientMap.computeIfAbsent(serviceId, (v) -> new HashSet()).add(client);
                hasRegistered = true;
            }
        }

        if (hasRegistered) {
            this.configMap.put(serviceId, config);
        }

        return serviceId;
    }

    public void unregisterAll() {
        for (Set<DiscoveryClient> set : this.clientMap.values()) {
            for (DiscoveryClient client : set) {
                synchronized (MTX) {
                    client.cancelTask();
                }
            }
        }

        this.configMap.clear();
        this.clientMap.clear();
    }

    public void close() {
        if (null != this.pool) {
            this.pool.shutdown();
        }

    }
}
