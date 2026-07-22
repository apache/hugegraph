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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.RegistryService;
import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.util.StoreRestAddressUtil;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.model.SDConfig;
import org.apache.hugegraph.pd.rest.MemberAPI.CallStreamObserverWrap;
import org.apache.hugegraph.pd.util.HgMapCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class SDConfigService {

    private static String defaultPath = "/actuator/prometheus";
    private final SDConfig pdModel = SDConfig.of()
                                             .addLabel("__app_name", "pd")
                                             .setScheme("http")
                                             .setMetricsPath("/actuator/prometheus");
    private final SDConfig storeModel = SDConfig.of()
                                                .addLabel("__app_name", "store")
                                                .setScheme("http")
                                                .setMetricsPath("/actuator/prometheus");
    @Autowired
    private PDConfig pdConfig;
    @Autowired
    private PDService pdService;
    private volatile RegistryService register;
    private HgMapCache<String, Set<String>> targetsCache =
            HgMapCache.expiredOf(24 * 60 * 60 * 1000);

    private RegistryService getRegister() {
        RegistryService local = this.register;
        if (local == null) {
            synchronized (this) {
                if (this.register == null) {
                    this.register = new RegistryService(this.pdConfig);
                }
                local = this.register;
            }
        }
        return local;
    }

    public List<SDConfig> getAllTargets() {
        List<SDConfig> res = new LinkedList<>();
        List<SDConfig> buf = this.toModels(this.getRegister().getNodes(Query.newBuilder().build()));
        if (buf != null) {
            res.addAll(buf);
        }
        res.add(getPdTargets());
        res.add(getStoreTargets());
        return res;
    }

    /**
     * @param appName
     * @return null if it's not existing
     */
    public List<SDConfig> getTargets(String appName) {
        HgAssert.isArgumentNotNull(appName, "appName");
        switch (appName) {
            case "pd":
                return Collections.singletonList(this.getPdTargets());
            case "store":
                return Collections.singletonList(this.getStoreTargets());
            default:
                return this.toModels(
                        this.getRegister()
                            .getNodes(Query.newBuilder().setAppName(appName).build()));
        }
    }

    private SDConfig getPdTargets() {
        return setTargets(pdModel, () -> this.mergeCache("pd", getPdAddresses()));
    }

    private SDConfig getStoreTargets() {
        return setTargets(storeModel, () -> this.mergeCache("store", getStoreAddresses()));
    }

    private SDConfig setTargets(SDConfig model, Supplier<Set<String>> supplier) {
        return model.setTargets(supplier.get())
                    .setClusterId(String.valueOf(pdConfig.getClusterId()));
    }

    private Set<String> mergeCache(String key, Set<String> set) {
        Set<String> buf = this.targetsCache.get(key);

        if (buf == null) {
            buf = new HashSet<>();
            this.targetsCache.put(key, buf);
        }

        if (set != null) {
            buf.addAll(set);
        }

        return buf;
    }

    private List<SDConfig> toModels(NodeInfos info) {
        if (info == null) {
            return null;
        }
        List<NodeInfo> nodes = info.getInfoList();
        if (nodes == null || nodes.isEmpty()) {
            return null;
        }
        List<SDConfig> res =
                nodes.stream().map(e -> {
                         Map<String, String> labels = e.getLabelsMap();
                         String target = labels.get("target");
                         if (HgAssert.isInvalid(target)) {
                             return null;
                         }
                         SDConfig model = SDConfig.of();
                         model.addTarget(target);
                         model.addLabel("__app_name", e.getAppName());
                         labels.forEach((k, v) -> {
                             k = k.trim();
                             switch (k) {
                                 case "metrics":
                                     model.setMetricsPath(v.trim());
                                     break;
                                 case "scheme":
                                     model.setScheme(v.trim());
                                     break;
                                 default:
                                     if (k.startsWith("__")) {
                                         model.addLabel(k, v);
                                     }
                             }
                         });
                         return model;
                     })
                     .filter(e -> e != null)
                     .collect(Collectors.toList());

        if (res.isEmpty()) {
            return null;
        }
        return res;
    }

    private Set<String> getPdAddresses() {
        CallStreamObserverWrap<Pdpb.GetMembersResponse> response = new CallStreamObserverWrap<>();
        pdService.getMembers(Pdpb.GetMembersRequest.newBuilder().build(), response);
        List<Metapb.Member> members = null;
        try {
            members = response.get().get(0).getMembersList();
        } catch (Throwable e) {
            log.error("Failed to get all pd members.", e);
        }
        Set<String> res = new HashSet<>();
        if (members != null) {
            members.stream().forEach(e -> res.add(e.getRestUrl()));
        }
        return res;
    }

    private Set<String> getStoreAddresses() {
        Set<String> res = new HashSet<>();
        List<Metapb.Store> stores = null;
        try {
            stores = pdService.getStoreNodeService().getStores();
        } catch (PDException e) {
            log.error("Failed to get all stores.", e);
        }
        if (stores != null) {
            stores.stream().forEach(e -> {
                String buf = StoreRestAddressUtil.getRestAddress(e);
                if (buf != null) {
                    res.add(buf);
                }
            });
        }
        return res;
    }

    public List<SDConfig> getConfigs(String appName, String path) {
        HgAssert.isArgumentNotNull(appName, "appName");
        SDConfig config;
        switch (appName) {
            case "pd":
                config = getPdConfig(appName, path);
                config.setTargets(mergeCache(appName, getPdAddresses()));
                return Collections.singletonList(config);
            case "store":
                config = getPdConfig(appName, path);
                config.setTargets(mergeCache(appName, getStoreAddresses()));
                return Collections.singletonList(config);
            default:
                return toModels(
                        getRegister().getNodes(Query.newBuilder().setAppName(appName).build()));
        }
    }

    public SDConfig getPdConfig(String appName, String path) {
        SDConfig config = SDConfig.of().addLabel("__app_name", appName).setScheme("http");
        if (StringUtils.isEmpty(path)) {
            config.setMetricsPath(defaultPath);
        } else {
            config.setMetricsPath(path);
        }
        return config;
    }
}
