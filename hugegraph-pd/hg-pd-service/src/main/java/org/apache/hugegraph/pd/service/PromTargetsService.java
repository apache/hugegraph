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
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.hugegraph.pd.RegistryService;
import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.model.PromTargetsModel;
import org.apache.hugegraph.pd.rest.MemberAPI;
import org.apache.hugegraph.pd.util.HgMapCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class PromTargetsService {

    private final PromTargetsModel pdModel = PromTargetsModel.of()
                                                             .addLabel("__app_name", "pd")
                                                             .setScheme("http")
                                                             .setMetricsPath(
                                                                     "/actuator/prometheus");
    private final PromTargetsModel storeModel = PromTargetsModel.of()
                                                                .addLabel("__app_name", "store")
                                                                .setScheme("http")
                                                                .setMetricsPath(
                                                                        "/actuator/prometheus");
    private final HgMapCache<String, Set<String>> targetsCache =
            HgMapCache.expiredOf(24 * 60 * 60 * 1000);// expired after 24H.
    @Autowired
    private PDConfig pdConfig;
    @Autowired
    private PDService pdService;
    private RegistryService register;

    private RegistryService getRegister() {
        if (this.register == null) {
            this.register = new RegistryService(this.pdConfig);
        }
        return this.register;
    }

    public List<PromTargetsModel> getAllTargets() {
        List<PromTargetsModel> res = new LinkedList<>();
        List<PromTargetsModel> buf =
                this.toModels(this.getRegister().getNodes(Query.newBuilder().build()));

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
    public List<PromTargetsModel> getTargets(String appName) {
        HgAssert.isArgumentNotNull(appName, "appName");
        return switch (appName) {
            case "pd" -> Collections.singletonList(this.getPdTargets());
            case "store" -> Collections.singletonList(this.getStoreTargets());
            default -> this.toModels(this.getRegister()
                                     .getNodes(Query.newBuilder().setAppName(appName).build()));
        };
    }

    private PromTargetsModel getPdTargets() {
        return setTargets(pdModel, () -> this.mergeCache("pd", getPdAddresses()));
    }

    private PromTargetsModel getStoreTargets() {
        return setTargets(storeModel, () -> this.mergeCache("store", getStoreAddresses()));
    }

    private PromTargetsModel setTargets(PromTargetsModel model, Supplier<Set<String>> supplier) {
        return model.setTargets(supplier.get())
                    .setClusterId(String.valueOf(pdConfig.getClusterId()));
    }

    /* to prevent the failure of connection between pd and store or pd and pd.*/
    //TODO: To add a schedule task to refresh targets, not to retrieve in every time.
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

    private List<PromTargetsModel> toModels(NodeInfos info) {
        if (info == null) {
            return null;
        }

        List<NodeInfo> nodes = info.getInfoList();
        if (nodes == null || nodes.isEmpty()) {
            return null;
        }

        List<PromTargetsModel> res =
                nodes.stream().map(e -> {
                         Map<String, String> labels = e.getLabelsMap();

                         String target = labels.get("target");
                         if (HgAssert.isInvalid(target)) {
                             return null;
                         }

                         PromTargetsModel model = PromTargetsModel.of();
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
        MemberAPI.CallStreamObserverWrap<Pdpb.GetMembersResponse> response =
                new MemberAPI.CallStreamObserverWrap<>();
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
                String buf = this.getRestAddress(e);
                if (buf != null) {
                    res.add(buf);
                }
            });
        }

        return res;
    }

    //TODO: optimized store registry data, to add host:port of REST server.
    private String getRestAddress(Metapb.Store store) {
        String address = store.getAddress();
        if (address == null || address.isEmpty()) {
            return null;
        }
        try {
            Optional<String> port = store.getLabelsList().stream().map(
                    e -> {
                        if ("rest.port".equals(e.getKey())) {
                            return e.getValue();
                        }
                        return null;
                    }).filter(e -> e != null).findFirst();

            if (port.isPresent()) {
                address = address.substring(0, address.indexOf(':') + 1);
                address = address + port.get();

            }
        } catch (Throwable t) {
            log.error("Failed to extract the REST address of store, cause by:", t);
        }
        return address;

    }
}
