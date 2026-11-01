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

package org.apache.hugegraph.meta.managers;

import static org.apache.hugegraph.meta.MetaManager.META_PATH_ADD;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_DELIMITER;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_EVENT;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_GRAPHSPACE;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_HUGEGRAPH;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_JOIN;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_REMOVE;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_SERVICE;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_SERVICE_CONF;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_UPDATE;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.logging.log4j.util.Strings;

public class ServiceMetaManager extends AbstractMetaManager {

    public ServiceMetaManager(MetaDriver metaDriver, String cluster) {
        super(metaDriver, cluster);
    }

    private static String serviceName(String graphSpace, String name) {
        return String.join(META_PATH_JOIN, graphSpace, name);
    }

    public Map<String, Service> serviceConfigs(String graphSpace) {
        Map<String, Service> serviceMap = new HashMap<>();
        String prefix = this.serviceConfPrefix(graphSpace);
        Map<String, String> keyValues = this.metaDriver.scanWithPrefix(prefix);
        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            String key = entry.getKey();
            serviceMap.put(key.substring(prefix.length()),
                           JsonUtil.fromJson(entry.getValue(), Service.class));
        }
        return serviceMap;
    }

    public String getServiceRawConfig(String graphSpace, String service) {
        return this.metaDriver.get(this.serviceConfKey(graphSpace, service));
    }

    public Service getServiceConfig(String graphSpace, String service) {
        String s = this.getServiceRawConfig(graphSpace, service);
        return this.parseServiceRawConfig(s);
    }

    public Service parseServiceRawConfig(String serviceRawConf) {
        return JsonUtil.fromJson(serviceRawConf, Service.class);
    }

    public void notifyServiceAdd(String graphSpace, String name) {
        this.metaDriver.put(this.serviceAddKey(),
                            serviceName(graphSpace, name));
    }

    public void notifyServiceRemove(String graphSpace, String name) {
        this.metaDriver.put(this.serviceRemoveKey(),
                            serviceName(graphSpace, name));
    }

    public void notifyServiceUpdate(String graphSpace, String name) {
        this.metaDriver.put(this.serviceUpdateKey(),
                            serviceName(graphSpace, name));
    }

    public Service service(String graphSpace, String name) {
        String service = this.metaDriver.get(this.serviceConfKey(graphSpace,
                                                                 name));
        if (StringUtils.isEmpty(service)) {
            return null;
        }
        return JsonUtil.fromJson(service, Service.class);
    }

    public void addServiceConfig(String graphSpace, Service service) {
        this.metaDriver.put(this.serviceConfKey(graphSpace, service.name()),
                            JsonUtil.toJson(service));
    }

    public void removeServiceConfig(String graphSpace, String service) {
        this.metaDriver.delete(this.serviceConfKey(graphSpace, service));
    }

    public void updateServiceConfig(String graphSpace, Service service) {
        this.addServiceConfig(graphSpace, service);
    }

    public <T> void listenServiceAdd(Consumer<T> consumer) {
        this.listen(this.serviceAddKey(), consumer);
    }

    public <T> void listenServiceRemove(Consumer<T> consumer) {
        this.listen(this.serviceRemoveKey(), consumer);
    }

    public <T> void listenServiceUpdate(Consumer<T> consumer) {
        this.listen(this.serviceUpdateKey(), consumer);
    }

    private String serviceConfPrefix(String graphSpace) {
        return this.serviceConfKey(graphSpace, Strings.EMPTY);
    }

    private String serviceAddKey() {
        // HUGEGRAPH/{cluster}/EVENT/SERVICE/ADD
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_EVENT,
                           META_PATH_SERVICE,
                           META_PATH_ADD);
    }

    private String serviceRemoveKey() {
        // HUGEGRAPH/{cluster}/EVENT/SERVICE/REMOVE
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_EVENT,
                           META_PATH_SERVICE,
                           META_PATH_REMOVE);
    }

    private String serviceUpdateKey() {
        // HUGEGRAPH/{cluster}/EVENT/SERVICE/UPDATE
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_EVENT,
                           META_PATH_SERVICE,
                           META_PATH_UPDATE);
    }

    private String serviceConfKey(String graphSpace, String name) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/SERVICE_CONF
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_SERVICE_CONF,
                           name);
    }
}
