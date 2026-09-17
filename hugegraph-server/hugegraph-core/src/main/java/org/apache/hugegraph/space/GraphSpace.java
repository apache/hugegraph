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

package org.apache.hugegraph.space;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.util.E;

public class GraphSpace {

    public static final String DEFAULT_GRAPH_SPACE_SERVICE_NAME = "DEFAULT";
    public static final String DEFAULT_NICKNAME = "默认图空间";
    public static final String DEFAULT_GRAPH_SPACE_DESCRIPTION =
            "The system default graph space";
    public static final String DEFAULT_CREATOR_NAME = "anonymous";

    public static final int DEFAULT_CPU_LIMIT = 4;
    public static final int DEFAULT_MEMORY_LIMIT = 8;
    public static final int DEFAULT_STORAGE_LIMIT = 100;

    public static final int DEFAULT_MAX_GRAPH_NUMBER = 100;
    public static final int DEFAULT_MAX_ROLE_NUMBER = 100;
    private final String creator;
    public int storageLimit; // GB
    public String oltpNamespace;
    private String name;
    private String nickname;
    private String description;
    private int cpuLimit;
    private int memoryLimit; // GB
    private int computeCpuLimit;
    private int computeMemoryLimit; // GB
    private String olapNamespace;
    private String storageNamespace;
    private int maxGraphNumber;
    private int maxRoleNumber;
    private Boolean auth;
    private Map<String, Object> configs;
    private int cpuUsed;
    private int memoryUsed; // GB
    private int storageUsed; // GB
    private int graphNumberUsed;
    private int roleNumberUsed;
    private String operatorImagePath = ""; // path of compute operator image
    private String internalAlgorithmImageUrl = "";
    private Date createTime;
    private Date updateTime;

    public GraphSpace() {
        this.creator = DEFAULT_CREATOR_NAME;
    }

    public GraphSpace(String name) {
        E.checkArgument(name != null && !StringUtils.isEmpty(name),
                        "The name of graph space can't be null or empty");
        this.name = name;
        this.nickname = DEFAULT_NICKNAME;

        this.maxGraphNumber = DEFAULT_MAX_GRAPH_NUMBER;
        this.maxRoleNumber = DEFAULT_MAX_ROLE_NUMBER;

        this.cpuLimit = DEFAULT_CPU_LIMIT;
        this.memoryLimit = DEFAULT_MEMORY_LIMIT;
        this.storageLimit = DEFAULT_STORAGE_LIMIT;

        this.computeCpuLimit = DEFAULT_CPU_LIMIT;
        this.computeMemoryLimit = DEFAULT_MEMORY_LIMIT;

        this.auth = false;
        this.creator = DEFAULT_CREATOR_NAME;
        this.configs = new HashMap<>();
    }

    public GraphSpace(String name, String nickname, String description,
                      int cpuLimit,
                      int memoryLimit, int storageLimit, int maxGraphNumber,
                      int maxRoleNumber, boolean auth, String creator,
                      Map<String, Object> config) {
        E.checkArgument(name != null && !StringUtils.isEmpty(name),
                        "The name of graph space can't be null or empty");
        E.checkArgument(cpuLimit > 0, "The cpu limit must > 0");
        E.checkArgument(memoryLimit > 0, "The memory limit must > 0");
        E.checkArgument(storageLimit > 0, "The storage limit must > 0");
        E.checkArgument(maxGraphNumber > 0, "The max graph number must > 0");
        this.name = name;
        this.nickname = nickname;
        this.description = description;
        this.cpuLimit = cpuLimit;
        this.memoryLimit = memoryLimit;
        this.storageLimit = storageLimit;
        this.maxGraphNumber = maxGraphNumber;
        this.maxRoleNumber = maxRoleNumber;

        this.auth = auth;
        if (config == null) {
            this.configs = new HashMap<>();
        } else {
            this.configs = config;
        }

        this.createTime = new Date();
        this.updateTime = this.createTime;
        this.creator = creator;
    }

    public GraphSpace(String name, String nickname, String description,
                      int cpuLimit,
                      int memoryLimit, int storageLimit, int maxGraphNumber,
                      int maxRoleNumber, String oltpNamespace,
                      String olapNamespace, String storageNamespace,
                      int cpuUsed, int memoryUsed, int storageUsed,
                      int graphNumberUsed, int roleNumberUsed,
                      boolean auth, String creator, Map<String, Object> config) {
        E.checkArgument(name != null && !StringUtils.isEmpty(name),
                        "The name of graph space can't be null or empty");
        E.checkArgument(cpuLimit > 0, "The cpu limit must > 0");
        E.checkArgument(memoryLimit > 0, "The memory limit must > 0");
        E.checkArgument(storageLimit > 0, "The storage limit must > 0");
        E.checkArgument(maxGraphNumber > 0, "The max graph number must > 0");
        this.name = name;
        this.nickname = nickname;
        this.description = description;

        this.cpuLimit = cpuLimit;
        this.memoryLimit = memoryLimit;
        this.storageLimit = storageLimit;

        this.maxGraphNumber = maxGraphNumber;
        this.maxRoleNumber = maxRoleNumber;

        this.oltpNamespace = oltpNamespace;
        this.olapNamespace = olapNamespace;
        this.storageNamespace = storageNamespace;

        this.cpuUsed = cpuUsed;
        this.memoryUsed = memoryUsed;
        this.storageUsed = storageUsed;

        this.graphNumberUsed = graphNumberUsed;
        this.roleNumberUsed = roleNumberUsed;

        this.auth = auth;
        this.creator = creator;

        this.configs = new HashMap<>();
        if (config != null) {
            this.configs = config;
        }
    }

    public String name() {
        return this.name;
    }

    public void name(String name) {
        this.name = name;
    }

    public String nickname() {
        return this.nickname;
    }

    public void nickname(String nickname) {
        this.nickname = nickname;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    public int cpuLimit() {
        return this.cpuLimit;
    }

    public void cpuLimit(int cpuLimit) {
        E.checkArgument(cpuLimit > 0,
                        "The cpu limit must be > 0, but got: %s", cpuLimit);
        this.cpuLimit = cpuLimit;
    }

    public int memoryLimit() {
        return this.memoryLimit;
    }

    public void memoryLimit(int memoryLimit) {
        E.checkArgument(memoryLimit > 0,
                        "The memory limit must be > 0, but got: %s",
                        memoryLimit);
        this.memoryLimit = memoryLimit;
    }

    public int storageLimit() {
        return this.storageLimit;
    }

    public void storageLimit(int storageLimit) {
        E.checkArgument(storageLimit > 0,
                        "The storage limit must be > 0, but got: %s",
                        storageLimit);
        this.storageLimit = storageLimit;
    }

    public void setStorageUsed(int storageUsed) {
        this.storageUsed = storageUsed;
    }

    public int computeCpuLimit() {
        return this.computeCpuLimit;
    }

    public void computeCpuLimit(int computeCpuLimit) {
        E.checkArgument(computeCpuLimit >= 0,
                        "The compute cpu limit must be >= 0, but got: %s", computeCpuLimit);
        this.computeCpuLimit = computeCpuLimit;
    }

    public int computeMemoryLimit() {
        return this.computeMemoryLimit;
    }

    public void computeMemoryLimit(int computeMemoryLimit) {
        E.checkArgument(computeMemoryLimit >= 0,
                        "The compute memory limit must be >= 0, but got: %s",
                        computeMemoryLimit);
        this.computeMemoryLimit = computeMemoryLimit;
    }

    public String oltpNamespace() {
        return this.oltpNamespace;
    }

    public void oltpNamespace(String oltpNamespace) {
        this.oltpNamespace = oltpNamespace;
    }

    public String olapNamespace() {
        return this.olapNamespace;
    }

    public void olapNamespace(String olapNamespace) {
        this.olapNamespace = olapNamespace;
    }

    public String storageNamespace() {
        return this.storageNamespace;
    }

    public void storageNamespace(String storageNamespace) {
        this.storageNamespace = storageNamespace;
    }

    public int maxGraphNumber() {
        return this.maxGraphNumber;
    }

    public void maxGraphNumber(int maxGraphNumber) {
        this.maxGraphNumber = maxGraphNumber;
    }

    public int maxRoleNumber() {
        return this.maxRoleNumber;
    }

    public void maxRoleNumber(int maxRoleNumber) {
        this.maxRoleNumber = maxRoleNumber;
    }

    public int graphNumberUsed() {
        return this.graphNumberUsed;
    }

    public void graphNumberUsed(int graphNumberUsed) {
        this.graphNumberUsed = graphNumberUsed;
    }

    public int roleNumberUsed() {
        return this.roleNumberUsed;
    }

    public void roleNumberUsed(int roleNumberUsed) {
        this.roleNumberUsed = roleNumberUsed;
    }

    public boolean auth() {
        return this.auth;
    }

    public void auth(boolean auth) {
        this.auth = auth;
    }

    public Map<String, Object> configs() {
        return this.configs;
    }

    public void configs(Map<String, Object> configs) {
        this.configs.putAll(configs);
    }

    public void operatorImagePath(String path) {
        this.operatorImagePath = path;
    }

    public String operatorImagePath() {
        return this.operatorImagePath;
    }

    public void internalAlgorithmImageUrl(String url) {
        if (StringUtils.isNotBlank(url)) {
            this.internalAlgorithmImageUrl = url;
        }
    }

    public String internalAlgorithmImageUrl() {
        return this.internalAlgorithmImageUrl;
    }

    public Date createTime() {
        return this.createTime;
    }

    public Date updateTime() {
        return this.updateTime;
    }

    public String creator() {
        return this.creator;
    }

    public void updateTime(Date update) {
        this.updateTime = update;
    }

    public void createTime(Date create) {
        this.createTime = create;
    }

    public void refreshUpdate() {
        this.updateTime = new Date();
    }

    public Map<String, Object> info() {
        Map<String, Object> infos = new LinkedHashMap<>();
        infos.put("name", this.name);
        infos.put("nickname", this.nickname);
        infos.put("description", this.description);

        infos.put("cpu_limit", this.cpuLimit);
        infos.put("memory_limit", this.memoryLimit);
        infos.put("storage_limit", this.storageLimit);

        infos.put("compute_cpu_limit", this.computeCpuLimit);
        infos.put("compute_memory_limit", this.computeMemoryLimit);

        infos.put("oltp_namespace", this.oltpNamespace);
        infos.put("olap_namespace", this.olapNamespace);
        infos.put("storage_namespace", this.storageNamespace);

        infos.put("max_graph_number", this.maxGraphNumber);
        infos.put("max_role_number", this.maxRoleNumber);

        infos.putAll(this.configs);
        // sources used info is not automatically updated, it could be
        // updated by pdClient of GraphManager
        infos.put("cpu_used", this.cpuUsed);
        infos.put("memory_used", this.memoryUsed);
        infos.put("storage_used", this.storageUsed);
        // Locale.ROOT: the default locale may format with a decimal comma
        // ("0,33" on pl_PL, de_DE, fr_FR, ...), which parseFloat() rejects
        float storageUserPercent = Float.parseFloat(
                String.format(Locale.ROOT, "%.2f", (float) this.storageUsed /
                                                   ((float) this.storageLimit * 1.0)));
        infos.put("storage_percent", storageUserPercent);
        infos.put("graph_number_used", this.graphNumberUsed);
        infos.put("role_number_used", this.roleNumberUsed);

        infos.put("auth", this.auth);

        infos.put("operator_image_path", this.operatorImagePath);
        infos.put("internal_algorithm_image_url", this.internalAlgorithmImageUrl);

        infos.put("create_time", this.createTime);
        infos.put("update_time", this.updateTime);
        infos.put("creator", this.creator);
        return infos;
    }

    private synchronized void incrCpuUsed(int acquiredCount) {
        if (acquiredCount < 0) {
            throw new HugeException("cannot increase cpu used since acquired count is negative");
        }
        this.cpuUsed += acquiredCount;
    }

    private synchronized void decrCpuUsed(int releasedCount) {
        if (releasedCount < 0) {
            throw new HugeException("cannot decrease cpu used since released count is negative");
        }
        if (cpuUsed < releasedCount) {
            cpuUsed = 0;
        } else {
            this.cpuUsed -= releasedCount;
        }
    }

    private synchronized void incrMemoryUsed(int acquiredCount) {
        if (acquiredCount < 0) {
            throw new HugeException("cannot increase memory used since acquired count is negative");
        }
        this.memoryUsed += acquiredCount;
    }

    private synchronized void decrMemoryUsed(int releasedCount) {
        if (releasedCount < 0) {
            throw new HugeException("cannot decrease memory used since released count is negative");
        }
        if (memoryUsed < releasedCount) {
            this.memoryUsed = 0;
        } else {
            this.memoryUsed -= releasedCount;
        }
    }

    /**
     * Only limit the resource usage for oltp service under k8s
     *
     * @param service
     * @return
     */
    public boolean tryOfferResourceFor(Service service) {
        if (!service.k8s()) {
            return true;
        }
        int count = service.count();
        int leftCpu = this.cpuLimit - this.cpuUsed;
        int leftMemory = this.memoryLimit - this.memoryUsed;
        int acquiredCpu = service.cpuLimit() * count;
        int acquiredMemory = service.memoryLimit() * count;
        if (acquiredCpu > leftCpu ||
            acquiredMemory > leftMemory) {
            return false;
        }
        this.incrCpuUsed(acquiredCpu);
        this.incrMemoryUsed(acquiredMemory);
        return true;
    }

    public void recycleResourceFor(Service service) {
        int count = service.count();
        this.decrCpuUsed(service.cpuLimit() * count);
        this.decrMemoryUsed(service.memoryLimit() * count);
    }

    public boolean tryOfferGraph() {
        return this.tryOfferGraph(1);
    }

    public boolean tryOfferGraph(int count) {
        if (this.graphNumberUsed + count > this.maxGraphNumber) {
            return false;
        }
        this.graphNumberUsed += count;
        return true;
    }

    public void recycleGraph() {
        this.recycleGraph(1);
    }

    public void recycleGraph(int count) {
        this.graphNumberUsed -= count;
    }

    public boolean tryOfferRole() {
        return this.tryOfferRole(1);
    }

    public boolean tryOfferRole(int count) {
        if (this.roleNumberUsed + count > this.maxRoleNumber) {
            return false;
        }
        this.roleNumberUsed += count;
        return true;
    }

    public void recycleRole() {
        this.recycleRole(1);
    }

    public void recycleRole(int count) {
        this.roleNumberUsed -= count;
    }
}
