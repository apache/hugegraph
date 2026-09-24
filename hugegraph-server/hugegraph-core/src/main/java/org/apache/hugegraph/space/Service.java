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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.util.E;

public class Service {

    public static final int DEFAULT_COUNT = 1;
    public static final String DEFAULT_ROUTE_TYPE = "NodePort";
    public static final int DEFAULT_PORT = 0;

    public static final int DEFAULT_CPU_LIMIT = 4;
    public static final int DEFAULT_MEMORY_LIMIT = 8;
    public static final int DEFAULT_STORAGE_LIMIT = 100;

    private final String creator;
    private final String name;
    private ServiceType type;
    private DeploymentType deploymentType;
    private String description;
    private Status status;
    private int count;
    private int running;
    private int cpuLimit;
    private int memoryLimit; // GB
    private int storageLimit; // GB
    private String routeType;
    private int port;
    private Set<String> urls = new HashSet<>();
    private Set<String> serverDdsUrls = new HashSet<>();
    private Set<String> serverNodePortUrls = new HashSet<>();
    private String serviceId;
    private String pdServiceId;
    private Date createTime;
    private Date updateTime;

    public Service(String name, String creator, ServiceType type,
                   DeploymentType deploymentType) {
        E.checkArgument(name != null && !StringUtils.isEmpty(name),
                        "The name of service can't be null or empty");
        E.checkArgumentNotNull(type, "The type of service can't be null");
        E.checkArgumentNotNull(deploymentType,
                               "The deployment type of service can't be null");
        this.name = name;
        this.type = type;
        this.deploymentType = deploymentType;
        this.status = Status.UNKNOWN;
        this.count = DEFAULT_COUNT;
        this.running = 0;
        this.routeType = DEFAULT_ROUTE_TYPE;
        this.port = DEFAULT_PORT;
        this.cpuLimit = DEFAULT_CPU_LIMIT;
        this.memoryLimit = DEFAULT_MEMORY_LIMIT;
        this.storageLimit = DEFAULT_STORAGE_LIMIT;

        this.creator = creator;
        this.createTime = new Date();
        this.updateTime = this.createTime;
    }

    public Service(String name, String creator, String description, ServiceType type,
                   DeploymentType deploymentType, int count, int running,
                   int cpuLimit, int memoryLimit, int storageLimit,
                   String routeType, int port, Set<String> urls) {
        E.checkArgument(name != null && !StringUtils.isEmpty(name),
                        "The name of service can't be null or empty");
        E.checkArgumentNotNull(type, "The type of service can't be null");
        this.name = name;
        this.description = description;
        this.type = type;
        this.status = Status.UNKNOWN;
        this.deploymentType = deploymentType;
        this.count = count;
        this.running = running;
        this.cpuLimit = cpuLimit;
        this.memoryLimit = memoryLimit;
        this.storageLimit = storageLimit;
        this.routeType = routeType;
        this.port = port;
        this.urls = urls;

        this.creator = creator;
        this.createTime = new Date();
        this.updateTime = this.createTime;
    }

    public String name() {
        return this.name;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    public ServiceType type() {
        return this.type;
    }

    public void type(ServiceType type) {
        this.type = type;
    }

    public DeploymentType deploymentType() {
        return this.deploymentType;
    }

    public void deploymentType(DeploymentType deploymentType) {
        this.deploymentType = deploymentType;
    }

    public Status status() {
        return this.status;
    }

    public void status(Status status) {
        this.status = status;
    }

    public int count() {
        return this.count;
    }

    public void count(int count) {
        E.checkArgument(count > 0,
                        "The service count must be > 0, but got: %s", count);
        this.count = count;
    }

    public int running() {
        return this.running;
    }

    public void running(int running) {
        E.checkArgument(running <= this.count,
                        "The running count must be < count %s, but got: %s",
                        this.count, running);
        this.running = running;
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

    public String routeType() {
        return this.routeType;
    }

    public void routeType(String routeType) {
        this.routeType = routeType;
    }

    public int port() {
        return this.port;
    }

    public void port(int port) {
        this.port = port;
    }

    public Set<String> urls() {
        if (this.urls == null) {
            this.urls = new HashSet<>();
        }
        return this.urls;
    }

    public void urls(Set<String> urls) {
        this.urls = urls;
    }

    public Set<String> serverDdsUrls() {
        if (this.serverDdsUrls == null) {
            this.serverDdsUrls = new HashSet<>();
        }
        return this.serverDdsUrls;
    }

    public void serverDdsUrls(Set<String> urls) {
        this.serverDdsUrls = urls;
    }

    public Set<String> serverNodePortUrls() {
        if (this.serverNodePortUrls == null) {
            this.serverNodePortUrls = new HashSet<>();
        }
        return this.serverNodePortUrls;
    }

    public void serverNodePortUrls(Set<String> urls) {
        this.serverNodePortUrls = urls;
    }

    public void url(String url) {
        if (this.urls == null) {
            this.urls = new HashSet<>();
        }
        this.urls.add(url);
    }

    public boolean manual() {
        return DeploymentType.MANUAL.equals(this.deploymentType);
    }

    public boolean k8s() {
        return DeploymentType.K8S.equals(this.deploymentType);
    }

    public String creator() {
        return this.creator;
    }

    public Date createdTime() {
        return this.createTime;
    }

    public Date updateTime() {
        return this.updateTime;
    }

    public void createTime(Date create) {
        this.createTime = create;
    }

    public void updateTime(Date update) {
        this.updateTime = update;
    }

    public void refreshUpdate() {
        this.updateTime = new Date();
    }

    public boolean sameService(Service other) {
        if (other.deploymentType == DeploymentType.K8S ||
            this.deploymentType == DeploymentType.K8S) {
            return true;
        }
        return (this.name.equals(other.name) &&
                this.type.equals(other.type) &&
                this.deploymentType == other.deploymentType &&
                this.urls.equals(other.urls) &&
                this.port == other.port);
    }

    public Map<String, Object> info() {
        Map<String, Object> infos = new LinkedHashMap<>();
        infos.put("name", this.name);
        infos.put("type", this.type);
        infos.put("deployment_type", this.deploymentType);
        infos.put("description", this.description);
        infos.put("status", this.status);
        infos.put("count", this.count);
        infos.put("running", this.running);

        infos.put("cpu_limit", this.cpuLimit);
        infos.put("memory_limit", this.memoryLimit);
        infos.put("storage_limit", this.storageLimit);

        infos.put("route_type", this.routeType);
        infos.put("port", this.port);
        infos.put("urls", this.urls);
        infos.put("server_dds_urls", this.serverDdsUrls);
        infos.put("server_node_port_urls", this.serverNodePortUrls);

        infos.put("service_id", this.serviceId);
        infos.put("pd_service_id", this.pdServiceId);

        infos.put("creator", this.creator);
        infos.put("create_time", this.createTime);
        infos.put("update_time", this.updateTime);

        return infos;
    }

    public String serviceId() {
        return this.serviceId;
    }

    public void serviceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public String pdServiceId() {
        return this.pdServiceId;
    }

    public void pdServiceId(String serviceId) {
        this.pdServiceId = serviceId;
    }

    public enum DeploymentType {
        MANUAL,
        K8S,
    }

    public enum ServiceType {
        OLTP,
        OLAP,
        STORAGE
    }

    public enum Status {
        UNKNOWN,
        STARTING,
        RUNNING,
        STOPPED
    }
}
