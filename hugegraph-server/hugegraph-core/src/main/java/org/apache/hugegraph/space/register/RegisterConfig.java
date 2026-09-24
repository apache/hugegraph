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
package org.apache.hugegraph.space.register;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class RegisterConfig {

    private String grpcAddress;
    private String appName;
    private String nodeName;
    private String nodePort;
    private String podIp;
    private String podPort = "8080";
    private String version = "1.0.0";
    private Map<String, String> labelMap;
    private Set<String> urls;
    private int interval = 15000;
    private String ddsHost;
    private Boolean ddsSlave = false;
    private Consumer consumer;

    public String getGrpcAddress() {
        return this.grpcAddress;
    }

    public RegisterConfig setGrpcAddress(String grpcAddress) {
        this.grpcAddress = grpcAddress;
        return this;
    }

    public String getAppName() {
        return this.appName;
    }

    public RegisterConfig setAppName(String appName) {
        this.appName = appName;
        return this;
    }

    public String getNodeName() {
        return this.nodeName;
    }

    public RegisterConfig setNodeName(String nodeName) {
        this.nodeName = nodeName;
        return this;
    }

    public String getNodePort() {
        return this.nodePort;
    }

    public RegisterConfig setNodePort(String nodePort) {
        this.nodePort = nodePort;
        return this;
    }

    public String getPodIp() {
        return this.podIp;
    }

    public RegisterConfig setPodIp(String podIp) {
        this.podIp = podIp;
        return this;
    }

    public String getPodPort() {
        return this.podPort;
    }

    public RegisterConfig setPodPort(String podPort) {
        this.podPort = podPort;
        return this;
    }

    public String getVersion() {
        return this.version;
    }

    public RegisterConfig setVersion(String version) {
        this.version = version;
        return this;
    }

    public Map<String, String> getLabelMap() {
        return this.labelMap;
    }

    public RegisterConfig setLabelMap(Map<String, String> labelMap) {
        this.labelMap = labelMap;
        return this;
    }

    public Set<String> getUrls() {
        return this.urls;
    }

    public RegisterConfig setUrls(Set<String> urls) {
        this.urls = urls;
        return this;
    }

    public int getInterval() {
        return this.interval;
    }

    public RegisterConfig setInterval(int interval) {
        this.interval = interval;
        return this;
    }

    public String getDdsHost() {
        return this.ddsHost;
    }

    public RegisterConfig setDdsHost(String ddsHost) {
        this.ddsHost = ddsHost;
        return this;
    }

    public Boolean getDdsSlave() {
        return this.ddsSlave;
    }

    public RegisterConfig setDdsSlave(Boolean ddsSlave) {
        this.ddsSlave = ddsSlave;
        return this;
    }

    public Consumer getConsumer() {
        return this.consumer;
    }

    public RegisterConfig setConsumer(Consumer consumer) {
        this.consumer = consumer;
        return this;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof RegisterConfig)) {
            return false;
        } else {
            RegisterConfig other = (RegisterConfig) o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                Object this$grpcAddress = this.getGrpcAddress();
                Object other$grpcAddress = other.getGrpcAddress();
                if (this$grpcAddress == null) {
                    if (other$grpcAddress != null) {
                        return false;
                    }
                } else if (!this$grpcAddress.equals(other$grpcAddress)) {
                    return false;
                }

                Object this$appName = this.getAppName();
                Object other$appName = other.getAppName();
                if (this$appName == null) {
                    if (other$appName != null) {
                        return false;
                    }
                } else if (!this$appName.equals(other$appName)) {
                    return false;
                }

                Object this$nodeName = this.getNodeName();
                Object other$nodeName = other.getNodeName();
                if (this$nodeName == null) {
                    if (other$nodeName != null) {
                        return false;
                    }
                } else if (!this$nodeName.equals(other$nodeName)) {
                    return false;
                }

                Object this$nodePort = this.getNodePort();
                Object other$nodePort = other.getNodePort();
                if (this$nodePort == null) {
                    if (other$nodePort != null) {
                        return false;
                    }
                } else if (!this$nodePort.equals(other$nodePort)) {
                    return false;
                }

                Object this$podIp = this.getPodIp();
                Object other$podIp = other.getPodIp();
                if (this$podIp == null) {
                    if (other$podIp != null) {
                        return false;
                    }
                } else if (!this$podIp.equals(other$podIp)) {
                    return false;
                }

                Object this$podPort = this.getPodPort();
                Object other$podPort = other.getPodPort();
                if (this$podPort == null) {
                    if (other$podPort != null) {
                        return false;
                    }
                } else if (!this$podPort.equals(other$podPort)) {
                    return false;
                }

                Object this$version = this.getVersion();
                Object other$version = other.getVersion();
                if (this$version == null) {
                    if (other$version != null) {
                        return false;
                    }
                } else if (!this$version.equals(other$version)) {
                    return false;
                }

                Object this$labelMap = this.getLabelMap();
                Object other$labelMap = other.getLabelMap();
                if (this$labelMap == null) {
                    if (other$labelMap != null) {
                        return false;
                    }
                } else if (!this$labelMap.equals(other$labelMap)) {
                    return false;
                }

                Object this$urls = this.getUrls();
                Object other$urls = other.getUrls();
                if (this$urls == null) {
                    if (other$urls != null) {
                        return false;
                    }
                } else if (!this$urls.equals(other$urls)) {
                    return false;
                }

                if (this.getInterval() != other.getInterval()) {
                    return false;
                } else {
                    Object this$ddsHost = this.getDdsHost();
                    Object other$ddsHost = other.getDdsHost();
                    if (this$ddsHost == null) {
                        if (other$ddsHost != null) {
                            return false;
                        }
                    } else if (!this$ddsHost.equals(other$ddsHost)) {
                        return false;
                    }

                    Object this$ddsSlave = this.getDdsSlave();
                    Object other$ddsSlave = other.getDdsSlave();
                    if (this$ddsSlave == null) {
                        if (other$ddsSlave != null) {
                            return false;
                        }
                    } else if (!this$ddsSlave.equals(other$ddsSlave)) {
                        return false;
                    }

                    Object this$consumer = this.getConsumer();
                    Object other$consumer = other.getConsumer();
                    if (this$consumer == null) {
                        return other$consumer == null;
                    } else return this$consumer.equals(other$consumer);
                }
            }
        }
    }

    protected boolean canEqual(Object other) {
        return other instanceof RegisterConfig;
    }

    public int hashCode() {
        int PRIME = 59;
        int result = 1;
        Object $grpcAddress = this.getGrpcAddress();
        result = result * 59 + ($grpcAddress == null ? 43 : $grpcAddress.hashCode());
        Object $appName = this.getAppName();
        result = result * 59 + ($appName == null ? 43 : $appName.hashCode());
        Object $nodeName = this.getNodeName();
        result = result * 59 + ($nodeName == null ? 43 : $nodeName.hashCode());
        Object $nodePort = this.getNodePort();
        result = result * 59 + ($nodePort == null ? 43 : $nodePort.hashCode());
        Object $podIp = this.getPodIp();
        result = result * 59 + ($podIp == null ? 43 : $podIp.hashCode());
        Object $podPort = this.getPodPort();
        result = result * 59 + ($podPort == null ? 43 : $podPort.hashCode());
        Object $version = this.getVersion();
        result = result * 59 + ($version == null ? 43 : $version.hashCode());
        Object $labelMap = this.getLabelMap();
        result = result * 59 + ($labelMap == null ? 43 : $labelMap.hashCode());
        Object $urls = this.getUrls();
        result = result * 59 + ($urls == null ? 43 : $urls.hashCode());
        result = result * 59 + this.getInterval();
        Object $ddsHost = this.getDdsHost();
        result = result * 59 + ($ddsHost == null ? 43 : $ddsHost.hashCode());
        Object $ddsSlave = this.getDdsSlave();
        result = result * 59 + ($ddsSlave == null ? 43 : $ddsSlave.hashCode());
        Object $consumer = this.getConsumer();
        result = result * 59 + ($consumer == null ? 43 : $consumer.hashCode());
        return result;
    }

    public String toString() {
        return "RegisterConfig(grpcAddress=" + this.getGrpcAddress() + ", appName=" +
               this.getAppName() + ", nodeName=" + this.getNodeName() + ", nodePort=" +
               this.getNodePort() + ", podIp=" + this.getPodIp() + ", podPort=" +
               this.getPodPort() + ", version=" + this.getVersion() + ", labelMap=" +
               this.getLabelMap() + ", urls=" + this.getUrls() + ", interval=" +
               this.getInterval() + ", ddsHost=" + this.getDdsHost() + ", ddsSlave=" +
               this.getDdsSlave() + ", consumer=" + this.getConsumer() + ")";
    }
}
