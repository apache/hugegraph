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

package org.apache.hugegraph.space.register.dto;

import java.util.Map;

public class EurekaInstanceDTO {

    private String instanceId;
    private String ipAddr;
    private Map<String, Object> port;
    private String hostName;
    private String app;
    private String status;
    private Map<String, String> metadata;
    private Map<String, String> dataCenterInfo;
    private String healthCheckUrl;
    private String secureViaAddress;
    private String vipAddress;
    private Map<String, Object> securePort;
    private String homePageUrl;
    private String statusPageUrl;

    public String getInstanceId() {
        return this.instanceId;
    }

    public EurekaInstanceDTO setInstanceId(String instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    public String getIpAddr() {
        return this.ipAddr;
    }

    public EurekaInstanceDTO setIpAddr(String ipAddr) {
        this.ipAddr = ipAddr;
        return this;
    }

    public Map<String, Object> getPort() {
        return this.port;
    }

    public EurekaInstanceDTO setPort(Map<String, Object> port) {
        this.port = port;
        return this;
    }

    public String getHostName() {
        return this.hostName;
    }

    public EurekaInstanceDTO setHostName(String hostName) {
        this.hostName = hostName;
        return this;
    }

    public String getApp() {
        return this.app;
    }

    public EurekaInstanceDTO setApp(String app) {
        this.app = app;
        return this;
    }

    public String getStatus() {
        return this.status;
    }

    public EurekaInstanceDTO setStatus(String status) {
        this.status = status;
        return this;
    }

    public Map<String, String> getMetadata() {
        return this.metadata;
    }

    public EurekaInstanceDTO setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
        return this;
    }

    public Map<String, String> getDataCenterInfo() {
        return this.dataCenterInfo;
    }

    public EurekaInstanceDTO setDataCenterInfo(Map<String, String> dataCenterInfo) {
        this.dataCenterInfo = dataCenterInfo;
        return this;
    }

    public String getHealthCheckUrl() {
        return this.healthCheckUrl;
    }

    public EurekaInstanceDTO setHealthCheckUrl(String healthCheckUrl) {
        this.healthCheckUrl = healthCheckUrl;
        return this;
    }

    public String getSecureViaAddress() {
        return this.secureViaAddress;
    }

    public EurekaInstanceDTO setSecureViaAddress(String secureViaAddress) {
        this.secureViaAddress = secureViaAddress;
        return this;
    }

    public String getVipAddress() {
        return this.vipAddress;
    }

    public EurekaInstanceDTO setVipAddress(String vipAddress) {
        this.vipAddress = vipAddress;
        return this;
    }

    public Map<String, Object> getSecurePort() {
        return this.securePort;
    }

    public EurekaInstanceDTO setSecurePort(Map<String, Object> securePort) {
        this.securePort = securePort;
        return this;
    }

    public String getHomePageUrl() {
        return this.homePageUrl;
    }

    public EurekaInstanceDTO setHomePageUrl(String homePageUrl) {
        this.homePageUrl = homePageUrl;
        return this;
    }

    public String getStatusPageUrl() {
        return this.statusPageUrl;
    }

    public EurekaInstanceDTO setStatusPageUrl(String statusPageUrl) {
        this.statusPageUrl = statusPageUrl;
        return this;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof EurekaInstanceDTO)) {
            return false;
        } else {
            EurekaInstanceDTO other = (EurekaInstanceDTO) o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                Object this$instanceId = this.getInstanceId();
                Object other$instanceId = other.getInstanceId();
                if (this$instanceId == null) {
                    if (other$instanceId != null) {
                        return false;
                    }
                } else if (!this$instanceId.equals(other$instanceId)) {
                    return false;
                }

                Object this$ipAddr = this.getIpAddr();
                Object other$ipAddr = other.getIpAddr();
                if (this$ipAddr == null) {
                    if (other$ipAddr != null) {
                        return false;
                    }
                } else if (!this$ipAddr.equals(other$ipAddr)) {
                    return false;
                }

                Object this$port = this.getPort();
                Object other$port = other.getPort();
                if (this$port == null) {
                    if (other$port != null) {
                        return false;
                    }
                } else if (!this$port.equals(other$port)) {
                    return false;
                }

                Object this$hostName = this.getHostName();
                Object other$hostName = other.getHostName();
                if (this$hostName == null) {
                    if (other$hostName != null) {
                        return false;
                    }
                } else if (!this$hostName.equals(other$hostName)) {
                    return false;
                }

                Object this$app = this.getApp();
                Object other$app = other.getApp();
                if (this$app == null) {
                    if (other$app != null) {
                        return false;
                    }
                } else if (!this$app.equals(other$app)) {
                    return false;
                }

                Object this$status = this.getStatus();
                Object other$status = other.getStatus();
                if (this$status == null) {
                    if (other$status != null) {
                        return false;
                    }
                } else if (!this$status.equals(other$status)) {
                    return false;
                }

                Object this$metadata = this.getMetadata();
                Object other$metadata = other.getMetadata();
                if (this$metadata == null) {
                    if (other$metadata != null) {
                        return false;
                    }
                } else if (!this$metadata.equals(other$metadata)) {
                    return false;
                }

                Object this$dataCenterInfo = this.getDataCenterInfo();
                Object other$dataCenterInfo = other.getDataCenterInfo();
                if (this$dataCenterInfo == null) {
                    if (other$dataCenterInfo != null) {
                        return false;
                    }
                } else if (!this$dataCenterInfo.equals(other$dataCenterInfo)) {
                    return false;
                }

                Object this$healthCheckUrl = this.getHealthCheckUrl();
                Object other$healthCheckUrl = other.getHealthCheckUrl();
                if (this$healthCheckUrl == null) {
                    if (other$healthCheckUrl != null) {
                        return false;
                    }
                } else if (!this$healthCheckUrl.equals(other$healthCheckUrl)) {
                    return false;
                }

                Object this$secureViaAddress = this.getSecureViaAddress();
                Object other$secureViaAddress = other.getSecureViaAddress();
                if (this$secureViaAddress == null) {
                    if (other$secureViaAddress != null) {
                        return false;
                    }
                } else if (!this$secureViaAddress.equals(other$secureViaAddress)) {
                    return false;
                }

                Object this$vipAddress = this.getVipAddress();
                Object other$vipAddress = other.getVipAddress();
                if (this$vipAddress == null) {
                    if (other$vipAddress != null) {
                        return false;
                    }
                } else if (!this$vipAddress.equals(other$vipAddress)) {
                    return false;
                }

                Object this$securePort = this.getSecurePort();
                Object other$securePort = other.getSecurePort();
                if (this$securePort == null) {
                    if (other$securePort != null) {
                        return false;
                    }
                } else if (!this$securePort.equals(other$securePort)) {
                    return false;
                }

                Object this$homePageUrl = this.getHomePageUrl();
                Object other$homePageUrl = other.getHomePageUrl();
                if (this$homePageUrl == null) {
                    if (other$homePageUrl != null) {
                        return false;
                    }
                } else if (!this$homePageUrl.equals(other$homePageUrl)) {
                    return false;
                }

                Object this$statusPageUrl = this.getStatusPageUrl();
                Object other$statusPageUrl = other.getStatusPageUrl();
                if (this$statusPageUrl == null) {
                    return other$statusPageUrl == null;
                } else return this$statusPageUrl.equals(other$statusPageUrl);
            }
        }
    }

    protected boolean canEqual(Object other) {
        return other instanceof EurekaInstanceDTO;
    }

    public int hashCode() {
        int PRIME = 59;
        int result = 1;
        Object $instanceId = this.getInstanceId();
        result = result * 59 + ($instanceId == null ? 43 : $instanceId.hashCode());
        Object $ipAddr = this.getIpAddr();
        result = result * 59 + ($ipAddr == null ? 43 : $ipAddr.hashCode());
        Object $port = this.getPort();
        result = result * 59 + ($port == null ? 43 : $port.hashCode());
        Object $hostName = this.getHostName();
        result = result * 59 + ($hostName == null ? 43 : $hostName.hashCode());
        Object $app = this.getApp();
        result = result * 59 + ($app == null ? 43 : $app.hashCode());
        Object $status = this.getStatus();
        result = result * 59 + ($status == null ? 43 : $status.hashCode());
        Object $metadata = this.getMetadata();
        result = result * 59 + ($metadata == null ? 43 : $metadata.hashCode());
        Object $dataCenterInfo = this.getDataCenterInfo();
        result = result * 59 + ($dataCenterInfo == null ? 43 : $dataCenterInfo.hashCode());
        Object $healthCheckUrl = this.getHealthCheckUrl();
        result = result * 59 + ($healthCheckUrl == null ? 43 : $healthCheckUrl.hashCode());
        Object $secureViaAddress = this.getSecureViaAddress();
        result = result * 59 + ($secureViaAddress == null ? 43 : $secureViaAddress.hashCode());
        Object $vipAddress = this.getVipAddress();
        result = result * 59 + ($vipAddress == null ? 43 : $vipAddress.hashCode());
        Object $securePort = this.getSecurePort();
        result = result * 59 + ($securePort == null ? 43 : $securePort.hashCode());
        Object $homePageUrl = this.getHomePageUrl();
        result = result * 59 + ($homePageUrl == null ? 43 : $homePageUrl.hashCode());
        Object $statusPageUrl = this.getStatusPageUrl();
        result = result * 59 + ($statusPageUrl == null ? 43 : $statusPageUrl.hashCode());
        return result;
    }

    public String toString() {
        return "EurekaInstanceDTO(instanceId=" + this.getInstanceId() + ", ipAddr=" +
               this.getIpAddr() + ", port=" + this.getPort() + ", hostName=" + this.getHostName() +
               ", app=" + this.getApp() + ", status=" + this.getStatus() + ", metadata=" +
               this.getMetadata() + ", dataCenterInfo=" + this.getDataCenterInfo() +
               ", healthCheckUrl=" + this.getHealthCheckUrl() + ", secureViaAddress=" +
               this.getSecureViaAddress() + ", vipAddress=" + this.getVipAddress() +
               ", securePort=" + this.getSecurePort() + ", homePageUrl=" + this.getHomePageUrl() +
               ", statusPageUrl=" + this.getStatusPageUrl() + ")";
    }
}
