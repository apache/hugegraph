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

import java.util.List;

public class SpecDTO {

    private List<org.apache.hugegraph.space.register.dto.PortDTO> ports;
    private String clusterIP;
    private String type;

    public List<org.apache.hugegraph.space.register.dto.PortDTO> getPorts() {
        return this.ports;
    }

    public void setPorts(List<org.apache.hugegraph.space.register.dto.PortDTO> ports) {
        this.ports = ports;
    }

    public String getClusterIP() {
        return this.clusterIP;
    }

    public void setClusterIP(String clusterIP) {
        this.clusterIP = clusterIP;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof SpecDTO)) {
            return false;
        } else {
            SpecDTO other = (SpecDTO) o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                Object this$ports = this.getPorts();
                Object other$ports = other.getPorts();
                if (this$ports == null) {
                    if (other$ports != null) {
                        return false;
                    }
                } else if (!this$ports.equals(other$ports)) {
                    return false;
                }

                Object this$clusterIP = this.getClusterIP();
                Object other$clusterIP = other.getClusterIP();
                if (this$clusterIP == null) {
                    if (other$clusterIP != null) {
                        return false;
                    }
                } else if (!this$clusterIP.equals(other$clusterIP)) {
                    return false;
                }

                Object this$type = this.getType();
                Object other$type = other.getType();
                if (this$type == null) {
                    return other$type == null;
                } else return this$type.equals(other$type);
            }
        }
    }

    protected boolean canEqual(Object other) {
        return other instanceof SpecDTO;
    }

    public int hashCode() {
        int PRIME = 59;
        int result = 1;
        Object $ports = this.getPorts();
        result = result * 59 + ($ports == null ? 43 : $ports.hashCode());
        Object $clusterIP = this.getClusterIP();
        result = result * 59 + ($clusterIP == null ? 43 : $clusterIP.hashCode());
        Object $type = this.getType();
        result = result * 59 + ($type == null ? 43 : $type.hashCode());
        return result;
    }

    public String toString() {
        return "SpecDTO(ports=" + this.getPorts() + ", clusterIP=" + this.getClusterIP() +
               ", type=" + this.getType() + ")";
    }
}
