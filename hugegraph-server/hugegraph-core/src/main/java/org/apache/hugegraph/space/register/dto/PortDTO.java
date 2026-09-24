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

public class PortDTO {

    private String name;
    private String protocol;
    private Integer port;
    private Integer targetPort;
    private Integer nodePort;

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProtocol() {
        return this.protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public Integer getPort() {
        return this.port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public Integer getTargetPort() {
        return this.targetPort;
    }

    public void setTargetPort(Integer targetPort) {
        this.targetPort = targetPort;
    }

    public Integer getNodePort() {
        return this.nodePort;
    }

    public void setNodePort(Integer nodePort) {
        this.nodePort = nodePort;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof PortDTO)) {
            return false;
        } else {
            PortDTO other = (PortDTO) o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                Object this$name = this.getName();
                Object other$name = other.getName();
                if (this$name == null) {
                    if (other$name != null) {
                        return false;
                    }
                } else if (!this$name.equals(other$name)) {
                    return false;
                }

                Object this$protocol = this.getProtocol();
                Object other$protocol = other.getProtocol();
                if (this$protocol == null) {
                    if (other$protocol != null) {
                        return false;
                    }
                } else if (!this$protocol.equals(other$protocol)) {
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

                Object this$targetPort = this.getTargetPort();
                Object other$targetPort = other.getTargetPort();
                if (this$targetPort == null) {
                    if (other$targetPort != null) {
                        return false;
                    }
                } else if (!this$targetPort.equals(other$targetPort)) {
                    return false;
                }

                Object this$nodePort = this.getNodePort();
                Object other$nodePort = other.getNodePort();
                if (this$nodePort == null) {
                    return other$nodePort == null;
                } else return this$nodePort.equals(other$nodePort);
            }
        }
    }

    protected boolean canEqual(Object other) {
        return other instanceof PortDTO;
    }

    public int hashCode() {
        int PRIME = 59;
        int result = 1;
        Object $name = this.getName();
        result = result * 59 + ($name == null ? 43 : $name.hashCode());
        Object $protocol = this.getProtocol();
        result = result * 59 + ($protocol == null ? 43 : $protocol.hashCode());
        Object $port = this.getPort();
        result = result * 59 + ($port == null ? 43 : $port.hashCode());
        Object $targetPort = this.getTargetPort();
        result = result * 59 + ($targetPort == null ? 43 : $targetPort.hashCode());
        Object $nodePort = this.getNodePort();
        result = result * 59 + ($nodePort == null ? 43 : $nodePort.hashCode());
        return result;
    }

    public String toString() {
        return "PortDTO(name=" + this.getName() + ", protocol=" + this.getProtocol() + ", port=" +
               this.getPort() + ", targetPort=" + this.getTargetPort() + ", nodePort=" +
               this.getNodePort() + ")";
    }
}
