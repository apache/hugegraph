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

public class ServiceDTO {

    private String kind;
    private String apiVersion;
    private org.apache.hugegraph.space.register.dto.MetadataDTO metadata;
    private SpecDTO spec;

    public String getKind() {
        return this.kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public String getApiVersion() {
        return this.apiVersion;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    public org.apache.hugegraph.space.register.dto.MetadataDTO getMetadata() {
        return this.metadata;
    }

    public void setMetadata(org.apache.hugegraph.space.register.dto.MetadataDTO metadata) {
        this.metadata = metadata;
    }

    public SpecDTO getSpec() {
        return this.spec;
    }

    public void setSpec(SpecDTO spec) {
        this.spec = spec;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof ServiceDTO)) {
            return false;
        } else {
            ServiceDTO other = (ServiceDTO) o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                Object this$kind = this.getKind();
                Object other$kind = other.getKind();
                if (this$kind == null) {
                    if (other$kind != null) {
                        return false;
                    }
                } else if (!this$kind.equals(other$kind)) {
                    return false;
                }

                Object this$apiVersion = this.getApiVersion();
                Object other$apiVersion = other.getApiVersion();
                if (this$apiVersion == null) {
                    if (other$apiVersion != null) {
                        return false;
                    }
                } else if (!this$apiVersion.equals(other$apiVersion)) {
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

                Object this$spec = this.getSpec();
                Object other$spec = other.getSpec();
                if (this$spec == null) {
                    return other$spec == null;
                } else return this$spec.equals(other$spec);
            }
        }
    }

    protected boolean canEqual(Object other) {
        return other instanceof ServiceDTO;
    }

    public int hashCode() {
        int PRIME = 59;
        int result = 1;
        Object $kind = this.getKind();
        result = result * 59 + ($kind == null ? 43 : $kind.hashCode());
        Object $apiVersion = this.getApiVersion();
        result = result * 59 + ($apiVersion == null ? 43 : $apiVersion.hashCode());
        Object $metadata = this.getMetadata();
        result = result * 59 + ($metadata == null ? 43 : $metadata.hashCode());
        Object $spec = this.getSpec();
        result = result * 59 + ($spec == null ? 43 : $spec.hashCode());
        return result;
    }

    public String toString() {
        return "ServiceDTO(kind=" + this.getKind() + ", apiVersion=" + this.getApiVersion() +
               ", metadata=" + this.getMetadata() + ", spec=" + this.getSpec() + ")";
    }
}
