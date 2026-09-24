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

import java.util.Date;

public class MetadataDTO {

    private String name;
    private String namespace;
    private String uid;
    private String resourceVersion;
    private Date creationTimestamp;

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNamespace() {
        return this.namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getUid() {
        return this.uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getResourceVersion() {
        return this.resourceVersion;
    }

    public void setResourceVersion(String resourceVersion) {
        this.resourceVersion = resourceVersion;
    }

    public Date getCreationTimestamp() {
        return this.creationTimestamp;
    }

    public void setCreationTimestamp(Date creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof MetadataDTO)) {
            return false;
        } else {
            MetadataDTO other = (MetadataDTO) o;
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

                Object this$namespace = this.getNamespace();
                Object other$namespace = other.getNamespace();
                if (this$namespace == null) {
                    if (other$namespace != null) {
                        return false;
                    }
                } else if (!this$namespace.equals(other$namespace)) {
                    return false;
                }

                Object this$uid = this.getUid();
                Object other$uid = other.getUid();
                if (this$uid == null) {
                    if (other$uid != null) {
                        return false;
                    }
                } else if (!this$uid.equals(other$uid)) {
                    return false;
                }

                Object this$resourceVersion = this.getResourceVersion();
                Object other$resourceVersion = other.getResourceVersion();
                if (this$resourceVersion == null) {
                    if (other$resourceVersion != null) {
                        return false;
                    }
                } else if (!this$resourceVersion.equals(other$resourceVersion)) {
                    return false;
                }

                Object this$creationTimestamp = this.getCreationTimestamp();
                Object other$creationTimestamp = other.getCreationTimestamp();
                if (this$creationTimestamp == null) {
                    return other$creationTimestamp == null;
                } else return this$creationTimestamp.equals(other$creationTimestamp);
            }
        }
    }

    protected boolean canEqual(Object other) {
        return other instanceof MetadataDTO;
    }

    public int hashCode() {
        int PRIME = 59;
        int result = 1;
        Object $name = this.getName();
        result = result * 59 + ($name == null ? 43 : $name.hashCode());
        Object $namespace = this.getNamespace();
        result = result * 59 + ($namespace == null ? 43 : $namespace.hashCode());
        Object $uid = this.getUid();
        result = result * 59 + ($uid == null ? 43 : $uid.hashCode());
        Object $resourceVersion = this.getResourceVersion();
        result = result * 59 + ($resourceVersion == null ? 43 : $resourceVersion.hashCode());
        Object $creationTimestamp = this.getCreationTimestamp();
        result = result * 59 + ($creationTimestamp == null ? 43 : $creationTimestamp.hashCode());
        return result;
    }

    public String toString() {
        return "MetadataDTO(name=" + this.getName() + ", namespace=" + this.getNamespace() +
               ", uid=" + this.getUid() + ", resourceVersion=" + this.getResourceVersion() +
               ", creationTimestamp=" + this.getCreationTimestamp() + ")";
    }
}
