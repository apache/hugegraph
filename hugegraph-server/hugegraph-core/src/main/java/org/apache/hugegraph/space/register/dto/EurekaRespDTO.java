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

public class EurekaRespDTO {

    private String name;
    private List<org.apache.hugegraph.space.register.dto.EurekaInstanceDTO> instance;

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<org.apache.hugegraph.space.register.dto.EurekaInstanceDTO> getInstance() {
        return this.instance;
    }

    public void setInstance(
            List<org.apache.hugegraph.space.register.dto.EurekaInstanceDTO> instance) {
        this.instance = instance;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof EurekaRespDTO)) {
            return false;
        } else {
            EurekaRespDTO other = (EurekaRespDTO) o;
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

                Object this$instance = this.getInstance();
                Object other$instance = other.getInstance();
                if (this$instance == null) {
                    return other$instance == null;
                } else return this$instance.equals(other$instance);
            }
        }
    }

    protected boolean canEqual(Object other) {
        return other instanceof EurekaRespDTO;
    }

    public int hashCode() {
        int PRIME = 59;
        int result = 1;
        Object $name = this.getName();
        result = result * 59 + ($name == null ? 43 : $name.hashCode());
        Object $instance = this.getInstance();
        result = result * 59 + ($instance == null ? 43 : $instance.hashCode());
        return result;
    }

    public String toString() {
        return "EurekaRespDTO(name=" + this.getName() + ", instance=" + this.getInstance() + ")";
    }
}
