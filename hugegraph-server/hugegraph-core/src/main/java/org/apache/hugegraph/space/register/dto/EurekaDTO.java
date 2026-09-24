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

public class EurekaDTO {

    private EurekaInstanceDTO instance;

    public EurekaInstanceDTO getInstance() {
        return this.instance;
    }

    public EurekaDTO setInstance(EurekaInstanceDTO instance) {
        this.instance = instance;
        return this;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof EurekaDTO)) {
            return false;
        } else {
            EurekaDTO other = (EurekaDTO) o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                Object this$instance = this.getInstance();
                Object other$instance = other.getInstance();
                if (this$instance == null) {
                    return other$instance == null;
                } else return this$instance.equals(other$instance);
            }
        }
    }

    protected boolean canEqual(Object other) {
        return other instanceof EurekaDTO;
    }

    public int hashCode() {
        int PRIME = 59;
        int result = 1;
        Object $instance = this.getInstance();
        result = result * 59 + ($instance == null ? 43 : $instance.hashCode());
        return result;
    }

    public String toString() {
        return "EurekaDTO(instance=" + this.getInstance() + ")";
    }
}
