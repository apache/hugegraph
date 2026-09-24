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

public class ApplicationDTO {

    private EurekaRespDTO application;

    public EurekaRespDTO getApplication() {
        return this.application;
    }

    public void setApplication(EurekaRespDTO application) {
        this.application = application;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof ApplicationDTO)) {
            return false;
        } else {
            ApplicationDTO other = (ApplicationDTO) o;
            if (!other.canEqual(this)) {
                return false;
            } else {
                Object this$application = this.getApplication();
                Object other$application = other.getApplication();
                if (this$application == null) {
                    return other$application == null;
                } else return this$application.equals(other$application);
            }
        }
    }

    protected boolean canEqual(Object other) {
        return other instanceof ApplicationDTO;
    }

    public int hashCode() {
        int PRIME = 59;
        int result = 1;
        Object $application = this.getApplication();
        result = result * 59 + ($application == null ? 43 : $application.hashCode());
        return result;
    }

    public String toString() {
        return "ApplicationDTO(application=" + this.getApplication() + ")";
    }
}
