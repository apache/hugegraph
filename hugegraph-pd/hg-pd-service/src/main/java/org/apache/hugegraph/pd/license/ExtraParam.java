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

package org.apache.hugegraph.pd.license;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExtraParam {

    @JsonProperty("username")
    private String username;

    @JsonProperty("license_type")
    private String licenseType;

    @JsonProperty("id")
    private String id;

    @JsonProperty("version")
    private String version;

    @JsonProperty("graphs")
    private int graphs;

    @JsonProperty("ip")
    private String ip;

    @JsonProperty("mac")
    private String mac;

    @JsonProperty("cpus")
    private int cpus;

    // The unit is MB
    @JsonProperty("ram")
    private int ram;

    @JsonProperty("threads")
    private int threads;

    // The unit is MB
    @JsonProperty("memory")
    private int memory;

    @JsonProperty("nodes")
    private int nodes;

    // The unit is MB
    @JsonProperty("data_size")
    private long dataSize;

    @JsonProperty("vertices")
    private long vertices;

    @JsonProperty("edges")
    private long edges;

    public String username() {
        return this.username;
    }

    public String licenseType() {
        return this.licenseType;
    }

    public String id() {
        return this.id;
    }

    public String version() {
        return this.version;
    }

    public int graphs() {
        return this.graphs;
    }

    public String ip() {
        return this.ip;
    }

    public String mac() {
        return this.mac;
    }

    public int cpus() {
        return this.cpus;
    }

    public int ram() {
        return this.ram;
    }

    public int threads() {
        return this.threads;
    }

    public int memory() {
        return this.memory;
    }

    public int nodes() {
        return this.nodes;
    }

    public long dataSize() {
        return this.dataSize;
    }

    public long vertices() {
        return this.vertices;
    }

    public long edges() {
        return this.edges;
    }
}
