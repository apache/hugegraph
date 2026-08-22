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

package org.apache.hugegraph.ct.config;

import static org.apache.hugegraph.ct.base.ClusterConstant.CONFIG_FILE_PATH;
import static org.apache.hugegraph.ct.base.ClusterConstant.LOCALHOST;
import static org.apache.hugegraph.ct.base.ClusterConstant.SERVER_PROPERTIES;
import static org.apache.hugegraph.ct.base.ClusterConstant.SERVER_TEMPLATE_FILE;
import static org.apache.hugegraph.ct.base.EnvUtil.getAvailablePort;

import java.nio.file.Paths;

import lombok.Getter;

@Getter
public class ServerConfig extends AbstractConfig {

    private final int rpcPort;
    private final int restPort;
    private final int gremlinPort;

    public ServerConfig() {
        readTemplate(Paths.get(CONFIG_FILE_PATH + SERVER_TEMPLATE_FILE));
        this.fileName = SERVER_PROPERTIES;
        this.rpcPort = getAvailablePort();
        this.restPort = getAvailablePort();
        this.gremlinPort = getAvailablePort();
        properties.put("REST_SERVER_ADDRESS", LOCALHOST + ":" + this.restPort);
        properties.put("GREMLIN_SERVER_ADDRESS",
                       LOCALHOST + ":" + this.gremlinPort);
        properties.put("RPC_PORT", String.valueOf(this.rpcPort));
    }

    public void setServerID(String serverID) {
        setProperty("SERVER_ID", serverID);
    }

    public void setRole(String role) {
        setProperty("ROLE", role);
    }
}

