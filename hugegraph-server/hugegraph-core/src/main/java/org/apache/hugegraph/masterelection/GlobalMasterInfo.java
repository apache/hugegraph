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

package org.apache.hugegraph.masterelection;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.type.define.NodeRole;
import org.apache.hugegraph.util.E;

public final class GlobalMasterInfo {

    private volatile Id nodeId;
    private volatile NodeRole nodeRole;

    public GlobalMasterInfo() {
        this.nodeId = null;
        this.nodeRole = null;
    }

    public Id nodeId() {
        return this.nodeId;
    }

    public NodeRole nodeRole() {
        return this.nodeRole;
    }

    public void initNodeId(Id id) {
        this.nodeId = id;
    }

    public void initNodeRole(NodeRole role) {
        E.checkArgument(role != null, "The server role can't be null");
        E.checkArgument(this.nodeRole == null,
                        "The server role can't be init twice");
        this.nodeRole = role;
    }

    public void changeNodeRole(NodeRole role) {
        E.checkArgument(role != null, "The server role can't be null");
        this.nodeRole = role;
    }

    public static GlobalMasterInfo master(String nodeId) {
        GlobalMasterInfo nodeInfo = new GlobalMasterInfo();
        nodeInfo.nodeId = IdGenerator.of(nodeId);
        nodeInfo.nodeRole = NodeRole.MASTER;
        return nodeInfo;
    }
}
