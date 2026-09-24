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

package org.apache.hugegraph.pd.client;

import java.util.LinkedList;

import org.apache.hugegraph.pd.grpc.PDGrpc;

public class StubProxy {

    private volatile PDGrpc.PDBlockingStub stub;
    private LinkedList<String> hosts = new LinkedList<>();
    private String leader;

    public StubProxy(String[] hosts) {
        for (String host : hosts) {
            if (!host.isEmpty()) {
                this.hosts.offer(host);
            }
        }
    }

    public String nextHost() {
        String host = hosts.poll();
        hosts.offer(host);
        return host;
    }

    public void set(PDGrpc.PDBlockingStub stub) {
        this.stub = stub;
    }

    public PDGrpc.PDBlockingStub get() {
        return this.stub;
    }

    public String getHost() {
        return hosts.peek();
    }

    public int getHostCount() {
        return hosts.size();
    }

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }
}
