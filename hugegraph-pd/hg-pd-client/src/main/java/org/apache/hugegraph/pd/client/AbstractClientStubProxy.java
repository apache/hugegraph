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

import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;

public class AbstractClientStubProxy {

    private LinkedList<String> hostList = new LinkedList<>();
    private AbstractBlockingStub blockingStub;
    private AbstractStub stub;

    public AbstractClientStubProxy(String[] hosts) {
        for (String host : hosts) {
            if (!host.isEmpty()) {
                hostList.offer(host);
            }
        }
    }

    public LinkedList<String> getHostList() {
        return hostList;
    }

    public String nextHost() {
        String host = hostList.poll();
        hostList.offer(host);
        return host;
    }

    public AbstractBlockingStub getBlockingStub() {
        return this.blockingStub;
    }

    public void setBlockingStub(AbstractBlockingStub stub) {
        this.blockingStub = stub;
    }

    public String getHost() {
        return hostList.peek();
    }

    public int getHostCount() {
        return hostList.size();
    }

    public AbstractStub getStub() {
        return stub;
    }

    public void setStub(AbstractStub stub) {
        this.stub = stub;
    }
}
