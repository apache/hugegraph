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

package org.apache.hugegraph.store;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.store.client.HgStoreNodeManager;
import org.apache.hugegraph.store.client.HgStoreNodePartitionerImpl;
import org.apache.hugegraph.store.client.HgStoreSessionProvider;

/**
 * Maintain HgStoreSession instances.
 * HgStore-clusters.
 */

@ThreadSafe
public final class HgStoreClient {

    // TODO: Holding more than one HgSessionManager is available,if you want to connect multi
    private final HgSessionProvider sessionProvider;
    private PDClient pdClient;

    private HgSessionConfig sessionConfig;

    public HgStoreClient() {
        this.sessionProvider = new HgStoreSessionProvider();
    }

    public HgStoreClient(PDConfig config) {
        this.sessionProvider = new HgStoreSessionProvider();
        pdClient = PDClient.create(config);
        setPdClient(pdClient);
    }

    public HgStoreClient(PDClient pdClient) {
        this.sessionProvider = new HgStoreSessionProvider();
        setPdClient(pdClient);
    }

    public static HgStoreClient create(PDConfig config) {
        return new HgStoreClient(config);
    }

    public static HgStoreClient create(PDClient pdClient) {
        return new HgStoreClient(pdClient);
    }

    public static HgStoreClient create() {
        return new HgStoreClient();
    }

    public void setPDConfig(PDConfig config) {
        pdClient = PDClient.create(config);
        setPdClient(pdClient);
    }

    public void setSessionConfig(HgSessionConfig sessionConfig) {
        this.sessionConfig = sessionConfig;
    }

    /**
     * Retrieve or create a HgStoreSession.
     *
     * @param graphName
     * @return
     */
    public HgStoreSession openSession(String graphName) {
        return this.sessionProvider.createSession(graphName, this.sessionConfig);
    }

    public PDClient getPdClient() {
        return pdClient;
    }

    public void setPdClient(PDClient client) {
        this.pdClient = client;
        HgStoreNodeManager nodeManager =
                HgStoreNodeManager.getInstance();

        HgStoreNodePartitionerImpl p = new HgStoreNodePartitionerImpl(pdClient, nodeManager);
        nodeManager.setNodeProvider(p);
        nodeManager.setNodePartitioner(p);
        nodeManager.setNodeNotifier(p);
    }

}
