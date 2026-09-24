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

package org.apache.hugegraph.store.client;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.store.HgSessionConfig;
import org.apache.hugegraph.store.HgStoreSession;

/**
 * created on 2021/10/12
 */
@ThreadSafe
public final class MultiNodeSessionFactory {

    // TODO multi-instance ?
    private final static MultiNodeSessionFactory INSTANCE = new MultiNodeSessionFactory();
    // TODO multi-instance ?
    private final HgStoreNodeManager nodeManager = HgStoreNodeManager.getInstance();
    // TODO: to be a chain assigned to each graph
    //private HgStoreNodeDispatcher storeNodeDispatcher;

    private MultiNodeSessionFactory() {
    }

    static MultiNodeSessionFactory getInstance() {
        return INSTANCE;
    }

    HgStoreSession createStoreSession(String graphName) {
        return buildProxy(graphName, null);
    }

    HgStoreSession createStoreSession(String graphName, HgSessionConfig config) {
        return buildProxy(graphName, config);
    }

    private HgStoreSession buildProxy(String graphName, HgSessionConfig config) {
        //return new MultiNodeSessionProxy(graphName, nodeManager, storeNodeDispatcher);
        //return new NodePartitionSessionProxy(graphName,nodeManager);
        //return new NodeRetrySessionProxy(graphName,nodeManager);
        if (config == null) {
            return new NodeTxSessionProxy(graphName, nodeManager);
        }

        return new NodeTxSessionProxy(graphName, nodeManager, config);
    }
}
