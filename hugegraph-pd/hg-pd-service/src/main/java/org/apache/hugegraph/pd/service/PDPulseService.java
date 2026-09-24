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

package org.apache.hugegraph.pd.service;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.HgPdPulseGrpc;
import org.apache.hugegraph.pd.grpc.pulse.PulseRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.meta.QueueStore;
import org.apache.hugegraph.pd.pulse.PDPulseSubject;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@GRpcService
public class PDPulseService extends HgPdPulseGrpc.HgPdPulseImplBase {

    private static Supplier<List<Metapb.QueueItem>> queueRetrieveFunction =
            () -> Collections.emptyList();
    private static Function<Metapb.QueueItem, Boolean> queueDurableFunction = (e) -> true;
    private static final Function<String, Boolean> queueRemoveFunction = (e) -> true;
    @Autowired
    private PDConfig pdConfig;
    private volatile QueueStore queueStore;

    public PDPulseService() {
        PDPulseSubject.setQueueRetrieveFunction(() -> getQueue());
        PDPulseSubject.setQueueDurableFunction(getQueueDurableFunction());
        PDPulseSubject.setQueueRemoveFunction(getQueueRemoveFunction());
    }

    @Override
    public StreamObserver<PulseRequest> pulse(StreamObserver<PulseResponse> responseObserver) {
        return PDPulseSubject.addObserver(responseObserver);
    }

    private Function<String, Boolean> getQueueRemoveFunction() {
        return itemId -> {
            try {
                this.getQueueStore().removeItem(itemId);
                return true;
            } catch (Throwable t) {
                log.error("Failed to remove item from store, item-id: " + itemId + ", cause by:",
                          t);
            }
            return false;
        };
    }

    private Function<Metapb.QueueItem, Boolean> getQueueDurableFunction() {
        return item -> {
            try {
                this.getQueueStore().addItem(item);
                return true;
            } catch (Throwable t) {
                log.error("Failed to add item to store, item: " + item.toString() + ", cause by:",
                          t);
            }
            return false;
        };
    }

    private boolean isLeader() {
        return RaftEngine.getInstance().isLeader();
    }

    private List<Metapb.QueueItem> getQueue() {

        if (!isLeader()) {
            return Collections.emptyList();
        }

        try {
            return this.getQueueStore().getQueue();
        } catch (Throwable t) {
            log.error("Failed to retrieve queue from QueueStore, cause by:", t);
        }

        log.warn("Returned empty queue list.");
        return Collections.emptyList();
    }

    private QueueStore getQueueStore() {
        QueueStore local = this.queueStore;
        if (local == null) {
            synchronized (this) {
                local = this.queueStore;
                if (local == null) {
                    local = MetadataFactory.newQueueStore(pdConfig);
                    this.queueStore = local;
                }
            }
        }
        return local;
    }
}
