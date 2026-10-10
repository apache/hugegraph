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

package org.apache.hugegraph.store.node.grpc;

import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hugegraph.store.grpc.stream.KvStream;

import com.alipay.sofa.jraft.util.concurrent.ConcurrentHashSet;

import io.grpc.stub.StreamObserver;

public class ScanBatchResponseFactory {

    private final static ScanBatchResponseFactory instance = new ScanBatchResponseFactory();
    private final Set<StreamObserver> streamObservers = new ConcurrentHashSet<>();

    public static ScanBatchResponseFactory getInstance() {
        return instance;
    }

    public static StreamObserver of(StreamObserver<KvStream> responseObserver,
                                    HgStoreWrapperEx wrapper, ThreadPoolExecutor executor) {
        StreamObserver observer = of(responseObserver, wrapper, executor, new ScanLifecycle());
        getInstance().checkStreamActive();
        return observer;
    }

    static StreamObserver of(StreamObserver<KvStream> responseObserver,
                             HgStoreWrapperEx wrapper, ThreadPoolExecutor executor,
                             ScanLifecycle lifecycle) {
        StreamObserver observer = new ScanBatchResponse(responseObserver, wrapper, executor, lifecycle);
        getInstance().addStreamObserver(observer);
        return observer;
    }

    public int addStreamObserver(StreamObserver observer) {
        streamObservers.add(observer);
        return streamObservers.size();
    }

    public int removeStreamObserver(StreamObserver observer) {
        streamObservers.remove(observer);
        return streamObservers.size();
    }

    /**
     * Check if the Stream is active, and close the timed-out Stream in a timely manner.
     */
    public void checkStreamActive() {
        streamObservers.forEach(streamObserver -> {
            ((ScanBatchResponse) streamObserver).checkActiveTimeout();
        });
    }
}
