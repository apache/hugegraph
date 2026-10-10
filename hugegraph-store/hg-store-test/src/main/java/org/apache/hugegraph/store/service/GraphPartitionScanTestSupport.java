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

package org.apache.hugegraph.store.service;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.store.grpc.Graphpb;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;
import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;

abstract class GraphPartitionScanTestSupport {

    static ScanPartitionRequest request(long limit) {
        return ScanPartitionRequest.newBuilder().setScanRequest(Graphpb.ScanPartitionRequest.Request.newBuilder()
                .setGraphName("TEST/credit-window")
                .setScanType(Graphpb.ScanPartitionRequest.ScanType.SCAN_VERTEX).setLimit(limit)).build();
    }

    static ThreadPoolExecutor executor() {
        return new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }

    static HgStoreStreamImpl service(ThreadPoolExecutor executor) throws Exception {
        HgStoreStreamImpl service = new HgStoreStreamImpl();
        Field field = HgStoreStreamImpl.class.getDeclaredField("executor");
        field.setAccessible(true);
        field.set(service, executor);
        return service;
    }

    static Map<?, ?> registry(HgStoreStreamImpl service) throws Exception {
        Field field = HgStoreStreamImpl.class.getDeclaredField("scans");
        field.setAccessible(true);
        return (Map<?, ?>) field.get(service);
    }
}
