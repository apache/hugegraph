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

package org.apache.hugegraph.store.client.grpc;

import java.util.function.Function;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.client.HgStoreNodeSession;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq;

import io.grpc.stub.StreamObserver;

public final class KvPageScannerTestSupport {

    private KvPageScannerTestSupport() {
    }

    public static HgKvIterator<HgKvEntry> iterator(
            HgStoreNodeSession session, ScanStreamReq.Builder builder,
            Function<StreamObserver<KvPageRes>,
                     StreamObserver<ScanStreamReq>> streamFactory) {
        KvPageScanner scanner = new KvPageScanner(session, builder,
                                                  streamFactory);
        return GrpcKvIteratorImpl.of(session, scanner);
    }
}
