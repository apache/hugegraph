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

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq;
import org.apache.hugegraph.store.node.util.HgGrpc;
import org.apache.hugegraph.store.node.util.HgStoreNodeUtil;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * created on 2022/02/17
 *
 * @version 3.6.0
 */
@Slf4j
public class ScanOneShotResponse {

    /**
     * Handle one-shot scan
     *
     * @param request
     * @param responseObserver
     */
    public static void scanOneShot(ScanStreamReq request,
                                   StreamObserver<KvPageRes> responseObserver,
                                   HgStoreWrapperEx wrapper) {
        KvPageRes.Builder resBuilder = KvPageRes.newBuilder();
        Kv.Builder kvBuilder = Kv.newBuilder();

        long limit = request.getLimit();

        if (limit <= 0) {
            responseObserver.onError(HgGrpc.toErr("limit<=0, please to invoke stream scan."));
            return;
        }
        ScanIterator iterator = ScanUtil.getIterator(request, wrapper);

        int count = 0;

        try {
            while (iterator.hasNext()) {

                if (++count > limit) {
                    break;
                }

                RocksDBSession.BackendColumn col = iterator.next();

                resBuilder.addData(kvBuilder
                                           .setKey(ByteString.copyFrom(col.name))
                                           .setValue(ByteString.copyFrom(col.value))
                                           .setCode(HgStoreNodeUtil.toInt(iterator.position()))
                );

            }

            responseObserver.onNext(
                    resBuilder.setVersion(ScanUtil.responseVersion(request))
                              .build());
            responseObserver.onCompleted();

        } catch (Throwable t) {
            String msg = "an exception occurred during data scanning";
            responseObserver.onError(HgGrpc.toErr(Status.INTERNAL, msg, t));
        } finally {
            iterator.close();
        }

    }

}
