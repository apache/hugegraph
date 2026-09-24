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

import static org.apache.hugegraph.store.node.grpc.ScanUtil.getIterator;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.ScanQueryRequest;
import org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq;
import org.apache.hugegraph.store.node.util.HgGrpc;
import org.apache.hugegraph.store.node.util.HgStoreNodeUtil;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * created on 2022/04/08
 *
 * @version 0.1.0
 */
@Slf4j
public class ScanBatchOneShotResponse {

    /**
     * Handle one-shot batch scan
     *
     * @param request
     * @param responseObserver
     */
    public static void scanOneShot(ScanStreamBatchReq request,
                                   StreamObserver<KvPageRes> responseObserver,
                                   HgStoreWrapperEx wrapper) {

        String graph = request.getHeader().getGraph();
        ScanQueryRequest queryRequest = request.getQueryRequest();
        ScanIterator iterator = getIterator(graph, queryRequest, wrapper);

        KvPageRes.Builder resBuilder = KvPageRes.newBuilder();
        Kv.Builder kvBuilder = Kv.newBuilder();

        long limit = queryRequest.getLimit();

        if (limit <= 0) {
            limit = Integer.MAX_VALUE;
            log.warn("As limit is less than or equals 0, default limit was effective:[ {} ]",
                     Integer.MAX_VALUE);
        }

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

            responseObserver.onNext(resBuilder.build());
            responseObserver.onCompleted();

        } catch (Throwable t) {
            String msg = "Failed to do oneshot batch scan, scanning was interrupted, cause by:";
            responseObserver.onError(
                    HgGrpc.toErr(Status.Code.INTERNAL, msg, t));
        } finally {
            iterator.close();
        }

    }

}
