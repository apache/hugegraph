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

package org.apache.hugegraph.store.node.grpc.query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.consts.PoolNames;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.query.QueryRequest;
import org.apache.hugegraph.store.grpc.query.QueryResponse;
import org.apache.hugegraph.store.grpc.query.QueryServiceGrpc;
import org.apache.hugegraph.store.query.KvSerializer;
import org.apache.hugegraph.store.util.ExecutorUtil;
import org.lognet.springboot.grpc.GRpcService;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@GRpcService
public class AggregativeQueryService extends QueryServiceGrpc.QueryServiceImplBase {

    private final int batchSize;

    private final Long timeout;

    @Getter
    private final ThreadPoolExecutor threadPool;

    public AggregativeQueryService() {
        var queryPushDownOption = HgStoreEngine.getInstance().getOption().getQueryPushDownOption();

        timeout = queryPushDownOption.getFetchTimeout();
        batchSize = queryPushDownOption.getFetchBatchSize();

        this.threadPool = ExecutorUtil.createExecutor(PoolNames.SCAN_V2,
                                                      Runtime.getRuntime().availableProcessors(),
                                                      queryPushDownOption.getThreadPoolSize(),
                                                      10000, true);
    }

    /**
     * Generate error response.
     *
     * @param queryId query identifier
     * @param t       exception object
     * @return query response object
     */
    public static QueryResponse errorResponse(QueryResponse.Builder builder, String queryId,
                                              Throwable t) {
        return builder.setQueryId(queryId)
                      .setIsOk(false)
                      .setIsFinished(false)
                      .setMessage(t.getMessage() == null ? "" : t.getMessage())
                      .build();
    }

    @Override
    public StreamObserver<QueryRequest> query(StreamObserver<QueryResponse> observer) {
        return new AggregativeQueryObserver(observer, threadPool, timeout, batchSize);
    }

    @Override
    public void query0(QueryRequest request, StreamObserver<QueryResponse> observer) {

        var itr = QueryUtil.getIterator(request);
        var builder = QueryResponse.newBuilder();
        var kvBuilder = Kv.newBuilder();

        try {
            while (itr.hasNext()) {
                var column = (RocksDBSession.BackendColumn) itr.next();
                if (column != null) {
                    builder.addData(kvBuilder.setKey(ByteString.copyFrom(column.name))
                                             .setValue(column.value == null ? ByteString.EMPTY :
                                                       ByteString.copyFrom(column.value))
                                             .build());
                }
            }
            builder.setQueryId(request.getQueryId());
            builder.setIsOk(true);
            builder.setIsFinished(true);
            observer.onNext(builder.build());
        } catch (Exception e) {
            observer.onNext(errorResponse(builder, request.getQueryId(), e));
        }
        observer.onCompleted();
    }

    /**
     * Query data count
     *
     * @param request  query request object
     * @param observer Observer object for receiving query response results
     */
    @Override
    public void count(QueryRequest request, StreamObserver<QueryResponse> observer) {

        log.debug("query id : {}, simple count of table: {}", request.getQueryId(),
                  request.getTable());
        var builder = QueryResponse.newBuilder();
        var kvBuilder = Kv.newBuilder();

        try {

            var handler = new QueryUtil().getHandler();
            long start = System.currentTimeMillis();
            long count = handler.count(request.getGraph(), request.getTable());
            log.debug("query id: {}, count of cost: {} ms", request.getQueryId(),
                      System.currentTimeMillis() - start);
            List<Object> array = new ArrayList<>();
            for (int i = 0; i < request.getFunctionsList().size(); i++) {
                array.add(new AtomicLong(count));
            }

            kvBuilder.setKey(ByteString.copyFrom(KvSerializer.toBytes(List.of())));
            kvBuilder.setValue(ByteString.copyFrom(KvSerializer.toBytes(array)));
            builder.addData(kvBuilder.build());
            builder.setQueryId(request.getQueryId());
            builder.setIsOk(true);
            builder.setIsFinished(true);
            observer.onNext(builder.build());
        } catch (Exception e) {
            observer.onNext(errorResponse(builder, request.getQueryId(), e));
        }
        observer.onCompleted();
    }
}
