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

package org.apache.hugegraph.store.client.query;

import static org.apache.hugegraph.store.constant.HugeServerTables.IN_EDGE_TABLE;
import static org.apache.hugegraph.store.constant.HugeServerTables.OUT_EDGE_TABLE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.serializer.BinaryElementSerializer;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.client.HgStoreNodePartitioner;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.query.AggregateFunc;
import org.apache.hugegraph.store.grpc.query.AggregationType;
import org.apache.hugegraph.store.grpc.query.DeDupOption;
import org.apache.hugegraph.store.grpc.query.Index;
import org.apache.hugegraph.store.grpc.query.QueryRequest;
import org.apache.hugegraph.store.grpc.query.QueryResponse;
import org.apache.hugegraph.store.grpc.query.ScanType;
import org.apache.hugegraph.store.grpc.query.ScanTypeParam;
import org.apache.hugegraph.store.query.BaseElementComparator;
import org.apache.hugegraph.store.query.KvSerializer;
import org.apache.hugegraph.store.query.QueryTypeParam;
import org.apache.hugegraph.store.query.StoreQueryParam;
import org.apache.hugegraph.store.query.StoreQueryType;
import org.apache.hugegraph.store.query.Tuple2;
import org.apache.hugegraph.store.query.func.AggregationFunctionParam;
import org.apache.hugegraph.store.query.util.KeyUtil;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.structure.KvElement;

import com.google.protobuf.ByteString;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryExecutor {

    private final HgStoreNodePartitioner nodePartitioner;

    private static final QueryV2Client client = new QueryV2Client();

    private static final BinaryElementSerializer serializer = new BinaryElementSerializer();

    private final HugeGraphSupplier supplier;

    /**
     * Timeout duration for StreamObserver receiving response
     */
    private long timeout = 60_000;

    /**
     * Used for testing single machine
     */
    private static final ThreadLocal<String> filterStore = new ThreadLocal<>();

    public QueryExecutor(HgStoreNodePartitioner nodePartitioner, HugeGraphSupplier supplier,
                         Long timeout) {
        this.nodePartitioner = nodePartitioner;
        this.supplier = supplier;
        this.timeout = timeout;
    }

    /**
     * Get iterator list based on query condition
     *
     * @param query query parameter
     * @return iterator list
     * @throws PDException throw PDException if any error occurs
     */
    public List<HgKvIterator<BaseElement>> getIterators(StoreQueryParam query) throws PDException {
        if (isSimpleQuery(query)) {
            return getSimpleIterator(query);
        }

        List<HgKvIterator<BaseElement>> iterators;

        if (isSimpleCountQuery(query)) {
            iterators = getCountIterator(query);
        } else {
            // Obtain iterator of all nodes
            iterators = getNodeTasks(query)
                    .parallelStream()
                    .map(tuple -> getIterator(tuple.getV1(), tuple.getV2().build()))
                    .collect(Collectors.toList());
        }

        if (isEmpty(query.getFuncList()) && isEmpty(query.getOrderBy()) && query.getLimit() == 0) {
            return iterators;
        }

        HgKvIterator<BaseElement> iterator;

        if (!isEmpty(query.getFuncList())) {
            // agg: sort first, and then calculate
            iterator = new StreamSortedIterator<>(iterators, (o1, o2) -> {
                if (o1 == null && o2 == null) {
                    return 0;
                }
                if (o1 != null && o2 != null) {
                    if (o1 instanceof KvElement && o2 instanceof KvElement) {
                        return ((KvElement) o1).compareTo((KvElement) o2);
                    }
                    if (!(o1 instanceof KvElement)) {
                        throw new IllegalStateException(
                                "Expected KvElement but got: " + o1.getClass().getName());
                    }
                    // !(o2 instanceof KvElement)
                    throw new IllegalStateException(
                            "Expected KvElement but got: " + o2.getClass().getName());
                }

                return o1 != null ? 1 : -1;
            });

            iterator = new StreamFinalAggregationIterator<>(iterator, query.getFuncList());
            if (query.getSampleFactor() != 1) {
                // Sampling is not conducted during the process, but performed at the end
                iterator = new StreamSampleIterator<>(iterator, query.getSampleFactor());
            }
        } else if (!isEmpty(query.getOrderBy())) {
            // If sort
            if (query.getSortOrder() != StoreQueryParam.SORT_ORDER.STRICT_ORDER) {
                iterator = new StreamSortedIterator<>(iterators,
                                                      new BaseElementComparator(query.getOrderBy(),
                                                                                query.getSortOrder() ==
                                                                                StoreQueryParam.SORT_ORDER.ASC));
            } else {
                iterator = new StreamStrictOrderIterator<>(query.getQueryParam(), iterators,
                                                           query.getGraph(), this.nodePartitioner);
            }
        } else {
            // with limit
            iterator = new MultiStreamIterator<>(iterators);
        }

        if (query.getLimit() > 0) {
            iterator = new StreamLimitIterator<>(iterator, query.getLimit());
        }

        return List.of(iterator);
    }

    /**
     * Use StreamKvIterator to encapsulate the returned result
     *
     * @param address store node addr
     * @param request initial request
     * @return iterator result
     */
    private StreamKvIterator<BaseElement> getIterator(String address, QueryRequest request) {
        var stub = client.getQueryServiceStub(address);
        var hasAgg = !isEmpty(request.getFunctionsList());

        var observer = new CommonKvStreamObserver<QueryResponse, BaseElement>(
                response -> new Iterator<>() {
                    final Iterator<Kv> itr = response.getDataList().iterator();

                    BaseElement element = null;

                    @Override
                    public boolean hasNext() {
                        if (element == null) {
                            while (itr.hasNext()) {
                                element = fromKv(request.getTable(), itr.next(), hasAgg);
                                if (element != null) {
                                    break;
                                }
                            }
                        }
                        return element != null;
                    }

                    @Override
                    public BaseElement next() {
                        try {
                            return element;
                        } finally {
                            element = null;
                        }
                    }
                },

                response -> {
                    // is OK | is finished
                    // T | T  -> finished
                    // T | F  -> has more result
                    // F | T  -> busy
                    // F | F  -> error
                    var ok = response.getIsOk();
                    var finished = response.getIsFinished();

                    if (ok & finished) {
                        return ResultState.FINISHED;
                    }

                    if (ok & !finished) {
                        return ResultState.IDLE;
                    }

                    if (finished) {
                        return ResultState.ERROR.setMessage("server is busy");
                    }
                    return ResultState.ERROR.setMessage(response.getMessage());
                }

        );

        var reqStream = stub.query(observer);
        observer.setWatcherQueryId(request.getQueryId() + '-' + address);
        observer.setRequestSender(r -> reqStream.onNext(request));
        observer.setTransferComplete(r -> reqStream.onCompleted());
        observer.setTimeout(this.timeout);

        var itr = new StreamKvIterator<>(b -> observer.clear(), observer::consume);
        observer.sendRequest();
        return itr;
    }

    private static <E> boolean isEmpty(Collection<E> c) {
        return c == null || c.isEmpty();
    }

    // return node addr -> query proto
    // Only split the id scan, and broadcast the rest to all stores
    private List<Tuple2<String, QueryRequest.Builder>> getNodeTasks(StoreQueryParam query) throws
                                                                                           PDException {
        var graph = query.getGraph();
        var stores = this.nodePartitioner.getStores(graph);

        if (stores.isEmpty()) {
            log.warn("no stores found, query: {}", query);
        }

        Map<String, QueryRequest.Builder> tasks = new HashMap<>();

        if (query.getQueryType() == StoreQueryType.PRIMARY_SCAN) {
            // The primary operation is to split the query parameters, primarily focusing on the
            // id scan
            for (var param : query.getQueryParam()) {
                if (param.getCode() != -1) {
                    var addr = this.nodePartitioner.partition(graph, param.getCode());
                    if (!tasks.containsKey(addr)) {
                        tasks.put(addr, fromQuery(query));
                    }
                    tasks.get(addr).addScanTypeParam(fromParam(query.getTable(), param));
                } else {
                    for (String addr : stores) {
                        if (!tasks.containsKey(addr)) {
                            tasks.put(addr, fromQuery(query));
                        }
                        tasks.get(addr).addScanTypeParam(fromParam(query.getTable(), param));
                    }
                }
            }
        } else {
            for (String addr : stores) {
                tasks.computeIfAbsent(addr, t -> fromQuery(query));
            }
        }

        if (filterStore.get() != null) {
            String filterStoreStr = filterStore.get();
            return tasks.containsKey(filterStoreStr) ?
                   List.of(Tuple2.of(filterStoreStr, tasks.get(filterStoreStr))) : List.of();
        }

        return tasks.entrySet().stream()
                    .map(entry -> Tuple2.of(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList());
    }

    /**
     * Build QueryRequest.Builder object from query param
     *
     * @param query query param
     * @return QueryRequest.Builder
     */
    private static QueryRequest.Builder fromQuery(StoreQueryParam query) {
        var builder = QueryRequest.newBuilder()
                                  .setQueryId(query.getQueryId())
                                  .setGraph(query.getGraph())
                                  .setTable(query.getTable())
                                  .addAllFunctions(getAggregationProto(query.getFuncList()))
                                  .addAllProperty(idToBytes(query.getProperties().getPropertyIds()))
                                  .setNullProperty(query.getProperties().isEmptyId())
                                  .addAllGroupBy(idToBytes(query.getGroupBy()))
                                  .addAllOrderBy(idToBytes(query.getOrderBy()))
                                  .addAllHaving(getOrCreate(query.getHaving()))
                                  .setScanType(ScanType.forNumber(query.getQueryType().ordinal()))
                                  // .addAllScanTypeParam(fromParams(query.getTable(), query
                                  // .getQueryParam()))
                                  .setDedupOption(
                                          DeDupOption.forNumber(query.getDedupOption().ordinal()))
                                  .setOffset(query.getOffset())
                                  .addAllOlapProperty(idToBytes(query.getOlapProperties()))
                                  .setLoadPropertyFromIndex(query.isLoadPropertyFromIndex())
                                  .setGroupBySchemaLabel(query.isGroupBySchemaLabel());

        if (query.getSortOrder() != StoreQueryParam.SORT_ORDER.STRICT_ORDER) {
            builder.setSortOrder(query.getSortOrder() == StoreQueryParam.SORT_ORDER.ASC);
        }

        // When a count doesn't require a reverse lookup (no deduplication or union needed),
        // change it to NO_SCAN. Each result set will have a size of 1 (all are index scans)
        if (query.getQueryType() == StoreQueryType.INDEX_SCAN
            && query.getDedupOption() == StoreQueryParam.DEDUP_OPTION.NONE) {
            if (!isEmpty(query.getFuncList()) &&
                query.getFuncList().stream().allMatch(f -> f.getFunctionType() ==
                                                           AggregationFunctionParam.AggregationFunctionType.COUNT) &&
                query.getConditionQuery() == null) {
                if (query.getIndexes().stream()
                         .allMatch(i -> i.size() == 1 && i.get(0).isIndexScan())) {
                    log.info("trans query id {} from INDEX_SCAN to NO_SCAN", query.getQueryId());
                    builder.setScanType(ScanType.NO_SCAN);
                }
            }
        }

        if (query.getConditionQuery() != null) {
            builder.setCondition(ByteString.copyFrom(query.getConditionQuery().bytes()));
        }

        if (query.getPosition() != null) {
            builder.setPosition(ByteString.copyFrom(query.getPosition()));
        }

        // sample and limit are set to default when has function list.
        builder.setSampleFactor(isEmpty(query.getFuncList()) ? query.getSampleFactor() : 1.0);
        builder.setLimit(isEmpty(query.getFuncList()) ? query.getLimit() : 0);

        if (query.getIndexes() != null) {
            builder.addAllIndexes(fromIndex(query.getIndexes()));
        }

        builder.setCheckTtl(query.isCheckTTL());

        return builder;
    }

    private static ScanTypeParam fromParam(String table, QueryTypeParam param) {
        var builder = ScanTypeParam.newBuilder()
                                   .setKeyStart(ByteString.copyFrom(param.getStart()))
                                   .setScanBoundary(param.getBoundary())
                                   .setIsPrefix(param.isPrefix())
                                   .setIsSecondaryIndex(param.isSecondaryIndex());

        if (param.getEnd() != null) {
            builder.setKeyEnd(ByteString.copyFrom(param.getEnd()));
        }

        if (param.isIdScan() && param.getCode() == -1) {
            builder.setCode(
                    PartitionUtils.calcHashcode(KeyUtil.getOwnerKey(table, param.getStart())));
        } else {
            builder.setCode(param.getCode());
        }

        if (param.getIdPrefix() != null) {
            builder.setIdPrefix(ByteString.copyFrom(param.getIdPrefix()));
        }
        return builder.build();
    }

    private static List<ScanTypeParam> fromParams(String table, List<QueryTypeParam> params) {
        if (isEmpty(params)) {
            return new ArrayList<>();
        }
        return params.stream().map(p -> fromParam(table, p)).collect(Collectors.toList());
    }

    private static List<Index> fromIndex(List<List<QueryTypeParam>> indexes) {
        return indexes.stream()
                      .map(x -> Index.newBuilder().addAllParams(fromParams("", x)).build())
                      .collect(Collectors.toList());
    }

    private static List<AggregateFunc> getAggregationProto(
            List<AggregationFunctionParam> aggParams) {
        if (isEmpty(aggParams)) {
            return new ArrayList<>();
        }
        return aggParams.stream().map(param -> {

            var builder = AggregateFunc.newBuilder();

            builder.setFuncType(AggregationType.forNumber(param.getFunctionType().ordinal()));

            if (param.getField() != null) {
                builder.setField(idToBytes(param.getField()));
            }

            if (param.getFieldType() != null) {
                builder.setType(param.getFieldType().getGenericType());
            }
            return builder.build();
        }).collect(Collectors.toList());
    }

    private static List<ByteString> idToBytes(List<Id> ids) {
        if (isEmpty(ids)) {
            return new ArrayList<>();
        }

        return ids.stream().map(QueryExecutor::idToBytes).collect(Collectors.toList());
    }

    public static ByteString idToBytes(Id id) {
        BytesBuffer buffer = BytesBuffer.allocate(2);
        buffer.writeId(id);
        return ByteString.copyFrom(buffer.bytes());
    }

    private static <E> List<E> getOrCreate(List<E> list) {
        if (list != null) {
            return list;
        }
        return new ArrayList<>();
    }

    private BaseElement fromKv(String table, Kv kv, boolean isAgg) {
        if (isAgg) {
            return KvElement.of(KvSerializer.fromBytes(kv.getKey().toByteArray()),
                                KvSerializer.fromObjectBytes(kv.getValue().toByteArray()));
        }

        var backendColumn =
                BackendColumn.of(kv.getKey().toByteArray(), kv.getValue().toByteArray());
        try {
            if (IN_EDGE_TABLE.equals(table) || OUT_EDGE_TABLE.equals(table)) {
                return serializer.parseEdge(this.supplier, backendColumn, null, true);
            }
            return serializer.parseVertex(this.supplier, backendColumn, null);
        } catch (Exception e) {
            log.error("parse element error,", e);
            return null;
        }
    }

    /**
     * execute plan is empty, simple query
     *
     * @param param query param
     * @return true if id simple scan query
     */
    private boolean isSimpleQuery(StoreQueryParam param) {
        if (param.getQueryType() == StoreQueryType.PRIMARY_SCAN && !param.isCheckTTL() &&
            param.getLimit() == 0) {
            // all id scan:
            if (param.getQueryParam().stream().allMatch(QueryTypeParam::isIdScan)) {
                return isEmpty(param.getFuncList()) &&
                       isEmpty(param.getOrderBy()) &&
                       isEmpty(param.getGroupBy()) &&
                       isEmpty(param.getOlapProperties()) &&
                       !param.getProperties().needSerialize() &&
                       param.getConditionQuery() == null &&
                       param.getSampleFactor() == 1.0;
            }
        }

        return false;
    }

    /**
     * Judge if it is a simple agg query
     *
     * @param param query param
     * @return true if it is a simple agg query, false if not
     */
    private boolean isSimpleCountQuery(StoreQueryParam param) {
        if (param.getQueryType() == StoreQueryType.TABLE_SCAN && (!isEmpty(param.getFuncList()))) {
            return param.getFuncList()
                        .stream()
                        .allMatch(f -> f.getFunctionType() ==
                                       AggregationFunctionParam.AggregationFunctionType.COUNT)
                   && isEmpty(param.getGroupBy())
                   && isEmpty(param.getOrderBy())
                   && param.getConditionQuery() == null
                   && !param.isGroupBySchemaLabel()
                   && !param.isCheckTTL();
        }

        return false;
    }

    /**
     * Get simple iterator from query param
     *
     * @param query query param
     * @return List contains BaseElement
     * @throws PDException if failed
     */
    private List<HgKvIterator<BaseElement>> getSimpleIterator(StoreQueryParam query) throws
                                                                                     PDException {

        return getNodeTasks(query).parallelStream()
                                  .map(entry -> {
                                      var stub = client.getQueryServiceBlockingStub(entry.getV1());
                                      var response = stub.query0(entry.getV2().build());
                                      if (!response.getIsOk()) {
                                          throw new RuntimeException(response.getMessage());
                                      }

                                      var data = response.getDataList().iterator();
                                      return new HgKvIterator<BaseElement>() {
                                          @Override
                                          public boolean hasNext() {
                                              return data.hasNext();
                                          }

                                          @Override
                                          public BaseElement next() {
                                              return fromKv(query.getTable(), data.next(), false);
                                          }
                                      };
                                  })
                                  .collect(Collectors.toList());
    }

    /**
     * Get an iterator for the count of elements in the query result
     *
     * @param query query param
     * @return List contains count iterator
     * @throws PDException if failed
     */
    private List<HgKvIterator<BaseElement>> getCountIterator(StoreQueryParam query) throws
                                                                                    PDException {

        return getNodeTasks(query).parallelStream()
                                  .map(entry -> {
                                      var stub = client.getQueryServiceBlockingStub(entry.getV1())
                                                       .withDeadlineAfter(3600, TimeUnit.SECONDS);
                                      var response = stub.count(entry.getV2().build());
                                      if (!response.getIsOk()) {
                                          throw new RuntimeException(response.getMessage());
                                      }

                                      var data = response.getDataList().iterator();
                                      return new HgKvIterator<BaseElement>() {
                                          @Override
                                          public boolean hasNext() {
                                              return data.hasNext();
                                          }

                                          @Override
                                          public BaseElement next() {
                                              return fromKv(query.getTable(), data.next(), true);
                                          }
                                      };
                                  })
                                  .collect(Collectors.toList());
    }
}
