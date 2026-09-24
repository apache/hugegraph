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

import static org.apache.hugegraph.store.business.BusinessHandlerImpl.getGraphSupplier;
import static org.apache.hugegraph.store.constant.HugeServerTables.OLAP_TABLE;
import static org.apache.hugegraph.store.constant.HugeServerTables.TASK_TABLE;
import static org.apache.hugegraph.store.constant.HugeServerTables.VERTEX_TABLE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdUtil;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.serializer.BinaryElementSerializer;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.grpc.query.AggregationType;
import org.apache.hugegraph.store.grpc.query.DeDupOption;
import org.apache.hugegraph.store.grpc.query.QueryRequest;
import org.apache.hugegraph.store.grpc.query.ScanType;
import org.apache.hugegraph.store.grpc.query.ScanTypeParam;
import org.apache.hugegraph.store.node.grpc.EmptyIterator;
import org.apache.hugegraph.store.node.grpc.query.model.QueryPlan;
import org.apache.hugegraph.store.query.QueryTypeParam;
import org.apache.hugegraph.store.query.Tuple2;
import org.apache.hugegraph.store.query.func.AggregationFunction;
import org.apache.hugegraph.store.query.func.AggregationFunctions;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.structure.BaseVertex;

import com.google.protobuf.ByteString;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryUtil {

    public static final List<Object> EMPTY_AGG_KEY = new ArrayList<>();

    private static final Integer TOP_LIMIT = 10000;

    private static BusinessHandler handler;

    private static final BinaryElementSerializer serializer = new BinaryElementSerializer();

    private static final Set<String> vertexTables =
            new HashSet<>(List.of(VERTEX_TABLE, OLAP_TABLE, TASK_TABLE));

    /**
     * Requires semantic and sequential relationships
     *
     * @param request query request
     * @return query plan
     */
    public static QueryPlan buildPlan(QueryRequest request) {
        QueryPlan plan = new QueryPlan();

        if (request.getSampleFactor() == 0.0) {
            // No sampling at all
            plan.addStage(QueryStages.ofStopStage());
            return plan;
        }

        if (request.getSampleFactor() < 1.0) {
            var sampleStage = QueryStages.ofSampleStage();
            sampleStage.init(request.getSampleFactor());
            plan.addStage(sampleStage);
        }

        // only count agg. fast-forward
        if (isOnlyCountAggregationFunction(request)) {
            var simple = QueryStages.ofSimpleCountStage();
            simple.init(request.getFunctionsList().size());
            plan.addStage(simple);
        } else {
            if (request.getCheckTtl()) {
                var ttl = QueryStages.ofTtlCheckStage();
                ttl.init(isVertex(request.getTable()));
                plan.addStage(ttl);
            }

            // when to de-serialization ?
            if (needDeserialize(request)) {
                var deserializeStage = QueryStages.ofDeserializationStage();
                deserializeStage.init(request.getTable(),
                                      getGraphSupplier(request.getGraph()));
                plan.addStage(deserializeStage);
            }

            if (!isEmpty(request.getOlapPropertyList())) {
                var olap = QueryStages.ofOlapStage();
                olap.init(request.getGraph(), request.getTable(), request.getOlapPropertyList());
                plan.addStage(olap);
            }

            if (!request.getCondition().isEmpty()) {
                var filterStage = QueryStages.ofFilterStage();
                filterStage.init(request.getCondition().toByteArray());
                plan.addStage(filterStage);
            }

            if (!isEmpty(request.getFunctionsList())) {
                var extractAggField = QueryStages.ofExtractAggFieldStage();
                List<ByteString> fields = new ArrayList<>();
                for (var func : request.getFunctionsList()) {
                    if (func.getFuncType() == AggregationType.COUNT) {
                        fields.add(null);
                    } else {
                        fields.add(func.getField());
                    }
                }

                extractAggField.init(request.getGroupByList(), fields,
                                     request.getGroupBySchemaLabel(), isVertex(request.getTable()));
                plan.addStage(extractAggField);
            }
        }

        // aggregation
        if (!isEmpty(request.getFunctionsList())) {
            var agg = QueryStages.ofAggStage();
            List<Tuple2<AggregationType, String>> funcMetas = new ArrayList<>();
            for (var func : request.getFunctionsList()) {
                funcMetas.add(new Tuple2<>(func.getFuncType(), func.getType()));
            }
            agg.init(funcMetas);
            plan.addStage(agg);
        }

        if (!isEmpty(request.getPropertyList()) || request.getNullProperty()) {
            var selector = QueryStages.ofProjectionStage();
            selector.init(request.getPropertyList(), request.getNullProperty());
            plan.addStage(selector);
        }

        // sort + limit -> top operation
        if (canOptimiseToTop(request)) {
            var topStage = QueryStages.ofTopStage();
            topStage.init(request.getLimit(), request.getOrderByList(), request.getSortOrder());
            plan.addStage(topStage);
        } else {
            if (!isEmpty(request.getOrderByList())) {
                var order = QueryStages.ofOrderByStage();
                order.init(request.getOrderByList(), request.getGroupByList(),
                           !isEmpty(request.getFunctionsList()),
                           request.getSortOrder());
                plan.addStage(order);
            }

            if (request.getLimit() > 0) {
                var limit = QueryStages.ofLimitStage();
                limit.init(request.getLimit());
                plan.addStage(limit);
            }
        }

        log.debug("query id: {} ,build plan result: {}", request.getQueryId(), plan);
        return plan;
    }

    private static boolean isOnlyCountAggregationFunction(QueryRequest request) {
        return !isEmpty(request.getFunctionsList()) &&
               request.getFunctionsList().stream()
                      .allMatch(f -> f.getFuncType() == AggregationType.COUNT) &&
               isEmpty(request.getGroupByList()) && request.getCondition().isEmpty()
               && !request.getGroupBySchemaLabel();
    }

    private static boolean canOptimiseToTop(QueryRequest request) {
        return !isEmpty(request.getOrderByList()) && request.getLimit() < TOP_LIMIT &&
               request.getLimit() > 0;
    }

    /**
     * Determine whether deserialization is needed.
     *
     * @param request query request object.
     * @return true if deserialization is needed, false otherwise.
     */
    private static boolean needDeserialize(QueryRequest request) {
        return !isEmpty(request.getOrderByList()) || !isEmpty(request.getPropertyList())
               || !request.getCondition().isEmpty() || !isEmpty(request.getFunctionsList())
                                                       && !request.getGroupBySchemaLabel();
    }

    /**
     * Get a scan iterator.
     *
     * @param request query request object.
     * @return query iterator.
     */
    public static ScanIterator getIterator(QueryRequest request) {

        var handler = new QueryUtil().getHandler();

        switch (request.getScanType()) {
            case TABLE_SCAN:
                return handler.scanAll(request.getGraph(), request.getTable());

            case PRIMARY_SCAN:
                // id scan
                // todo: For multiple primary key queries + exact deduplication + limit scenarios, consider using map for partial exact processing
                return handler.scan(request.getGraph(), request.getTable(),
                                    toQTP(request.getScanTypeParamList()),
                                    request.getDedupOption());

            case NO_SCAN:
                // no scan - no need for reverse lookup:
                // 1. Can be parsed directly, no reverse lookup needed. 2. No deduplication needed, get count directly
                return handler.scanIndex(request.getGraph(),
                                         request.getIndexesList().stream()
                                                .map(x -> toQTP(x.getParamsList()))
                                                .collect(Collectors.toList()),
                                         request.getDedupOption(),
                                         request.getLoadPropertyFromIndex(),
                                         request.getCheckTtl());

            case INDEX_SCAN:
                return handler.scanIndex(request.getGraph(),
                                         request.getTable(),
                                         request.getIndexesList().stream()
                                                .map(x -> toQTP(x.getParamsList()))
                                                .collect(Collectors.toList()),
                                         request.getDedupOption(),
                                         true,
                                         needIndexTransKey(request),
                                         request.getCheckTtl(),
                                         request.getLimit());
            default:
                break;
        }

        return new EmptyIterator();
    }

    /**
     * 1. no scan/ no need to go back to table
     * 2. only one index,
     *
     * @param request
     * @return
     */
    private static boolean needIndexTransKey(QueryRequest request) {
        if (request.getScanType() == ScanType.NO_SCAN) {
            return !isOnlyCountAggregationFunction(request) &&
                   request.getDedupOption() == DeDupOption.NONE;
        }
        return true;
    }

    private static List<QueryTypeParam> toQTP(List<ScanTypeParam> range) {
        return range.stream().map(QueryUtil::fromScanTypeParam).collect(Collectors.toList());
    }

    private static QueryTypeParam fromScanTypeParam(ScanTypeParam param) {
        return new QueryTypeParam(param.getKeyStart().toByteArray(),
                                  param.getKeyEnd().toByteArray(),
                                  param.getScanBoundary(),
                                  param.getIsPrefix(),
                                  param.getIsSecondaryIndex(),
                                  param.getCode(),
                                  param.getIdPrefix().toByteArray());
    }

    public static <E> boolean isEmpty(Collection<E> c) {
        return c == null || c.size() == 0;
    }

    public static BaseElement parseEntry(HugeGraphSupplier graph,
                                         BackendColumn column,
                                         boolean isVertex) {
        if (isVertex) {
            return serializer.parseVertex(graph, column, null);
        } else {
            return serializer.parseEdge(graph, column, null, true);
        }
    }

    public static BaseElement parseOlap(BackendColumn column, BaseVertex vertex) {
        return serializer.parseVertexOlap(null, column, vertex);
    }

    /**
     * One-time vertex serialization - deserialization
     *
     * @param vertexColumn vertex
     * @param olap         olap vertex
     * @return new vertex
     */
    public static BackendColumn combineColumn(BackendColumn vertexColumn,
                                              List<BackendColumn> olap) {
        return serializer.mergeCols(vertexColumn, olap.toArray(new BackendColumn[0]));
    }

    public static AggregationFunction createFunc(AggregationType funcType, String genericType) {
        AggregationFunction func = null;
        switch (funcType) {
            case AVG:
                func = new AggregationFunctions.AvgFunction(
                        getAggregationBufferSupplier(genericType));
                break;
            case SUM:
                func = new AggregationFunctions.SumFunction(
                        getAggregationBufferSupplier(genericType));
                break;
            case MAX:
                func = new AggregationFunctions.MaxFunction(
                        getAggregationBufferSupplier(genericType));
                break;
            case MIN:
                func = new AggregationFunctions.MinFunction(
                        getAggregationBufferSupplier(genericType));
                break;
            case COUNT:
                func = new AggregationFunctions.CountFunction();
                break;
            default:
                break;
        }
        return func;
    }

    public static Supplier getAggregationBufferSupplier(String genericType) {
        return AggregationFunctions.getAggregationBufferSupplier(genericType);
    }

    public static List<Id> fromStringBytes(List<ByteString> list) {
        return list.stream()
                   .map(id -> id == null ? null : IdUtil.fromBytes(id.toByteArray()))
                   .collect(Collectors.toList());
    }

    /**
     * Determine whether the table is a vertex table
     *
     * @param table table name to be determined
     * @return true if it is a vertex table, false otherwise.
     */
    public static boolean isVertex(String table) {
        return vertexTables.contains(table);
    }

    public static Long getLabelId(RocksDBSession.BackendColumn column, boolean isVertex) {
        var id = serializer.parseLabelFromCol(BackendColumn.of(column.name, column.value),
                                              isVertex);
        return id.asLong();
    }

    public BusinessHandler getHandler() {
        if (handler == null) {
            synchronized (this) {
                if (handler == null) {
                    handler = HgStoreEngine.getInstance().getBusinessHandler();
                }
            }
        }
        return handler;
    }

}
