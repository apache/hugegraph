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

package org.apache.hugegraph.store.business;

import static org.apache.hugegraph.store.business.MultiPartitionIterator.EMPTY_BYTES;
import static org.apache.hugegraph.store.constant.HugeServerTables.INDEX_TABLE;
import static org.apache.hugegraph.store.constant.HugeServerTables.IN_EDGE_TABLE;
import static org.apache.hugegraph.store.constant.HugeServerTables.OUT_EDGE_TABLE;
import static org.apache.hugegraph.store.constant.HugeServerTables.VERTEX_TABLE;
import static org.apache.hugegraph.store.util.HgStoreConst.SCAN_ALL_PARTITIONS_ID;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.SchemaGraph;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.pulse.CleanType;
import org.apache.hugegraph.rocksdb.access.DBStoreException;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.RocksdbChangedListener;
import org.apache.hugegraph.rocksdb.access.RocksDBOptions;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.apache.hugegraph.serializer.BinaryElementSerializer;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.serializer.DirectBinarySerializer;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.business.itrv2.BatchGetIterator;
import org.apache.hugegraph.store.business.itrv2.InAccurateIntersectionIterator;
import org.apache.hugegraph.store.business.itrv2.InAccurateUnionFilterIterator;
import org.apache.hugegraph.store.business.itrv2.IntersectionFilterIterator;
import org.apache.hugegraph.store.business.itrv2.IntersectionWrapper;
import org.apache.hugegraph.store.business.itrv2.MapJoinIterator;
import org.apache.hugegraph.store.business.itrv2.MapLimitIterator;
import org.apache.hugegraph.store.business.itrv2.MapUnionIterator;
import org.apache.hugegraph.store.business.itrv2.MultiListIterator;
import org.apache.hugegraph.store.business.itrv2.TypeTransIterator;
import org.apache.hugegraph.store.business.itrv2.UnionFilterIterator;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.apache.hugegraph.store.cmd.HgCmdClient;
import org.apache.hugegraph.store.cmd.request.BlankTaskRequest;
import org.apache.hugegraph.store.cmd.request.CleanDataRequest;
import org.apache.hugegraph.store.consts.PoolNames;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.Request;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.ScanType;
import org.apache.hugegraph.store.grpc.query.DeDupOption;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.meta.asynctask.AsyncTaskState;
import org.apache.hugegraph.store.meta.asynctask.CleanTask;
import org.apache.hugegraph.store.metric.HgStoreMetric;
import org.apache.hugegraph.store.pd.DefaultPdProvider;
import org.apache.hugegraph.store.pd.PdProvider;
import org.apache.hugegraph.store.query.QueryTypeParam;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftOperation;
import org.apache.hugegraph.store.term.Bits;
import org.apache.hugegraph.store.term.HgPair;
import org.apache.hugegraph.store.util.ExecutorUtil;
import org.apache.hugegraph.store.util.HgStoreException;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.util.Bytes;
import org.rocksdb.Cache;
import org.rocksdb.MemoryUsageType;

import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.ByteString;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BusinessHandlerImpl implements BusinessHandler {

    private static final Map<String, HugeGraphSupplier> GRAPH_SUPPLIER_CACHE =
            new ConcurrentHashMap<>();
    private static final int batchSize = 10000;
    private static Long indexDataSize = 50 * 1024L;
    private static final RocksDBFactory factory = RocksDBFactory.getInstance();
    private static final HashMap<ScanType, String> tableMapping = new HashMap<>() {{
        put(ScanType.SCAN_VERTEX, VERTEX_TABLE);
        put(ScanType.SCAN_EDGE, OUT_EDGE_TABLE);
    }};
    private static final Map<Integer, String> dbNames = new ConcurrentHashMap<>();
    private static HugeGraphSupplier mockGraphSupplier = null;
    private static final ConcurrentMap<String, AtomicInteger> pathLock = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Integer, AtomicInteger> compactionState =
            new ConcurrentHashMap<>();
    // Default core thread count
    private static final int compactionThreadCount = 64;
    private static final int compactionMaxThreadCount = 256;
    // Max size of compaction queue
    private static final int compactionQueueSize = 1000;
    private static final ThreadPoolExecutor compactionPool =
            ExecutorUtil.createExecutor(PoolNames.COMPACT, compactionThreadCount,
                                        compactionMaxThreadCount, compactionQueueSize);
    private static final int timeoutMillis = 6 * 3600 * 1000;
    private final BinaryElementSerializer serializer = BinaryElementSerializer.getInstance();
    private final DirectBinarySerializer directBinarySerializer = new DirectBinarySerializer();
    private final PartitionManager partitionManager;
    private final PdProvider provider;
    private final InnerKeyCreator keyCreator;
    private final Semaphore semaphore = new Semaphore(1);

    public BusinessHandlerImpl(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
        this.provider = partitionManager.getPdProvider();
        this.keyCreator = new InnerKeyCreator(this);

        factory.addRocksdbChangedListener(new RocksdbChangedListener() {
            @Override
            public void onDBDeleteBegin(String dbName, String filePath) {
                partitionManager.getDeletedFileManager().addDeletedFile(filePath);
            }

            @Override
            public void onDBDeleted(String dbName, String filePath) {
                partitionManager.getDeletedFileManager().removeDeletedFile(filePath);
            }

            @Override
            public void onDBSessionReleased(RocksDBSession dbSession) {

            }
        });
    }

    public static HugeConfig initRocksdb(Map<String, Object> rocksdbConfig,
                                         RocksdbChangedListener listener) {
        // Register rocksdb configuration
        OptionSpace.register("rocksdb", "org.apache.hugegraph.rocksdb.access.RocksDBOptions");
        RocksDBOptions.instance();
        HugeConfig hConfig = new HugeConfig(rocksdbConfig);
        factory.setHugeConfig(hConfig);
        if (listener != null) {
            factory.addRocksdbChangedListener(listener);
        }
        return hConfig;
    }

    public static void setIndexDataSize(long dataSize) {
        if (dataSize > 0) {
            indexDataSize = dataSize;
        }
    }

    /**
     * FNV hash method
     *
     * @param key hash input
     * @return a long hash value
     */
    public static Long fnvHash(byte[] key) {
        long rv = 0xcbf29ce484222325L;
        for (var b : key) {
            rv ^= b;
            rv *= 0x100000001b3L;
        }
        return rv;
    }

    public static String getDbName(int partId) {
        String dbName = dbNames.get(partId);
        if (dbName == null) {
            dbName = String.format("%05d", partId);
            dbNames.put(partId, dbName);
        }
        // Each partition corresponds to a rocksdb instance, so the rocksdb instance name is partId.
        return dbName;
    }

    public static ThreadPoolExecutor getCompactionPool() {
        return compactionPool;
    }

    /**
     * used for testing, setting fake graph supplier
     *
     * @param supplier
     */
    public static void setMockGraphSupplier(HugeGraphSupplier supplier) {
        mockGraphSupplier = supplier;
    }

    public static HugeGraphSupplier getGraphSupplier(String graph) {
        if (mockGraphSupplier != null) {
            return mockGraphSupplier;
        }

        if (GRAPH_SUPPLIER_CACHE.get(graph) == null) {
            synchronized (BusinessHandlerImpl.class) {
                if (GRAPH_SUPPLIER_CACHE.get(graph) == null) {
                    var config =
                            PDConfig.of(HgStoreEngine.getInstance().getOption().getPdAddress());
                    config.setAuthority(DefaultPdProvider.name, DefaultPdProvider.authority);
                    String[] parts = graph.split("/");
                    assert (parts.length > 1);
                    GRAPH_SUPPLIER_CACHE.put(graph, new SchemaGraph(parts[0], parts[1], config));
                }
            }
        }

        return GRAPH_SUPPLIER_CACHE.get(graph);
    }

    @Override
    public void doPut(String graph, int code, String table, byte[] key, byte[] value) throws
                                                                                      HgStoreException {

        int partId = provider.getPartitionByCode(graph, code).getId();
        try (RocksDBSession dbSession = getSession(graph, table, partId)) {
            SessionOperator op = dbSession.sessionOp();
            try {
                op.prepare();
                byte[] targetKey = keyCreator.getKeyOrCreate(partId, graph, code, key);
                op.put(table, targetKey, value);
                op.commit();
            } catch (Exception e) {
                log.error("Graph " + graph + " doPut exception", e);
                op.rollback();
                throw new HgStoreException(HgStoreException.EC_RKDB_DOPUT_FAIL, e.toString());
            }
        }
    }

    @Override
    public byte[] doGet(String graph, int code, String table, byte[] key) throws HgStoreException {
        int partId = provider.getPartitionByCode(graph, code).getId();
        if (!partitionManager.hasPartition(graph, partId)) {
            return null;
        }

        try (RocksDBSession dbSession = getSession(graph, table, partId)) {
            byte[] targetKey = keyCreator.getKey(partId, graph, code, key);
            return dbSession.sessionOp().get(table, targetKey);
        } catch (Exception e) {
            log.error("Graph " + graph + " doGet exception", e);
            throw new HgStoreException(HgStoreException.EC_RKDB_DOGET_FAIL, e.toString());
        }
    }

    @Override
    public ScanIterator scanAll(String graph, String table) throws HgStoreException {
        List<Integer> ids = this.getLeaderPartitionIds(graph);

        BiFunction<Integer, byte[], ScanIterator> function = (id, position) -> {
            try (RocksDBSession dbSession = getSession(graph, table, id)) {
                return new InnerKeyFilter(dbSession.sessionOp().scan(table, position == null ?
                                                                            keyCreator.getStartKey(
                                                                                    id, graph) :
                                                                            keyCreator.getStartKey(
                                                                                    id, graph,
                                                                                    position),
                                                                     keyCreator.getEndKey(id,
                                                                                          graph),
                                                                     ScanIterator.Trait.SCAN_LT_END));
            }
        };
        return MultiPartitionIterator.of(ids, function);
    }

    @Override
    public ScanIterator scanAll(String graph, String table, byte[] query) throws HgStoreException {
        return scanAll(graph, table);
    }

    @Override
    public ScanIterator scan(String graph, int code, String table, byte[] start, byte[] end,
                             int scanType) throws HgStoreException {
        List<Integer> ids;
        if (code == SCAN_ALL_PARTITIONS_ID) {
            ids = this.getLeaderPartitionIds(graph);
        } else {
            ids = new ArrayList<>();
            ids.add(partitionManager.getPartitionIdByCode(graph, code));
        }
        BiFunction<Integer, byte[], ScanIterator> function = (id, position) -> {
            byte[] endKey;
            int type;
            if (ArrayUtils.isEmpty(end)) {
                endKey = keyCreator.getEndKey(id, graph);
                type = ScanIterator.Trait.SCAN_LT_END;
            } else {
                endKey = keyCreator.getEndKey(id, graph, end);
                type = scanType;
            }
            try (RocksDBSession dbSession = getSession(graph, table, id)) {
                return new InnerKeyFilter(dbSession.sessionOp().scan(table,
                                                                     keyCreator.getStartKey(id,
                                                                                            graph,
                                                                                            toPosition(
                                                                                                    start,
                                                                                                    position)),
                                                                     endKey, type));
            }
        };
        return MultiPartitionIterator.of(ids, function);
    }

    /**
     * Merge ID scans into a single list, and invoke the scan function for others
     *
     * @param graph       graph
     * @param table       table
     * @param params      primary scan params
     * @param dedupOption de-duplicate option, 0: none, 1: none-exactly 2: exactly
     * @return an iterator
     * @throws HgStoreException when get db session fail
     */
    @Override
    public ScanIterator scan(String graph, String table, List<QueryTypeParam> params,
                             DeDupOption dedupOption) throws HgStoreException {

        var iterator = scan(graph, table, params);

        if (!(iterator instanceof MultiListIterator)) {
            return iterator;
        }

        switch (dedupOption) {
            case NONE:
                return iterator;
            case DEDUP:
                return new InAccurateUnionFilterIterator<>(iterator,
                                                           BusinessHandlerImpl::getColumnByteHash);
            case LIMIT_DEDUP:
                return new MapLimitIterator<>(iterator);
            case PRECISE_DEDUP:
                // todo: optimize?
                var wrapper =
                        new IntersectionWrapper<>(iterator, BusinessHandlerImpl::getColumnByteHash);
                wrapper.proc();
                // Scan again
                return new UnionFilterIterator<>(scan(graph, table, params), wrapper,
                                                 (o1, o2) -> Arrays.compare(o1.name, o2.name),
                                                 SortShuffleSerializer.ofBackendColumnSerializer());
            default:
                return null;
        }
    }

    private ScanIterator scan(String graph, String table, List<QueryTypeParam> params) throws
                                                                                       HgStoreException {
        // put id scan in to a single list
        var idList = params.stream().filter(QueryTypeParam::isIdScan).collect(Collectors.toList());

        var itr = new MultiListIterator();
        for (var param : params) {
            if (param.isPrefixScan()) {
                // prefix scan
                itr.addIterator(scanPrefix(graph, param.getCode(), table, param.getStart(),
                                           param.getBoundary()));
            } else if (param.isRangeScan()) {
                // ranged scan
                itr.addIterator(
                        scan(graph, param.getCode(), table, param.getStart(), param.getEnd(),
                             param.getBoundary()));
            }
        }

        if (!idList.isEmpty()) {
            itr.addIterator(new BatchGetIterator(idList.iterator(),
                                                 idParam -> doGet(graph, idParam.getCode(), table,
                                                                  idParam.getStart())));
        }

        return itr.getIterators().size() == 1 ? itr.getIterators().get(0) : itr;
    }

    /**
     * According to keyCode range return data, left closed right open.
     *
     * @param graph
     * @param table
     * @param codeFrom Starting code, including this value
     * @param codeTo   End code, does not include this value
     * @return
     * @throws HgStoreException
     */
    @Override
    public ScanIterator scan(String graph, String table, int codeFrom, int codeTo) throws
                                                                                   HgStoreException {

        List<Integer> ids = new ArrayList<>();
        ids.add(partitionManager.getPartitionIdByCode(graph, codeFrom));
        BiFunction<Integer, byte[], ScanIterator> function = (id, position) -> {
            try (RocksDBSession dbSession = getSession(graph, table, id)) {
                byte[] startKey;
                if (position != null) {
                    startKey = keyCreator.getStartKey(id, graph, position);
                } else {
                    startKey = keyCreator.getStartKey(id, graph);
                }
                byte[] endKey = keyCreator.getEndKey(id, graph);
                ScanIterator iterator = dbSession.sessionOp().scan(table, startKey, endKey,
                                                                   ScanIterator.Trait.SCAN_LT_END);
                return new InnerKeyFilter(iterator, codeFrom, codeTo);
            }
        };
        return MultiPartitionIterator.of(ids, function);
    }

    @Override
    public ScanIterator scan(String graph, int code, String table, byte[] start, byte[] end,
                             int scanType, byte[] conditionQuery) throws HgStoreException {
        ScanIterator it = null;
        if ((scanType & ScanIterator.Trait.SCAN_HASHCODE) == ScanIterator.Trait.SCAN_HASHCODE) {
            int codeFrom = Bits.toInt(start);
            int codeTo = Bits.toInt(end);
            it = scan(graph, table, codeFrom, codeTo);
        } else {
            it = scan(graph, code, table, start, end, scanType);
        }
        return it;
    }

    @Override
    public GraphStoreIterator scan(ScanPartitionRequest spr) throws HgStoreException {
        return new GraphStoreIterator(scanOriginal(spr), spr);
    }

    private ToLongFunction<BaseElement> getBaseElementHashFunction() {
        return value -> fnvHash(value.id().asBytes());
    }

    @Override
    public ScanIterator scanIndex(String graph, String table, List<List<QueryTypeParam>> params,
                                  DeDupOption dedupOption, boolean lookupBack, boolean transKey,
                                  boolean filterTTL, int limit) throws HgStoreException {

        ScanIterator result;

        boolean onlyPrimary =
                params.stream().allMatch(sub -> sub.size() == 1 && !sub.get(0).isIndexScan());

        boolean needLookup = lookupBack && !onlyPrimary;

        if (params.size() == 1) {
            // no union operation
            result = indexIntersection(graph, table, params.get(0), dedupOption, onlyPrimary,
                                       filterTTL, needLookup, limit);
        } else {
            // Multiple Index
            var sub = params.stream()
                            .map(p2 -> indexIntersection(graph, table, p2, dedupOption, onlyPrimary,
                                                         filterTTL, needLookup, limit))
                            .collect(Collectors.toList());

            switch (dedupOption) {
                case NONE:
                    result = new MultiListIterator(sub);
                    break;
                case DEDUP:
                    result = new InAccurateUnionFilterIterator<>(new MultiListIterator(sub),
                                                                 BusinessHandlerImpl::getColumnByteHash);
                    break;
                case LIMIT_DEDUP:
                    result = new MapLimitIterator<>(new MultiListIterator(sub));
                    break;
                case PRECISE_DEDUP:
                    if (limit > 0) {
                        // Map limit deduplication
                        result = new MapLimitIterator<RocksDBSession.BackendColumn>(
                                new MultiListIterator(sub));
                    } else {
                        // union operation
                        var fileSize = getQueryFileSize(graph, table, getLeaderPartitionIds(graph),
                                                        params);
                        if (fileSize < indexDataSize * params.size()) {
                            // using map
                            result = new MapUnionIterator<RocksDBSession.BackendColumn, String>(sub,
                                                                                                col -> Arrays.toString(
                                                                                                        col.name));
                        } else {
                            result = new MultiListIterator(sub);
                            var wrapper = new IntersectionWrapper<>(result,
                                                                    BusinessHandlerImpl::getColumnByteHash);
                            wrapper.proc();

                            var round2 = new MultiListIterator();
                            for (int i = 0; i < params.size(); i++) {
                                var itr = sub.get(i);
                                if (itr instanceof MapJoinIterator) {
                                    // It's in memory, no need to recalculate
                                    ((MapJoinIterator<?, ?>) itr).reset();
                                    round2.addIterator(itr);
                                } else {
                                    round2.addIterator(
                                            indexIntersection(graph, table, params.get(i),
                                                              dedupOption, onlyPrimary, filterTTL,
                                                              needLookup, limit));
                                }
                            }
                            result = new UnionFilterIterator<>(round2, wrapper,
                                                               (o1, o2) -> Arrays.compare(o1.name,
                                                                                          o2.name),
                                                               SortShuffleSerializer.ofBackendColumnSerializer());
                        }
                    }
                    break;
                default:
                    throw new HgStoreException("deduplication option not supported");
            }
        }

        if (needLookup) {
            // query the original table
            result =
                    new TypeTransIterator<RocksDBSession.BackendColumn,
                            RocksDBSession.BackendColumn>(
                            result, column -> {
                        if (column != null && column.name != null) {
                            // var id = KeyUtil.getOwnerKey(table, backendColumn.name);
                            var value =
                                    doGet(graph, PartitionUtils.calcHashcode(column.value), table,
                                          column.name);
                            if (value != null && value.length > 0) {
                                return RocksDBSession.BackendColumn.of(column.name, value);
                            }
                        }
                        return null;
                    }, "lookup-back-table");
        }
        return result;
    }

    /**
     * for no scan:
     * case 1: count case， multi param + no dedup + no transElement
     * case 2: transElement， one param + dedup + transElement
     */
    @Override
    public ScanIterator scanIndex(String graph, List<List<QueryTypeParam>> params,
                                  DeDupOption dedupOption, boolean transElement,
                                  boolean filterTTL) throws HgStoreException {
        // case 1
        if (!transElement) {
            if (params.size() == 1) {
                var param = params.get(0).get(0);
                if (param.isRangeIndexScan()) {
                    return scan(graph, param.getCode(), "g+index", param.getStart(), param.getEnd(),
                                param.getBoundary());
                } else {
                    return scanPrefix(graph, param.getCode(), "g+index", param.getStart(),
                                      param.getBoundary());
                }
            } else {
                // todo: change multiListIterator of MultiPartition to ? ,
                //  combine multi id?
                var result = new MultiListIterator();
                params.forEach(sub -> {
                    var param = sub.get(0);
                    if (param.isRangeIndexScan()) {
                        result.addIterator(scan(graph, param.getCode(), "g+index", param.getStart(),
                                                param.getEnd(), param.getBoundary()));
                    } else {
                        result.addIterator(
                                scanPrefix(graph, param.getCode(), "g+index", param.getStart(),
                                           param.getBoundary()));
                    }
                });
                return result;
            }
        }

        // case 2
        var param = params.get(0).get(0);
        var result = scanIndexToBaseElement(graph, param, filterTTL);

        switch (dedupOption) {
            case NONE:
                return result;
            case DEDUP:
                return new InAccurateUnionFilterIterator<>(result, getBaseElementHashFunction());
            case LIMIT_DEDUP:
                return new MapLimitIterator<>(result);
            case PRECISE_DEDUP:
                var wrapper = new IntersectionWrapper<>(result, getBaseElementHashFunction());
                wrapper.proc();
                return new UnionFilterIterator<>(scanIndexToBaseElement(graph, param, filterTTL),
                                                 wrapper,
                                                 (o1, o2) -> Arrays.compare(o1.id().asBytes(),
                                                                            o2.id().asBytes()),
                                                 SortShuffleSerializer.ofBaseElementSerializer());
            default:
                return null;
        }
    }

    public ScanIterator indexIntersection(String graph, String table, List<QueryTypeParam> params,
                                          DeDupOption dedupOption, boolean onlyPrimary,
                                          boolean filterTTL, boolean lookup, int limit) throws
                                                                                        HgStoreException {

        // Primary key queries do not require deduplication and only support a single primary key,
        // For other index queries, deduplication should be performed based on BackendColumn,
        // removing the value.
        if (params.size() == 1 && !params.get(0).isIndexScan()) {
            var iterator = scan(graph, table, params);
            // need to remove value and index to dedup
            return onlyPrimary ? iterator : new TypeTransIterator<>(iterator,
                                                                    (Function<RocksDBSession.BackendColumn, RocksDBSession.BackendColumn>) column -> {
                                                                        // todo: from key
                                                                        //  to owner key
                                                                        BaseElement element;
                                                                        try {
                                                                            if (IN_EDGE_TABLE.equals(
                                                                                    table) ||
                                                                                OUT_EDGE_TABLE.equals(
                                                                                        table)) {
                                                                                element =
                                                                                        serializer.parseEdge(
                                                                                                getGraphSupplier(
                                                                                                        graph),
                                                                                                BackendColumn.of(
                                                                                                        column.name,
                                                                                                        column.value),
                                                                                                null,
                                                                                                false);
                                                                            } else {
                                                                                element =
                                                                                        serializer.parseVertex(
                                                                                                getGraphSupplier(
                                                                                                        graph),
                                                                                                BackendColumn.of(
                                                                                                        column.name,
                                                                                                        column.value),
                                                                                                null);
                                                                            }
                                                                        } catch (Exception e) {
                                                                            log.error("parse " +
                                                                                      "element " +
                                                                                      "error, " +
                                                                                      "graph" +
                                                                                      " " +
                                                                                      "{}, table," +
                                                                                      " {}", graph,
                                                                                      table, e);
                                                                            return null;
                                                                        }
                                                                        // column.value =
                                                                        // KeyUtil
                                                                        // .idToBytes
                                                                        // (BinaryElementSerializer.ownerId
                                                                        // (element));
                                                                        column.value =
                                                                                BinaryElementSerializer.ownerId(
                                                                                                               element)
                                                                                                       .asBytes();
                                                                        return column;
                                                                    }, "replace-pk");
        }

        var iterators =
                params.stream().map(param -> scanIndexToElementId(graph, param, filterTTL, lookup))
                      .collect(Collectors.toList());

        // Reduce iterator hierarchy
        ScanIterator result =
                params.size() == 1 ? iterators.get(0) : new MultiListIterator(iterators);

        if (dedupOption == DeDupOption.NONE) {
            return result;
        } else if (dedupOption == DeDupOption.DEDUP) {
            return params.size() == 1 ? new InAccurateUnionFilterIterator<>(result,
                                                                            BusinessHandlerImpl::getColumnByteHash) :
                   new InAccurateIntersectionIterator<>(result,
                                                        BusinessHandlerImpl::getColumnByteHash);
        } else if (dedupOption == DeDupOption.PRECISE_DEDUP && limit > 0 ||
                   dedupOption == DeDupOption.LIMIT_DEDUP) {
            // Exact deduplication with limit using map-based deduplication
            return new MapLimitIterator<RocksDBSession.BackendColumn>(result);
        } else {
            // todo: single index need not to deduplication
            var ids = this.getLeaderPartitionIds(graph);
            var sizes = params.stream().map(param -> getQueryFileSize(graph, "g+v", ids, param))
                              .collect(Collectors.toList());

            log.debug("queries: {} ,sizes : {}", params, sizes);
            Long minSize = Long.MAX_VALUE;
            int loc = -1;
            for (int i = 0; i < sizes.size(); i++) {
                if (sizes.get(i) < minSize) {
                    minSize = sizes.get(i);
                    loc = i;
                }
            }

            if (minSize < indexDataSize) {
                return new MapJoinIterator<RocksDBSession.BackendColumn, String>(iterators, loc,
                                                                                 col -> Arrays.toString(
                                                                                         col.name));
            } else {
                var wrapper =
                        new IntersectionWrapper<>(result, BusinessHandlerImpl::getColumnByteHash,
                                                  true);
                wrapper.proc();

                var r2 = multiIndexIterator(graph, params, filterTTL, lookup);
                return params.size() == 1 ? new UnionFilterIterator<>(r2, wrapper,
                                                                      (o1, o2) -> Arrays.compare(
                                                                              o1.name, o2.name),
                                                                      SortShuffleSerializer.ofBackendColumnSerializer()) :
                       new IntersectionFilterIterator(r2, wrapper, params.size());
            }
        }
    }

    private long getQueryFileSize(String graph, String table, List<Integer> partitions,
                                  List<List<QueryTypeParam>> params) {
        long total = 0;
        for (var sub : params) {
            var size = sub.stream().map(param -> getQueryFileSize(graph,
                                                                  param.isIndexScan() ? "g+index" :
                                                                  table, partitions, param))
                          .min(Long::compareTo);
            total += size.get();
        }
        return total;
    }

    private long getQueryFileSize(String graph, String table, List<Integer> partitions,
                                  QueryTypeParam param) {
        long total = 0;
        for (int partId : partitions) {
            try (RocksDBSession dbSession = getSession(graph, partId)) {
                total += dbSession.getApproximateDataSize(table, param.getStart(), param.getEnd());
            }
        }
        return total;
    }

    private ScanIterator multiIndexIterator(String graph, List<QueryTypeParam> params,
                                            boolean filterTTL, boolean lookup) {
        var iterators =
                params.stream().map(param -> scanIndexToElementId(graph, param, filterTTL, lookup))
                      .collect(Collectors.toList());
        return params.size() == 1 ? iterators.get(0) : new MultiListIterator(iterators);
    }

    private ScanIterator scanIndexToElementId(String graph, QueryTypeParam param, boolean filterTTL,
                                              boolean lookup) {
        long now = System.currentTimeMillis();
        return new TypeTransIterator<RocksDBSession.BackendColumn, RocksDBSession.BackendColumn>(
                param.isRangeIndexScan() ?
                scan(graph, param.getCode(), INDEX_TABLE, param.getStart(), param.getEnd(),
                     param.getBoundary()) :
                scanPrefix(graph, param.getCode(), INDEX_TABLE, param.getStart(),
                           param.getBoundary()), column -> {
            if (filterTTL && isIndexExpire(column, now)) {
                return null;
            }

            // todo: Use parseIndex(BackendColumn indexCol) later
            var index = serializer.parseIndex(getGraphSupplier(graph),
                                              BackendColumn.of(column.name, column.value), null);

            if (param.getIdPrefix() != null &&
                !Bytes.prefixWith(index.elementId().asBytes(), param.getIdPrefix())) {
                return null;
            }

            Id elementId = index.elementId();
            if (elementId instanceof EdgeId) {
                column.name = new BytesBuffer().writeEdgeId(elementId).bytes();
            } else {
                column.name = new BytesBuffer().writeId(elementId).bytes();
            }

            if (lookup) {
                // Store the owner key
                column.value = BinaryElementSerializer.ownerId(index).asBytes();
                // column.value = KeyUtil.idToBytes(BinaryElementSerializer.ownerId(index));
            }
            return column;
        }, "trans-index-to-element-id");
    }

    private ScanIterator scanIndexToBaseElement(String graph, QueryTypeParam param,
                                                boolean filterTTL) {

        long now = System.currentTimeMillis();
        return new TypeTransIterator<RocksDBSession.BackendColumn, BaseElement>(
                param.isRangeIndexScan() ?
                scan(graph, param.getCode(), INDEX_TABLE, param.getStart(), param.getEnd(),
                     param.getBoundary()) :
                scanPrefix(graph, param.getCode(), INDEX_TABLE, param.getStart(),
                           param.getBoundary()), column -> {
            if (filterTTL && isIndexExpire(column, now)) {
                return null;
            }

            var e = serializer.index2Element(getGraphSupplier(graph),
                                             BackendColumn.of(column.name, column.value));

            if (param.getIdPrefix() != null &&
                !Bytes.prefixWith(e.id().asBytes(), param.getIdPrefix())) {
                return null;
            }

            return e;
            // return new BaseVertex(IdUtil.readLong(String.valueOf(random.nextLong())),
            // VertexLabel.GENERAL);
        }, "trans-index-to-base-element");
    }

    private boolean isIndexExpire(RocksDBSession.BackendColumn column, long now) {
        var e = directBinarySerializer.parseIndex(column.name, column.value);
        return e.expiredTime() > 0 && e.expiredTime() < now;
    }

    @Override
    public ScanIterator scanOriginal(ScanPartitionRequest spr) throws HgStoreException {
        Request request = spr.getScanRequest();
        String graph = request.getGraphName();
        List<Integer> ids;
        int partitionId = request.getPartitionId();
        int startCode = request.getStartCode();
        int endCode = request.getEndCode();
        if (partitionId == SCAN_ALL_PARTITIONS_ID) {
            ids = this.getLeaderPartitionIds(graph);
        } else {
            ids = new ArrayList<>();
            if (startCode != 0 || endCode != 0) {
                ids.add(partitionManager.getPartitionIdByCode(graph, startCode));
            } else {
                ids.add(partitionId);
            }
        }
        String table = request.getTable();
        if (StringUtils.isEmpty(table)) {
            table = tableMapping.get(request.getScanType());
        }
        int scanType = request.getBoundary();
        if (scanType == 0) {
            scanType = ScanIterator.Trait.SCAN_LT_END;
        }
        String tab = table;
        int st = scanType;
        BiFunction<Integer, byte[], ScanIterator> func = (id, position) -> {
            try (RocksDBSession dbSession = getSession(graph, tab, id)) {
                byte[] startPos = toPosition(EMPTY_BYTES, position);
                byte[] startKey = keyCreator.getStartKey(id, graph, startPos);
                byte[] endKey = keyCreator.getEndKey(id, graph);
                ScanIterator iter = dbSession.sessionOp().scan(tab, startKey, endKey, st);
                return new InnerKeyFilter(iter);
            }
        };
        return MultiPartitionIterator.of(ids, func);
    }

    @Override
    public ScanIterator scanPrefix(String graph, int code, String table, byte[] prefix,
                                   int scanType) throws HgStoreException {
        List<Integer> ids;
        if (code == SCAN_ALL_PARTITIONS_ID) {
            ids = this.getLeaderPartitionIds(graph);
        } else {
            ids = new ArrayList<>();
            ids.add(partitionManager.getPartitionIdByCode(graph, code));
        }
        BiFunction<Integer, byte[], ScanIterator> function = (id, position) -> {
            try (RocksDBSession dbSession = getSession(graph, table, id)) {
                return new InnerKeyFilter(dbSession.sessionOp().scan(table,
                                                                     keyCreator.getPrefixKey(id,
                                                                                             graph,
                                                                                             toPosition(
                                                                                                     prefix,
                                                                                                     position)),
                                                                     scanType));
            }
        };
        return MultiPartitionIterator.of(ids, function);
    }

    @Override
    public ScanIterator scanPrefix(String graph, int code, String table, byte[] prefix) throws
                                                                                        HgStoreException {

        return scanPrefix(graph, code, table, prefix, 0);
    }

    private byte[] toPosition(byte[] start, byte[] position) {
        if (position == null || position.length == 0) {
            return start;
        }
        return position;
    }

    @Override
    public HgStoreMetric.Partition getPartitionMetric(String graph, int partId,
                                                      boolean accurateCount) throws
                                                                             HgStoreException {
        // get key count
        Map<String, Long> countMap = null;
        Map<String, String> sizeMap = null;

        try (RocksDBSession dbSession = getSession(graph, partId)) {
            countMap = dbSession.getKeyCountPerCF(keyCreator.getStartKey(partId, graph),
                                                  keyCreator.getEndKey(partId, graph),
                                                  accurateCount);
            sizeMap = dbSession.getApproximateCFDataSize(keyCreator.getStartKey(partId, graph),
                                                         keyCreator.getEndKey(partId, graph));

            HgStoreMetric.Partition partMetric = new HgStoreMetric.Partition();
            partMetric.setPartitionId(partId);

            List<HgStoreMetric.Table> tables = new ArrayList<>(sizeMap.size());
            for (String tableName : sizeMap.keySet()) {
                HgStoreMetric.Table table = new HgStoreMetric.Table();
                table.setTableName(tableName);
                table.setKeyCount(countMap.get(tableName));
                table.setDataSize(sizeMap.get(tableName));
                tables.add(table);
            }

            partMetric.setTables(tables);
            return partMetric;
        }
    }

    @Override
    public HgStoreMetric.Graph getGraphMetric(String graph, int partId) {
        HgStoreMetric.Graph graphMetric = new HgStoreMetric.Graph();
        try (RocksDBSession dbSession = getSession(graph, partId)) {
            graphMetric.setApproxDataSize(
                    dbSession.getApproximateDataSize(keyCreator.getStartKey(partId, graph),
                                                     keyCreator.getEndKey(partId, graph)));
            graphMetric.setApproxKeyCount(dbSession.getEstimateNumKeys());

            return graphMetric;
        }
    }

    @Override
    public void batchGet(String graph, String table, Supplier<HgPair<Integer, byte[]>> s,
                         Consumer<HgPair<byte[], byte[]>> c) throws HgStoreException {

        int count = 0;
        while (true) {
            // Prevent dead loops
            if (count++ == Integer.MAX_VALUE) {
                break;
            }
            HgPair<Integer, byte[]> duality = s.get();
            if (duality == null) {
                break;
            }
            int code = duality.getKey();
            byte[] key = duality.getValue();

            int partId = provider.getPartitionByCode(graph, code).getId();

            try (RocksDBSession dbSession = getSession(graph, table, partId)) {
                byte[] targetKey = keyCreator.getKey(partId, graph, code, key);
                byte[] value = dbSession.sessionOp().get(table, targetKey);
                c.accept(new HgPair<>(key, value));
            }

        }
    }

    /**
     * Clear map data
     */
    @Override
    public void truncate(String graphName, int partId) throws HgStoreException {
        // Each partition corresponds to a rocksdb instance, so the rocksdb instance name is
        // rocksdb + partId
        try (RocksDBSession dbSession = getSession(graphName, partId)) {
            dbSession.sessionOp().deleteRange(keyCreator.getStartKey(partId, graphName),
                                              keyCreator.getEndKey(partId, graphName));
            // Release map ID
            keyCreator.delGraphId(partId, graphName);
        }
    }

    @Override
    public void flushAll() {
        log.warn("Flush all!!! ");
        factory.getGraphNames().forEach(dbName -> {
            try (RocksDBSession dbSession = factory.queryGraphDB(dbName)) {
                if (dbSession != null) {
                    dbSession.flush(false);
                }
            }
        });
    }

    @Override
    public void closeDB(int partId) {
        factory.releaseGraphDB(getDbName(partId));
    }

    @Override
    public void closeAll() {
        log.warn("close all db!!! ");
        factory.getGraphNames().forEach(dbName -> {
            factory.releaseGraphDB(dbName);
        });
    }

    @Override
    public Map<MemoryUsageType, Long> getApproximateMemoryUsageByType(List<Cache> caches) {
        try {
            return factory.getApproximateMemoryUsageByType(null, caches);
        } catch (Exception e) {
            return new HashMap<>();
        }
    }

    @Override
    public List<Integer> getLeaderPartitionIds(String graph) {
        return partitionManager.getLeaderPartitionIds(graph);
    }

    @Override
    public Set<Integer> getLeaderPartitionIdSet() {
        return partitionManager.getLeaderPartitionIdSet();
    }

    @Override
    public void saveSnapshot(String snapshotPath, String graph, int partId) throws
                                                                            HgStoreException {
        try (RocksDBSession dbSession = getSession(graph, partId)) {
            dbSession.saveSnapshot(snapshotPath);
        } catch (DBStoreException e) {
            throw new HgStoreException(HgStoreException.EC_RKDB_EXPORT_SNAPSHOT_FAIL, e.toString());
        }
    }

    @Override
    public void loadSnapshot(String snapshotPath, String graph, int partId, long v1) throws
                                                                                     HgStoreException {
        try (RocksDBSession dbSession = getSession(graph, partId)) {
            dbSession.loadSnapshot(snapshotPath, v1);
            keyCreator.clearCache(partId);
            factory.destroyGraphDB(dbSession.getGraphName());
        } catch (DBStoreException e) {
            throw new HgStoreException(HgStoreException.EC_RKDB_IMPORT_SNAPSHOT_FAIL, e.toString());
        }
    }

    @Override
    public long getLatestSequenceNumber(String graph, int partId) {
        try (RocksDBSession dbSession = getSession(graph, partId)) {
            return dbSession.getLatestSequenceNumber();
        }
    }

    @Override
    public ScanIterator scanRaw(String graph, int partId, long seqNum) throws HgStoreException {
        try (RocksDBSession dbSession = getSession(graph, partId)) {
            return dbSession.sessionOp().scanRaw(null, null, seqNum);
        } catch (DBStoreException e) {
            throw new HgStoreException(HgStoreException.EC_RKDB_EXPORT_SNAPSHOT_FAIL, e.toString());
        }
    }

    @Override
    public void ingestSstFile(String graph, int partId, Map<byte[], List<String>> sstFiles) throws
                                                                                            HgStoreException {
        try (RocksDBSession dbSession = getSession(graph, partId)) {
            dbSession.ingestSstFile(sstFiles);
        }
    }

    @Override
    public boolean cleanPartition(String graph, int partId) {
        Partition partition = partitionManager.getPartitionFromPD(graph, partId);
        cleanPartition(graph, partId, partition.getStartKey(), partition.getEndKey(),
                       CleanType.CLEAN_TYPE_KEEP_RANGE);
        return true;
    }

    @Override
    public boolean cleanPartition(String graph, int partId, long startKey, long endKey,
                                  CleanType cleanType) {
        Partition partition = partitionManager.getPartition(graph, partId);
        if (partition == null) {
            return true;
        }

        log.info("cleanPartition: graph {}, part id: {}, {} -> {}, cleanType:{}", graph, partId,
                 startKey, endKey, cleanType);

        var taskManager = HgStoreEngine.getInstance().getPartitionEngine(partId).getTaskManager();
        CleanDataRequest request = new CleanDataRequest();
        request.setPartitionId(partId);
        request.setGraphName(graph);
        request.setKeyStart(startKey);
        request.setKeyEnd(endKey);
        request.setCleanType(cleanType);
        var cleanTask = new CleanTask(partId, graph, AsyncTaskState.START, request);
        taskManager.putAsyncTask(cleanTask);

        Utils.runInThread(() -> {
            cleanPartition(partition, code -> {
                // in range
                boolean flag = code >= startKey && code < endKey;
                return (cleanType == CleanType.CLEAN_TYPE_KEEP_RANGE) == flag;
            });
            // May have been destroyed.
            if (HgStoreEngine.getInstance().getPartitionEngine(partId) != null) {
                taskManager.updateAsyncTaskState(partId, graph, cleanTask.getId(),
                                                 AsyncTaskState.SUCCESS);
            }
        });
        return true;
    }

    /**
     * Clean up partition data, delete data not belonging to this partition.
     * Traverse all keys of partId, read code, if code >= splitKey generate a new key, write to
     * newPartId
     */
    private boolean cleanPartition(Partition partition,
                                   Function<Integer, Boolean> belongsFunction) {
        log.info("Partition {}-{} cleanPartition begin... {}", partition.getGraphName(),
                 partition.getId(), partition);
        int counter = 0;

        SessionOperator op = getSession(partition.getGraphName(), partition.getId()).sessionOp();
        try {
            ScanIterator cfIterator =
                    op.scanRaw(keyCreator.getStartKey(partition.getId(), partition.getGraphName()),
                               keyCreator.getEndKey(partition.getId(), partition.getGraphName()),
                               0);
            while (cfIterator.hasNext()) {
                ScanIterator iterator = cfIterator.next();
                String table = new String(cfIterator.position());
                long deleted = 0;
                long total = 0;
                while (iterator.hasNext()) {
                    total += 1;
                    RocksDBSession.BackendColumn col = iterator.next();
                    int keyCode = keyCreator.parseKeyCode(col.name);
                    // if (keyCode < partition.getStartKey() || keyCode >= partition.getEndKey()) {
                    if (!belongsFunction.apply(keyCode)) {
                        if (counter == 0) {
                            op.prepare();
                        }
                        op.delete(table, col.name); // delete old data
                        if (++counter > batchSize) {
                            op.commit();
                            counter = 0;
                        }
                        deleted += 1;
                    }
                }
                iterator.close();
                log.info("partition {}-{}, table:{}, delete keys {}, total:{}",
                         partition.getGraphName(), partition.getId(), table, deleted, total);
            }
            cfIterator.close();
        } catch (Exception e) {
            log.error("Partition {}-{} cleanPartition exception {}", partition.getGraphName(),
                      partition.getId(), e);
            op.rollback();
            throw e;
        } finally {
            if (counter > 0) {
                try {
                    op.commit();
                } catch (Exception e) {
                    op.rollback();
                    throw e;
                }
            }
            op.getDBSession().close();
        }
        op.compactRange();
        log.info("Partition {}-{} cleanPartition end", partition.getGraphName(), partition.getId());
        return true;
    }

    @Override
    public boolean deletePartition(String graph, int partId) {
        try {
            deleteGraphDatabase(graph, partId);
        } catch (Exception e) {
            log.error("Partition {}-{} deletePartition exception {}", graph, partId, e);
        }
        return true;
    }

    @Override
    public List<String> getTableNames(String graph, int partId) {
        try (RocksDBSession dbSession = getSession(graph, partId)) {
            List<String> tables = null;
            tables = dbSession.getTables().keySet().stream().collect(Collectors.toList());
            return tables;
        }
    }

    private RocksDBSession getSession(String graph, String table, int partId) throws
                                                                              HgStoreException {
        RocksDBSession dbSession = getSession(partId);
        dbSession.checkTable(table);
        return dbSession;
    }

    private RocksDBSession getSession(String graphName, int partId) throws HgStoreException {
        return getSession(partId);
    }

    /**
     * Get dbsession, do not update dbsession active time
     */
    @Override
    public RocksDBSession getSession(int partId) throws HgStoreException {
        // Each partition corresponds to a rocksdb instance, so the rocksdb instance name is
        // rocksdb + partId
        String dbName = getDbName(partId);
        if (HgStoreEngine.getInstance().isClosing().get()) {
            HgStoreException closeException =
                    new HgStoreException(HgStoreException.EC_CLOSE, "store is closing", dbName);
            log.error("get session with error:", closeException);
            throw closeException;
        }
        RocksDBSession dbSession = factory.queryGraphDB(dbName);
        if (dbSession == null) {
            long version = HgStoreEngine.getInstance().getCommittedIndex(partId);
            dbSession =
                    factory.createGraphDB(partitionManager.getDbDataPath(partId, dbName), dbName,
                                          version);
            if (dbSession == null) {
                log.info("failed to create a new graph db: {}", dbName);
                throw new HgStoreException(HgStoreException.EC_RKDB_CREATE_FAIL,
                                           "failed to create a new graph db: {}", dbName);
            }
        }
        dbSession.setDisableWAL(true); // raft mode, disable rocksdb log
        return dbSession;
    }

    private void deleteGraphDatabase(String graph, int partId) throws IOException {
        truncate(graph, partId);
    }

    @Override
    public TxBuilder txBuilder(String graph, int partId) throws HgStoreException {
        return new TxBuilderImpl(graph, partId, getSession(graph, partId));
    }

    @Override
    public boolean cleanTtl(String graph, int partId, String table, List<ByteString> ids) {

        try (RocksDBSession dbSession = getSession(graph, table, partId)) {
            SessionOperator op = dbSession.sessionOp();
            try {
                op.prepare();
                for (ByteString bs : ids) {
                    byte[] targetKey = keyCreator.getKey(partId, graph, bs.toByteArray());
                    op.delete(table, targetKey);
                }
                op.commit();
            } catch (Exception e) {
                log.error("Graph: " + graph + " cleanTTL exception", e);
                op.rollback();
                throw new HgStoreException(HgStoreException.EC_RKDB_DODEL_FAIL, e.toString());
            }
        }
        return true;
    }

    @Override
    public boolean existsTable(String graph, int partId, String table) {
        try (RocksDBSession session = getSession(graph, partId)) {
            return session.tableIsExist(table);
        }
    }

    @Override
    public void createTable(String graph, int partId, String table) {
        try (RocksDBSession session = getSession(graph, partId)) {
            session.checkTable(table);
        }
    }

    @Override
    public void deleteTable(String graph, int partId, String table) {
        dropTable(graph, partId, table);
        // TODO: Check if the table is empty, if empty then truly delete the table
//        try (RocksDBSession session = getOrCreateGraphDB(graph, partId)) {
//            session.deleteTables(table);
//        }
    }

    @Override
    public void dropTable(String graph, int partId, String table) {
        try (RocksDBSession session = getSession(graph, partId)) {
//            session.dropTables(table);
            session.sessionOp().deleteRange(table, keyCreator.getStartKey(partId, graph),
                                            keyCreator.getEndKey(partId, graph));
        }
    }

    /**
     * Perform compaction on RocksDB
     */
    @Override
    public boolean dbCompaction(String graphName, int partitionId) {
        return this.dbCompaction(graphName, partitionId, "");
    }

    /**
     * Perform compaction on RocksDB
     */
    @Override
    public boolean dbCompaction(String graphName, int id, String tableName) {
        try {
            compactionPool.submit(() -> {
                try {
                    String path = getLockPath(id);
                    try (RocksDBSession session = getSession(graphName, id)) {
                        SessionOperator op = session.sessionOp();
                        pathLock.putIfAbsent(path, new AtomicInteger(compactionCanStart));
                        compactionState.putIfAbsent(id, new AtomicInteger(0));
                        log.info("Partition {} dbCompaction started", id);
                        if (tableName.isEmpty()) {
                            lock(path);
                            setState(id, doing);
                            log.info("Partition {}-{} got lock, dbCompaction start", id, path);
                            op.compactRange();
                            setState(id, compactionDone);
                            log.info("Partition {} dbCompaction end and start to do snapshot", id);
                            PartitionEngine pe = HgStoreEngine.getInstance().getPartitionEngine(id);
                            // find leader and send blankTask, after execution
                            if (pe.isLeader()) {
                                RaftClosure bc = (closure) -> {
                                };
                                pe.addRaftTask(RaftOperation.create(RaftOperation.SYNC_BLANK_TASK),
                                               bc);
                            } else {
                                HgCmdClient client = HgStoreEngine.getInstance().getHgCmdClient();
                                BlankTaskRequest request = new BlankTaskRequest();
                                request.setGraphName("");
                                request.setPartitionId(id);
                                client.tryInternalCallSyncWithRpc(request);
                            }
                            setAndNotifyState(id, compactionDone);
                        } else {
                            op.compactRange(tableName);
                        }
                    }
                    log.info("Partition {}-{} dbCompaction end", id, path);
                } catch (Exception e) {
                    log.error("do dbCompaction with error: ", e);
                } finally {
                    try {
                        semaphore.release();
                    } catch (Exception e) {

                    }
                }
            });
        } catch (Exception e) {

        }
        return true;
    }

    @Override
    public void lock(String path) throws InterruptedException, TimeoutException {
        long start = System.currentTimeMillis();
        while (!compareAndSetLock(path)) {
            AtomicInteger lock = pathLock.get(path);
            synchronized (lock) {
                lock.wait(1000);
                if (System.currentTimeMillis() - start > timeoutMillis) {
                    throw new TimeoutException("wait compaction start timeout");
                }
            }
        }
    }

    @Override
    public void unlock(String path) {
        AtomicInteger l = pathLock.get(path);
        l.set(compactionCanStart);
        synchronized (l) {
            l.notifyAll();
        }
    }

    private boolean compareAndSetLock(String path) {
        AtomicInteger l = pathLock.get(path);
        return l.compareAndSet(compactionCanStart, doing);
    }

    @Override
    public void awaitAndSetLock(int id, int expectedValue, int value) throws InterruptedException,
                                                                             TimeoutException {
        long start = System.currentTimeMillis();
        while (!compareAndSetState(id, expectedValue, value)) {
            AtomicInteger state = compactionState.get(id);
            synchronized (state) {
                state.wait(500);
                if (System.currentTimeMillis() - start > timeoutMillis) {
                    throw new TimeoutException("wait compaction start timeout");
                }
            }
        }
    }

    @Override
    public void setAndNotifyState(int id, int state) {
        AtomicInteger l = compactionState.get(id);
        l.set(state);
        synchronized (l) {
            l.notifyAll();
        }
    }

    @Override
    public AtomicInteger getState(int id) {
        AtomicInteger l = compactionState.get(id);
        return l;
    }

    private AtomicInteger setState(int id, int state) {
        AtomicInteger l = compactionState.get(id);
        l.set(state);
        return l;
    }

    private boolean compareAndSetState(int id, int expectedState, int newState) {
        AtomicInteger l = compactionState.get(id);
        return l.compareAndSet(expectedState, newState);
    }

    @Override
    public String getLockPath(int partitionId) {
        String dataPath = partitionManager.getDbDataPath(partitionId);
        File file = FileUtils.getFile(dataPath);
        File pf = file.getParentFile();
        return pf.getAbsolutePath();
    }

    @Override
    public List<Integer> getPartitionIds(String graph) {
        return partitionManager.getPartitionIds(graph);
    }

    @Override
    public boolean blockingCompact(String graphName, int partitionId) {
        //FIXME acquire semaphore here but release in dbCompaction
        boolean locked = semaphore.tryAcquire();
        if (locked) {
            dbCompaction(graphName, partitionId, "");
        } else {
            return false;
        }
        return true;
    }

    /**
     * Destroy the map, and delete the data file.
     *
     * @param graphName
     * @param partId
     */
    @Override
    public void destroyGraphDB(String graphName, int partId) throws HgStoreException {
        // Each graph each partition corresponds to a rocksdb instance, so the rocksdb instance
        // name is rocksdb + partId
        String dbName = getDbName(partId);

        factory.destroyGraphDB(dbName);
        keyCreator.clearCache(partId);
    }

    @Override
    public long count(String graph, String table) {
        List<Integer> ids = this.getLeaderPartitionIds(graph);
        Long all = ids.parallelStream().map((id) -> {
            InnerKeyFilter it = null;
            try (RocksDBSession dbSession = getSession(graph, table, id)) {
                long count = 0;
                SessionOperator op = dbSession.sessionOp();
                it = new InnerKeyFilter(op.scan(table, keyCreator.getStartKey(id, graph),
                                                keyCreator.getEndKey(id, graph),
                                                ScanIterator.Trait.SCAN_LT_END));
                while (it.hasNext()) {
                    it.next();
                    count++;
                }
                return count;
            } catch (Exception e) {
                throw e;
            } finally {
                if (it != null) {
                    try {
                        it.close();
                    } catch (Exception e) {

                    }
                }
            }
        }).collect(Collectors.summingLong(l -> l));
        return all;
    }

    public InnerKeyCreator getKeyCreator() {
        return keyCreator;
    }

    public static Long getColumnByteHash(RocksDBSession.BackendColumn column) {
        return fnvHash(column.name);
    }

    @NotThreadSafe
    private class TxBuilderImpl implements TxBuilder {

        private final String graph;
        private final int partId;
        private final RocksDBSession dbSession;
        private final SessionOperator op;

        private TxBuilderImpl(String graph, int partId, RocksDBSession dbSession) {
            this.graph = graph;
            this.partId = partId;
            this.dbSession = dbSession;
            this.op = this.dbSession.sessionOp();
            this.op.prepare();
        }

        @Override
        public TxBuilder put(int code, String table, byte[] key, byte[] value) throws
                                                                               HgStoreException {
            try {
                byte[] targetKey = keyCreator.getKey(this.partId, graph, code, key);
                this.op.put(table, targetKey, value);
            } catch (DBStoreException e) {
                throw new HgStoreException(HgStoreException.EC_RKDB_DOPUT_FAIL, e.toString());
            }
            return this;
        }

        @Override
        public TxBuilder del(int code, String table, byte[] key) throws HgStoreException {
            try {
                byte[] targetKey = keyCreator.getKey(this.partId, graph, code, key);
                this.op.delete(table, targetKey);
            } catch (DBStoreException e) {
                throw new HgStoreException(HgStoreException.EC_RKDB_DODEL_FAIL, e.toString());
            }

            return this;
        }

        @Override
        public TxBuilder delSingle(int code, String table, byte[] key) throws HgStoreException {

            try {
                byte[] targetKey = keyCreator.getKey(this.partId, graph, code, key);
                op.deleteSingle(table, targetKey);
            } catch (DBStoreException e) {
                throw new HgStoreException(HgStoreException.EC_RDKDB_DOSINGLEDEL_FAIL,
                                           e.toString());
            }

            return this;
        }

        @Override
        public TxBuilder delPrefix(int code, String table, byte[] prefix) throws HgStoreException {

            try {
                this.op.deletePrefix(table, keyCreator.getPrefixKey(this.partId, graph, prefix));
            } catch (DBStoreException e) {
                throw new HgStoreException(HgStoreException.EC_RKDB_DODELPREFIX_FAIL, e.toString());
            }

            return this;
        }

        @Override
        public TxBuilder delRange(int code, String table, byte[] start, byte[] end) throws
                                                                                    HgStoreException {

            try {
                this.op.deleteRange(table, keyCreator.getStartKey(this.partId, graph, start),
                                    keyCreator.getEndKey(this.partId, graph, end));
            } catch (DBStoreException e) {
                throw new HgStoreException(HgStoreException.EC_RKDB_DODELRANGE_FAIL, e.toString());
            }

            return this;
        }

        @Override
        public TxBuilder merge(int code, String table, byte[] key, byte[] value) throws
                                                                                 HgStoreException {

            try {
                byte[] targetKey = keyCreator.getKey(this.partId, graph, code, key);
                op.merge(table, targetKey, value);
            } catch (DBStoreException e) {
                throw new HgStoreException(HgStoreException.EC_RKDB_DOMERGE_FAIL, e.toString());
            }
            return this;
        }

        @Override
        public Tx build() {
            return new Tx() {
                @Override
                public void commit() throws HgStoreException {
                    op.commit();  // After an exception occurs in commit, rollback must be
                    // called, otherwise it will cause the lock not to be released.
                    dbSession.close();
                }

                @Override
                public void rollback() throws HgStoreException {
                    try {
                        op.rollback();
                    } finally {
                        dbSession.close();
                    }
                }
            };
        }
    }

    public static void clearCache() {
        GRAPH_SUPPLIER_CACHE.clear();
    }
}
