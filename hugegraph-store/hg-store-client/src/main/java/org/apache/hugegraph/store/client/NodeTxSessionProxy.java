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

package org.apache.hugegraph.store.client;

import static java.util.stream.Collectors.groupingBy;
import static org.apache.hugegraph.store.client.util.HgAssert.isArgumentNotNull;
import static org.apache.hugegraph.store.client.util.HgAssert.isArgumentValid;
import static org.apache.hugegraph.store.client.util.HgAssert.isFalse;
import static org.apache.hugegraph.store.client.util.HgStoreClientConst.EMPTY_STRING;
import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.err;
import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.toStr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvOrderedIterator;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgScanQuery;
import org.apache.hugegraph.store.HgSessionConfig;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.client.grpc.KvBatchScanner;
import org.apache.hugegraph.store.client.grpc.KvCloseableIterator;
import org.apache.hugegraph.store.client.query.QueryExecutor;
import org.apache.hugegraph.store.client.util.HgAssert;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.client.util.HgStoreClientUtil;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq.Builder;
import org.apache.hugegraph.store.query.StoreQueryParam;
import org.apache.hugegraph.store.term.HgPair;
import org.apache.hugegraph.store.term.HgTriple;
import org.apache.hugegraph.structure.BaseElement;

import lombok.extern.slf4j.Slf4j;

/**
 * created on 2022/01/19
 *
 * @version 0.6.0 added batch scan on 2022/03/03
 */
@Slf4j
@NotThreadSafe
public class NodeTxSessionProxy implements HgStoreSession {

    private final HgSessionConfig sessionConfig;
    private final HgStoreNodeManager nodeManager;
    private final HgStoreNodePartitioner nodePartitioner;
    private final String graphName;
    private final NodeTxExecutor txExecutor;

    public NodeTxSessionProxy(String graphName, HgStoreNodeManager nodeManager) {
        this.nodeManager = nodeManager;
        this.graphName = graphName;
        this.nodePartitioner = this.nodeManager.getNodePartitioner();
        this.txExecutor = NodeTxExecutor.graphOf(this.graphName, this);

        isFalse(this.nodePartitioner == null,
                "Failed to retrieve the node-partitioner from node-manager.");
        sessionConfig = new HgSessionConfig();
    }

    public NodeTxSessionProxy(String graphName, HgStoreNodeManager nodeManager,
                              HgSessionConfig config) {
        this.nodeManager = nodeManager;
        this.graphName = graphName;
        this.nodePartitioner = this.nodeManager.getNodePartitioner();
        this.txExecutor = NodeTxExecutor.graphOf(this.graphName, this);

        isFalse(this.nodePartitioner == null,
                "Failed to retrieve the node-partitioner from node-manager.");
        sessionConfig = config;
    }

    @Override
    public void beginTx() {
        this.txExecutor.setTx(true);
    }

    @Override
    public void commit() {
        this.txExecutor.commitTx();
    }

    @Override
    public void rollback() {
        this.txExecutor.rollbackTx();
    }

    @Override
    public boolean isTx() {
        return this.txExecutor.isTx();
    }

    @Override
    public boolean put(String table, HgOwnerKey ownerKey, byte[] value) {
        // isArgumentValid(table, "table");
        // isArgumentNotNull(ownerKey, "ownerKey");
        // log.info("put -> graph: {}, table: {}, key: {}, value: {}",
        // graphName, table, ownerKey, toByteStr(value));
        // return this.txExecutor.prepareTx(
        //        () -> getNodeStream(table, ownerKey),
        //        e -> e.session.put(table, e.data.getKey(), value)
        // );
        return this.txExecutor.prepareTx(new HgTriple(table, ownerKey, null),
                                         e -> e.getSession().put(table,
                                                                 e.getKey(),
                                                                 value));
    }

    @Override
    public boolean directPut(String table, int partitionId, HgOwnerKey ownerKey, byte[] value) {
        isArgumentValid(table, "table");
        isArgumentNotNull(ownerKey, "ownerKey");

        return this.txExecutor.prepareTx(
                new HgTriple(table, ownerKey, partitionId),
                e -> e.getSession().put(table, e.getKey(), value)
        );
    }

    @Override
    public boolean delete(String table, HgOwnerKey ownerKey) {
        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");
        HgAssert.isFalse(ownerKey == null, "The argument is invalid: ownerKey");

        if (log.isDebugEnabled()) {
            log.debug("delete -> graph: {}, table: {}, key: {}"
                    , graphName, table, toStr(ownerKey));
        }

        return this.txExecutor
                .prepareTx(
                        new HgTriple(table, ownerKey, null),
                        e -> e.getSession().delete(table, e.getKey())
                );
    }

    @Override
    public boolean deleteSingle(String table, HgOwnerKey ownerKey) {
        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");
        HgAssert.isFalse(ownerKey == null, "The argument is invalid: ownerKey");

        if (log.isDebugEnabled()) {
            log.debug("deleteSingle -> graph: {}, table: {}, key: {}"
                    , graphName, table, toStr(ownerKey));
        }

        return this.txExecutor
                .prepareTx(
                        new HgTriple(table, ownerKey, null),
                        e -> e.getSession().deleteSingle(table, e.getKey())
                );
    }

    @Override
    public boolean deletePrefix(String table, HgOwnerKey prefix) {
        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");
        HgAssert.isFalse(prefix == null, "The argument is invalid: prefix");

        if (log.isDebugEnabled()) {
            log.debug("deletePrefix -> graph: {}, table: {}, prefix: {}"
                    , graphName, table, toStr(prefix));
        }

        return this.txExecutor
                .prepareTx(
                        new HgTriple(table, prefix, null),
                        e -> e.getSession().deletePrefix(table, e.getKey())
                );
    }

    @Override
    public boolean deleteRange(String table, HgOwnerKey start, HgOwnerKey end) {
        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");
        HgAssert.isFalse(start == null, "The argument is invalid: start");
        HgAssert.isFalse(end == null, "The argument is invalid: end");

        if (log.isDebugEnabled()) {
            log.debug("deleteRange -> graph: {}, table: {}, start: {}, end: {}"
                    , graphName, table, toStr(start), toStr(end));
        }

        return this.txExecutor
                .prepareTx(
                        new HgTriple(table, start, end),
                        e -> e.getSession().deleteRange(table, e.getKey(), e.getEndKey())
                );
    }

    @Override
    public boolean merge(String table, HgOwnerKey key, byte[] value) {
        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");
        HgAssert.isFalse(key == null, "The argument is invalid: key");
        HgAssert.isFalse(value == null, "The argument is invalid: value");

        if (log.isDebugEnabled()) {
            log.debug("merge -> graph: {}, table: {}, key: {}, value: {}"
                    , graphName, table, toStr(key), toStr(value));
        }

        return this.txExecutor
                .prepareTx(
                        new HgTriple(table, key, value),
                        e -> e.getSession().merge(table, e.getKey(), value)
                );
    }

    /*--- tx end ---*/

    @Override
    public byte[] get(String table, HgOwnerKey ownerKey) {
        isArgumentValid(table, "table");
        isArgumentNotNull(ownerKey, "ownerKey");

        return this.txExecutor
                .limitOne(
                        () -> this.getNodeStream(table, ownerKey),
                        e -> e.session.get(table, e.data.getKey()), HgStoreClientConst.EMPTY_BYTES
                );
    }

    @Override
    public boolean clean(int partId) {
        Collection<HgNodePartition> nodes = this.doPartition("", partId);
        return nodes.parallelStream()
                    .map(
                            e -> this.getStoreNode(e.getNodeId()).openSession(this.graphName)
                                     .clean(partId)
                    ).findFirst().get();
    }

    @Override
    @Deprecated
    public List<HgKvEntry> batchGetOwner(String table, List<HgOwnerKey> keyList) {
        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");
        HgAssert.isFalse(HgAssert.isInvalid(keyList), "The argument is invalid: keyList");

        return this.txExecutor
                .toList(
                        (l) -> this.getStoreNode(l),
                        keyList,
                        key -> this.toNodeTkvList(table, key, key).stream(),
                        e -> e.session.batchGetOwner(table, e.data)
                );
    }

    @Override
    public HgKvIterator<HgKvEntry> batchPrefix(String table, List<HgOwnerKey> keyList) {
        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");
        HgAssert.isFalse(HgAssert.isInvalid(keyList), "The argument is invalid: keyList");
        return this.toHgKvIteratorProxy(
                this.txExecutor
                        .toList(
                                (l) -> this.getStoreNode(l),
                                keyList,
                                key -> this.toNodeTkvList(table, key, key).stream(),
                                e -> Collections.singletonList(e.session.batchPrefix(table, e.data))
                        )
                , Long.MAX_VALUE);
    }

    @Override
    public boolean truncate() {
        return this.txExecutor
                .isAllTrue(
                        () -> this.getNodeStream(EMPTY_STRING),
                        e -> e.session.truncate()
                );
    }

    @Override
    public boolean existsTable(String table) {
        return this.txExecutor
                .ifAnyTrue(
                        () -> this.getNodeStream(EMPTY_STRING),
                        e -> e.session.existsTable(table)
                );
    }

    @Override
    public boolean createTable(String table) {
        return this.txExecutor
                .isAllTrue(
                        () -> this.getNodeStream(EMPTY_STRING),
                        e -> e.session.createTable(table)
                );
    }

    @Override
    public boolean deleteTable(String table) {
        return this.txExecutor
                .isAllTrue(
                        () -> this.getNodeStream(EMPTY_STRING),
                        e -> e.session.deleteTable(table)
                );
    }

    @Override
    public boolean dropTable(String table) {
        return this.txExecutor
                .isAllTrue(
                        () -> this.getNodeStream(table),
                        e -> e.session.dropTable(table)
                );
    }

    @Override
    public boolean deleteGraph(String graph) {
        return this.txExecutor
                .isAllTrue(
                        () -> this.getNodeStream(EMPTY_STRING),
                        e -> e.session.deleteGraph(graph)
                );
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table) {
        return scanIterator(table, 0);
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, byte[] query) {
        return scanIterator(table, 0, query);
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, long limit) {
        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");

        return this.toHgKvIteratorProxy(
                this.toNodeTkvList(table)
                    .parallelStream()
                    .map(
                            e -> this.getStoreNode(e.getNodeId()).openSession(this.graphName)
                                     .scanIterator(e.getTable(), limit)
                    )
                    .collect(Collectors.toList())
                , limit);
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, long limit, byte[] query) {
        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");

        return this.toHgKvIteratorProxy(
                this.toNodeTkvList(table)
                    .parallelStream()
                    .map(
                            e -> this.getStoreNode(e.getNodeId()).openSession(this.graphName)
                                     .scanIterator(e.getTable(), e.getKey(), limit, query)
                    )
                    .collect(Collectors.toList())
                , limit);
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey keyPrefix) {
        return scanIterator(table, keyPrefix, 0);
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey keyPrefix, long limit) {
        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");
        HgAssert.isFalse(keyPrefix == null, "The argument is invalid: keyPrefix");

        return this.toHgKvIteratorProxy(
                this.toNodeTkvList(table, keyPrefix)
                    .parallelStream()
                    .map(
                            e -> this.getStoreNode(e.getNodeId()).openSession(this.graphName)
                                     .scanIterator(e.getTable(), e.getKey(), limit)
                    )
                    .collect(Collectors.toList())
                , limit);

    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey keyPrefix, long limit,
                                                byte[] query) {
        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");
        HgAssert.isFalse(keyPrefix == null, "The argument is invalid: keyPrefix");

        return this.toHgKvIteratorProxy(
                this.toNodeTkvList(table, keyPrefix)
                    .parallelStream()
                    .map(
                            e -> this.getStoreNode(e.getNodeId()).openSession(this.graphName)
                                     .scanIterator(e.getTable(), e.getKey(), limit, query)
                    )
                    .collect(Collectors.toList())
                , limit);
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey,
                                                HgOwnerKey endKey) {
        return this.scanIterator(table, startKey, endKey, 0, null);
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey,
                                                HgOwnerKey endKey, long limit) {
        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");
        HgAssert.isFalse(startKey == null, "The argument is invalid: startKey");
        HgAssert.isFalse(endKey == null, "The argument is invalid: endKey");

        return this.toHgKvIteratorProxy(
                this.toNodeTkvList(table, startKey, endKey)
                    .parallelStream()
                    .map(
                            e -> this.getStoreNode(e.getNodeId()).openSession(this.graphName)
                                     .scanIterator(e.getTable(), e.getKey(), e.getEndKey(), limit)
                    )
                    .collect(Collectors.toList())
                , limit);
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey, HgOwnerKey endKey
            , long limit, byte[] query) {
        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");
        HgAssert.isFalse(startKey == null, "The argument is invalid: startKey");
        HgAssert.isFalse(endKey == null, "The argument is invalid: endKey");

        return this.toHgKvIteratorProxy(
                this.toNodeTkvList(table, startKey, endKey)
                    .parallelStream()
                    .map(
                            e -> this.getStoreNode(e.getNodeId()).openSession(this.graphName)
                                     .scanIterator(e.getTable(), e.getKey(), e.getEndKey(), limit,
                                                   query)
                    )
                    .collect(Collectors.toList())
                , limit);

    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey, HgOwnerKey endKey
            , long limit, int scanType, byte[] query) {
        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");
        HgAssert.isFalse(startKey == null, "The argument is invalid: startKey");
        HgAssert.isFalse(endKey == null, "The argument is invalid: endKey");

        return this.toHgKvIteratorProxy(
                this.toNodeTkvList(table, startKey, endKey)
                    .parallelStream()
                    .map(
                            e -> this.getStoreNode(e.getNodeId()).openSession(this.graphName)
                                     .scanIterator(e.getTable(), e.getKey(), e.getEndKey(), limit,
                                                   scanType, query)
                    )
                    .collect(Collectors.toList())
                , limit);

    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, int codeFrom, int codeTo,
                                                int scanType, byte[] query) {
        if (log.isDebugEnabled()) {
            log.debug("graph: {}, table: {}, codeFrom: {}, codeTo: {}, scanType: {}, query: {}"
                    , graphName, table, codeFrom, codeTo, scanType, HgStoreClientUtil.toStr(query));
        }

        HgAssert.isFalse(HgAssert.isInvalid(table), "The argument is invalid: table");
        return this.toHgKvIteratorProxy(
                this.toNodeTkvList(table, codeFrom, codeTo)
                    .parallelStream()
                    .map(
                            e -> this.getStoreNode(e.getNodeId()).openSession(this.graphName)
                                     .scanIterator(e.getTable()
                                             , e.getKey().getKeyCode()
                                             , e.getEndKey().getKeyCode(),
                                                   scanType, query)
                    )
                    .collect(Collectors.toList())
                , 0);

    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(Builder scanReqBuilder) {
        List<NodeTkv> nodeTKvs = this.toNodeTkvList(scanReqBuilder);
        Function<NodeTkv, HgKvIterator<HgKvEntry>> hgKvIteratorFunction = e -> {
            HgStoreSession session = this.getStoreNode(e.getNodeId())
                                         .openSession(this.graphName);
            return session.scanIterator(scanReqBuilder);
        };
        List<HgKvIterator> iterators = nodeTKvs.parallelStream()
                                               .map(hgKvIteratorFunction)
                                               .collect(Collectors.toList());
        return this.toHgKvIteratorProxy(iterators, scanReqBuilder.getLimit());
    }

    @Override
    public List<HgKvIterator<HgKvEntry>> scanBatch(HgScanQuery scanQuery) {
        HgAssert.isArgumentNotNull(scanQuery, "scanQuery");

        return this.toTkvMapFunc(scanQuery.getScanMethod())
                   .apply(scanQuery)
                   .entrySet()
                   .stream()
                   .map(e ->
                                this.getStoreNode(e.getKey())
                                    .openSession(this.graphName)
                                    .scanBatch(toScanQueryFunc(scanQuery.getScanMethod())
                                                       .apply(scanQuery.getTable(), e.getValue())
                                                       .setQuery(scanQuery.getQuery())
                                                       .setLimit(scanQuery.getLimit())
                                                       .setPerKeyLimit(scanQuery.getPerKeyLimit())
                                                       .setPerKeyMax((scanQuery.getPerKeyMax()))
                                                       .setScanType(scanQuery.getScanType())
                                                       .build()
                                    )
                   )
                   //.peek(e->log.info("{}",e))
                   .flatMap(List::stream)
                   .collect(Collectors.toList());

    }

    @Override
    public KvCloseableIterator<HgKvIterator<HgKvEntry>> scanBatch2(HgScanQuery scanQuery) {
        return scanBatch3(scanQuery, null);
    }

    @Override
    public KvCloseableIterator<HgKvIterator<HgKvEntry>> scanBatch3(HgScanQuery scanQuery,
                                                                   KvCloseableIterator iterator) {
        KvCloseableIterator notifierWrap = KvBatchScanner.ofMerger(scanQuery, (query, notifier) -> {
            Map<Long, List<NodeTkv>> nodeTkvs = this.toTkvMapFunc(scanQuery.getScanMethod())
                                                    .apply(query);

            nodeTkvs.forEach((storeId, tkvs) -> {
                this.getStoreNode(storeId)
                    .openSession(this.graphName)
                    .scanBatch3(toScanQueryFunc(scanQuery.getScanMethod())
                                        .apply(scanQuery.getTable(), tkvs)
                                        .setQuery(scanQuery.getQuery())
                                        .setLimit(scanQuery.getLimit())
                                        .setSkipDegree(scanQuery.getSkipDegree())
                                        .setPerKeyLimit(scanQuery.getPerKeyLimit())
                                        .setPerKeyMax((scanQuery.getPerKeyMax()))
                                        .setScanType(scanQuery.getScanType())
                                        .setOrderType(scanQuery.getOrderType())
                                        .build(), notifier
                    );
            });
            return true;
        });
        return notifierWrap;
    }

    private Function<HgScanQuery, Map<Long, List<NodeTkv>>> toTkvMapFunc(
            HgScanQuery.ScanMethod scanMethod) {
        switch (scanMethod) {
            case RANGE:
                return scanQuery -> {
                    List<HgOwnerKey> starts = scanQuery.getStartList();
                    List<HgOwnerKey> ends = scanQuery.getEndList();
                    int size = starts.size();
                    return IntStream.range(0, size)
                                    .boxed()
                                    .map(i -> this.toNodeTkvList(scanQuery.getTable(),
                                                                 starts.get(i), ends.get(i)))
                                    .flatMap(List::stream)
                                    .collect(groupingBy(NodeTkv::getNodeId));
                };
            case PREFIX:
                return scanQuery ->
                        scanQuery.getPrefixList()
                                 .stream()
                                 .map(keyPrefix -> this.toNodeTkvList(scanQuery.getTable(),
                                                                      keyPrefix))
                                 .flatMap(List::stream)
                                 .collect(groupingBy(NodeTkv::getNodeId));

            default:
                return scanQuery -> this.toNodeTkvList(scanQuery.getTable())
                                        .stream()
                                        .collect(groupingBy(NodeTkv::getNodeId));
        }
    }

    private BiFunction<String, List<NodeTkv>, HgScanQuery.ScanBuilder> toScanQueryFunc(
            HgScanQuery.ScanMethod scanMethod) {
        switch (scanMethod) {
            case RANGE:
                return (table, tkvList) -> {
                    List<HgOwnerKey> startList = new LinkedList();
                    List<HgOwnerKey> endList = new LinkedList();

                    tkvList.stream().forEach(e -> {
                        startList.add(e.getKey());
                        endList.add(e.getEndKey());
                    });

                    return HgScanQuery.ScanBuilder.rangeOf(table, startList, endList);
                };
            case PREFIX:
                return (table, tkvList) ->
                        HgScanQuery.ScanBuilder.prefixOf(table,
                                                         tkvList.stream()
                                                                .map(e -> e.getKey())
                                                                .collect(Collectors.toList())
                        );
            default:
                return (table, tkvList) -> HgScanQuery.ScanBuilder.tableOf(table);
        }

    }

    /*-- common --*/
    private HgKvIterator toHgKvIteratorProxy(List<HgKvIterator> iteratorList, long limit) {
        boolean isAllOrderedLimiter = iteratorList.stream()
                                                  .allMatch(
                                                          e -> e instanceof HgKvOrderedIterator);

        HgKvIterator<HgKvEntry> iterator;
        if (isAllOrderedLimiter) {
            iterator = new SequencedIterator(iteratorList.stream()
                                                         .map(e -> (HgKvOrderedIterator) e)
                                                         .collect(Collectors.toList()), limit);
        } else {
            iterator = new TopWorkIteratorProxy(iteratorList, limit);
        }

        return iterator;
    }

    HgStoreNode getStoreNode(Long nodeId) {
        HgStoreNode res = this.nodeManager.applyNode(this.graphName, nodeId);

        if (res == null) {
            throw err("Failed to apply for an instance of HgStoreNode from node-manager.");
        }

        return res;
    }

    public boolean doAction(String table, HgOwnerKey startKey, HgOwnerKey endKey,
                            Function<NodeTkv, Boolean> action) {
        Collection<HgNodePartition> partitions =
                doPartition(table, startKey.getOwner(), endKey.getOwner());
        for (HgNodePartition partition : partitions) {
            HgStoreNode storeNode = this.getStoreNode(partition.getNodeId());
            HgStoreSession session = this.txExecutor.openNodeSession(storeNode);
            NodeTkv data = new NodeTkv(partition, table, startKey, endKey);
            data.setSession(session);
            if (!action.apply(data)) {
                return false;
            }
        }
        return true;
    }

    public boolean doAction(String table, HgOwnerKey startKey, Integer code,
                            Function<NodeTkv, Boolean> action) {
        Collection<HgNodePartition> partitions = this.doPartition(table, code);
        for (HgNodePartition partition : partitions) {
            HgStoreNode storeNode = this.getStoreNode(partition.getNodeId());
            HgStoreSession session = this.txExecutor.openNodeSession(storeNode);
            NodeTkv data = new NodeTkv(partition, table, startKey, code);
            data.setSession(session);
            if (!action.apply(data)) {
                return false;
            }
        }
        return true;
    }

    private List<NodeTkv> toNodeTkvList(Builder scanReqBuilder) {
        // TODO: use builder to get owner
        String table = scanReqBuilder.getTable();
        HgOwnerKey ownerKey = HgStoreClientConst.ALL_PARTITION_OWNER_KEY;
        byte[] allOwner = ownerKey.getOwner();
        Collection<HgNodePartition> partitions = doPartition(table,
                                                             allOwner,
                                                             allOwner);
        List<NodeTkv> nodeTkvs = new ArrayList<>(partitions.size());
        for (HgNodePartition partition : partitions) {
            nodeTkvs.add(new NodeTkv(partition, table, ownerKey, ownerKey));
        }
        return nodeTkvs;
    }

    private List<NodeTkv> toNodeTkvList(String table) {
        Collection<HgNodePartition> partitions = doPartition(table,
                                                             HgStoreClientConst.ALL_PARTITION_OWNER_KEY.getOwner(),
                                                             HgStoreClientConst.ALL_PARTITION_OWNER_KEY.getOwner());
        ArrayList<NodeTkv> nodeTkvs = new ArrayList<>(partitions.size());
        for (HgNodePartition partition : partitions) {
            nodeTkvs.add(new NodeTkv(partition, table, HgStoreClientConst.ALL_PARTITION_OWNER_KEY,
                                     HgStoreClientConst.ALL_PARTITION_OWNER_KEY));
        }
        return nodeTkvs;
    }

    private List<NodeTkv> toNodeTkvList(String table, HgOwnerKey ownerKey) {
        Collection<HgNodePartition> partitions =
                doPartition(table, ownerKey.getOwner(), ownerKey.getOwner());
        ArrayList<NodeTkv> nodeTkvs = new ArrayList<>(partitions.size());
        for (HgNodePartition partition : partitions) {
            nodeTkvs.add(new NodeTkv(partition, table, ownerKey, ownerKey));
        }

        return nodeTkvs;
    }

    private List<NodeTkv> toNodeTkvList(String table, HgOwnerKey startKey, HgOwnerKey endKey) {
        Collection<HgNodePartition> partitions =
                doPartition(table, startKey.getOwner(), endKey.getOwner());
        ArrayList<NodeTkv> nodeTkvs = new ArrayList<>(partitions.size());
        for (HgNodePartition partition : partitions) {
            nodeTkvs.add(new NodeTkv(partition, table, startKey, endKey));
        }
        return nodeTkvs;
    }

    private List<NodeTkv> toNodeTkvList(String table, int startCode, int endCode) {
        Collection<HgNodePartition> partitions = this.doPartition(table, startCode, endCode);
        ArrayList<NodeTkv> nodeTkvs = new ArrayList<>(partitions.size());
        for (HgNodePartition partition : partitions) {
            nodeTkvs.add(
                    new NodeTkv(partition, table, HgOwnerKey.codeOf(startCode),
                                HgOwnerKey.codeOf(endCode)));
        }
        return nodeTkvs;
    }

    /**
     * @return not null
     */
    private Collection<HgNodePartition> doPartition(String table, byte[] startKey, byte[] endKey) {
        HgNodePartitionerBuilder partitionerBuilder = HgNodePartitionerBuilder.resetAndGet();

        int status = this.nodePartitioner.partition(partitionerBuilder, this.graphName, startKey,
                                                    endKey);

        if (status != 0) {
            throw err("The node-partitioner is not work.");
        }

        Collection<HgNodePartition> partitions = partitionerBuilder.getPartitions();

        if (partitions.isEmpty()) {
            throw err("Failed to get the collection of HgNodePartition from node-partitioner.");
        }

        return partitions;
    }

    /**
     * @return @return not null
     */
    private Collection<HgNodePartition> doPartition(String table, int startCode, int endCode) {
        HgNodePartitionerBuilder partitionerBuilder = HgNodePartitionerBuilder.resetAndGet();
        int status = this.nodePartitioner.partition(partitionerBuilder, this.graphName, startCode,
                                                    endCode);

        if (status != 0) {
            throw err("The node-partitioner is not work.");
        }

        Collection<HgNodePartition> partitions = partitionerBuilder.getPartitions();

        if (partitions.isEmpty()) {
            throw err("Failed to get the collection of HgNodePartition from node-partitioner.");
        }

        return partitions;
    }

    Collection<HgNodePartition> doPartition(String table, int partitionId) {
        HgNodePartitionerBuilder partitionerBuilder = HgNodePartitionerBuilder.resetAndGet();
        int status =
                this.nodePartitioner.partition(partitionerBuilder, this.graphName, partitionId);

        if (status != 0) {
            throw err("The node-partitioner is not work.");
        }

        Collection<HgNodePartition> partitions = partitionerBuilder.getPartitions();

        if (partitions.isEmpty()) {
            throw err("Failed to get the collection of HgNodePartition from node-partitioner.");
        }

        return partitions;
    }

    private Stream<HgPair<HgStoreNode, NodeTkv>> getNodeStream(String table) {
        return this.toNodeTkvList(table)
                   .stream()
                   .map(
                           e -> new HgPair<>(this.getStoreNode(e.getNodeId()), e)
                   );
    }

    Stream<HgPair<HgStoreNode, NodeTkv>> getNodeStream(String table,
                                                       HgOwnerKey ownerKey) {
        return this.toNodeTkvList(table, ownerKey)
                   .stream()
                   .map(
                           e -> new HgPair<>(this.getStoreNode(e.getNodeId()), e)
                   );
    }

    Stream<HgPair<HgStoreNode, NodeTkv>> getNodeStream(String table, HgOwnerKey startKey,
                                                       HgOwnerKey endKey) {
        return this.toNodeTkvList(table, startKey, endKey)
                   .stream()
                   .map(
                           e -> new HgPair<>(this.getStoreNode(e.getNodeId()), e)
                   );

    }

    // private List<HgPair<HgStoreNode, NodeTkv>> getNode(String table) {
    //    List<NodeTkv> nodeTkvList = this.toNodeTkvList(table);
    //    return nodeTkv2Node(nodeTkvList);
    // }

    List<HgPair<HgStoreNode, NodeTkv>> getNode(String table, HgOwnerKey ownerKey) {
        List<NodeTkv> nodeTkvList = this.toNodeTkvList(table, ownerKey);
        return nodeTkv2Node(nodeTkvList);
    }

    List<HgPair<HgStoreNode, NodeTkv>> getNode(String table, HgOwnerKey startKey,
                                               HgOwnerKey endKey) {
        List<NodeTkv> nodeTkvList = this.toNodeTkvList(table, startKey, endKey);
        return nodeTkv2Node(nodeTkvList);

    }
    //
    //boolean doAction(String table, HgOwnerKey startKey, HgOwnerKey endKey,
    //                                            Function<NodeTkv, Boolean> action) {
    //    return this.doAction(table, startKey, endKey, action);
    //
    //}

    // List<HgPair<HgStoreNode, NodeTkv>> getNode(String table, Integer endKey) {
    //       .stream()
    //            .map(e -> new NodeTkv(e, nodeParams.getX(), nodeParams.getY(), nodeParams.getY
    //            ().getKeyCode()))
    //            .map(
    //                    e -> new HgPair<>(this.proxy.getStoreNode(e.getNodeId()), e)
    //                );
    //    Collection<HgNodePartition> nodePartitions = this.doPartition(table, endKey);
    //    for (HgNodePartition nodePartition: nodePartitions) {
    //
    //    }
    //    return nodeTkv2Node(nodeTkvList);
    //
    // }

    private List<HgPair<HgStoreNode, NodeTkv>> nodeTkv2Node(Collection<NodeTkv> nodeTkvList) {
        ArrayList<HgPair<HgStoreNode, NodeTkv>> hgPairs = new ArrayList<>(nodeTkvList.size());
        for (NodeTkv nodeTkv : nodeTkvList) {
            hgPairs.add(new HgPair<>(this.getStoreNode(nodeTkv.getNodeId()), nodeTkv));
        }
        return hgPairs;
    }

    @Override
    public List<HgKvIterator<BaseElement>> query(StoreQueryParam query,
                                                 HugeGraphSupplier supplier) throws
                                                                             PDException {
        long current = System.nanoTime();
        QueryExecutor planner = new QueryExecutor(this.nodePartitioner, supplier,
                                                  this.sessionConfig.getQueryPushDownTimeout());
        query.checkQuery();
        var iteratorList = planner.getIterators(query);
        log.debug("[time_stat] query id: {}, size {},  get Iterator cost: {} ms",
                  query.getQueryId(),
                  iteratorList.size(),
                  (System.nanoTime() - current) * 1.0 / 1000_000);
        return iteratorList;
    }
}
