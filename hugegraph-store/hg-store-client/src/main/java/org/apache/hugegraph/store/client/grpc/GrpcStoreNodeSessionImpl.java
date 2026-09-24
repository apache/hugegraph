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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvStore;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgScanQuery;
import org.apache.hugegraph.store.client.HgStoreNode;
import org.apache.hugegraph.store.client.HgStoreNodeManager;
import org.apache.hugegraph.store.client.HgStoreNodeSession;
import org.apache.hugegraph.store.client.util.HgAssert;
import org.apache.hugegraph.store.client.util.HgStoreClientConfig;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.client.util.HgStoreClientUtil;
import org.apache.hugegraph.store.client.util.HgUuid;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.grpc.common.GraphMethod;
import org.apache.hugegraph.store.grpc.common.Key;
import org.apache.hugegraph.store.grpc.common.OpType;
import org.apache.hugegraph.store.grpc.common.TableMethod;
import org.apache.hugegraph.store.grpc.session.BatchEntry;
import org.apache.hugegraph.store.grpc.stream.HgStoreStreamGrpc.HgStoreStreamStub;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq;
import org.apache.hugegraph.store.query.StoreQueryParam;
import org.apache.hugegraph.structure.BaseElement;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

import lombok.extern.slf4j.Slf4j;

/**
 * created on 2022/01/19
 *
 * @version 0.6.0 added batch get on 2022/04/06
 */
@Slf4j
@NotThreadSafe
class GrpcStoreNodeSessionImpl implements HgStoreNodeSession {

    private static final HgStoreClientConfig hgStoreClientConfig = HgStoreClientConfig.of();
    private final HgStoreNode storeNode;
    private final String graphName;
    private final GrpcStoreSessionClient storeSessionClient;
    private final GrpcStoreStreamClient storeStreamClient;
    private final HgStoreNodeManager nodeManager;
    private final NotifyingExecutor notifier;
    private final SwitchingExecutor switcher;
    private final BatchEntry.Builder batchEntryBuilder = BatchEntry.newBuilder();
    private final Key.Builder builder = Key.newBuilder();
    private boolean isAutoCommit = true;
    private String batchId;
    private LinkedList<BatchEntry> batchEntries = new LinkedList<>();

    GrpcStoreNodeSessionImpl(HgStoreNode storeNode, String graphName,
                             HgStoreNodeManager nodeManager,
                             GrpcStoreSessionClient sessionClient,
                             GrpcStoreStreamClient streamClient) {
        HgAssert.isFalse(HgAssert.isInvalid(graphName), "the argument: graphName is invalid.");
        HgAssert.isFalse(nodeManager == null, "the argument: nodeManager is null.");
        HgAssert.isFalse(storeNode == null, "the argument: storeNode is null.");
        HgAssert.isFalse(sessionClient == null, "the argument: sessionClient is null.");
        HgAssert.isFalse(streamClient == null, "the argument: streamClient is null.");

        this.graphName = graphName;
        this.storeNode = storeNode;
        this.storeSessionClient = sessionClient;
        this.storeStreamClient = streamClient;
        this.nodeManager = nodeManager;

        this.notifier = new NotifyingExecutor(this.graphName, this.nodeManager, this);
        this.switcher = SwitchingExecutor.of();
    }

    @Override
    public String getGraphName() {
        return graphName;
    }

    @Override
    public HgStoreNode getStoreNode() {
        return storeNode;
    }

    public Key toKey(HgOwnerKey ownerKey) {
        if (ownerKey == null) {
            return null;
        }
        return builder
                .setKey(UnsafeByteOperations.unsafeWrap(ownerKey.getKey()))
                .setCode(ownerKey.getKeyCode())
                .build();
    }

    @Override
    public void beginTx() {
        this.isAutoCommit = false;
    }

    @Override
    public void commit() {
        try {
            if (this.isAutoCommit) {
                throw new IllegalStateException("It's not in tx state");
            }
            if (this.batchEntries.isEmpty()) {
                this.resetTx();
                return;
            }
            if (!this.doCommit(this.batchEntries)) {
                throw new Exception("Failed to invoke doCommit");
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            this.resetTx();
        }

    }

    @Override
    public void rollback() {
        if (this.isAutoCommit) {
            throw new IllegalStateException("It's not in tx state");
        }
        this.resetTx();
    }

    @Override
    public boolean isTx() {
        return !this.isAutoCommit;
    }

    private void resetTx() {
        this.isAutoCommit = true;
        this.batchId = null;
        this.batchEntries = new LinkedList<>();
    }

    //TODO: not support distributed tx yet.
    private String getBatchId() {
        if (this.isAutoCommit) {
            this.batchId = HgUuid.newUUID();
        } else {
            if (this.batchId == null) {
                this.batchId = HgUuid.newUUID();
            }
        }
        return this.batchId;
    }

    @Override
    public boolean put(String table, HgOwnerKey ownerKey, byte[] value) {
        return this.prepareBatchEntry(OpType.OP_TYPE_PUT, table, ownerKey, null, value);
    }

    @Override
    public boolean directPut(String table, int partitionId, HgOwnerKey key, byte[] value) {
        return false;
    }

    @Override
    public boolean delete(String table, HgOwnerKey ownerKey) {
        return this.prepareBatchEntry(OpType.OP_TYPE_DEL, table, ownerKey, null, null);
    }

    @Override
    public boolean deleteSingle(String table, HgOwnerKey ownerKey) {
        return this.prepareBatchEntry(OpType.OP_TYPE_DEL_SINGLE, table, ownerKey, null, null);
    }

    @Override
    public boolean deletePrefix(String table, HgOwnerKey prefix) {
        return this.prepareBatchEntry(OpType.OP_TYPE_DEL_PREFIX, table, prefix, null, null);
    }

    @Override
    public boolean deleteRange(String table, HgOwnerKey start, HgOwnerKey end) {
        return this.prepareBatchEntry(OpType.OP_TYPE_DEL_RANGE, table, start, end, null);
    }

    @Override
    public boolean merge(String table, HgOwnerKey key, byte[] value) {
        return this.prepareBatchEntry(OpType.OP_TYPE_MERGE, table, key, null, value);
    }

    private boolean prepareBatchEntry(OpType opType, String table
            , HgOwnerKey startKey, HgOwnerKey endKey, byte[] value) {
        this.batchEntryBuilder.clear().setOpType(opType);
        this.batchEntryBuilder.setTable(HugeServerTables.TABLES_MAP.get(table));
        if (startKey != null) {
            this.batchEntryBuilder.setStartKey(toKey(startKey));
        }
        if (endKey != null) {
            this.batchEntryBuilder.setEndKey(toKey(endKey));
        }
        if (value != null) {
            this.batchEntryBuilder.setValue(ByteString.copyFrom(value));
        }
        if (this.isAutoCommit) {
            return this.doCommit(Collections.singletonList(this.batchEntryBuilder.build()));
        } else {
            return this.batchEntries.add(this.batchEntryBuilder.build());
        }

    }

    private boolean doCommit(List<BatchEntry> entries) {
        return this.notifier.invoke(
                () -> this.storeSessionClient.doBatch(this, this.getBatchId(), entries),
                e -> true
        ).orElse(false);
    }

    @Override
    public byte[] get(String table, HgOwnerKey ownerKey) {
        return this.notifier.invoke(
                () -> this.storeSessionClient.doGet(this, table, ownerKey)
                ,
                e -> e.getValueResponse().getValue().toByteArray()
        ).orElse(HgStoreClientConst.EMPTY_BYTES);
    }

    @Override
    public boolean clean(int partId) {
        return this.notifier.invoke(
                () -> this.storeSessionClient.doClean(this, partId)
                ,
                e -> true

        ).orElse(false);
    }

    @Override
    public List<HgKvEntry> batchGetOwner(String table, List<HgOwnerKey> keyList) {
        return this.notifier.invoke(
                           () -> this.storeSessionClient.doBatchGet(this, table, keyList),
                           e -> e.getKeyValueResponse().getKvList()
                                 .stream()
                                 .map(kv -> (HgKvEntry) new GrpcKvEntryImpl(kv.getKey().toByteArray()
                                         , kv.getValue().toByteArray(), kv.getCode())
                                 )
                                 .collect(Collectors.toList()))
                            .orElse((List<HgKvEntry>) HgStoreClientConst.EMPTY_LIST);
    }

    @Override
    public HgKvIterator<HgKvEntry> batchPrefix(String table, List<HgOwnerKey> keyList) {
        return GrpcKvIteratorImpl.of(this,
                                     this.storeStreamClient.doBatchScanOneShot(this,
                                                                               HgScanQuery.prefixOf(
                                                                                       table,
                                                                                       keyList))
        );
    }

    @Override
    public boolean existsTable(String table) {
        return this.notifier.invoke(
                           () -> this.storeSessionClient.doTable(this, table,
                                                                 TableMethod.TABLE_METHOD_EXISTS),
                           e -> true)
                            .orElse(false);
    }

    @Override
    public boolean createTable(String table) {
        return this.notifier.invoke(
                           () -> this.storeSessionClient.doTable(this, table,
                                                                 TableMethod.TABLE_METHOD_CREATE),
                           e -> true)
                            .orElse(false);
    }

    @Override
    public boolean deleteTable(String table) {
        return this.notifier.invoke(
                           () -> this.storeSessionClient.doTable(this, table,
                                                                 TableMethod.TABLE_METHOD_DELETE),
                           e -> true)
                            .orElse(false);
    }

    @Override
    public boolean dropTable(String table) {
        return this.notifier.invoke(
                           () -> this.storeSessionClient.doTable(this, table,
                                                                 TableMethod.TABLE_METHOD_DROP),
                           e -> true)
                            .orElse(false);
    }

    @Override
    public boolean deleteGraph(String graph) {
        return this.notifier.invoke(
                           () -> this.storeSessionClient.doGraph(this, graph,
                                                                 GraphMethod.GRAPH_METHOD_DELETE),
                           e -> true)
                            .orElse(false);
    }

    @Override
    public boolean truncate() {
        return this.notifier.invoke(
                           () -> this.storeSessionClient.doTable(this,
                                                                 HgStoreClientConst.EMPTY_TABLE
                                   , TableMethod.TABLE_METHOD_TRUNCATE),
                           e -> true)
                            .orElse(false);
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table) {
        return GrpcKvIteratorImpl.of(this, this.storeStreamClient.doScan(this, table, 0));
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, long limit) {
        return this.switcher.invoke(getSwitcherSupplier(limit)
                , () -> GrpcKvIteratorImpl.of(this, this.storeStreamClient.doScan
                                                                                  (this, table,
                                                                                   limit))
                , () -> GrpcKvIteratorImpl.of(this, this.storeStreamClient.doScanOneShot
                                                                                  (this, table,
                                                                                   limit))
        ).get();
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(ScanStreamReq.Builder builder) {
        HgStoreStreamStub stub = getStub();
        KvPageScanner scanner = new KvPageScanner(this,
                                                  stub,
                                                  builder);
        return GrpcKvIteratorImpl.of(this, scanner);
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, byte[] query) {
        return GrpcKvIteratorImpl.of(this, this.storeStreamClient.doScan(this, table, 0, query));
    }

    private HgStoreStreamStub getStub() {
        return this.storeStreamClient.getStub(this);
    }

    // @Override
    // public HgKvIterator<HgKvEntry> scanIterator(ScanStreamReq scanReq) {
    //    KvPageScanner6 scanner = new KvPageScanner6(this,
    //                                                getStub(),
    //                                                scanReq.toBuilder());
    //    return GrpcKvIteratorImpl.of(this, scanner);
    // }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, long limit, byte[] query) {
        return this.switcher.invoke(getSwitcherSupplier(limit)
                , () -> GrpcKvIteratorImpl.of(this, this.storeStreamClient.doScan
                                                                                  (this, table,
                                                                                   limit, query))
                , () -> GrpcKvIteratorImpl.of(this, this.storeStreamClient.doScanOneShot
                                                                                  (this, table,
                                                                                   limit, query))
        ).get();
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey keyPrefix) {
        return GrpcKvIteratorImpl.of(this,
                                     this.storeStreamClient.doScan(this, table, keyPrefix, 0));
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey keyPrefix, long limit) {
        return this.switcher.invoke(getSwitcherSupplier(limit),
                                    () -> GrpcKvIteratorImpl.of(this,
                                                                this.storeStreamClient.doScan(this,
                                                                                              table,
                                                                                              keyPrefix,
                                                                                              limit)),
                                    () -> GrpcKvIteratorImpl.of(this,
                                                                this.storeStreamClient.doScanOneShot(
                                                                        this,
                                                                        table,
                                                                        keyPrefix,
                                                                        limit)))
                            .get();
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey keyPrefix, long limit,
                                                byte[] query) {
        return this.switcher.invoke(getSwitcherSupplier(limit),
                                    () -> GrpcKvIteratorImpl.of(this,
                                                                this.storeStreamClient.doScan(
                                                                        this,
                                                                        table,
                                                                        keyPrefix,
                                                                        limit,
                                                                        query)),
                                    () -> GrpcKvIteratorImpl.of(this,
                                                                this.storeStreamClient.doScanOneShot(
                                                                        this,
                                                                        table,
                                                                        keyPrefix,
                                                                        limit,
                                                                        query)))
                            .get();
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey,
                                                HgOwnerKey endKey) {
        return scanIterator(table, startKey, endKey, 0, HgKvStore.SCAN_ANY, null);
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey,
                                                HgOwnerKey endKey, long limit) {
        return scanIterator(table, startKey, endKey, limit,
                            HgStoreClientUtil.isValid(endKey) ? HgStoreClientConst.SCAN_TYPE_RANGE :
                            HgStoreClientConst.SCAN_TYPE_ANY, null);
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey,
                                                HgOwnerKey endKey, long limit, byte[] query) {
        return scanIterator(table, startKey, endKey, limit,
                            HgStoreClientUtil.isValid(endKey) ? HgStoreClientConst.SCAN_TYPE_RANGE :
                            HgStoreClientConst.SCAN_TYPE_ANY, query);
    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey,
                                                HgOwnerKey endKey,
                                                long limit, int scanType, byte[] query) {

        return this.switcher.invoke(getSwitcherSupplier(limit),
                                    () -> GrpcKvIteratorImpl.of(this,
                                                                this.storeStreamClient.doScan(
                                                                        this,
                                                                        table,
                                                                        startKey,
                                                                        endKey,
                                                                        limit,
                                                                        scanType,
                                                                        query)),
                                    () -> GrpcKvIteratorImpl.of(this,
                                                                this.storeStreamClient.doScanOneShot(
                                                                        this,
                                                                        table,
                                                                        startKey,
                                                                        endKey,
                                                                        limit,
                                                                        scanType,
                                                                        query)))
                            .get();

    }

    @Override
    public HgKvIterator<HgKvEntry> scanIterator(String table, int codeFrom, int codeTo,
                                                int scanType, byte[] query) {
        //TODO: Should be changed when start using hashcode as partitionId.
        if (log.isDebugEnabled()) {
            log.debug("scanIterator-scanType: {}", scanType);
        }
        return GrpcKvIteratorImpl.of(this,
                                     this.storeStreamClient.doScan(this, table
                                             , HgOwnerKey.newEmpty().codeToKey(codeFrom)
                                             , HgOwnerKey.newEmpty().codeToKey(codeTo)
                                             , HgStoreClientConst.NO_LIMIT
                                             , HgKvStore.SCAN_PREFIX_BEGIN |
                                               HgKvStore.SCAN_HASHCODE | scanType
                                             , query
                                     )
        );
    }

    @Override
    public List<HgKvIterator<HgKvEntry>> scanBatch(HgScanQuery scanQuery) {
        return Collections.singletonList(GrpcKvIteratorImpl.of(this,
                                                               this.storeStreamClient.doBatchScan(
                                                                       this, scanQuery)
        ));
    }

    @Override
    public KvCloseableIterator<HgKvIterator<HgKvEntry>> scanBatch2(HgScanQuery scanQuery) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public KvCloseableIterator<HgKvIterator<HgKvEntry>> scanBatch3(HgScanQuery scanQuery,
                                                                   KvCloseableIterator iterator) {
        return this.storeStreamClient.doBatchScan3(this, scanQuery, iterator);
    }

    private Supplier<Boolean> getSwitcherSupplier(long limit) {
        return () -> limit <= 0 || limit > hgStoreClientConfig.getNetKvScannerPageSize();
    }

    @Override
    public String toString() {
        return "storeNodeSession: {" + storeNode + ", graphName: \"" + graphName + "\"}";
    }

    @Override
    public List<HgKvIterator<BaseElement>> query(StoreQueryParam query,
                                                 HugeGraphSupplier supplier) throws PDException {
        throw new UnsupportedOperationException("query() not supported yet");
    }
}
