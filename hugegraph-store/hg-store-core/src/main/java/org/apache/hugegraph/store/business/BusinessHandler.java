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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.pd.grpc.pulse.CleanType;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.grpc.Graphpb;
import org.apache.hugegraph.store.grpc.common.Key;
import org.apache.hugegraph.store.grpc.common.OpType;
import org.apache.hugegraph.store.grpc.query.DeDupOption;
import org.apache.hugegraph.store.grpc.session.BatchEntry;
import org.apache.hugegraph.store.meta.base.DBSessionBuilder;
import org.apache.hugegraph.store.metric.HgStoreMetric;
import org.apache.hugegraph.store.query.QueryTypeParam;
import org.apache.hugegraph.store.raft.PartitionStateMachine;
import org.apache.hugegraph.store.term.HgPair;
import org.apache.hugegraph.store.util.HgStoreException;
import org.rocksdb.Cache;
import org.rocksdb.MemoryUsageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public interface BusinessHandler extends DBSessionBuilder {

    Logger log = LoggerFactory.getLogger(PartitionStateMachine.class);
    int compactionCanStart = 0;
    int compactionDone = 1;
    int doing = -1;

    void doPut(String graph, int code, String table, byte[] key, byte[] value) throws
                                                                               HgStoreException;

    byte[] doGet(String graph, int code, String table, byte[] key) throws HgStoreException;

    ScanIterator scanAll(String graph, String table) throws HgStoreException;

    ScanIterator scanAll(String graph, String table, byte[] query) throws HgStoreException;

    ScanIterator scan(String graph, String table, int codeFrom, int codeTo) throws HgStoreException;

    ScanIterator scan(String graph, int code, String table, byte[] start,
                      byte[] end, int scanType) throws HgStoreException;

    default ScanIterator scanOrdered(String graph, String table, byte[] start,
                                     byte[] end, int scanType)
                                     throws HgStoreException {
        throw new UnsupportedOperationException(
                "Ordered scan is not supported");
    }

    /**
     * primary index scan
     */
    ScanIterator scan(String graph, String table, List<QueryTypeParam> params,
                      DeDupOption dedupOption)
            throws HgStoreException;

    ScanIterator scan(String graph, int code, String table, byte[] start, byte[] end, int scanType,
                      byte[] conditionQuery) throws HgStoreException;

    <T> GraphStoreIterator<T> scan(Graphpb.ScanPartitionRequest request);

    ScanIterator scanOriginal(Graphpb.ScanPartitionRequest request);

    ScanIterator scanPrefix(String graph, int code, String table, byte[] prefix,
                            int scanType) throws HgStoreException;

    ScanIterator scanPrefix(String graph, int code, String table, byte[] prefix) throws
                                                                                 HgStoreException;

    ScanIterator scanIndex(String graph, List<List<QueryTypeParam>> param,
                           DeDupOption dedupOption, boolean transElement, boolean filterTTL) throws
                                                                                             HgStoreException;

    ScanIterator scanIndex(String graph, String table, List<List<QueryTypeParam>> params,
                           DeDupOption dedupOption, boolean lookupBack, boolean transKey,
                           boolean filterTTL, int limit)
            throws HgStoreException;

    HgStoreMetric.Partition getPartitionMetric(String graph, int partId,
                                               boolean accurateCount) throws HgStoreException;

    void batchGet(String graph, String table, Supplier<HgPair<Integer, byte[]>> s,
                  Consumer<HgPair<byte[], byte[]>> c) throws HgStoreException;

    void truncate(String graph, int partId) throws HgStoreException;

    void flushAll();

    void closeDB(int partId);

    void closeAll();

    Map<MemoryUsageType, Long> getApproximateMemoryUsageByType(List<Cache> caches);

    List<Integer> getLeaderPartitionIds(String graph);

    Set<Integer> getLeaderPartitionIdSet();

    HgStoreMetric.Graph getGraphMetric(String graph, int partId);

    void saveSnapshot(String snapshotPath, String graph, int partId) throws HgStoreException;

    void loadSnapshot(String snapshotPath, String graph, int partId, long version) throws
                                                                                   HgStoreException;

    long getLatestSequenceNumber(String graph, int partId);

    // Scan partition starting from seqnum for kv
    ScanIterator scanRaw(String graph, int partId, long seqNum) throws HgStoreException;

    void ingestSstFile(String graph, int partId, Map<byte[], List<String>> sstFiles) throws
                                                                                     HgStoreException;

    // Submit partition split, delete old data
    // Delete partition data
    boolean deletePartition(String graph, int partId);

    // Clean up partitions, delete unnecessary data
    boolean cleanPartition(String graph, int partId);

    boolean cleanPartition(String graph, int partId, long startKey, long endKey,
                           CleanType cleanType);

    // All table names for all specified partition graph
    List<String> getTableNames(String graph, int partId);

    TxBuilder txBuilder(String graph, int partId);

    boolean cleanTtl(String graph, int partId, String table, List<ByteString> ids);

    default void doBatch(String graph, int partId, List<BatchEntry> entryList) {
        BusinessHandler.TxBuilder builder = txBuilder(graph, partId);
        BusinessHandler.Tx transaction = builder.build();
        try {
            for (BatchEntry b : entryList) {
                Key start = b.getStartKey();
                String table = HugeServerTables.TABLES[b.getTable()];
                byte[] startKey = start.getKey().toByteArray();
                int number = b.getOpType().getNumber();
                if (number == OpType.OP_TYPE_PUT_VALUE) {
                    builder.put(start.getCode(), table, startKey, b.getValue().toByteArray());
                } else {
                    switch (number) {
                        case OpType.OP_TYPE_DEL_VALUE:
                            builder.del(start.getCode(), table, startKey);
                            continue;
                        case OpType.OP_TYPE_DEL_PREFIX_VALUE:
                            builder.delPrefix(start.getCode(), table, startKey);
                            continue;
                        case OpType.OP_TYPE_DEL_RANGE_VALUE:
                            builder.delRange(start.getCode(), table, startKey,
                                             b.getEndKey().getKey().toByteArray());
                            continue;
                        case OpType.OP_TYPE_DEL_SINGLE_VALUE:
                            builder.delSingle(start.getCode(), table, startKey);
                            continue;
                        case OpType.OP_TYPE_MERGE_VALUE:
                            builder.merge(start.getCode(), table, startKey,
                                          b.getValue().toByteArray());
                            continue;
                        default:
                            throw new IllegalArgumentException(
                                    "unsupported batch-op-type: " + b.getOpType().name());
                    }
                }
            }
            transaction.commit();
        } catch (Throwable e) {
            String msg =
                    String.format("graph data %s-%s do batch insert with error:", graph, partId);
            log.error(msg, e);
            try {
                transaction.rollback();
            } catch (Throwable rollbackError) {
                e.addSuppressed(rollbackError);
            }
            throw e;
        }
    }

    boolean existsTable(String graph, int partId, String table);

    void createTable(String graph, int partId, String table);

    void deleteTable(String graph, int partId, String table);

    void dropTable(String graph, int partId, String table);

    boolean dbCompaction(String graphName, int partitionId);

    boolean dbCompaction(String graphName, int partitionId, String tableName);

    boolean blockingCompact(String graphName, int partitionId);

    void destroyGraphDB(String graphName, int partId) throws HgStoreException;

    long count(String graphName, String table);

    void lock(String path) throws InterruptedException,
                                  TimeoutException;

    void unlock(String path);

    void awaitAndSetLock(int id, int expectedValue, int value) throws InterruptedException,
                                                                      TimeoutException;

    void setAndNotifyState(int id, int state);

    AtomicInteger getState(int id);

    String getLockPath(int partitionId);

    List<Integer> getPartitionIds(String graph);

    @NotThreadSafe
    interface TxBuilder {

        TxBuilder put(int code, String table, byte[] key, byte[] value) throws HgStoreException;

        TxBuilder del(int code, String table, byte[] key) throws HgStoreException;

        TxBuilder delSingle(int code, String table, byte[] key) throws HgStoreException;

        TxBuilder delPrefix(int code, String table, byte[] prefix) throws HgStoreException;

        TxBuilder delRange(int code, String table, byte[] start, byte[] end) throws
                                                                             HgStoreException;

        TxBuilder merge(int code, String table, byte[] key, byte[] value) throws HgStoreException;

        Tx build();
    }

    interface Tx {

        void commit() throws HgStoreException;

        void rollback() throws HgStoreException;
    }
}
