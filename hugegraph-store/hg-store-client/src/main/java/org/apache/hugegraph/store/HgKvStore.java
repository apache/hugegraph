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

package org.apache.hugegraph.store;

import java.util.List;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.store.client.grpc.KvCloseableIterator;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq;
import org.apache.hugegraph.store.query.StoreQueryParam;
import org.apache.hugegraph.structure.BaseElement;

/**
 * @version 0.2.0
 */
public interface HgKvStore {

    /**
     * CAUTION: THE CONST BELOW MUST KEEP CONSISTENCE TO ScanIterator.Trait.
     */
    int SCAN_ANY = 0x80;
    int SCAN_PREFIX_BEGIN = 0x01;
    int SCAN_PREFIX_END = 0x02;
    int SCAN_GT_BEGIN = 0x04;
    int SCAN_GTE_BEGIN = 0x0c;
    int SCAN_LT_END = 0x10;
    int SCAN_LTE_END = 0x30;
    int SCAN_KEYONLY = 0x40;
    int SCAN_HASHCODE = 0x100;

    boolean put(String table, HgOwnerKey ownerKey, byte[] value);

    /**
     * This version is used internally by the store. Write data to the partition,
     * partitionId and key.keyCode must be consistent with the partition information stored in pd.
     */
    boolean directPut(String table, int partitionId, HgOwnerKey key, byte[] value);

    byte[] get(String table, HgOwnerKey ownerKey);

    boolean clean(int partId);

    boolean delete(String table, HgOwnerKey ownerKey);

    boolean deleteSingle(String table, HgOwnerKey ownerKey);

    boolean deletePrefix(String table, HgOwnerKey prefix);

    boolean deleteRange(String table, HgOwnerKey start, HgOwnerKey end);

    boolean merge(String table, HgOwnerKey key, byte[] value);

    @Deprecated
    List<HgKvEntry> batchGetOwner(String table, List<HgOwnerKey> keyList);

    HgKvIterator<HgKvEntry> scanIterator(String table);

    HgKvIterator<HgKvEntry> scanIterator(String table, byte[] query);

    HgKvIterator<HgKvEntry> scanIterator(String table, long limit);

    HgKvIterator<HgKvEntry> scanIterator(String table, long limit, byte[] query);

    HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey keyPrefix);

    HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey keyPrefix, long limit);

    HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey keyPrefix, long limit,
                                         byte[] query);

    HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey, HgOwnerKey endKey);

    HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey, HgOwnerKey endKey,
                                         long limit);

    HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey, HgOwnerKey endKey,
                                         long limit, byte[] query);

    HgKvIterator<HgKvEntry> scanIterator(String table, HgOwnerKey startKey, HgOwnerKey endKey,
                                         long limit, int scanType, byte[] query);

    /**
     * Scan a range in global key order across all partitions.
     *
     * Low-level node sessions don't provide cross-partition ordering. The
     * multi-node session proxy overrides this capability.
     */
    default HgKvIterator<HgKvEntry> scanIteratorOrdered(
            String table, HgOwnerKey startKey, HgOwnerKey endKey, long limit,
            int scanType, byte[] query) {
        throw new UnsupportedOperationException(
                "Global ordered scan is not supported");
    }

    HgKvIterator<HgKvEntry> scanIterator(String table, int codeFrom, int codeTo, int scanType,
                                         byte[] query);

    // HgKvIterator<HgKvEntry> scanIterator(ScanStreamReq scanReq);

    HgKvIterator<HgKvEntry> scanIterator(ScanStreamReq.Builder scanReqBuilder);

    List<HgKvIterator<BaseElement>> query(StoreQueryParam query, HugeGraphSupplier supplier) throws
                                                                                             PDException;

    boolean truncate();

    default boolean existsTable(String table) {
        return false;
    }

    boolean createTable(String table);

    boolean deleteTable(String table);

    boolean dropTable(String table);

    boolean deleteGraph(String graph);

    List<HgKvIterator<HgKvEntry>> scanBatch(HgScanQuery scanQuery);

    KvCloseableIterator<HgKvIterator<HgKvEntry>> scanBatch2(HgScanQuery scanQuery);

    KvCloseableIterator<HgKvIterator<HgKvEntry>> scanBatch3(HgScanQuery scanQuery,
                                                            KvCloseableIterator iterator);

    HgKvIterator<HgKvEntry> batchPrefix(String table, List<HgOwnerKey> prefixList);
}
