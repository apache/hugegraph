/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.backend.store.hstore;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumn;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.BackendEntryIterator;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.meta.PdMetaDriver.PDAuthConfig;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgScanQuery;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.client.grpc.KvCloseableIterator;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.grpc.common.ScanOrderType;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.StringEncoding;

/**
 * This class is an implementation of HstoreSessions, which is used to manage sessions for HStore
 * backend (Raft + RocksDB).
 * It provides methods to create, drop, and check the existence of tables, as well as to perform
 * CRUD operations on the tables.
 * It also provides methods to manage transactions, such as commit, rollback, and check if there
 * are any changes in the session.
 */
public class HstoreSessionsImpl extends HstoreSessions {

    private static final Set<String> infoInitializedGraph =
            Collections.synchronizedSet(new HashSet<>());
    private static int tableCode = 0;
    private static volatile Boolean initializedNode = Boolean.FALSE;
    private static volatile PDClient defaultPdClient;
    private static volatile HgStoreClient hgStoreClient;
    private final HugeConfig config;
    private final HstoreSession session;
    private final Map<String, Integer> tables;
    private final AtomicInteger refCount;
    private final String graphName;

    public HstoreSessionsImpl(HugeConfig config, String database, String store) {
        super(config, database, store);
        this.config = config;
        this.graphName = database + "/" + store;
        this.initStoreNode(config);
        this.session = new HstoreSession(this.config, graphName);
        this.tables = new ConcurrentHashMap<>();
        this.refCount = new AtomicInteger(1);
    }

    public static HgStoreClient getHgStoreClient() {
        return hgStoreClient;
    }

    public static PDClient getDefaultPdClient() {
        return defaultPdClient;
    }

    public static byte[] encode(String string) {
        return StringEncoding.encode(string);
    }

    public static String decode(byte[] bytes) {
        return StringEncoding.decode(bytes);
    }

    private void initStoreNode(HugeConfig config) {
        if (!initializedNode) {
            synchronized (this) {
                if (!initializedNode) {
                    PDConfig pdConfig = PDConfig.of(config.get(CoreOptions.PD_PEERS))
                                                .setAuthority(PDAuthConfig.service(), PDAuthConfig.token())
                                                .setEnableCache(true);
                    defaultPdClient = PDClient.create(pdConfig);
                    hgStoreClient = HgStoreClient.create(defaultPdClient);
                    initializedNode = Boolean.TRUE;
                }
            }
        }
    }

    @Override
    public void open() throws Exception {
        if (!infoInitializedGraph.contains(this.graphName)) {
            synchronized (infoInitializedGraph) {
                if (!infoInitializedGraph.contains(this.graphName)) {
                    Integer partitionCount = this.config.get(HstoreOptions.PARTITION_COUNT);
                    Assert.assertTrue("The value of hstore.partition_count" +
                                      " cannot be less than 0.", partitionCount > -1);
                    defaultPdClient.setGraph(Metapb.Graph.newBuilder()
                                                         .setGraphName(this.graphName)
                                                         .setPartitionCount(partitionCount)
                                                         .build());
                    infoInitializedGraph.add(this.graphName);
                }
            }
        }
        this.session.open();
    }

    @Override
    protected boolean opened() {
        return this.session != null;
    }

    @Override
    public Set<String> openedTables() {
        return this.tables.keySet();
    }

    @Override
    public synchronized void createTable(String... tables) {
        for (String table : tables) {
            this.session.createTable(table);
            this.tables.put(table, tableCode++);
        }
    }

    @Override
    public synchronized void dropTable(String... tables) {
        for (String table : tables) {
            this.session.dropTable(table);
            this.tables.remove(table);
        }
    }

    @Override
    public boolean existsTable(String table) {
        return this.session.existsTable(table);
    }

    @Override
    public void truncateTable(String table) {
        this.session.truncateTable(table);
    }

    @Override
    public void clear() {
        this.session.deleteGraph();
        try {
            hgStoreClient.getPdClient().delGraph(this.graphName);
        } catch (PDException ignored) {

        }
    }

    @Override
    public final Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected final Session newSession() {
        return new HstoreSession(this.config(), this.graphName);
    }

    @Override
    protected synchronized void doClose() {
        this.checkValid();
        if (this.refCount != null) {
            if (this.refCount.decrementAndGet() > 0) {
                return;
            }
            if (this.refCount.get() != 0) {
                return;
            }
        }
        assert this.refCount.get() == 0;
        this.tables.clear();
        this.session.close();
    }

    private void checkValid() {
    }

    private static class ColumnIterator<T extends HgKvIterator> implements
                                                                BackendColumnIterator,
                                                                Countable {

        private final T iter;
        private final byte[] keyBegin;
        private final byte[] keyEnd;
        private final int scanType;
        private final String table;
        private final byte[] value;
        private boolean gotNext;
        private byte[] position;

        public ColumnIterator(String table, T results) {
            this(table, results, null, null, 0);
        }

        public ColumnIterator(String table, T results, byte[] keyBegin,
                              byte[] keyEnd, int scanType) {
            E.checkNotNull(results, "results");
            this.table = table;
            this.iter = results;
            this.keyBegin = keyBegin;
            this.keyEnd = keyEnd;
            this.scanType = scanType;
            this.value = null;
            if (this.iter.hasNext()) {
                this.iter.next();
                this.gotNext = true;
                this.position = iter.position();
            } else {
                this.gotNext = false;
                // QUESTION: Resetting the position may result in the caller being unable to
                //           retrieve the corresponding position.
                this.position = null;
            }
            if (!ArrayUtils.isEmpty(this.keyBegin) ||
                !ArrayUtils.isEmpty(this.keyEnd)) {
                this.checkArguments();
            }

        }

        public T iter() {
            return iter;
        }

        private void checkArguments() {
            E.checkArgument(!(this.match(Session.SCAN_PREFIX_BEGIN) &&
                              this.match(Session.SCAN_PREFIX_END)),
                            "Can't set SCAN_PREFIX_WITH_BEGIN and " +
                            "SCAN_PREFIX_WITH_END at the same time");

            E.checkArgument(!(this.match(Session.SCAN_PREFIX_BEGIN) &&
                              this.match(Session.SCAN_GT_BEGIN)),
                            "Can't set SCAN_PREFIX_WITH_BEGIN and " +
                            "SCAN_GT_BEGIN/SCAN_GTE_BEGIN at the same time");

            E.checkArgument(!(this.match(Session.SCAN_PREFIX_END) &&
                              this.match(Session.SCAN_LT_END)),
                            "Can't set SCAN_PREFIX_WITH_END and " +
                            "SCAN_LT_END/SCAN_LTE_END at the same time");

            if (this.match(Session.SCAN_PREFIX_BEGIN) && !matchHash()) {
                E.checkArgument(this.keyBegin != null,
                                "Parameter `keyBegin` can't be null " +
                                "if set SCAN_PREFIX_WITH_BEGIN");
                E.checkArgument(this.keyEnd == null,
                                "Parameter `keyEnd` must be null " +
                                "if set SCAN_PREFIX_WITH_BEGIN");
            }

            if (this.match(Session.SCAN_PREFIX_END) && !matchHash()) {
                E.checkArgument(this.keyEnd != null,
                                "Parameter `keyEnd` can't be null " +
                                "if set SCAN_PREFIX_WITH_END");
            }

            if (this.match(Session.SCAN_GT_BEGIN) && !matchHash()) {
                E.checkArgument(this.keyBegin != null,
                                "Parameter `keyBegin` can't be null " +
                                "if set SCAN_GT_BEGIN or SCAN_GTE_BEGIN");
            }

            if (this.match(Session.SCAN_LT_END) && !matchHash()) {
                E.checkArgument(this.keyEnd != null,
                                "Parameter `keyEnd` can't be null " +
                                "if set SCAN_LT_END or SCAN_LTE_END");
            }
        }

        private boolean matchHash() {
            return this.scanType == Session.SCAN_HASHCODE;
        }

        private boolean match(int expected) {
            return Session.matchScanType(expected, this.scanType);
        }

        @Override
        public boolean hasNext() {
            if (gotNext) {
                this.position = this.iter.position();
            } else {
                // QUESTION: Resetting the position may result in the caller being unable to
                //           retrieve the corresponding position.
                this.position = null;
            }
            return gotNext;
        }

        private boolean filter(byte[] key) {
            if (this.match(Session.SCAN_PREFIX_BEGIN)) {
                /*
                 * Prefix with `keyBegin`?
                 * TODO: use custom prefix_extractor instead
                 *       or use ReadOptions.prefix_same_as_start
                 */
                return Bytes.prefixWith(key, this.keyBegin);
            } else if (this.match(Session.SCAN_PREFIX_END)) {
                /*
                 * Prefix with `keyEnd`?
                 * like the following query for range index:
                 *  key > 'age:20' and prefix with 'age'
                 */
                assert this.keyEnd != null;
                return Bytes.prefixWith(key, this.keyEnd);
            } else if (this.match(Session.SCAN_LT_END)) {
                /*
                 * Less (equal) than `keyEnd`?
                 * NOTE: don't use BytewiseComparator due to signed byte
                 */
                if ((this.scanType | Session.SCAN_HASHCODE) != 0) {
                    return true;
                }
                assert this.keyEnd != null;
                if (this.match(Session.SCAN_LTE_END)) {
                    // Just compare the prefix, can be there are excess tail
                    key = Arrays.copyOfRange(key, 0, this.keyEnd.length);
                    return Bytes.compare(key, this.keyEnd) <= 0;
                } else {
                    return Bytes.compare(key, this.keyEnd) < 0;
                }
            } else {
                assert this.match(Session.SCAN_ANY) || this.match(Session.SCAN_GT_BEGIN) ||
                       this.match(
                               Session.SCAN_GTE_BEGIN) : "Unknown scan type";
                return true;
            }
        }

        @Override
        public BackendColumn next() {
            BackendEntryIterator.checkInterrupted();
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            BackendColumn col =
                    BackendColumn.of(this.iter.key(),
                                     this.iter.value());
            if (this.iter.hasNext()) {
                gotNext = true;
                this.iter.next();
            } else {
                gotNext = false;
            }
            return col;
        }

        @Override
        public long count() {
            long count = 0L;
            while (this.hasNext()) {
                this.next();
                count++;
                BackendEntryIterator.checkInterrupted();
            }
            return count;
        }

        @Override
        public byte[] position() {
            return this.position;
        }

        @Override
        public void close() {
            if (this.iter != null) {
                this.iter.close();
            }
        }
    }

    /**
     * HstoreSession implement for hstore
     */
    private final class HstoreSession extends Session {

        private static final boolean TRANSACTIONAL = true;
        private final HgStoreSession graph;
        int changedSize = 0;

        public HstoreSession(HugeConfig conf, String graphName) {
            setGraphName(graphName);
            setConf(conf);
            this.graph = hgStoreClient.openSession(graphName);
        }

        @Override
        public void open() {
            this.opened = true;
        }

        @Override
        public void close() {
            this.opened = false;
        }

        @Override
        public void reset() {
            if (this.changedSize != 0) {
                this.rollback();
                this.changedSize = 0;
            }
        }

        /**
         * Any change in the session
         */
        @Override
        public boolean hasChanges() {
            return this.changedSize > 0;
        }

        /**
         * Commit all updates(put/delete) to DB
         */
        @Override
        public Integer commit() {
            if (!this.hasChanges()) {
                // TODO: log a message with level WARNING
                return 0;
            }
            int commitSize = this.changedSize;
            if (TRANSACTIONAL) {
                this.graph.commit();
            }
            this.changedSize = 0;
            return commitSize;
        }

        /**
         * Rollback all updates(put/delete) not committed
         */
        @Override
        public void rollback() {
            if (TRANSACTIONAL) {
                this.graph.rollback();
            }
            this.changedSize = 0;
        }

        @Override
        public void createTable(String tableName) {
            this.graph.createTable(tableName);
        }

        @Override
        public void dropTable(String tableName) {
            this.graph.dropTable(tableName);
        }

        @Override
        public boolean existsTable(String tableName) {
            return this.graph.existsTable(tableName);
        }

        @Override
        public void truncateTable(String tableName) {
            this.graph.deleteTable(tableName);
        }

        @Override
        public void deleteGraph() {
            this.graph.deleteGraph(this.getGraphName());
        }

        @Override
        public Pair<byte[], byte[]> keyRange(String table) {
            return null;
        }

        private void prepare() {
            if (!this.hasChanges() && TRANSACTIONAL) {
                this.graph.beginTx();
            }
            this.changedSize++;
        }

        /**
         * Add a KV record to a table
         */
        @Override
        public void put(String table, byte[] ownerKey, byte[] key,
                        byte[] value) {
            prepare();
            this.graph.put(table, HgOwnerKey.of(ownerKey, key), value);
        }

        @Override
        public synchronized void increase(String table, byte[] ownerKey,
                                          byte[] key, byte[] value) {
            prepare();
            this.graph.merge(table, HgOwnerKey.of(ownerKey, key), value);
        }

        @Override
        public void delete(String table, byte[] ownerKey, byte[] key) {
            prepare();
            this.graph.delete(table, HgOwnerKey.of(ownerKey, key));
        }

        @Override
        public void deletePrefix(String table, byte[] ownerKey, byte[] key) {
            prepare();
            this.graph.deletePrefix(table, HgOwnerKey.of(ownerKey, key));
        }

        /**
         * Delete a range of keys from a table
         */
        @Override
        public void deleteRange(String table, byte[] ownerKeyFrom,
                                byte[] ownerKeyTo, byte[] keyFrom,
                                byte[] keyTo) {
            prepare();
            this.graph.deleteRange(table, HgOwnerKey.of(ownerKeyFrom, keyFrom),
                                   HgOwnerKey.of(ownerKeyTo, keyTo));
        }

        @Override
        public byte[] get(String table, byte[] key) {
            return this.graph.get(table, HgOwnerKey.of(
                    HgStoreClientConst.ALL_PARTITION_OWNER, key));
        }

        @Override
        public byte[] get(String table, byte[] ownerKey, byte[] key) {
            byte[] values = this.graph.get(table, HgOwnerKey.of(ownerKey, key));
            return values != null ? values : new byte[0];
        }

        @Override
        public void beginTx() {
            this.graph.beginTx();
        }

        @Override
        public BackendColumnIterator scan(String table) {
            assert !this.hasChanges();
            return new ColumnIterator<>(table, this.graph.scanIterator(table));
        }

        @Override
        public BackendColumnIterator scan(String table,
                                          byte[] conditionQueryToByte) {
            assert !this.hasChanges();
            HgKvIterator<HgKvEntry> results = this.graph.scanIterator(table, conditionQueryToByte);
            return new ColumnIterator<>(table, results);
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] ownerKey,
                                          byte[] prefix) {
            assert !this.hasChanges();
            HgKvIterator<HgKvEntry> result = this.graph.scanIterator(table, HgOwnerKey.of(ownerKey,
                                                                                          prefix));
            return new ColumnIterator<>(table, result);
        }

        @Override
        public List<BackendColumnIterator> scan(String table,
                                                List<HgOwnerKey> keys,
                                                int scanType, long limit,
                                                byte[] query) {
            HgScanQuery scanQuery = HgScanQuery.prefixOf(table, keys).builder()
                                               .setScanType(scanType)
                                               .setQuery(query)
                                               .setPerKeyLimit(limit).build();
            List<HgKvIterator<HgKvEntry>> scanIterators =
                    this.graph.scanBatch(scanQuery);
            LinkedList<BackendColumnIterator> columnIterators =
                    new LinkedList<>();
            scanIterators.forEach(item -> {
                columnIterators.add(
                        new ColumnIterator<>(table, item));
            });
            return columnIterators;
        }

        @Override
        public BackendEntry.BackendIterator<BackendColumnIterator> scan(
                String table,
                Iterator<HgOwnerKey> keys,
                int scanType, Query queryParam, byte[] query) {
            ScanOrderType orderType;
            switch (queryParam.orderType()) {
                case ORDER_NONE:
                    orderType = ScanOrderType.ORDER_NONE;
                    break;
                case ORDER_WITHIN_VERTEX:
                    orderType = ScanOrderType.ORDER_WITHIN_VERTEX;
                    break;
                case ORDER_STRICT:
                    orderType = ScanOrderType.ORDER_STRICT;
                    break;
                default:
                    throw new RuntimeException("not implement");
            }
            HgScanQuery scanQuery = HgScanQuery.prefixIteratorOf(table, keys)
                                               .builder()
                                               .setScanType(scanType)
                                               .setQuery(query)
                                               .setPerKeyMax(queryParam.limit())
                                               .setOrderType(orderType)
                                               .setOnlyKey(
                                                       !queryParam.withProperties())
                                               .setSkipDegree(
                                                       queryParam.skipDegree())
                                               .build();
            KvCloseableIterator<HgKvIterator<HgKvEntry>> scanIterators =
                    this.graph.scanBatch2(scanQuery);
            return new BackendEntry.BackendIterator<BackendColumnIterator>() {
                @Override
                public void close() {
                    scanIterators.close();
                }

                @Override
                public byte[] position() {
                    throw new NotImplementedException();
                }

                @Override
                public boolean hasNext() {
                    return scanIterators.hasNext();
                }

                @Override
                public BackendColumnIterator next() {
                    return new ColumnIterator<HgKvIterator>(table, scanIterators.next());
                }
            };
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] ownerKeyFrom,
                                          byte[] ownerKeyTo,
                                          byte[] keyFrom, byte[] keyTo,
                                          int scanType) {
            assert !this.hasChanges();
            HgKvIterator<HgKvEntry> result = this.graph.scanIterator(table,
                                                                     HgOwnerKey.of(ownerKeyFrom,
                                                                                   keyFrom),
                                                                     HgOwnerKey.of(ownerKeyTo,
                                                                                   keyTo),
                                                                     0, scanType, null);
            return new ColumnIterator<>(table, result, keyFrom, keyTo, scanType);
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] ownerKeyFrom,
                                          byte[] ownerKeyTo,
                                          byte[] keyFrom, byte[] keyTo,
                                          int scanType, byte[] query) {
            assert !this.hasChanges();
            HgKvIterator<HgKvEntry> result = this.graph.scanIterator(table,
                                                                     HgOwnerKey.of(
                                                                             ownerKeyFrom,
                                                                             keyFrom),
                                                                     HgOwnerKey.of(
                                                                             ownerKeyTo,
                                                                             keyTo),
                                                                     0,
                                                                     scanType,
                                                                     query);
            return new ColumnIterator<>(table, result, keyFrom, keyTo,
                                        scanType);
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] ownerKeyFrom,
                                          byte[] ownerKeyTo,
                                          byte[] keyFrom, byte[] keyTo,
                                          int scanType, byte[] query,
                                          byte[] position) {
            assert !this.hasChanges();
            HgKvIterator<HgKvEntry> result = this.graph.scanIterator(table,
                                                                     HgOwnerKey.of(
                                                                             ownerKeyFrom,
                                                                             keyFrom),
                                                                     HgOwnerKey.of(
                                                                             ownerKeyTo,
                                                                             keyTo),
                                                                     0,
                                                                     scanType,
                                                                     query);
            result.seek(position);
            return new ColumnIterator<>(table, result, keyFrom, keyTo,
                                        scanType);
        }

        @Override
        public BackendColumnIterator scan(String table, int codeFrom,
                                          int codeTo, int scanType,
                                          byte[] query) {
            assert !this.hasChanges();
            HgKvIterator<HgKvEntry> iterator =
                    this.graph.scanIterator(table, codeFrom, codeTo, 256,
                                            new byte[0]);
            return new ColumnIterator<>(table, iterator, new byte[0],
                                        new byte[0], scanType);
        }

        @Override
        public BackendColumnIterator scan(String table, int codeFrom,
                                          int codeTo, int scanType,
                                          byte[] query, byte[] position) {
            assert !this.hasChanges();
            HgKvIterator<HgKvEntry> iterator =
                    this.graph.scanIterator(table, codeFrom, codeTo, 256,
                                            new byte[0]);
            iterator.seek(position);
            return new ColumnIterator<>(table, iterator, new byte[0],
                                        new byte[0], scanType);
        }

        @Override
        public BackendColumnIterator getWithBatch(String table,
                                                  List<HgOwnerKey> keys) {
            assert !this.hasChanges();
            HgKvIterator<HgKvEntry> kvIterator =
                    this.graph.batchPrefix(table, keys);
            return new ColumnIterator<>(table, kvIterator);
        }

        @Override
        public void merge(String table, byte[] ownerKey, byte[] key,
                          byte[] value) {
            prepare();
            this.graph.merge(table, HgOwnerKey.of(ownerKey, key), value);
        }

        @Override
        public void setMode(GraphMode mode) {
            // no need to set pd mode
        }

        @Override
        public void truncate() throws Exception {
            this.graph.truncate();
            HstoreSessionsImpl.getDefaultPdClient()
                              .resetIdByKey(this.getGraphName());
        }

        @Override
        public int getActiveStoreSize() {
            try {
                return defaultPdClient.getActiveStores().size();
            } catch (PDException ignore) {
            }
            return 0;
        }
    }
}
