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

package org.apache.hugegraph.store.meta;

import static org.apache.hugegraph.store.constant.HugeServerTables.VERTEX_TABLE;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.meta.base.DBSessionBuilder;
import org.apache.hugegraph.store.meta.base.PartitionMetaStore;
import org.apache.hugegraph.store.term.Bits;
import org.apache.hugegraph.store.util.Asserts;
import org.apache.hugegraph.store.util.HgStoreException;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.protobuf.Int64Value;
import com.google.protobuf.InvalidProtocolBufferException;

import lombok.extern.slf4j.Slf4j;

/**
 * GraphId Manager, maintains a self-incrementing circular ID, responsible for managing the
 * mapping between GraphName and GraphId.
 */
@Slf4j
public class GraphIdManager extends PartitionMetaStore {

    protected static final String GRAPH_ID_PREFIX = "@GRAPH_ID@";
    // Missing-graph sentinel; allocatable graph IDs are [0, maxGraphID)
    protected static int maxGraphID = 65535 - 1;
    static Object graphIdLock = new Object();
    static Object cidLock = new Object();
    final DBSessionBuilder sessionBuilder;
    final int partitionId;
    private final Map<String, Long> graphIdCache = new ConcurrentHashMap<>();

    public GraphIdManager(DBSessionBuilder sessionBuilder, int partitionId) {
        super(sessionBuilder, partitionId);
        this.sessionBuilder = sessionBuilder;
        this.partitionId = partitionId;
    }

    /**
     * Get the id of a graph
     */
    public long getGraphId(String graphName) {
        Long l = graphIdCache.get(graphName);
        if (l == null) {
            synchronized (graphIdLock) {
                if ((l = graphIdCache.get(graphName)) == null) {
                    byte[] key = MetadataKeyHelper.getGraphIDKey(graphName);
                    Int64Value id = get(Int64Value.parser(), key);
                    if (id == null) {
                        l = (long) maxGraphID;
                    } else {
                        l = checkGraphId(graphName, id.getValue(),
                                         this.partitionId);
                    }
                    graphIdCache.put(graphName, l);
                }
            }
        }
        if (l != maxGraphID) {
            checkGraphId(graphName, l, this.partitionId);
        }
        return l;
    }

    public long getGraphIdOrCreate(String graphName) {

        Long l = graphIdCache.get(graphName);
        if (l == null || l == maxGraphID) {
            synchronized (graphIdLock) {
                if ((l = graphIdCache.get(graphName)) == null || l == maxGraphID) {
                    byte[] key = MetadataKeyHelper.getGraphIDKey(graphName);
                    Int64Value id = get(Int64Value.parser(), key);
                    if (id == null) {
                        try (RocksDBSession session = this.sessionBuilder.getSession(this.partitionId)) {
                            checkLegacyGraphIdData(session, this.partitionId);
                        }
                        id = Int64Value.of(getCId(GRAPH_ID_PREFIX, maxGraphID));
                        if (id.getValue() == -1) {
                            throw new HgStoreException(HgStoreException.EC_FAIL,
                                                       "The number of graphs exceeds the maximum " +
                                                       maxGraphID);
                        }
                        log.info("partition: {}, Graph ID {} is allocated for graph {}, stack: {}",
                                 this.partitionId, id.getValue(), graphName,
                                 Arrays.toString(Thread.currentThread().getStackTrace()));
                        put(key, id);
                        flush();
                    } else {
                        checkGraphId(graphName, id.getValue(),
                                     this.partitionId);
                    }
                    l = id.getValue();
                    graphIdCache.put(graphName, l);
                }
            }
        }
        checkGraphId(graphName, l, this.partitionId);
        return l;
    }

    public static long checkGraphId(String graphName, long graphId,
                                    int partitionId) {
        if (graphId < 0L || graphId >= maxGraphID) {
            throw new HgStoreException(
                    HgStoreException.EC_FAIL,
                    "Invalid graph ID %s for graph '%s' in partition %s, " +
                    "expected a value in [0, %s)",
                    graphId, graphName, partitionId, maxGraphID);
        }
        return graphId;
    }

    public static void checkGraphIds(Map<String, Long> graphIds,
                                     int partitionId) {
        Map<Long, String> graphNames = new HashMap<>();
        graphIds.forEach((graphName, graphId) -> {
            if (graphId == null) {
                throw new HgStoreException(HgStoreException.EC_FAIL,
                                           "Invalid null graph ID for graph '%s' " +
                                           "in partition %s",
                                           graphName, partitionId);
            }
            checkGraphId(graphName, graphId, partitionId);
            String previous = graphNames.putIfAbsent(graphId, graphName);
            if (previous != null && !previous.equals(graphName)) {
                throw new HgStoreException(
                        HgStoreException.EC_FAIL,
                        "Graph ID %s is assigned to multiple graphs '%s' and '%s' " +
                        "in partition %s",
                        graphId, previous, graphName, partitionId);
            }
        });
    }

    public void updateGraphIds(Map<String, Long> graphIds) {
        Map<String, Long> updates = new HashMap<>(graphIds);
        checkGraphIds(updates, this.partitionId);
        synchronized (graphIdLock) {
            Map<String, Long> finalGraphIds = this.graphIds();
            Set<Long> previousGraphIds = updates.keySet().stream()
                                                .map(finalGraphIds::get)
                                                .filter(graphId -> graphId != null &&
                                                                   graphId >= 0L &&
                                                                   graphId < maxGraphID)
                                                .collect(Collectors.toSet());
            finalGraphIds.putAll(updates);
            this.checkUniqueGraphIds(finalGraphIds);
            this.writeGraphIds(updates, previousGraphIds);
            this.graphIdCache.putAll(updates);
        }
    }

    private Map<String, Long> graphIds() {
        byte[] prefix = MetadataKeyHelper.getGraphIDKey("");
        Map<String, Long> graphIds = new HashMap<>();
        for (RocksDBSession.BackendColumn column : scan(prefix)) {
            String graphName = new String(column.name, prefix.length,
                                          column.name.length - prefix.length,
                                          StandardCharsets.UTF_8);
            try {
                long graphId = Int64Value.parseFrom(column.value).getValue();
                graphIds.put(graphName, graphId);
            } catch (InvalidProtocolBufferException e) {
                throw new HgStoreException(HgStoreException.EC_FAIL, e);
            }
        }
        return graphIds;
    }

    private void checkUniqueGraphIds(Map<String, Long> graphIds) {
        Map<Long, String> graphNames = new HashMap<>();
        graphIds.forEach((graphName, graphId) -> {
            // Ignore legacy invalid IDs here so this repair path can replace them.
            if (graphId < 0L || graphId >= maxGraphID) {
                return;
            }
            String previous = graphNames.putIfAbsent(graphId, graphName);
            if (previous != null && !previous.equals(graphName)) {
                throw new HgStoreException(
                        HgStoreException.EC_FAIL,
                        "Graph ID %s is assigned to multiple graphs '%s' and '%s' " +
                        "in partition %s",
                        graphId, previous, graphName, this.partitionId);
            }
        });
    }

    private void writeGraphIds(Map<String, Long> graphIds,
                               Set<Long> previousGraphIds) {
        try (RocksDBSession dbSession = getRocksDBSession()) {
            SessionOperator operator = dbSession.sessionOp();
            try {
                operator.prepare();
                // A repaired mapping does not prove that the old graph's data
                // prefix is empty. Keep its ID reserved even if the slot was
                // absent in legacy metadata.
                for (Long graphId : previousGraphIds) {
                    byte[] slotKey = genCIDSlotKey(GRAPH_ID_PREFIX, graphId);
                    operator.put(getCFName(), slotKey,
                                 Int64Value.of(graphId).toByteArray());
                }
                // Publish mappings only after all IDs are reserved in the same batch.
                for (Map.Entry<String, Long> entry : graphIds.entrySet()) {
                    Int64Value value = Int64Value.of(entry.getValue());
                    byte[] slotKey = genCIDSlotKey(GRAPH_ID_PREFIX,
                                                  entry.getValue());
                    operator.put(getCFName(), slotKey, value.toByteArray());
                }
                for (Map.Entry<String, Long> entry : graphIds.entrySet()) {
                    Int64Value value = Int64Value.of(entry.getValue());
                    byte[] graphIdKey =
                            MetadataKeyHelper.getGraphIDKey(entry.getKey());
                    operator.put(getCFName(), graphIdKey, value.toByteArray());
                }
                operator.commit();
            } catch (RuntimeException e) {
                operator.rollback();
                throw e;
            }
        }
    }

    /**
     * Release a graph id
     */
    public long releaseGraphId(String graphName) {
        long gid = getGraphId(graphName);
        synchronized (graphIdLock) {
            graphIdCache.remove(graphName);
            byte[] key = MetadataKeyHelper.getGraphIDKey(graphName);
            delete(key);
            delCId(GRAPH_ID_PREFIX, gid);
            flush();
        }
        return gid;
    }

    /**
     * Reject unresolved rows written with the 1.7 missing-graph sentinel.
     * Their graph ownership cannot be inferred from the shared physical prefix.
     */
    public static void checkLegacyGraphIdData(RocksDBSession session, int partitionId) {
        var lock = session.getCfHandleReadLock();
        lock.lock();
        try {
            checkLegacyGraphIdData(session.getDB(), session.getTables(), partitionId);
        } catch (RocksDBException e) {
            throw new HgStoreException("Can't inspect legacy GraphID data in partition " + partitionId, e);
        } finally {
            lock.unlock();
        }
    }

    public static void checkLegacyGraphIdData(String existingDbPath, int partitionId) {
        try (Options options = new Options();
             ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
             DBOptions dbOptions = new DBOptions();
             Logger logger = new Logger(dbOptions) {
                 @Override
                 protected void log(InfoLogLevel level, String message) {
                     // Keep inspection from creating or replacing the DB's LOG file.
                 }
             }) {
            dbOptions.setLogger(logger);
            HugeConfig config = RocksDBFactory.getInstance().getHugeConfig();
            if (config != null) {
                RocksDBSession.initOptions(config, null, null, cfOptions, cfOptions);
            }
            List<byte[]> names = RocksDB.listColumnFamilies(options, existingDbPath);
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            for (byte[] name : names) {
                descriptors.add(new ColumnFamilyDescriptor(name, cfOptions));
            }
            List<ColumnFamilyHandle> handles = new ArrayList<>();
            RocksDB db = null;
            try {
                db = RocksDB.openReadOnly(dbOptions, existingDbPath, descriptors, handles);
                Map<String, ColumnFamilyHandle> tables = new HashMap<>();
                for (int i = 0; i < names.size(); i++) {
                    tables.put(new String(names.get(i), StandardCharsets.UTF_8), handles.get(i));
                }
                checkLegacyGraphIdData(db, tables, partitionId);
            } finally {
                for (ColumnFamilyHandle handle : handles) {
                    handle.close();
                }
                if (db != null) {
                    db.close();
                }
            }
        } catch (RocksDBException e) {
            throw new HgStoreException("Can't inspect legacy GraphID data in partition " + partitionId +
                                       " at " + existingDbPath, e);
        }
    }

    private static void checkLegacyGraphIdData(RocksDB db,
                                               Map<String, ColumnFamilyHandle> tables,
                                               int partitionId) throws RocksDBException {
        byte[] prefix = {(byte) 0xff, (byte) 0xfe};
        for (String table : HugeServerTables.TABLES) {
            ColumnFamilyHandle handle = tables.get(table);
            if (HugeServerTables.UNKNOWN_TABLE.equals(table) || handle == null) {
                continue;
            }
            try (var iterator = db.newIterator(handle)) {
                iterator.seek(prefix);
                iterator.status();
                if (iterator.isValid()) {
                    byte[] key = iterator.key();
                    if (key.length >= prefix.length && key[0] == prefix[0] && key[1] == prefix[1]) {
                        throw new HgStoreException(HgStoreException.EC_FAIL,
                                                   "Legacy GraphID data in partition " + partitionId +
                                                   ", table " + table + ", prefix 0xFFFE; " +
                                                   "stop upgrade and recover the affected data");
                    }
                }
            }
        }
    }

    /**
     * To maintain compatibility with affected graphs, ensure the g+v table contains no data
     *
     * @return Returns false if data exists, true if no data
     */
    private boolean checkCount(long l) {
        var start = new byte[2];
        Bits.putShort(start, 0, (short) l);
        try (var session = sessionBuilder.getSession(partitionId)) {
            if (!session.tableIsExist(VERTEX_TABLE)) {
                // Scanning a missing table creates it and requires a write lock
                return true;
            }
            try (var iterator = session.sessionOp().scan(VERTEX_TABLE, start)) {
                return iterator == null || !iterator.hasNext();
            }
        }
    }

    /**
     * Generate auto-incrementing cyclic unique IDs that reset to 0 upon reaching the upper limit
     *
     * @param key key
     * @param max max id limit, after reaching this value, it will reset to 0 and start
     *            incrementing again.
     * @return id
     */
    protected long getCId(String key, long max) {
        Asserts.isTrue(max > 0L, "The maximum cyclic ID must be positive");
        byte[] cidNextKey = MetadataKeyHelper.getCidKey(key);
        synchronized (cidLock) {
            Int64Value value = get(Int64Value.parser(), cidNextKey);
            long start = Math.floorMod(value != null ? value.getValue() : 0L, max);
            Set<Long> assignedGraphIds = this.assignedGraphIds(key);
            long current = this.findAvailableCId(key, start, max,
                                                 assignedGraphIds);
            if (current == -1L && start > 0L) {
                current = this.findAvailableCId(key, 0L, start,
                                                assignedGraphIds);
            }
            if (current == -1L) {
                return -1L;
            }

            // Save current id, mark as used
            put(genCIDSlotKey(key, current), Int64Value.of(current));
            // Keep the next traversal position inside [0, max)
            long next = current + 1L;
            put(cidNextKey, Int64Value.of(next == max ? 0L : next));
            return current;
        }
    }

    private long findAvailableCId(String key, long start, long end,
                                  Set<Long> assignedGraphIds) {
        Set<Long> idSet = scan(Int64Value.parser(), genCIDSlotKey(key, start),
                               genCIDSlotKey(key, end))
                               .stream()
                               .map(Int64Value::getValue)
                               .collect(Collectors.toSet());
        idSet.addAll(assignedGraphIds);
        for (long current = start; current < end; current++) {
            if (!idSet.contains(current) && checkCount(current)) {
                return current;
            }
        }
        return -1L;
    }

    private Set<Long> assignedGraphIds(String key) {
        if (!GRAPH_ID_PREFIX.equals(key)) {
            return Collections.emptySet();
        }
        // A persisted mapping stays authoritative even if its slot is missing.
        return scan(Int64Value.parser(), MetadataKeyHelper.getGraphIDKey(""))
                .stream()
                .map(Int64Value::getValue)
                .collect(Collectors.toSet());
    }

    /**
     * Return key with used Cid
     */
    public byte[] genCIDSlotKey(String key, long value) {
        byte[] keySlot = MetadataKeyHelper.getCidSlotKeyPrefix(key);
        ByteBuffer buf = ByteBuffer.allocate(keySlot.length + Long.SIZE);
        buf.put(keySlot);
        buf.putLong(value);
        return buf.array();
    }

    /**
     * Delete a loop ID, release the ID value
     */
    protected void delCId(String key, long cid) {
        delete(genCIDSlotKey(key, cid));
    }

}
