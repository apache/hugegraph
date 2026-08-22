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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.apache.hugegraph.store.meta.base.DBSessionBuilder;
import org.apache.hugegraph.store.meta.base.PartitionMetaStore;
import org.apache.hugegraph.store.term.Bits;
import org.apache.hugegraph.store.util.Asserts;
import org.apache.hugegraph.store.util.HgStoreException;

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
            Set<Long> graphIdsToRelease = updates.keySet().stream()
                                                 .map(finalGraphIds::get)
                                                 .filter(graphId -> graphId != null)
                                                 .collect(Collectors.toSet());
            finalGraphIds.putAll(updates);
            this.checkUniqueGraphIds(finalGraphIds);
            graphIdsToRelease.removeAll(finalGraphIds.values());
            this.writeGraphIds(updates, graphIdsToRelease);
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
                               Set<Long> graphIdsToRelease) {
        try (RocksDBSession dbSession = getRocksDBSession()) {
            SessionOperator operator = dbSession.sessionOp();
            try {
                operator.prepare();
                // Release only previous slots no longer referenced after this repair.
                for (Long graphId : graphIdsToRelease) {
                    byte[] slotKey = genCIDSlotKey(GRAPH_ID_PREFIX, graphId);
                    operator.delete(getCFName(), slotKey);
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
