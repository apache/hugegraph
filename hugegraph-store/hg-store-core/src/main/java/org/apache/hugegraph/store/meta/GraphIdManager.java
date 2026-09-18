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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.hugegraph.store.meta.base.DBSessionBuilder;
import org.apache.hugegraph.store.meta.base.PartitionMetaStore;
import org.apache.hugegraph.store.term.Bits;
import org.apache.hugegraph.store.util.HgStoreException;

import com.google.protobuf.Int64Value;

import lombok.extern.slf4j.Slf4j;

/**
 * GraphId Manager, maintains a self-incrementing circular ID, responsible for managing the
 * mapping between GraphName and GraphId.
 */
@Slf4j
public class GraphIdManager extends PartitionMetaStore {

    protected static final String GRAPH_ID_PREFIX = "@GRAPH_ID@";
    // FIXME: we need to ensure the right num & proper logic for it (IMPORTANT)
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
                        id = Int64Value.of(maxGraphID);
                    }
                    l = id.getValue();
                    graphIdCache.put(graphName, l);
                }
            }
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
                        id = Int64Value.of(getCId(GRAPH_ID_PREFIX, maxGraphID - 1));
                        if (id.getValue() == -1) {
                            throw new HgStoreException(HgStoreException.EC_FAIL,
                                                       "The number of graphs exceeds the maximum " +
                                                       "65535");
                        }
                        log.info("partition: {}, Graph ID {} is allocated for graph {}, stack: {}",
                                 this.partitionId, id.getValue(), graphName,
                                 Arrays.toString(Thread.currentThread().getStackTrace()));
                        put(key, id);
                        flush();
                    }
                    l = id.getValue();
                    graphIdCache.put(graphName, l);
                }
            }
        }
        return l;
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
                return true;
            }
            try (var itr = session.sessionOp().scan(VERTEX_TABLE, start)) {
                return itr == null || !itr.hasNext();
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
        byte[] cidNextKey = MetadataKeyHelper.getCidKey(key);
        synchronized (cidLock) {
            Int64Value value = get(Int64Value.parser(), cidNextKey);
            long current = value != null ? value.getValue() : 0L;
            long last = current == 0 ? max - 1 : current - 1;
            // Find an unused cid
            List<Int64Value> ids =
                    scan(Int64Value.parser(), genCIDSlotKey(key, current), genCIDSlotKey(key, max));
            var idSet = ids.stream().map(Int64Value::getValue).collect(Collectors.toSet());

            while (idSet.contains(current) || !checkCount(current)) {
                current++;
            }

            if (current == max - 1) {
                current = 0;
                ids = scan(Int64Value.parser(), genCIDSlotKey(key, current),
                           genCIDSlotKey(key, last));
                idSet = ids.stream().map(Int64Value::getValue).collect(Collectors.toSet());
                while (idSet.contains(current) || !checkCount(current)) {
                    current++;
                }
            }

            if (current == last) {
                return -1;
            }
            // Save current id, mark as used
            put(genCIDSlotKey(key, current), Int64Value.of(current));
            // Save the id for the next traversal
            put(cidNextKey, Int64Value.of(current + 1));
            return current;
        }
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
