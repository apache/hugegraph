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

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.BackendSession.AbstractBackendSession;
import org.apache.hugegraph.backend.store.BackendSessionPool;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.type.define.GraphMode;

public abstract class HstoreSessions extends BackendSessionPool {

    public HstoreSessions(HugeConfig config, String database, String store) {
        super(config, database + "/" + store);
    }

    public abstract Set<String> openedTables();

    public abstract void createTable(String... tables);

    public abstract void dropTable(String... tables);

    public abstract boolean existsTable(String table);

    public abstract void truncateTable(String table);

    public abstract void clear();

    @Override
    public abstract Session session();

    public interface Countable {

        public long count();
    }

    /**
     * Session for Hstore
     */
    public static abstract class Session extends AbstractBackendSession {

        public static final int SCAN_ANY = 0x80;
        public static final int SCAN_PREFIX_BEGIN = 0x01;
        public static final int SCAN_PREFIX_END = 0x02;
        public static final int SCAN_GT_BEGIN = 0x04;
        public static final int SCAN_GTE_BEGIN = 0x0c;
        public static final int SCAN_LT_END = 0x10;
        public static final int SCAN_LTE_END = 0x30;
        public static final int SCAN_KEY_ONLY = 0x40;
        public static final int SCAN_HASHCODE = 0x100;

        private HugeConfig conf;
        private String graphName;

        public static boolean matchScanType(int expected, int actual) {
            return (expected & actual) == expected;
        }

        public abstract void createTable(String tableName);

        public abstract void dropTable(String tableName);

        public abstract boolean existsTable(String tableName);

        public abstract void truncateTable(String tableName);

        public abstract void deleteGraph();

        public abstract Pair<byte[], byte[]> keyRange(String table);

        public abstract void put(String table, byte[] ownerKey,
                                 byte[] key, byte[] value);

        public abstract void increase(String table, byte[] ownerKey,
                                      byte[] key, byte[] value);

        public abstract void delete(String table, byte[] ownerKey, byte[] key);

        public abstract void deletePrefix(String table, byte[] ownerKey,
                                          byte[] key);

        public abstract void deleteRange(String table, byte[] ownerKeyFrom,
                                         byte[] ownerKeyTo, byte[] keyFrom,
                                         byte[] keyTo);

        public abstract byte[] get(String table, byte[] key);

        public abstract byte[] get(String table, byte[] ownerKey, byte[] key);

        public abstract BackendColumnIterator scan(String table);

        public abstract BackendColumnIterator scan(String table,
                                                   byte[] ownerKey,
                                                   byte[] prefix);

        public BackendColumnIterator scan(String table, byte[] ownerKeyFrom,
                                          byte[] ownerKeyTo, byte[] keyFrom,
                                          byte[] keyTo) {
            return this.scan(table, ownerKeyFrom, ownerKeyTo, keyFrom, keyTo,
                             SCAN_LT_END);
        }

        public abstract List<BackendColumnIterator> scan(String table,
                                                         List<HgOwnerKey> keys,
                                                         int scanType,
                                                         long limit,
                                                         byte[] query);

        public abstract BackendEntry.BackendIterator<BackendColumnIterator> scan(String table,
                                                                                 Iterator<HgOwnerKey> keys,
                                                                                 int scanType,
                                                                                 Query queryParam,
                                                                                 byte[] query);

        public abstract BackendColumnIterator scan(String table,
                                                   byte[] ownerKeyFrom,
                                                   byte[] ownerKeyTo,
                                                   byte[] keyFrom,
                                                   byte[] keyTo,
                                                   int scanType);

        public abstract BackendColumnIterator scan(String table,
                                                   byte[] ownerKeyFrom,
                                                   byte[] ownerKeyTo,
                                                   byte[] keyFrom,
                                                   byte[] keyTo,
                                                   int scanType,
                                                   byte[] query);

        public abstract BackendColumnIterator scan(String table,
                                                   byte[] ownerKeyFrom,
                                                   byte[] ownerKeyTo,
                                                   byte[] keyFrom,
                                                   byte[] keyTo,
                                                   int scanType,
                                                   byte[] query,
                                                   byte[] position);

        public abstract BackendColumnIterator scan(String table,
                                                   int codeFrom,
                                                   int codeTo,
                                                   int scanType,
                                                   byte[] query);

        public abstract BackendColumnIterator scan(String table,
                                                   int codeFrom,
                                                   int codeTo,
                                                   int scanType,
                                                   byte[] query,
                                                   byte[] position);

        public abstract BackendColumnIterator getWithBatch(String table,
                                                           List<HgOwnerKey> keys);

        public abstract void merge(String table, byte[] ownerKey,
                                   byte[] key, byte[] value);

        public abstract void setMode(GraphMode mode);

        public abstract void truncate() throws Exception;

        public abstract BackendColumnIterator scan(String table,
                                                   byte[] conditionQueryToByte);

        public HugeConfig getConf() {
            return conf;
        }

        public void setConf(HugeConfig conf) {
            this.conf = conf;
        }

        public String getGraphName() {
            return graphName;
        }

        public void setGraphName(String graphName) {
            this.graphName = graphName;
        }

        public abstract void beginTx();

        public abstract int getActiveStoreSize();
    }
}
