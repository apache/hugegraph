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

package org.apache.hugegraph.backend.store.rocksdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.BackendSession.AbstractBackendSession;
import org.apache.hugegraph.backend.store.BackendSessionPool;
import org.apache.hugegraph.config.HugeConfig;
import org.rocksdb.RocksDBException;

public abstract class RocksDBSessions extends BackendSessionPool {

    public RocksDBSessions(HugeConfig config, String database, String store) {
        super(config, database + "/" + store);
    }

    public abstract Set<String> openedTables();

    public abstract void createTable(String... tables) throws RocksDBException;

    public abstract void dropTable(String... tables) throws RocksDBException;

    public abstract boolean existsTable(String table);

    public abstract List<String> property(String property);

    public abstract void compactRange();

    public abstract RocksDBSessions copy(HugeConfig config, String database, String store);

    public abstract void createSnapshot(String snapshotPath);

    public abstract void resumeSnapshot(String snapshotPath);

    public abstract String buildSnapshotPath(String snapshotPrefix);

    public abstract String hardLinkSnapshot(String snapshotPath) throws RocksDBException;

    public abstract void reloadRocksDB() throws RocksDBException;

    public abstract void forceCloseRocksDB();

    public boolean databaseOpened() {
        return this.opened();
    }

    public void clearTables(Collection<String> tables) {
        Session session = (Session) this.useSession();
        try {
            List<Pair<String, Pair<byte[], byte[]>>> ranges =
                    new ArrayList<>();
            for (String table : tables) {
                Pair<byte[], byte[]> range = session.keyRange(table);
                if (range != null) {
                    ranges.add(Pair.of(table, range));
                }
            }

            for (Pair<String, Pair<byte[], byte[]>> entry : ranges) {
                String table = entry.getLeft();
                byte[] first = entry.getRight().getLeft();
                byte[] last = entry.getRight().getRight();
                if (!Arrays.equals(first, last)) {
                    session.deleteRange(table, first, last);
                }
                session.delete(table, last);
            }
            session.commit();
        } catch (Throwable e) {
            session.rollback();
            throw e;
        } finally {
            this.close();
        }
    }

    @Override
    public abstract Session session();

    /**
     * Session for RocksDB
     */
    public abstract static class Session extends AbstractBackendSession {

        public static final int SCAN_ANY = 0x80;
        public static final int SCAN_PREFIX_BEGIN = 0x01;
        public static final int SCAN_PREFIX_END = 0x02;
        public static final int SCAN_GT_BEGIN = 0x04;
        public static final int SCAN_GTE_BEGIN = 0x0c;
        public static final int SCAN_LT_END = 0x10;
        public static final int SCAN_LTE_END = 0x30;

        public abstract String dataPath();

        public abstract String walPath();

        public abstract String property(String table, String property);

        public abstract Pair<byte[], byte[]> keyRange(String table);

        public abstract void compactRange(String table);

        public abstract void put(String table, byte[] key, byte[] value);

        public abstract void merge(String table, byte[] key, byte[] value);

        public abstract void increase(String table, byte[] key, byte[] value);

        public abstract void delete(String table, byte[] key);

        public abstract void deleteSingle(String table, byte[] key);

        public abstract void deletePrefix(String table, byte[] key);

        public abstract void deleteRange(String table,
                                         byte[] keyFrom, byte[] keyTo);

        public abstract byte[] get(String table, byte[] key);

        public abstract BackendColumnIterator get(String table, List<byte[]> keys);

        public abstract BackendColumnIterator scan(String table);

        public abstract BackendColumnIterator scan(String table, byte[] prefix);

        public abstract BackendColumnIterator scan(String table, byte[] keyFrom,
                                                   byte[] keyTo, int scanType);

        public BackendColumnIterator scan(String table, byte[] keyFrom, byte[] keyTo) {
            return this.scan(table, keyFrom, keyTo, SCAN_LT_END);
        }

        public static boolean matchScanType(int expected, int actual) {
            return (expected & actual) == expected;
        }
    }

    public interface Countable {

        long count();
    }
}
