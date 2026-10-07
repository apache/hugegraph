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

package org.apache.hugegraph.backend.store.rocksdbsst;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBIngester;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBStdSessions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.exception.NotSupportException;
import org.apache.hugegraph.util.E;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

public class RocksDBSstSessions extends RocksDBSessions {

    private final String dataPath;
    private final Map<String, SstFileWriter> tables;

    public RocksDBSstSessions(HugeConfig config, String database, String store, String dataPath) {
        super(config, database, store);

        this.dataPath = dataPath;
        this.tables = new ConcurrentHashMap<>();

        File path = new File(this.dataPath);
        if (!path.exists()) {
            E.checkState(path.mkdirs(), "Can't mkdir '%s'", path);
        }
    }

    public RocksDBSstSessions(HugeConfig config, String dataPath, String database, String store,
                              List<String> tableNames) throws RocksDBException {
        this(config, dataPath, database, store);
        for (String table : tableNames) {
            this.createTable(table);
        }
    }

    private RocksDBSstSessions(HugeConfig config, String database, String store,
                               RocksDBSstSessions origin) {
        super(config, database, store);

        this.dataPath = origin.dataPath;
        this.tables = origin.tables;
    }

    @Override
    public void open() throws Exception {
        // pass
    }

    @Override
    protected boolean opened() {
        return true;
    }

    @Override
    public Set<String> openedTables() {
        return this.tables.keySet();
    }

    @Override
    public synchronized void createTable(String... tables) throws RocksDBException {
        for (String table : tables) {
            this.createTable(table);
        }
    }

    private void createTable(String table) throws RocksDBException {
        String number = String.format("%04d", 1);
        Path sstFile = Paths.get(this.dataPath, table, number + RocksDBIngester.SST);
        try {
            FileUtils.forceMkdir(sstFile.toAbsolutePath().getParent().toFile());
        } catch (IOException e) {
            throw new BackendException("Can't make directory for sst: '%s'",
                                       e, sstFile.toString());
        }

        EnvOptions env = new EnvOptions();
        Options options = new Options();
        RocksDBStdSessions.initOptions(this.config(), options, options, options, options);
        // NOTE: unset merge op due to SIGSEGV when cf.setMergeOperatorName()
        options.setMergeOperatorName("not-exist-merge-op");
        SstFileWriter sst = new SstFileWriter(env, options);
        sst.open(sstFile.toString());
        this.tables.put(table, sst);
    }

    @Override
    public synchronized void dropTable(String... tables) throws RocksDBException {
        for (String table : tables) {
            this.dropTable(table);
        }
    }

    public void dropTable(String table) throws RocksDBException {
        try (SstFileWriter sst = this.tables.remove(table)) {
            assert sst == null || !sst.isOwningHandle() : "Please close table before drop to " +
                                                          "ensure call sst.finish()";
        }
    }

    @Override
    public boolean existsTable(String table) {
        return this.tables.containsKey(table);
    }

    @Override
    public List<String> property(String property) {
        throw new UnsupportedOperationException("RocksDBSstStore property()");
    }

    @Override
    public void compactRange() {
        throw new NotSupportException("RocksDBSstStore compactRange()");
    }

    @Override
    public RocksDBSessions copy(HugeConfig config, String database, String store) {
        return new RocksDBSstSessions(config, database, store, this);
    }

    @Override
    public void createSnapshot(String snapshotPath) {
        throw new UnsupportedOperationException("createSnapshot");
    }

    @Override
    public void resumeSnapshot(String snapshotPath) {
        throw new UnsupportedOperationException("resumeSnapshot");
    }

    @Override
    public String buildSnapshotPath(String snapshotPrefix) {
        throw new UnsupportedOperationException("buildSnapshotPath");
    }

    @Override
    public String hardLinkSnapshot(String snapshotPath) {
        throw new UnsupportedOperationException("hardLinkSnapshot");
    }

    @Override
    public void reloadRocksDB() {
        throw new UnsupportedOperationException("reloadRocksDB");
    }

    @Override
    public void forceCloseRocksDB() {
        throw new UnsupportedOperationException("forceCloseRocksDB");
    }

    private SstFileWriter table(String table) {
        SstFileWriter sst = this.tables.get(table);
        if (sst == null) {
            throw new BackendException("Table '%s' is not opened", table);
        }
        return sst;
    }

    @Override
    public final Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected Session newSession() {
        return new SstSession();
    }

    @Override
    protected synchronized void doClose() {
        final String NO_ENTRIES = "Can't create sst file with no entries";

        for (SstFileWriter sst : this.tables.values()) {
            E.checkState(sst.isOwningHandle(), "SstFileWriter closed");
            try {
                sst.finish();
            } catch (RocksDBException e) {
                if (e.getMessage().equals(NO_ENTRIES)) {
                    continue;
                }
                throw new BackendException("Failed to close SstFileWriter", e);
            }
            sst.close();
        }
        this.tables.clear();
    }

    /**
     * SstSession implement for RocksDB
     */
    private final class SstSession extends Session {

        private final Map<String, Changes> batch;

        public SstSession() {
            this.batch = new HashMap<>();
        }

        @Override
        public void open() {
            this.opened = true;
        }

        @Override
        public void close() {
            assert this.closeable();
            this.opened = false;
        }

        /**
         * Any change in the session
         */
        @Override
        public boolean hasChanges() {
            return !this.batch.isEmpty();
        }

        /**
         * Commit all updates(put/delete) to DB
         */
        @Override
        public Integer commit() {
            int count = this.batch.size();
            if (count == 0) {
                return 0;
            }

            try {
                for (Entry<String, Changes> table : this.batch.entrySet()) {
                    if (table.getValue().isEmpty() ||
                        table.getKey().endsWith("i")) {
                        // Skip empty value table or index table
                        continue;
                    }

                    // TODO: limit individual SST file size
                    SstFileWriter sst = table(table.getKey());
                    for (Pair<byte[], byte[]> change : table.getValue()) {
                        sst.put(change.getKey(), change.getValue());
                    }
                }
            } catch (RocksDBException e) {
                throw new BackendException("Failed to commit", e);
            }

            // Clear batch if write() successfully (retained if failed)
            this.batch.clear();

            return count;
        }

        /**
         * Rollback updates not committed in the session
         */
        @Override
        public void rollback() {
            this.batch.clear();
        }

        @Override
        public String dataPath() {
            return RocksDBSstSessions.this.dataPath;
        }

        @Override
        public String walPath() {
            return RocksDBSstSessions.this.dataPath;
        }

        /**
         * Get property value by name from specified table
         */
        @Override
        public String property(String table, String property) {
            throw new NotSupportException("RocksDBSstStore property()");
        }

        @Override
        public Pair<byte[], byte[]> keyRange(String table) {
            return null;
        }

        @Override
        public void compactRange(String table) {
            throw new NotSupportException("RocksDBSstStore compactRange()");
        }

        /**
         * Add a KV record to a table
         */
        @Override
        public void put(String table, byte[] key, byte[] value) {
            Changes changes = this.batch.get(table);
            if (changes == null) {
                changes = new Changes();
                this.batch.put(table, changes);
            }
            changes.add(Pair.of(key, value));
        }

        /**
         * Merge a record to an existing key to a table
         * For more details about merge-operator:
         * <a href="https://github.com/facebook/rocksdb/wiki/merge-operator">...</a>
         */
        @Override
        public void merge(String table, byte[] key, byte[] value) {
            throw new NotSupportException("RocksDBSstStore merge()");
        }

        /**
         * Merge a record to an existing key to a table and commit immediately
         */
        @Override
        public void increase(String table, byte[] key, byte[] value) {
            throw new NotSupportException("RocksDBSstStore increase()");
        }

        /**
         * Delete a record by key from a table
         */
        @Override
        public void delete(String table, byte[] key) {
            throw new NotSupportException("RocksDBSstStore delete()");
        }

        /**
         * Delete the only one version of a record by key from a table
         * NOTE: requires that the key exists and was not overwritten.
         */
        @Override
        public void deleteSingle(String table, byte[] key) {
            throw new NotSupportException("RocksDBSstStore deleteSingle()");
        }

        /**
         * Delete a record by key(or prefix with key) from a table
         */
        @Override
        public void deletePrefix(String table, byte[] key) {
            throw new NotSupportException("RocksDBSstStore deletePrefix()");
        }

        /**
         * Delete a range of keys from a table
         */
        @Override
        public void deleteRange(String table, byte[] keyFrom, byte[] keyTo) {
            throw new NotSupportException("RocksDBSstStore deleteRange()");
        }

        /**
         * Get a record by key from a table
         */
        @Override
        public byte[] get(String table, byte[] key) {
            return null;
        }

        /**
         * Get records by a list of keys from a table
         */
        @Override
        public BackendColumnIterator get(String table, List<byte[]> keys) {
            assert !this.hasChanges();
            return BackendColumnIterator.empty();
        }

        /**
         * Scan all records from a table
         */
        @Override
        public BackendColumnIterator scan(String table) {
            assert !this.hasChanges();
            return BackendColumnIterator.empty();
        }

        /**
         * Scan records by key prefix from a table
         */
        @Override
        public BackendColumnIterator scan(String table, byte[] prefix) {
            assert !this.hasChanges();
            return BackendColumnIterator.empty();
        }

        /**
         * Scan records by key range from a table
         */
        @Override
        public BackendColumnIterator scan(String table, byte[] keyFrom,
                                          byte[] keyTo, int scanType) {
            assert !this.hasChanges();
            return BackendColumnIterator.empty();
        }
    }

    private static class Changes extends ArrayList<Pair<byte[], byte[]>> {

        private static final long serialVersionUID = 9047034706183029125L;
    }
}
