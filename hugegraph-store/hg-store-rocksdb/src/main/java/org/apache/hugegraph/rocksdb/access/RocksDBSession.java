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

package org.apache.hugegraph.rocksdb.access;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.rocksdb.access.util.Asserts;
import org.apache.hugegraph.store.term.HgPair;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ColumnFamilyOptionsInterface;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DBOptionsInterface;
import org.rocksdb.Env;
import org.rocksdb.FlushOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.MutableColumnFamilyOptionsInterface;
import org.rocksdb.MutableDBOptionsInterface;
import org.rocksdb.Options;
import org.rocksdb.Range;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SizeApproximationFlag;
import org.rocksdb.Slice;
import org.rocksdb.Statistics;
import org.rocksdb.WriteBufferManager;
import org.rocksdb.WriteOptions;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RocksDBSession implements AutoCloseable, Cloneable {

    private static final int CPUS = Runtime.getRuntime().availableProcessors();
    final Statistics rocksDbStats;
    final WriteOptions writeOptions;
    final AtomicInteger refCount;
    final AtomicBoolean shutdown;
    final String tempSuffix = "_temp_";
    private final transient String graphName;
    private final HugeConfig hugeConfig;
    private final ReentrantReadWriteLock cfHandleLock;
    private final Map<String, ColumnFamilyHandle> tables;
    private transient String dbPath;
    private RocksDB rocksDB;
    private DBOptions dbOptions;
    private volatile boolean closed = false;

    @Getter
    private Map<String, String> iteratorMap;

    public RocksDBSession(HugeConfig hugeConfig, String dbDataPath, String graphName, long version) {
        this.hugeConfig = hugeConfig;
        this.graphName = graphName;
        this.cfHandleLock = new ReentrantReadWriteLock();
        this.tables = new ConcurrentHashMap<>();
        this.refCount = new AtomicInteger(1);
        this.shutdown = new AtomicBoolean(false);
        this.writeOptions = new WriteOptions();
        this.rocksDbStats = new Statistics();
        this.iteratorMap = new ConcurrentHashMap<>();
        openRocksDB(dbDataPath, version);
    }

    private RocksDBSession(RocksDBSession origin) {
        this.hugeConfig = origin.hugeConfig;
        this.graphName = origin.graphName;
        this.cfHandleLock = origin.cfHandleLock;
        this.tables = origin.tables;
        this.dbPath = origin.dbPath;
        this.rocksDB = origin.rocksDB;
        this.dbOptions = origin.dbOptions;
        this.writeOptions = origin.writeOptions;
        this.rocksDbStats = origin.rocksDbStats;
        this.shutdown = origin.shutdown;
        this.iteratorMap = origin.iteratorMap;
        this.refCount = origin.refCount;
        this.refCount.incrementAndGet();
    }

    /**
     * create directory
     */
    public static boolean createDirectory(String dirPath) {
        Asserts.isTrue(!dirPath.isEmpty(), "dir path is empty");

        File dirObject = new File(dirPath);
        if (!dirObject.exists() || !dirObject.isDirectory()) {
            dirObject.mkdirs();
        }

        return true;
    }

    public static void initOptions(HugeConfig conf,
                                   DBOptionsInterface<?> db,
                                   MutableDBOptionsInterface<?> mdb,
                                   ColumnFamilyOptionsInterface<?> cf,
                                   MutableColumnFamilyOptionsInterface<?> mcf) {
        final boolean optimize = conf.get(RocksDBOptions.OPTIMIZE_MODE);
        if (db != null) {
            /*
             * Set true then the database will be created if it is missing.
             * should we use options.setCreateMissingColumnFamilies()?
             */
            db.setCreateIfMissing(true);
            if (optimize) {
                int processors = RocksDBSession.CPUS;
                db.setIncreaseParallelism(Math.max(processors / 2, 1));
                db.setAllowConcurrentMemtableWrite(true);
                db.setEnableWriteThreadAdaptiveYield(true);
            }
            db.setInfoLogLevel(InfoLogLevel.valueOf(
                    conf.get(RocksDBOptions.LOG_LEVEL) + "_LEVEL"));
            db.setMaxSubcompactions(conf.get(RocksDBOptions.MAX_SUB_COMPACTIONS));
            db.setAllowMmapWrites(conf.get(RocksDBOptions.ALLOW_MMAP_WRITES));
            db.setAllowMmapReads(conf.get(RocksDBOptions.ALLOW_MMAP_READS));
            db.setUseDirectReads(conf.get(RocksDBOptions.USE_DIRECT_READS));
            db.setUseDirectIoForFlushAndCompaction(
                    conf.get(RocksDBOptions.USE_DIRECT_READS_WRITES_FC));
            db.setMaxManifestFileSize(conf.get(RocksDBOptions.MAX_MANIFEST_FILE_SIZE));
            db.setSkipStatsUpdateOnDbOpen(conf.get(RocksDBOptions.SKIP_STATS_UPDATE_ON_DB_OPEN));
            db.setMaxFileOpeningThreads(conf.get(RocksDBOptions.MAX_FILE_OPENING_THREADS));
            db.setDbWriteBufferSize(conf.get(RocksDBOptions.DB_MEMTABLE_SIZE));
            WriteBufferManager bufferManager =
                    (WriteBufferManager) conf.getProperty(RocksDBOptions.WRITE_BUFFER_MANAGER);
            if (bufferManager != null) {
                db.setWriteBufferManager(bufferManager);
                log.info("rocksdb use global WriteBufferManager");
            }
            Env env = (Env) conf.getProperty(RocksDBOptions.ENV);
            if (env != null) {
                db.setEnv(env);
            }
        }

        if (mdb != null) {
            /*
             * Migrate to max_background_jobs option
             * https://github.com/facebook/rocksdb/wiki/Thread-Pool
             * https://github.com/facebook/rocksdb/pull/2205/files
             */
            mdb.setMaxBackgroundJobs(conf.get(RocksDBOptions.MAX_BG_JOBS));
            mdb.setDelayedWriteRate(conf.get(RocksDBOptions.DELAYED_WRITE_RATE));
            mdb.setMaxOpenFiles(conf.get(RocksDBOptions.MAX_OPEN_FILES));
            mdb.setMaxTotalWalSize(conf.get(RocksDBOptions.MAX_TOTAL_WAL_SIZE));
            mdb.setDeleteObsoleteFilesPeriodMicros(
                    1000000 * conf.get(RocksDBOptions.DELETE_OBSOLETE_FILE_PERIOD));
        }

        if (cf != null) {
            if (optimize) {
                // Optimize RocksDB
                cf.optimizeLevelStyleCompaction();
                cf.optimizeUniversalStyleCompaction();
            }

            int numLevels = conf.get(RocksDBOptions.NUM_LEVELS);
            List<CompressionType> compressions = conf.get(
                    RocksDBOptions.LEVELS_COMPRESSIONS);
            E.checkArgument(compressions.isEmpty() ||
                            compressions.size() == numLevels,
                            "Elements number of '%s' must be 0 or " +
                            "be the same as '%s', but got %s != %s",
                            RocksDBOptions.LEVELS_COMPRESSIONS.name(),
                            RocksDBOptions.NUM_LEVELS.name(),
                            compressions.size(), numLevels);

            cf.setNumLevels(numLevels);
            cf.setCompactionStyle(conf.get(RocksDBOptions.COMPACTION_STYLE));

            cf.setBottommostCompressionType(
                    conf.get(RocksDBOptions.BOTTOMMOST_COMPRESSION));
            if (!compressions.isEmpty()) {
                cf.setCompressionPerLevel(compressions);
            }

            cf.setMinWriteBufferNumberToMerge(
                    conf.get(RocksDBOptions.MIN_MEMTABLES_TO_MERGE));
            cf.setMaxWriteBufferNumberToMaintain(
                    conf.get(RocksDBOptions.MAX_MEMTABLES_TO_MAINTAIN));

            cf.setLevelCompactionDynamicLevelBytes(
                    conf.get(RocksDBOptions.DYNAMIC_LEVEL_BYTES));

            // https://github.com/facebook/rocksdb/wiki/Block-Cache
            BlockBasedTableConfig tableConfig =
                    (BlockBasedTableConfig) conf.getProperty(RocksDBOptions.BLOCK_TABLE_CONFIG);

            if (tableConfig != null) {
                cf.setTableFormatConfig(tableConfig);
            }

            cf.setOptimizeFiltersForHits(
                    conf.get(RocksDBOptions.BLOOM_FILTERS_SKIP_LAST_LEVEL));

            // https://github.com/facebook/rocksdb/tree/master/utilities/merge_operators
            cf.setMergeOperatorName("uint64add"); // uint64add/stringappend
        }

        if (mcf != null) {
            mcf.setCompressionType(conf.get(RocksDBOptions.COMPRESSION));

            mcf.setWriteBufferSize(conf.get(RocksDBOptions.MEMTABLE_SIZE));
            mcf.setMaxWriteBufferNumber(conf.get(RocksDBOptions.MAX_MEMTABLES));

            mcf.setMaxBytesForLevelBase(
                    conf.get(RocksDBOptions.MAX_LEVEL1_BYTES));
            mcf.setMaxBytesForLevelMultiplier(
                    conf.get(RocksDBOptions.MAX_LEVEL_BYTES_MULTIPLIER));

            mcf.setTargetFileSizeBase(
                    conf.get(RocksDBOptions.TARGET_FILE_SIZE_BASE));
            mcf.setTargetFileSizeMultiplier(
                    conf.get(RocksDBOptions.TARGET_FILE_SIZE_MULTIPLIER));

            mcf.setLevel0FileNumCompactionTrigger(
                    conf.get(RocksDBOptions.LEVEL0_COMPACTION_TRIGGER));
            mcf.setLevel0SlowdownWritesTrigger(
                    conf.get(RocksDBOptions.LEVEL0_SLOWDOWN_WRITES_TRIGGER));
            mcf.setLevel0StopWritesTrigger(
                    conf.get(RocksDBOptions.LEVEL0_STOP_WRITES_TRIGGER));

            mcf.setSoftPendingCompactionBytesLimit(
                    conf.get(RocksDBOptions.SOFT_PENDING_COMPACTION_LIMIT));
            mcf.setHardPendingCompactionBytesLimit(
                    conf.get(RocksDBOptions.HARD_PENDING_COMPACTION_LIMIT));

            // conf.get(RocksDBOptions.BULKLOAD_MODE);
        }
    }

    @Override
    public RocksDBSession clone() {
        return new RocksDBSession(this);
    }

    public RocksDB getDB() {
        return this.rocksDB;
    }

    public HugeConfig getHugeConfig() {
        return this.hugeConfig;
    }

    public String getGraphName() {
        return this.graphName;
    }

    public WriteOptions getWriteOptions() {
        return this.writeOptions;
    }

    public Map<String, ColumnFamilyHandle> getTables() {
        return tables;
    }

    public ReentrantReadWriteLock.ReadLock getCfHandleReadLock() {
        return this.cfHandleLock.readLock();
    }

    private String findLatestDBPath(String path, long version) {
        File file = new File(path);
        int strIndex = file.getName().indexOf("_");

        String defaultName;
        if (strIndex < 0) {
            defaultName = file.getName();
        } else {
            defaultName = file.getName().substring(0, strIndex);
        }
        String prefix = defaultName + "_";
        File parentFile = new File(file.getParent());
        ArrayList<HgPair<Long, Long>> dbs = new ArrayList<>();
        File[] files = parentFile.listFiles();
        if (files != null) {
            dbs.ensureCapacity(files.length);
            // search all db path
            for (final File sFile : files) {
                final String name = sFile.getName();
                if (!name.startsWith(prefix) && !name.equals(defaultName)) {
                    continue;
                }
                if (name.endsWith(tempSuffix)) {
                    continue;
                }
                long v1 = -1L;
                long v2 = -1L;
                if (name.length() > defaultName.length()) {
                    String[] versions = name.substring(prefix.length()).split("_");
                    if (versions.length == 1) {
                        v1 = Long.parseLong(versions[0]);
                    } else if (versions.length == 2) {
                        v1 = Long.parseLong(versions[0]);
                        v2 = Long.parseLong(versions[1]);
                    } else {
                        continue;
                    }
                }
                dbs.add(new HgPair<>(v1, v2));
            }
        }

        RocksDBFactory factory = RocksDBFactory.getInstance();
        // get last index db path
        String latestDBPath = "";
        if (!dbs.isEmpty()) {

            dbs.sort((o1, o2) -> o1.getKey().equals(o2.getKey()) ?
                                 o1.getValue().compareTo(o2.getValue()) :
                                 o1.getKey().compareTo(o2.getKey()));
            final int dbCount = dbs.size();
            for (int i = 0; i < dbCount; i++) {
                final HgPair<Long, Long> pair = dbs.get(i);
                String curDBName;
                if (pair.getKey() == -1L) {
                    curDBName = defaultName;
                } else if (pair.getValue() == -1L) {
                    curDBName = String.format("%s_%d", defaultName, pair.getKey());
                } else {
                    curDBName =
                            String.format("%s_%d_%d", defaultName, pair.getKey(), pair.getValue());
                }
                String curDBPath = Paths.get(parentFile.getPath(), curDBName).toString();
                if (i == dbCount - 1) {
                    latestDBPath = curDBPath;
                } else {
                    // delete old db, do not delete files in the deletion queue
                    if (!factory.findPathInRemovedList(curDBPath)) {
                        try {
                            FileUtils.deleteDirectory(new File(curDBPath));
                            log.info("delete old dbpath {}", curDBPath);
                        } catch (IOException e) {
                            log.error("fail to delete old dbpath {}", curDBPath, e);
                        }
                    }
                }
            }
        } else {
            latestDBPath = Paths.get(parentFile.getPath(), defaultName).toString();
        }
        if (factory.findPathInRemovedList(latestDBPath)) {
            // Has been deleted, create a new directory
            latestDBPath =
                    Paths.get(parentFile.getPath(), String.format("%s_%d", defaultName, version))
                         .toString();
        }
        //
        log.info("{} latest db path {}", this.graphName, latestDBPath);
        return latestDBPath;
    }

    public boolean setDisableWAL(final boolean flag) {
        this.writeOptions.setSync(!flag);
        return this.writeOptions.setDisableWAL(flag).disableWAL();
    }

    public void checkTable(String table) {
        try {
            ColumnFamilyHandle handle = tables.get(table);
            if (handle == null) {
                createTable(table);
            }
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        }
    }

    public boolean tableIsExist(String table) {
        return this.tables.containsKey(table);
    }

    private void openRocksDB(String dbDataPath, long version) {

        if (dbDataPath.endsWith(File.separator)) {
            this.dbPath = dbDataPath + this.graphName;
        } else {
            this.dbPath = dbDataPath + File.separator + this.graphName;
        }

        this.dbPath = findLatestDBPath(dbPath, version);

        Asserts.isTrue((dbPath != null),
                       () -> new DBStoreException("the data-path of RocksDB is null"));

        //makedir for rocksdb
        createDirectory(dbPath);

        Options opts = new Options();
        RocksDBSession.initOptions(hugeConfig, opts, opts, opts, opts);
        dbOptions = new DBOptions(opts);
        dbOptions.setStatistics(rocksDbStats);

        try {
            List<ColumnFamilyDescriptor> columnFamilyDescriptorList =
                    new ArrayList<>();
            List<byte[]> columnFamilyBytes = RocksDB.listColumnFamilies(new Options(), dbPath);

            ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
            RocksDBSession.initOptions(this.hugeConfig, null, null, cfOptions, cfOptions);

            if (columnFamilyBytes.size() > 0) {
                for (byte[] columnFamilyByte : columnFamilyBytes) {
                    columnFamilyDescriptorList.add(
                            new ColumnFamilyDescriptor(columnFamilyByte, cfOptions));
                }
            } else {
                columnFamilyDescriptorList.add(
                        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
            }
            List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
            this.rocksDB = RocksDB.open(dbOptions, dbPath, columnFamilyDescriptorList,
                                        columnFamilyHandleList);
            Asserts.isTrue(columnFamilyHandleList.size() > 0, "must have column family");

            for (ColumnFamilyHandle handle : columnFamilyHandleList) {
                this.tables.put(new String(handle.getDescriptor().getName()), handle);
            }
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        }
    }

    private ColumnFamilyHandle createTable(String table) throws RocksDBException {
        cfHandleLock.writeLock().lock();
        try {
            ColumnFamilyHandle handle = tables.get(table);
            if (handle == null) {
                ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
                RocksDBSession.initOptions(this.hugeConfig, null, null, cfOptions, cfOptions);
                ColumnFamilyDescriptor cfDescriptor =
                        new ColumnFamilyDescriptor(table.getBytes(), cfOptions);
                handle = this.rocksDB.createColumnFamily(cfDescriptor);
                tables.put(table, handle);
            }
            return handle;
        } finally {
            cfHandleLock.writeLock().unlock();
        }
    }

    public ColumnFamilyHandle getCF(String table) {
        ColumnFamilyHandle handle = this.tables.get(table);
        try {
            if (handle == null) {
                getCfHandleReadLock().unlock();
                handle = createTable(table);
                getCfHandleReadLock().lock();
            }
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        }
        return handle;
    }

    public CFHandleLock getCFHandleLock(String table) {
        this.cfHandleLock.readLock().lock();
        ColumnFamilyHandle handle = this.tables.get(table);
        try {
            if (handle == null) {
                this.cfHandleLock.readLock().unlock();
                handle = createTable(table);
                this.cfHandleLock.readLock().lock();
                handle = this.tables.get(table);
            }
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        }
        return new CFHandleLock(handle, this.cfHandleLock);
    }

    @Override
    public String toString() {
        return "RocksDBSession";
    }

    /**
     * Delete all data of table
     *
     * @param tables
     * @throws DBStoreException
     */
    public void deleteTables(String... tables) throws DBStoreException {
        dropTables(tables);
        createTables(tables);
    }

    public void createTables(String... tables) throws DBStoreException {
        if (!this.rocksDB.isOwningHandle()) {
            return;
        }
        cfHandleLock.writeLock().lock();
        try {
            List<ColumnFamilyDescriptor> cfList = new ArrayList<>();
            for (String table : tables) {
                if (this.tables.get(table) != null) {
                    continue;
                }

                ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(table.getBytes());
                cfList.add(cfDescriptor);
            }

            if (cfList.size() > 0) {
                List<ColumnFamilyHandle> cfHandles = this.rocksDB.createColumnFamilies(cfList);
                for (ColumnFamilyHandle handle : cfHandles) {
                    this.tables.put(new String(handle.getDescriptor().getName()), handle);
                }
            }

        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        } finally {
            cfHandleLock.writeLock().unlock();
        }

    }

    public void dropTables(String... tables) throws DBStoreException {
        if (!this.rocksDB.isOwningHandle()) {
            return;
        }

        cfHandleLock.writeLock().lock();
        try {
            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            for (String table : tables) {
                ColumnFamilyHandle handle = this.tables.get(table);
                if (handle != null) {
                    cfHandles.add(handle);
                } else {
                    this.tables.remove(table);
                }
            }

            if (cfHandles.size() > 0) {
                this.rocksDB.dropColumnFamilies(cfHandles);
            }

            for (ColumnFamilyHandle h : cfHandles) {
                String tName = new String(h.getDescriptor().getName());
                h.close();
                log.info("drop table: {}", tName);
                this.tables.remove(tName);
            }

        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        } finally {
            cfHandleLock.writeLock().unlock();
        }
    }

    public synchronized void truncate() {
        Set<String> tableNames = this.tables.keySet();
        String defaultCF = new String(RocksDB.DEFAULT_COLUMN_FAMILY);
        tableNames.remove(defaultCF);

        log.info("truncate table: {}", String.join(",", tableNames));
        this.dropTables(tableNames.toArray(new String[0]));
        this.createTables(tableNames.toArray(new String[0]));
    }

    public void flush(boolean wait) {
        cfHandleLock.readLock().lock();
        try {
            rocksDB.flush(new FlushOptions().setWaitForFlush(wait),
                          tables.entrySet()
                                .stream().map(e -> e.getValue())
                                .collect(Collectors.toList()));

        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        } finally {
            cfHandleLock.readLock().unlock();
        }
    }

    void shutdown() {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }
        log.info("shutdown db {}, path is {} ", getGraphName(), getDbPath());

        cfHandleLock.writeLock().lock();
        try {
            this.tables.forEach((k, v) -> {
                v.close();
            });
            this.tables.clear();

            if (rocksDB != null) {
                try {
                    this.rocksDB.syncWal();
                } catch (RocksDBException e) {
                    log.warn("exception ", e);
                }
                this.rocksDB.close();
            }
            rocksDB = null;
            if (dbOptions != null) {
                this.dbOptions.close();
                this.writeOptions.close();
                this.rocksDbStats.close();
                dbOptions = null;
            }
        } finally {
            cfHandleLock.writeLock().unlock();
        }
    }

    public SessionOperator sessionOp() {
        return new SessionOperatorImpl(this);
    }

    public long getLatestSequenceNumber() {
        return rocksDB.getLatestSequenceNumber();
    }

    public Statistics getRocksDbStats() {
        return rocksDbStats;
    }

    public void saveSnapshot(String snapshotPath) throws DBStoreException {
        long startTime = System.currentTimeMillis();
        log.info("begin save snapshot at {}", snapshotPath);
        cfHandleLock.readLock().lock();
        try (final Checkpoint checkpoint = Checkpoint.create(this.rocksDB)) {
            final String tempPath = Paths.get(snapshotPath) + "_temp";
            final File tempFile = new File(tempPath);
            FileUtils.deleteDirectory(tempFile);
            checkpoint.createCheckpoint(tempPath);
            final File snapshotFile = new File(snapshotPath);
            try {
                FileUtils.deleteDirectory(snapshotFile);
                FileUtils.moveDirectory(tempFile, snapshotFile);
            } catch (IOException e) {
                log.error("Fail to rename {} to {}", tempPath, snapshotPath, e);
                throw new DBStoreException(
                        String.format("Fail to rename %s to %s", tempPath, snapshotPath));
            }
        } catch (final DBStoreException e) {
            throw e;
        } catch (final Exception e) {
            log.error("Fail to write snapshot at path: {}", snapshotPath, e);
            throw new DBStoreException(
                    String.format("Fail to write snapshot at path %s", snapshotPath));
        } finally {
            cfHandleLock.readLock().unlock();
        }
        log.info("saved snapshot into {}, time cost {} ms", snapshotPath,
                 System.currentTimeMillis() - startTime);
    }

    private boolean verifySnapshot(String snapshotPath) {
        try {
            try (final Options options = new Options();
                 final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {
                List<ColumnFamilyDescriptor> columnFamilyDescriptorList = new ArrayList<>();
                List<byte[]> columnFamilyBytes = RocksDB.listColumnFamilies(options, snapshotPath);

                if (columnFamilyBytes.size() > 0) {
                    for (byte[] columnFamilyByte : columnFamilyBytes) {
                        columnFamilyDescriptorList.add(
                                new ColumnFamilyDescriptor(columnFamilyByte, cfOptions));
                    }
                } else {
                    columnFamilyDescriptorList.add(
                            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
                }
                List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

                try (final DBOptions dbOptions = new DBOptions();
                     final RocksDB db = RocksDB.openReadOnly(dbOptions, snapshotPath,
                                                             columnFamilyDescriptorList,
                                                             columnFamilyHandleList)) {
                    for (ColumnFamilyHandle handle : columnFamilyHandleList) {
                        if (handle == null) {
                            log.error("verifySnapshot some ColumnFamilyHandle is null in {}",
                                      snapshotPath);
                            return false;
                        }
                    }
                }
            }
            log.info("verifySnapshot {} Ok", snapshotPath);
        } catch (RocksDBException e) {
            log.error("verifySnapshot {} failed. {}", snapshotPath, e.toString());
            return false;
        }
        return true;
    }

    public void loadSnapshot(String snapshotPath, long v1) throws DBStoreException {
        long startTime = System.currentTimeMillis();
        log.info("begin load snapshot from {}", snapshotPath);
        cfHandleLock.writeLock().lock();
        try {
            final File snapshotFile = new File(snapshotPath);
            if (!snapshotFile.exists()) {
                log.error("Snapshot file {} not exists.", snapshotPath);
                throw new DBStoreException(
                        String.format("Snapshot file %s not exists", snapshotPath));
            }

            // replace rocksdb data with snapshot data
            File dbFile = new File(this.dbPath);
            String parent = dbFile.getParent();
            String defaultName = dbFile.getName().split("_")[0];
            String defaultPath = Paths.get(parent, defaultName).toString();
            String newDBPath = String.format("%s_%d", defaultPath, v1);
            String tempDBPath = newDBPath + tempSuffix;

            // first link snapshot to temp dir
            try {
                final File tempFile = new File(tempDBPath);
                FileUtils.deleteDirectory(tempFile);
                FileUtils.forceMkdir(tempFile);
                File[] fs = snapshotFile.listFiles();
                for (File f : fs) {
                    if (!f.isDirectory()) {
                        File target = Paths.get(tempFile.getPath(), f.getName()).toFile();
                        // create hard link
                        try {
                            Files.createLink(target.toPath(), f.toPath());
                        } catch (IOException e) {
                            log.error("link failed, {} -> {}, error:{}",
                                      f.getAbsolutePath(),
                                      target.getAbsolutePath(),
                                      e.getMessage());
                            // diff disk
                            Files.copy(f.toPath(), target.toPath());
                        }
                    }
                }
            } catch (IOException e) {
                log.error("Fail to copy {} to {}. {}", snapshotPath, tempDBPath, e.toString());
                try {
                    FileUtils.deleteDirectory(new File(tempDBPath));
                } catch (IOException e2) {
                    log.error("Fail to delete directory {}. {}", tempDBPath, e2.toString());
                }
                throw new DBStoreException(
                        String.format("Fail to copy %s to %s", snapshotPath, tempDBPath));
            }
            // verify temp path
            if (!verifySnapshot(tempDBPath)) {
                throw new DBStoreException(
                        String.format("failed to verify snapshot %s", tempDBPath));
            }

            // move temp to newDBPath
            try {
                FileUtils.deleteDirectory(new File(newDBPath));
                FileUtils.moveDirectory(new File(tempDBPath), new File(newDBPath));
            } catch (IOException e) {
                log.error("Fail to copy {} to {}. {}", snapshotPath, tempDBPath, e.toString());
                throw new DBStoreException(
                        String.format("Fail to move %s to %s", tempDBPath, newDBPath));
            }
        } catch (final DBStoreException e) {
            throw e;
        } catch (final Exception e) {
            log.error("failed to load snapshot from {}", snapshotPath);
            throw new DBStoreException(
                    String.format("Fail to write snapshot at path %s", snapshotPath));
        } finally {
            cfHandleLock.writeLock().unlock();
        }
        log.info("loaded snapshot from {}, time cost {} ms", snapshotPath,
                 System.currentTimeMillis() - startTime);
    }

    /**
     * @param :
     * @return the approximate size of the data.
     */
    public long getApproximateDataSize() {
        return getApproximateDataSize("\0".getBytes(StandardCharsets.UTF_8),
                                      "\255".getBytes(StandardCharsets.UTF_8));
    }

    public long getEstimateNumKeys() {
        cfHandleLock.readLock().lock();
        try {
            long totalCount = 0;
            for (ColumnFamilyHandle h : this.tables.values()) {
                long count = this.rocksDB.getLongProperty(h, "rocksdb.estimate-num-keys");
                totalCount += count;
            }
            return totalCount;
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        } finally {
            cfHandleLock.readLock().unlock();
        }
    }

    /**
     * @param :
     * @return the approximate size of the data.  N KB.
     */
    public long getApproximateDataSize(byte[] start, byte[] end) {
        cfHandleLock.readLock().lock();
        try {
            long kbSize = 0;
            long bytesSize = 0;
            Range r1 = new Range(new Slice(start), new Slice(end));
            for (ColumnFamilyHandle h : this.tables.values()) {
                long[] sizes = this.rocksDB.getApproximateSizes(
                        h,
                        List.of(r1),
                        SizeApproximationFlag.INCLUDE_FILES,
                        SizeApproximationFlag.INCLUDE_MEMTABLES);

                bytesSize += sizes[0];
                kbSize += bytesSize / 1024;
                bytesSize = bytesSize % 1024;
            }
            if (bytesSize != 0) {
                kbSize += 1;
            }
            return kbSize;
        } finally {
            cfHandleLock.readLock().unlock();
        }
    }

    /**
     * Get size by table name
     * @param table table
     * @param start key start
     * @param end key end
     * @return size
     */
    public long getApproximateDataSize(String table, byte[] start, byte[] end) {
        cfHandleLock.readLock().lock();
        try {
            if (!this.tables.containsKey(table)) {
                return 0;
            }

            long kbSize = 0;
            long bytesSize = 0;
            Range r1 = new Range(new Slice(start), new Slice(end));

            var h = this.tables.get(table);
            long[] sizes =
                this.rocksDB.getApproximateSizes(
                    h, Arrays.asList(r1), SizeApproximationFlag.INCLUDE_FILES, SizeApproximationFlag.INCLUDE_MEMTABLES);

            bytesSize += sizes[0];
            kbSize += bytesSize / 1024;
            bytesSize = bytesSize % 1024;

            if (bytesSize != 0) {
                kbSize += 1;
            }
            return kbSize;
        } finally {
            cfHandleLock.readLock().unlock();
        }
    }

    public Map<String, String> getApproximateCFDataSize(byte[] start, byte[] end) {
        Map<String, String> map = new ConcurrentHashMap<>(this.tables.size());
        cfHandleLock.readLock().lock();
        try {
            Range r1 = new Range(new Slice(start), new Slice(end));
            for (ColumnFamilyHandle h : this.tables.values()) {
                long[] sizes = this.rocksDB.getApproximateSizes(
                        h,
                        List.of(r1),
                        SizeApproximationFlag.INCLUDE_FILES,
                        SizeApproximationFlag.INCLUDE_MEMTABLES);
                String name = new String(h.getDescriptor().getName());
                String size = FileUtils.byteCountToDisplaySize(sizes[0]);
                map.put(name, size);
            }
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        } finally {
            cfHandleLock.readLock().unlock();
        }
        return map;
    }

    public Map<String, Long> getKeyCountPerCF(byte[] start, byte[] end, boolean accurate) throws
                                                                                          DBStoreException {
        ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>(this.tables.size());
        cfHandleLock.readLock().lock();
        try {
            SessionOperator op = this.sessionOp();
            for (ColumnFamilyHandle h : this.tables.values()) {
                String name = new String(h.getDescriptor().getName());
                long count = 0;
                if (accurate) {
                    count = op.keyCount(start, end, name);
                } else {
                    count = op.estimatedKeyCount(name);
                }
                map.put(name, count);
            }
        } catch (RocksDBException e) {
            throw new DBStoreException(e);
        } finally {
            cfHandleLock.readLock().unlock();
        }
        return map;
    }

    public String getDbPath() {
        return this.dbPath;
    }

    public void ingestSstFile(Map<byte[], List<String>> sstFiles) {
        long startTime = System.currentTimeMillis();
        log.info("begin ingestSstFile. graphName {}", this.graphName);
        try {
            for (Map.Entry<byte[], List<String>> entry : sstFiles.entrySet()) {
                String cfName = new String(entry.getKey());
                try (CFHandleLock cfHandle = this.getCFHandleLock(cfName)) {
                    try (final IngestExternalFileOptions ingestOptions =
                                 new IngestExternalFileOptions()
                                         .setMoveFiles(true)) {
                        this.rocksDB.ingestExternalFile(cfHandle.get(), entry.getValue(),
                                                        ingestOptions);
                        log.info("Rocksdb {} ingestSstFile cf:{}, sst: {}", this.graphName, cfName,
                                 entry.getValue());
                    }
                }
            }
        } catch (RocksDBException e) {
            throw new DBStoreException("Rocksdb  ingestSstFile error " + this.graphName, e);
        }
        log.info("end ingestSstFile. graphName {}, time cost {} ms", this.graphName,
                 System.currentTimeMillis() - startTime);
    }

    public String getProperty(String property) {
        try {
            return rocksDB.getProperty(property);
        } catch (RocksDBException e) {
            log.error("getProperty exception {}", e.getMessage());
            return "0";
        }
    }

    /**
     * @return true when all iterators were closed.
     */
    public int getRefCount() {
        return this.refCount.get();
    }

    public void forceResetRefCount() {
        this.refCount.set(0);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            log.warn("RocksDBSession has been closed!");
            return;
        }
        try {
            if (this.refCount.decrementAndGet() > 0) {
                return;
            }
        } finally {
            closed = true;
        }

        assert this.refCount.get() == 0;
        this.shutdown();
    }

    RefCounter getRefCounter() {
        return new RefCounter(this.refCount);
    }

    public static class CFHandleLock implements Closeable {

        private final ColumnFamilyHandle handle;
        private final ReentrantReadWriteLock lock;

        public CFHandleLock(ColumnFamilyHandle handle, ReentrantReadWriteLock lock) {
            this.handle = handle;
            this.lock = lock;
        }

        @Override
        public void close() {
            lock.readLock().unlock();
        }

        public ColumnFamilyHandle get() {
            return handle;
        }
    }

    /**
     * A wrapper for RocksIterator that convert RocksDB results to std Iterator
     */

    public static class BackendColumn implements Comparable<BackendColumn>, Serializable {

        public byte[] name;
        public byte[] value;

        public static BackendColumn of(byte[] name, byte[] value) {
            BackendColumn col = new BackendColumn();
            col.name = name;
            col.value = value;
            return col;
        }

        @Override
        public String toString() {
            return String.format("%s=%s",
                                 new String(name, StandardCharsets.UTF_8),
                                 new String(value, StandardCharsets.UTF_8));
        }

        @Override
        public int compareTo(BackendColumn other) {
            if (other == null) {
                return 1;
            }
            return Bytes.compare(this.name, other.name);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof BackendColumn)) {
                return false;
            }
            BackendColumn other = (BackendColumn) obj;
            return Bytes.equals(this.name, other.name) &&
                   Bytes.equals(this.value, other.value);
        }
    }

    class RefCounter {

        final AtomicInteger refCount;

        RefCounter(AtomicInteger refCount) {
            (this.refCount = refCount).incrementAndGet();
        }

        public void release() {
            if (0 == this.refCount.decrementAndGet()) {
                shutdown();
            }
        }
    }

    public static String stackToString() {
        return Arrays.stream(Thread.currentThread().getStackTrace())
                     .map(StackTraceElement::toString)
                     .collect(Collectors.joining("\n\t"));
    }

    public void addIterator(String key, ScanIterator iterator) {
        log.debug("add iterator, key {}", key);
        this.iteratorMap.put(key, stackToString());
    }

    public void removeIterator(String key) {
        log.debug("remove iterator key, {}", key);
        this.iteratorMap.remove(key);
    }
}
