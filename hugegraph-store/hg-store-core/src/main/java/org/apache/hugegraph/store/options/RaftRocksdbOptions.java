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

package org.apache.hugegraph.store.options;

import java.util.Map;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.rocksdb.access.RocksDBOptions;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBufferManager;
import org.rocksdb.util.SizeUnit;

import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;
import com.alipay.sofa.jraft.util.StorageOptionsFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RaftRocksdbOptions {

    // Block cache shared by all raft log DBs; RocksdbMemoryBudget reserves it outside the budget
    static final long RAFT_LOG_CACHE_SIZE = 1024L * 1024L * 1024L;
    // Memtables of all raft log DBs, charged to that cache
    static final long RAFT_LOG_WRITE_BUFFER_SIZE = 256L * 1024L * 1024L;

    private static RocksdbConfig rocksdbConfig = null;
    private static boolean raftRocksdbConfigRegistered = false;

    private static RocksdbConfig getRocksdbConfig(HugeConfig options) {
        if (rocksdbConfig == null) {
            synchronized (RocksdbConfig.class) {
                rocksdbConfig = new RocksdbConfig(options);
            }
        }
        return rocksdbConfig;
    }

    private static void registerRaftRocksdbConfig(HugeConfig options) {
        // StorageOptionsFactory.releaseAllOptions() (called by test setup between runs)
        // does not clear its table-format-config table, so registering RocksDBLogStorage's
        // config more than once per JVM throws IllegalStateException. Register only once.
        // The guard flag is held across the whole registration so a failure partway through
        // doesn't leave the flag set to true while some options were never registered.
        synchronized (RaftRocksdbOptions.class) {
            if (raftRocksdbConfigRegistered) {
                return;
            }

            Cache blockCache = new LRUCache(RAFT_LOG_CACHE_SIZE);
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                    .setIndexType(IndexType.kTwoLevelIndexSearch)
                    .setPartitionFilters(true) //
                    .setMetadataBlockSize(8 * SizeUnit.KB) //
                    .setCacheIndexAndFilterBlocks(
                            options.get(RocksDBOptions.PUT_FILTER_AND_INDEX_IN_CACHE))
                    .setCacheIndexAndFilterBlocksWithHighPriority(true)
                    .setPinL0FilterAndIndexBlocksInCache(
                            options.get(RocksDBOptions.PIN_L0_FILTER_AND_INDEX_IN_CACHE))
                    .setBlockSize(4 * SizeUnit.KB)
                    .setBlockCache(blockCache);

            StorageOptionsFactory.registerRocksDBTableFormatConfig(RocksDBLogStorage.class,
                                                                   tableConfig);

            DBOptions dbOptions = raftLogDBOptions(rocksdbConfig.getEnv(), blockCache);
            StorageOptionsFactory.registerRocksDBOptions(RocksDBLogStorage.class,
                                                         dbOptions);

            ColumnFamilyOptions cfOptions =
                    StorageOptionsFactory.getDefaultRocksDBColumnFamilyOptions();
            cfOptions.setTargetFileSizeBase(256 * SizeUnit.MB);
            cfOptions.setWriteBufferSize(8 * SizeUnit.MB);
            cfOptions.setNumLevels(3);
            cfOptions.setMaxWriteBufferNumber(3);
            cfOptions.setCompressionType(CompressionType.NO_COMPRESSION);
            cfOptions.setMaxBytesForLevelBase(2048 * SizeUnit.GB);

            StorageOptionsFactory.registerRocksDBColumnFamilyOptions(RocksDBLogStorage.class,
                                                                     cfOptions);

            raftRocksdbConfigRegistered = true;
        }
    }

    /**
     * Options for the raft log DBs. JRaft gives every log storage a copy of these, and a
     * copy keeps the same WriteBufferManager, so all groups share one memtable cap.
     */
    static DBOptions raftLogDBOptions(Env env, Cache blockCache) {
        DBOptions dbOptions = StorageOptionsFactory.getDefaultRocksDBOptions();
        dbOptions.setEnv(env);
        // Each partition's raft group opens its own log DB, so without a shared manager the
        // log memtables grow with the partition count. Charging them to the log block cache
        // makes them take room from it, and RocksdbMemoryBudget already reserves that cache,
        // instead of adding to it
        dbOptions.setWriteBufferManager(new WriteBufferManager(RAFT_LOG_WRITE_BUFFER_SIZE,
                                                               blockCache));
        dbOptions.setUnorderedWrite(true);
        return dbOptions;
    }

    public static void initRocksdbGlobalConfig(Map<String, Object> config) {
        HugeConfig hugeConfig = BusinessHandlerImpl.initRocksdb(config, null);
        RocksdbConfig rocksdbConfig = getRocksdbConfig(hugeConfig);
        registerRaftRocksdbConfig(hugeConfig);
        config.put(RocksDBOptions.ENV, rocksdbConfig.getEnv());
        config.put(RocksDBOptions.WRITE_BUFFER_MANAGER, rocksdbConfig.getBufferManager());
        config.put(RocksDBOptions.BLOCK_TABLE_CONFIG, rocksdbConfig.getTableConfig());
        config.put(RocksDBOptions.BLOCK_CACHE, rocksdbConfig.getBlockCache());
        config.put(RocksDBOptions.WRITE_CACHE, rocksdbConfig.getWriteCache());
    }

    public static WriteBufferManager getWriteBufferManager() {
        return rocksdbConfig.getBufferManager();
    }

    public static Env getEnv() {
        return rocksdbConfig.getEnv();
    }

    public static Cache getWriteCache() {
        return rocksdbConfig.getWriteCache();
    }

    public static Cache getBlockCache() {
        return rocksdbConfig.getBlockCache();
    }

    public static long getWriteCacheCapacity() {
        return rocksdbConfig.getWriteCacheCapacity();
    }

    public static long getBlockCacheCapacity() {
        return rocksdbConfig.getBlockCacheCapacity();
    }

    static class RocksdbConfig {

        private final Env env;
        private final LRUCache blockCache;
        private final LRUCache writeCache;
        private final WriteBufferManager bufferManager;
        private final BlockBasedTableConfig tableConfig;
        private final long blockCacheCapacity;
        private final long writeCacheCapacity;

        public RocksdbConfig(HugeConfig options) {
            RocksDB.loadLibrary();
            this.env = Env.getDefault();
            double writeBufferRatio = options.get(RocksDBOptions.WRITE_BUFFER_RATIO);
            this.writeCacheCapacity =
                    (long) (options.get(RocksDBOptions.TOTAL_MEMORY_SIZE) * writeBufferRatio);
            this.blockCacheCapacity =
                    options.get(RocksDBOptions.TOTAL_MEMORY_SIZE) - writeCacheCapacity;
            this.writeCache = new LRUCache(writeCacheCapacity);
            this.blockCache = new LRUCache(blockCacheCapacity);
            this.bufferManager = new WriteBufferManager(writeCacheCapacity, writeCache,
                                                        options.get(
                                                                RocksDBOptions.WRITE_BUFFER_ALLOW_STALL));
            this.tableConfig = new BlockBasedTableConfig() //
                                                           .setIndexType(
                                                                   IndexType.kTwoLevelIndexSearch) //
                                                           .setPartitionFilters(true) //
                                                           .setMetadataBlockSize(8 * SizeUnit.KB) //
                                                           .setCacheIndexAndFilterBlocks(
                                                                   options.get(
                                                                           RocksDBOptions.PUT_FILTER_AND_INDEX_IN_CACHE)) //
                                                           .setCacheIndexAndFilterBlocksWithHighPriority(
                                                                   true) //
                                                           .setPinL0FilterAndIndexBlocksInCache(
                                                                   options.get(
                                                                           RocksDBOptions.PIN_L0_FILTER_AND_INDEX_IN_CACHE)) //
                                                           .setBlockSize(4 * SizeUnit.KB)//
                                                           .setBlockCache(blockCache);

            int bitsPerKey = options.get(RocksDBOptions.BLOOM_FILTER_BITS_PER_KEY);
            if (bitsPerKey >= 0) {
                tableConfig.setFilterPolicy(new BloomFilter(bitsPerKey,
                                                            options.get(
                                                                    RocksDBOptions.BLOOM_FILTER_MODE)));
            }
            tableConfig.setWholeKeyFiltering(
                    options.get(RocksDBOptions.BLOOM_FILTER_WHOLE_KEY));
            log.info("RocksdbConfig {}", options.get(RocksDBOptions.BLOOM_FILTER_BITS_PER_KEY));
        }

        public Env getEnv() {
            return env;
        }

        public LRUCache getBlockCache() {
            return blockCache;
        }

        public LRUCache getWriteCache() {
            return writeCache;
        }

        public WriteBufferManager getBufferManager() {
            return bufferManager;
        }

        public BlockBasedTableConfig getTableConfig() {
            return tableConfig;
        }

        public long getBlockCacheCapacity() {
            return blockCacheCapacity;
        }

        public long getWriteCacheCapacity() {
            return writeCacheCapacity;
        }
    }
}
