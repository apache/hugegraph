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

import static org.apache.hugegraph.config.OptionChecker.allowValues;
import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;
import static org.apache.hugegraph.config.OptionChecker.inValues;
import static org.apache.hugegraph.config.OptionChecker.rangeDouble;
import static org.apache.hugegraph.config.OptionChecker.rangeInt;

import org.apache.hugegraph.config.ConfigConvOption;
import org.apache.hugegraph.config.ConfigListConvOption;
import org.apache.hugegraph.config.ConfigListOption;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.OptionHolder;
import org.apache.hugegraph.util.Bytes;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DataBlockIndexType;
import org.rocksdb.IndexType;

import com.google.common.collect.ImmutableList;

public class RocksDBOptions extends OptionHolder {

    private RocksDBOptions() {
        super();
    }

    private static volatile RocksDBOptions instance;

    public static synchronized RocksDBOptions instance() {
        if (instance == null) {
            instance = new RocksDBOptions();
            instance.registerOptions();
        }
        return instance;
    }

    // TODO: the entire align style is wrong, change it to 4 space later
    public static final ConfigOption<String> DATA_PATH =
            new ConfigOption<>(
                    "rocksdb.data_path",
                    "The path for storing data of RocksDB.",
                    disallowEmpty(),
                    "rocksdb-data/data"
            );

    public static final ConfigOption<String> PROVIDER =
            new ConfigOption<>(
                    "rocksdb.provider",
                    "The RocksDB-compatible runtime provider.",
                    allowValues("rocksdb", "topling"),
                    "rocksdb"
            );

    public static final ConfigListOption<String> DATA_DISKS =
            new ConfigListOption<>(
                    "rocksdb.data_disks",
                    false,
                    "The optimized disks for storing data of RocksDB. " +
                    "The format of each element: `STORE/TABLE: /path/disk`." +
                    "Allowed keys are [g/vertex, g/edge_out, g/edge_in, " +
                    "g/vertex_label_index, g/edge_label_index, " +
                    "g/range_int_index, g/range_float_index, " +
                    "g/range_long_index, g/range_double_index, " +
                    "g/secondary_index, g/search_index, g/shard_index, " +
                    "g/unique_index, g/olap]",
                    null,
                    String.class,
                    ImmutableList.of()
            );

    public static final ConfigOption<String> WAL_PATH =
            new ConfigOption<>(
                    "rocksdb.wal_path",
                    "The path for storing WAL of RocksDB.",
                    disallowEmpty(),
                    "rocksdb-data/wal"
            );

    public static final ConfigOption<String> SST_PATH =
            new ConfigOption<>(
                    "rocksdb.sst_path",
                    "The path for ingesting SST file into RocksDB.",
                    null,
                    ""
            );

    // TODO: support ConfigOption<InfoLogLevel>
    public static final ConfigOption<String> LOG_LEVEL =
            new ConfigOption<>(
                    "rocksdb.log_level",
                    "The info log level of RocksDB.",
                    allowValues("DEBUG", "INFO", "WARN", "ERROR", "FATAL", "HEADER"),
                    "INFO"
            );

    public static final ConfigOption<Integer> NUM_LEVELS =
            new ConfigOption<>(
                    "rocksdb.num_levels",
                    "Set the number of levels for this database.",
                    rangeInt(1, Integer.MAX_VALUE),
                    7
            );

    public static final ConfigConvOption<String, CompactionStyle> COMPACTION_STYLE =
            new ConfigConvOption<>(
                    "rocksdb.compaction_style",
                    "Set compaction style for RocksDB: LEVEL/UNIVERSAL/FIFO.",
                    allowValues("LEVEL", "UNIVERSAL", "FIFO"),
                    CompactionStyle::valueOf,
                    "LEVEL"
            );

    public static final ConfigOption<Boolean> OPTIMIZE_MODE =
            new ConfigOption<>(
                    "rocksdb.optimize_mode",
                    "Optimize for heavy workloads and big datasets.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<Boolean> BULKLOAD_MODE =
            new ConfigOption<>(
                    "rocksdb.bulkload_mode",
                    "Switch to the mode to bulk load data into RocksDB.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigListConvOption<String, CompressionType> LEVELS_COMPRESSIONS =
            new ConfigListConvOption<>(
                    "rocksdb.compression_per_level",
                    "The compression algorithms for different levels of RocksDB, " +
                    "allowed values are none/snappy/z/bzip2/lz4/lz4hc/xpress/zstd.",
                    inValues("none", "snappy", "z", "bzip2", "lz4", "lz4hc", "xpress", "zstd"),
                    CompressionType::getCompressionType,
                    "none", "none", "snappy", "snappy", "snappy", "snappy", "snappy"
            );

    public static final ConfigConvOption<String, CompressionType> BOTTOMMOST_COMPRESSION =
            new ConfigConvOption<>(
                    "rocksdb.bottommost_compression",
                    "The compression algorithm for the bottommost level of RocksDB, " +
                    "allowed values are none/snappy/z/bzip2/lz4/lz4hc/xpress/zstd.",
                    allowValues("none", "snappy", "z", "bzip2", "lz4", "lz4hc", "xpress", "zstd"),
                    CompressionType::getCompressionType,
                    "none"
            );

    public static final ConfigConvOption<String, CompressionType> COMPRESSION =
            new ConfigConvOption<>(
                    "rocksdb.compression",
                    "The compression algorithm for compressing blocks of RocksDB, " +
                    "allowed values are none/snappy/z/bzip2/lz4/lz4hc/xpress/zstd.",
                    allowValues("none", "snappy", "z", "bzip2", "lz4", "lz4hc", "xpress", "zstd"),
                    CompressionType::getCompressionType,
                    "snappy"
            );

    public static final ConfigOption<Integer> MAX_BG_JOBS =
            new ConfigOption<>(
                    "rocksdb.max_background_jobs",
                    "Maximum number of concurrent background jobs, " +
                    "including flushes and compactions.",
                    rangeInt(1, Integer.MAX_VALUE),
                    8
            );

    public static final ConfigOption<Integer> MAX_SUB_COMPACTIONS =
            new ConfigOption<>(
                    "rocksdb.max_subcompactions",
                    "The value represents the maximum number of threads per compaction job.",
                    rangeInt(1, Integer.MAX_VALUE),
                    4
            );

    public static final ConfigOption<Long> DELAYED_WRITE_RATE =
            new ConfigOption<>(
                    "rocksdb.delayed_write_rate",
                    "The rate limit in bytes/s of user write requests " +
                    "when need to slow down if the compaction gets behind.",
                    rangeInt(1L, Long.MAX_VALUE),
                    16L * Bytes.MB
            );

    public static final ConfigOption<Integer> MAX_OPEN_FILES =
            new ConfigOption<>(
                    "rocksdb.max_open_files",
                    "The maximum number of open files that can be cached by RocksDB, " +
                    "-1 means no limit.",
                    rangeInt(-1, Integer.MAX_VALUE),
                    -1
            );

    public static final ConfigOption<Long> MAX_MANIFEST_FILE_SIZE =
            new ConfigOption<>(
                    "rocksdb.max_manifest_file_size",
                    "The max size of manifest file in bytes.",
                    rangeInt(1L, Long.MAX_VALUE),
                    100L * Bytes.MB
            );

    public static final ConfigOption<Boolean> SKIP_STATS_UPDATE_ON_DB_OPEN =
            new ConfigOption<>(
                    "rocksdb.skip_stats_update_on_db_open",
                    "Whether to skip statistics update when opening the database, " +
                    "setting this flag true allows us to not update statistics.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> SKIP_CHECK_SIZE_ON_DB_OPEN =
            new ConfigOption<>(
                    "rocksdb.skip_check_sst_size_on_db_open",
                    "Whether to skip checking sizes of all sst files when " +
                    "opening the database.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Integer> MAX_FILE_OPENING_THREADS =
            new ConfigOption<>(
                    "rocksdb.max_file_opening_threads",
                    "The max number of threads used to open files.",
                    rangeInt(1, Integer.MAX_VALUE),
                    16
            );

    public static final ConfigOption<Long> MAX_TOTAL_WAL_SIZE =
            new ConfigOption<>(
                    "rocksdb.max_total_wal_size",
                    "Total size of WAL files in bytes. Once WALs exceed this size, " +
                    "we will start forcing the flush of column families related, " +
                    "0 means no limit.",
                    rangeInt(0L, Long.MAX_VALUE),
                    0L
            );

    public static final ConfigOption<Long> BYTES_PER_SYNC =
            new ConfigOption<>(
                    "rocksdb.bytes_per_sync",
                    "Allows OS to incrementally sync SST files to disk while " +
                    "they are being written, asynchronously in the background. " +
                    "Issue one request for every bytes_per_sync written. " +
                    "0 turns it off.",
                    rangeInt(0L, Long.MAX_VALUE),
                    0L
            );

    public static final ConfigOption<Long> WAL_BYTES_PER_SYNC =
            new ConfigOption<>(
                    "rocksdb.wal_bytes_per_sync",
                    "Allows OS to incrementally sync WAL files to disk while " +
                    "they are being written, asynchronously in the background. " +
                    "Issue one request for every bytes_per_sync written. " +
                    "0 turns it off.",
                    rangeInt(0L, Long.MAX_VALUE),
                    0L
            );

    public static final ConfigOption<Boolean> STRICT_BYTES_PER_SYNC =
            new ConfigOption<>(
                    "rocksdb.strict_bytes_per_sync",
                    "When true, guarantees SST/WAL files have at most " +
                    "bytes_per_sync/wal_bytes_per_sync bytes submitted for " +
                    "writeback at any given time. This can be used to handle " +
                    "cases where processing speed exceeds I/O speed.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Long> DB_MEMTABLE_SIZE =
            new ConfigOption<>(
                    "rocksdb.db_write_buffer_size",
                    "Total size of write buffers in bytes across all column families, " +
                    "0 means no limit.",
                    rangeInt(0L, Long.MAX_VALUE),
                    0L
            );

    public static final ConfigOption<Long> LOG_READAHEAD_SIZE =
            new ConfigOption<>(
                    "rocksdb.log_readahead_size",
                    "The number of bytes to prefetch when reading the log. " +
                    "0 means the prefetching is disabled.",
                    rangeInt(0L, Long.MAX_VALUE),
                    0L
            );

    public static final ConfigOption<Long> COMPACTION_READAHEAD_SIZE =
            new ConfigOption<>(
                    "rocksdb.compaction_readahead_size",
                    "The number of bytes to perform bigger reads when doing " +
                    "compaction. If running RocksDB on spinning disks, " +
                    "you should set this to at least 2MB. " +
                    "0 means the prefetching is disabled.",
                    rangeInt(0L, Long.MAX_VALUE),
                    0L
            );

    public static final ConfigOption<Long> ROW_CACHE_CAPACITY =
            new ConfigOption<>(
                    "rocksdb.row_cache_capacity",
                    "The capacity in bytes of global cache for table-level rows. " +
                    "0 means the row_cache is disabled.",
                    rangeInt(0L, Long.MAX_VALUE),
                    0L
            );

    public static final ConfigOption<Long> DELETE_OBSOLETE_FILE_PERIOD =
            new ConfigOption<>(
                    "rocksdb.delete_obsolete_files_period",
                    "The periodicity in seconds when obsolete files get deleted, " +
                    "0 means always do full purge.",
                    rangeInt(0L, Long.MAX_VALUE),
                    6L * 60 * 60
            );

    public static final ConfigOption<Long> MEMTABLE_SIZE =
            new ConfigOption<>(
                    "rocksdb.write_buffer_size",
                    "Amount of data in bytes to build up in memory.",
                    rangeInt(Bytes.MB, Long.MAX_VALUE),
                    128L * Bytes.MB
            );

    public static final ConfigOption<Integer> MAX_MEMTABLES =
            new ConfigOption<>(
                    "rocksdb.max_write_buffer_number",
                    "The maximum number of write buffers that are built up in memory.",
                    rangeInt(1, Integer.MAX_VALUE),
                    6
            );

    public static final ConfigOption<Integer> MIN_MEMTABLES_TO_MERGE =
            new ConfigOption<>(
                    "rocksdb.min_write_buffer_number_to_merge",
                    "The minimum number of write buffers that will be merged together.",
                    rangeInt(1, Integer.MAX_VALUE),
                    2
            );

    public static final ConfigOption<Integer> MAX_MEMTABLES_TO_MAINTAIN =
            new ConfigOption<>(
                    "rocksdb.max_write_buffer_number_to_maintain",
                    "The total maximum number of write buffers to maintain in memory " +
                    "for conflict checking when transactions are used.",
                    rangeInt(0, Integer.MAX_VALUE),
                    0
            );

    public static final ConfigOption<Double> MEMTABLE_BLOOM_SIZE_RATIO =
            new ConfigOption<>(
                    "rocksdb.memtable_bloom_size_ratio",
                    "If prefix-extractor is set and memtable_bloom_size_ratio " +
                    "is not 0, or if memtable_whole_key_filtering is set true, " +
                    "create bloom filter for memtable with the size of " +
                    "write_buffer_size * memtable_bloom_size_ratio. " +
                    "If it is larger than 0.25, it is santinized to 0.25.",
                    rangeDouble(0.0, 1.0),
                    0.0
            );

    public static final ConfigOption<Boolean> MEMTABLE_BLOOM_WHOLE_KEY_FILTERING =
            new ConfigOption<>(
                    "rocksdb.memtable_whole_key_filtering",
                    "Enable whole key bloom filter in memtable, it can " +
                    "potentially reduce CPU usage for point-look-ups. Note " +
                    "this will only take effect if memtable_bloom_size_ratio > 0.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Long> MEMTABL_BLOOM_HUGE_PAGE_SIZE =
            new ConfigOption<>(
                    "rocksdb.memtable_huge_page_size",
                    "The page size for huge page TLB for bloom in memtable. " +
                    "If <= 0, not allocate from huge page TLB but from malloc.",
                    rangeInt(0L, Long.MAX_VALUE),
                    0L
            );

    public static final ConfigOption<Boolean> MEMTABLE_INPLACE_UPDATE_SUPPORT =
            new ConfigOption<>(
                    "rocksdb.inplace_update_support",
                    "Allows thread-safe inplace updates if a put key exists " +
                    "in current memtable and sizeof new value is smaller.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> DYNAMIC_LEVEL_BYTES =
            new ConfigOption<>(
                    "rocksdb.level_compaction_dynamic_level_bytes",
                    "Whether to enable level_compaction_dynamic_level_bytes, " +
                    "if it's enabled we give max_bytes_for_level_multiplier a " +
                    "priority against max_bytes_for_level_base, the bytes of " +
                    "base level is dynamic for a more predictable LSM tree, " +
                    "it is useful to limit worse case space amplification. " +
                    "Turning this feature on/off for an existing DB can cause " +
                    "unexpected LSM tree structure so it's not recommended.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Long> MAX_LEVEL1_BYTES =
            new ConfigOption<>(
                    "rocksdb.max_bytes_for_level_base",
                    "The upper-bound of the total size of level-1 files in bytes.",
                    rangeInt(Bytes.MB, Long.MAX_VALUE),
                    512L * Bytes.MB
            );

    public static final ConfigOption<Double> MAX_LEVEL_BYTES_MULTIPLIER =
            new ConfigOption<>(
                    "rocksdb.max_bytes_for_level_multiplier",
                    "The ratio between the total size of level (L+1) files and " +
                    "the total size of level L files for all L.",
                    rangeDouble(1.0, Double.MAX_VALUE),
                    10.0
            );

    public static final ConfigOption<Long> TARGET_FILE_SIZE_BASE =
            new ConfigOption<>(
                    "rocksdb.target_file_size_base",
                    "The target file size for compaction in bytes.",
                    rangeInt(Bytes.MB, Long.MAX_VALUE),
                    64L * Bytes.MB
            );

    public static final ConfigOption<Integer> TARGET_FILE_SIZE_MULTIPLIER =
            new ConfigOption<>(
                    "rocksdb.target_file_size_multiplier",
                    "The size ratio between a level L file and a level (L+1) file.",
                    rangeInt(1, Integer.MAX_VALUE),
                    1
            );

    public static final ConfigOption<Integer> LEVEL0_COMPACTION_TRIGGER =
            new ConfigOption<>(
                    "rocksdb.level0_file_num_compaction_trigger",
                    "Number of files to trigger level-0 compaction.",
                    rangeInt(0, Integer.MAX_VALUE),
                    2
            );

    public static final ConfigOption<Integer> LEVEL0_SLOWDOWN_WRITES_TRIGGER =
            new ConfigOption<>(
                    "rocksdb.level0_slowdown_writes_trigger",
                    "Soft limit on number of level-0 files for slowing down writes.",
                    rangeInt(-1, Integer.MAX_VALUE),
                    20
            );

    public static final ConfigOption<Integer> LEVEL0_STOP_WRITES_TRIGGER =
            new ConfigOption<>(
                    "rocksdb.level0_stop_writes_trigger",
                    "Hard limit on number of level-0 files for stopping writes.",
                    rangeInt(-1, Integer.MAX_VALUE),
                    36
            );

    public static final ConfigOption<Long> SOFT_PENDING_COMPACTION_LIMIT =
            new ConfigOption<>(
                    "rocksdb.soft_pending_compaction_bytes_limit",
                    "The soft limit to impose on pending compaction in bytes.",
                    rangeInt(Bytes.GB, Long.MAX_VALUE),
                    64L * Bytes.GB
            );

    public static final ConfigOption<Long> HARD_PENDING_COMPACTION_LIMIT =
            new ConfigOption<>(
                    "rocksdb.hard_pending_compaction_bytes_limit",
                    "The hard limit to impose on pending compaction in bytes.",
                    rangeInt(Bytes.GB, Long.MAX_VALUE),
                    256L * Bytes.GB
            );

    public static final ConfigOption<Boolean> ALLOW_MMAP_WRITES =
            new ConfigOption<>(
                    "rocksdb.allow_mmap_writes",
                    "Allow the OS to mmap file for writing.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> ALLOW_MMAP_READS =
            new ConfigOption<>(
                    "rocksdb.allow_mmap_reads",
                    "Allow the OS to mmap file for reading sst tables.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> USE_DIRECT_READS =
            new ConfigOption<>(
                    "rocksdb.use_direct_reads",
                    "Enable the OS to use direct I/O for reading sst tables.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> USE_DIRECT_READS_WRITES_FC =
            new ConfigOption<>(
                    "rocksdb.use_direct_io_for_flush_and_compaction",
                    "Enable the OS to use direct read/writes in flush and compaction.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> USE_FSYNC =
            new ConfigOption<>(
                    "rocksdb.use_fsync",
                    "If true, then every store to stable storage will issue a fsync.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> ATOMIC_FLUSH =
            new ConfigOption<>(
                    "rocksdb.atomic_flush",
                    "If true, flushing multiple column families and committing " +
                    "their results atomically to MANIFEST. Note that it's not " +
                    "necessary to set atomic_flush=true if WAL is always enabled.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Integer> TABLE_FORMAT_VERSION =
            new ConfigOption<>(
                    "rocksdb.format_version",
                    "The format version of BlockBasedTable, allowed values are 0~5.",
                    rangeInt(0, 5),
                    5
            );

    public static final ConfigConvOption<String, IndexType> INDEX_TYPE =
            new ConfigConvOption<>(
                    "rocksdb.index_type",
                    "The index type used to lookup between data blocks " +
                    "with the sst table, allowed values are [kBinarySearch," +
                    "kHashSearch,kTwoLevelIndexSearch,kBinarySearchWithFirstKey].",
                    allowValues("kBinarySearch", "kHashSearch",
                                "kTwoLevelIndexSearch", "kBinarySearchWithFirstKey"),
                    IndexType::valueOf,
                    "kBinarySearch"
            );

    public static final ConfigConvOption<String, DataBlockIndexType> DATA_BLOCK_SEARCH_TYPE =
            new ConfigConvOption<>(
                    "rocksdb.data_block_index_type",
                    "The search type used to point lookup in data block with " +
                    "the sst table, allowed values are [kDataBlockBinarySearch," +
                    "kDataBlockBinaryAndHash].",
                    allowValues("kDataBlockBinarySearch", "kDataBlockBinaryAndHash"),
                    DataBlockIndexType::valueOf,
                    "kDataBlockBinarySearch"
            );

    public static final ConfigOption<Double> DATA_BLOCK_HASH_TABLE_RATIO =
            new ConfigOption<>(
                    "rocksdb.data_block_hash_table_util_ratio",
                    "The hash table utilization ratio value of entries/buckets. " +
                    "It is valid only when data_block_index_type=kDataBlockBinaryAndHash.",
                    rangeDouble(0.0, 1.0),
                    0.75
            );

    public static final ConfigOption<Long> BLOCK_SIZE =
            new ConfigOption<>(
                    "rocksdb.block_size",
                    "Approximate size of user data packed per block, Note that " +
                    "it corresponds to uncompressed data.",
                    rangeInt(0L, Long.MAX_VALUE),
                    4L * Bytes.KB
            );

    public static final ConfigOption<Integer> BLOCK_SIZE_DEVIATION =
            new ConfigOption<>(
                    "rocksdb.block_size_deviation",
                    "The percentage of free space used to close a block.",
                    rangeInt(0, 100),
                    10
            );

    public static final ConfigOption<Integer> BLOCK_RESTART_INTERVAL =
            new ConfigOption<>(
                    "rocksdb.block_restart_interval",
                    "The block restart interval for delta encoding in blocks.",
                    rangeInt(0, Integer.MAX_VALUE),
                    16
            );

    public static final ConfigOption<Long> BLOCK_CACHE_CAPACITY =
            new ConfigOption<>(
                    "rocksdb.block_cache_capacity",
                    "The amount of block cache in bytes that will be used by RocksDB, " +
                    "0 means no block cache.",
                    rangeInt(0L, Long.MAX_VALUE),
                    8L * Bytes.MB
            );

    public static final ConfigOption<Boolean> CACHE_FILTER_AND_INDEX =
            new ConfigOption<>(
                    "rocksdb.cache_index_and_filter_blocks",
                    "Set this option true if we'd put index/filter blocks to " +
                    "the block cache.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<Boolean> PIN_L0_INDEX_AND_FILTER =
            new ConfigOption<>(
                    "rocksdb.pin_l0_filter_and_index_blocks_in_cache",
                    "Set this option true if we'd pin L0 index/filter blocks to " +
                    "the block cache.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<Integer> BLOOM_FILTER_BITS_PER_KEY =
            new ConfigOption<>(
                    "rocksdb.bloom_filter_bits_per_key",
                    "The bits per key in bloom filter, a good value is 10, " +
                    "which yields a filter with ~ 1% false positive rate. " +
                    "Set bloom_filter_bits_per_key > 0 to enable bloom filter, " +
                    "-1 means no bloom filter (0~0.5 round down to no filter).",
                    rangeInt(-1, Integer.MAX_VALUE),
                    -1
            );

    public static final ConfigOption<Boolean> BLOOM_FILTER_MODE =
            new ConfigOption<>(
                    "rocksdb.bloom_filter_block_based_mode",
                    "If bloom filter is enabled, set this option true to " +
                    "use block based filter rather than full filter.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> BLOOM_FILTER_WHOLE_KEY =
            new ConfigOption<>(
                    "rocksdb.bloom_filter_whole_key_filtering",
                    "If bloom filter is enabled, set this option true to " +
                    "place whole keys in the bloom filter, else place the " +
                    "prefix of keys when prefix-extractor is set.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<Boolean> BLOOM_FILTERS_SKIP_LAST_LEVEL =
            new ConfigOption<>(
                    "rocksdb.optimize_filters_for_hits",
                    "If bloom filter is enabled, this flag allows us to not " +
                    "store filters for the last level. set this option true to " +
                    "optimize the filters mainly for cases where keys are found " +
                    "rather than also optimize for keys missed.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<Boolean> PARTITION_FILTERS_INDEXES =
            new ConfigOption<>(
                    "rocksdb.partition_filters_and_indexes",
                    "If bloom filter is enabled, set this option true to use " +
                    "partitioned full filters and indexes for each sst file. " +
                    "This option is incompatible with block-based filters.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> PIN_TOP_INDEX_AND_FILTER =
            new ConfigOption<>(
                    "rocksdb.pin_top_level_index_and_filter",
                    "If partition_filters_and_indexes is set true, set this " +
                    "option true if we'd pin top-level index of partitioned " +
                    "filter and index blocks to the block cache.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<Integer> PREFIX_EXTRACTOR_CAPPED =
            new ConfigOption<>(
                    "rocksdb.prefix_extractor_n_bytes",
                    "The prefix-extractor uses the first N bytes of a key as its prefix, " +
                    "it will use the full key when a key is shorter than the N. " +
                    "0 means unset prefix-extractor.",
                    rangeInt(0, Integer.MAX_VALUE),
                    0
            );
}
