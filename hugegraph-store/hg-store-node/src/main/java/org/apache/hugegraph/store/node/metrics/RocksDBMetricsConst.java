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

package org.apache.hugegraph.store.node.metrics;

import org.rocksdb.HistogramType;
import org.rocksdb.TickerType;

/**
 * Final class containing constants related to RocksDB metrics.
 * This class provides constants for metric prefixes, labels, and
 * arrays of tickers and histograms for monitoring RocksDB performance.
 * Metrics include both counters (tickers) and summaries (histograms).
 */
public final class RocksDBMetricsConst {

    // Prefix used for all RocksDB statistics.
    public static final String PREFIX = "rocks.stats";

    // Labels used for metrics, primarily for quantile information.
    public static final String LABELS = "quantile";

    // Specific quantile labels, representing the 50th, 95th, and 99th percentiles.
    public static final String LABEL_50 = "0.5";
    public static final String LABEL_95 = "0.95";
    public static final String LABEL_99 = "0.99";

    /**
     * Tickers - RocksDB equivalent of counters
     * <p>
     * Array of TickerType representing various RocksDB counters.
     * Tickers track the number of times a particular event has occurred (e.g., cache hits).
     * Useful for monitoring and debugging purposes, giving insights into database performance.
     */
    public static final TickerType[] TICKERS = {
            TickerType.BLOCK_CACHE_ADD, // Number of blocks added to the cache.
            TickerType.BLOCK_CACHE_HIT, // Number of cache hits.
            TickerType.BLOCK_CACHE_ADD_FAILURES, // Failures in adding blocks to the cache.
            TickerType.BLOCK_CACHE_INDEX_MISS, // Index cache misses.
            TickerType.BLOCK_CACHE_INDEX_HIT, // Index cache hits.
            TickerType.BLOCK_CACHE_INDEX_ADD, // Index blocks added to cache.
            TickerType.BLOCK_CACHE_INDEX_BYTES_INSERT, // Bytes inserted into index cache.
            // BLOCK_CACHE_INDEX_BYTES_EVICT removed in RocksDB 8.x
            TickerType.BLOCK_CACHE_FILTER_MISS, // Filter cache misses.
            TickerType.BLOCK_CACHE_FILTER_HIT, // Filter cache hits.
            TickerType.BLOCK_CACHE_FILTER_ADD, // Filter blocks added to cache.
            TickerType.BLOCK_CACHE_FILTER_BYTES_INSERT, // Bytes inserted in filter cache.
            // BLOCK_CACHE_FILTER_BYTES_EVICT removed in RocksDB 8.x
            TickerType.BLOCK_CACHE_DATA_MISS, // Data cache misses.
            TickerType.BLOCK_CACHE_DATA_HIT, // Data cache hits.
            TickerType.BLOCK_CACHE_DATA_ADD, // Data blocks added to cache.
            TickerType.BLOCK_CACHE_DATA_BYTES_INSERT, // Bytes inserted into data cache.
            TickerType.BLOCK_CACHE_BYTES_READ, // Bytes read from cache.
            TickerType.BLOCK_CACHE_BYTES_WRITE, // Bytes written to cache.
            TickerType.BLOOM_FILTER_USEFUL, // Bloom filter passes.
            TickerType.PERSISTENT_CACHE_HIT, // Hits in persistent cache.
            TickerType.PERSISTENT_CACHE_MISS, // Misses in persistent cache.
            TickerType.SIM_BLOCK_CACHE_HIT, // Simulated block cache hits.
            TickerType.SIM_BLOCK_CACHE_MISS, // Simulated block cache misses.
            TickerType.MEMTABLE_HIT, // Hits in the memtable.
            TickerType.MEMTABLE_MISS, // Misses in the memtable.
            TickerType.GET_HIT_L0, // Level 0 get hits.
            TickerType.GET_HIT_L1, // Level 1 get hits.
            TickerType.GET_HIT_L2_AND_UP, // Level 2 and above get hits.
            TickerType.COMPACTION_KEY_DROP_NEWER_ENTRY,
            // Keys dropped due to newer entry during compaction.
            TickerType.COMPACTION_KEY_DROP_OBSOLETE, // Obsolete keys dropped during compaction.
            TickerType.COMPACTION_KEY_DROP_RANGE_DEL,
            // Range deletion keys dropped during compaction.
            TickerType.COMPACTION_KEY_DROP_USER, // User keys dropped during compaction.
            TickerType.COMPACTION_RANGE_DEL_DROP_OBSOLETE, // Obsolete range deletes dropped.
            TickerType.NUMBER_KEYS_WRITTEN, // Total keys written.
            TickerType.NUMBER_KEYS_READ, // Total keys read.
            TickerType.NUMBER_KEYS_UPDATED, // Total keys updated.
            TickerType.BYTES_WRITTEN, // Number of bytes written.
            TickerType.BYTES_READ, // Number of bytes read.
            TickerType.NUMBER_DB_SEEK, // Number of seek operations.
            TickerType.NUMBER_DB_NEXT, // Number of next operations.
            TickerType.NUMBER_DB_PREV, // Number of previous operations.
            TickerType.NUMBER_DB_SEEK_FOUND, // Number of successful seek operations.
            TickerType.NUMBER_DB_NEXT_FOUND, // Number of successful next operations.
            TickerType.NUMBER_DB_PREV_FOUND, // Number of successful previous operations.
            TickerType.ITER_BYTES_READ, // Bytes read by iterators.
            // NO_FILE_CLOSES removed in RocksDB 8.x
            TickerType.NO_FILE_OPENS, // Number of file open operations.
            TickerType.NO_FILE_ERRORS, // Number of file errors.
            TickerType.STALL_MICROS, // Time spent in a stall micro.
            TickerType.DB_MUTEX_WAIT_MICROS, // Time spent waiting on a mutex.
            // RATE_LIMIT_DELAY_MILLIS removed in RocksDB 8.x
            // NO_ITERATORS removed in RocksDB 8.x
            TickerType.NUMBER_MULTIGET_BYTES_READ, // Bytes read by multi-get operations.
            TickerType.NUMBER_MULTIGET_KEYS_READ, // Keys read in multi-get operations.
            TickerType.NUMBER_MULTIGET_CALLS, // Number of multi-get operations.
            // NUMBER_FILTERED_DELETES removed in RocksDB 8.x
            TickerType.NUMBER_MERGE_FAILURES, // Number of merge failures.
            TickerType.BLOOM_FILTER_PREFIX_CHECKED, // Number of prefix bloom filter checks.
            TickerType.BLOOM_FILTER_PREFIX_USEFUL, // Number of useful prefix bloom filter checks.
            TickerType.NUMBER_OF_RESEEKS_IN_ITERATION, // Number of reseeks in iteration.
            TickerType.GET_UPDATES_SINCE_CALLS, // Number of get updates since calls.
            // BLOCK_CACHE_COMPRESSED_MISS removed in RocksDB 8.x
            // BLOCK_CACHE_COMPRESSED_HIT removed in RocksDB 8.x
            // BLOCK_CACHE_COMPRESSED_ADD removed in RocksDB 8.x
            // BLOCK_CACHE_COMPRESSED_ADD_FAILURES removed in RocksDB 8.x
            TickerType.WAL_FILE_SYNCED, // Number of synced WAL files.
            TickerType.WAL_FILE_BYTES, // Bytes written to WAL files.
            TickerType.WRITE_DONE_BY_SELF, // Writes completed by self.
            TickerType.WRITE_DONE_BY_OTHER, // Writes completed by others.
            // WRITE_TIMEDOUT removed in RocksDB 8.x
            TickerType.WRITE_WITH_WAL, // Writes involving WAL.
            TickerType.COMPACT_READ_BYTES, // Bytes read during compaction.
            TickerType.COMPACT_WRITE_BYTES, // Bytes written during compaction.
            TickerType.FLUSH_WRITE_BYTES, // Bytes written during flush.
            TickerType.NUMBER_DIRECT_LOAD_TABLE_PROPERTIES,
            // Number of direct load table properties.
            TickerType.NUMBER_SUPERVERSION_ACQUIRES, // Acquired superversions.
            TickerType.NUMBER_SUPERVERSION_RELEASES, // Released superversions.
            TickerType.NUMBER_SUPERVERSION_CLEANUPS, // Cleanups of superversions.
            TickerType.NUMBER_BLOCK_COMPRESSED, // Number of blocks compressed.
            TickerType.NUMBER_BLOCK_DECOMPRESSED, // Number of blocks decompressed.
            TickerType.NUMBER_BLOCK_NOT_COMPRESSED, // Number of blocks not compressed.
            TickerType.MERGE_OPERATION_TOTAL_TIME, // Time spent in merge operations.
            TickerType.FILTER_OPERATION_TOTAL_TIME, // Time spent in filter operations.
            TickerType.ROW_CACHE_HIT, // Hits in row cache.
            TickerType.ROW_CACHE_MISS, // Misses in row cache.
            TickerType.READ_AMP_ESTIMATE_USEFUL_BYTES,
            // Estimated useful bytes read due to read amplification.
            TickerType.READ_AMP_TOTAL_READ_BYTES, // Total bytes read due to read amplification.
            TickerType.NUMBER_RATE_LIMITER_DRAINS, // Number of times rate limiter is drained.
            TickerType.NUMBER_ITER_SKIP, // Number of iterator skips.
            TickerType.NUMBER_MULTIGET_KEYS_FOUND, // Number of keys found by multi-get operations.
    };

    /**
     * Histograms - treated as prometheus summaries
     * <p>
     * Array of HistogramType representing various RocksDB histograms.
     * Histograms track the distribution of certain metrics (e.g., latencies).
     * Useful for performance analysis by providing quantiles and averages.
     */
    public static final HistogramType[] HISTOGRAMS = {
            HistogramType.DB_GET, // Latency of database get operations.
            HistogramType.DB_WRITE, // Latency of database write operations.
            HistogramType.COMPACTION_TIME, // Time spent in compactions.
            HistogramType.SUBCOMPACTION_SETUP_TIME, // Time spent setting up subcompactions.
            HistogramType.TABLE_SYNC_MICROS, // Time spent synchronizing tables.
            HistogramType.COMPACTION_OUTFILE_SYNC_MICROS,
            // Time spent syncing compaction output files.
            HistogramType.WAL_FILE_SYNC_MICROS, // Time spent syncing WAL files.
            HistogramType.MANIFEST_FILE_SYNC_MICROS, // Time spent syncing manifest files.
            HistogramType.TABLE_OPEN_IO_MICROS, // Time spent opening tables (I/O).
            HistogramType.DB_MULTIGET, // Latency of database multi-get operations.
            HistogramType.READ_BLOCK_COMPACTION_MICROS,
            // Time spent reading blocks during compaction.
            HistogramType.READ_BLOCK_GET_MICROS, // Time spent reading blocks during get.
            HistogramType.WRITE_RAW_BLOCK_MICROS, // Time spent writing raw blocks.
            // STALL_L0_SLOWDOWN_COUNT removed in RocksDB 8.x
            // Note: The following constants were removed in RocksDB 8.10.2:
            // STALL_MEMTABLE_COMPACTION_COUNT, STALL_L0_NUM_FILES_COUNT,
            // HARD_RATE_LIMIT_DELAY_COUNT, SOFT_RATE_LIMIT_DELAY_COUNT
            HistogramType.NUM_FILES_IN_SINGLE_COMPACTION, // Number of files in a single compaction.
            HistogramType.DB_SEEK, // Latency of database seek operations.
            HistogramType.WRITE_STALL, // Time spent in write stalls.
            HistogramType.SST_READ_MICROS, // Time spent reading SST files.
            HistogramType.NUM_SUBCOMPACTIONS_SCHEDULED, // Number of subcompactions scheduled.
            HistogramType.BYTES_PER_READ, // Distribution of bytes read per operation.
            HistogramType.BYTES_PER_WRITE, // Distribution of bytes written per operation.
            HistogramType.BYTES_PER_MULTIGET, // Distribution of bytes read per multi-get operation.
            HistogramType.BYTES_COMPRESSED, // Distribution of compressed bytes.
            HistogramType.BYTES_DECOMPRESSED, // Distribution of decompressed bytes.
            HistogramType.COMPRESSION_TIMES_NANOS, // Time spent in compression operations.
            HistogramType.DECOMPRESSION_TIMES_NANOS, // Time spent in decompression operations.
            HistogramType.READ_NUM_MERGE_OPERANDS, // Distribution of number of merge operands read.
    };
}
