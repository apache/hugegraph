# Standard RocksDB runtime upgrade

The standard runtime aligns RocksDB JNI to 8.10.2. Validate existing data before rolling out this dependency change. Keep a restorable backup made by the old runtime; do not assume that data opened or modified by 8.10.2 can be reopened by an older runtime. Rollback requires restoring the backup with the matching old application and dependencies, rather than replacing the JAR in an upgraded data directory.

All components use JRaft 1.3.14: Server upgrades from 1.3.11, and PD/Store from 1.3.13. Older JRaft versions call compressed-cache methods removed from RocksDB 8.x, preventing Raft storage initialization, including Server when `raft.mode=true`. Treat the JNI and JRaft changes as one service upgrade and validate existing Raft logs with the complete new package.

## RocksDB compatibility test

The permanent [compatibility test](../hugegraph-server/hugegraph-test/src/test/rocksdb-compatibility/run.sh) uses Maven effective POMs to compare the actual Server, PD and Store RocksDB versions. CI compares PR base/head or push before/after, fails if a version cannot be resolved, and runs only changed version pairs, deduplicated across components. POM or JRaft changes alone do not run the JNI test. No historical version matrix is maintained.

Run from two complete source checkouts with Java 11+, Maven and Python 3:

```bash
bash hugegraph-server/hugegraph-test/src/test/rocksdb-compatibility/run.sh \
  /path/to/before /path/to/after /tmp/new-rocksdb-compatibility-run
```

Each pair creates synthetic SST and synchronous WAL data with the old JNI, reads/modifies/deletes/adds data with the new JNI, then verifies it in a separate JVM. Original data, commands, JAR hashes, native versions and failure evidence are retained in the new output directory. This test never opens business data, runs at application startup, or enters a production binary distribution. When changing this test or its CI, verify both changed/unchanged version selection and run the changed version pairs. Service-created data, Raft logs, cluster upgrades and Topling acceptance remain separate checks.

## Store monitoring changes

The JNI upgrade removes registration of these unavailable ticker constants:

- `BLOCK_CACHE_INDEX_BYTES_EVICT`, `BLOCK_CACHE_FILTER_BYTES_EVICT`
- `NO_FILE_CLOSES`, `RATE_LIMIT_DELAY_MILLIS`, `NO_ITERATORS`
- `NUMBER_FILTERED_DELETES`
- `BLOCK_CACHE_COMPRESSED_MISS`, `BLOCK_CACHE_COMPRESSED_HIT`,
  `BLOCK_CACHE_COMPRESSED_ADD`, `BLOCK_CACHE_COMPRESSED_ADD_FAILURES`
- `WRITE_TIMEDOUT`

It also removes these histogram constants:

- `STALL_L0_SLOWDOWN_COUNT`, `STALL_MEMTABLE_COMPACTION_COUNT`,
  `STALL_L0_NUM_FILES_COUNT`
- `HARD_RATE_LIMIT_DELAY_COUNT`, `SOFT_RATE_LIMIT_DELAY_COUNT`

Store meter names use `rocks.stats.` plus the lowercase constant name. For histograms, the removed meters include `.max`, `.mean`, `.min`, `.summary` (with its quantile tags), `.summary.sum` and `.summary.count`. Exporters may normalize names further. Update dashboards and alerts that reference these series; their disappearance is not a zero reading. No replacement series or semantic equivalence is asserted here. See [RocksDBMetricsConst](../hugegraph-store/hg-store-node/src/main/java/org/apache/hugegraph/store/node/metrics/RocksDBMetricsConst.java) and [meter registration](../hugegraph-store/hg-store-node/src/main/java/org/apache/hugegraph/store/node/metrics/RocksDBMetrics.java).
