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

package org.apache.hugegraph.backend.store.rocksdbcloud;

import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;

import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.OptionHolder;

/**
 * Configuration options for the RocksDB-Cloud backend (S3-backed storage).*
 * Usage in hugegraph.properties:
 * <pre>
 *   backend=rocksdb-cloud
 *   serializer=binary
 *
 *   rocksdb.data_path=rocksdb-cloud-data/data
 *   rocksdb.wal_path=rocksdb-cloud-data/wal
 *
 *   rocksdb.cloud.s3_bucket_name=my-hugegraph-bucket
 *   rocksdb.cloud.s3_region=us-east-1
 *   rocksdb.cloud.s3_object_prefix=hugegraph/
 *   # Optional: leave empty to use IAM role / environment credentials
 *   rocksdb.cloud.aws_access_key_id=
 *   rocksdb.cloud.aws_secret_access_key=
 *
 *   # Durability mode (production recommendation):
 *   #   sync  — S3 upload happens inline on every write commit (zero data-loss)
 *   #   async — S3 upload happens in background (higher throughput, bounded loss)
 *   rocksdb.cloud.sync_mode=sync
 *
 *   # Only relevant in async mode — ignored when sync_mode=sync:
 *   rocksdb.cloud.sync_interval_seconds=60
 *   rocksdb.cloud.sync_on_write_count=100000
 * </pre>
 */
public class RocksDBCloudOptions extends OptionHolder {

    private RocksDBCloudOptions() {
        super();
    }

    private static volatile RocksDBCloudOptions instance;

    public static synchronized RocksDBCloudOptions instance() {
        if (instance == null) {
            instance = new RocksDBCloudOptions();
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<String> S3_BUCKET_NAME =
            new ConfigOption<>(
                    "rocksdb.cloud.s3_bucket_name",
                    "The S3 bucket name used for RocksDB Cloud storage.",
                    disallowEmpty(),
                    "hugegraph-rocksdb"
            );

    public static final ConfigOption<String> S3_REGION =
            new ConfigOption<>(
                    "rocksdb.cloud.s3_region",
                    "The AWS region of the S3 bucket.",
                    disallowEmpty(),
                    "us-east-1"
            );

    public static final ConfigOption<String> S3_OBJECT_PREFIX =
            new ConfigOption<>(
                    "rocksdb.cloud.s3_object_prefix",
                    "The object key prefix within the S3 bucket (acts as a directory " +
                    "within the bucket). Must end with '/'.",
                    null,
                    "hugegraph/"
            );

    public static final ConfigOption<String> AWS_ACCESS_KEY_ID =
            new ConfigOption<>(
                    "rocksdb.cloud.aws_access_key_id",
                    "AWS Access Key ID for S3 authentication. " +
                    "Leave empty to use IAM role or environment credentials.",
                    null,
                    ""
            );

    public static final ConfigOption<String> AWS_SECRET_ACCESS_KEY =
            new ConfigOption<>(
                    "rocksdb.cloud.aws_secret_access_key",
                    "AWS Secret Access Key for S3 authentication. " +
                    "Leave empty to use IAM role or environment credentials.",
                    null,
                    ""
            );

    public static final ConfigOption<String> S3_ENDPOINT =
            new ConfigOption<>(
                    "rocksdb.cloud.s3_endpoint",
                    "Optional custom S3-compatible endpoint URL (e.g. MinIO). " +
                    "Leave empty to use the standard AWS endpoint.",
                    null,
                    ""
            );

    public static final ConfigOption<Boolean> S3_PATH_STYLE_ACCESS =
            new ConfigOption<>(
                    "rocksdb.cloud.s3_path_style_access",
                    "Use path-style access for S3 (required by MinIO and " +
                    "some S3-compatible stores).",
                    null,
                    false
            );

    public static final ConfigOption<Integer> SYNC_INTERVAL_SECONDS =
            new ConfigOption<>(
                    "rocksdb.cloud.sync_interval_seconds",
                    "How often (in seconds) to automatically sync local SST files to S3. " +
                    "Set to 0 to disable periodic sync (sync only on close). " +
                    "Recommended: 30-300 seconds for production to limit data loss window.",
                    null,
                    60
            );

    public static final ConfigOption<Long> SYNC_ON_WRITE_COUNT =
            new ConfigOption<>(
                    "rocksdb.cloud.sync_on_write_count",
                    "Trigger an incremental S3 sync after this many write operations " +
                    "(vertices + edges). Set to 0 to disable write-count-based sync. " +
                    "Works in combination with sync_interval_seconds.",
                    null,
                    100_000L
            );

    public static final ConfigOption<Boolean> SYNC_INCREMENTAL =
            new ConfigOption<>(
                    "rocksdb.cloud.sync_incremental",
                    "When true, only upload SST files that are new or changed since the " +
                    "last sync (skip files whose size+name already exist in S3). " +
                    "Greatly reduces S3 PUT costs and sync time for large databases. " +
                    "When false, upload all files on every sync (safer but slower).",
                    null,
                    true
            );

    /**
     * Controls whether S3 sync happens synchronously on every write commit
     * (production-safe, zero data-loss window) or asynchronously in the
     * background (higher throughput, bounded data-loss window).
     *
     * <ul>
     *   <li><b>async</b> (default) — S3 upload runs in a background thread.
     *       Writes are fast; data-loss window = sync_interval_seconds or
     *       sync_on_write_count, whichever fires first.</li>
     *   <li><b>sync</b> — every {@code onWriteCommit} flushes memtable and
     *       uploads changed SST files to S3 before returning to the caller.
     *       Zero data-loss window. Write throughput is limited by S3 PUT
     *       latency (typically 5–50 ms per sync on LAN/MinIO).
     *       Use this for production workloads where durability matters more
     *       than raw write speed.</li>
     * </ul>
     */
    public static final ConfigOption<String> SYNC_MODE =
            new ConfigOption<>(
                    "rocksdb.cloud.sync_mode",
                    "S3 sync durability mode: 'async' (background sync, higher throughput) " +
                    "or 'sync' (synchronous S3 flush on every write commit, zero data-loss " +
                    "window, production-safe). Default is 'async'.",
                    null,
                    "async"
            );
}
