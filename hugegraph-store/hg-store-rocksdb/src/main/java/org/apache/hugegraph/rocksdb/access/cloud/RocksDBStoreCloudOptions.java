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

package org.apache.hugegraph.rocksdb.access.cloud;

import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;
import static org.apache.hugegraph.config.OptionChecker.rangeInt;

import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.OptionHolder;

@SuppressWarnings("unused")
public class RocksDBStoreCloudOptions extends OptionHolder {

    public static final ConfigOption<Boolean> CLOUD_ENABLED =
            new ConfigOption<>(
                    "rocksdb.cloud_enabled",
                    "Enable cloud sync for store-side RocksDB.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<String> CLOUD_BUCKET =
            new ConfigOption<>(
                    "rocksdb.cloud_bucket",
                    "Cloud storage bucket for store-side RocksDB files.",
                    null,
                    "hugegraph-rocksdb"
            );

    public static final ConfigOption<String> CLOUD_ENDPOINT =
            new ConfigOption<>(
                    "rocksdb.cloud_endpoint",
                    "Cloud storage endpoint URL for S3-compatible providers.",
                    null,
                    ""
            );

    public static final ConfigOption<String> CLOUD_REGION =
            new ConfigOption<>(
                    "rocksdb.cloud_region",
                    "Cloud storage region used by SDK.",
                    null,
                    "us-east-1"
            );

    public static final ConfigOption<String> CLOUD_ACCESS_KEY =
            new ConfigOption<>(
                    "rocksdb.cloud_access_key",
                    "Cloud storage access key.",
                    null,
                    ""
            );

    public static final ConfigOption<String> CLOUD_SECRET_KEY =
            new ConfigOption<>(
                    "rocksdb.cloud_secret_key",
                    "Cloud storage secret key.",
                    null,
                    ""
            );

    public static final ConfigOption<Boolean> CLOUD_PATH_STYLE =
            new ConfigOption<>(
                    "rocksdb.cloud_path_style",
                    "Use path-style addressing for compatible object storage providers.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<String> CLOUD_OBJECT_PREFIX =
            new ConfigOption<>(
                    "rocksdb.cloud_object_prefix",
                    "Node-specific cloud object prefix, e.g. store0.",
                    null,
                    "store"
            );

    public static final ConfigOption<Integer> CLOUD_SYNC_INTERVAL_SECONDS =
            new ConfigOption<>(
                    "rocksdb.cloud_sync_interval_seconds",
                    "Periodic sync interval in seconds, 0 to disable.",
                    rangeInt(0, Integer.MAX_VALUE),
                    60
            );

    public static final ConfigOption<Boolean> CLOUD_SYNC_INCREMENTAL =
            new ConfigOption<>(
                    "rocksdb.cloud_sync_incremental",
                    "Upload changed files only.",
                    disallowEmpty(),
                    true
            );

     public static final ConfigOption<Boolean> SYNCHRONOUS_SST_UPLOAD_MODE =
             new ConfigOption<>(
                     "rocksdb.cloud.synchronous_sst_upload_mode",
                     "Single control flag for cloud upload mode. If true, SST-triggered uploads " +
                     "run synchronously. If false, SST-triggered uploads are disabled and cloud " +
                     "sync uses periodic background reconciliation only.",
                     disallowEmpty(),
                      true
             );

     public static final ConfigOption<Integer> CLOUD_SYNC_RETRY_MAX =
            new ConfigOption<>(
                    "rocksdb.cloud_sync_retry_max",
                    "Max retries when commit-time sync waits for syncInProgress lock.",
                    rangeInt(1, Integer.MAX_VALUE),
                    100
            );

    public static final ConfigOption<Integer> CLOUD_SYNC_RETRY_BACKOFF_MS =
            new ConfigOption<>(
                    "rocksdb.cloud_sync_retry_backoff_ms",
                    "Initial backoff in milliseconds for commit-time sync retry loop.",
                    rangeInt(1, Integer.MAX_VALUE),
                    10
            );

    public static final ConfigOption<Integer> CLOUD_SYNC_RETRY_MAX_BACKOFF_MS =
            new ConfigOption<>(
                    "rocksdb.cloud_sync_retry_max_backoff_ms",
                    "Maximum backoff cap in milliseconds for exponential backoff.",
                    rangeInt(1, Integer.MAX_VALUE),
                    1000
            );

    private static volatile RocksDBStoreCloudOptions instance;

    private RocksDBStoreCloudOptions() {
        super();
    }

    public static synchronized RocksDBStoreCloudOptions instance() {
        if (instance == null) {
            instance = new RocksDBStoreCloudOptions();
            instance.registerOptions();
        }
        return instance;
    }
}

