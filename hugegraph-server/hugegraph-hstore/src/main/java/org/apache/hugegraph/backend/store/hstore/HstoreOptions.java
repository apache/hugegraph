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

import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;
import static org.apache.hugegraph.config.OptionChecker.rangeInt;

import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.OptionHolder;

/**
 * Configuration options for the hstore backend (distributed storage with optional cloud sync).
 * Usage in hugegraph.properties:
 * <pre>
 *   backend=hstore
 *   serializer=binary
 *   hstore.partition_count=16
 *
 *   # Optional: Enable cloud sync (S3/MinIO)
 *   hstore.cloud_enabled=true
 *   hstore.cloud_s3_bucket=my-graph-data
 *   hstore.cloud_s3_region=us-east-1
 *   hstore.cloud_s3_endpoint=<a href="https://s3.amazonaws.com">...</a>  # or MinIO endpoint
 *   hstore.cloud_s3_access_key=your_access_key
 *   hstore.cloud_s3_secret_key=your_secret_key
 *   hstore.cloud_s3_path_style=false  # true for MinIO
 *
 *   # Cloud sync durability mode
 *   hstore.cloud_sync_mode=sync  # sync or async
 *   hstore.cloud_sync_interval_seconds=60
 *   hstore.cloud_sync_incremental=true
 * </pre>
 */
public class HstoreOptions extends OptionHolder {

    public static final ConfigOption<Integer> PARTITION_COUNT = new ConfigOption<>(
            "hstore.partition_count",
            "Number of partitions, which PD controls partitions based on.",
            disallowEmpty(),
            0
    );

    // Cloud sync options
    public static final ConfigOption<Boolean> CLOUD_ENABLED = new ConfigOption<>(
            "hstore.cloud_enabled",
            "Enable cloud sync (S3/MinIO) for store-side data durability.",
            disallowEmpty(),
            false
    );

    public static final ConfigOption<String> CLOUD_S3_BUCKET = new ConfigOption<>(
            "hstore.cloud_s3_bucket",
            "S3 bucket name for cloud storage. Each store node should use its own bucket.",
            null,
            "hugegraph-data"
    );

    public static final ConfigOption<String> CLOUD_S3_REGION = new ConfigOption<>(
            "hstore.cloud_s3_region",
            "AWS region for S3 bucket. Ignored if using S3 endpoint URL.",
            null,
            "us-east-1"
    );

    public static final ConfigOption<String> CLOUD_S3_ENDPOINT = new ConfigOption<>(
            "hstore.cloud_s3_endpoint",
            "Custom S3-compatible endpoint URL (e.g., MinIO). Leave empty for AWS S3.",
            null,
            ""
    );

    public static final ConfigOption<Boolean> CLOUD_S3_PATH_STYLE = new ConfigOption<>(
            "hstore.cloud_s3_path_style",
            "Use path-style addressing (required for MinIO and some S3-compatible stores).",
            disallowEmpty(),
            false
    );

    public static final ConfigOption<String> CLOUD_SYNC_MODE = new ConfigOption<>(
            "hstore.cloud_sync_mode",
            "Cloud sync durability mode: 'sync' (zero data-loss, synchronous S3 flush on " +
            "every commit) or 'async' (higher throughput, background sync with bounded loss).",
            null,
            "sync"
    );

    public static final ConfigOption<Integer> CLOUD_SYNC_INTERVAL_SECONDS = new ConfigOption<>(
            "hstore.cloud_sync_interval_seconds",
            "Periodic S3 sync interval in seconds (only used in async mode). 0 to disable periodic sync.",
            rangeInt(0, Integer.MAX_VALUE),
            60
    );

    private static volatile HstoreOptions instance;

    private HstoreOptions() {
        super();
    }

    public static synchronized HstoreOptions instance() {
        if (instance == null) {
            instance = new HstoreOptions();
            instance.registerOptions();
        }
        return instance;
    }
}
