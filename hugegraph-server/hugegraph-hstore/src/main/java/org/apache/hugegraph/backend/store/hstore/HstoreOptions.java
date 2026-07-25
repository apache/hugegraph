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
 * Configuration options for the hstore backend.
 *
 * <p>Usage in hugegraph.properties:</p>
 * <pre>
 *   backend=hstore
 *   serializer=binary
 *   hstore.partition_count=16
 *
 *   # Optional: Enable cloud storage sync (S3-compatible, Azure, GCS, etc.)
 *   hstore.cloud_enabled=true
 *   hstore.cloud_provider=s3                    # Cloud storage provider (default: s3)
 *   hstore.cloud_bucket=my-graph-data
 *   hstore.cloud_region=us-east-1
 *   hstore.cloud_endpoint=<a href="https://s3.amazonaws.com">...</a>  # or S3-compatible endpoint
 *   hstore.cloud_access_key=your_access_key
 *   hstore.cloud_secret_key=your_secret_key
 *   hstore.cloud_path_style=false               # true for some S3-compatible providers
 *
 *   # Cloud storage sync durability mode
 *   hstore.cloud_sync_mode=sync                 # sync (cloud-first) or async
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

    // Cloud storage sync options
    public static final ConfigOption<Boolean> CLOUD_ENABLED = new ConfigOption<>(
            "hstore.cloud_enabled",
            "Enable cloud storage sync (S3-compatible, Azure, GCS) for store-side data durability.",
            disallowEmpty(),
            false
    );

    public static final ConfigOption<String> CLOUD_BUCKET = new ConfigOption<>(
            "hstore.cloud_bucket",
            "Cloud storage bucket name. Each store node should use its own bucket.",
            null,
            "hugegraph-data"
    );

    public static final ConfigOption<String> CLOUD_REGION = new ConfigOption<>(
            "hstore.cloud_region",
            "Cloud storage region (for S3-compatible providers). Ignored if using custom endpoint URL.",
            null,
            "us-east-1"
    );

    public static final ConfigOption<String> CLOUD_ENDPOINT = new ConfigOption<>(
            "hstore.cloud_endpoint",
            "Custom S3-compatible endpoint URL. Leave empty for AWS S3.",
            null,
            ""
    );

    public static final ConfigOption<Boolean> CLOUD_PATH_STYLE = new ConfigOption<>(
            "hstore.cloud_path_style",
            "Use path-style addressing (required for some S3-compatible providers).",
            disallowEmpty(),
            false
    );

    public static final ConfigOption<String> CLOUD_SYNC_MODE = new ConfigOption<>(
            "hstore.cloud_sync_mode",
            "Cloud storage sync durability mode: 'sync' (cloud-first, zero data-loss, " +
            "synchronous cloud flush on every commit) or 'async' (higher throughput, " +
            "background sync with bounded loss).",
            null,
            "sync"
    );

    public static final ConfigOption<Integer> CLOUD_SYNC_INTERVAL_SECONDS = new ConfigOption<>(
            "hstore.cloud_sync_interval_seconds",
            "Periodic cloud storage sync interval in seconds (only used in async mode). " +
            "0 to disable periodic sync.",
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
