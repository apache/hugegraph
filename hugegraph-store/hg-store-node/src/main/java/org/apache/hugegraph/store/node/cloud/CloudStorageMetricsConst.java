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

package org.apache.hugegraph.store.node.cloud;

/**
 * Constants for cloud storage metrics.
 * Defines metric names and tags used for monitoring cloud storage operations.
 */
public final class CloudStorageMetricsConst {

    // Prefix used for all cloud storage metrics
    public static final String PREFIX = "cloud_storage";

    // Metric names
    public static final String CONFIRMED_FILES = PREFIX + "_confirmed_files_total";
    public static final String UPLOAD_FAILURES = PREFIX + "_upload_failures_total";
    public static final String RETRY_QUEUE_SIZE = PREFIX + "_retry_queue_size";
    public static final String SYNC_LATENCY_MS = PREFIX + "_sync_latency_ms";
    public static final String DELETE_GUARD_REUPLOAD_COUNT = PREFIX + "_delete_guard_reupload_count";
    // 1 = DLQ on-disk persistence healthy; 0 = degraded (retry intent may not survive a crash).
    public static final String DLQ_PERSISTENCE_HEALTHY = PREFIX + "_dlq_persistence_healthy";
    // 1 = pending-delete marker persistence healthy; 0 = degraded (a marker could not be durably
    // written, so a DB delete during a provider-unavailable window may not be guarded against
    // re-hydration after a crash). A hard error state: delete progression is held while degraded.
    public static final String DELETE_MARKER_HEALTHY = PREFIX + "_delete_marker_healthy";

    // Tag names
    public static final String TAG_DB_NAME = "db_name";

    private CloudStorageMetricsConst() {
    }
}


