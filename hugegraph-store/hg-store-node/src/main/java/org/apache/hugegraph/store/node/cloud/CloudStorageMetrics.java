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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.store.node.util.HgAssert;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

/**
 * Cloud Storage Metrics registration and management.
 * Exposes metrics for cloud storage operations including upload success/failure,
 * sync latency, confirmed files (SSTs synced to cloud), and delete guard re-upload activity.
 */
@Slf4j
public final class CloudStorageMetrics {

    private static MeterRegistry registry;
    private static CloudSyncTracker syncTracker;

    // Per-database counters for delete guard re-uploads
    private static final ConcurrentHashMap<String, AtomicLong> deleteGuardReuploadPerDb =
            new ConcurrentHashMap<>();

    // Overall upload failure counter (will be incremented with tags)
    private static Counter uploadFailuresCounter;

    // Sync latency timer
    private static Timer syncLatencyTimer;

    private CloudStorageMetrics() {
    }

    /**
     * Initialize cloud storage metrics.
     * Must be called once at application startup with MeterRegistry and CloudSyncTracker instances.
     */
    public static void init(final MeterRegistry meterRegistry, final CloudSyncTracker tracker) {
        HgAssert.isArgumentNotNull(meterRegistry, "meterRegistry");
        HgAssert.isArgumentNotNull(tracker, "CloudSyncTracker");

        if (registry != null) {
            return;
        }

        registry = meterRegistry;
        syncTracker = tracker;

        // Register global metrics
        uploadFailuresCounter = Counter.builder(CloudStorageMetricsConst.UPLOAD_FAILURES)
                .description("Total upload failures (transient and permanent)")
                .register(registry);

        syncLatencyTimer = Timer.builder(CloudStorageMetricsConst.SYNC_LATENCY_MS)
                .description("Time in milliseconds from SST file creation to cloud confirmation")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        // Register gauge for retry queue size (monitored at runtime)
        Gauge.builder(CloudStorageMetricsConst.RETRY_QUEUE_SIZE, () -> 0)
                .description("Number of files waiting in the upload retry queue")
                .register(registry);

        log.info("Cloud storage metrics initialized");
    }

    /**
     * Register a per-database metrics gauges.
     * Should be called when a database opens for the first time.
     */
    public static void registerDatabaseMetrics(final String dbName) {
        if (registry == null) {
            return;
        }

        try {
            // Register confirmed files gauge for this database (files successfully synced to cloud)
            Gauge.builder(CloudStorageMetricsConst.CONFIRMED_FILES,
                    () -> getConfirmedFileCount(dbName))
                    .description("Number of SST files confirmed present in cloud storage")
                    .tag(CloudStorageMetricsConst.TAG_DB_NAME, dbName)
                    .register(registry);

            // Register delete guard re-upload counter for this database
            deleteGuardReuploadPerDb.putIfAbsent(dbName, new AtomicLong(0));

            Gauge.builder(CloudStorageMetricsConst.DELETE_GUARD_REUPLOAD_COUNT,
                    () -> getDeleteGuardReuploadCount(dbName))
                    .description("Number of times delete guard re-uploaded unconfirmed files during " +
                                 "compaction")
                    .tag(CloudStorageMetricsConst.TAG_DB_NAME, dbName)
                    .register(registry);

            log.debug("Registered cloud storage metrics for database: {}", dbName);
        } catch (IllegalArgumentException e) {
            // Gauge already registered for this database
            log.debug("Cloud storage metrics already registered for database: {}", dbName);
        }
    }

    /**
     * Record a sync latency measurement (time to confirm upload to cloud).
     */
    @SuppressWarnings("unused")  // dbName reserved for future per-db latency tracking
    public static void recordSyncLatency(final String dbName, final long latencyMs) {
        if (syncLatencyTimer == null) {
            return;
        }
        syncLatencyTimer.record(latencyMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * Record an upload failure.
     */
    @SuppressWarnings("unused")  // Parameters reserved for future per-db/cf failure tracking
    public static void recordUploadFailure(final String dbName, final String cfName,
                                           final String errorType) {
        if (uploadFailuresCounter == null) {
            return;
        }
        uploadFailuresCounter.increment();
    }

    /**
     * Get the current count of confirmed SST files for a database (from bitmap).
     */
    public static long getConfirmedFileCount(final String dbName) {
        if (syncTracker == null) {
            return 0;
        }
        return syncTracker.confirmedCount(dbName);
    }

    /**
     * Increment the delete guard re-upload count for a database.
     */
    public static void incrementDeleteGuardReupload(final String dbName) {
        deleteGuardReuploadPerDb.computeIfAbsent(dbName, k -> new AtomicLong(0)).incrementAndGet();
    }

    /**
     * Get the delete guard re-upload count for a database.
     */
    public static long getDeleteGuardReuploadCount(final String dbName) {
        AtomicLong count = deleteGuardReuploadPerDb.get(dbName);
        return count != null ? count.get() : 0;
    }

    /**
     * Update retry queue size (called by CloudUploadRetryQueue).
     */
    @SuppressWarnings("unused")  // Placeholder for future dynamic retry queue size tracking
    public static void setRetryQueueSize(final int size) {
        // This is exposed as a gauge; the actual value is updated externally
    }
}





