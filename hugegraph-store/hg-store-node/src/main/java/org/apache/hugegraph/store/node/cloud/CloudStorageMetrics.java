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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

import org.apache.hugegraph.store.node.util.HgAssert;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
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

    // Per-database registered gauges, tracked so they can be removed when a DB is deleted.
    // Without this, per-DB (db_name-tagged) meters accumulate forever as DBs are created and
    // destroyed (partition rebalancing, graph drops), causing unbounded meter cardinality.
    private static final ConcurrentHashMap<String, List<Meter>> perDbMeters =
            new ConcurrentHashMap<>();

    // Overall upload failure counter (will be incremented with tags)
    private static Counter uploadFailuresCounter;

    // Sync latency timer
    private static Timer syncLatencyTimer;

    // Live source for the RETRY_QUEUE_SIZE gauge. Bound to the retry queue via
    // bindRetryQueueSizeSupplier once it is constructed; until then the gauge reports 0.
    // volatile so the gauge (read on scrape threads) sees the binding published by startup wiring.
    private static volatile IntSupplier retryQueueSizeSupplier = () -> 0;

    // Live source for the DLQ_PERSISTENCE_HEALTHY gauge (1 = healthy, 0 = degraded). Bound to the
    // retry queue via bindDlqPersistenceHealthySupplier once it is constructed; until then reports 1.
    private static volatile IntSupplier dlqPersistenceHealthySupplier = () -> 1;

    // Live source for the DELETE_MARKER_HEALTHY gauge (1 = healthy, 0 = degraded). Bound to the
    // listener via bindDeleteMarkerHealthySupplier once it is constructed; until then reports 1.
    private static volatile IntSupplier deleteMarkerHealthySupplier = () -> 1;

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

        // Register gauge for retry queue size (monitored at runtime). The supplier is read on every
        // scrape and delegates to retryQueueSizeSupplier, which is bound to the live retry queue via
        // bindRetryQueueSizeSupplier once it is constructed (defaults to 0 until then).
        Gauge.builder(CloudStorageMetricsConst.RETRY_QUEUE_SIZE,
                      () -> retryQueueSizeSupplier.getAsInt())
                .description("Number of files waiting in the upload retry queue")
                .register(registry);

        // DLQ on-disk persistence health: 1 = healthy, 0 = degraded (a DLQ append/rewrite failed,
        // so retry intent may not survive a crash). Bound to the live queue via
        // bindDlqPersistenceHealthySupplier; defaults to 1 until then.
        Gauge.builder(CloudStorageMetricsConst.DLQ_PERSISTENCE_HEALTHY,
                      () -> dlqPersistenceHealthySupplier.getAsInt())
                .description("DLQ on-disk persistence health (1=healthy, 0=degraded)")
                .register(registry);

        // Pending-delete marker persistence health: 1 = healthy, 0 = degraded (a marker could not be
        // durably fsynced, so a DB delete may be unguarded against re-hydration after a crash). Bound
        // to the live listener via bindDeleteMarkerHealthySupplier; defaults to 1 until then.
        Gauge.builder(CloudStorageMetricsConst.DELETE_MARKER_HEALTHY,
                      () -> deleteMarkerHealthySupplier.getAsInt())
                .description("Pending-delete marker persistence health (1=healthy, 0=degraded)")
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
            // Micrometer registration is idempotent per registry: registering the same name+tags
            // returns the existing meter. We (re-)capture the returned meters into perDbMeters so
            // unregisterDatabaseMetrics can remove precisely these when the DB is deleted. Using
            // put() (overwrite) keeps exactly one tracking entry per DB regardless of how often
            // this is called, and correctly re-tracks if the underlying registry was swapped.
            List<Meter> meters = new CopyOnWriteArrayList<>();

            // Register confirmed files gauge for this database (files successfully synced to cloud)
            Gauge confirmed = Gauge.builder(CloudStorageMetricsConst.CONFIRMED_FILES,
                    () -> getConfirmedFileCount(dbName))
                    .description("Number of SST files confirmed present in cloud storage")
                    .tag(CloudStorageMetricsConst.TAG_DB_NAME, dbName)
                    .register(registry);
            meters.add(confirmed);

            // Register delete guard re-upload counter for this database
            deleteGuardReuploadPerDb.putIfAbsent(dbName, new AtomicLong(0));

            Gauge reupload = Gauge.builder(CloudStorageMetricsConst.DELETE_GUARD_REUPLOAD_COUNT,
                    () -> getDeleteGuardReuploadCount(dbName))
                    .description("Number of times delete guard re-uploaded unconfirmed files during " +
                                 "compaction")
                    .tag(CloudStorageMetricsConst.TAG_DB_NAME, dbName)
                    .register(registry);
            meters.add(reupload);

            perDbMeters.put(dbName, meters);
            log.debug("Registered cloud storage metrics for database: {}", dbName);
        } catch (IllegalArgumentException e) {
            // Gauge already registered for this database
            log.debug("Cloud storage metrics already registered for database: {}", dbName);
        }
    }

    /**
     * Removes all per-database metrics registered by {@link #registerDatabaseMetrics} for a DB that
     * has been deleted, so meter cardinality does not grow without bound as DBs are created and
     * destroyed. Safe to call for a DB that was never registered.
     */
    public static void unregisterDatabaseMetrics(final String dbName) {
        deleteGuardReuploadPerDb.remove(dbName);
        List<Meter> meters = perDbMeters.remove(dbName);
        if (meters == null || registry == null) {
            return;
        }
        for (Meter m : meters) {
            try {
                registry.remove(m);
            } catch (RuntimeException e) {
                log.debug("Failed to remove cloud storage meter for db={}: {}",
                          dbName, e.getMessage());
            }
        }
        log.debug("Unregistered cloud storage metrics for database: {}", dbName);
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
     * Binds the RETRY_QUEUE_SIZE gauge to a live source (e.g. {@code retryQueue::getInFlightCount}).
     * The supplier is polled on every metrics scrape, so the gauge always reflects the current
     * backlog. Call once during startup after the retry queue is constructed.
     */
    public static void bindRetryQueueSizeSupplier(final IntSupplier supplier) {
        retryQueueSizeSupplier = (supplier != null) ? supplier : () -> 0;
    }

    /**
     * Sets a fixed retry-queue-size value for the gauge. Prefer {@link #bindRetryQueueSizeSupplier}
     * to track a live queue; this push-style setter is retained for tests and manual overrides.
     */
    public static void setRetryQueueSize(final int size) {
        retryQueueSizeSupplier = () -> size;
    }

    /**
     * Binds the DLQ_PERSISTENCE_HEALTHY gauge to a live source (e.g.
     * {@code () -> retryQueue.isDlqPersistenceHealthy() ? 1 : 0}). Polled on every scrape. Call once
     * during startup after the retry queue is constructed.
     */
    public static void bindDlqPersistenceHealthySupplier(final IntSupplier supplier) {
        dlqPersistenceHealthySupplier = (supplier != null) ? supplier : () -> 1;
    }

    /**
     * Binds the DELETE_MARKER_HEALTHY gauge to a live source (e.g.
     * {@code () -> listener.isDeleteMarkerHealthy() ? 1 : 0}). Polled on every scrape. Call once
     * during startup after the listener is constructed.
     */
    public static void bindDeleteMarkerHealthySupplier(final IntSupplier supplier) {
        deleteMarkerHealthySupplier = (supplier != null) ? supplier : () -> 1;
    }
}





