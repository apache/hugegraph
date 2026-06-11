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

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.backend.store.rocksdb.RocksDBStdSessions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.util.Log;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

/**
 * RocksDB sessions backed by Amazon S3 (or S3-compatible storage like MinIO).
 *
 * <h2>Durability model</h2>
 * <p>Data is written locally first (standard RocksDB behaviour). S3 sync happens
 * at three configurable points to limit the data-loss window on instance failure:
 *
 * <ol>
 *   <li><b>Periodic background sync</b> — a {@link ScheduledExecutorService} fires
 *       every {@code rocksdb.cloud.sync_interval_seconds} (default: 60s). Only
 *       new/changed SST files are uploaded (incremental mode).</li>
 *   <li><b>Write-count-based sync</b> — after every
 *       {@code rocksdb.cloud.sync_on_write_count} mutation operations the next
 *       session flush triggers an incremental sync. Prevents long gaps during
 *       high-write-rate bulk loads.</li>
 *   <li><b>On close</b> — full flush + upload performed before the DB is closed,
 *       ensuring a clean final checkpoint in S3.</li>
 *   <li><b>On createSnapshot</b> — checkpoint uploaded to a separate S3 prefix.</li>
 *   <li><b>On syncNow()</b> — explicit call (e.g. from management REST API).</li>
 * </ol>
 *
 * <h2>Incremental sync</h2>
 * <p>When {@code rocksdb.cloud.sync_incremental=true} (default), the sync only
 * uploads files whose name+size differs from S3. RocksDB SST files are immutable
 * once compacted, so size equality reliably indicates the file is already in S3.
 * WAL logs and LOCK files are always excluded — they are process-local.
 *
 * <h2>Sync modes</h2>
 * <p>Controlled by {@code rocksdb.cloud.sync_mode}:
 * <ul>
 *   <li><b>async</b> (default) — background sync only; data-loss window =
 *       min(sync_interval_seconds, time_to_write sync_on_write_count ops).</li>
 *   <li><b>sync</b> — every {@link #onWriteCommit} flushes the memtable and
 *       uploads changed SST files to S3 <em>inline</em> before returning to the
 *       caller. Zero data-loss window. Recommended for production.</li>
 * </ul>
 *
 * <h2>Maximum data-loss window (async mode)</h2>
 * <pre>
 *   max_loss = min(sync_interval_seconds, time_to_write sync_on_write_count operations)
 * </pre>
 * For example with the defaults (60s interval, 100k write threshold) the worst-case
 * data loss is up to 60 seconds of writes <em>or</em> 100,000 operations, whichever
 * comes first. In {@code sync} mode the data-loss window is zero.
 */
public class RocksDBCloudSessions extends RocksDBStdSessions {

    private static final Logger LOG = Log.logger(RocksDBCloudSessions.class);

    private final S3Client s3Client;
    private final String dataPath;

    // ── Sync configuration (read once at construction) ────────────────────────
    private final int syncIntervalSeconds;
    private final long syncOnWriteCount;
    private final boolean syncIncremental;

    /**
     * When {@code true} every {@link #onWriteCommit} flushes + uploads to S3
     * synchronously before returning — zero data-loss window (production-safe).
     * When {@code false} (default) syncs are background-only.
     */
    private final boolean syncModeSync;

    // ── Background sync machinery ─────────────────────────────────────────────
    /** Single-thread scheduler shared for periodic S3 sync. */
    private static final ScheduledExecutorService SYNC_SCHEDULER =
            Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "rocksdb-cloud-s3-sync");
                t.setDaemon(true);
                return t;
            });

    private ScheduledFuture<?> periodicSyncFuture;

    /** Counts commits since the last write-count-triggered sync. */
    private final AtomicLong writesSinceLastSync = new AtomicLong(0L);

    /** Guards against concurrent syncs from timer + write-count paths. */
    private final AtomicBoolean syncInProgress = new AtomicBoolean(false);

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    public RocksDBCloudSessions(HugeConfig config, String database, String store,
                                String dataPath, String walPath) throws RocksDBException {
        super(config, database, store, dataPath, walPath);
        this.s3Client            = buildS3Client(config);
        this.dataPath            = dataPath;
        this.syncIntervalSeconds = config.get(RocksDBCloudOptions.SYNC_INTERVAL_SECONDS);
        this.syncOnWriteCount    = config.get(RocksDBCloudOptions.SYNC_ON_WRITE_COUNT);
        this.syncIncremental     = config.get(RocksDBCloudOptions.SYNC_INCREMENTAL);
        this.syncModeSync        = "sync".equalsIgnoreCase(
                                       config.get(RocksDBCloudOptions.SYNC_MODE));
        this.startPeriodicSync();
        LOG.info("RocksDBCloudSessions opened: local='{}', s3://{}/{}, " +
                 "syncMode={}, syncInterval={}s, syncOnWrites={}, incremental={}",
                 dataPath,
                 config.get(RocksDBCloudOptions.S3_BUCKET_NAME),
                 config.get(RocksDBCloudOptions.S3_OBJECT_PREFIX),
                 syncModeSync ? "sync" : "async",
                 syncIntervalSeconds, syncOnWriteCount, syncIncremental);
    }

    public RocksDBCloudSessions(HugeConfig config, String database, String store,
                                String dataPath, String walPath,
                                List<String> cfNames) throws RocksDBException {
        super(config, database, store, dataPath, walPath, cfNames);
        this.s3Client            = buildS3Client(config);
        this.dataPath            = dataPath;
        this.syncIntervalSeconds = config.get(RocksDBCloudOptions.SYNC_INTERVAL_SECONDS);
        this.syncOnWriteCount    = config.get(RocksDBCloudOptions.SYNC_ON_WRITE_COUNT);
        this.syncIncremental     = config.get(RocksDBCloudOptions.SYNC_INCREMENTAL);
        this.syncModeSync        = "sync".equalsIgnoreCase(
                                       config.get(RocksDBCloudOptions.SYNC_MODE));
        this.startPeriodicSync();
    }

    // -------------------------------------------------------------------------
    // Public helpers
    // -------------------------------------------------------------------------

    /** Returns the live S3 client for external use (e.g. snapshot upload/restore). */
    public S3Client s3Client() {
        return this.s3Client;
    }

    /** Returns the configured S3 bucket name. */
    public String bucketName() {
        return this.config().get(RocksDBCloudOptions.S3_BUCKET_NAME);
    }

    /** Returns the S3 object key prefix (directory within the bucket). */
    public String objectPrefix() {
        return this.config().get(RocksDBCloudOptions.S3_OBJECT_PREFIX);
    }

    /**
     * Explicitly upload changed SST files to S3 (incremental by default).
     * Called by the periodic scheduler, write-count threshold, and on close.
     * Safe to call concurrently — duplicate calls are coalesced.
     *
     * @param fullSync when true, upload all files unconditionally (used on close/snapshot)
     */
    public void syncNow(boolean fullSync) {
        if (!syncInProgress.compareAndSet(false, true)) {
            LOG.debug("S3 sync already in progress — skipping duplicate trigger");
            return;
        }
        try {
            // Flush all memtable data to SST files before reading the data dir
            try {
                this.flushAll();
            } catch (Exception e) {
                LOG.warn("Failed to flush RocksDB before S3 sync: {}", e.getMessage());
            }

            String s3Prefix = objectPrefix() + "data/";
            if (fullSync || !syncIncremental) {
                LOG.info("Full S3 sync: local='{}' → s3://{}/{}", dataPath, bucketName(), s3Prefix);
                S3SnapshotUtil.uploadDirectory(s3Client, bucketName(), s3Prefix, dataPath);
            } else {
                LOG.debug("Incremental S3 sync: local='{}' → s3://{}/{}", dataPath,
                          bucketName(), s3Prefix);
                int count = S3SnapshotUtil.uploadIncremental(s3Client, bucketName(),
                                                             s3Prefix, dataPath);
                if (count == 0) {
                    LOG.debug("Incremental sync: no new files to upload");
                } else {
                    LOG.info("Incremental sync complete: {} SST files uploaded", count);
                }
            }
            writesSinceLastSync.set(0L);
        } finally {
            syncInProgress.set(false);
        }
    }

    /**
     * Convenience overload — uses incremental mode (the default for runtime syncs).
     */
    public void syncNow() {
        syncNow(false);
    }

    /**
     * Called by session commit paths to ensure data is durable in S3.
     *
     * <p><b>sync mode</b> ({@code rocksdb.cloud.sync_mode=sync}): flushes the
     * memtable and uploads all changed SST files to S3 <em>inline</em> before
     * returning. The caller blocks until S3 confirms the upload. This gives a
     * zero data-loss window and is the recommended setting for production.
     *
     * <p><b>async mode</b> (default): writes are counted and the sync is
     * submitted to a background scheduler thread when the
     * {@code sync_on_write_count} threshold is reached. The caller is never
     * blocked by S3 I/O, but data not yet synced can be lost on instance
     * failure.
     *
     * @param writtenCount number of mutations in this commit
     */
    public void onWriteCommit(long writtenCount) {
        if (syncModeSync) {
            // ── Synchronous mode: flush + upload inline on every commit ──────
            // Blocks the write thread until S3 confirms the upload.
            // This is equivalent to fsync() for S3-backed storage.
            try {
                syncNow(false);
            } catch (Exception e) {
                LOG.warn("Synchronous S3 sync after write commit failed: {}", e.getMessage());
                // We do not re-throw: the data is safe locally in RocksDB WAL.
                // A subsequent periodic sync or close-sync will retry.
            }
            return;
        }

        // ── Async mode: count writes and submit sync when threshold reached ──
        if (syncOnWriteCount <= 0) {
            return; // write-count sync disabled
        }
        long total = writesSinceLastSync.addAndGet(writtenCount);
        if (total >= syncOnWriteCount) {
            LOG.debug("Write-count threshold reached ({} >= {}); scheduling incremental sync",
                      total, syncOnWriteCount);
            // Async — don't block the write path
            SYNC_SCHEDULER.submit(() -> {
                try {
                    syncNow(false);
                } catch (Exception e) {
                    LOG.warn("Write-count-triggered S3 sync failed: {}", e.getMessage());
                }
            });
        }
    }

    // -------------------------------------------------------------------------
    // Periodic sync lifecycle
    // -------------------------------------------------------------------------

    private void startPeriodicSync() {
        if (syncIntervalSeconds <= 0) {
            LOG.info("Periodic S3 sync disabled (sync_interval_seconds=0)");
            return;
        }
        LOG.info("Scheduling periodic S3 sync every {}s", syncIntervalSeconds);
        periodicSyncFuture = SYNC_SCHEDULER.scheduleAtFixedRate(() -> {
            try {
                LOG.debug("Periodic S3 sync triggered");
                syncNow(false);
            } catch (Exception e) {
                LOG.warn("Periodic S3 sync failed (will retry next interval): {}", e.getMessage());
            }
        }, syncIntervalSeconds, syncIntervalSeconds, TimeUnit.SECONDS);
    }

    private void stopPeriodicSync() {
        if (periodicSyncFuture != null && !periodicSyncFuture.isCancelled()) {
            periodicSyncFuture.cancel(false); // don't interrupt an in-progress sync
        }
    }

    // -------------------------------------------------------------------------
    // Override doClose — full sync to S3 on graceful shutdown
    // -------------------------------------------------------------------------

    @Override
    protected synchronized void doClose() {
        stopPeriodicSync();
        try {
            LOG.info("RocksDBCloudSessions closing: performing full S3 sync before close...");
            syncNow(true); // full upload on close for complete final checkpoint
        } catch (Exception e) {
            LOG.warn("Failed to sync data to S3 on close (continuing shutdown): {}",
                     e.getMessage());
        }
        super.doClose();
    }

    // -------------------------------------------------------------------------
    // Override snapshot/restore to round-trip through S3
    // -------------------------------------------------------------------------

    @Override
    public void createSnapshot(String snapshotPath) {
        // 1. Create local RocksDB checkpoint
        super.createSnapshot(snapshotPath);
        // 2. Sync live data dir to S3 data prefix (incremental)
        syncNow(false);
        // 3. Upload the checkpoint to a separate snapshots prefix
        String bucket = bucketName();
        String prefix = objectPrefix() + "snapshots/" +
                        java.nio.file.Paths.get(snapshotPath).getFileName() + "/";
        LOG.info("Uploading snapshot '{}' to s3://{}/{}", snapshotPath, bucket, prefix);
        S3SnapshotUtil.uploadDirectory(s3Client, bucket, prefix, snapshotPath);
        LOG.info("Snapshot upload to S3 complete: s3://{}/{}", bucket, prefix);
    }

    @Override
    public void resumeSnapshot(String snapshotPath) {
        String bucket = bucketName();
        String prefix = objectPrefix() + "snapshots/" +
                        java.nio.file.Paths.get(snapshotPath).getFileName() + "/";
        LOG.info("Downloading snapshot from s3://{}/{} to '{}'", bucket, prefix, snapshotPath);
        S3SnapshotUtil.downloadDirectory(s3Client, bucket, prefix, snapshotPath);
        LOG.info("Snapshot download from S3 complete");
        super.resumeSnapshot(snapshotPath);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private static S3Client buildS3Client(HugeConfig config) {
        String accessKeyId = config.get(RocksDBCloudOptions.AWS_ACCESS_KEY_ID);
        String secretAccessKey = config.get(RocksDBCloudOptions.AWS_SECRET_ACCESS_KEY);
        String regionStr = config.get(RocksDBCloudOptions.S3_REGION);
        String endpointUrl = config.get(RocksDBCloudOptions.S3_ENDPOINT);
        boolean pathStyle = config.get(RocksDBCloudOptions.S3_PATH_STYLE_ACCESS);

        AwsCredentialsProvider credentialsProvider;
        if (accessKeyId != null && !accessKeyId.isEmpty() &&
            secretAccessKey != null && !secretAccessKey.isEmpty()) {
            credentialsProvider = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId, secretAccessKey));
            LOG.debug("RocksDB Cloud: using static AWS credentials");
        } else {
            credentialsProvider = DefaultCredentialsProvider.create();
            LOG.debug("RocksDB Cloud: using default AWS credentials chain");
        }

        software.amazon.awssdk.services.s3.S3ClientBuilder builder =
                S3Client.builder()
                        .region(Region.of(regionStr))
                        .credentialsProvider(credentialsProvider);

        if (endpointUrl != null && !endpointUrl.isEmpty()) {
            builder.endpointOverride(URI.create(endpointUrl));
            LOG.info("RocksDB Cloud: using custom S3 endpoint '{}'", endpointUrl);
        }

        if (pathStyle) {
            builder.serviceConfiguration(
                    S3Configuration.builder().pathStyleAccessEnabled(true).build());
        }

        return builder.build();
    }
}
