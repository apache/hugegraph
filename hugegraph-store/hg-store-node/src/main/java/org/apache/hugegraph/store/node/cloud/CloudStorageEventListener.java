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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.LiveSstFile;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.MetadataSnapshot;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.RocksdbChangedListener;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import org.apache.hugegraph.store.cloud.CloudStorageProviderFactory;

import lombok.extern.slf4j.Slf4j;

/**
 * {@link RocksdbChangedListener} that bridges RocksDB table-file lifecycle events
 * to the active {@link CloudStorageProvider}.
 *
 * <p>When cloud storage is enabled:
 * <ul>
 *   <li>{@link #onDBCreated} uploads any SST files that already exist in the DB directory
 *       (e.g. surviving from a previous run), triggers a non-blocking MemTable flush so
 *       WAL-recovered or recently-written data is written to SST files (completion is signalled
 *       event-driven via {@link #onTableFileCreated}), then mirrors metadata inline.</li>
 *   <li>{@link #onTableFileCreated} uploads newly created SST files and mirrors metadata inline.</li>
 *   <li>{@link #onTableFileDeleted} mirrors metadata first, then removes the superseded SST object.</li>
 * </ul>
 *
 * <h3>Remote key construction</h3>
 * The remote key is derived by stripping the {@code dataRoot} prefix from the absolute
 * local file path.  This keeps the object layout clean and independent of the container
 * filesystem layout:
 * <pre>
 *   dataRoot  = /hugegraph-store/storage
 *   filePath  = /hugegraph-store/storage/hgstore-metadata/000008.sst
 *   remoteKey = store-127.0.0.1_8501/hgstore-metadata/000008.sst
 *   (with path-prefix "hugegraph") → hugegraph/store-127.0.0.1_8501/hgstore-metadata/000008.sst
 * </pre>
 *
 * This listener is registered with {@link RocksDBFactory} during application startup
 * (see {@link org.apache.hugegraph.store.node.AppConfig}).
 */
@Slf4j
public class CloudStorageEventListener implements RocksdbChangedListener {

    /** Absolute, normalised path of the store's data root directory. */
    private final String dataRoot;

    /** Optional per-store namespace prefix prepended to every remote cloud key. */
    private final String storeScopePrefix;

    private static final long DEFAULT_READ_MISS_GUARD_WINDOW_MS = 3000L;

    private final boolean startupHydrationEnabled;
    private final long readMissGuardWindowMs;
    private final Map<String, Long> readMissAttemptTs;

    /**
     * Optional retry queue; when non-null, upload failures are submitted here instead
     * of just being logged. When null, failures are only logged (no retry).
     */
    private final CloudUploadRetryQueue retryQueue;

    /** Tracks which SST files are confirmed present in cloud (per-DB Roaring bitmap). */
    private final CloudSyncTracker syncTracker;

    /**
     * When {@code > 0}, {@link #onTableFileCreated} slows the RocksDB flush/compaction thread while
     * the number of not-yet-durable uploads (retry-queue in-flight + DLQ) exceeds this watermark,
     * so ingestion cannot outrun the cloud mirror. {@code 0} disables backpressure.
     */
    private final int backpressureHighWatermark;

    /** Upper bound on how long a single {@link #onTableFileCreated} call will block for backpressure. */
    private static final long BACKPRESSURE_MAX_WAIT_MS = 30_000L;
    private static final long BACKPRESSURE_POLL_MS = 50L;

    // -----------------------------------------------------------------------
    // Metadata (CURRENT/MANIFEST/OPTIONS[/WAL]) mirroring & consistent restore
    // -----------------------------------------------------------------------


    /**
     * When {@code true} ({@code wal-mode: wal}), the active WAL {@code *.log} segments are mirrored
     * alongside the metadata and replayed on restore. When {@code false} ({@code wal-mode: flush}),
     * no WAL is mirrored.
     */
    private final boolean walModeEnabled;

    /** Resolved DB directory -> logical DB name, so {@link #onCompacted} resolves path events. */
    private final Map<String, String> dbNameByDir = new ConcurrentHashMap<>();

    /**
     * Tracks which DBs are currently being truncated. While a DB is in this set,
     * metadata sync operations are skipped to allow the purge to complete cleanly
     * without new metadata files being re-uploaded.
     */
    private final Set<String> truncatingDbs = ConcurrentHashMap.newKeySet();

    /**
     * Tracks the timestamp of recent truncations (DB name -> truncation time in ms).
     * Used to suppress metadata syncs for a grace period after truncation, allowing
     * pending RocksDB background operations and callbacks to complete without
     * re-uploading metadata that was just purged.
     */
    private final Map<String, Long> truncationTimes = new ConcurrentHashMap<>();

    /**
     * Grace period (ms) after truncation during which metadata syncs are suppressed.
     * This allows pending RocksDB background callbacks to complete without re-uploading
     * metadata that was purged during truncation.
     */
    private static final long TRUNCATION_GRACE_PERIOD_MS = 5_000L;

    /**
     * Sentinel object key written to the DB prefix during database deletion.
     * {@link #preHydrateDbFiles} checks for this key and skips hydration when present, preventing
     * a newly-recreated DB from ingesting data that belonged to a previous deleted generation.
     */
    static final String DB_TOMBSTONE_FILE = "_DELETED";

    /**
     * @param dataRoot absolute path of the store's data directory
     *                 (value of {@code app.data-path}, resolved to an absolute path).
     */
    public CloudStorageEventListener(String dataRoot) {
        this(dataRoot, true, DEFAULT_READ_MISS_GUARD_WINDOW_MS, null);
    }

    public CloudStorageEventListener(String dataRoot,
                                     boolean startupHydrationEnabled) {
        this(dataRoot, startupHydrationEnabled, DEFAULT_READ_MISS_GUARD_WINDOW_MS, null);
    }

    /**
     * @param readMissGuardWindowMs guard window in ms for repeated read-miss hydration attempts
     *                              for the same db/table pair (cloud.storage.read-miss-guard-window-ms)
     */
    public CloudStorageEventListener(String dataRoot,
                                     boolean startupHydrationEnabled,
                                     long readMissGuardWindowMs) {
        this(dataRoot, startupHydrationEnabled, readMissGuardWindowMs, null);
    }

    /**
     * @param retryQueue optional {@link CloudUploadRetryQueue}; when non-null, upload failures
     *                   are retried asynchronously and eventually moved to the dead-letter queue.
     *                   Pass {@code null} to disable retries (failures are only logged).
     */
    public CloudStorageEventListener(String dataRoot,
                                     boolean startupHydrationEnabled,
                                     long readMissGuardWindowMs,
                                     CloudUploadRetryQueue retryQueue) {
        this(dataRoot, startupHydrationEnabled, readMissGuardWindowMs, retryQueue,
             new CloudSyncTracker(), 0);
    }

    /**
     * Full constructor.
     *
     * @param syncTracker               tracks SST files confirmed present in cloud; the delete guard
     *                                  uses it to avoid deleting a superseded object before its
     *                                  replacements are durable. Must be shared with the retry queue.
     * @param backpressureHighWatermark {@code > 0} to slow ingestion while the pending-upload backlog
     *                                  exceeds this value; {@code 0} disables backpressure.
     */
    public CloudStorageEventListener(String dataRoot,
                                     boolean startupHydrationEnabled,
                                     long readMissGuardWindowMs,
                                     CloudUploadRetryQueue retryQueue,
                                     CloudSyncTracker syncTracker,
                                     int backpressureHighWatermark) {
        this(dataRoot, startupHydrationEnabled, readMissGuardWindowMs, retryQueue, syncTracker,
             backpressureHighWatermark, false);
    }

    /**
     * Full constructor including metadata-mirroring parameters.
     *
     * @param walModeEnabled         {@code true} for {@code wal-mode: wal} (mirror + replay WAL);
     *                               {@code false} for {@code wal-mode: flush} (force flush, no WAL)
     */
    public CloudStorageEventListener(String dataRoot,
                                     boolean startupHydrationEnabled,
                                     long readMissGuardWindowMs,
                                     CloudUploadRetryQueue retryQueue,
                                     CloudSyncTracker syncTracker,
                                     int backpressureHighWatermark,
                                     boolean walModeEnabled) {
        this(dataRoot, startupHydrationEnabled, readMissGuardWindowMs, retryQueue, syncTracker,
             backpressureHighWatermark, walModeEnabled, null);
    }

    /**
     * Full constructor including metadata-mirroring and per-store key namespace parameters.
     *
     * @param storeScopePrefix optional per-store key prefix to isolate cloud objects across
     *                         distributed store nodes sharing the same bucket/path-prefix.
     */
    public CloudStorageEventListener(String dataRoot,
                                     boolean startupHydrationEnabled,
                                     long readMissGuardWindowMs,
                                     CloudUploadRetryQueue retryQueue,
                                     CloudSyncTracker syncTracker,
                                     int backpressureHighWatermark,
                                     boolean walModeEnabled,
                                     String storeScopePrefix) {
        String normalised = Paths.get(dataRoot).toAbsolutePath().normalize().toString();
        // Strip trailing separator so substring arithmetic is consistent.
        this.dataRoot = normalised.endsWith(File.separator)
                        ? normalised.substring(0, normalised.length() - 1)
                        : normalised;
        this.startupHydrationEnabled = startupHydrationEnabled;
        this.readMissGuardWindowMs = Math.max(0L, readMissGuardWindowMs);
        this.readMissAttemptTs = new ConcurrentHashMap<>();
        this.retryQueue = retryQueue;
        this.syncTracker = syncTracker != null ? syncTracker : new CloudSyncTracker();
        this.backpressureHighWatermark = Math.max(0, backpressureHighWatermark);
        this.walModeEnabled = walModeEnabled;
        this.storeScopePrefix = normaliseKeyPrefix(storeScopePrefix);
    }

    // -----------------------------------------------------------------------
    // RocksdbChangedListener
    // -----------------------------------------------------------------------

    @Override
    public void onDBOpening(String dbName, String dbPath) {
        if (!startupHydrationEnabled) {
            return;
        }
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            return;
        }
        preHydrateDbFiles(provider, dbName, dbPath);
    }

    /**
     * Called when a read returns null in RocksDB.
     *
     * <p>We restore only the SST files that RocksDB references as <em>live</em> in its manifest but
     * that are physically missing on local disk, downloading each back to its <em>exact original
     * path</em> so RocksDB finds it on the next access. This deliberately avoids
     * {@code ingestExternalFile}, which would (a) risk placing a file into the wrong column family
     * and (b) assign a fresh sequence number that can resurrect deleted keys. Restricting to the
     * live set also guarantees superseded / compacted-away objects are never resurrected.
     *
     * <p>Note: a genuine key-not-found also arrives here as {@code value == null}; in that case no
     * live file is missing and we return {@code false} without any cloud I/O.
     */
    @Override
    public boolean onReadMiss(RocksDBSession session, String table, byte[] key) {
        String dbName = session.getGraphName();
        if (!shouldAttemptReadMissHydration(dbName, table)) {
            return false;
        }
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            return false;
        }
        int restored = restoreMissingLiveFiles(provider, dbName,
                                               RocksDBFactory.getInstance().getLiveSstFiles(dbName));
        if (restored > 0) {
            log.info("Cloud read-miss hydration: restored {} missing live SST file(s) for db={}",
                     restored, dbName);
            return true;
        }
        return false;
    }

    /**
     * Downloads back any SST files that are live in RocksDB's manifest but missing on local disk,
     * writing each to its original path. Returns the number of files restored.
     *
     * <p>Package-private testable seam: caller supplies the live-file set.
     */
    int restoreMissingLiveFiles(CloudStorageProvider provider, String dbName,
                                List<LiveSstFile> liveFiles) {
        int restored = 0;
        for (LiveSstFile live : liveFiles) {
            Path localPath = Paths.get(live.getAbsolutePath());
            if (Files.exists(localPath)) {
                continue;
            }
            String remoteKey = toRelativeKey(live.getAbsolutePath());
            try {
                if (!provider.fileExists(remoteKey)) {
                    log.warn("Cloud read-miss: live file missing locally AND absent in cloud: "
                             + "db={}, key={}", dbName, remoteKey);
                    continue;
                }
                Files.createDirectories(localPath.getParent());
                // Download to a temp file then atomically move into place so a crash mid-download
                // never leaves RocksDB reading a truncated SST at the expected path.
                Path tmp = localPath.resolveSibling(localPath.getFileName() + ".hydrate");
                provider.downloadFile(remoteKey, tmp.toString());
                Files.move(tmp, localPath, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                syncTracker.markConfirmed(dbName, live.getAbsolutePath());
                restored++;
            } catch (IOException e) {
                log.warn("Cloud read-miss restore failed: db={}, key={}, reason={}",
                         dbName, remoteKey, e.getMessage());
            }
        }
        return restored;
    }

    /**
     * Called when a new RocksDB instance is opened for the first time.
     *
     * <p>Uploads any SST files that already exist in {@code dbPath} (e.g. from a previous run)
     * and then triggers a MemTable flush so that WAL-recovered data is also written to
     * SST files and eventually forwarded here via {@link #onTableFileCreated}.
     *
     * @param dbName logical name of the graph / partition
     * @param dbPath absolute path of the RocksDB directory
     */
    @Override
    public void onDBCreated(String dbName, String dbPath) {
        recordDb(dbName, dbPath);
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            return;
        }
        uploadExistingSstFiles(provider, dbName, dbPath);
        flushDb(dbName);
        // Mirror metadata immediately after initial upload/flush to keep cloud state recoverable.
        syncMetadataSnapshotInline(provider, dbName);
    }

    /**
     * Called just before the local RocksDB directory is removed. Writes a small tombstone object
     * ({@value #DB_TOMBSTONE_FILE}) to the DB's remote prefix so that any subsequent
     * {@link #preHydrateDbFiles} call for the same path will detect the deleted generation and
     * skip hydration rather than re-ingesting stale objects.
     *
     * <p>This callback fires while the session is still in a pending-destroy list (refcount may
     * be non-zero). The tombstone write is best-effort: a failure is logged but does not block
     * the deletion. The cloud purge in {@link #onDBDeleted} provides a second line of defence.
     *
     * @param dbName  logical graph/partition name
     * @param dbPath  absolute path of the RocksDB directory being destroyed
     */
    @Override
    public void onDBDeleteBegin(String dbName, String dbPath) {
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            return;
        }
        String tombstoneKey = dbPrefix(dbPath) + "/" + DB_TOMBSTONE_FILE;
        Path tmp = null;
        try {
            tmp = Files.createTempFile("hgstore-tombstone-", ".tmp");
            Files.write(tmp, "deleted".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            provider.uploadFile(tmp.toString(), tombstoneKey);
            log.info("Cloud DB tombstone written: db={}, key={}", dbName, tombstoneKey);
        } catch (Exception e) {
            log.warn("Cloud DB tombstone write failed (onDBDeleted will still purge): "
                     + "db={}, key={}, reason={}", dbName, tombstoneKey, e.getMessage());
        } finally {
            if (tmp != null) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignore) {
                    // best-effort temp-file cleanup
                }
            }
        }
    }

    /**
     * Called after the local RocksDB directory has been physically removed. Purges all cloud
     * objects under the DB prefix (SSTs, metadata, tombstone) so a future creation at the same
     * path starts with a clean remote state. Also clears all in-memory state for this DB.
     *
     * <p>The purge is best-effort: individual delete failures are logged at DEBUG level and do
     * not throw. Any objects that survive the purge are neutralised by the tombstone check in
     * {@link #preHydrateDbFiles}: the next open will find the tombstone (or an empty prefix if
     * the purge was complete), skip hydration, and clean up any leftovers.
     *
     * @param dbName  logical graph/partition name
     * @param dbPath  absolute path of the now-deleted RocksDB directory
     */
    @Override
    public void onDBDeleted(String dbName, String dbPath) {
        // Clear in-memory tracking so no stale state bleeds into a recreated DB.
        syncTracker.clearDb(dbName);
        readMissAttemptTs.entrySet().removeIf(e -> e.getKey().startsWith(dbName + "::"));
        dbNameByDir.values().removeIf(dbName::equals);

        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            return;
        }
        purgeRemotePrefix(provider, dbName, dbPrefix(dbPath));
    }

    /**
     * Called after a RocksDB has been truncated (all data cleared but directory preserved).
     * Purges all cloud objects under the DB prefix (SSTs, metadata) so the remote state matches
     * the now-empty local state. Also clears all in-memory sync tracking for this DB.
     *
     * <p>This is triggered by graph.clear() operations to ensure cloud storage is cleaned up
     * when the graph data is cleared.
     *
     * @param dbName  logical graph/partition name
     * @param dbPath  absolute path of the RocksDB directory
     */
    @Override
    public void onDBTruncateBegin(String dbName, String dbPath) {
        truncatingDbs.add(dbName);
        truncationTimes.put(dbName, System.currentTimeMillis());
        syncTracker.clearDb(dbName);
        readMissAttemptTs.entrySet().removeIf(e -> e.getKey().startsWith(dbName + "::"));
    }

    @Override
    public void onDBTruncated(String dbName, String dbPath) {
        truncatingDbs.add(dbName);
        try {
            CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
            if (provider == null) {
                return;
            }
            purgeRemotePrefix(provider, dbName, dbPrefix(dbPath));
        } finally {
            truncationTimes.put(dbName, System.currentTimeMillis());
            truncatingDbs.remove(dbName);
        }
    }

    /**
     * Checks if a DB is within the grace period after truncation, during which
     * metadata syncs should be suppressed to prevent re-uploading purged data.
     */
    private boolean isInTruncationGracePeriod(String dbName) {
        Long truncationTime = truncationTimes.get(dbName);
        if (truncationTime == null) {
            return false;
        }
        long elapsed = System.currentTimeMillis() - truncationTime;
        if (elapsed > TRUNCATION_GRACE_PERIOD_MS) {
            // Grace period expired, remove the record
            truncationTimes.remove(dbName);
            return false;
        }
        return true;
    }

    /**
     * Deletes every remote object under {@code prefix} using an optimized prefix-level delete
     * if available, falling back to individual file deletion if necessary.
     * This is called during DB destruction to prevent a recreated DB from hydrating stale data.
     */
    private void purgeRemotePrefix(CloudStorageProvider provider, String dbName, String prefix) {
        String normalizedPrefix = prefix.endsWith("/") ? prefix : prefix + "/";
        try {
            int deleted = provider.deletePrefix(normalizedPrefix);
            if (deleted > 0) {
                log.info("Cloud DB purge completed: db={}, prefix={}, deleted={}",
                         dbName, prefix, deleted);
            }
        } catch (IOException e) {
            log.warn("Cloud DB purge failed for db={}, prefix={}: {}",
                     dbName, prefix, e.getMessage());
        }
    }

    /**
     * Uploads the newly created SST file to the active cloud storage provider.
     *
     * @param dbName   RocksDB instance name (partition id)
     * @param cfName   column-family name
     * @param filePath absolute local path of the new SST file
     * @param fileSize file size in bytes (informational)
     */
    @Override
    public void onTableFileCreated(String dbName, String cfName,
                                   String filePath, long fileSize) {
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            return;
        }
        recordDb(dbName, parentDir(filePath));
        CloudStorageMetrics.registerDatabaseMetrics(dbName);
        String remoteKey = toRelativeKey(filePath);
        long startTimeMs = System.currentTimeMillis();
        try {
            provider.uploadFile(filePath, remoteKey);
            syncTracker.markConfirmed(dbName, filePath);
            long syncLatencyMs = System.currentTimeMillis() - startTimeMs;
            CloudStorageMetrics.recordSyncLatency(dbName, syncLatencyMs);
             // Skip metadata sync if DB is being truncated or in grace period after truncation
             // to allow purge to complete cleanly without metadata files being re-uploaded.
             if (!truncatingDbs.contains(dbName) && !isInTruncationGracePeriod(dbName)) {
                 syncMetadataSnapshotInline(provider, dbName);
             }
            log.debug("Cloud upload success: db={}, cf={}, path={}, size={}, latencyMs={}",
                      dbName, cfName, filePath, fileSize, syncLatencyMs);
        } catch (Exception e) {
            // ...existing code...
            String errorType = e.getClass().getSimpleName();
            CloudStorageMetrics.recordUploadFailure(dbName, cfName, errorType);
            log.error("Cloud upload failed (will retry on next compaction): "
                      + "db={}, cf={}, path={}, error={}", dbName, cfName, filePath, e.getMessage());
            if (retryQueue != null) {
                retryQueue.submit(dbName, cfName, filePath, remoteKey, e);
            }
        }
        // Apply backpressure AFTER handling this file so the flush/compaction thread slows down
        // while the cloud mirror is behind, preventing ingestion from outrunning durability.
        applyBackpressure(dbName);
    }

    /**
     * Blocks the calling (RocksDB flush/compaction) thread while the pending-upload backlog exceeds
     * {@link #backpressureHighWatermark}, up to {@link #BACKPRESSURE_MAX_WAIT_MS}. This is the
     * durability-tier backpressure: it keeps at-risk local-only data bounded.
     */
    private void applyBackpressure(String dbName) {
        if (backpressureHighWatermark <= 0 || retryQueue == null) {
            return;
        }
        long waited = 0L;
        boolean logged = false;
        while (pendingUploadBacklog() > backpressureHighWatermark
               && waited < BACKPRESSURE_MAX_WAIT_MS) {
            if (!logged) {
                log.warn("Cloud upload backpressure: db={}, backlog={} > watermark={}, "
                         + "slowing ingestion", dbName, pendingUploadBacklog(),
                         backpressureHighWatermark);
                logged = true;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(BACKPRESSURE_POLL_MS));
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            waited += BACKPRESSURE_POLL_MS;
        }
        if (logged) {
            log.info("Cloud upload backpressure released: db={}, backlog={}, waitedMs={}",
                     dbName, pendingUploadBacklog(), waited);
        }
    }

    private int pendingUploadBacklog() {
        if (retryQueue == null) {
            return 0;
        }
        return retryQueue.getInFlightCount() + retryQueue.getDlqSize();
    }

    /**
     * Removes the deleted SST file from the active cloud storage provider.
     *
     * @param dbName   RocksDB instance name (partition id)
     * @param cfName   column-family name
     * @param filePath absolute local path of the deleted SST file
     */
    @Override
    public void onTableFileDeleted(String dbName, String cfName, String filePath) {
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            return;
        }
        // Skip file deletion during truncation or grace period; the entire DB prefix will be purged anyway
        if (truncatingDbs.contains(dbName) || isInTruncationGracePeriod(dbName)) {
            log.debug("Skipping delete during truncation: db={}, path={}", dbName, filePath);
            return;
        }
        // DATA-LOSS GUARD: never delete a superseded cloud object until every SST file currently
        // live in this DB is confirmed present in cloud.
        if (!ensureLiveSetUploaded(provider, dbName)) {
            log.warn("Delete skipped (live set not fully durable in cloud): db={}, filePath={}",
                     dbName, filePath);
            return;
        }

        // MANIFEST-BEFORE-DELETE INVARIANT: publish an updated MANIFEST+CURRENT that reflects
        // the post-compaction live set before removing the old SST from cloud.  Without this, a
        // crash between the SST delete and the next metadata-sync attempt leaves a cloud
        // MANIFEST that references an object that no longer exists, making recovery impossible.
        //
        // We use syncMetadataSnapshotInline (no MemTable flush) because:
        //   (a) this callback fires on a RocksDB compaction thread — calling flushSession(wait=true)
        //       here would deadlock against RocksDB background execution;
        //   (b) a flush is not needed: by the time onTableFileDeleted fires the compaction is
        //       complete, new SSTs are already on disk and uploaded, and the MANIFEST already
        //       reflects the post-compaction state.
        if (!syncMetadataSnapshotInline(provider, dbName)) {
            log.warn("Delete skipped (metadata sync failed, MANIFEST not yet updated): "
                     + "db={}, filePath={}", dbName, filePath);
            return;
        }

        String remoteKey = toRelativeKey(filePath);
        try {
            provider.deleteFile(remoteKey);
            syncTracker.clearConfirmed(dbName, filePath);
            log.debug("Cloud delete success: db={}, cf={}, path={}", dbName, cfName, filePath);
        } catch (Exception e) {
            // Non-fatal: log and continue.
            log.error("Cloud delete failed: db={}, cf={}, path={}", dbName, cfName, filePath, e);
        }
    }

    /**
     * Ensures every live SST file of {@code dbName} is confirmed present in cloud, uploading any
     * that are not yet confirmed. Returns {@code true} only when the entire live set is durable.
     *
     * <h4>Fast path (common case)</h4>
     * {@link #preHydrateDbFiles} seeds {@link #syncTracker} from the cloud listing on startup,
     * and {@link #onTableFileCreated} marks every successful upload confirmed. By the time
     * {@link #onTableFileDeleted} fires the entire live set is normally already confirmed, so
     * {@link CloudSyncTracker#allConfirmed} returns {@code true} after one lock acquisition with
     * zero cloud I/O — regardless of live-set size.
     *
     * <h4>Why we must check the whole live set, not just the deleted {@code filePath}</h4>
     * {@code filePath} is the <em>old</em> compaction input being removed from cloud. Its bit
     * being set in the bitmap only confirms it was uploaded previously — it says nothing about
     * whether the <em>compaction outputs</em> (the replacement files, which form the new live
     * set) are also in cloud. Deleting the input before the outputs are durable would leave
     * data irretrievably lost on a node crash.
     *
     * <h4>Slow path (rare)</h4>
     * Entered only when {@code allConfirmed} returns {@code false} (e.g. a previous upload
     * failed). Files present locally are uploaded directly without a {@code fileExists} probe
     * (idempotent PUT). Files absent locally and not confirmed are treated as not-yet-durable
     * and the delete is held.
     */
    private boolean ensureLiveSetUploaded(CloudStorageProvider provider, String dbName) {
        return ensureLiveSetUploaded(provider, dbName,
                                     RocksDBFactory.getInstance().getLiveSstFiles(dbName));
    }

    /** Testable seam: caller supplies the live-file set instead of reading the RocksDB singleton. */
    boolean ensureLiveSetUploaded(CloudStorageProvider provider, String dbName,
                                  List<LiveSstFile> liveFiles) {
        // Fast path: single lock acquisition, zero cloud I/O — the common case.
        if (syncTracker.allConfirmed(dbName, liveFiles)) {
            return true;
        }

        // Slow path: one or more live files not yet confirmed — find and upload them.
        log.info("Checking live set durability: db={}, liveFileCount={}", dbName, liveFiles.size());

        java.util.List<String> unconfirmedFiles = new java.util.ArrayList<>();
        boolean allDurable = true;

        for (LiveSstFile live : liveFiles) {
            String localPath = live.getAbsolutePath();
            if (syncTracker.isConfirmed(dbName, localPath)) {
                continue;
            }
            unconfirmedFiles.add(localPath);
            Path localFile = Paths.get(localPath);
            String remoteKey = toRelativeKey(localPath);
            if (!Files.exists(localFile)) {
                // File absent locally and absent from bitmap. Startup hydration (preHydrateDbFiles)
                // seeds the bitmap from the cloud listing, so reaching here means this file is
                // genuinely not durable in cloud — hold the delete.
                allDurable = false;
                log.warn("Delete guard: live file absent locally and not confirmed in cloud: "
                         + "db={}, filePath={}", dbName, localPath);
                continue;
            }
            try {
                // Upload directly — no fileExists probe (idempotent PUT is cheaper than a probe).
                provider.uploadFile(localPath, remoteKey);
                syncTracker.markConfirmed(dbName, localPath);
            } catch (Exception e) {
                allDurable = false;
                log.warn("Delete guard: failed to upload live file: db={}, filePath={}, error={}",
                         dbName, localPath, e.getMessage());
                if (retryQueue != null) {
                    retryQueue.submit(dbName, live.getCfName(), localPath, remoteKey, e);
                }
            }
        }

        if (!unconfirmedFiles.isEmpty()) {
            CloudStorageMetrics.incrementDeleteGuardReupload(dbName);
            if (allDurable) {
                log.info("Re-uploaded {} previously unconfirmed live files (delete proceeding): "
                         + "db={}, files={}", unconfirmedFiles.size(), dbName, unconfirmedFiles);
            } else {
                log.warn("Re-upload attempted for {} unconfirmed live files (some still not durable, "
                         + "delete held): db={}, files={}", unconfirmedFiles.size(), dbName,
                         unconfirmedFiles);
            }
        }

        return allDurable;
    }

    /**
     * A compaction changed this DB's live SST set; mirror metadata immediately (event-driven).
     *
     * <p>Note: RocksDB delivers the DB <em>directory path</em> here (from {@code db.getName()}),
     * not the logical name, so we resolve it against {@link #dbNameByDir}.
     */
    @Override
    public void onCompacted(String dbNameOrPath) {
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            return;
        }
        String normalised = Paths.get(dbNameOrPath).toAbsolutePath().normalize().toString();
        String dbName = dbNameByDir.getOrDefault(normalised, dbNameOrPath);
        // Skip metadata sync during truncation or grace period to allow purge to complete cleanly
        if (!truncatingDbs.contains(dbName) && !isInTruncationGracePeriod(dbName)) {
            syncMetadataSnapshotInline(provider, dbName);
        }
    }

    // -----------------------------------------------------------------------
    // Metadata mirroring
    // -----------------------------------------------------------------------

    /** Records the resolved DB directory for a logical DB name (idempotent). */
    private void recordDb(String dbName, String dbDir) {
        if (dbName == null || dbDir == null) {
            return;
        }
        String normalised = Paths.get(dbDir).toAbsolutePath().normalize().toString();
        dbNameByDir.put(normalised, dbName);
    }

    /** Absolute, normalised parent directory of a file path (the DB dir of an SST). */
    private String parentDir(String filePath) {
        Path parent = Paths.get(filePath).toAbsolutePath().normalize().getParent();
        return parent == null ? null : parent.toString();
    }


    /**
     * Captures a consistent metadata snapshot and mirrors it to cloud without first flushing the
     * MemTable. Safe to call from any thread, including a RocksDB event/compaction callback.
     *
     * <p>By the time a compaction event fires, the MANIFEST already reflects the post-compaction
     * live SST set — a checkpoint captures that consistent state without needing a flush. In
     * {@code wal} mode the WAL tail is included in the checkpoint, so un-flushed data is still
     * recoverable. In {@code flush} mode un-flushed MemTable data written since the last sync is
     * not captured, but that is acceptable here because the goal is to publish a MANIFEST that
     * does not reference any cloud object that is about to be deleted.
     *
     * <p>Package-private for testability.
     */
    boolean syncMetadataSnapshotInline(CloudStorageProvider provider, String dbName) {
        MetadataSnapshot snapshot = RocksDBFactory.getInstance().captureMetadataSnapshot(dbName);
        if (snapshot == null) {
            return false;
        }
        try {
            return uploadMetadataSnapshot(provider, dbName, snapshot);
        } finally {
            snapshot.cleanup();
        }
    }

    /**
     * Uploads a captured metadata snapshot to cloud in strict consistency order:
     * <ol>
     *   <li>every SST the captured manifest references (from the checkpoint's hard-links);</li>
     *   <li>the mirrored WAL {@code *.log} tail ({@code wal} mode only);</li>
     *   <li>the {@code OPTIONS-*} and {@code MANIFEST-<n>} blobs;</li>
     *   <li><b>last</b>, {@code CURRENT} — the pointer — so a restore that fetches {@code CURRENT}
     *       never sees it referencing a manifest whose SSTs are not all in cloud;</li>
     *   <li>only then prune superseded remote {@code MANIFEST-*}/{@code OPTIONS-*}/{@code *.log}.</li>
     * </ol>
     * If any referenced SST cannot be made durable, the method aborts before publishing
     * {@code MANIFEST}/{@code CURRENT} and returns {@code false}, leaving the previous durable
     * generation intact.
     */
    boolean uploadMetadataSnapshot(CloudStorageProvider provider, String dbName,
                                   MetadataSnapshot snapshot) {
        String dbDir = snapshot.getDbDir();
        String tempDir = snapshot.getTempDir();

        // (1) Confirm every manifest-referenced SST is present in cloud.
        if (!ensureSnapshotSstsUploaded(provider, dbName, snapshot)) {
            log.warn("Cloud metadata-sync: SST set not fully durable, holding metadata publish "
                     + "for db={}", dbName);
            return false;
        }

        try {
            // (2) WAL tail (wal mode only).
            if (walModeEnabled) {
                for (String walName : snapshot.getWalFileNames()) {
                    uploadMetaFile(provider, dbDir, tempDir, walName);
                }
            }
            // (3) OPTIONS-* then MANIFEST-<n>.
            for (String optionsName : snapshot.getOptionsFileNames()) {
                uploadMetaFile(provider, dbDir, tempDir, optionsName);
            }
            if (snapshot.getManifestFileName() != null) {
                uploadMetaFile(provider, dbDir, tempDir, snapshot.getManifestFileName());
            }
            // (4) CURRENT last — the atomic pointer publish.
            if (snapshot.getCurrentFileName() != null) {
                uploadMetaFile(provider, dbDir, tempDir, snapshot.getCurrentFileName());
            }
        } catch (IOException e) {
            log.warn("Cloud metadata-sync: failed to publish metadata for db={}: {}",
                     dbName, e.getMessage());
            return false;
        }

        // (5) Prune superseded remote metadata now that the new CURRENT is durable.
        pruneRemoteMetadata(provider, dbDir, snapshot);
        log.debug("Cloud metadata-sync published: db={}, manifest={}, options={}, wal={}",
                  dbName, snapshot.getManifestFileName(), snapshot.getOptionsFileNames().size(),
                  walModeEnabled ? snapshot.getWalFileNames().size() : 0);
        return true;
    }

    /**
     * Ensures every SST the captured manifest references (the checkpoint's hard-linked {@code *.sst}
     * set) is present in cloud, uploading from the hard-link when not. The hard-link pins the
     * content even if compaction removed the original, so this cannot race a concurrent delete.
     */
    private boolean ensureSnapshotSstsUploaded(CloudStorageProvider provider, String dbName,
                                               MetadataSnapshot snapshot) {
        String dbDir = snapshot.getDbDir();
        String tempDir = snapshot.getTempDir();
        boolean allDurable = true;
        for (String sstName : snapshot.getSstFileNames()) {
            String realPath = joinPath(dbDir, sstName);
            String remoteKey = toRelativeKey(realPath);
            if (syncTracker.isConfirmed(dbName, realPath)) {
                continue;
            }
            try {
                if (provider.fileExists(remoteKey)) {
                    syncTracker.markConfirmed(dbName, realPath);
                    continue;
                }
                provider.uploadFile(joinPath(tempDir, sstName), remoteKey);
                syncTracker.markConfirmed(dbName, realPath);
            } catch (Exception e) {
                allDurable = false;
                log.warn("Cloud metadata-sync: failed to confirm SST db={}, key={}, reason={}",
                         dbName, remoteKey, e.getMessage());
                if (retryQueue != null && Files.exists(Paths.get(realPath))) {
                    retryQueue.submit(dbName, null, realPath, remoteKey, e);
                }
            }
        }
        return allDurable;
    }

    /** Uploads one metadata file from the checkpoint temp dir to its real-path-derived remote key. */
    private void uploadMetaFile(CloudStorageProvider provider, String dbDir, String tempDir,
                                String fileName) throws IOException {
        provider.uploadFile(joinPath(tempDir, fileName), toRelativeKey(joinPath(dbDir, fileName)));
    }

    /**
     * Deletes remote {@code MANIFEST-*}/{@code OPTIONS-*} (and, always, {@code *.log}) objects for
     * this DB that are not part of the just-published snapshot, bounding remote metadata growth.
     * {@code CURRENT} and {@code *.sst} are never pruned here (SST lifecycle is the delete guard's).
     */
    private void pruneRemoteMetadata(CloudStorageProvider provider, String dbDir,
                                     MetadataSnapshot snapshot) {
        Set<String> keep = new HashSet<>();
        if (snapshot.getManifestFileName() != null) {
            keep.add(snapshot.getManifestFileName());
        }
        keep.addAll(snapshot.getOptionsFileNames());
        if (walModeEnabled) {
            keep.addAll(snapshot.getWalFileNames());
        }
        String prefix = dbPrefix(dbDir);
        List<String> remoteKeys;
        try {
            remoteKeys = provider.listFiles(prefix.endsWith("/") ? prefix : prefix + "/");
        } catch (IOException e) {
            log.debug("Cloud metadata-sync prune: list failed for prefix={}: {}",
                      prefix, e.getMessage());
            return;
        }
        for (String remoteKey : remoteKeys) {
            int slash = remoteKey.lastIndexOf('/');
            String base = slash >= 0 ? remoteKey.substring(slash + 1) : remoteKey;
            boolean isSupersededMeta = (base.startsWith("MANIFEST-") || base.startsWith("OPTIONS-")
                                        || base.endsWith(".log")) && !keep.contains(base);
            if (!isSupersededMeta) {
                continue;
            }
            try {
                provider.deleteFile(remoteKey);
                log.debug("Cloud metadata-sync prune: deleted superseded {}", remoteKey);
            } catch (IOException e) {
                log.debug("Cloud metadata-sync prune: delete failed for {}: {}",
                          remoteKey, e.getMessage());
            }
        }
    }

    /** Joins a directory and a file name with the platform separator. */
    private static String joinPath(String dir, String name) {
        return dir.endsWith(File.separator) || dir.endsWith("/")
               ? dir + name
               : dir + File.separator + name;
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /**
     * Converts an absolute local file path to a remote key by stripping the data-root prefix.
     *
     * <pre>
     *   dataRoot = /hugegraph-store/storage
     *   filePath = /hugegraph-store/storage/hgstore-metadata/000008.sst
     *   result   = hgstore-metadata/000008.sst
     * </pre>
     *
     * If {@code filePath} does not start with {@code dataRoot} the leading slash is simply
     * stripped so the key is still valid (though possibly not ideallyformatted).
     */
    String toRelativeKey(String filePath) {
        String relative;
        if (filePath.startsWith(dataRoot)) {
            String rel = filePath.substring(dataRoot.length());
            // Strip leading separator produced by the substring.
            relative = rel.startsWith("/") || rel.startsWith(File.separator)
                       ? rel.substring(1)
                       : rel;
            return withStoreScope(relative);
        }
        // Fallback: strip any leading slash so the key does not start with '/'.
        relative = filePath.startsWith("/") ? filePath.substring(1) : filePath;
        return withStoreScope(relative);
    }

    /**
     * Walks {@code dbPath} and uploads every {@code *.sst} file that is not already
     * present in cloud storage.  This handles restarts where SST files from a previous
     * run were never uploaded (e.g. cloud storage was enabled after the last shutdown).
     */
    private void uploadExistingSstFiles(CloudStorageProvider provider, String dbName,
                                        String dbPath) {
        Path root = Paths.get(dbPath);
        if (!root.toFile().isDirectory()) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(p -> p.toString().endsWith(".sst"))
                 .forEach(p -> {
                     String localPath = p.toString();
                     String remoteKey = toRelativeKey(localPath);
                     try {
                         if (!provider.fileExists(remoteKey)) {
                             provider.uploadFile(localPath, remoteKey);
                             log.info("Cloud initial-upload: {} -> {}", localPath, remoteKey);
                         }
                     } catch (IOException e) {
                         throw new IllegalStateException(
                                 String.format("Cloud initial-upload failed for db=%s path=%s",
                                               dbName, localPath), e);
                     }
                 });
        } catch (IOException e) {
            throw new IllegalStateException(
                    String.format("Cloud initial-upload scan failed for db=%s dbPath=%s",
                                  dbName, dbPath), e);
        }
    }

    private void preHydrateDbFiles(CloudStorageProvider provider, String dbName, String dbPath) {
        Path root = Paths.get(dbPath);
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new IllegalStateException(
                    String.format("Cloud pre-hydration mkdir failed for db=%s path=%s",
                                  dbName, dbPath), e);
        }

        CloudStorageMetrics.registerDatabaseMetrics(dbName);

        String prefix = dbPrefix(dbPath);

        // STALE-DATA GUARD: if the remote prefix carries a _DELETED tombstone, the previous
        // generation of this DB was destroyed but cloud objects were not fully removed. Hydrating
        // them would silently resurrect deleted data in the new DB. Instead, skip hydration,
        // trigger a best-effort remote purge so the prefix is clean, and start fresh.
        String tombstoneKey = prefix + "/" + DB_TOMBSTONE_FILE;
        try {
            if (provider.fileExists(tombstoneKey)) {
                log.warn("Cloud pre-hydration skipped: tombstone found for db={} — previous "
                         + "generation was deleted. Purging stale remote objects.", dbName);
                purgeRemotePrefix(provider, dbName, prefix);
                return;
            }
        } catch (IOException e) {
            // Tombstone check itself failed — cannot safely determine generation; skip hydration.
            log.warn("Cloud pre-hydration: tombstone check failed for db={}, skipping to avoid "
                     + "stale-data risk: {}", dbName, e.getMessage());
            return;
        }

        List<String> remoteFiles = listRemoteKeys(provider, prefix);
        if (remoteFiles.isEmpty()) {
            log.debug("Cloud pre-hydration skipped: no remote files for db={} prefix={}",
                      dbName, prefix);
            return;
        }

        int downloaded = 0;
        for (String remoteKey : remoteFiles) {
            Path localPath = resolveLocalPath(remoteKey);
            if (Files.exists(localPath)) {
                continue;
            }
            try {
                Files.createDirectories(localPath.getParent());
                provider.downloadFile(remoteKey, localPath.toString());
                downloaded++;
            } catch (IOException e) {
                throw new IllegalStateException(
                        String.format("Cloud pre-hydration failed for db=%s key=%s",
                                      dbName, remoteKey), e);
            }
        }
        if (downloaded > 0) {
            log.info("Pre-hydration completed: db={}, downloadedFiles={}", dbName, downloaded);
        }

        int confirmed = 0;
        for (String remoteKey : remoteFiles) {
            if (remoteKey.endsWith(".sst")) {
                syncTracker.markConfirmed(dbName, resolveLocalPath(remoteKey).toString());
                confirmed++;
            }
        }
        if (confirmed > 0) {
            log.info("Seeded sync-tracker bitmap with {} confirmed files from remote listing: "
                     + "db={}", confirmed, dbName);
        }

        verifyMetadataConsistency(dbName, root);
    }

    /**
     * Consistent-restore guard: if a {@code CURRENT} pointer was mirrored, the {@code MANIFEST-<n>}
     * it references must be present locally after pre-hydration. Opening RocksDB with a
     * {@code CURRENT} pointing at a missing manifest would silently yield an empty / partial DB, so
     * we fail loudly instead. (A manifest-referenced <em>SST</em> that is absent is caught loudly by
     * RocksDB's own open, which refuses to start on a missing referenced file.)
     */
    private void verifyMetadataConsistency(String dbName, Path dbRoot) {
        Path current = dbRoot.resolve("CURRENT");
        if (!Files.exists(current)) {
            // No mirrored metadata (or a genuinely empty prefix) — nothing to verify.
            return;
        }
        String manifestName;
        try {
            manifestName = Files.readString(current).trim();
        } catch (IOException e) {
            throw new IllegalStateException(
                    String.format("Cloud restore: unreadable CURRENT for db=%s", dbName), e);
        }
        if (manifestName.isEmpty() || !Files.exists(dbRoot.resolve(manifestName))) {
            throw new IllegalStateException(String.format(
                    "Cloud restore inconsistent for db=%s: CURRENT references manifest '%s' which is "
                    + "absent locally and in cloud. Refusing to open a partial DB.",
                    dbName, manifestName));
        }
    }

    private List<String> listRemoteKeys(CloudStorageProvider provider, String prefix) {
        try {
            return provider.listFiles(prefix.endsWith("/") ? prefix : prefix + "/");
        } catch (IOException e) {
            throw new IllegalStateException(
                    String.format("Cloud list failed for prefix=%s", prefix), e);
        }
    }

    private String dbPrefix(String dbPath) {
        String relative = toRelativeKey(dbPath);
        return relative.endsWith("/") ? relative.substring(0, relative.length() - 1) : relative;
    }

    private Path resolveLocalPath(String remoteKey) {
        String relative = stripStoreScope(remoteKey);
        Path root = Paths.get(this.dataRoot);
        Path local = root.resolve(relative).normalize();
        if (!local.startsWith(root)) {
            throw new IllegalArgumentException("Invalid remote key outside data root: " + remoteKey);
        }
        return local;
    }

    private String withStoreScope(String relativeKey) {
        if (storeScopePrefix.isEmpty()) {
            return relativeKey;
        }
        if (relativeKey == null || relativeKey.isEmpty()) {
            return storeScopePrefix;
        }
        return storeScopePrefix + "/" + relativeKey;
    }

    private String stripStoreScope(String remoteKey) {
        if (storeScopePrefix.isEmpty()) {
            return remoteKey;
        }
        String scopedPrefix = storeScopePrefix + "/";
        if (remoteKey.startsWith(scopedPrefix)) {
            return remoteKey.substring(scopedPrefix.length());
        }
        if (remoteKey.equals(storeScopePrefix)) {
            return "";
        }
        throw new IllegalArgumentException(String.format(
                "Remote key '%s' does not belong to store scope '%s'", remoteKey, storeScopePrefix));
    }

    private static String normaliseKeyPrefix(String prefix) {
        if (prefix == null) {
            return "";
        }
        String trimmed = prefix.trim();
        if (trimmed.isEmpty()) {
            return "";
        }
        String normalized = trimmed.replace('\\', '/');
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    /**
     * Triggers a non-blocking MemTable flush for the named DB via {@link RocksDBFactory}.
     * The call returns immediately (fire-and-forget); when RocksDB completes the flush it
     * fires {@link #onTableFileCreated} via its event-listener mechanism, which then uploads
     * the resulting SST file to cloud storage. The overall flow is event-driven, not async
     * in the sense of a background thread managed by this class.
     */
    private void flushDb(String dbName) {
        try {
            RocksDBFactory.getInstance().flushSession(dbName, false);
            log.debug("Cloud storage: triggered flush (non-blocking, event-driven) for db={}", dbName);
        } catch (Exception e) {
            log.warn("Cloud storage: flush failed for db={}: {}", dbName, e.getMessage());
        }
    }

    boolean shouldAttemptReadMissHydration(String dbName, String table) {
        if (readMissGuardWindowMs <= 0) {
            return true;
        }
        long now = System.currentTimeMillis();
        String guardKey = dbName + "::" + table;
        Long prev = readMissAttemptTs.put(guardKey, now);
        if (prev == null) {
            return true;
        }
        long elapsed = now - prev;
        if (elapsed >= readMissGuardWindowMs) {
            return true;
        }
        log.debug("Skip read-miss hydration due to guard window: db={}, table={}, elapsedMs={}",
                  dbName, table, elapsed);
        return false;
    }
}
