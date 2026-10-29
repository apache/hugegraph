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
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import lombok.Getter;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.LiveSstFile;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.MetadataSnapshot;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.RocksdbChangedListener;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;

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
 *   <li>{@link #onTableFileCreated} pins newly created SST files via a hard link, dispatches
 *       upload work to a bounded background executor, and mirrors metadata after upload completes.
 *       If the hard link fails (e.g. cross-device mount, filesystem limits), the upload is routed
 *       directly to the retry queue using the original SST path — no copy is made, so no extra
 *       disk space is consumed.</li>
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
public class CloudStorageEventListener implements RocksdbChangedListener, AutoCloseable {

    /** Absolute, normalised paths of configured store data roots (one per configured partition root). */
    private final String primaryDataRoot;
    private final List<String> allDataRoots;

    /** Optional per-store namespace prefix prepended to every remote cloud key. */
    private final String storeScopePrefix;

    static final long DEFAULT_READ_MISS_GUARD_WINDOW_MS = 3000L;

    private final boolean startupHydrationEnabled;
    private final long readMissGuardWindowMs;
    private final Map<String, Long> readMissAttemptTs;

    /**
     * Tracks which {@code (dbName, remoteKey)} pairs are currently being restored by
     * {@link #restoreMissingLiveFiles} so a thundering herd of concurrent read misses on the
     * same cold SST does not download the same object many times over. This is a best-effort
     * de-duplication; correctness against concurrent restores of the same file is guaranteed by
     * the per-attempt unique temp name plus an atomic replace-move, not by this set.
     */
    private final Set<String> inFlightRestores = ConcurrentHashMap.newKeySet();

    /**
     * Optional retry queue; when non-null, upload failures are submitted here instead
     * of just being logged. When null, failures are only logged (no retry).
     */
    private final CloudUploadRetryQueue retryQueue;

    /** Initialized cloud storage provider; resolved once at startup and never reconfigured. */
    private final CloudStorageProvider provider;

    /** Tracks which SST files are confirmed present in cloud (per-DB confirmed-file-number set). */
    private final CloudSyncTracker syncTracker;

    /**
     * Hard health flag for pending-delete marker durability. Flips to {@code false} when a marker
     * cannot be durably persisted (write + fsync) — a state in which a DB delete whose remote
     * cleanup fails may be unguarded against re-hydration after a crash — and back to
     * {@code true} once a marker is durably persisted again. Surfaced via
     * {@link CloudStorageMetricsConst#DELETE_MARKER_HEALTHY}. {@code volatile}: written from delete
     * callbacks, read from metric-scrape threads.
     * -- GETTER --
     *  Whether pending-delete marker persistence is currently healthy.
     *  indicates a
     *  marker could not be durably written, so a delete whose remote cleanup fails may not
     *  survive a crash as a hydration guard. Bound to
     * <p>
     * .

     */
    @Getter
    private volatile boolean deleteMarkerHealthy = true;

    /** Directory fsync is not supported on Windows directory handles. */
    private static final boolean WINDOWS_OS =
            System.getProperty("os.name", "").toLowerCase().contains("win");

    /** Bounded async upload dispatcher so RocksDB callbacks return quickly. */
    private static final int ASYNC_UPLOAD_THREADS = 2;
    private static final int ASYNC_UPLOAD_QUEUE_CAPACITY = 256;
    /** Upload pool owned by this listener; drained by {@link #close()}. */
    private final ThreadPoolExecutor uploadExecutor;

    // -----------------------------------------------------------------------
    // Metadata-sync coalescing
    // -----------------------------------------------------------------------
    // onTableFileCreated fires once per flushed/compacted SST; syncing metadata inline on every
    // one triggers a full RocksDB checkpoint + S3 list/prune per SST. Instead each upload marks
    // its DB dirty and a fixed-delay drain publishes each dirty DB at most once per interval.
    // Event-driven callers that need an immediate publish (delete guard, onCompacted,
    // onDBCreated) still call syncMetadataSnapshotInline directly and are NOT coalesced.

    /** Default drain interval for the post-upload metadata sync. */
    static final long DEFAULT_METADATA_SYNC_DEBOUNCE_MS = 1_000L;

    /**
     * Scheduler for the metadata-sync drain and purge/delete retries. Owned by this listener and
     * shut down by {@link #close()}; a schedule after that throws
     * {@link RejectedExecutionException}, which every call site handles.
     */
    private final ScheduledExecutorService metadataSyncScheduler;

    /** Drain interval (ms); {@code <= 0} publishes inline on every SST upload instead. */
    private final long metadataSyncDebounceMs;

    /** DBs with uploads not yet reflected in a published manifest; drained by the scheduler. */
    private final Set<String> dirtyMetadataDbs = ConcurrentHashMap.newKeySet();

    // -----------------------------------------------------------------------
    // Metadata (CURRENT/MANIFEST/OPTIONS) mirroring & consistent restore
    // -----------------------------------------------------------------------

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

    /** Per-DB mutexes to serialize metadata capture/publication/pruning. */
    private final Map<String, Object> metadataSyncLocks = new ConcurrentHashMap<>();

    /**
     * Per-remote-prefix mutexes serializing pending-delete cleanup (inline at open vs. the async
     * retry). Ensures the purge runs at most once and only while the marker is present, so it can
     * never delete data a re-created DB uploads to the same prefix after cleanup completes.
     */
    private final Map<String, Object> pendingDeleteLocks = new ConcurrentHashMap<>();

    /**
     * Per-remote-prefix generation, bumped every time a DB is opened or created at that prefix.
     * An asynchronous pending-delete purge captures the value when it is scheduled and aborts
     * (without purging) if it changed, so a retry can never delete objects that a re-created DB
     * has since taken over the prefix with. Entries are never removed: dropping one would reset
     * the counter and let a stale retry match again.
     */
    private final Map<String, AtomicLong> prefixEpochs = new ConcurrentHashMap<>();

    /**
     * Per-DB write generation, bumped when the DB is opened/created and on every SST creation. A
     * truncate purge retry captures it when it is scheduled and aborts if the DB has resumed
     * writing (new objects would be purged along with the stale ones). Never removed, see above.
     */
    private final Map<String, AtomicLong> dbWriteEpochs = new ConcurrentHashMap<>();

    /** Per-DB metadata-publish failure state used to back off a persistently failing publish. */
    private final Map<String, PublishBackoff> publishBackoffs = new ConcurrentHashMap<>();

    /** Cap for the exponential publish-retry backoff. */
    static final long MAX_PUBLISH_BACKOFF_MS = 5 * 60 * 1000L;
    /** Minimum base for the publish-retry backoff, so a tiny debounce does not hot-loop failures. */
    static final long MIN_PUBLISH_BACKOFF_BASE_MS = 1_000L;

    /** Bounded synchronous attempts at completing a pending delete while a DB is being opened. */
    private static final int OPEN_PENDING_DELETE_ATTEMPTS = 3;
    private static final long OPEN_PENDING_DELETE_BACKOFF_MS = 200L;
    /** Bounded attempts at flushing dirty metadata on close(). */
    private static final int CLOSE_FLUSH_ATTEMPTS = 3;
    private static final long CLOSE_FLUSH_BACKOFF_MS = 200L;

    /** Consecutive publish failures and the earliest time the next attempt is allowed. */
    private static final class PublishBackoff {

        final int failures;
        final long nextAttemptAtMs;

        PublishBackoff(int failures, long nextAttemptAtMs) {
            this.failures = failures;
            this.nextAttemptAtMs = nextAttemptAtMs;
        }
    }

    /** Last successfully published RocksDB generation per DB. */
    private final Map<String, Long> lastPublishedMetadataGeneration = new ConcurrentHashMap<>();

    /**
     * Grace period (ms) after truncation during which metadata syncs are suppressed.
     * This allows pending RocksDB background callbacks to complete without re-uploading
     * metadata that was purged during truncation.
     */
    private static final long TRUNCATION_GRACE_PERIOD_MS = 5_000L;

    /**
     * Suffix appended to the DB prefix (not inside it) when a database is deleted.
     * Placing the tombstone as a sibling of the data prefix means the
     * {@link #purgeRemotePrefix} call in {@link #onDBDeleted} cannot accidentally remove it
     * while stale SST or metadata objects remain, so {@link #preHydrateDbFiles} can still
     * detect the deleted generation and skip hydration.
     *
     * <p>Example: data prefix = {@code store-host_8500/hugegraph/db},
     * tombstone key = {@code store-host_8500/hugegraph/db_DELETED}.
     */
    static final String DB_TOMBSTONE_SUFFIX = "_DELETED";

    /** Infix of hydration temp files ({@code <name>.hyd-tmp-<threadId>-<nanoTime>}). */
    private static final String HYDRATION_TEMP_MARKER = ".hyd-tmp-";

    /**
     * Fully-parameterised constructor: the listener is completely configured the moment it is
     * constructed, so there is no window in which a registered listener can be observed
     * partially configured.
     *
     * @param provider initialized cloud storage provider
     * @param dataRoots configured store data roots (typically parsed from comma-separated
     *                  {@code app.data-path})
     * @param readMissGuardWindowMs guard window in ms for repeated read-miss hydration attempts
     *                              for the same db/table pair (cloud.storage.read-miss-guard-window-ms)
     * @param retryQueue optional {@link CloudUploadRetryQueue}; when non-null, upload failures
     *                   are retried asynchronously and eventually given up on.
     *                   Pass {@code null} to disable retries (failures are only logged).
     * @param syncTracker tracks SST files confirmed present in cloud; the delete guard uses it
     *                    to avoid deleting a superseded object before replacements are durable.
     *                    Must be shared with the retry queue. Pass {@code null} for a fresh
     *                    {@link CloudSyncTracker}.
     * @param storeScopePrefix optional per-store key prefix to isolate cloud objects
     * @param metadataSyncDebounceMs drain interval (ms) for the per-SST metadata sync;
     *                               {@code <= 0} publishes inline on every SST upload
     */
    public CloudStorageEventListener(CloudStorageProvider provider,
                                     List<String> dataRoots,
                                     boolean startupHydrationEnabled,
                                     long readMissGuardWindowMs,
                                     CloudUploadRetryQueue retryQueue,
                                     CloudSyncTracker syncTracker,
                                     String storeScopePrefix,
                                     long metadataSyncDebounceMs) {
        // Fail fast on a missing/empty data-root list: primaryDataRoot is derived from index 0
        // below, and every key<->path conversion depends on at least one root. Without this guard
        // the constructor would throw an opaque IndexOutOfBoundsException at
        // allDataRoots.get(0), which is far harder to diagnose than a config error.
        if (dataRoots == null || dataRoots.isEmpty()) {
            throw new IllegalArgumentException(
                    "CloudStorageEventListener requires at least one data root; none configured. "
                    + "Check the store data-path configuration (e.g. rocksdb.data_path / "
                    + "raft.path) used to derive the cloud storage data roots.");
        }
        // Normalize each root
        this.allDataRoots = new ArrayList<>();
        for (String root : dataRoots) {
            String normalised = Paths.get(root).toAbsolutePath().normalize().toString();
            // Strip trailing separator so substring arithmetic is consistent.
            normalised = normalised.endsWith(File.separator)
                    ? normalised.substring(0, normalised.length() - 1)
                    : normalised;
            this.allDataRoots.add(normalised);
        }
        this.primaryDataRoot = this.allDataRoots.get(0);
        this.provider = Objects.requireNonNull(provider, "provider");
        this.startupHydrationEnabled = startupHydrationEnabled;
        this.readMissGuardWindowMs = Math.max(0L, readMissGuardWindowMs);
        this.readMissAttemptTs = new ConcurrentHashMap<>();
        this.retryQueue = retryQueue;
        this.syncTracker = syncTracker != null ? syncTracker : new CloudSyncTracker();
        this.storeScopePrefix = normaliseKeyPrefix(storeScopePrefix);
        this.uploadExecutor = new ThreadPoolExecutor(
                ASYNC_UPLOAD_THREADS,
                ASYNC_UPLOAD_THREADS,
                60L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(ASYNC_UPLOAD_QUEUE_CAPACITY),
                newUploadThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        ScheduledThreadPoolExecutor syncScheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "cloud-metadata-sync");
            t.setDaemon(true);
            return t;
        });
        // close() uses a graceful shutdown() so an in-flight publish completes; not-yet-due delayed
        // tasks (purge/delete retries) are cancelled instead of holding close() open. Their intent
        // is crash-durable (local markers) and is re-driven by the next startup scan.
        syncScheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.metadataSyncScheduler = syncScheduler;
        this.metadataSyncDebounceMs = metadataSyncDebounceMs;
        if (metadataSyncDebounceMs > 0) {
            this.metadataSyncScheduler.scheduleWithFixedDelay(this::drainDirtyMetadata,
                                                              metadataSyncDebounceMs,
                                                              metadataSyncDebounceMs,
                                                              TimeUnit.MILLISECONDS);
        }

        // A crash may leave local pending-delete markers whose remote cleanup never completed.
        // Kick off bounded async retries for each so stale remote data is eventually purged even if
        // the affected DB is never re-opened (an open would otherwise be the only trigger).
        scanMarkers(pendingDeleteDir(), (db, prefix) -> {
            log.warn("Cloud startup: found pending remote-delete marker for prefix={} — "
                     + "scheduling deferred purge to prevent stale-data re-hydration.", prefix);
            scheduleDeletePurgeRetry(db, prefix, 1);
        });
        // Truncate purge intent is also crash-durable via local markers.
        scanMarkers(pendingTruncateDir(), (db, prefix) -> {
            log.warn("Cloud startup: found pending truncate marker for prefix={} — scheduling "
                     + "deferred purge retry.", prefix);
            scheduleTruncatePurgeRetry(db, prefix);
        });
        // Upload intent is not persisted across restarts, so any staging pin left by the previous
        // process is orphaned; the live SSTs it pinned are re-uploaded by startup backfill.
        sweepStagingDirsOnStartup();
    }

    private static ThreadFactory newUploadThreadFactory() {
        return r -> {
            Thread t = new Thread(r, "cloud-upload-dispatch");
            t.setDaemon(true);
            return t;
        };
    }

    /** Shuts down this listener's executors, waiting up to 10s for in-flight uploads. */
    @Override
    public void close() {
        close(10, TimeUnit.SECONDS);
    }

    /**
     * Shuts down the SST upload executor, waiting up to {@code timeout} for in-flight uploads to
     * complete, then the metadata-sync scheduler. Called from
     * {@link org.apache.hugegraph.store.node.AppConfig#onDestroy()} after the listener has been
     * deregistered, so uploads started before shutdown have a chance to finish.
     */
    void close(long timeout, TimeUnit unit) {
        // Drain the upload executor FIRST so DBs marked dirty by in-flight uploads are flushed below.
        uploadExecutor.shutdown();
        try {
            if (!uploadExecutor.awaitTermination(timeout, unit)) {
                log.warn("CloudStorageEventListener: upload executor did not terminate within "
                         + "{}{}; forcing shutdown", timeout, unit);
                handoffDroppedUploadTasks(uploadExecutor.shutdownNow());
            }
        } catch (InterruptedException e) {
            handoffDroppedUploadTasks(uploadExecutor.shutdownNow());
            Thread.currentThread().interrupt();
        }
        // Graceful stop: an in-flight drain/publish finishes instead of being interrupted mid-upload,
        // and the final flush below runs only after the drain thread is gone so the two cannot
        // publish the same DB concurrently.
        metadataSyncScheduler.shutdown();
        try {
            if (!metadataSyncScheduler.awaitTermination(timeout, unit)) {
                log.warn("CloudStorageEventListener: metadata-sync scheduler did not terminate "
                         + "within {}{}; forcing shutdown", timeout, unit);
                metadataSyncScheduler.shutdownNow();
                metadataSyncScheduler.awaitTermination(1, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            metadataSyncScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        // Flush dirty DBs so uploads that completed during the drain are referenced by the
        // mirrored CURRENT/MANIFEST. Later requests (e.g. retry-queue completions) see the
        // scheduler shut down in markMetadataDirty and publish inline.
        flushDirtyMetadataOnClose();
    }

    /**
     * Final bounded flush of every dirty DB. A failed publish is retried a few times; DBs that
     * still cannot be published (or are mid-truncate) are logged so the gap is visible.
     */
    private void flushDirtyMetadataOnClose() {
        List<String> remaining = new ArrayList<>(dirtyMetadataDbs);
        for (int attempt = 1; attempt <= CLOSE_FLUSH_ATTEMPTS && !remaining.isEmpty(); attempt++) {
            for (String dbName : new ArrayList<>(remaining)) {
                if (isActivelyTruncating(dbName) || isInTruncationGracePeriod(dbName)) {
                    continue; // the purge supersedes it; keep it reported below
                }
                dirtyMetadataDbs.remove(dbName);
                if (publishDirtyMetadata(dbName)) {
                    remaining.remove(dbName);
                } else {
                    dirtyMetadataDbs.add(dbName);
                }
            }
            if (remaining.isEmpty() || attempt == CLOSE_FLUSH_ATTEMPTS
                || remaining.stream().allMatch(db -> isActivelyTruncating(db)
                                                     || isInTruncationGracePeriod(db))) {
                break;
            }
            try {
                Thread.sleep(CLOSE_FLUSH_BACKOFF_MS * attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (!remaining.isEmpty()) {
            log.error("CloudStorageEventListener: closed with metadata NOT published for {} DB(s): "
                      + "{} — the mirrored CURRENT/MANIFEST may lag the uploaded SSTs until the next "
                      + "start publishes them.", remaining.size(), remaining);
        }
    }

    /**
     * Routes upload tasks discarded by a forced {@code shutdownNow()} into their retry queue
     * so an SST accepted locally but never mirrored is not silently lost from the durability
     * pipeline. Only {@link SstUploadTask}s carry the metadata needed for handoff; any other
     * runnable (there should be none) is ignored.
     */
    private static void handoffDroppedUploadTasks(java.util.List<Runnable> dropped) {
        if (dropped == null || dropped.isEmpty()) {
            return;
        }
        int handed = 0;
        for (Runnable r : dropped) {
            if (r instanceof SstUploadTask) {
                try {
                    ((SstUploadTask) r).handoffOnShutdown();
                    handed++;
                } catch (Exception e) {
                    log.warn("Failed to hand off a dropped upload task to the retry queue: {}",
                             e.getMessage());
                }
            }
        }
        if (handed > 0) {
            log.warn("CloudStorageEventListener: handed off {} queued upload task(s) to the retry queue "
                     + "during shutdown so their upload intent survives.", handed);
        }
    }

    /**
     * Finds which configured data roots contain the given file path.
     * Returns the matching root, or the primary root if no exact match is found.
     *
     * <p>Uses {@link Path#startsWith} so that {@code /data/store1} never incorrectly
     * matches {@code /data/store10/...} (a raw string prefix match would).
     *
     * @param filePath absolute file path
     * @return the matched configured data root for this file
     */
    private String findMatchingDataRoot(String filePath) {
        Path fileNormalized = Paths.get(filePath).toAbsolutePath().normalize();
        for (String root : allDataRoots) {
            if (fileNormalized.startsWith(Paths.get(root))) {
                return root;
            }
        }
        // Fallback to primary root (should not happen in normal operation)
        return primaryDataRoot;
    }

    // -----------------------------------------------------------------------
    // RocksdbChangedListener
    // -----------------------------------------------------------------------

    @Override
    public boolean managesRemoteState() {
        return true;
    }

    /**
     * Picks the directory to open when the local one holds no DB. A partition can move to a
     * version-suffixed directory ({@code <name>_<v1>[_<v2>]}) and its objects then live under that
     * prefix, so after a disk loss the default path would hydrate nothing. Lists the remote
     * siblings that share the base name and returns the newest restorable one, matching the order
     * {@code RocksDBSession} uses locally. A sibling is restorable when it has a remote
     * {@code CURRENT}, no delete tombstone and no local pending-delete marker. If listing fails the
     * path is kept, and the hydration of that path applies its own failure rules.
     */
    @Override
    public String resolveOpenPath(String dbName, String dbPath) {
        if (!startupHydrationEnabled) {
            return dbPath;
        }
        Path candidate = Paths.get(dbPath).toAbsolutePath().normalize();
        Path parent = candidate.getParent();
        if (parent == null) {
            return dbPath;
        }
        String name = candidate.getFileName().toString();
        int sep = name.indexOf('_');
        String baseName = sep < 0 ? name : name.substring(0, sep);
        String basePrefix = dbPrefix(parent.resolve(baseName).toString());
        if (!basePrefix.endsWith(baseName)) {
            return dbPath;
        }
        String parentKeyPrefix = basePrefix.substring(0, basePrefix.length() - baseName.length());
        List<String> keys;
        try {
            keys = provider.listFiles(basePrefix);
        } catch (IOException e) {
            log.warn("Cloud open: listing version siblings of db={} failed, keeping {}: {}",
                     dbName, dbPath, e.getMessage());
            return dbPath;
        }
        String newest = newestRestorableSibling(parentKeyPrefix, baseName, keys);
        if (newest == null || newest.equals(name)) {
            return dbPath;
        }
        log.info("Cloud open: db={} has no local DB at {}; restoring newest remote sibling '{}'",
                 dbName, dbPath, newest);
        return parent.resolve(newest).toString();
    }

    /**
     * Newest restorable directory name among {@code baseName} and its version-suffixed siblings
     * found in {@code keys}, or {@code null} if none (package-private for tests).
     */
    String newestRestorableSibling(String parentKeyPrefix, String baseName, List<String> keys) {
        Pattern dirName = Pattern.compile(Pattern.quote(baseName) + "(?:_(\\d+)(?:_(\\d+))?)?");
        Set<String> withCurrent = new HashSet<>();
        Set<String> tombstoned = new HashSet<>();
        for (String key : keys) {
            if (!key.startsWith(parentKeyPrefix)) {
                continue;
            }
            String rel = key.substring(parentKeyPrefix.length());
            int slash = rel.indexOf('/');
            if (slash < 0) {
                if (rel.endsWith(DB_TOMBSTONE_SUFFIX)) {
                    tombstoned.add(rel.substring(0, rel.length() - DB_TOMBSTONE_SUFFIX.length()));
                }
                continue;
            }
            String dir = rel.substring(0, slash);
            if ("CURRENT".equals(rel.substring(slash + 1)) && dirName.matcher(dir).matches()) {
                withCurrent.add(dir);
            }
        }
        String newest = null;
        long[] newestVersion = null;
        for (String dir : withCurrent) {
            if (tombstoned.contains(dir) || hasPendingDeleteMarker(parentKeyPrefix + dir)) {
                continue;
            }
            Matcher m = dirName.matcher(dir);
            if (!m.matches()) {
                continue;
            }
            long[] version;
            try {
                version = new long[]{m.group(1) == null ? -1L : Long.parseLong(m.group(1)),
                                     m.group(2) == null ? -1L : Long.parseLong(m.group(2))};
            } catch (NumberFormatException e) {
                continue;
            }
            if (newestVersion == null || version[0] > newestVersion[0]
                || (version[0] == newestVersion[0] && version[1] > newestVersion[1])) {
                newest = dir;
                newestVersion = version;
            }
        }
        return newest;
    }

    @Override
    public void onDBOpening(String dbName, String dbPath) {
        // Register the dir -> logical-name mapping first: native events that arrive before
        // onDBCreated (or on a disabled-hydration node) must resolve to the logical name.
        recordDb(dbName, dbPath);
        // Stale hydration temp files can only be left by a crashed download; sweep them from the
        // local dir on every open, not just when something is about to be hydrated.
        sweepStaleHydrationTemps(Paths.get(dbPath));

        String prefix = dbPrefix(dbPath);
        // Invalidate every async purge scheduled for the previous generation at this prefix BEFORE
        // anything below can write to it. Retries compare against this epoch under the prefix lock.
        bumpEpoch(prefixEpochs, prefix);
        bumpEpoch(dbWriteEpochs, dbName);

        // Pending-delete guard, independent of hydration: a prior delete's remote cleanup may not
        // be confirmed, so the reopened DB must not upload into (or hydrate from) a prefix that
        // still holds the deleted generation and a delete marker.
        if (hasPendingDeleteMarker(prefix)) {
            completePendingDeleteOrBlockOpen(dbName, prefix);
        }
        boolean truncatePurgeConfirmed = true;
        if (hasPendingTruncateMarker(prefix)) {
            truncatePurgeConfirmed = tryCompletePendingTruncate(dbName, prefix);
        }
        if (!startupHydrationEnabled) {
            // Nothing is hydrated, but the DB will upload into this prefix: a leftover tombstone
            // must be resolved now, or a later open with hydration enabled would purge its data.
            purgeTombstonedPrefix(provider, dbName, Paths.get(dbPath), prefix, false);
            return;
        }
        if (!truncatePurgeConfirmed) {
            log.warn("Cloud pre-hydration skipped for db={}: remote truncate purge not yet "
                     + "confirmed — opening without re-hydrating cleared data.", dbName);
            return;
        }
        preHydrateDbFiles(provider, dbName, dbPath);
    }

    /**
     * Finishes a pending remote delete synchronously (bounded attempts) before the open proceeds.
     * If it cannot be completed the open is BLOCKED: the marker means the prefix still holds the
     * deleted generation, and opening anyway would let the new DB upload into it while the async
     * retry (or a restart) later purges the new data. The async retry is re-armed so the delete
     * finishes when the cloud recovers, and a subsequent open attempt simply tries again.
     */
    private void completePendingDeleteOrBlockOpen(String dbName, String prefix) {
        for (int attempt = 1; attempt <= OPEN_PENDING_DELETE_ATTEMPTS; attempt++) {
            if (tryCompletePendingDelete(provider, dbName, prefix, null)) {
                return;
            }
            if (attempt < OPEN_PENDING_DELETE_ATTEMPTS) {
                try {
                    Thread.sleep(OPEN_PENDING_DELETE_BACKOFF_MS * attempt);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        scheduleDeletePurgeRetry(dbName, prefix, 1);
        throw new IllegalStateException(String.format(
                "Cloud open blocked for db=%s: remote-delete cleanup of prefix '%s' is not yet "
                + "confirmed. Opening now would let the new DB upload into a prefix still holding "
                + "the deleted generation; the cleanup is being retried in the background.",
                dbName, prefix));
    }

    /** Best-effort inline purge of a pending truncate at open; re-arms the async retry on failure. */
    private boolean tryCompletePendingTruncate(String dbName, String prefix) {
        Object lock = pendingDeleteLocks.computeIfAbsent(prefix, k -> new Object());
        synchronized (lock) {
            if (!hasPendingTruncateMarker(prefix)) {
                return true;
            }
            if (purgeRemotePrefix(provider, dbName, prefix)) {
                removeMarker(pendingTruncateDir(), prefix);
                return true;
            }
        }
        scheduleTruncatePurgeRetry(dbName, prefix);
        return false;
    }

    /** Deletes stale {@code .hyd-tmp-} download temp files left under a local DB dir by a crash. */
    private void sweepStaleHydrationTemps(Path root) {
        if (!Files.isDirectory(root)) {
            return;
        }
        try (Stream<Path> stale = Files.walk(root, 10)) {
            stale.filter(p -> p.getFileName() != null
                              && p.getFileName().toString().contains(HYDRATION_TEMP_MARKER))
                 .forEach(p -> deleteIfExistsQuietly(p, "stale hydration temp cleanup"));
        } catch (IOException e) {
            log.debug("Failed to scan stale hydration temp files under {}: {}",
                      root, e.getMessage());
        }
    }

    private static long bumpEpoch(Map<String, AtomicLong> epochs, String key) {
        return epochs.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
    }

    private static long epochOf(Map<String, AtomicLong> epochs, String key) {
        AtomicLong epoch = epochs.get(key);
        return epoch == null ? 0L : epoch.get();
    }

    /** Current open/create generation of {@code prefix} (package-private for tests). */
    long prefixEpoch(String prefix) {
        return epochOf(prefixEpochs, prefix);
    }

    /**
     * Called when a RocksDB read fails, typically because a live SST is missing on local disk.
     *
     * <p>We restore only the SST files that RocksDB references as <em>live</em> in its manifest but
     * that are physically missing on local disk, downloading each back to its <em>exact original
     * path</em> so RocksDB finds it on the next access. This deliberately avoids
     * {@code ingestExternalFile}, which would (a) risk placing a file into the wrong column family
     * and (b) assign a fresh sequence number that can resurrect deleted keys. Restricting to the
     * live set also guarantees superseded / compacted-away objects are never resurrected.
     *
     * <p>If no live file is missing (the error has another cause) this returns {@code false}
     * without any cloud I/O, and the original error propagates.
     */
    @Override
    public boolean onReadMiss(RocksDBSession session, String table, byte[] key) {
        String dbName = session.getGraphName();
        if (!shouldAttemptReadMissHydration(dbName, table)) {
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
            // De-dup concurrent restores of the same object (thundering herd on a cold read
            // miss). If another thread already holds the slot, skip: it will produce the file.
            String restoreGuardKey = dbName + "::" + remoteKey;
            if (!inFlightRestores.add(restoreGuardKey)) {
                continue;
            }
            try {
                // Re-check after acquiring the slot: a concurrent restore may have just finished.
                if (Files.exists(localPath)) {
                    continue;
                }
                if (!provider.fileExists(remoteKey)) {
                    log.warn("Cloud read-miss: live file missing locally AND absent in cloud: "
                             + "db={}, key={}", dbName, remoteKey);
                    continue;
                }
                downloadAtomically(provider, remoteKey, localPath);
                syncTracker.markConfirmed(dbName, live.getAbsolutePath());
                restored++;
            } catch (IOException e) {
                log.warn("Cloud read-miss restore failed: db={}, key={}, reason={}",
                         dbName, remoteKey, e.getMessage());
            } finally {
                inFlightRestores.remove(restoreGuardKey);
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
        // The DB is live at this prefix now: cancel any purge scheduled for an earlier generation.
        bumpEpoch(prefixEpochs, dbPrefix(dbPath));
        bumpEpoch(dbWriteEpochs, dbName);
        // Do not propagate upload failures: the session is already live in dbSessionMap at
        // this point, so throwing here would cause the createGraphDB caller to receive an
        // error while the DB is actually open and usable by other threads (split-brain).
        try {
            uploadExistingSstFiles(provider, dbName, dbPath);
        } catch (Exception e) {
            log.warn("Cloud initial-upload failed for db={}: {} — DB is open, "
                     + "existing SSTs may not be in cloud yet", dbName, e.getMessage());
        }
        flushDb(dbName);
        // Mirror metadata immediately after initial upload/flush to keep cloud state recoverable.
        syncMetadataSnapshotInline(provider, dbName);
    }

    /**
     * Called just before the local RocksDB directory is removed. Writes a small tombstone object
     * (key = {@code dbPrefix + }{@value #DB_TOMBSTONE_SUFFIX}) outside the data prefix so that
     * any subsequent {@link #preHydrateDbFiles} call for the same path will detect the deleted
     * generation and skip hydration rather than re-ingesting stale objects.
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
        String prefix = dbPrefix(dbPath);
        // ALWAYS persist a LOCAL pending-delete marker first. This is the durable anti-resurrection
        // guard that survives a failed remote cleanup: even if we cannot write the remote
        // tombstone or purge below, the marker blocks hydration of this DB (see onDBOpening) and
        // drives async cleanup until confirmed. It is removed once the remote purge succeeds.
        //
        // Marker durability is a hard precondition for a safe delete: if we cannot fsync it, a crash
        // could lose the guard and let stale remote SST/metadata be re-hydrated as live data. So on a
        // persistence failure we flip the health signal to degraded and HOLD delete progression
        // (throw) rather than proceeding unguarded — the caller can retry once local storage recovers.
        try {
            writeMarker(pendingDeleteDir(), prefix);
            deleteMarkerHealthy = true;
            log.debug("Cloud pending-delete marker persisted (fsync'd): db={}, prefix={}",
                      dbName, prefix);
        } catch (IOException e) {
            deleteMarkerHealthy = false;
            log.error("Cloud pending-delete marker could not be durably persisted for db={} "
                      + "prefix={}: {} — HOLDING delete to avoid an unguarded delete that a crash "
                      + "could let re-hydrate. Delete-marker health is DEGRADED.",
                      dbName, prefix, e.getMessage());
            throw new IllegalStateException(
                    "Cannot durably persist pending-delete marker for db=" + dbName
                    + "; holding delete to preserve the anti-resurrection guard", e);
        }
        // The delete is going ahead and the prefix will be purged, so publishing the DB's
        // metadata when its session closes (onDBClosing) would be wasted work.
        dirtyMetadataDbs.remove(dbName);

        // Tombstone lives OUTSIDE the data prefix (sibling, not child) so it is not
        // accidentally deleted by purgeRemotePrefix when SST objects are still present.
        String tombstoneKey = prefix + DB_TOMBSTONE_SUFFIX;
        try {
            writeTombstone(prefix);
            log.info("Cloud DB tombstone written: db={}, key={}", dbName, tombstoneKey);
        } catch (Exception e) {
            log.warn("Cloud DB tombstone write failed (onDBDeleted will still purge): "
                     + "db={}, key={}, reason={}", dbName, tombstoneKey, e.getMessage());
        }
    }

    /**
     * Publishes a still-dirty DB's metadata while its session is open. The release path (a
     * partition moving away, and {@code releaseAllGraphDB()} at shutdown, which runs before this
     * listener is closed) would otherwise close the DB first, and the deferred publish could then
     * never capture it, leaving the cloud CURRENT/MANIFEST behind the uploaded SSTs.
     */
    @Override
    public void onDBClosing(String dbName) {
        if (isActivelyTruncating(dbName) || isInTruncationGracePeriod(dbName)) {
            return; // the truncate purge supersedes the publish
        }
        if (!dirtyMetadataDbs.remove(dbName)) {
            return;
        }
        if (!publishDirtyMetadata(dbName)) {
            // Not re-marked: the DB is closing, so a deferred publish would only fail again.
            log.warn("Cloud metadata publish before close failed for db={}: the mirrored "
                     + "CURRENT/MANIFEST may lag the uploaded SSTs until the DB is next opened.",
                     dbName);
        }
    }

    /** Uploads the sibling delete-tombstone for {@code prefix} from a short-lived temp file. */
    private void writeTombstone(String prefix) throws IOException {
        Path tmp = Files.createTempFile("hgstore-tombstone-", ".tmp");
        try {
            Files.writeString(tmp, "deleted");
            provider.uploadFile(tmp.toString(), prefix + DB_TOMBSTONE_SUFFIX);
        } finally {
            deleteIfExistsQuietly(tmp, "tombstone temp cleanup");
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
        String deletedDir = normaliseDir(dbPath);
        dbNameByDir.remove(deletedDir);
        if (dbNameByDir.containsValue(dbName)) {
            // The same logical DB is live at another directory (it was re-created there while this
            // one awaited destruction). Its name-keyed state belongs to the live copy, so only
            // the work queued for files of the deleted directory is dropped.
            deferredSstDeletes.removeIf(t -> dbName.equals(t.dbName)
                                             && deletedDir.equals(parentDir(t.filePath)));
            log.info("Cloud DB deleted at {} while db={} is live elsewhere; keeping its state",
                     deletedDir, dbName);
        } else {
            // Drop the dirty mark before the truncation guards below: a concurrent drain that
            // sees the guards cleared must then fail its dirtyMetadataDbs.remove() and skip.
            dirtyMetadataDbs.remove(dbName);
            publishBackoffs.remove(dbName);
            // Clear in-memory tracking so no stale state bleeds into a recreated DB.
            syncTracker.clearDb(dbName);
            readMissAttemptTs.entrySet().removeIf(e -> e.getKey().startsWith(dbName + "::"));
            dbNameByDir.values().removeIf(dbName::equals);
            metadataSyncLocks.remove(dbName);
            lastPublishedMetadataGeneration.remove(dbName);
            truncationTimes.remove(dbName);
            truncatingDbs.remove(dbName);
            deferredSstDeletes.removeIf(t -> dbName.equals(t.dbName));
            // Remove per-DB meters so meter cardinality does not grow without bound as DBs churn.
            CloudStorageMetrics.unregisterDatabaseMetrics(dbName);
        }

        String prefix = dbPrefix(dbPath);
        if (purgeRemotePrefix(provider, dbName, prefix)
            && deleteTombstone(provider, dbName, prefix)) {
            // Remote data and the sibling tombstone are gone: drop the local marker (cleanup done).
            removeMarker(pendingDeleteDir(), prefix);
        } else {
            // Purge or tombstone delete failed. Keep the local marker so a future onDBOpening is
            // blocked until both succeed, and retry (bounded) — same safety level as the truncate
            // purge. The marker must outlive the tombstone: a tombstone left next to a re-created
            // DB would make a later open purge that DB's live objects.
            log.warn("Cloud DB delete cleanup failed for db={}: local marker preserved to block "
                     + "re-opening the prefix; scheduling bounded retries.", dbName);
            scheduleDeletePurgeRetry(dbName, prefix, 1);
        }
    }

    /**
     * Deletes the sibling delete-tombstone once the prefix purge has succeeded. Returns
     * {@code false} on failure: the caller must then keep the pending-delete marker (and so keep
     * the prefix blocked), because a tombstone that outlives the marker would make the next open
     * purge the live objects of a DB re-created at the same prefix.
     */
    private boolean deleteTombstone(CloudStorageProvider provider, String dbName, String prefix) {
        String tombstoneKey = prefix + DB_TOMBSTONE_SUFFIX;
        try {
            provider.deleteFile(tombstoneKey);
            log.debug("Cloud DB tombstone cleaned up: db={}, key={}", dbName, tombstoneKey);
            return true;
        } catch (IOException e) {
            log.warn("Cloud DB tombstone cleanup failed: db={}, key={}: {}",
                     dbName, tombstoneKey, e.getMessage());
            return false;
        }
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
        syncTracker.clearDb(dbName);
        readMissAttemptTs.entrySet().removeIf(e -> e.getKey().startsWith(dbName + "::"));
        deferredSstDeletes.removeIf(t -> dbName.equals(t.dbName));
    }

    @Override
    public void onDBTruncated(String dbName, String dbPath) {
        truncatingDbs.add(dbName);
        try {
            String prefix = dbPrefix(dbPath);
            try {
                writeMarker(pendingTruncateDir(), prefix);
                log.debug("Cloud pending-truncate marker persisted (fsync'd): db={}, prefix={}",
                          dbName, prefix);
            } catch (IOException e) {
                log.error("Cloud truncate marker persist failed for db={}, prefix={}: {} — "
                          + "remote purge intent is not crash-durable until the next successful "
                          + "persist.",
                          dbName, prefix, e.getMessage());
            }
            boolean purged = purgeRemotePrefix(provider, dbName, prefix);
            if (purged) {
                removeMarker(pendingTruncateDir(), prefix);
            }
            if (!purged) {
                // Do NOT ignore a failed purge: stale remote SST/metadata still describe the
                // pre-clear generation and could be re-hydrated after a restart + disk loss,
                // silently resurrecting data the operator explicitly cleared. A transient cloud
                // error is the common cause, so retry the purge (bounded, with backoff) on the
                // scheduler. If every attempt fails we log loudly for operator intervention rather
                // than reporting a clean truncate.
                log.error("Cloud truncate purge failed for db={} (prefix={}): stale remote objects "
                          + "remain; scheduling bounded retries to prevent resurrection of cleared "
                          + "data on a future restore.", dbName, prefix);
                scheduleTruncatePurgeRetry(dbName, prefix);
            }
        } finally {
            truncationTimes.put(dbName, System.currentTimeMillis());
            truncatingDbs.remove(dbName);
        }
    }

    /** Max asynchronous retries for a failed truncate purge before giving up (logging loudly). */
    private static final int MAX_TRUNCATE_PURGE_RETRIES = 5;
    /** Base backoff (ms) for truncate-purge retries; doubles each attempt up to a small cap. */
    private static final long TRUNCATE_PURGE_RETRY_BASE_MS = 500L;

    /**
     * Retries a failed truncate purge asynchronously with capped exponential backoff. Each attempt
     * refreshes the truncation grace window so metadata mirroring stays suppressed until the remote
     * prefix is confirmed clean, avoiding a re-mirror of not-yet-purged stale objects. After
     * {@link #MAX_TRUNCATE_PURGE_RETRIES} failures it logs an error and stops — the stale objects
     * then require manual cleanup, but the failure is at least visible rather than silent.
     */
    private void scheduleTruncatePurgeRetry(String dbName, String prefix) {
        // Capture the write generation now: the purge is only valid for the cleared generation.
        scheduleTruncatePurgeRetry(dbName, prefix, 1, epochOf(dbWriteEpochs, dbName));
    }

    private void scheduleTruncatePurgeRetry(String dbName, String prefix, int attempt,
                                            long writeEpoch) {
        if (attempt > MAX_TRUNCATE_PURGE_RETRIES) {
            log.error("Cloud truncate purge for db={} still failing after {} attempt(s) — remote "
                      + "prefix '{}' may retain stale objects that could be re-hydrated on restore. "
                      + "Manual cleanup of that prefix is required.",
                      dbName, MAX_TRUNCATE_PURGE_RETRIES, prefix);
            return;
        }
        int exp = Math.min(Math.max(attempt - 1, 0), 30);
        long delay = Math.min(TRUNCATE_PURGE_RETRY_BASE_MS * (1L << exp), 8_000L);
        // Keep the grace window fresh so we do not mirror new metadata over the still-stale prefix.
        truncationTimes.put(dbName, System.currentTimeMillis());
        try {
            metadataSyncScheduler.schedule(() -> {
                boolean retryAgain = false;
                // Same per-prefix lock as the delete purge, so the epoch check and the purge are
                // atomic with respect to an open/create at this prefix.
                Object lock = pendingDeleteLocks.computeIfAbsent(prefix, k -> new Object());
                synchronized (lock) {
                    if (epochOf(dbWriteEpochs, dbName) != writeEpoch) {
                        // The DB resumed writing after the truncate: the prefix now holds live
                        // objects, which a purge would destroy. The stale ones are unreferenced
                        // by the freshly published CURRENT/MANIFEST, so drop the purge intent
                        // (also from disk, or a restart would re-drive it).
                        removeMarker(pendingTruncateDir(), prefix);
                        log.warn("Cloud truncate purge for db={} abandoned: the DB wrote new data "
                                 + "after the truncate, so purging prefix '{}' would delete live "
                                 + "objects.", dbName, prefix);
                        return;
                    }
                    if (purgeRemotePrefix(provider, dbName, prefix)) {
                        removeMarker(pendingTruncateDir(), prefix);
                        log.info("Cloud truncate purge succeeded on retry for db={} (attempt {})",
                                 dbName, attempt);
                    } else {
                        retryAgain = true;
                    }
                }
                if (retryAgain) {
                    scheduleTruncatePurgeRetry(dbName, prefix, attempt + 1, writeEpoch);
                }
            }, delay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            log.error("Cloud truncate purge retry could not be scheduled for db={} (scheduler "
                      + "shutting down) — remote prefix '{}' may retain stale objects.",
                      dbName, prefix);
        }
    }

    // -----------------------------------------------------------------------
    // DB-deletion remote cleanup: local pending-delete marker + bounded purge retry
    // -----------------------------------------------------------------------

    /** Local subdirectory of the primary data root holding pending-remote-delete markers. */
    static final String PENDING_DELETE_DIR = ".cloud-pending-delete";
    /** Local subdirectory of each data root holding hard-linked SSTs awaiting async upload. */
    static final String STAGING_DIR = ".cloud-upload-staging";
    /** Local subdirectory holding pending-remote-truncate purge markers. */
    static final String PENDING_TRUNCATE_DIR = ".cloud-pending-truncate";
    /** Max async retries for a failed delete purge before giving up (logging loudly). */
    private static final int MAX_DELETE_PURGE_RETRIES = 8;
    /** Base backoff (ms) for delete-purge retries; doubles each attempt up to a small cap. */
    private static final long DELETE_PURGE_RETRY_BASE_MS = 500L;
    /** Retry delay for superseded-SST deletes deferred by transient metadata/cloud failures. */
    private static final long DEFERRED_SST_DELETE_RETRY_MS = 500L;
    /** Emit one WARN when deferred delete backlog crosses this size; otherwise keep DEBUG-only. */
    private static final int DEFERRED_SST_DELETE_WARN_THRESHOLD = 64;

    /** Pending superseded-SST deletes held until metadata/cloud preconditions are satisfied. */
    private final Set<DeferredSstDelete> deferredSstDeletes = ConcurrentHashMap.newKeySet();
    /** Coalescing guard so only one deferred-delete retry task is scheduled at a time. */
    private final AtomicBoolean deferredSstDeleteRetryScheduled = new AtomicBoolean(false);
    /** Ensures backlog WARN is emitted only once per threshold crossing. */
    private final AtomicBoolean deferredSstDeleteBacklogWarned = new AtomicBoolean(false);

    /** Immutable payload for a deferred superseded-SST delete. */
    private static final class DeferredSstDelete {

        final String dbName;
        final String filePath;

        DeferredSstDelete(String dbName, String filePath) {
            this.dbName = dbName;
            this.filePath = filePath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DeferredSstDelete that)) {
                return false;
            }
            return java.util.Objects.equals(this.dbName, that.dbName)
                   && java.util.Objects.equals(this.filePath, that.filePath);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(this.dbName, this.filePath);
        }
    }

    private Path pendingDeleteDir() {
        return Paths.get(primaryDataRoot, PENDING_DELETE_DIR);
    }

    private Path pendingTruncateDir() {
        return Paths.get(primaryDataRoot, PENDING_TRUNCATE_DIR);
    }

    private static void deleteIfExistsQuietly(Path path, String context) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException cleanupError) {
            log.debug("Failed to cleanup temp file during {}: path={}, reason={}",
                      context, path, cleanupError.getMessage());
        }
    }

    /**
     * Marker file path for a remote prefix. The remote prefix contains '/', so the filename is a
     * URL-safe Base64 of the prefix — reversible (for the startup scan) and collision-free. The
     * filename is the only payload; the marker file itself is empty.
     */
    private static Path markerPath(Path dir, String prefix) {
        return dir.resolve(encodeMarkerName(prefix));
    }

    private static String encodeMarkerName(String prefix) {
        return java.util.Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /** Decodes the marker filename (Base64 URL, no padding) back to the original remote prefix. */
    private static String decodeMarkerName(String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return null;
        }
        try {
            byte[] bytes = java.util.Base64.getUrlDecoder().decode(encoded);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Crash-safely persists an empty marker for {@code prefix} under {@code dir}. Pending-delete
     * markers are the durable anti-resurrection guard, so this is NOT best-effort: the marker file
     * is created and fsynced, and the containing directory is fsynced so the new directory entry
     * itself survives a crash. Throws {@link IOException} if durability cannot be guaranteed.
     */
    private static void writeMarker(Path dir, String prefix) throws IOException {
        Files.createDirectories(dir);
        // TRUNCATE_EXISTING drops the payload of markers written by older versions.
        try (FileChannel ch = FileChannel.open(markerPath(dir, prefix),
                                               StandardOpenOption.CREATE,
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.TRUNCATE_EXISTING)) {
            ch.force(true);
        }
        // fsync the directory so the new marker entry itself is durable (a file fsync does not
        // guarantee the directory entry pointing at it survives a crash on many filesystems).
        try (FileChannel dirCh = FileChannel.open(dir, StandardOpenOption.READ)) {
            dirCh.force(true);
        } catch (IOException e) {
            // Windows rejects directory-handle fsync; elsewhere it is a real durability error.
            if (WINDOWS_OS) {
                log.debug("Directory fsync of {} not supported on this platform: {}",
                          dir, e.getMessage());
            } else {
                throw e;
            }
        }
    }

    private static void removeMarker(Path dir, String prefix) {
        try {
            Files.deleteIfExists(markerPath(dir, prefix));
        } catch (IOException e) {
            log.debug("Failed to remove marker in {} for prefix={}: {}",
                      dir, prefix, e.getMessage());
        }
    }

    /** Whether a local pending-remote-delete marker exists for {@code prefix}. */
    boolean hasPendingDeleteMarker(String prefix) {
        return Files.exists(markerPath(pendingDeleteDir(), prefix));
    }

    /** Whether a local pending-remote-truncate marker exists for {@code prefix}. */
    boolean hasPendingTruncateMarker(String prefix) {
        return Files.exists(markerPath(pendingTruncateDir(), prefix));
    }

    /**
     * Completes a pending remote delete under a per-prefix lock: if the marker is still present,
     * ensure the tombstone exists, purge the prefix, and on success drop the tombstone + marker.
     * Returns {@code true} when cleanup is confirmed complete (marker absent after this call) or
     * the call was cancelled, {@code false} when the purge failed and should be retried.
     *
     * <p>The lock + marker re-check make the purge run at most once and ONLY while the marker is
     * present. {@code expectedEpoch} (non-null for the async retry) is the {@link #prefixEpoch}
     * captured when the retry was scheduled: a DB opened or created at the prefix since then bumped
     * it, so the retry stands down without purging — the prefix may already hold the new DB's
     * objects. An open never passes an epoch: it bumped the epoch itself and runs the cleanup
     * inline, before its DB can upload anything.
     */
    private boolean tryCompletePendingDelete(CloudStorageProvider provider, String dbName,
                                             String prefix, Long expectedEpoch) {
        Object lock = pendingDeleteLocks.computeIfAbsent(prefix, k -> new Object());
        synchronized (lock) {
            if (expectedEpoch != null && prefixEpoch(prefix) != expectedEpoch) {
                log.info("Cloud pending-delete purge cancelled for db={} (prefix={}): the prefix "
                         + "was opened/created since it was scheduled.", dbName, prefix);
                return true;
            }
            if (!hasPendingDeleteMarker(prefix)) {
                return true; // already cleaned up by another caller
            }
            ensureTombstonePresent(provider, dbName, prefix);
            if (purgeRemotePrefix(provider, dbName, prefix)
                && deleteTombstone(provider, dbName, prefix)) {
                removeMarker(pendingDeleteDir(), prefix);
                log.info("Cloud pending-delete cleanup completed for db={} (prefix={})",
                         dbName, prefix);
                return true;
            }
            return false;
        }
    }

    /**
     * Retries the remote purge for a deleted DB with capped exponential backoff — the same safety
     * level as the truncate purge. While retries are pending, the local pending-delete marker keeps
     * {@link #onDBOpening} from re-hydrating stale data. After {@link #MAX_DELETE_PURGE_RETRIES}
     * failures it logs loudly for operator action; the marker and tombstone remain, so
     * re-hydration stays blocked until manual cleanup.
     */
    private void scheduleDeletePurgeRetry(String dbName, String prefix, int attempt) {
        scheduleDeletePurgeRetry(dbName, prefix, attempt, prefixEpoch(prefix));
    }

    private void scheduleDeletePurgeRetry(String dbName, String prefix, int attempt,
                                          long prefixEpochAtSchedule) {
        if (attempt > MAX_DELETE_PURGE_RETRIES) {
            log.error("Cloud DB delete purge for db={} still failing after {} attempt(s) — remote "
                      + "prefix '{}' may retain stale objects. The local pending-delete marker and "
                      + "tombstone remain (re-hydration stays blocked); manual cleanup is required.",
                      dbName, MAX_DELETE_PURGE_RETRIES, prefix);
            return;
        }
        long delay = Math.min(DELETE_PURGE_RETRY_BASE_MS * (1L << (attempt - 1)), 8_000L);
        try {
            metadataSyncScheduler.schedule(() -> {
                // Cancelled (returns true) when the marker is already cleared OR a DB was opened /
                // created at the prefix since this retry was scheduled, so the retry never purges a
                // prefix a re-created DB has taken over.
                if (!tryCompletePendingDelete(provider, dbName, prefix, prefixEpochAtSchedule)) {
                    scheduleDeletePurgeRetry(dbName, prefix, attempt + 1, prefixEpochAtSchedule);
                }
            }, delay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            log.error("Cloud DB delete purge retry could not be scheduled for db={} (scheduler "
                      + "shutting down) — local marker preserved; remote prefix '{}' may retain "
                      + "stale objects.", dbName, prefix);
        }
    }

    /** Writes the delete-tombstone if it is not already present (best-effort). */
    private void ensureTombstonePresent(CloudStorageProvider provider, String dbName, String prefix) {
        String tombstoneKey = prefix + DB_TOMBSTONE_SUFFIX;
        try {
            if (provider.fileExists(tombstoneKey)) {
                return;
            }
            writeTombstone(prefix);
            log.info("Cloud DB tombstone written (deferred): db={}, key={}", dbName, tombstoneKey);
        } catch (Exception e) {
            log.warn("Deferred tombstone write failed for db={}, key={}: {}",
                     dbName, tombstoneKey, e.getMessage());
        }
    }

    /** Deletes staging hard-links left in every data root by a previous process. */
    private void sweepStagingDirsOnStartup() {
        for (String root : allDataRoots) {
            Path dir = Paths.get(root, STAGING_DIR);
            if (!Files.isDirectory(dir)) {
                continue;
            }
            int removed = 0;
            try (Stream<Path> pins = Files.list(dir)) {
                for (Path pin : (Iterable<Path>) pins::iterator) {
                    try {
                        if (Files.deleteIfExists(pin)) {
                            removed++;
                        }
                    } catch (IOException e) {
                        log.warn("Failed to delete orphaned staging file {}: {}", pin,
                                 e.getMessage());
                    }
                }
            } catch (IOException e) {
                log.warn("Failed to scan staging dir {}: {}", dir, e.getMessage());
            }
            if (removed > 0) {
                log.info("Cloud startup: removed {} orphaned staging file(s) from {}", removed, dir);
            }
        }
    }

    /**
     * Scans {@code dir} for markers left by a previous process and calls {@code onMarker} with
     * (inferred dbName, prefix) for each valid one. Markers whose name does not decode or whose
     * prefix is out of this listener's scope are logged and left in place for operator inspection.
     */
    private void scanMarkers(Path dir, BiConsumer<String, String> onMarker) {
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (Stream<Path> markers = Files.list(dir)) {
            markers.forEach(marker -> {
                Path fileName = marker.getFileName();
                String prefix = fileName == null ? null : decodeMarkerName(fileName.toString());
                if (prefix == null) {
                    log.error("Cloud startup: ignoring marker {} with undecodable name", marker);
                    return;
                }
                if (isMarkerPrefixOutOfScope(prefix)) {
                    log.error("Cloud startup: ignoring out-of-scope marker {} for prefix='{}'",
                              marker, prefix);
                    return;
                }
                onMarker.accept(inferDbNameFromPrefix(prefix), prefix);
            });
        } catch (IOException e) {
            log.warn("Failed to scan markers in {}: {}", dir, e.getMessage());
        }
    }

    /**
     * Returns {@code true} if a marker prefix must be rejected: empty, absolute, not normalized, or
     * outside this listener's store scope.
     */
    private boolean isMarkerPrefixOutOfScope(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return true;
        }
        String normalized = prefix.replace('\\', '/');
        if (normalized.startsWith("/") || normalized.contains("//")) {
            return true;
        }
        String[] parts = normalized.split("/");
        for (String p : parts) {
            if (p.isEmpty() || ".".equals(p) || "..".equals(p)) {
                return true;
            }
        }
        if (storeScopePrefix.isEmpty()) {
            return false;
        }
        String scopedPrefix = storeScopePrefix + "/";
        return !normalized.equals(storeScopePrefix) && !normalized.startsWith(scopedPrefix);
    }

    /** Best-effort DB-name inference for startup marker logs/scheduling. */
    private String inferDbNameFromPrefix(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return prefix;
        }
        try {
            String stripped = stripStoreScope(prefix);
            return stripped.isEmpty() ? prefix : stripped;
        } catch (IllegalArgumentException e) {
            return prefix;
        }
    }

    /**
     * A truncate failed partway (drop/create threw after {@link #onDBTruncateBegin}). Clear the
     * "truncating" suppression so cloud metadata mirroring, object deletion, and the delete guard
     * resume immediately — but do NOT purge remote state: the data intended for clearing may still
     * be present locally and must remain recoverable from cloud.
     */
    @Override
    public void onDBTruncateAbort(String dbName, String dbPath) {
        truncatingDbs.remove(dbName);
        // Drop the begin-timestamp so the (successful-truncate) grace period does not suppress
        // mirroring for a truncate that never completed.
        truncationTimes.remove(dbName);
        log.warn("Cloud truncate aborted for db={} — cleared 'truncating' suppression without "
                 + "purging remote state (data may still be present locally)", dbName);
    }

    /**
     * Returns {@code true} while a truncate is in progress for {@code dbName}. The latch set by
     * {@link #onDBTruncateBegin} is always cleared by {@link #onDBTruncated} or
     * {@link #onDBTruncateAbort}: {@code RocksDBSession.truncate()} fires one of them in a
     * {@code finally}.
     */
    boolean isActivelyTruncating(String dbName) {
        return truncatingDbs.contains(dbName);
    }

    /** Test seam exposing {@link #isInTruncationGracePeriod} for assertions. */
    @SuppressWarnings("SameParameterValue")
    boolean isInTruncationGracePeriodForTest(String dbName) {
        return isInTruncationGracePeriod(dbName);
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
     *
     * @return {@code true} if the purge completed without error; {@code false} if it failed.
     *         The caller uses the return value to decide whether it is safe to remove the
     *         tombstone — the tombstone must survive any partial purge.
     */
    private boolean purgeRemotePrefix(CloudStorageProvider provider, String dbName, String prefix) {
        String normalizedPrefix = prefix.endsWith("/") ? prefix : prefix + "/";
        try {
            int deleted = provider.deletePrefix(normalizedPrefix);
            if (deleted > 0) {
                log.info("Cloud DB purge completed: db={}, prefix={}, deleted={}",
                         dbName, prefix, deleted);
            }
            // Every confirmation for this DB now names a deleted object. Drop them (bumping the
            // tracker epoch so in-flight confirmations are discarded too); otherwise the snapshot
            // SST check would skip re-uploading them and publish a MANIFEST pointing at nothing.
            // Skipped when the name is live under another prefix: those confirmations are valid.
            if (!liveUnderOtherPrefix(dbName, prefix)) {
                syncTracker.clearDb(dbName);
            }
            return true;
        } catch (IOException e) {
            log.warn("Cloud DB purge failed for db={}, prefix={}: {}",
                     dbName, prefix, e.getMessage());
            return false;
        }
    }

    /**
     * Pins and asynchronously uploads the newly created SST file to the active cloud
     * storage provider.
     *
     * @param dbNameOrPath RocksDB instance name (partition id) or DB directory path
     * @param cfName   column-family name
     * @param filePath absolute local path of the new SST file
     * @param fileSize file size in bytes (informational)
     */
    @Override
    public void onTableFileCreated(String dbNameOrPath, String cfName,
                                   String filePath, long fileSize) {
        String dbName = resolveDbName(dbNameOrPath, filePath);
        recordDb(dbName, parentDir(filePath));
        // New data is being written under this DB: a truncate purge scheduled earlier must no
        // longer run, or it would delete this SST's remote object.
        bumpEpoch(dbWriteEpochs, dbName);
        CloudStorageMetrics.registerDatabaseMetrics(dbName);
        String remoteKey = toRelativeKey(filePath);

        // Capture the epoch before we hand off to a background thread.  The async callback
        // uses markConfirmedIfEpoch so a late confirmation after clearDb() + DB recreation
        // with reused file numbers is silently dropped instead of producing stale durability state.
        long uploadEpoch = syncTracker.currentEpoch(dbName);


        Path pinned;
        try {
            pinned = pinForAsyncUpload(filePath);
        } catch (Exception e) {
            String errorType = e.getClass().getSimpleName();
            CloudStorageMetrics.recordUploadFailure(dbName, cfName, errorType);
            // Hard link failed — no copy fallback, no extra disk use. Route original SST path
            // to retry queue; retry will upload directly from the original file if still present.
            log.warn("Cloud upload staging (hard link failed): db={}, cf={}, path={} "
                     + "— routing original SST to retry queue: {}",
                     dbName, cfName, filePath, e.getMessage());
            if (retryQueue != null) {
                // Pass the captured epoch so a successful retry is confirmed via the epoch-guarded
                // callback; the plain submit() would use epoch 0 and be silently dropped.
                retryQueue.submit(dbName, cfName, filePath, remoteKey, uploadEpoch, e);
            }
            return;
        }

        try {
            uploadExecutor.execute(new SstUploadTask(provider, dbName, cfName, pinned, filePath,
                                                     remoteKey, uploadEpoch, fileSize));
        } catch (RejectedExecutionException e) {
            // Queue is full. Keep the hard-link pin alive and submit IT to the retry queue so
            // the file survives even if RocksDB compacts the original SST before the retry fires.
            // Pass both the pinned path (for the actual upload) and the original SST path (for
            // confirmation and cleanup) so CloudSyncTracker can parse the file number correctly.
            CloudStorageMetrics.recordUploadFailure(dbName, cfName, "UploadQueueFull");
            log.error("Cloud upload dispatch rejected (queue full): db={}, cf={}, path={}",
                      dbName, cfName, filePath);
            if (retryQueue != null) {
                retryQueue.submitPinned(dbName, cfName, pinned.toString(), filePath, remoteKey,
                                        uploadEpoch,
                                        new IOException("cloud upload dispatch queue full", e));
            } else {
                // No retry queue — nothing more we can do; clean up the pin.
                try {
                    Files.deleteIfExists(pinned);
                } catch (IOException ioe) {
                    log.debug("Failed to cleanup staged upload file {}: {}", pinned, ioe.getMessage());
                }
            }
        }
    }

    /**
     * A single asynchronous SST upload, as a typed {@link Runnable} rather than a lambda so that a
     * forced {@code shutdownNow()} of the upload executor returns the actual task objects
     * (a {@link ThreadPoolExecutor} returns the exact runnables it had queued). {@link
     * #handoffOnShutdown()} then routes any never-run task into the retry queue so an SST that
     * was accepted locally but not yet mirrored is not silently dropped from the durability
     * pipeline. {@code run()} preserves the original inline behaviour exactly.
     */
    private final class SstUploadTask implements Runnable {

        private final CloudStorageProvider provider;
        private final String dbName;
        private final String cfName;
        private final Path pinned;
        private final String filePath;
        private final String remoteKey;
        private final long uploadEpoch;
        private final long fileSize;

        SstUploadTask(CloudStorageProvider provider, String dbName, String cfName, Path pinned,
                      String filePath, String remoteKey, long uploadEpoch, long fileSize) {
            this.provider = provider;
            this.dbName = dbName;
            this.cfName = cfName;
            this.pinned = pinned;
            this.filePath = filePath;
            this.remoteKey = remoteKey;
            this.uploadEpoch = uploadEpoch;
            this.fileSize = fileSize;
        }

        @Override
        public void run() {
            long startTimeMs = System.currentTimeMillis();
            // When the upload fails and is handed to the retry queue, OWNERSHIP of the pinned
            // hard-link transfers to the retry queue lifecycle — it must NOT be deleted here, or a
            // retry firing after RocksDB has compacted away the original SST would find no source
            // and be silently dropped (retry intent lost). We only delete the pin when we still
            // own it (successful upload, or no retry queue to hand off to).
            boolean pinHandedOff = false;
            try {
                // ---- Upload proper: ONLY a failure here is an upload failure that should retry. ----
                try {
                    provider.uploadFile(pinned.toString(), remoteKey);
                } catch (Exception e) {
                    String errorType = e.getClass().getSimpleName();
                    CloudStorageMetrics.recordUploadFailure(dbName, cfName, errorType);
                    log.error("Cloud upload failed (will retry on next compaction): "
                              + "db={}, cf={}, path={}, error={}", dbName, cfName,
                              filePath, e.getMessage());
                    if (retryQueue != null) {
                        // Hand the SURVIVING pinned hard-link to the retry queue (submitPinned), not
                        // the original SST path (plain submit): compaction may delete the original
                        // before the retry fires, but the pin keeps the byteset alive.
                        // sourceSstPath=filePath is still used for epoch-guarded confirmation and
                        // staging cleanup on success.
                        retryQueue.submitPinned(dbName, cfName, pinned.toString(), filePath,
                                                remoteKey, uploadEpoch, e);
                        pinHandedOff = true;
                    }
                    return;
                }
                // ---- Post-upload: the upload SUCCEEDED. Errors below (e.g. an inline metadata
                // publish failing during shutdown) must NOT be reclassified as an upload failure
                // and re-uploaded or given up on; the SST is already durable in cloud. ----
                syncTracker.markConfirmedIfEpoch(dbName, filePath, uploadEpoch);
                long syncLatencyMs = System.currentTimeMillis() - startTimeMs;
                CloudStorageMetrics.recordSyncLatency(dbName, syncLatencyMs);
                try {
                    // Coalesce the per-SST metadata sync into the periodic drain. (Truncation/grace
                    // suppression is handled inside markMetadataDirty.)
                    markMetadataDirty(dbName);
                } catch (Exception e) {
                    log.warn("Cloud upload succeeded but post-upload metadata-sync scheduling "
                             + "failed (SST is durable; CURRENT/MANIFEST will catch up on the next "
                             + "sync): db={}, path={}: {}", dbName, filePath, e.getMessage());
                }
                log.debug("Cloud upload success: db={}, cf={}, path={}, size={}, latencyMs={}",
                          dbName, cfName, filePath, fileSize, syncLatencyMs);
            } finally {
                if (!pinHandedOff) {
                    try {
                        Files.deleteIfExists(pinned);
                    } catch (IOException e) {
                        log.debug("Failed to cleanup staged upload file {}: {}",
                                  pinned, e.getMessage());
                    }
                }
            }
        }

        /**
         * Called for a task that was queued but never ran because the executor was force-stopped at
         * shutdown. Hands the still-pinned file to the retry queue so its upload intent survives:
         * the pin is kept alive (not deleted) so the retry queue can upload it even if RocksDB has
         * since compacted the original SST away.
         */
        void handoffOnShutdown() {
            if (retryQueue == null) {
                // No retry queue: best-effort cleanup of the pin; nothing else we can do.
                try {
                    Files.deleteIfExists(pinned);
                } catch (IOException ignore) {
                    // best-effort
                }
                return;
            }
            log.warn("Cloud upload task not executed before shutdown; handing off to retry queue: "
                     + "db={}, cf={}, path={}", dbName, cfName, filePath);
            retryQueue.submitPinned(dbName, cfName, pinned.toString(), filePath, remoteKey,
                                    uploadEpoch,
                                    new IOException("upload executor stopped before task ran"));
        }
    }

    /**
     * Creates a stable hard-link snapshot of the SST file for async upload, so the upload worker
     * can read a consistent source even after RocksDB deletes the original during compaction.
     *
     * <p>Hard links share the same inode — no extra data blocks are consumed. If the original is
     * deleted by compaction before the worker runs, the hard-link still holds the inode alive so
     * the upload can proceed normally.
     *
     * <p>If the hard link fails (e.g. cross-device mount, filesystem hard-link limits), an
     * {@link IOException} is thrown. The caller ({@link #onTableFileCreated}) catches this and
     * routes the upload to the retry queue using the original SST path — no copy is made and no
     * extra disk space is consumed. The retry will succeed as long as the original file still
     * exists when it fires; if it has been compacted away the retry queue silently drops it.
     */
    private Path pinForAsyncUpload(String filePath) throws IOException {
        Path source = Paths.get(filePath);
        // Use the staging dir that lives on the same filesystem as the source SST to avoid
        // cross-device hard-link failures in multi-disk deployments.
        String matchingRoot = findMatchingDataRoot(source.toAbsolutePath().normalize().toString());
        Path stagingDir = Paths.get(matchingRoot, STAGING_DIR);
        Files.createDirectories(stagingDir);
        String fileName = source.getFileName().toString();
        Path staged = stagingDir.resolve(fileName + ".upload-" + System.nanoTime());
        try {
            Files.createLink(staged, source);
            return staged;
        } catch (Exception linkEx) {
            throw new IOException(
                    "Hard link failed; upload will be retried from original SST path: "
                    + linkEx.getMessage(), linkEx);
        }
    }

    /**
     * Removes the deleted SST file from the active cloud storage provider.
     *
     * <p>This callback runs on a RocksDB background (compaction/flush) thread, so it performs no
     * cloud I/O itself: the delete is queued and {@link #processDeferredSstDeletes} runs the
     * live-set durability guard, the MANIFEST publish and the remote delete on the metadata-sync
     * thread, publishing metadata once per DB for every delete a compaction produced.
     *
     * @param dbNameOrPath RocksDB instance name (partition id) or DB directory path
     * @param cfName   column-family name
     * @param filePath absolute local path of the deleted SST file
     */
    @Override
    public void onTableFileDeleted(String dbNameOrPath, String cfName, String filePath) {
        // RocksDB delivers the DB *directory path* here (from db.getName()), not the logical name —
        // exactly as it does for onTableFileCreated. Resolve it the same way: without this, the
        // delete guard's live-set lookup (RocksDBFactory.getLiveSstFiles), the metadata-sync
        // capture (captureMetadataSnapshot), and the sync-tracker updates all key off a path that
        // is absent from dbSessionMap, so every superseded-SST removal is silently skipped and the
        // object leaks in cloud forever (with an endless deferred-delete retry loop).
        String dbName = resolveDbName(dbNameOrPath, filePath);
        // Skip file deletion during truncation or grace period; the entire DB prefix will be purged anyway
        if (isActivelyTruncating(dbName) || isInTruncationGracePeriod(dbName)) {
            log.debug("Skipping delete during truncation: db={}, path={}", dbName, filePath);
            return;
        }
        if (deferredSstDeletes.add(new DeferredSstDelete(dbName, filePath))) {
            log.debug("Cloud delete queued: db={}, cf={}, path={}", dbName, cfName, filePath);
            updateDeferredSstDeleteWarnState();
        }
        scheduleDeferredSstDeleteRetry(0L);
    }

    /** Emits one WARN only when deferred backlog crosses the threshold, then rearms below it. */
    private void updateDeferredSstDeleteWarnState() {
        int backlog = deferredSstDeletes.size();
        if (backlog >= DEFERRED_SST_DELETE_WARN_THRESHOLD) {
            if (deferredSstDeleteBacklogWarned.compareAndSet(false, true)) {
                log.warn("Deferred cloud-delete backlog crossed threshold: pending={}, threshold={}",
                         backlog, DEFERRED_SST_DELETE_WARN_THRESHOLD);
            }
            return;
        }
        deferredSstDeleteBacklogWarned.set(false);
    }

    private void scheduleDeferredSstDeleteRetry(long delayMs) {
        if (deferredSstDeletes.isEmpty()) {
            return;
        }
        if (!deferredSstDeleteRetryScheduled.compareAndSet(false, true)) {
            return;
        }
        long safeDelay = Math.max(0L, delayMs);
        try {
            metadataSyncScheduler.schedule(() -> {
                deferredSstDeleteRetryScheduled.set(false);
                processDeferredSstDeletes();
            }, safeDelay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            deferredSstDeleteRetryScheduled.set(false);
            log.debug("Deferred cloud-delete retry scheduling rejected (scheduler shutting down): {}",
                      e.getMessage());
        }
    }

    /**
     * Deletes queued superseded SSTs from cloud, on the metadata-sync thread. For each DB the
     * preconditions are checked once per pass, whatever the number of queued deletes:
     *
     * <ol>
     *   <li>DATA-LOSS GUARD: every SST currently live in the DB is confirmed present in cloud, so
     *       a compaction input is never removed before its outputs are durable.</li>
     *   <li>MANIFEST-BEFORE-DELETE: an updated MANIFEST+CURRENT reflecting the post-compaction
     *       live set is published first; otherwise a crash between the SST delete and the next
     *       metadata sync leaves a cloud MANIFEST that references a deleted object.</li>
     * </ol>
     *
     * <p>Deletes whose DB fails either check, or whose remote delete fails, stay queued and are
     * retried. Package-private (and serialised) so tests can drive a pass deterministically.
     */
    synchronized void processDeferredSstDeletes() {
        if (deferredSstDeletes.isEmpty()) {
            return;
        }
        Map<String, Boolean> readyByDb = new HashMap<>();
        int succeeded = 0;
        for (DeferredSstDelete task : new ArrayList<>(deferredSstDeletes)) {
            if (isActivelyTruncating(task.dbName) || isInTruncationGracePeriod(task.dbName)) {
                continue;
            }
            boolean ready = readyByDb.computeIfAbsent(task.dbName, this::readyForSstDeletes);
            if (!ready) {
                continue;
            }
            String remoteKey = toRelativeKey(task.filePath);
            try {
                provider.deleteFile(remoteKey);
                syncTracker.clearConfirmed(task.dbName, task.filePath);
                if (deferredSstDeletes.remove(task)) {
                    succeeded++;
                }
            } catch (Exception e) {
                log.debug("Deferred cloud-delete retry failed: db={}, path={}, reason={}",
                          task.dbName, task.filePath, e.getMessage());
            }
        }
        if (succeeded > 0) {
            log.debug("Cloud delete removed {} superseded SST(s)", succeeded);
        }
        updateDeferredSstDeleteWarnState();
        if (!deferredSstDeletes.isEmpty()) {
            scheduleDeferredSstDeleteRetry(DEFERRED_SST_DELETE_RETRY_MS);
        }
    }

    /** Whether superseded SSTs of {@code dbName} may be deleted from cloud right now. */
    private boolean readyForSstDeletes(String dbName) {
        // Share the drain's per-DB publish backoff: while a DB's publishes keep failing, the 500 ms
        // delete retry must not redo a checkpoint (which flushes the memtable) and an upload.
        PublishBackoff backoff = publishBackoffs.get(dbName);
        if (backoff != null && System.currentTimeMillis() < backoff.nextAttemptAtMs) {
            return false;
        }
        if (ensureLiveSetUploaded(provider, dbName)) {
            log.warn("Cloud delete held (live set not fully durable in cloud): db={}", dbName);
            return false;
        }
        // The compaction is complete, so its outputs are already on disk and the MANIFEST already
        // reflects the post-compaction state; a failure here is recorded for the backoff.
        if (!publishDirtyMetadata(dbName)) {
            log.warn("Cloud delete held (metadata sync failed, MANIFEST not yet updated): db={}",
                     dbName);
            return false;
        }
        return true;
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
        return !ensureLiveSetUploaded(provider, dbName, currentLiveSstFiles(dbName));
    }

    /**
     * Returns the DB's current live SST set. Package-private and overridable purely as a test seam
     * so the {@link #onTableFileDeleted} delete-guard path can be exercised with a controlled
     * live-set (the production implementation reads the RocksDB singleton, which a unit test without
     * a live session cannot populate).
     */
    List<LiveSstFile> currentLiveSstFiles(String dbName) {
        return RocksDBFactory.getInstance().getLiveSstFiles(dbName);
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
            long uploadEpoch = syncTracker.currentEpoch(dbName);
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
                if (!syncTracker.markConfirmedIfEpoch(dbName, localPath, uploadEpoch)) {
                    allDurable = false;
                    log.warn("Delete guard: stale live-file confirmation dropped by epoch guard: "
                             + "db={}, filePath={}, epoch={}", dbName, localPath, uploadEpoch);
                }
            } catch (Exception e) {
                allDurable = false;
                log.warn("Delete guard: failed to upload live file: db={}, filePath={}, error={}",
                         dbName, localPath, e.getMessage());
                if (retryQueue != null) {
                    // Epoch captured now so a successful retry confirms via the epoch-guarded
                    // callback (plain submit() uses epoch 0 → dropped, so the file would never be
                    // marked durable and the delete guard would keep re-uploading it).
                    retryQueue.submit(dbName, live.getCfName(), localPath, remoteKey,
                                      uploadEpoch, e);
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
     * A compaction changed this DB's live SST set; schedule a (coalesced) metadata publish.
     *
     * <p>Note: RocksDB delivers the DB <em>directory path</em> here (from {@code db.getName()}),
     * not the logical name, so we resolve it against {@link #dbNameByDir}.
     */
    @Override
    public void onCompacted(String dbNameOrPath) {
        String dbName = resolveDbName(dbNameOrPath, null);
        // Runs on a RocksDB compaction thread: only mark the DB dirty, the coalesced drain does
        // the checkpoint + upload. markMetadataDirty keeps the mark across truncation/grace so the
        // publish happens afterwards.
        markMetadataDirty(dbName);
    }

    // -----------------------------------------------------------------------
    // Metadata mirroring
    // -----------------------------------------------------------------------

    /** Records the resolved DB directory for a logical DB name (idempotent). */
    private void recordDb(String dbName, String dbDir) {
        if (dbName == null || dbDir == null) {
            return;
        }
        dbNameByDir.put(normaliseDir(dbDir), dbName);
    }

    private static String normaliseDir(String dir) {
        return Paths.get(dir).toAbsolutePath().normalize().toString();
    }

    /** Whether {@code dbName} is mapped to a DB directory whose remote prefix is not {@code prefix}. */
    private boolean liveUnderOtherPrefix(String dbName, String prefix) {
        for (Map.Entry<String, String> e : dbNameByDir.entrySet()) {
            if (dbName.equals(e.getValue()) && !prefix.equals(dbPrefix(e.getKey()))) {
                return true;
            }
        }
        return false;
    }

    /** Absolute, normalised parent directory of a file path (the DB dir of an SST). */
    private String parentDir(String filePath) {
        Path parent = Paths.get(filePath).toAbsolutePath().normalize().getParent();
        return parent == null ? null : parent.toString();
    }

    /** Resolves callback DB identifiers that may be path-form into logical DB names. */
    private String resolveDbName(String dbNameOrPath, String filePath) {
        if (dbNameOrPath == null || dbNameOrPath.isBlank()) {
            String parent = parentDir(filePath);
            if (parent == null) {
                return dbNameOrPath;
            }
            Path name = Paths.get(parent).getFileName();
            return name == null ? dbNameOrPath : name.toString();
        }

        if (!isPathLike(dbNameOrPath)) {
            return dbNameOrPath;
        }

        String normalised = Paths.get(dbNameOrPath).toAbsolutePath().normalize().toString();
        String mapped = dbNameByDir.get(normalised);
        if (mapped != null && !mapped.isBlank()) {
            return mapped;
        }

        Path name = Paths.get(normalised).getFileName();
        return name == null ? dbNameOrPath : name.toString();
    }

    private static boolean isPathLike(String value) {
        return value.contains("/") || value.contains("\\");
    }

    /**
     * Coalesces the high-frequency, per-SST metadata sync triggered by {@link #onTableFileCreated}
     * by marking {@code dbName} dirty for {@link #drainDirtyMetadata}. This does not weaken the
     * durability contract: SST objects are uploaded eagerly regardless — only the (idempotent,
     * generation-guarded) metadata pointer publish is coalesced. Publishes inline instead when
     * coalescing is disabled or the scheduler has shut down.
     */
    void markMetadataDirty(String dbName) {
        if (isActivelyTruncating(dbName) || isInTruncationGracePeriod(dbName)) {
            // Keep the mark: the drain skips this DB while it truncates and publishes once the
            // grace period ends. (Dropping it would leave the SSTs written right after the clear
            // unreferenced by the mirrored CURRENT/MANIFEST until an unrelated sync fires.)
            dirtyMetadataDbs.add(dbName);
            if (metadataSyncDebounceMs <= 0) {
                scheduleGraceDrain();
            }
            return;
        }
        if (metadataSyncDebounceMs <= 0) {
            publishDirtyMetadata(dbName);
            return;
        }
        dirtyMetadataDbs.add(dbName);
        // Re-check after adding so a mark racing close()'s final flush is not stranded.
        if (metadataSyncScheduler.isShutdown() && dirtyMetadataDbs.remove(dbName)) {
            publishDirtyMetadata(dbName);
        }
    }

    /** With no periodic drain (inline mode), schedules a one-shot drain after the grace period. */
    private void scheduleGraceDrain() {
        try {
            metadataSyncScheduler.schedule(this::drainDirtyMetadata,
                                           TRUNCATION_GRACE_PERIOD_MS + 100L,
                                           TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            log.debug("Grace-period metadata drain not scheduled (scheduler shut down): {}",
                      e.getMessage());
        }
    }

    /**
     * Trigger invoked by {@link CloudUploadRetryQueue} after an upload becomes durable via retry.
     * Publishes CURRENT/MANIFEST (coalesced) so the mirrored recovery point advances
     * even when the retry succeeded during an otherwise-quiet period on an idle DB — without this,
     * cloud could hold the SST but keep exposing a stale CURRENT. Runs on the retry-queue thread.
     */
    public void onRetryUploadDurable(String dbName) {
        // During shutdown the retry queue is closed AFTER this listener and BEFORE the provider, so
        // a retry that becomes durable in that window must still advance the pointer. Once the
        // scheduler is shut down, markMetadataDirty falls back to an inline publish.
        markMetadataDirty(dbName);
        // A newly durable upload may unblock one or more deferred superseded-SST deletes.
        scheduleDeferredSstDeleteRetry(0L);
    }

    /**
     * Periodic drain on {@link #metadataSyncScheduler}: publishes each dirty DB once. A DB that is
     * truncating (or in the post-truncate grace period) keeps its mark and is skipped. A failed
     * publish re-marks the DB and backs off exponentially per DB (see {@link #publishBackoffMs}),
     * so a persistently failing DB does not redo a checkpoint + upload every interval.
     */
    private void drainDirtyMetadata() {
        boolean skippedForTruncation = false;
        long now = System.currentTimeMillis();
        for (String dbName : new ArrayList<>(dirtyMetadataDbs)) {
            if (isActivelyTruncating(dbName) || isInTruncationGracePeriod(dbName)) {
                skippedForTruncation = true;
                continue;
            }
            PublishBackoff backoff = publishBackoffs.get(dbName);
            if (backoff != null && now < backoff.nextAttemptAtMs) {
                continue;
            }
            if (!dirtyMetadataDbs.remove(dbName)) {
                continue;
            }
            if (publishDirtyMetadata(dbName)) {
                publishBackoffs.remove(dbName);
            } else {
                dirtyMetadataDbs.add(dbName);
            }
        }
        if (skippedForTruncation && metadataSyncDebounceMs <= 0) {
            scheduleGraceDrain();
        }
    }

    /** Backoff before the next publish attempt after {@code failures} consecutive failures. */
    long publishBackoffMs(int failures) {
        long base = Math.max(MIN_PUBLISH_BACKOFF_BASE_MS, metadataSyncDebounceMs);
        int exp = Math.min(Math.max(failures - 1, 0), 30);
        long delay = base * (1L << exp);
        return delay <= 0L || delay > MAX_PUBLISH_BACKOFF_MS ? MAX_PUBLISH_BACKOFF_MS : delay;
    }

    /** Consecutive publish failures currently recorded for {@code dbName} (test seam). */
    int publishFailureCount(String dbName) {
        PublishBackoff backoff = publishBackoffs.get(dbName);
        return backoff == null ? 0 : backoff.failures;
    }

    /**
     * Publishes metadata for a dirty DB. Returns {@code true} when published, {@code false} when
     * the publish failed (the failure is recorded for backoff; success clears it). Callers decide
     * whether a truncating DB is skipped; this method does not.
     */
    private boolean publishDirtyMetadata(String dbName) {
        boolean published;
        try {
            published = syncMetadataSnapshotInline(provider, dbName);
        } catch (Exception e) {
            log.warn("Deferred cloud metadata-sync failed for db={}: {}", dbName, e.getMessage());
            published = false;
        }
        if (published) {
            publishBackoffs.remove(dbName);
        } else {
            publishBackoffs.compute(dbName, (k, prev) -> {
                int failures = prev == null ? 1 : prev.failures + 1;
                return new PublishBackoff(failures, System.currentTimeMillis()
                                                    + publishBackoffMs(failures));
            });
        }
        return published;
    }

    /**
     * Captures a consistent metadata snapshot (via a RocksDB {@link org.rocksdb.Checkpoint}) and
     * mirrors it to cloud. Safe to call from any thread, including a RocksDB event/compaction
     * callback (it does not call {@code flushSession(wait=true)}, which would deadlock there).
     *
     * <p><b>Flush behavior:</b> the capture uses RocksJava {@code Checkpoint.createCheckpoint},
     * which flushes the memtable (verified by {@code RocksDBFactoryTest}). That flush is intentional
     * and beneficial here: it persists un-flushed MemTable data into an SST that is then mirrored, so
     * recent writes are recoverable from cloud (the RocksDB WAL is disabled under Raft, so a flush is
     * the only way to push the tail into cloud; the Raft log is the tail's local durability source).
     * The cost is a small extra L0 SST (write amplification) whose <em>frequency</em> is bounded by
     * the metadata-sync drain interval; when the memtable is empty the flush is a no-op.
     *
     * <p>Package-private for testability.
     */
    boolean syncMetadataSnapshotInline(CloudStorageProvider provider, String dbName) {
        Object lock = metadataSyncLocks.computeIfAbsent(dbName, ignored -> new Object());
        synchronized (lock) {
            MetadataSnapshot snapshot = captureMetadataSnapshot(dbName);
            if (snapshot == null) {
                return false;
            }
            try {
                return publishSnapshotWithGenerationCheck(provider, dbName, snapshot);
            } finally {
                snapshot.cleanup();
            }
        }
    }

    MetadataSnapshot captureMetadataSnapshot(String dbName) {
        return RocksDBFactory.getInstance().captureMetadataSnapshot(dbName);
    }

    private boolean publishSnapshotWithGenerationCheck(CloudStorageProvider provider,
                                                       String dbName,
                                                       MetadataSnapshot snapshot) {
        long snapshotGeneration = snapshot.getGeneration();
        long lastPublished = lastPublishedMetadataGeneration.getOrDefault(dbName, Long.MIN_VALUE);
        if (snapshotGeneration >= 0L && snapshotGeneration < lastPublished) {
            log.warn("Cloud metadata-sync rejected stale snapshot: db={}, generation={}, lastPublished={}",
                     dbName, snapshotGeneration, lastPublished);
            return false;
        }
        boolean published = uploadMetadataSnapshot(provider, dbName, snapshot);
        if (published && snapshotGeneration >= 0L) {
            lastPublishedMetadataGeneration.merge(dbName, snapshotGeneration, Math::max);
        }
        return published;
    }

    /**
     * Uploads a captured metadata snapshot to cloud in strict consistency order:
     * <ol>
     *   <li>every SST the captured manifest references (from the checkpoint's hard-links);</li>
     *   <li>the {@code OPTIONS-*} and {@code MANIFEST-<n>} blobs;</li>
     *   <li><b>last</b>, {@code CURRENT} — the pointer — so a restore that fetches {@code CURRENT}
     *       never sees it referencing a manifest whose SSTs are not all in cloud;</li>
     *   <li>only then prune superseded remote {@code MANIFEST-*}/{@code OPTIONS-*}.</li>
     * </ol>
     * If any referenced SST cannot be made durable, the method aborts before publishing
     * {@code MANIFEST}/{@code CURRENT} and returns {@code false}, leaving the previous durable
     * generation intact.
     */
    boolean uploadMetadataSnapshot(CloudStorageProvider provider, String dbName,
                                   MetadataSnapshot snapshot) {
        String dbDir = snapshot.getDbDir();
        String tempDir = snapshot.getTempDir();

        // (0) Defense in depth: a snapshot without both CURRENT and MANIFEST is degenerate. If we
        // proceeded, steps (3)/(4) would upload nothing (both null-guarded) but step (5) would still
        // run pruneRemoteMetadata, which deletes remote MANIFEST-* objects not in the (empty) keep
        // set — destroying the previously-valid remote MANIFEST while the remote CURRENT still
        // points at it, corrupting the cloud copy irrecoverably. Treat it as a failed publish.
        if (snapshot.getManifestFileName() == null || snapshot.getCurrentFileName() == null) {
            log.warn("Cloud metadata-sync: snapshot for db={} missing {} — skipping publish/prune "
                     + "to avoid deleting the valid remote MANIFEST", dbName,
                     snapshot.getManifestFileName() == null ? "MANIFEST" : "CURRENT");
            return false;
        }

        // (1) Confirm every manifest-referenced SST is present in cloud.
        if (!ensureSnapshotSstsUploaded(provider, dbName, snapshot)) {
            log.warn("Cloud metadata-sync: SST set not fully durable, holding metadata publish "
                     + "for db={}", dbName);
            return false;
        }

        try {
            // (2) OPTIONS-* then MANIFEST-<n>.
            for (String optionsName : snapshot.getOptionsFileNames()) {
                uploadMetaFile(provider, dbDir, tempDir, optionsName);
            }
            uploadMetaFile(provider, dbDir, tempDir, snapshot.getManifestFileName());
            // (3) CURRENT last — the atomic pointer publish.
            uploadMetaFile(provider, dbDir, tempDir, snapshot.getCurrentFileName());
        } catch (IOException e) {
            log.warn("Cloud metadata-sync: failed to publish metadata for db={}: {}",
                     dbName, e.getMessage());
            return false;
        }

        // (4) Prune superseded remote metadata now that the new CURRENT is durable.
        pruneRemoteMetadata(provider, dbDir, snapshot);
        log.debug("Cloud metadata-sync published: db={}, manifest={}, options={}",
                  dbName, snapshot.getManifestFileName(), snapshot.getOptionsFileNames().size());
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
            long uploadEpoch = syncTracker.currentEpoch(dbName);
            if (syncTracker.isConfirmed(dbName, realPath)) {
                continue;
            }
            try {
                if (provider.fileExists(remoteKey)) {
                    if (!syncTracker.markConfirmedIfEpoch(dbName, realPath, uploadEpoch)) {
                        allDurable = false;
                        log.warn("Cloud metadata-sync: stale SST confirmation dropped by epoch guard: "
                                 + "db={}, key={}, epoch={}", dbName, remoteKey, uploadEpoch);
                    }
                    continue;
                }
                provider.uploadFile(joinPath(tempDir, sstName), remoteKey);
                if (!syncTracker.markConfirmedIfEpoch(dbName, realPath, uploadEpoch)) {
                    allDurable = false;
                    log.warn("Cloud metadata-sync: stale SST upload confirmation dropped by epoch "
                             + "guard: db={}, key={}, epoch={}", dbName, remoteKey, uploadEpoch);
                }
            } catch (Exception e) {
                allDurable = false;
                log.warn("Cloud metadata-sync: failed to confirm SST db={}, key={}, reason={}",
                         dbName, remoteKey, e.getMessage());
                if (retryQueue != null && Files.exists(Paths.get(realPath))
                    && !retryQueue.isRetryPending(dbName, remoteKey)) {
                    // Epoch captured now so a successful retry confirms via the epoch-guarded
                    // callback (plain submit() uses epoch 0 → dropped). Skipped when a retry for
                    // this object is already queued, so every failed publish does not stack
                    // another retry chain.
                    retryQueue.submit(dbName, null, realPath, remoteKey, uploadEpoch, e);
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
     * Deletes remote {@code MANIFEST-*}/{@code OPTIONS-*} objects for this DB that are not part of
     * the just-published snapshot, bounding remote metadata growth. {@code CURRENT} and
     * {@code *.sst} are never pruned here (SST lifecycle is the delete guard's). WAL {@code *.log}
     * files are never mirrored to cloud (the snapshot only uploads CURRENT/MANIFEST/OPTIONS/SST),
     * so there is nothing to prune for them.
     */
    private void pruneRemoteMetadata(CloudStorageProvider provider, String dbDir,
                                     MetadataSnapshot snapshot) {
        Set<String> keep = new HashSet<>();
        if (snapshot.getManifestFileName() != null) {
            keep.add(snapshot.getManifestFileName());
        }
        keep.addAll(snapshot.getOptionsFileNames());
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
            boolean isSupersededMeta = (base.startsWith("MANIFEST-") || base.startsWith("OPTIONS-"))
                                       && !keep.contains(base);
            if (!isSupersededMeta) {
                continue;
            }
            try {
                provider.deleteFile(remoteKey);
                log.debug("Cloud metadata-sync prune: deleted superseded {}", remoteKey);
            } catch (IOException e) {
                log.debug("Cloud metadata-sync prune: delete failed for {}: {}", remoteKey, e.getMessage());
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
     * Converts an absolute local file path to a remote key by stripping the matching configured
     * data-root prefix.
     *
     * <pre>
     *   dataRoot = /hugegraph-store/storage
     *   filePath = /hugegraph-store/storage/hgstore-metadata/000008.sst
     *   result   = hgstore-metadata/000008.sst
     * </pre>
     *
     * If {@code filePath} does not start with any configured data roots the leading slash is simply
     * stripped so the key is still valid (though possibly not ideally formatted).
     */
    String toRelativeKey(String filePath) {
        String matchingRoot = findMatchingDataRoot(filePath);
        // Use Path.startsWith for boundary-safe matching so /data/store1 does not match
        // /data/store10/... the way a raw String.startsWith would.
        Path fileNormalized = Paths.get(filePath).toAbsolutePath().normalize();
        Path rootPath = Paths.get(matchingRoot);
        String relative;
        if (fileNormalized.startsWith(rootPath)) {
            Path relPath = rootPath.relativize(fileNormalized);
            relative = relPath.toString();
            return withStoreScope(relative);
        }
        // Fallback: strip any leading slash so the key does not start with '/'.
        String normalized = fileNormalized.toString();
        relative = normalized.startsWith("/") ? normalized.substring(1) : normalized;
        return withStoreScope(relative);
    }

    /**
     * STALE-DATA GUARD: if a tombstone exists (written by {@link #onDBDeleteBegin} outside the data
     * prefix), the previous generation at this prefix was destroyed but its cloud objects may not
     * have been fully removed. Purge them and the tombstone before the opening DB can hydrate from,
     * or upload into, the prefix; otherwise the stale objects would resurrect deleted data, and the
     * leftover tombstone would make a later open purge the new DB's live objects. The open is
     * blocked if either step fails.
     *
     * <p>If the tombstone check itself fails, the generation state is unknown. With
     * {@code requireConsistentLocal} (startup hydration) the open is blocked unless local metadata
     * is self-consistent; without it (no hydration, so nothing remote is read) the open proceeds.
     *
     * @return {@code true} if a tombstone was found and the prefix purged (do not hydrate)
     */
    private boolean purgeTombstonedPrefix(CloudStorageProvider provider, String dbName, Path root,
                                          String prefix, boolean requireConsistentLocal) {
        String tombstoneKey = prefix + DB_TOMBSTONE_SUFFIX;
        boolean tombstoned;
        try {
            tombstoned = provider.fileExists(tombstoneKey);
        } catch (IOException e) {
            // Tombstone check itself failed — cannot safely determine generation, and cloud cannot
            // validate the DB. Proceed from local state ONLY if it is self-consistent (valid
            // CURRENT→MANIFEST); orphan SSTs without a valid lineage must not silently boot.
            if (requireConsistentLocal && hasInconsistentLocalMetadata(root)) {
                throw new IllegalStateException(
                        String.format("Cloud pre-hydration: tombstone check failed for db=%s and "
                                      + "local metadata is not self-consistent (no valid "
                                      + "CURRENT→MANIFEST lineage) — cannot safely open DB without "
                                      + "knowing generation state: %s", dbName, e.getMessage()), e);
            }
            log.warn("Cloud open: tombstone check failed for db={}, proceeding with local state: {}",
                     dbName, e.getMessage());
            return false;
        }
        if (!tombstoned) {
            return false;
        }
        log.warn("Cloud open: tombstone found for db={} — previous generation was deleted. "
                 + "Purging stale remote objects; hydration is skipped.", dbName);
        Object lock = pendingDeleteLocks.computeIfAbsent(prefix, k -> new Object());
        synchronized (lock) {
            if (!purgeRemotePrefix(provider, dbName, prefix)) {
                // Leave the tombstone in place so the next open attempts the purge again and does
                // not accidentally hydrate stale objects.
                throw new IllegalStateException(
                        String.format("Cloud open: prefix purge failed for db=%s — tombstone "
                                      + "preserved to guard against stale-data re-hydration. "
                                      + "DB open is blocked.", dbName));
            }
            // The tombstone lives outside the slash-terminated purged prefix, so purgeRemotePrefix
            // never touches it — it must be deleted explicitly. If that fails the open stays
            // blocked: the next open re-purges the (already clean) prefix and retries the delete.
            if (!deleteTombstone(provider, dbName, prefix)) {
                throw new IllegalStateException(
                        String.format("Cloud open: tombstone deletion failed for db=%s after a "
                                      + "successful prefix purge — opening now would let a later "
                                      + "open purge this DB's new objects. DB open is blocked "
                                      + "until tombstone '%s' is removed.", dbName, tombstoneKey));
            }
        }
        log.info("Cloud open: tombstone cleaned up: db={}", dbName);
        return true;
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

        // Snapshot whether a local CURRENT existed BEFORE hydration. Only then can the download loop
        // (which skips already-present files) leave a stale pointer in place; if CURRENT was absent,
        // the loop fetches the authoritative remote copy and no reconciliation is needed. Capturing
        // it here also avoids a redundant compare-download of a CURRENT we just hydrated.
        boolean localCurrentPreexisted = Files.exists(root.resolve("CURRENT"));

        String prefix = dbPrefix(dbPath);

        if (purgeTombstonedPrefix(provider, dbName, root, prefix, true)) {
            return;
        }

        List<String> remoteFiles = listRemoteKeys(provider, prefix, root);
        if (remoteFiles.isEmpty()) {
            log.debug("Cloud pre-hydration skipped: no remote files for db={} prefix={}",
                      dbName, prefix);
            return;
        }

        // Stale hydration temp files were already swept at the start of onDBOpening.

        int downloaded = 0;
        for (String remoteKey : remoteFiles) {
            Path localPath;
            try {
                localPath = resolveLocalPath(remoteKey, dbPath);
            } catch (IllegalArgumentException e) {
                // Key doesn't belong to the current store scope (e.g. left over from a previous
                // node identity after an IP change). Skip it rather than aborting hydration.
                log.warn("Cloud pre-hydration: skipping remote key outside current scope "
                         + "(db={}, key={}): {}", dbName, remoteKey, e.getMessage());
                continue;
            }
            if (Files.exists(localPath)) {
                continue;
            }
            try {
                downloadAtomically(provider, remoteKey, localPath);
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
                Path localSst;
                try {
                    localSst = resolveLocalPath(remoteKey, dbPath);
                } catch (IllegalArgumentException e) {
                    continue; // scope mismatch already warned above
                }
                syncTracker.markConfirmed(dbName, localSst.toString());
                confirmed++;
            }
        }
        if (confirmed > 0) {
            log.info("Seeded sync-tracker bitmap with {} confirmed files from remote listing: "
                     + "db={}", confirmed, dbName);
        }

        // Converge the CURRENT pointer to the newest generation. The download loop above skips any
        // file already present locally, which is correct for immutable metadata/SSTs but would
        // leave a STALE local CURRENT in place when cloud holds a newer generation — a silent
        // rollback. Only relevant when CURRENT pre-existed (otherwise the loop already fetched the
        // authoritative remote copy). Reconcile explicitly (generation-compared) before validating.
        if (localCurrentPreexisted) {
            reconcileCurrentPointer(provider, dbName, root);
        }

        verifyMetadataConsistency(dbName, root);
    }

    /**
     * Reconciles the local {@code CURRENT} pointer with the remote one when both exist.
     *
     * <p>{@link #preHydrateDbFiles}'s download loop skips any file already present locally. That is
     * correct for RocksDB's immutable, number-addressed metadata ({@code MANIFEST-<n>},
     * {@code OPTIONS-<n>}) and SSTs — a same-named local copy is byte-identical to the remote one.
     * It is WRONG for {@code CURRENT}, whose name is fixed while its content advances every
     * generation. A stale local {@code CURRENT} (e.g. left by a partial disk rollback) while cloud
     * holds a newer generation would make the node silently open the older generation.
     *
     * <p>We compare generations (parsed from the {@code MANIFEST-<n>} each CURRENT references) and
     * adopt the remote pointer only when it is strictly newer. When the local generation is newer
     * (recent writes not yet mirrored) the local pointer wins, so hydration never rolls the DB back
     * to an older cloud generation. The newer generation's MANIFEST/OPTIONS/SSTs carry new numbers
     * and were therefore already fetched by the main download loop.
     */
    private void reconcileCurrentPointer(CloudStorageProvider provider, String dbName,
                                         Path root) {
        Path localCurrent = root.resolve("CURRENT");
        if (!Files.exists(localCurrent)) {
            // No local pointer to reconcile — the loop already fetched the remote CURRENT if any.
            return;
        }
        String currentRemoteKey = toRelativeKey(localCurrent.toString());
        Path remoteCurrent = hydrationTempFor(localCurrent);
        try {
            if (!provider.fileExists(currentRemoteKey)) {
                return; // no remote CURRENT to compare against
            }
            provider.downloadFile(currentRemoteKey, remoteCurrent.toString());
            String remoteManifest = Files.readString(remoteCurrent).trim();
            String localManifest = Files.readString(localCurrent).trim();
            long remoteGen = parseManifestGeneration(remoteManifest);
            long localGen = parseManifestGeneration(localManifest);
            if (remoteGen < 0L || localGen < 0L) {
                // An unparseable pointer — do not guess. Keep local; verifyMetadataConsistency and
                // RocksDB's own open still guard against a genuinely broken CURRENT.
                log.warn("Cloud pre-hydration: unparseable CURRENT during reconciliation for db={} "
                         + "(local='{}', remote='{}') — keeping local pointer",
                         dbName, localManifest, remoteManifest);
                return;
            }
            if (remoteGen > localGen) {
                moveReplacing(remoteCurrent, localCurrent);
                log.warn("Cloud pre-hydration: local CURRENT for db={} pointed at {} (gen {}) but "
                         + "cloud holds {} (gen {}); adopted the newer remote generation to avoid a "
                         + "silent rollback.", dbName, localManifest, localGen, remoteManifest,
                         remoteGen);
            } else if (remoteGen < localGen) {
                log.debug("Cloud pre-hydration: keeping newer local CURRENT (gen {} > remote gen "
                          + "{}) for db={}", localGen, remoteGen, dbName);
            }
        } catch (IOException e) {
            log.warn("Cloud pre-hydration: CURRENT reconciliation failed for db={}: {} — keeping "
                     + "local pointer", dbName, e.getMessage());
        } finally {
            deleteIfExistsQuietly(remoteCurrent, "CURRENT reconciliation temp cleanup");
        }
    }

    /**
     * Downloads {@code remoteKey} to a unique sibling temp file, then atomically moves it to
     * {@code dest}. The per-thread/per-attempt suffix prevents two concurrent restorers from
     * writing the same temp file (which would interleave into a corrupt SST), and REPLACE_EXISTING
     * makes a late second mover a harmless idempotent overwrite. A crash mid-download never leaves
     * RocksDB reading a truncated file at the expected path; the temp file is removed on failure.
     */
    private static void downloadAtomically(CloudStorageProvider provider, String remoteKey,
                                           Path dest) throws IOException {
        Files.createDirectories(dest.getParent());
        Path tmp = hydrationTempFor(dest);
        try {
            provider.downloadFile(remoteKey, tmp.toString());
            moveReplacing(tmp, dest);
        } finally {
            deleteIfExistsQuietly(tmp, "hydration temp cleanup");
        }
    }

    /** Unique sibling temp path for {@code dest}, swept by {@link #preHydrateDbFiles} on startup. */
    private static Path hydrationTempFor(Path dest) {
        return dest.resolveSibling(dest.getFileName() + HYDRATION_TEMP_MARKER
                                   + Thread.currentThread().getId() + "-" + System.nanoTime());
    }

    /** Atomically replaces {@code dest} with {@code src}, falling back to a non-atomic replace. */
    private static void moveReplacing(Path src, Path dest) throws IOException {
        try {
            Files.move(src, dest, StandardCopyOption.ATOMIC_MOVE,
                       StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            // Cross-filesystem / FS without atomic-move support: still a replace.
            Files.move(src, dest, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Parses the RocksDB generation encoded in a {@code MANIFEST-<n>} file name, or {@code -1} if
     * {@code manifestName} is null / not a well-formed manifest name.
     */
    private static long parseManifestGeneration(String manifestName) {
        if (manifestName == null || !manifestName.startsWith("MANIFEST-")) {
            return -1L;
        }
        try {
            return Long.parseLong(manifestName.substring("MANIFEST-".length()));
        } catch (NumberFormatException e) {
            return -1L;
        }
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

    private List<String> listRemoteKeys(CloudStorageProvider provider, String prefix,
                                        Path localRoot) {
        try {
            return provider.listFiles(prefix.endsWith("/") ? prefix : prefix + "/");
        } catch (IOException e) {
            // Cloud listing failed during startup hydration; cloud cannot validate the DB. Fall back
            // to local state ONLY if the local metadata is self-consistent (valid CURRENT→MANIFEST).
            // A weaker "any SST present" check would let a stale/partial directory (orphan SSTs, no
            // valid CURRENT lineage) boot silently — risking a rollback/partial open. Require the
            // stronger predicate; otherwise fail startup loudly.
            if (hasInconsistentLocalMetadata(localRoot)) {
                throw new IllegalStateException(
                        String.format("Cloud pre-hydration list failed for prefix=%s and local "
                                      + "metadata is not self-consistent (no valid CURRENT→MANIFEST "
                                      + "lineage) — refusing to open a partial/unvalidated DB while "
                                      + "cloud is unreachable: %s", prefix, e.getMessage()), e);
            }
            log.warn("Cloud pre-hydration list failed for prefix={}, proceeding with self-consistent "
                     + "local state (valid CURRENT→MANIFEST present): {}", prefix, e.getMessage());
            return java.util.Collections.emptyList();
        }
    }

    /**
     * Returns {@code true} when the local DB metadata is NOT self-consistent — i.e. there is no
     * usable {@code CURRENT} → {@code MANIFEST-*} lineage: {@code CURRENT} is absent, empty, or
     * unreadable, or it references a {@code MANIFEST-*} that is missing locally. Returns
     * {@code false} only when {@code CURRENT} points at a {@code MANIFEST-*} that exists locally
     * (a valid lineage).
     *
     * <p>This is a stronger predicate than a "local SST exists" check — orphan SSTs without a valid
     * CURRENT→MANIFEST lineage (a partial/rolled-back directory) count as inconsistent. Callers use
     * it to refuse opening a DB from local state when cloud is unreachable and cannot validate it:
     * an inconsistent directory ({@code true}) must not silently boot as a partial/rolled-back DB.
     */
    private static boolean hasInconsistentLocalMetadata(Path root) {
        Path current = root.resolve("CURRENT");
        if (!Files.exists(current)) {
            return true;
        }
        try {
            String manifest = Files.readString(current).trim();
            return manifest.isEmpty() || !Files.exists(root.resolve(manifest));
        } catch (IOException e) {
            return true;
        }
    }

    private String dbPrefix(String dbPath) {
        String relative = toRelativeKey(dbPath);
        return relative.endsWith("/") ? relative.substring(0, relative.length() - 1) : relative;
    }

    /**
     * Resolves the download destination
     * under the data root that actually contains {@code dbPath}, rather than always using
     * the primary root. Used by {@link #preHydrateDbFiles} so that files from a secondary
     * data root are written back to the correct root on restore.
     */
    private Path resolveLocalPath(String remoteKey, String dbPath) {
        String relative = stripStoreScope(remoteKey);
        String matchingRoot = findMatchingDataRoot(
                Paths.get(dbPath).toAbsolutePath().normalize().toString());
        Path root = Paths.get(matchingRoot);
        Path local = root.resolve(relative).normalize();
        if (!local.startsWith(root)) {
            throw new IllegalArgumentException("Invalid remote key outside configured data roots: "
                                               + remoteKey);
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
        // Atomically decide admission AND update the timestamp under the per-key lock of the
        // ConcurrentHashMap. A non-atomic get-then-put lets two concurrent read-misses both observe
        // no/expired entry and both get admitted, defeating the throttle and triggering redundant
        // live-set scans + cloud checks under a read storm. compute() runs the remap exactly once
        // per key with mutual exclusion, so only the first caller in a window flips it to admitted.
        boolean[] admitted = {false};
        readMissAttemptTs.compute(guardKey, (k, prev) -> {
            if (prev == null || now - prev >= readMissGuardWindowMs) {
                admitted[0] = true;
                return now;              // open a fresh window for this caller
            }
            return prev;                 // keep the existing window; do not extend it
        });
        if (!admitted[0]) {
            log.debug("Skip read-miss hydration due to guard window: db={}, table={}, nowMs={}",
                      dbName, table, now);
        }
        return admitted[0];
    }
}
