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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import lombok.Getter;

import lombok.Setter;

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
public class CloudStorageEventListener implements RocksdbChangedListener {

    /** Absolute, normalised paths of configured store data roots (one per configured partition root). */
    private final String primaryDataRoot;
    private final List<String> allDataRoots;

    /** Optional per-store namespace prefix prepended to every remote cloud key. */
    private final String storeScopePrefix;

    private static final long DEFAULT_READ_MISS_GUARD_WINDOW_MS = 3000L;

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

    /** Tracks which SST files are confirmed present in cloud (per-DB Roaring bitmap). */
    private final CloudSyncTracker syncTracker;

    /**
     * When {@code > 0}, {@link #onTableFileCreated} slows the RocksDB flush/compaction thread while
     * the pending-upload backlog exceeds this watermark, so ingestion cannot outrun the cloud
     * mirror. The backlog is the executor's queued/active uploads + the retry queue's in-flight
     * retries + a bounded DLQ enqueue rate (see {@link #dlqEnqueueRateBacklog()}). {@code 0}
     * disables backpressure.
     */
    private final int backpressureHighWatermark;


    /** Upper bound on how long a single {@link #onTableFileCreated} call will block for backpressure. */
    private static final long BACKPRESSURE_MAX_WAIT_MS = 30_000L;
    private static final long BACKPRESSURE_POLL_MS = 50L;

    /**
     * Window over which the DLQ enqueue rate (uploads that exhausted their retries and became
     * local-only) is measured for backpressure. The count of DLQ enqueues observed in the trailing
     * window is added — capped at {@link #backpressureHighWatermark} — to the backpressure backlog,
     * so a sustained cloud outage that keeps pushing uploads to the DLQ throttles ingestion, while a
     * static post-recovery DLQ (rate 0) does not.
     */
    private static final long DLQ_ENQUEUE_RATE_WINDOW_MS = 1_000L;

    /** Guards the DLQ enqueue-rate sample below (touched from every backpressure poll). */
    private final Object dlqRateLock = new Object();
    /** Wall-clock time of the last DLQ enqueue-rate sample; {@code 0} until first primed. */
    private long lastDlqRateSampleMs = 0L;
    /** {@link CloudUploadRetryQueue#getDlqEnqueuedTotal()} captured at the last sample. */
    private long lastDlqEnqueuedTotalAtSample = 0L;
    /** Cached bounded DLQ enqueue-rate contribution to the backpressure backlog. */
    private int dlqRateBacklogContribution = 0;

    /**
     * Hard health flag for pending-delete marker durability. Flips to {@code false} when a marker
     * cannot be durably persisted (write + fsync) — a state in which a DB delete during a
     * provider-unavailable window may be unguarded against re-hydration after a crash — and back to
     * {@code true} once a marker is durably persisted again. Surfaced via
     * {@link CloudStorageMetricsConst#DELETE_MARKER_HEALTHY}. {@code volatile}: written from delete
     * callbacks, read from metric-scrape threads.
     * -- GETTER --
     *  Whether pending-delete marker persistence is currently healthy.
     *  indicates a
     *  marker could not be durably written, so a delete during a provider-unavailable window may not
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
    /**
     * Backing field for {@link #sharedUploadExecutor()}. Not {@code final}: it is recreated on
     * demand after {@link #shutdownSharedUploadExecutor} clears it, so a Spring context restart in
     * the same JVM binds new listeners to a live executor instead of a TERMINATED one (which would
     * reject every upload via AbortPolicy and silently divert all SSTs to the DLQ).
     */
    private static ThreadPoolExecutor sharedUploadExecutor;
    private final ThreadPoolExecutor uploadExecutor;

    /**
     * Shutdown gate for the upload subsystem. Set true at the start of
     * {@link #shutdownSharedUploadExecutor} so in-flight upload tasks draining during shutdown do
     * NOT schedule new trailing metadata syncs or resurrect the (about-to-be-torn-down)
     * {@link #metadataSyncScheduler}. Reset to false when a fresh executor is (re)created for a
     * Spring context restart in the same JVM.
     */
    private static volatile boolean uploadSubsystemShuttingDown = false;

    /** Returns the shared upload executor, (re)creating it if absent or already shut down. */
    private static synchronized ThreadPoolExecutor sharedUploadExecutor() {
        if (sharedUploadExecutor == null || sharedUploadExecutor.isShutdown()) {
            // A fresh executor means a new lifecycle (e.g. context restart) — reopen the gate.
            uploadSubsystemShuttingDown = false;
            sharedUploadExecutor = new ThreadPoolExecutor(
                    ASYNC_UPLOAD_THREADS,
                    ASYNC_UPLOAD_THREADS,
                    60L,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(ASYNC_UPLOAD_QUEUE_CAPACITY),
                    newUploadThreadFactory(),
                    new ThreadPoolExecutor.AbortPolicy());
        }
        return sharedUploadExecutor;
    }

    // -----------------------------------------------------------------------
    // Metadata-sync debounce
    // -----------------------------------------------------------------------
    // onTableFileCreated fires once per flushed/compacted SST; syncing metadata inline on every
    // one triggers a full RocksDB checkpoint + S3 list/prune per SST. These fields coalesce those
    // high-frequency syncs into at most one per debounce window per DB, with a trailing sync so
    // the final state is always eventually published even if writes stop mid-window. Event-driven
    // callers that need an immediate publish (delete guard, onCompacted, onDBCreated) still call
    // syncMetadataSnapshotInline directly and are NOT debounced.

    /** Default debounce window for the post-upload metadata sync. */
    private static final long DEFAULT_METADATA_SYNC_DEBOUNCE_MS = 1_000L;

    /**
     * Single shared scheduler for trailing (deferred) metadata syncs. Not {@code final}: like the
     * upload executor it is recreated on demand after {@link #shutdownSharedUploadExecutor} clears
     * it, so a Spring context restart in the same JVM gets a live scheduler. Otherwise every
     * post-restart trailing sync would be rejected and fall back to an inline publish (a checkpoint
     * per SST), defeating debouncing exactly when a burst is most likely.
     */
    private static ScheduledExecutorService metadataSyncScheduler;

    /**
     * Returns the metadata-sync scheduler, (re)creating it if absent or already shut down.
     *
     * <p>During upload-subsystem shutdown it must NOT resurrect the scheduler — otherwise an
     * in-flight upload task draining after {@link #shutdownSharedUploadExecutor} tore the scheduler
     * down would create a brand-new one and schedule a trailing sync that fires after provider
     * teardown, leaking the scheduler across a context stop/start. Returns the current field
     * (possibly {@code null}) in that case; callers must tolerate a {@code null}.
     */
    private static synchronized ScheduledExecutorService metadataSyncScheduler() {
        if (uploadSubsystemShuttingDown) {
            return metadataSyncScheduler;
        }
        if (metadataSyncScheduler == null || metadataSyncScheduler.isShutdown()) {
            metadataSyncScheduler = Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "cloud-metadata-sync");
                t.setDaemon(true);
                return t;
            });
        }
        return metadataSyncScheduler;
    }

    /** Effective debounce window; overridable in tests.
     * -- SETTER --
     *  Sets the debounce window (ms) for the per-SST metadata sync. Values
     *  disable
     *  debouncing (publish metadata on every SST upload, the pre-debounce behavior).
     */
    @Setter
    private volatile long metadataSyncDebounceMs;

    /**
     * Backlog bound on the debounce: the time window alone lets an unbounded number of SSTs be
     * uploaded-but-not-yet-mirrored during a heavy-ingestion burst, widening the cloud recovery
     * point (a crash + local-disk-loss in that window loses the flushed SSTs whose manifest was
     * not yet republished). Once this many uploads accumulate without a metadata publish for a DB,
     * a publish is forced immediately regardless of the time window, bounding RPO by count as well
     * as by time. {@code <= 0} disables the count bound (time-only debounce).
     */
    private static final int DEFAULT_METADATA_SYNC_MAX_UNPUBLISHED = 32;
    /**
     * -- SETTER --
     *  Sets the maximum number of uploaded-but-unmirrored SSTs tolerated before a metadata publish
     *  is forced regardless of the debounce window (bounds the cloud recovery point by count during
     *  heavy-ingestion bursts). Values
     *  disable the count bound (time-only debounce).
     */
    @Setter
    private volatile int metadataSyncMaxUnpublished;

    /** Last time a post-upload metadata sync completed, per DB (epoch millis). */
    private final Map<String, Long> lastMetadataSyncMs = new ConcurrentHashMap<>();

    /** Count of SST uploads confirmed but not yet reflected in a published manifest, per DB. */
    private final Map<String, java.util.concurrent.atomic.AtomicInteger> unpublishedUploads =
            new ConcurrentHashMap<>();

    /** DBs with a trailing metadata sync already scheduled (coalescing guard). */
    private final Set<String> pendingMetadataSync = ConcurrentHashMap.newKeySet();

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

    /**
     * Convenience constructor with startup hydration enabled and default read-miss guard window.
     *
     * @param dataRoots configured store data roots (typically parsed from comma-separated
     *                  {@code app.data-path})
     */
    public CloudStorageEventListener(List<String> dataRoots) {
        this(dataRoots, true, DEFAULT_READ_MISS_GUARD_WINDOW_MS, null);
    }

    /**
     * @param dataRoots configured store data roots
     */
    public CloudStorageEventListener(List<String> dataRoots,
                                     boolean startupHydrationEnabled) {
        this(dataRoots, startupHydrationEnabled, DEFAULT_READ_MISS_GUARD_WINDOW_MS, null);
    }

    /**
     * @param dataRoots configured store data roots
     * @param readMissGuardWindowMs guard window in ms for repeated read-miss hydration attempts
     *                              for the same db/table pair (cloud.storage.read-miss-guard-window-ms)
     */
    public CloudStorageEventListener(List<String> dataRoots,
                                     boolean startupHydrationEnabled,
                                     long readMissGuardWindowMs) {
        this(dataRoots, startupHydrationEnabled, readMissGuardWindowMs, null);
    }

    /**
     * @param dataRoots configured store data roots
     * @param retryQueue optional {@link CloudUploadRetryQueue}; when non-null, upload failures
     *                   are retried asynchronously and eventually moved to the dead-letter queue.
     *                   Pass {@code null} to disable retries (failures are only logged).
     */
    public CloudStorageEventListener(List<String> dataRoots,
                                     boolean startupHydrationEnabled,
                                     long readMissGuardWindowMs,
                                     CloudUploadRetryQueue retryQueue) {
        this(dataRoots, startupHydrationEnabled, readMissGuardWindowMs, retryQueue,
             new CloudSyncTracker(), 0);
    }

    /**
     * @param dataRoots configured store data roots
     * @param syncTracker tracks SST files confirmed present in cloud; the delete guard uses it
     *                    to avoid deleting a superseded object before replacements are durable.
     *                    Must be shared with the retry queue.
     * @param backpressureHighWatermark {@code > 0} to slow ingestion while pending-upload backlog
     *                                  exceeds this value; {@code 0} disables backpressure.
     */
    public CloudStorageEventListener(List<String> dataRoots,
                                     boolean startupHydrationEnabled,
                                     long readMissGuardWindowMs,
                                     CloudUploadRetryQueue retryQueue,
                                     CloudSyncTracker syncTracker,
                                     int backpressureHighWatermark) {
        this(dataRoots, startupHydrationEnabled, readMissGuardWindowMs, retryQueue, syncTracker,
             backpressureHighWatermark, null);
    }

    /**
     * Multi-root constructor for comma-separated app.data-path configuration.
     *
     * @param dataRoots configured store data roots (absolute, normalised)
     * @param storeScopePrefix optional per-store key prefix to isolate cloud objects
     */
    public CloudStorageEventListener(List<String> dataRoots,
                                     boolean startupHydrationEnabled,
                                     long readMissGuardWindowMs,
                                     CloudUploadRetryQueue retryQueue,
                                     CloudSyncTracker syncTracker,
                                     int backpressureHighWatermark,
                                     String storeScopePrefix) {
        this(dataRoots, startupHydrationEnabled, readMissGuardWindowMs, retryQueue, syncTracker,
             backpressureHighWatermark, storeScopePrefix, Tuning.defaults());
    }

    /**
     * Fully-parameterised constructor. Prefer this in production wiring: passing {@link Tuning}
     * makes the listener completely configured the moment it is constructed, so there is no window
     * in which a registered listener can be observed with the tuning setters not yet applied. The
     * equivalent {@code set*} methods remain for tests and runtime overrides.
     *
     * @param storeScopePrefix optional per-store key prefix to isolate cloud objects
     * @param tuning debounce / backlog-bound tuning (never {@code null}; use
     *               {@link Tuning#defaults()} for defaults)
     */
    public CloudStorageEventListener(List<String> dataRoots,
                                     boolean startupHydrationEnabled,
                                     long readMissGuardWindowMs,
                                     CloudUploadRetryQueue retryQueue,
                                     CloudSyncTracker syncTracker,
                                     int backpressureHighWatermark,
                                     String storeScopePrefix,
                                     Tuning tuning) {
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
        this.startupHydrationEnabled = startupHydrationEnabled;
        this.readMissGuardWindowMs = Math.max(0L, readMissGuardWindowMs);
        this.readMissAttemptTs = new ConcurrentHashMap<>();
        this.retryQueue = retryQueue;
        this.syncTracker = syncTracker != null ? syncTracker : new CloudSyncTracker();
        this.backpressureHighWatermark = Math.max(0, backpressureHighWatermark);
        this.storeScopePrefix = normaliseKeyPrefix(storeScopePrefix);
        this.uploadExecutor = sharedUploadExecutor();

        Tuning t = tuning != null ? tuning : Tuning.defaults();
        this.metadataSyncDebounceMs = t.metadataSyncDebounceMs;
        this.metadataSyncMaxUnpublished = t.metadataSyncMaxUnpublished;

        // A crash may leave local pending-delete markers whose remote cleanup never completed.
        // Kick off bounded async retries for each so stale remote data is eventually purged even if
        // the affected DB is never re-opened (an open would otherwise be the only trigger).
        processPendingDeleteMarkersOnStartup();
        // Truncate purge intent is also crash-durable via local markers.
        processPendingTruncateMarkersOnStartup();
    }

    /**
     * Immutable tuning bundle for the listener's post-upload metadata sync behaviour. Grouping these
     * into one params object keeps the constructor readable and lets the listener be fully configured
     * at construction time. Build with {@link #builder()}; unset knobs fall back to the documented
     * defaults.
     */
    public static final class Tuning {

        private final long metadataSyncDebounceMs;
        private final int metadataSyncMaxUnpublished;

        private Tuning(Builder b) {
            this.metadataSyncDebounceMs = b.metadataSyncDebounceMs;
            this.metadataSyncMaxUnpublished = b.metadataSyncMaxUnpublished;
        }

        /** Tuning with all defaults (equivalent to constructing the listener with no {@code set*}). */
        public static Tuning defaults() {
            return builder().build();
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {

            private long metadataSyncDebounceMs = DEFAULT_METADATA_SYNC_DEBOUNCE_MS;
            private int metadataSyncMaxUnpublished = DEFAULT_METADATA_SYNC_MAX_UNPUBLISHED;

            /** @see CloudStorageEventListener#setMetadataSyncDebounceMs(long) */
            public Builder metadataSyncDebounceMs(long ms) {
                this.metadataSyncDebounceMs = ms;
                return this;
            }

            /** @see CloudStorageEventListener#setMetadataSyncMaxUnpublished(int) */
            public Builder metadataSyncMaxUnpublished(int maxUnpublished) {
                this.metadataSyncMaxUnpublished = maxUnpublished;
                return this;
            }

            public Tuning build() {
                return new Tuning(this);
            }
        }
    }

    private static ThreadFactory newUploadThreadFactory() {
        return r -> {
            Thread t = new Thread(r, "cloud-upload-dispatch");
            t.setDaemon(true);
            return t;
        };
    }

    /**
     * Shuts down the shared SST upload executor, waiting up to {@code timeout} for in-flight
     * uploads to complete. Called from {@link org.apache.hugegraph.store.node.AppConfig#onDestroy()}
     * so uploads started before shutdown have a chance to finish before the JVM exits.
     */
    public static void shutdownSharedUploadExecutor(long timeout,
                                                    java.util.concurrent.TimeUnit unit) {
        ThreadPoolExecutor executor;
        synchronized (CloudStorageEventListener.class) {
            // Raise the gate BEFORE draining: in-flight upload tasks that finish during the drain
            // call requestDebouncedMetadataSync(), which now short-circuits so it cannot resurrect
            // the metadata-sync scheduler we are about to tear down.
            uploadSubsystemShuttingDown = true;
            executor = sharedUploadExecutor;
            // Clear the executor field so a later listener construction (Spring context restart in
            // the same JVM) lazily recreates a live instance rather than reusing a terminated one.
            sharedUploadExecutor = null;
        }
        // Drain the upload executor FIRST, while the metadata-sync scheduler is still alive (so any
        // inline metadata publish an in-flight task performs can still run). The gate prevents new
        // trailing syncs from being scheduled.
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(timeout, unit)) {
                    log.warn("CloudStorageEventListener: shared upload executor did not terminate "
                             + "within {}{}; forcing shutdown", timeout, unit);
                    handoffDroppedUploadTasks(executor.shutdownNow());
                }
            } catch (InterruptedException e) {
                handoffDroppedUploadTasks(executor.shutdownNow());
                Thread.currentThread().interrupt();
            }
        }
        // Only AFTER the upload executor has fully drained do we tear down the metadata-sync
        // scheduler — no in-flight upload task can recreate it now (executor is terminated and the
        // gate is up).
        ScheduledExecutorService scheduler;
        synchronized (CloudStorageEventListener.class) {
            scheduler = metadataSyncScheduler;
            metadataSyncScheduler = null;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    /**
     * Routes upload tasks discarded by a forced {@code shutdownNow()} into their retry queue / DLQ
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
                    log.warn("Failed to hand off a dropped upload task to retry/DLQ: {}",
                             e.getMessage());
                }
            }
        }
        if (handed > 0) {
            log.warn("CloudStorageEventListener: handed off {} queued upload task(s) to retry/DLQ "
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
    public void onDBOpening(String dbName, String dbPath) {
        if (!startupHydrationEnabled) {
            return;
        }
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        // Pending-delete guard: a prior delete's remote cleanup may not be confirmed (provider was
        // down, or the purge failed and is being retried). Checked BEFORE the provider-null return
        // so a re-create during a provider outage still cannot re-hydrate the deleted generation.
        String prefix = dbPrefix(dbPath);
        if (hasPendingDeleteMarker(prefix)) {
            if (provider != null) {
                // Finish cleanup inline (under the per-prefix lock) BEFORE the open proceeds, so the
                // re-created DB's later uploads cannot be purged by the async retry. If it succeeds
                // the marker is removed and it is safe to hydrate (the prefix is now empty).
                tryCompletePendingDelete(provider, dbName, prefix);
            }
            if (hasPendingDeleteMarker(prefix)) {
                log.warn("Cloud pre-hydration skipped for db={}: remote-delete cleanup not yet "
                         + "confirmed — opening fresh and blocking re-hydration of deleted data.",
                         dbName);
                return;
            }
        }
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
                Files.createDirectories(localPath.getParent());
                // Download to a UNIQUE sibling temp file, then atomically move into place. The
                // per-thread/per-attempt suffix prevents two concurrent restorers from writing the
                // same temp file (which would interleave into a corrupt SST), and REPLACE_EXISTING
                // makes a late second mover a harmless idempotent overwrite. A crash mid-download
                // never leaves RocksDB reading a truncated SST at the expected path.
                Path tmp = localPath.resolveSibling(
                        localPath.getFileName() + ".hydrate-" + Thread.currentThread().getId()
                        + "-" + System.nanoTime());
                try {
                    provider.downloadFile(remoteKey, tmp.toString());
                    try {
                        Files.move(tmp, localPath,
                                   java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                                   java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    } catch (java.nio.file.AtomicMoveNotSupportedException ex) {
                        // Cross-filesystem / FS without atomic-move support: fall back to a
                        // non-atomic replace so restore still succeeds.
                        Files.move(tmp, localPath,
                                   java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (IOException e) {
                    deleteIfExistsQuietly(tmp, "read-miss restore temp cleanup");
                    throw e;
                }
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
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            return;
        }
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
        // guard that survives a provider-unavailable window: even if we cannot write the remote
        // tombstone or purge below, the marker blocks hydration of this DB (see onDBOpening) and
        // drives async cleanup until confirmed. It is removed once the remote purge succeeds.
        //
        // Marker durability is a hard precondition for a safe delete: if we cannot fsync it, a crash
        // could lose the guard and let stale remote SST/metadata be re-hydrated as live data. So on a
        // persistence failure we flip the health signal to degraded and HOLD delete progression
        // (throw) rather than proceeding unguarded — the caller can retry once local storage recovers.
        try {
            writePendingDeleteMarker(dbName, prefix);
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

        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            // No provider now — the local marker keeps hydration blocked and onDBDeleted will
            // schedule the retry that writes the tombstone + purges once a provider returns.
            log.warn("Cloud DB delete begin with no active provider: db={} — persisted local "
                     + "pending-delete marker to block re-hydration until cleanup completes.", dbName);
            return;
        }
        // Tombstone lives OUTSIDE the data prefix (sibling, not child) so it is not
        // accidentally deleted by purgeRemotePrefix when SST objects are still present.
        String tombstoneKey = prefix + DB_TOMBSTONE_SUFFIX;
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
        metadataSyncLocks.remove(dbName);
        lastPublishedMetadataGeneration.remove(dbName);
        truncationTimes.remove(dbName);
        truncatingDbs.remove(dbName);
        deferredSstDeletes.removeIf(t -> dbName.equals(t.dbName));
        lastMetadataSyncMs.remove(dbName);
        pendingMetadataSync.remove(dbName);
        unpublishedUploads.remove(dbName);
        // Remove per-DB meters so meter cardinality does not grow without bound as DBs churn.
        CloudStorageMetrics.unregisterDatabaseMetrics(dbName);

        String prefix = dbPrefix(dbPath);
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            // Provider unavailable: cannot purge now. The local pending-delete marker (written in
            // onDBDeleteBegin) keeps hydration blocked; schedule a bounded retry that purges once a
            // provider returns, so stale remote data cannot be resurrected.
            log.warn("Cloud DB delete with no active provider: db={} — scheduling deferred remote "
                     + "purge; local pending-delete marker guards re-hydration meanwhile.", dbName);
            scheduleDeletePurgeRetry(dbName, prefix, 1);
            return;
        }
        boolean purgeSucceeded = purgeRemotePrefix(provider, dbName, prefix);
        if (purgeSucceeded) {
            // Remote data is gone: drop the sibling tombstone and the local marker (cleanup done).
            deleteTombstoneBestEffort(provider, dbName, prefix);
            removePendingDeleteMarker(prefix);
        } else {
            // Purge failed: stale SST objects may remain. Keep the tombstone AND the local marker so
            // a future onDBOpening still blocks re-hydration, and retry the purge (bounded) — same
            // safety level as the truncate purge, instead of a single best-effort attempt.
            log.warn("Cloud DB purge failed for db={}: tombstone + local marker preserved to guard "
                     + "re-hydration; scheduling bounded retries.", dbName);
            scheduleDeletePurgeRetry(dbName, prefix, 1);
        }
    }

    /** Best-effort deletion of the sibling delete-tombstone once the prefix purge has succeeded. */
    private void deleteTombstoneBestEffort(CloudStorageProvider provider, String dbName,
                                           String prefix) {
        String tombstoneKey = prefix + DB_TOMBSTONE_SUFFIX;
        try {
            provider.deleteFile(tombstoneKey);
            log.debug("Cloud DB tombstone cleaned up: db={}, key={}", dbName, tombstoneKey);
        } catch (IOException e) {
            log.debug("Cloud DB tombstone cleanup failed (non-critical): db={}, key={}: {}",
                      dbName, tombstoneKey, e.getMessage());
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
        truncationTimes.put(dbName, System.currentTimeMillis());
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
                writePendingTruncateMarker(dbName, prefix);
            } catch (IOException e) {
                log.error("Cloud truncate marker persist failed for db={}, prefix={}: {} — "
                          + "remote purge intent is not crash-durable until the next successful "
                          + "persist.",
                          dbName, prefix, e.getMessage());
            }
            CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
            if (provider == null) {
                // Provider can be transiently unavailable during reconfiguration; keep the bounded
                // retry chain alive so stale pre-truncate objects are still purged once it returns.
                log.warn("Cloud truncate purge for db={} found no active provider; scheduling "
                         + "bounded retries for prefix={}", dbName, prefix);
                scheduleTruncatePurgeRetry(dbName, prefix);
                return;
            }
            boolean purged = purgeRemotePrefix(provider, dbName, prefix);
            if (purged) {
                removePendingTruncateMarker(prefix);
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
    /** Cap for provider-unavailable truncate-retry backoff (ms), separate from purge failures. */
    private static final long TRUNCATE_PROVIDER_UNAVAILABLE_MAX_DELAY_MS = 8_000L;

    /**
     * Retries a failed truncate purge asynchronously with capped exponential backoff. Each attempt
     * refreshes the truncation grace window so metadata mirroring stays suppressed until the remote
     * prefix is confirmed clean, avoiding a re-mirror of not-yet-purged stale objects. After
     * {@link #MAX_TRUNCATE_PURGE_RETRIES} failures it logs an error and stops — the stale objects
     * then require manual cleanup, but the failure is at least visible rather than silent.
     */
    private void scheduleTruncatePurgeRetry(String dbName, String prefix) {
        scheduleTruncatePurgeRetry(dbName, prefix, 1, 0);
    }

    private void scheduleTruncatePurgeRetry(String dbName, String prefix, int attempt,
                                            int providerUnavailableRetries) {
        if (attempt > MAX_TRUNCATE_PURGE_RETRIES) {
            log.error("Cloud truncate purge for db={} still failing after {} attempt(s) — remote "
                      + "prefix '{}' may retain stale objects that could be re-hydrated on restore. "
                      + "Manual cleanup of that prefix is required.",
                      dbName, MAX_TRUNCATE_PURGE_RETRIES, prefix);
            return;
        }
        int exp = providerUnavailableRetries > 0 ? providerUnavailableRetries - 1 : attempt - 1;
        exp = Math.min(Math.max(exp, 0), 30);
        long cap = providerUnavailableRetries > 0
                   ? TRUNCATE_PROVIDER_UNAVAILABLE_MAX_DELAY_MS
                   : 8_000L;
        long delay = Math.min(TRUNCATE_PURGE_RETRY_BASE_MS * (1L << exp), cap);
        // Keep the grace window fresh so we do not mirror new metadata over the still-stale prefix.
        truncationTimes.put(dbName, System.currentTimeMillis());
        ScheduledExecutorService scheduler = metadataSyncScheduler();
        if (scheduler == null) {
            // Upload subsystem is shutting down; the scheduler will not be resurrected. The remote
            // prefix may retain stale objects — surface it rather than NPE on a null scheduler.
            log.error("Cloud truncate purge retry for db={} cannot be scheduled (subsystem "
                      + "shutting down) — remote prefix '{}' may retain stale objects.",
                      dbName, prefix);
            return;
        }
        try {
            scheduler.schedule(() -> {
                CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
                if (provider == null) {
                    // Provider temporarily unavailable (e.g. reconfiguration window). This is NOT a
                    // remote purge failure, so do not consume the bounded purge-attempt budget here;
                    // otherwise a long outage can exhaust retries before the provider returns and
                    // leave stale pre-truncate objects behind indefinitely.
                    log.warn("Cloud truncate purge retry for db={} found no active provider "
                             + "(attempt {}, providerUnavailableRetries={}); rescheduling "
                             + "without consuming retry budget.",
                             dbName, attempt, providerUnavailableRetries);
                    scheduleTruncatePurgeRetry(dbName, prefix, attempt,
                                              providerUnavailableRetries + 1);
                    return;
                }
                if (purgeRemotePrefix(provider, dbName, prefix)) {
                    removePendingTruncateMarker(prefix);
                    log.info("Cloud truncate purge succeeded on retry for db={} (attempt {})",
                             dbName, attempt);
                } else {
                    scheduleTruncatePurgeRetry(dbName, prefix, attempt + 1, 0);
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
            if (!(o instanceof DeferredSstDelete)) {
                return false;
            }
            DeferredSstDelete that = (DeferredSstDelete) o;
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
     * URL-safe Base64 of the prefix — reversible (for the startup scan) and collision-free.
     */
    private Path pendingDeleteMarkerPath(String prefix) {
        return pendingDeleteDir().resolve(encodeMarkerName(prefix));
    }

    private Path pendingTruncateMarkerPath(String prefix) {
        return pendingTruncateDir().resolve(encodeMarkerName(prefix));
    }

    private static String encodeMarkerName(String prefix) {
        return java.util.Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Crash-safely persists a local pending-remote-delete marker for {@code prefix}. This is the
     * durable anti-resurrection guard, so it is NOT best-effort: the marker file is written and
     * fsynced, and the containing directory is fsynced so the new directory entry itself survives a
     * crash. Throws {@link IOException} if durability cannot be guaranteed — the caller must then
     * hold delete progression rather than proceed with an unguarded delete.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void writePendingDeleteMarker(String dbName, String prefix) throws IOException {
        Path dir = pendingDeleteDir();
        Files.createDirectories(dir);
        Path marker = pendingDeleteMarkerPath(prefix);
        // Write + fsync the marker file so its bytes are on stable storage.
        try (FileChannel ch = FileChannel.open(marker, StandardOpenOption.CREATE,
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.TRUNCATE_EXISTING)) {
            ch.write(java.nio.ByteBuffer.wrap(
                    prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            ch.force(true);
        }
        // fsync the directory so the new/renamed marker entry itself is durable (a file fsync does
        // not guarantee the directory entry pointing at it survives a crash on many filesystems).
        try (FileChannel dirCh = FileChannel.open(dir, StandardOpenOption.READ)) {
            dirCh.force(true);
        } catch (IOException e) {
            // Windows rejects directory-handle fsync; elsewhere treat it as a real durability error.
            if (WINDOWS_OS) {
                log.debug("Directory fsync of {} not supported on this platform: {}",
                          dir, e.getMessage());
            } else {
                throw e;
            }
        }
        deleteMarkerHealthy = true;
        log.debug("Cloud pending-delete marker persisted (fsync'd): db={}, prefix={}", dbName, prefix);
    }

    /**
     * Crash-safely persists a local pending-truncate marker for {@code prefix}. This keeps remote
     * truncate-purge intent durable across process restarts while the provider is unavailable.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void writePendingTruncateMarker(String dbName, String prefix) throws IOException {
        Path dir = pendingTruncateDir();
        Files.createDirectories(dir);
        Path marker = pendingTruncateMarkerPath(prefix);
        try (FileChannel ch = FileChannel.open(marker, StandardOpenOption.CREATE,
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.TRUNCATE_EXISTING)) {
            ch.write(java.nio.ByteBuffer.wrap(
                    prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            ch.force(true);
        }
        try (FileChannel dirCh = FileChannel.open(dir, StandardOpenOption.READ)) {
            dirCh.force(true);
        } catch (IOException e) {
            if (WINDOWS_OS) {
                log.debug("Directory fsync of {} not supported on this platform: {}",
                          dir, e.getMessage());
            } else {
                throw e;
            }
        }
        log.debug("Cloud pending-truncate marker persisted (fsync'd): db={}, prefix={}",
                  dbName, prefix);
    }

    private void removePendingDeleteMarker(String prefix) {
        try {
            Files.deleteIfExists(pendingDeleteMarkerPath(prefix));
        } catch (IOException e) {
            log.debug("Failed to remove pending-delete marker for prefix={}: {}",
                      prefix, e.getMessage());
        }
    }

    /** Whether a local pending-remote-delete marker exists for {@code prefix}. */
    boolean hasPendingDeleteMarker(String prefix) {
        return Files.exists(pendingDeleteMarkerPath(prefix));
    }

    private void removePendingTruncateMarker(String prefix) {
        try {
            Files.deleteIfExists(pendingTruncateMarkerPath(prefix));
        } catch (IOException e) {
            log.debug("Failed to remove pending-truncate marker for prefix={}: {}",
                      prefix, e.getMessage());
        }
    }

    /**
     * Completes a pending remote delete under a per-prefix lock: if the marker is still present,
     * ensure the tombstone exists, purge the prefix, and on success drop the tombstone + marker.
     * Returns {@code true} when cleanup is confirmed complete (marker absent after this call).
     *
     * <p>The lock + marker re-check make the purge run at most once and ONLY while the marker is
     * present. A re-created DB's {@link #onDBOpening} calls this before returning (so the open
     * blocks until cleanup finishes), and its uploads happen only afterwards — so the async retry,
     * which also takes the lock and skips when the marker is gone, can never purge freshly-written
     * data for the re-created generation.
     */
    private boolean tryCompletePendingDelete(CloudStorageProvider provider, String dbName,
                                             String prefix) {
        Object lock = pendingDeleteLocks.computeIfAbsent(prefix, k -> new Object());
        synchronized (lock) {
            if (!hasPendingDeleteMarker(prefix)) {
                return true; // already cleaned up by another caller
            }
            ensureTombstonePresent(provider, dbName, prefix);
            if (purgeRemotePrefix(provider, dbName, prefix)) {
                deleteTombstoneBestEffort(provider, dbName, prefix);
                removePendingDeleteMarker(prefix);
                pendingDeleteLocks.remove(prefix);
                log.info("Cloud pending-delete cleanup completed for db={} (prefix={})",
                         dbName, prefix);
                return true;
            }
            return false;
        }
    }

    /**
     * Retries the remote purge for a deleted DB with capped exponential backoff — the same safety
     * level as the truncate purge. A {@code null} provider is treated as a failed attempt and
     * rescheduled (transient reconfiguration window) rather than abandoned. While retries are
     * pending, the local pending-delete marker keeps {@link #onDBOpening} from re-hydrating stale
     * data. After {@link #MAX_DELETE_PURGE_RETRIES} failures it logs loudly for operator action; the
     * marker and tombstone remain, so re-hydration stays blocked until manual cleanup.
     */
    private void scheduleDeletePurgeRetry(String dbName, String prefix, int attempt) {
        if (attempt > MAX_DELETE_PURGE_RETRIES) {
            log.error("Cloud DB delete purge for db={} still failing after {} attempt(s) — remote "
                      + "prefix '{}' may retain stale objects. The local pending-delete marker and "
                      + "tombstone remain (re-hydration stays blocked); manual cleanup is required.",
                      dbName, MAX_DELETE_PURGE_RETRIES, prefix);
            return;
        }
        long delay = Math.min(DELETE_PURGE_RETRY_BASE_MS * (1L << (attempt - 1)), 8_000L);
        ScheduledExecutorService scheduler = metadataSyncScheduler();
        if (scheduler == null) {
            log.error("Cloud DB delete purge retry for db={} cannot be scheduled (subsystem shutting "
                      + "down) — local marker preserved; remote prefix '{}' may retain stale "
                      + "objects until next startup.", dbName, prefix);
            return;
        }
        try {
            scheduler.schedule(() -> {
                CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
                if (provider == null) {
                    log.warn("Cloud DB delete purge retry for db={} found no active provider "
                             + "(attempt {}); rescheduling.", dbName, attempt);
                    scheduleDeletePurgeRetry(dbName, prefix, attempt + 1);
                    return;
                }
                // tryCompletePendingDelete is a no-op (returns true) if the marker was already
                // cleared (e.g. by an inline open-time cleanup), so the retry never purges a prefix
                // a re-created DB may have taken over.
                if (!tryCompletePendingDelete(provider, dbName, prefix)) {
                    scheduleDeletePurgeRetry(dbName, prefix, attempt + 1);
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
        Path tmp = null;
        try {
            if (provider.fileExists(tombstoneKey)) {
                return;
            }
            tmp = Files.createTempFile("hgstore-tombstone-", ".tmp");
            Files.write(tmp, "deleted".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            provider.uploadFile(tmp.toString(), tombstoneKey);
            log.info("Cloud DB tombstone written (deferred): db={}, key={}", dbName, tombstoneKey);
        } catch (Exception e) {
            log.warn("Deferred tombstone write failed for db={}, key={}: {}",
                     dbName, tombstoneKey, e.getMessage());
        } finally {
            if (tmp != null) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignore) {
                    // best-effort temp cleanup
                }
            }
        }
    }

    /** On startup, schedule a bounded purge retry for each leftover pending-delete marker. */
    private void processPendingDeleteMarkersOnStartup() {
        Path dir = pendingDeleteDir();
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (Stream<Path> markers = Files.list(dir)) {
            markers.forEach(marker -> {
                String encoded = marker.getFileName() == null ? "" : marker.getFileName().toString();
                String prefix;
                try {
                    prefix = Files.readString(marker).trim();
                } catch (IOException e) {
                    log.warn("Failed to read pending-delete marker {}: {}", marker, e.getMessage());
                    return;
                }
                if (prefix.isEmpty()) {
                    return;
                }
                String decoded = decodePendingDeleteMarkerName(encoded);
                if (decoded == null || !decoded.equals(prefix)) {
                    log.error("Cloud startup: ignoring invalid pending-delete marker {} (payload does "
                              + "not match encoded prefix)", marker);
                    return;
                }
                if (isPendingDeletePrefixInScope(prefix)) {
                    log.error("Cloud startup: ignoring out-of-scope pending-delete marker {} for "
                              + "prefix='{}'", marker, prefix);
                    return;
                }
                String inferredDbName = inferDbNameFromPrefix(prefix);
                log.warn("Cloud startup: found pending remote-delete marker for prefix={} — "
                         + "scheduling deferred purge to prevent stale-data re-hydration.", prefix);
                scheduleDeletePurgeRetry(inferredDbName, prefix, 1);
            });
        } catch (IOException e) {
            log.warn("Failed to scan pending-delete markers in {}: {}", dir, e.getMessage());
        }
    }

    /** On startup, schedule retries for leftover pending-truncate markers. */
    private void processPendingTruncateMarkersOnStartup() {
        Path dir = pendingTruncateDir();
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (Stream<Path> markers = Files.list(dir)) {
            markers.forEach(marker -> {
                String encoded = marker.getFileName() == null ? "" : marker.getFileName().toString();
                String prefix;
                try {
                    prefix = Files.readString(marker).trim();
                } catch (IOException e) {
                    log.warn("Failed to read pending-truncate marker {}: {}",
                             marker, e.getMessage());
                    return;
                }
                if (prefix.isEmpty()) {
                    return;
                }
                String decoded = decodePendingDeleteMarkerName(encoded);
                if (decoded == null || !decoded.equals(prefix)) {
                    log.error("Cloud startup: ignoring invalid pending-truncate marker {} "
                              + "(payload does not match encoded prefix)", marker);
                    return;
                }
                if (isPendingDeletePrefixInScope(prefix)) {
                    log.error("Cloud startup: ignoring out-of-scope pending-truncate marker {} "
                              + "for prefix='{}'", marker, prefix);
                    return;
                }
                String inferredDbName = inferDbNameFromPrefix(prefix);
                log.warn("Cloud startup: found pending truncate marker for prefix={} — scheduling "
                         + "deferred purge retry.", prefix);
                scheduleTruncatePurgeRetry(inferredDbName, prefix);
            });
        } catch (IOException e) {
            log.warn("Failed to scan pending-truncate markers in {}: {}", dir, e.getMessage());
        }
    }

    /** Decodes the marker filename (Base64 URL, no padding) back to the original remote prefix. */
    private static String decodePendingDeleteMarkerName(String encoded) {
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

    /** Validates that a pending-delete prefix is relative, normalized, and in this listener's scope. */
    private boolean isPendingDeletePrefixInScope(String prefix) {
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
     * Upper bound on how long a truncate may legitimately stay "in progress". A truncate is a
     * drop+recreate of column families and completes in well under this window; anything longer
     * means the completion callback ({@link #onDBTruncated}) never fired.
     */
    private static final long MAX_TRUNCATION_DURATION_MS = 60_000L;

    /** Effective max-truncation window; overridable in tests to avoid a 60 s wait. */
    private volatile long maxTruncationDurationMs = MAX_TRUNCATION_DURATION_MS;

    /** Test seam: shrink the stale-latch window so self-healing can be exercised quickly. */
    void setMaxTruncationDurationMsForTest() {
        this.maxTruncationDurationMs = 50L;
    }

    /**
     * Returns {@code true} while a truncate is actively in progress for {@code dbName}.
     *
     * <p><b>Self-healing latch.</b> {@code truncatingDbs} is set by {@link #onDBTruncateBegin} and
     * normally cleared by {@link #onDBTruncated}. However {@code RocksDBSession.truncate()} invokes
     * {@code dropTables}/{@code createTables} (which throw the unchecked {@code DBStoreException})
     * between the begin and completion notifications without a {@code finally}, so a failure there
     * leaves the completion callback un-invoked and the latch stuck. A stuck latch would silently
     * disable metadata mirroring, cloud object deletion, and the delete guard for that DB for the
     * lifetime of the process. To bound that blast radius, treat the latch as stale (and clear it)
     * once the recorded truncation start is older than {@link #MAX_TRUNCATION_DURATION_MS}.
     */
    boolean isActivelyTruncating(String dbName) {
        if (!truncatingDbs.contains(dbName)) {
            return false;
        }
        Long startedAt = truncationTimes.get(dbName);
        if (startedAt != null
                && System.currentTimeMillis() - startedAt > maxTruncationDurationMs) {
            truncatingDbs.remove(dbName);
            log.warn("Cleared stale 'truncating' flag for db={} ({} ms elapsed with no completion "
                     + "callback) — resuming cloud metadata mirroring and cleanup", dbName,
                     System.currentTimeMillis() - startedAt);
            return false;
        }
        return true;
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
        CloudStorageMetrics.registerDatabaseMetrics(dbName);
        String remoteKey = toRelativeKey(filePath);

        // Capture the epoch before we hand off to a background thread.  The async callback
        // uses markConfirmedIfEpoch so a late confirmation after clearDb() + DB recreation
        // with reused file numbers is silently dropped instead of producing stale durability state.
        long uploadEpoch = syncTracker.currentEpoch(dbName);

        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            CloudStorageMetrics.recordUploadFailure(dbName, cfName, "NoActiveProvider");
            if (retryQueue != null) {
                try {
                    Path pinned = pinForAsyncUpload(filePath);
                    retryQueue.submitPinned(dbName, cfName, pinned.toString(), filePath, remoteKey,
                                            uploadEpoch,
                                            new IOException("no active cloud provider"));
                    log.warn("Cloud upload deferred due to missing provider: db={}, cf={}, path={}"
                             + " (staged pin routed to retry queue)", dbName, cfName, filePath);
                } catch (Exception pinError) {
                    log.warn("Cloud upload deferred due to missing provider and staging failed: "
                             + "db={}, cf={}, path={} — routing original SST to retry queue: {}",
                             dbName, cfName, filePath, pinError.getMessage());
                    retryQueue.submit(dbName, cfName, filePath, remoteKey, uploadEpoch,
                                      new IOException("no active cloud provider", pinError));
                }
            } else {
                log.warn("Cloud upload skipped: no active provider and no retry queue: db={}, cf={}, "
                         + "path={}", dbName, cfName, filePath);
            }
            applyBackpressure(dbName);
            return;
        }

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
            applyBackpressure(dbName);
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

        // Apply backpressure AFTER handling this file so the flush/compaction thread slows down
        // while the cloud mirror is behind, preventing ingestion from outrunning durability.
        applyBackpressure(dbName);
    }

    /**
     * A single asynchronous SST upload, as a typed {@link Runnable} rather than a lambda so that a
     * forced {@code shutdownNow()} of the shared upload executor returns the actual task objects
     * (a {@link ThreadPoolExecutor} returns the exact runnables it had queued). {@link
     * #handoffOnShutdown()} then routes any never-run task into the retry queue / DLQ so an SST that
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
            // hard-link transfers to the retry/DLQ lifecycle — it must NOT be deleted here, or a
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
                // ---- Post-upload: the upload SUCCEEDED. Errors below (e.g. a null metadata-sync
                // scheduler during a shutdown race) must NOT be reclassified as an upload failure
                // and re-uploaded/DLQ'd; the SST is already durable in cloud. ----
                syncTracker.markConfirmedIfEpoch(dbName, filePath, uploadEpoch);
                long syncLatencyMs = System.currentTimeMillis() - startTimeMs;
                CloudStorageMetrics.recordSyncLatency(dbName, syncLatencyMs);
                try {
                    // Coalesce the per-SST metadata sync: at most one publish per debounce window
                    // per DB, with a trailing sync. (Truncation/grace suppression is handled inside
                    // requestDebouncedMetadataSync.)
                    requestDebouncedMetadataSync(provider, dbName);
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
         * the pin is kept alive (not deleted) so the retry/DLQ can upload it even if RocksDB has
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
            log.warn("Cloud upload task not executed before shutdown; handing off to retry/DLQ: "
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
        Path stagingDir = Paths.get(matchingRoot, ".cloud-upload-staging");
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
        int backlog = uploadExecutor.getQueue().size() + uploadExecutor.getActiveCount();
        if (retryQueue != null) {
            // Active lag: the executor's queued/active uploads plus the retry queue's in-flight
            // (scheduled/executing) retries.
            backlog += retryQueue.getInFlightCount();
            // Exhausted-failure pressure: fold in the DLQ ENQUEUE RATE (uploads that just exhausted
            // their retries and became local-only), bounded by the watermark. This throttles
            // ingestion while durability is actively degrading during a sustained outage — the
            // realistic data-loss window the retry-queue in-flight count alone misses once retries
            // are exhausted. We deliberately use the enqueue RATE, not static DLQ depth: counting
            // depth would keep the write path throttled long after the provider recovered (historical
            // debt awaiting an explicit replayDlq()), a self-inflicted availability degradation.
            // Static DLQ depth remains a separate health signal (getDlqSize / persistence metric).
            backlog += dlqEnqueueRateBacklog();
        }
        return backlog;
    }

    /**
     * Bounded backpressure contribution from the DLQ enqueue rate: the number of uploads that
     * exhausted their retries (moved to the DLQ) within the trailing {@link #DLQ_ENQUEUE_RATE_WINDOW_MS}
     * window, capped at {@link #backpressureHighWatermark}. Positive only while durability is
     * actively degrading; falls to zero once failures stop, so it never pins the write path on a
     * static, post-recovery DLQ.
     */
    private int dlqEnqueueRateBacklog() {
        if (retryQueue == null || backpressureHighWatermark <= 0) {
            return 0;
        }
        long now = System.currentTimeMillis();
        synchronized (dlqRateLock) {
            if (lastDlqRateSampleMs == 0L) {
                // First observation: prime the baseline so a historical DLQ backlog present at
                // startup is not mistaken for a fresh enqueue burst. Contribute nothing this round.
                lastDlqRateSampleMs = now;
                lastDlqEnqueuedTotalAtSample = retryQueue.getDlqEnqueuedTotal();
                return 0;
            }
            if (now - lastDlqRateSampleMs >= DLQ_ENQUEUE_RATE_WINDOW_MS) {
                long total = retryQueue.getDlqEnqueuedTotal();
                long enqueuedInWindow = Math.max(0L, total - lastDlqEnqueuedTotalAtSample);
                lastDlqEnqueuedTotalAtSample = total;
                lastDlqRateSampleMs = now;
                dlqRateBacklogContribution =
                        (int) Math.min(backpressureHighWatermark, enqueuedInWindow);
            }
            return dlqRateBacklogContribution;
        }
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
        if (isActivelyTruncating(dbName) || isInTruncationGracePeriod(dbName)) {
            log.debug("Skipping delete during truncation: db={}, path={}", dbName, filePath);
            return;
        }
        // DATA-LOSS GUARD: never delete a superseded cloud object until every SST file currently
        // live in this DB is confirmed present in cloud.
        if (ensureLiveSetUploaded(provider, dbName)) {
            log.warn("Delete skipped (live set not fully durable in cloud): db={}, filePath={}",
                     dbName, filePath);
            enqueueDeferredSstDelete(dbName, cfName, filePath,
                                     "live set not yet fully durable in cloud");
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
            enqueueDeferredSstDelete(dbName, cfName, filePath,
                                     "metadata sync failed before safe delete");
            return;
        }

        String remoteKey = toRelativeKey(filePath);
        try {
            provider.deleteFile(remoteKey);
            syncTracker.clearConfirmed(dbName, filePath);
            deferredSstDeletes.remove(new DeferredSstDelete(dbName, filePath));
            log.debug("Cloud delete success: db={}, cf={}, path={}", dbName, cfName, filePath);
        } catch (Exception e) {
            // Non-fatal: log and continue.
            log.error("Cloud delete failed: db={}, cf={}, path={}", dbName, cfName, filePath, e);
            enqueueDeferredSstDelete(dbName, cfName, filePath,
                                     "cloud delete failed after metadata publish");
        }
    }

    private void enqueueDeferredSstDelete(String dbName, String cfName, String filePath,
                                          String reason) {
        DeferredSstDelete task = new DeferredSstDelete(dbName, filePath);
        boolean added = deferredSstDeletes.add(task);
        if (added) {
            log.debug("Cloud delete deferred: db={}, cf={}, path={}, reason={}",
                      dbName, cfName, filePath, reason);
            updateDeferredSstDeleteWarnState();
        }
        scheduleDeferredSstDeleteRetry(DEFERRED_SST_DELETE_RETRY_MS);
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
        ScheduledExecutorService scheduler = metadataSyncScheduler();
        if (scheduler == null) {
            log.debug("Deferred cloud-delete retry cannot be scheduled (subsystem shutting down); "
                      + "{} pending delete(s) remain deferred", deferredSstDeletes.size());
            return;
        }
        if (!deferredSstDeleteRetryScheduled.compareAndSet(false, true)) {
            return;
        }
        long safeDelay = Math.max(0L, delayMs);
        try {
            scheduler.schedule(() -> {
                deferredSstDeleteRetryScheduled.set(false);
                retryDeferredSstDeletes();
            }, safeDelay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            deferredSstDeleteRetryScheduled.set(false);
            log.debug("Deferred cloud-delete retry scheduling rejected (scheduler shutting down): {}",
                      e.getMessage());
        }
    }

    /** Retries superseded-SST deletes that were deferred on transient cloud/metadata failures. */
    private void retryDeferredSstDeletes() {
        if (deferredSstDeletes.isEmpty()) {
            return;
        }
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            scheduleDeferredSstDeleteRetry(DEFERRED_SST_DELETE_RETRY_MS);
            return;
        }
        int succeeded = 0;
        for (DeferredSstDelete task : new ArrayList<>(deferredSstDeletes)) {
            if (isActivelyTruncating(task.dbName) || isInTruncationGracePeriod(task.dbName)) {
                continue;
            }
            if (ensureLiveSetUploaded(provider, task.dbName)) {
                continue;
            }
            if (!syncMetadataSnapshotInline(provider, task.dbName)) {
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
            log.debug("Deferred cloud-delete retry succeeded for {} superseded SST(s)", succeeded);
        }
        updateDeferredSstDeleteWarnState();
        if (!deferredSstDeletes.isEmpty()) {
            scheduleDeferredSstDeleteRetry(DEFERRED_SST_DELETE_RETRY_MS);
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
        String dbName = resolveDbName(dbNameOrPath, null);
        // Skip metadata sync during truncation or grace period to allow purge to complete cleanly
        if (!isActivelyTruncating(dbName) && !isInTruncationGracePeriod(dbName)) {
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
     * Coalesces the high-frequency, per-SST metadata sync triggered by {@link #onTableFileCreated}.
     *
     * <p>At most one sync runs per {@link #metadataSyncDebounceMs} window per DB. The first upload
     * in a window publishes immediately (leading edge); further uploads within the window schedule
     * a single trailing sync so the final MANIFEST/CURRENT is always eventually published even if
     * writes stop mid-window. This does not weaken the durability contract: SST objects are
     * uploaded eagerly in {@link #onTableFileCreated} regardless — only the (idempotent, generation
     * -guarded) metadata pointer publish is coalesced. Event-driven callers that must publish
     * immediately (delete guard, {@link #onCompacted}, {@link #onDBCreated}) call
     * {@link #syncMetadataSnapshotInline} directly and are intentionally not routed through here.
     */
    void requestDebouncedMetadataSync(CloudStorageProvider provider, String dbName) {
        if (uploadSubsystemShuttingDown) {
            // Shutdown in progress: the debounce scheduler is being torn down and must NOT be
            // resurrected. But an upload that completed during the drain still has to advance
            // CURRENT/MANIFEST — otherwise the SST is durable in cloud yet unreferenced by a stale
            // pointer, and a post-restart recovery (after local disk loss) would miss it. Publish
            // the final metadata state INLINE (no scheduler) so uploads that finish during shutdown
            // are reflected in the mirrored recovery point. The listener has already been removed
            // from RocksDBFactory at this point, so no alternative event path would publish it.
            if (isActivelyTruncating(dbName) || isInTruncationGracePeriod(dbName)) {
                return;
            }
            syncMetadataSnapshotInline(provider, dbName);
            return;
        }
        if (isActivelyTruncating(dbName) || isInTruncationGracePeriod(dbName)) {
            return;
        }
        // Count this upload as not-yet-mirrored. syncMetadataSnapshotInline resets the counter on a
        // successful publish, so it reflects the SSTs uploaded since the last mirrored manifest.
        int pending = unpublishedUploads
                .computeIfAbsent(dbName, k -> new java.util.concurrent.atomic.AtomicInteger())
                .incrementAndGet();

        long now = System.currentTimeMillis();
        Long last = lastMetadataSyncMs.get(dbName);
        boolean windowElapsed = (last == null || now - last >= metadataSyncDebounceMs);
        boolean backlogExceeded = (metadataSyncMaxUnpublished > 0
                                   && pending >= metadataSyncMaxUnpublished);
        if (windowElapsed || backlogExceeded) {
            // Leading edge (window elapsed) OR the unpublished-upload backlog hit its bound: publish
            // now so the cloud recovery point cannot fall arbitrarily far behind during a burst.
            // Record the time first so uploads during the (slow) sync defer to a single trailing run
            // rather than piling up behind the per-DB metadata lock.
            lastMetadataSyncMs.put(dbName, now);
            // Consume the backlog for THIS forced attempt up front, regardless of the publish
            // outcome. syncMetadataSnapshotInline also resets the counter to 0 on a successful
            // publish; doing it here as well ensures a FAILED publish (e.g. a metadata-publish
            // outage while SST uploads still succeed, or a transient capture failure) cannot leave
            // the counter pinned at/above the bound. If it did, every subsequent upload would take
            // this branch and run a full checkpoint + publish attempt inline on the upload
            // executor, starving it. Subtract the observed count (rather than set 0) so uploads
            // that raced in after we read `pending` are still counted toward the next bound.
            //
            // Drain with a floor-at-zero CAS loop rather than a plain addAndGet(-pending): two
            // concurrent forced attempts each read their own `pending` and would otherwise both
            // subtract, driving the counter negative. A negative counter means many subsequent
            // uploads must first climb back to zero before the bound can be hit again, delaying or
            // skipping forced publishes and widening the recovery point past
            // metadataSyncMaxUnpublished.
            java.util.concurrent.atomic.AtomicInteger counter = unpublishedUploads.get(dbName);
            if (counter != null) {
                int prev;
                int next;
                do {
                    prev = counter.get();
                    next = Math.max(0, prev - pending);
                } while (!counter.compareAndSet(prev, next));
            }
            syncMetadataSnapshotInline(provider, dbName);
            return;
        }
        // Within the window and under the backlog bound: ensure exactly one trailing sync captures
        // the final state.
        if (!pendingMetadataSync.add(dbName)) {
            return; // a trailing sync is already scheduled for this DB
        }
        long delay = Math.max(0L, metadataSyncDebounceMs - (now - last));
        ScheduledExecutorService scheduler = metadataSyncScheduler();
        if (scheduler == null) {
            // metadataSyncScheduler() legally returns null once shutdown has begun (the gate flipped
            // between our entry check and here). Do NOT call schedule() on null — that NPE would
            // propagate to the caller (e.g. SstUploadTask.run) and be misclassified as an UPLOAD
            // failure, wrongly retrying/DLQ'ing a successful upload. The final metadata state is
            // published by the direct-sync callers during shutdown, so skip the trailing sync.
            pendingMetadataSync.remove(dbName);
            return;
        }
        try {
            scheduler.schedule(() -> runTrailingMetadataSync(dbName),
                               delay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // Scheduler shutting down: don't drop the sync — run it inline now.
            pendingMetadataSync.remove(dbName);
            lastMetadataSyncMs.put(dbName, now);
            syncMetadataSnapshotInline(provider, dbName);
        }
    }

    /**
     * Trigger invoked by {@link CloudUploadRetryQueue} after an upload becomes durable via retry or
     * DLQ replay. Publishes CURRENT/MANIFEST (debounced) so the mirrored recovery point advances
     * even when the retry succeeded during an otherwise-quiet period on an idle DB — without this,
     * cloud could hold the SST but keep exposing a stale CURRENT. Runs on the retry-queue thread.
     */
    public void onRetryUploadDurable(String dbName) {
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            return;
        }
        // During shutdown the retry queue is closed AFTER the upload executor drains and BEFORE the
        // provider is closed, so a retry that becomes durable in that window must still advance the
        // pointer. requestDebouncedMetadataSync now publishes inline (no scheduler resurrection) when
        // shutting down, so we route through it unconditionally rather than dropping the sync here.
        requestDebouncedMetadataSync(provider, dbName);
        // A newly durable upload may unblock one or more deferred superseded-SST deletes.
        scheduleDeferredSstDeleteRetry(0L);
    }

    /** Trailing (deferred) metadata sync body; runs on {@link #metadataSyncScheduler()}. */
    private void runTrailingMetadataSync(String dbName) {
        pendingMetadataSync.remove(dbName);
        if (isActivelyTruncating(dbName) || isInTruncationGracePeriod(dbName)) {
            return;
        }
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            return;
        }
        lastMetadataSyncMs.put(dbName, System.currentTimeMillis());
        try {
            syncMetadataSnapshotInline(provider, dbName);
        } catch (Exception e) {
            log.warn("Deferred cloud metadata-sync failed for db={}: {}", dbName, e.getMessage());
        }
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
     * the metadata-sync debounce window and the unpublished-upload backlog cap; when the memtable is
     * empty the flush is a no-op.
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
                boolean published = publishSnapshotWithGenerationCheck(provider, dbName, snapshot);
                if (published) {
                    // The mirrored manifest now reflects the current live set — clear the
                    // unpublished-upload backlog so the count bound measures uploads since THIS
                    // publish. Reset to 0 (rather than decrement) since the checkpoint captured the
                    // state as of its start; any uploads racing the publish are re-counted next.
                    java.util.concurrent.atomic.AtomicInteger c = unpublishedUploads.get(dbName);
                    if (c != null) {
                        c.set(0);
                    }
                }
                return published;
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
                if (retryQueue != null && Files.exists(Paths.get(realPath))) {
                    // Epoch captured now so a successful retry confirms via the epoch-guarded
                    // callback (plain submit() uses epoch 0 → dropped).
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

        // STALE-DATA GUARD: if a tombstone exists (written by onDBDeleteBegin outside the
        // data prefix), the previous generation was destroyed but cloud objects may not have
        // been fully removed. Hydrating them would silently resurrect deleted data in the new
        // DB. Instead, skip hydration, trigger a best-effort remote purge, and start fresh.
        String tombstoneKey = prefix + DB_TOMBSTONE_SUFFIX;
        try {
            if (provider.fileExists(tombstoneKey)) {
                log.warn("Cloud pre-hydration skipped: tombstone found for db={} — previous "
                         + "generation was deleted. Purging stale remote objects.", dbName);
                boolean purgeSucceeded = purgeRemotePrefix(provider, dbName, prefix);
                if (!purgeSucceeded) {
                    // Purge failed: leave the tombstone in place so the next restart will
                    // attempt the purge again and not accidentally hydrate stale objects.
                    throw new IllegalStateException(
                            String.format("Cloud pre-hydration: prefix purge failed for db=%s — "
                                          + "tombstone preserved to guard against stale-data "
                                          + "re-hydration. DB open is blocked.", dbName));
                }
                // Purge succeeded: now delete the tombstone so the next restart hydrates normally.
                // The tombstone lives outside the slash-terminated purged prefix, so
                // purgeRemotePrefix never touches it — we must delete it explicitly here.
                try {
                    provider.deleteFile(tombstoneKey);
                    log.info("Cloud pre-hydration: tombstone cleaned up: db={}", dbName);
                } catch (IOException e) {
                    // Tombstone deletion failed after a successful purge. If we proceed, the next
                    // restart will see the tombstone, re-purge an already-clean prefix, and block
                    // hydration of legitimately written new-generation data.  Fail loudly here
                    // instead; the operator can manually delete the tombstone to unblock.
                    throw new IllegalStateException(
                            String.format("Cloud pre-hydration: tombstone deletion failed for "
                                          + "db=%s after successful prefix purge — the tombstone "
                                          + "key '%s' must be manually removed to allow future "
                                          + "hydration. DB open is blocked.", dbName, tombstoneKey),
                            e);
                }
                return;
            }
        } catch (IOException e) {
            // Tombstone check itself failed — cannot safely determine generation, and cloud cannot
            // validate the DB. Proceed from local state ONLY if it is self-consistent (valid
            // CURRENT→MANIFEST); orphan SSTs without a valid lineage must not silently boot.
            if (hasConsistentLocalMetadata(root)) {
                throw new IllegalStateException(
                        String.format("Cloud pre-hydration: tombstone check failed for db=%s and "
                                      + "local metadata is not self-consistent (no valid "
                                      + "CURRENT→MANIFEST lineage) — cannot safely open DB without "
                                      + "knowing generation state: %s", dbName, e.getMessage()), e);
            }
            // Local metadata is self-consistent: safe to open from local state; log and continue.
            log.warn("Cloud pre-hydration: tombstone check failed for db={}, proceeding with "
                     + "self-consistent local state (valid CURRENT→MANIFEST present): {}",
                     dbName, e.getMessage());
        }

        List<String> remoteFiles = listRemoteKeys(provider, prefix, root);
        if (remoteFiles.isEmpty()) {
            log.debug("Cloud pre-hydration skipped: no remote files for db={} prefix={}",
                      dbName, prefix);
            return;
        }

        // Clean up any stale temp files left by a previous crashed download before
        // deciding whether a local file is complete.  Match both legacy ".hyd-tmp" and the
        // current per-thread naming ".hyd-tmp-<threadId>-<nanoTime>" so all crash leftovers
        // are removed regardless of which code version created them.
        try (Stream<Path> stale = Files.walk(root, 10)) {
            stale.filter(p -> {
                     String name = p.getFileName().toString();
                     return name.endsWith(".hyd-tmp") || name.contains(".hyd-tmp-");
                 })
                 .forEach(p -> deleteIfExistsQuietly(p, "pre-hydration stale-temp cleanup"));
        } catch (IOException e) {
            log.debug("Failed to scan stale hydration temp files under {}: {}",
                      root, e.getMessage());
        }

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
                Files.createDirectories(localPath.getParent());
                // Download to a unique sibling temp file, then atomically move into place. The
                // per-thread/per-attempt suffix prevents two concurrent restorers from writing the
                // same temp file (which would interleave into a corrupt SST), and REPLACE_EXISTING
                // makes a late second mover a harmless idempotent overwrite. A crash mid-download
                // never leaves RocksDB reading a truncated SST at the expected path.
                Path tmp = localPath.resolveSibling(
                        localPath.getFileName() + ".hyd-tmp-" + Thread.currentThread().getId()
                        + "-" + System.nanoTime());
                try {
                    provider.downloadFile(remoteKey, tmp.toString());
                    try {
                        Files.move(tmp, localPath,
                                   java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                                   java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    } catch (java.nio.file.AtomicMoveNotSupportedException ex) {
                        // Cross-filesystem fallback: non-atomic but still a replace.
                        Files.move(tmp, localPath,
                                   java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (IOException e) {
                    deleteIfExistsQuietly(tmp, "pre-hydration temp cleanup");
                    throw e;
                }
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
        Path tmp = null;
        try {
            if (!provider.fileExists(currentRemoteKey)) {
                return; // no remote CURRENT to compare against
            }
            tmp = Files.createTempFile("hg-current-cmp-", ".tmp");
            provider.downloadFile(currentRemoteKey, tmp.toString());
            String remoteManifest = Files.readString(tmp).trim();
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
                try {
                    Files.move(tmp, localCurrent,
                               java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                               java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                    Files.move(tmp, localCurrent,
                               java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                }
                tmp = null;
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
            if (tmp != null) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignore) {
                    // best-effort temp cleanup
                }
            }
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
            if (hasConsistentLocalMetadata(localRoot)) {
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
     * Returns {@code true} only if the local DB has SELF-CONSISTENT metadata: a {@code CURRENT}
     * pointer that references a {@code MANIFEST-*} present locally. This is a stronger readiness
     * predicate than a "local SST exists" check — orphan SSTs without a valid CURRENT→MANIFEST
     * lineage (a partial/rolled-back directory) do NOT qualify. Used to decide whether it is safe
     * to fall back to local state when cloud is unreachable and cannot validate the DB.
     */
    private static boolean hasConsistentLocalMetadata(Path root) {
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
