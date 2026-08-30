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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import org.apache.hugegraph.store.cloud.CloudStorageNonRetryableException;
import org.apache.hugegraph.store.cloud.CloudStorageProviderFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Asynchronous retry queue with a file-backed dead-letter queue (DLQ) for failed
 * cloud SST uploads.
 *
 * <h3>Retry strategy</h3>
 * <p>The number of whole-file retries is configured by the caller via
 * {@code cloud.storage.upload-retry-max-attempts}, which defaults to {@code 3} (whole-file retries
 * enabled). {@code maxAttempts == 0} is an opt-in mode — not the shipped default — for deployments
 * whose provider already performs sufficient internal retries (e.g. S3 multipart-part-retry).
 * <ul>
 *   <li>When {@code maxAttempts == 0}, failures go directly to the DLQ with no whole-file retry.</li>
 *   <li>When {@code maxAttempts > 0} (the default), the first retry is scheduled after
 *       {@code initialDelayMs} milliseconds.</li>
 *   <li>Subsequent retries use exponential backoff: {@code delay = initialDelayMs * 2^(attempt-1)},
 *       capped at {@code maxDelayMs}.</li>
 *   <li>After {@code maxAttempts} consecutive failures the task is moved to the DLQ.</li>
 *   <li>If the local SST file no longer exists at retry time (compacted away by RocksDB)
 *       the retry is silently dropped — there is nothing to upload.</li>
 * </ul>
 *
 * <h3>Dead-letter queue (DLQ)</h3>
 * <ul>
 *   <li>In-memory: {@link #getDlqEntries()} returns a snapshot.</li>
 *   <li>On-disk: entries are appended to {@code <dataRoot>/.cloud-upload-dlq.tsv} in a
 *       tab-separated format so they survive JVM restarts. Existing entries are loaded
 *       back into memory at construction time.</li>
 *   <li>{@link #replayDlq()} retries all DLQ entries immediately; successful uploads are
 *       removed and the file is rewritten. Failed entries are re-submitted to the retry
 *       queue with a fresh attempt cycle.</li>
 * </ul>
 *
 * <h3>Thread safety</h3>
 * All public methods are thread-safe. The in-memory DLQ is a {@link ConcurrentLinkedDeque};
 * disk I/O is serialised onto the internal {@link ScheduledExecutorService} (for appends)
 * or synchronised explicitly (for rewrites).
 *
 * <h3>Lifecycle</h3>
 * Call {@link #close()} (e.g. from a Spring {@code @PreDestroy} method) to shut down the
 * background executor gracefully.
 */
@Slf4j
public class CloudUploadRetryQueue implements Closeable {

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------

    /** Name of the on-disk DLQ file, relative to {@code dataRoot}. */
    static final String DLQ_FILE_NAME = ".cloud-upload-dlq.tsv";

    private static final String DLQ_COMMENT =
            "# Cloud SST upload dead-letter queue – tab-separated: "
            + "failedAt\\tattemptCount\\tdbName\\tcfName\\tfilePath\\tremoteKey\\tlastError"
            + "\\tsourceSstPath\\tuploadEpoch";

    /** Max provider-unavailable reschedules before surfacing a terminal local-only DLQ signal. */
    private static final int DEFAULT_MAX_PROVIDER_UNAVAILABLE_RETRIES = 256;
    /** Backoff cap for provider-unavailable reschedules (ms), independent of upload-attempt cap. */
    private static final long DEFAULT_PROVIDER_UNAVAILABLE_MAX_DELAY_MS = 30_000L;

    // -----------------------------------------------------------------------
    // Configuration
    // -----------------------------------------------------------------------

    private final int maxAttempts;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private volatile int maxProviderUnavailableRetries =
            DEFAULT_MAX_PROVIDER_UNAVAILABLE_RETRIES;
    private volatile long providerUnavailableMaxDelayMs =
            DEFAULT_PROVIDER_UNAVAILABLE_MAX_DELAY_MS;

    /**
     * Invoked with {@code (dbName, sourceSstPath, uploadEpoch)} whenever an upload succeeds via
     * retry or DLQ replay, so the caller can mark the file confirmed-present in cloud.
     * The {@code uploadEpoch} is the {@link CloudSyncTracker} epoch captured at submission time;
     * the callback should use {@link CloudSyncTracker#markConfirmedIfEpoch} to drop stale callbacks.
     */
    @FunctionalInterface
    public interface UploadConfirmedCallback {
        void onConfirmed(String dbName, String sourceSstPath, long uploadEpoch);
    }

    private final UploadConfirmedCallback onUploadConfirmed;

    /**
     * Invoked with {@code dbName} after an upload becomes durable via retry or DLQ replay, so the
     * caller can (debounced) publish CURRENT/MANIFEST for that DB. Without this, a retry that
     * succeeds during a quiet period would leave the SST in cloud but the mirrored CURRENT stale
     * until some unrelated sync fires (or never, on an idle DB) — widening the recovery point.
     */
    @FunctionalInterface
    public interface MetadataSyncTrigger {
        void onUploadDurable(String dbName);
    }

    /** Optional; set via {@link #setMetadataSyncTrigger} after the listener is constructed. */
    @Setter
    private volatile MetadataSyncTrigger metadataSyncTrigger;

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    /** In-memory dead-letter queue – holds tasks that exhausted all retries. */
    private final Deque<FailedUploadTask> dlq = new ConcurrentLinkedDeque<>();

    /**
     * Retries that are scheduled but have not started running yet. A scheduled task is added here
     * before {@code scheduler.schedule(...)} and removed the moment it starts executing. On
     * {@link #close()}, a forced {@code shutdownNow()} silently discards the executor's queued
     * (delayed) tasks — but those wrappers are opaque, so we cannot recover their payload from the
     * returned runnables. Tracking the payload here lets us move any retry that never ran into the
     * DLQ, so retry intent (the only remaining upload path for a compaction-raced file) is not lost.
     */
    private final Set<RetryContext> pendingRetries = ConcurrentHashMap.newKeySet();

    /**
     * Retries that have STARTED executing but not yet finished. A task moves from
     * {@link #pendingRetries} to this set when it begins running, and is removed when it completes
     * (success, DLQ, or reschedule). On {@link #close()}, a retry still here after the shutdown
     * timeout is one hung in {@code provider.uploadFile(...)}; it is persisted to the DLQ so its
     * upload intent survives even a forced shutdown. DLQ replay is idempotent (idempotent PUT +
     * epoch-guarded confirmation), so a duplicate entry for a retry that actually completed is safe.
     */
    private final Set<RetryContext> inFlightRetries = ConcurrentHashMap.newKeySet();

    /**
     * Upper bound on DLQ entries (in memory and, amortized, on disk). Without a cap, a prolonged
     * provider outage with {@code maxAttempts == 0} routes every failed SST upload straight to the
     * DLQ, growing process memory and the on-disk file without bound (OOM / disk exhaustion).
     * When the cap is exceeded the oldest entries are evicted; those files remain recoverable via
     * the delete guard and startup SST backfill, which re-upload any live SST missing from cloud.
     */
    private static final int DEFAULT_MAX_DLQ_SIZE = 100_000;
    private volatile int maxDlqSize = DEFAULT_MAX_DLQ_SIZE;

    /** Count of DLQ entries evicted due to the size cap (monitoring / tests). */
    private final AtomicInteger droppedDlqEntries = new AtomicInteger(0);

    /**
     * Cumulative, monotonically-increasing count of entries moved to the DLQ since startup (i.e.
     * uploads that EXHAUSTED their retries and are now local-only). Unlike {@link #getDlqSize()},
     * which shrinks on eviction/replay, this only grows — so a consumer can measure the DLQ
     * ENQUEUE RATE (durability-loss rate) by sampling the delta over a window. Used by the listener
     * to fold exhausted-failure pressure into backpressure without pinning the write path on a
     * static, post-recovery DLQ depth.
     */
    private final java.util.concurrent.atomic.AtomicLong dlqEnqueuedTotal =
            new java.util.concurrent.atomic.AtomicLong(0);

    /** Appends since the last on-disk compaction, used to amortize {@link #rewriteDlqFile}. */
    private final AtomicInteger appendsSinceRewrite = new AtomicInteger(0);

    /**
     * DLQ on-disk persistence health. The in-memory DLQ is authoritative at runtime, but the
     * on-disk file is what survives a process restart. If a persist (append/rewrite) fails, retry
     * intent for the failed entry would be lost on a crash before the next successful persist.
     * We flip this to {@code false} and increment {@link #dlqPersistenceFailures} so operators can
     * alert on a "degraded durability" state via {@link #isDlqPersistenceHealthy()} rather than
     * treating a silently-swallowed IO error as healthy. It recovers to {@code true} on the next
     * successful persist. Live SSTs remain recoverable via the delete guard and startup backfill,
     * so this is a degraded signal, not hard data loss.
     */
    @Getter
    private volatile boolean dlqPersistenceHealthy = true;

    /** Cumulative count of DLQ persistence failures (monitoring / tests). */
    private final AtomicInteger dlqPersistenceFailures = new AtomicInteger(0);

    /** Absolute path of the on-disk DLQ file. */
    private final Path dlqFile;

    /** Background thread pool used to schedule retries. */
    private final ScheduledExecutorService scheduler;

    /** Counter of in-flight retry tasks (for testing / monitoring). */
    private final AtomicInteger inFlightCount = new AtomicInteger(0);

    /** Serialises all DLQ file mutations (append and rewrite) across scheduler threads. */
    private final ReentrantLock dlqFileLock = new ReentrantLock();

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    /**
     * @param maxAttempts      maximum whole-file upload retries after a first failure.
     *                         {@code 0} means no retries – failures go directly to DLQ
     *                         (provider is expected to handle its own retries internally).
     *                         Positive values enable exponential-backoff whole-file retries.
     * @param initialDelayMs   delay before the first retry, in milliseconds; clamped to &ge; 100.
     *                         Ignored when {@code maxAttempts == 0}.
     * @param maxDelayMs       upper bound for exponential backoff delay; clamped to
     *                         &ge; {@code initialDelayMs}. Ignored when {@code maxAttempts == 0}.
     * @param dataRoot         absolute path of the store's data directory – the DLQ file is
     *                         written here
     */
    public CloudUploadRetryQueue(int maxAttempts, long initialDelayMs,
                                 long maxDelayMs, String dataRoot) {
        this(maxAttempts, initialDelayMs, maxDelayMs, dataRoot,
             (UploadConfirmedCallback) null);
    }

    /**
     * Legacy convenience constructor accepting a simple {@link java.util.function.BiConsumer}.
     * The epoch parameter is not forwarded; use the {@link UploadConfirmedCallback} overload for
     * epoch-safe confirmation.
     */
    public CloudUploadRetryQueue(int maxAttempts, long initialDelayMs,
                                 long maxDelayMs, String dataRoot,
                                 java.util.function.BiConsumer<String, String> onUploadConfirmed) {
        this(maxAttempts, initialDelayMs, maxDelayMs, dataRoot,
             onUploadConfirmed == null ? null
                                      : (db, path, epoch) -> onUploadConfirmed.accept(db, path));
    }

    /**
     * @param onUploadConfirmed epoch-aware callback invoked on every successful retry/DLQ-replay
     *                          upload; pass {@code null} for none.
     */
    public CloudUploadRetryQueue(int maxAttempts, long initialDelayMs,
                                 long maxDelayMs, String dataRoot,
                                 UploadConfirmedCallback onUploadConfirmed) {
        this(maxAttempts, initialDelayMs, maxDelayMs, dataRoot, onUploadConfirmed,
             DEFAULT_MAX_DLQ_SIZE);
    }

    /**
     * Fully-configured constructor. Prefer this over the {@link #setMaxDlqSize(int)} setter when the
     * cap is known at construction time: setting it here applies the cap <em>before</em> the on-disk
     * DLQ is loaded, so a large persisted file is bounded to the configured cap during the load
     * itself — avoiding the post-load trim-and-rewrite the setter must perform.
     *
     * @param onUploadConfirmed epoch-aware callback invoked on every successful retry/DLQ-replay
     *                          upload; pass {@code null} for none.
     * @param maxDlqSize        maximum number of DLQ entries retained before the oldest are evicted;
     *                          a non-positive value falls back to {@link #DEFAULT_MAX_DLQ_SIZE} so a
     *                          misconfiguration cannot turn the DLQ into a zero-capacity queue.
     */
    public CloudUploadRetryQueue(int maxAttempts, long initialDelayMs,
                                 long maxDelayMs, String dataRoot,
                                 UploadConfirmedCallback onUploadConfirmed, int maxDlqSize) {
        this.maxAttempts = Math.max(0, maxAttempts);
        this.initialDelayMs = Math.max(100L, initialDelayMs);
        this.maxDelayMs = Math.max(this.initialDelayMs, maxDelayMs);
        this.onUploadConfirmed = onUploadConfirmed != null
                                 ? onUploadConfirmed
                                 : (db, path, epoch) -> { };
        this.dlqFile = Paths.get(dataRoot, DLQ_FILE_NAME);
        // Apply the cap before loadDlqFromDisk() so the load bounds the in-memory DLQ (and the
        // rewritten on-disk file) to the configured value directly. A non-positive value keeps the
        // default cap set at field initialisation, matching setMaxDlqSize()'s guard.
        if (maxDlqSize <= 0) {
            log.warn("Ignoring non-positive DLQ max size {}; keeping default {}",
                     maxDlqSize, this.maxDlqSize);
        } else {
            this.maxDlqSize = maxDlqSize;
        }

        java.util.concurrent.ScheduledThreadPoolExecutor sched =
                new java.util.concurrent.ScheduledThreadPoolExecutor(2, r -> {
                    Thread t = new Thread(r, "cloud-upload-retry");
                    t.setDaemon(true);
                    return t;
                });
        // On shutdown(), cancel not-yet-due delayed retries instead of holding the executor open
        // until their (possibly minutes-long) delay elapses. close() then rescues these pending
        // retries into the DLQ via drainPendingRetriesToDlq(), so their upload intent is preserved
        // without a slow, pointless wait for retries that will never get to run.
        sched.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.scheduler = sched;

        loadDlqFromDisk();
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Submits a failed upload to the retry queue or directly to the DLQ.
     *
     * <p>If {@code maxAttempts == 0} (opt-in mode, not the shipped default), the task goes directly
     * to the DLQ — the provider is expected to handle its own retries internally.
     *
     * <p>If {@code maxAttempts > 0} (the default is {@code 3}), the first retry is scheduled after
     * {@code initialDelayMs} ms with exponential backoff. After all attempts are exhausted the task
     * is moved to the DLQ.
     *
     * <p>If the file no longer exists at retry time (compacted away by RocksDB) the task is
     * silently dropped.
     *
     * @param dbName    RocksDB instance name (partition id)
     * @param cfName    column-family name
     * @param filePath  absolute local path of the SST file (also used for confirmation)
     * @param remoteKey remote object key (relative to bucket root)
     * @param cause     the exception from the first upload attempt
     */
    public void submit(String dbName, String cfName, String filePath,
                       String remoteKey, Exception cause) {
        submitPinned(dbName, cfName, filePath, filePath, remoteKey, 0L, cause);
    }

    /**
     * Epoch-aware variant of {@link #submit}. Callers that use an epoch-guarded confirmation
     * callback ({@link CloudSyncTracker#markConfirmedIfEpoch}) MUST use this overload and pass the
     * epoch captured before the upload was initiated. The plain {@link #submit} hardcodes epoch
     * {@code 0}, which never matches a live {@link CloudSyncTracker} epoch (they start at 1), so a
     * retried upload confirmed through it would be silently dropped.
     *
     * @param uploadEpoch the {@link CloudSyncTracker} epoch captured at upload-initiation time
     */
    public void submit(String dbName, String cfName, String filePath,
                       String remoteKey, long uploadEpoch, Exception cause) {
        submitPinned(dbName, cfName, filePath, filePath, remoteKey, uploadEpoch, cause);
    }

    /**
     * Submits a failed upload where the upload file is a hard-linked staging copy that
     * differs from the original source SST.
     *
     * <p>The retry will upload from {@code pinnedPath} (the staging hard-link). On success,
     * {@code onUploadConfirmed} is called with {@code sourceSstPath} (the original {@code *.sst}
     * path), and the staging file at {@code pinnedPath} is deleted.
     *
     * @param dbName        RocksDB instance name (partition id)
     * @param cfName        column-family name
     * @param pinnedPath    path of the staging hard-link to upload from
     * @param sourceSstPath original {@code *.sst} path used for confirmation and cleanup
     * @param remoteKey     remote object key (relative to bucket root)
     * @param cause         the exception from the first upload attempt
     */
    public void submitPinned(String dbName, String cfName, String pinnedPath,
                             String sourceSstPath, String remoteKey, Exception cause) {
        submitPinned(dbName, cfName, pinnedPath, sourceSstPath, remoteKey, 0L, cause);
    }

    /**
     * Epoch-aware variant. The {@code uploadEpoch} is passed through to the
     * {@link UploadConfirmedCallback} so that stale callbacks after DB recreation are discarded.
     */
    public void submitPinned(String dbName, String cfName, String pinnedPath,
                             String sourceSstPath, String remoteKey,
                             long uploadEpoch, Exception cause) {
        if (cause instanceof CloudStorageNonRetryableException || maxAttempts == 0) {
            moveToDlq(dbName, cfName, pinnedPath, sourceSstPath, remoteKey, uploadEpoch, 0,
                      cause.getMessage() != null ? cause.getMessage() : "non-retryable");
            return;
        }
        // This method is invoked from RocksDB flush/compaction callbacks (via
        // CloudStorageEventListener). It must never throw back across that JNI/event boundary.
        // When the scheduler is shutting down, scheduler.schedule(...) throws
        // RejectedExecutionException — in that race we persist the task directly to the DLQ
        // (best-effort disk append) so the retry intent is not lost, and return normally.
        try {
            scheduleRetry(dbName, cfName, pinnedPath, sourceSstPath, remoteKey, uploadEpoch,
                          1, 0);
        } catch (RejectedExecutionException e) {
            log.warn("Cloud upload retry scheduling rejected (scheduler shutting down); "
                     + "persisting directly to DLQ: db={}, cf={}, path={}",
                     dbName, cfName, pinnedPath);
            moveToDlq(dbName, cfName, pinnedPath, sourceSstPath, remoteKey, uploadEpoch, 1,
                      "retry scheduling rejected during shutdown: "
                      + (cause.getMessage() != null ? cause.getMessage() : cause.toString()));
        }
    }

    /**
     * Returns an unmodifiable snapshot of all current DLQ entries.
     */
    public List<FailedUploadTask> getDlqEntries() {
        return List.copyOf(dlq);
    }

    /**
     * Returns the number of entries currently in the DLQ.
     */
    public int getDlqSize() {
        return dlq.size();
    }

    /**
     * Returns the number of DLQ entries evicted because the DLQ size cap was exceeded (e.g. during
     * a prolonged provider outage). A non-zero value indicates local-only files whose DLQ record
     * was dropped; they remain recoverable via the delete guard and startup SST backfill.
     */
    public int getDroppedDlqCount() {
        return droppedDlqEntries.get();
    }

    /**
     * Returns the cumulative, monotonic count of uploads that have EXHAUSTED their retries and been
     * moved to the DLQ since startup. Sampling the delta over a window yields the DLQ enqueue rate
     * (the rate at which uploads become local-only) — the durability-risk signal used for
     * backpressure. Never decreases (unlike {@link #getDlqSize()}), so a static, post-recovery DLQ
     * depth contributes zero rate and does not keep the write path throttled.
     */
    public long getDlqEnqueuedTotal() {
        return dlqEnqueuedTotal.get();
    }

    /** Cumulative number of DLQ on-disk persistence failures since startup (monitoring / tests). */
    public int getDlqPersistenceFailureCount() {
        return dlqPersistenceFailures.get();
    }

    /** Fires the metadata-sync trigger (if wired), swallowing any error — the upload is durable. */
    private void triggerMetadataSync(String dbName) {
        MetadataSyncTrigger trigger = this.metadataSyncTrigger;
        if (trigger == null) {
            return;
        }
        try {
            trigger.onUploadDurable(dbName);
        } catch (Exception e) {
            log.warn("Post-upload metadata-sync trigger failed for db={} (upload is durable, "
                     + "CURRENT/MANIFEST will catch up on the next sync): {}", dbName,
                     e.getMessage());
        }
    }

    /**
     * Sets the maximum number of DLQ entries retained before the oldest are evicted. A
     * non-positive value is ignored (keeps the current cap) so a misconfiguration cannot turn the
     * DLQ into a zero-capacity queue that drops every entry.
     */
    public void setMaxDlqSize(int maxSize) {
        if (maxSize <= 0) {
            log.warn("Ignoring non-positive DLQ max size {}; keeping {}", maxSize, this.maxDlqSize);
            return;
        }
        this.maxDlqSize = maxSize;
        // The startup load bounds the in-memory DLQ to the DEFAULT cap (the configured value is not
        // known during construction). If the operator configured a SMALLER cap, apply it now so a
        // large persisted file loaded at startup is trimmed to the configured bound and the on-disk
        // file is rewritten to match — otherwise up to DEFAULT_MAX_DLQ_SIZE stale entries could
        // linger in memory and on disk despite a tighter configuration.
        int evicted = 0;
        while (dlq.size() > maxSize) {
            if (dlq.pollFirst() == null) {
                break;
            }
            evicted++;
        }
        if (evicted > 0) {
            int total = droppedDlqEntries.addAndGet(evicted);
            rewriteDlqFile();
            log.warn("DLQ: max size lowered to {} — evicted {} oldest entry(ies) to honor the "
                     + "configured cap (total dropped={}).", maxSize, evicted, total);
        }
    }

    /** Test seam: cap provider-unavailable reschedules so terminal behavior is deterministic. */
    @SuppressWarnings("SameParameterValue")
    void setMaxProviderUnavailableRetriesForTest(int retries) {
        this.maxProviderUnavailableRetries = Math.max(1, retries);
    }

    /** Test seam: speed up provider-unavailable retry cadence for deterministic timing tests. */
    @SuppressWarnings("SameParameterValue")
    void setProviderUnavailableMaxDelayMsForTest(long maxDelayMs) {
        this.providerUnavailableMaxDelayMs = Math.max(10L, maxDelayMs);
    }

    /**
     * Returns the number of retry tasks currently scheduled or executing.
     */
    public int getInFlightCount() {
        return inFlightCount.get();
    }

    /**
     * Replays all DLQ entries immediately by attempting to re-upload each file.
     *
     * <ul>
     *   <li>Entries that succeed are removed from the in-memory DLQ and the DLQ file is
     *       rewritten without them.</li>
     *   <li>Entries whose local file no longer exists are silently dropped.</li>
     *   <li>Entries that fail are re-submitted to the retry queue with a fresh attempt cycle
     *       (they are removed from the DLQ for now and will re-enter it only if all retries
     *       fail again).</li>
     * </ul>
     */
    public void replayDlq() {
        // Drain the whole DLQ atomically so concurrent appends are not affected.
        List<FailedUploadTask> snapshot = new ArrayList<>();
        FailedUploadTask entry;
        while ((entry = dlq.pollFirst()) != null) {
            snapshot.add(entry);
        }
        if (snapshot.isEmpty()) {
            log.info("DLQ replay: queue is empty, nothing to do");
            return;
        }
        log.info("DLQ replay started: {} entries", snapshot.size());

        int succeeded = 0;
        int dropped = 0;
        int requeued = 0;

        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();

        // Process each task. Re-queue failures back into dlq BEFORE rewriting the file so
        // that a JVM crash during replay doesn't lose tasks that are in-flight in the scheduler.
        for (FailedUploadTask task : snapshot) {
            // Prefer the staged hard-link (filePath); if it was already cleaned up, fall back to the
            // original source SST which may still exist and is equally uploadable. Only drop when
            // NEITHER path is present — dropping on filePath alone would discard a recoverable upload
            // and, if the source is compacted shortly after, lose the mirror opportunity entirely.
            String uploadPath = null;
            if (Files.exists(Paths.get(task.getFilePath()))) {
                uploadPath = task.getFilePath();
            } else {
                String source = task.getSourceSstPath();
                if (source != null && !source.equals(task.getFilePath())
                        && Files.exists(Paths.get(source))) {
                    log.info("DLQ replay: staging pin gone, falling back to source SST: db={}, "
                             + "pin={}, source={}", task.getDbName(), task.getFilePath(), source);
                    uploadPath = source;
                }
            }
            if (uploadPath == null) {
                log.info("DLQ replay: neither staging pin nor source SST exists, dropping: "
                         + "db={}, pin={}, source={}", task.getDbName(), task.getFilePath(),
                         task.getSourceSstPath());
                dropped++;
                continue;
            }
            if (provider == null) {
                // No provider — put back on DLQ (will be persisted below).
                dlq.addLast(task);
                requeued++;
                continue;
            }
            try {
                provider.uploadFile(uploadPath, task.getRemoteKey());
            } catch (Exception e) {
                log.warn("DLQ replay failed, re-queuing: db={}, cf={}, path={}, reason={}",
                         task.getDbName(), task.getCfName(), task.getFilePath(), e.getMessage());
                // Put back on in-memory DLQ first; rewriteDlqFile below will persist it.
                dlq.addLast(task);
                requeued++;
                continue;
            }
            // Upload succeeded — invoke callback with sourceSstPath and the original epoch so
            // CloudSyncTracker can call markConfirmedIfEpoch and drop stale callbacks after
            // DB recreation with reused file numbers.
            try {
                onUploadConfirmed.onConfirmed(task.getDbName(), task.getSourceSstPath(),
                                              task.getUploadEpoch());
            } catch (Exception e) {
                log.warn("DLQ replay: onUploadConfirmed threw after successful upload "
                         + "(upload is durable, ignoring): db={}, path={}, reason={}",
                         task.getDbName(), task.getSourceSstPath(), e.getMessage());
            }
            // Advance the mirrored recovery point now that this SST is durable again.
            triggerMetadataSync(task.getDbName());
            // If this was a pinned staging file, clean it up.
            if (!task.getFilePath().equals(task.getSourceSstPath())) {
                try {
                    Files.deleteIfExists(Paths.get(task.getFilePath()));
                } catch (IOException e) {
                    log.debug("DLQ replay: failed to clean up staging file: {}",
                              task.getFilePath());
                }
            }
            log.info("DLQ replay succeeded: db={}, cf={}, path={}",
                     task.getDbName(), task.getCfName(), task.getFilePath());
            succeeded++;
        }

        // Rewrite the on-disk DLQ to match the current in-memory state (failed tasks re-added
        // above are now persisted; succeeded/dropped tasks are absent).
        rewriteDlqFile();

        log.info("DLQ replay finished: succeeded={}, dropped={}, requeued={}",
                 succeeded, dropped, requeued);
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /**
     * Shuts down the background retry executor, waiting up to 10 seconds for
     * in-flight tasks to complete.
     */
    @Override
    public void close() {
        close(10, TimeUnit.SECONDS);
    }

    /**
     * Shuts down with a caller-specified drain timeout. Package-private so tests can force a fast
     * shutdown (they do not want to wait the production 10 s for a deliberately hung upload).
     */
    void close(long timeout, TimeUnit unit) {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(timeout, unit)) {
                log.warn("CloudUploadRetryQueue: executor did not terminate within {}{}; "
                         + "forcing shutdown ({} tasks still in flight)",
                         timeout, unit, inFlightCount.get());
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            // A forced shutdownNow() discards scheduled-but-unrun retries AND interrupts (but cannot
            // guarantee prompt exit of) retries hung in a slow provider.uploadFile(...). Persist the
            // intent of BOTH so a compaction-raced file's only remaining upload path is not lost. If
            // the executor drained gracefully both sets are already empty (each task removed itself),
            // so this is a no-op on the happy path.
            drainUnfinishedRetriesToDlq();
        }
    }

    /**
     * Moves any scheduled-but-unrun ({@link #pendingRetries}) and started-but-unfinished
     * ({@link #inFlightRetries}) retries into the DLQ (called from {@link #close()}). Duplicates are
     * tolerated: DLQ replay re-uploads idempotently and confirms via the epoch guard.
     */
    private void drainUnfinishedRetriesToDlq() {
        int rescued = 0;
        rescued += drainRetrySet(pendingRetries,
                                 "retry not executed before shutdown (forced) — persisted to DLQ");
        rescued += drainRetrySet(inFlightRetries,
                                 "retry still in flight at shutdown (forced) — persisted to DLQ");
        if (rescued > 0) {
            log.warn("CloudUploadRetryQueue: rescued {} unfinished retry(ies) into the DLQ during "
                     + "shutdown so their upload intent survives restart.", rescued);
        }
    }

    /** Drains one retry-context set into the DLQ, claiming each entry exactly once. */
    private int drainRetrySet(Set<RetryContext> set, String reason) {
        int rescued = 0;
        for (RetryContext ctx : set) {
            if (!set.remove(ctx)) {
                continue; // raced with the task itself removing it; it will handle its own outcome
            }
            moveToDlq(ctx.dbName, ctx.cfName, ctx.filePath, ctx.sourceSstPath, ctx.remoteKey,
                      ctx.uploadEpoch, ctx.attempt, reason);
            rescued++;
        }
        return rescued;
    }

    // -----------------------------------------------------------------------
    // Internal – retry scheduling
    // -----------------------------------------------------------------------

    private void scheduleRetry(String dbName, String cfName, String filePath,
                                String sourceSstPath, String remoteKey,
                                long uploadEpoch, int attempt,
                                int providerUnavailableRetries) {
        long delayMs = providerUnavailableRetries > 0
                       ? computeProviderUnavailableDelay(providerUnavailableRetries)
                       : computeDelay(attempt);
        log.info("Cloud upload retry scheduled: db={}, cf={}, path={}, attempt={}/{}, "
                 + "providerUnavailableRetries={}, delayMs={}",
                 dbName, cfName, filePath, attempt, maxAttempts,
                 providerUnavailableRetries, delayMs);
        // Increment before schedule; decrement here on rejection (executeRetry's finally
        // handles the decrement on the happy path, but RejectedExecutionException bypasses it).
        inFlightCount.incrementAndGet();
        // Record the payload so a forced shutdown can rescue this retry into the DLQ if it never
        // runs. Removed when the task starts executing (see wrapper below).
        RetryContext ctx = new RetryContext(dbName, cfName, filePath, sourceSstPath, remoteKey,
                                            uploadEpoch, attempt,
                                            providerUnavailableRetries);
        pendingRetries.add(ctx);
        try {
            scheduler.schedule(() -> {
                // Move from "scheduled" to "in-flight" so close() can still rescue this retry's
                // intent if the upload below hangs past the shutdown timeout.
                pendingRetries.remove(ctx);
                inFlightRetries.add(ctx);
                try {
                    executeRetry(dbName, cfName, filePath, sourceSstPath, remoteKey,
                                 uploadEpoch, attempt, providerUnavailableRetries);
                } finally {
                    inFlightRetries.remove(ctx);
                }
            }, delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            pendingRetries.remove(ctx);
            inFlightCount.decrementAndGet();
            throw e;
        }
    }

    /** Immutable payload of a scheduled-but-not-yet-started retry, tracked in {@link #pendingRetries}. */
    private static final class RetryContext {
        final String dbName;
        final String cfName;
        final String filePath;
        final String sourceSstPath;
        final String remoteKey;
        final long uploadEpoch;
        final int attempt;
        final int providerUnavailableRetries;

        RetryContext(String dbName, String cfName, String filePath, String sourceSstPath,
                     String remoteKey, long uploadEpoch, int attempt,
                     int providerUnavailableRetries) {
            this.dbName = dbName;
            this.cfName = cfName;
            this.filePath = filePath;
            this.sourceSstPath = sourceSstPath;
            this.remoteKey = remoteKey;
            this.uploadEpoch = uploadEpoch;
            this.attempt = attempt;
            this.providerUnavailableRetries = providerUnavailableRetries;
        }
    }

    private void executeRetry(String dbName, String cfName, String filePath,
                               String sourceSstPath, String remoteKey,
                               long uploadEpoch, int attempt,
                               int providerUnavailableRetries) {
        try {
            // If the local SST (or staging pin) is gone, drop silently – nothing to upload.
            if (!Files.exists(Paths.get(filePath))) {
                log.info("Cloud upload retry: local file no longer exists (compacted?), "
                         + "dropping: db={}, cf={}, path={}", dbName, cfName, filePath);
                return;
            }

            CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
            if (provider == null) {
                int nextUnavailableRetries = providerUnavailableRetries + 1;
                if (nextUnavailableRetries > maxProviderUnavailableRetries) {
                    log.error("Cloud upload retry: provider unavailable for {} consecutive cycle(s); "
                              + "moving to DLQ: db={}, cf={}, path={}",
                              providerUnavailableRetries, dbName, cfName, filePath);
                    moveToDlq(dbName, cfName, filePath, sourceSstPath, remoteKey, uploadEpoch,
                              attempt, "No active provider (exceeded unavailable retry limit)");
                    return;
                }
                // Provider-unavailable windows are infrastructure/transient state, not upload
                // attempts. Keep the same upload-attempt number, but use a dedicated backoff
                // counter so long outages do not hot-loop the scheduler.
                log.warn("Cloud upload retry: no active provider (upload attempt {}/{}; "
                         + "providerUnavailableRetries={}/{}), rescheduling without consuming "
                         + "retry budget: db={}, cf={}, path={}",
                         attempt, maxAttempts,
                         providerUnavailableRetries, maxProviderUnavailableRetries,
                         dbName, cfName, filePath);
                try {
                    scheduleRetry(dbName, cfName, filePath, sourceSstPath, remoteKey,
                                  uploadEpoch, attempt, nextUnavailableRetries);
                } catch (RejectedExecutionException ree) {
                    // Same shutdown-race handling as the generic failure path below: ensure the
                    // upload intent is still durable via DLQ.
                    log.warn("Cloud upload retry reschedule rejected (scheduler shutting down); "
                             + "moving to DLQ: db={}, cf={}, path={}", dbName, cfName, filePath);
                    moveToDlq(dbName, cfName, filePath, sourceSstPath, remoteKey, uploadEpoch,
                              attempt, "retry reschedule rejected during shutdown");
                }
                return;
            }

            provider.uploadFile(filePath, remoteKey);
            // Confirm using the original *.sst path so CloudSyncTracker can parse the file number.
            // Pass the epoch so markConfirmedIfEpoch can drop stale callbacks after DB recreation.
            try {
                onUploadConfirmed.onConfirmed(dbName, sourceSstPath, uploadEpoch);
            } catch (Exception callbackError) {
                // Upload already succeeded; callback failures are control-plane/observer failures,
                // not data-plane upload failures. Keep the upload path successful and avoid
                // misclassifying into retry/DLQ.
                log.warn("Cloud upload retry callback failed after successful upload: db={}, "
                         + "path={}, reason={}",
                         dbName, sourceSstPath, callbackError.getMessage());
            }
            // The SST set is now more durable than the mirrored CURRENT/MANIFEST — trigger a
            // (debounced) metadata publish so the recovery point advances even if this retry
            // succeeded during an otherwise-quiet period on an idle DB.
            triggerMetadataSync(dbName);
            // If we uploaded from a staging hard-link, clean it up now.
            if (!filePath.equals(sourceSstPath)) {
                try {
                    Files.deleteIfExists(Paths.get(filePath));
                } catch (IOException e) {
                    log.debug("Failed to clean up staging file after retry: {}", filePath);
                }
            }
            log.info("Cloud upload retry succeeded: db={}, cf={}, path={}, attempt={}",
                     dbName, cfName, filePath, attempt);

        } catch (Exception e) {
            log.warn("Cloud upload retry failed: db={}, cf={}, path={}, attempt={}/{}, reason={}",
                     dbName, cfName, filePath, attempt, maxAttempts, e.getMessage());
            if (e instanceof CloudStorageNonRetryableException) {
                moveToDlq(dbName, cfName, filePath, sourceSstPath, remoteKey, uploadEpoch,
                          attempt, e.getMessage() != null ? e.getMessage() : "non-retryable");
                return;
            }
            if (attempt >= maxAttempts) {
                moveToDlq(dbName, cfName, filePath, sourceSstPath, remoteKey, uploadEpoch,
                          attempt, e.getMessage());
            } else {
                try {
                    scheduleRetry(dbName, cfName, filePath, sourceSstPath, remoteKey,
                                  uploadEpoch, attempt + 1, 0);
                } catch (RejectedExecutionException ree) {
                    // Scheduler is shutting down. scheduleRetry rethrows the rejection; if we let
                    // it escape here it would propagate out of this scheduler task and be silently
                    // swallowed by the executor, leaving the file neither retried nor recorded.
                    // Persist it to the DLQ so it survives as a local-only entry recoverable on
                    // restart.
                    log.warn("Cloud upload retry reschedule rejected (scheduler shutting down); "
                             + "moving to DLQ: db={}, cf={}, path={}", dbName, cfName, filePath);
                    moveToDlq(dbName, cfName, filePath, sourceSstPath, remoteKey, uploadEpoch,
                              attempt, "retry reschedule rejected during shutdown");
                }
            }
        } finally {
            inFlightCount.decrementAndGet();
        }
    }

    private void moveToDlq(String dbName, String cfName, String filePath,
                            String sourceSstPath, String remoteKey,
                            long uploadEpoch, int attemptCount, String lastError) {
        FailedUploadTask task = new FailedUploadTask(dbName, cfName, filePath,
                                                     sourceSstPath, remoteKey,
                                                     System.currentTimeMillis(),
                                                     attemptCount, lastError, uploadEpoch);
        dlq.addLast(task);
        // Count this exhausted-retry upload for the durability-loss (enqueue) rate signal. Monotonic
        // so it is unaffected by the eviction/replay below that mutate the live DLQ size.
        dlqEnqueuedTotal.incrementAndGet();

        // Enforce the size cap: evict oldest entries so a sustained outage cannot grow the DLQ
        // (and process memory) without bound. Approximate under concurrency (the deque may briefly
        // exceed the cap by the number of concurrent movers), which is fine.
        int evicted = 0;
        while (dlq.size() > maxDlqSize) {
            if (dlq.pollFirst() == null) {
                break;
            }
            evicted++;
        }
        if (evicted > 0) {
            int total = droppedDlqEntries.addAndGet(evicted);
            log.warn("Cloud upload DLQ exceeded cap {} — evicted {} oldest entry(ies) "
                     + "(total dropped={}). Live SSTs remain recoverable via delete guard / "
                     + "startup backfill.", maxDlqSize, evicted, total);
        }

        appendDlqEntryToDisk(task);
        // Amortized disk compaction: periodically rewrite the on-disk file from the (capped)
        // in-memory DLQ so the file cannot grow without bound across a long outage.
        int appends = appendsSinceRewrite.incrementAndGet();
        if (appends >= maxDlqSize && appendsSinceRewrite.compareAndSet(appends, 0)) {
            rewriteDlqFile();
        }

        log.error("Cloud upload moved to DLQ after {} attempt(s) – file is local-only: "
                  + "db={}, cf={}, path={}, remoteKey={}",
                  attemptCount, dbName, cfName, filePath, remoteKey);
    }

    /**
     * Exponential back-off: {@code initialDelayMs * 2^(attempt-1)}, capped at {@code maxDelayMs}.
     */
    private long computeDelay(int attempt) {
        // Guard against overflow for large attempt numbers.
        int exp = Math.min(attempt - 1, 30);
        long delay = initialDelayMs * (1L << exp);
        return Math.min(delay, maxDelayMs);
    }

    /** Backoff for provider-unavailable cycles (separate from upload-attempt backoff). */
    private long computeProviderUnavailableDelay(int providerUnavailableRetries) {
        int exp = Math.min(Math.max(providerUnavailableRetries - 1, 0), 30);
        long delay = initialDelayMs * (1L << exp);
        return Math.min(delay, providerUnavailableMaxDelayMs);
    }

    // -----------------------------------------------------------------------
    // Internal – DLQ persistence
    // -----------------------------------------------------------------------

    /** Loads any persisted DLQ entries from disk at startup. */
    private void loadDlqFromDisk() {
        if (!Files.exists(dlqFile)) {
            return;
        }
        int loaded = 0;
        int dropped = 0;
        try (BufferedReader reader = Files.newBufferedReader(dlqFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                FailedUploadTask task = deserialize(line);
                if (task == null) {
                    continue;
                }
                // Bound memory while loading: a DLQ file grown large during a long outage must not
                // be loaded unbounded (startup latency spike / OOM before the node is healthy).
                // Keep the newest maxDlqSize entries by evicting the oldest as we go.
                dlq.addLast(task);
                loaded++;
                while (dlq.size() > maxDlqSize) {
                    if (dlq.pollFirst() == null) {
                        break;
                    }
                    dropped++;
                }
            }
        } catch (IOException e) {
            log.warn("DLQ: failed to load persisted entries from {}: {}",
                     dlqFile, e.getMessage());
        }
        if (dropped > 0) {
            int total = droppedDlqEntries.addAndGet(dropped);
            // Rewrite the on-disk file down to the bounded in-memory set so a large file cannot be
            // re-read unbounded on the next restart either.
            rewriteDlqFile();
            log.warn("DLQ: persisted file exceeded cap {} — dropped {} oldest entry(ies) during "
                     + "load (total dropped={}). Live SSTs remain recoverable via delete guard / "
                     + "startup backfill.", maxDlqSize, dropped, total);
        }
        int retained = loaded - dropped;
        if (retained > 0) {
            log.warn("DLQ: loaded {} pending failed upload(s) from disk – "
                     + "call replayDlq() to retry them (file={})", retained, dlqFile);
        }
    }

    /** Appends a single DLQ entry to the on-disk file. */
    private void appendDlqEntryToDisk(FailedUploadTask task) {
        dlqFileLock.lock();
        try {
            if (!dlqPersistenceHealthy) {
                // Degraded: a prior append/rewrite failed, so older in-memory entries may never have
                // reached disk. A plain append of just THIS task would not make those durable — and
                // must not flip health back to green (a crash would still lose the memory-only
                // entries). Do a FULL rewrite of the in-memory DLQ instead: it persists every
                // outstanding entry (including `task`, already added by moveToDlq) and, on success,
                // is the only thing that legitimately restores healthy status.
                rewriteDlqFile();
                return;
            }
            boolean newFile = !Files.exists(dlqFile);
            try (BufferedWriter bw = Files.newBufferedWriter(
                    dlqFile, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND)) {
                if (newFile) {
                    bw.write(DLQ_COMMENT);
                    bw.newLine();
                }
                bw.write(serialize(task));
                bw.newLine();
            }
            // Deliberately do NOT restore health here: a single successful append does not prove all
            // outstanding entries are durable. Health is restored only by a successful full rewrite.
        } catch (IOException e) {
            markDlqPersistenceFailed("append", e);
        } finally {
            dlqFileLock.unlock();
        }
    }

    /**
     * Records that the full in-memory DLQ was persisted (via a successful {@link #rewriteDlqFile}),
     * clearing any prior degraded state. Only a full rewrite proves EVERY outstanding entry is
     * durable, so this is the single legitimate path back to healthy.
     */
    private void markDlqPersistenceHealthy() {
        if (!dlqPersistenceHealthy) {
            dlqPersistenceHealthy = true;
            log.info("DLQ: on-disk persistence recovered via full rewrite (file={})", dlqFile);
        }
    }

    /**
     * Records a DLQ persistence failure. Unlike a plain {@code log.warn}, this flips the queue into
     * a degraded state observable via {@link #isDlqPersistenceHealthy()} so a crash-window where
     * retry intent could be lost does not masquerade as healthy durability.
     */
    private void markDlqPersistenceFailed(String op, IOException e) {
        dlqPersistenceHealthy = false;
        int failures = dlqPersistenceFailures.incrementAndGet();
        log.error("DLQ: on-disk {} failed (file={}, totalFailures={}) — retry intent for affected "
                  + "entries is not durable and would be lost on a crash before the next successful "
                  + "persist. Live SSTs remain recoverable via delete guard / startup backfill: {}",
                  op, dlqFile, failures, e.getMessage());
    }

    /**
     * Rewrites the DLQ file from the current in-memory DLQ (used after replay / compaction).
     *
     * <p>Crash-safe: the new contents are written to a sibling {@code *.tmp} file, fsync'd, then
     * atomically renamed over the original, and finally the parent directory is fsync'd. This
     * guarantees a crash/power-loss during compaction can only ever leave either the complete old
     * file or the complete new file — never a truncated/empty DLQ that would silently drop pending
     * failed uploads. (An in-place truncate-then-rewrite has exactly that failure window.)
     */
    private void rewriteDlqFile() {
        dlqFileLock.lock();
        try {
            Path tmp = dlqFile.resolveSibling(dlqFile.getFileName() + ".tmp");
            try (FileChannel channel = FileChannel.open(
                         tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                         StandardOpenOption.TRUNCATE_EXISTING);
                 BufferedWriter bw = new BufferedWriter(
                         new java.io.OutputStreamWriter(
                                 java.nio.channels.Channels.newOutputStream(channel),
                                 StandardCharsets.UTF_8))) {
                bw.write(DLQ_COMMENT);
                bw.newLine();
                for (FailedUploadTask t : dlq) {
                    bw.write(serialize(t));
                    bw.newLine();
                }
                bw.flush();
                // fsync the file contents before the rename so the rename cannot expose an empty
                // (metadata-committed, data-not-yet-flushed) file after a crash.
                channel.force(true);
            }
            try {
                Files.move(tmp, dlqFile,
                           StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                // Filesystem without atomic rename: fall back to a plain replace. Still far safer
                // than an in-place truncate because the fully-written temp file already exists.
                Files.move(tmp, dlqFile, StandardCopyOption.REPLACE_EXISTING);
            }
            fsyncDir(dlqFile.getParent());
            markDlqPersistenceHealthy();
        } catch (IOException e) {
            markDlqPersistenceFailed("rewrite", e);
        } finally {
            dlqFileLock.unlock();
        }
    }

    /** Best-effort directory fsync so a rename is durable across power loss. */
    private static void fsyncDir(Path dir) {
        if (dir == null) {
            return;
        }
        try (FileChannel dirChannel = FileChannel.open(dir, StandardOpenOption.READ)) {
            dirChannel.force(true);
        } catch (IOException e) {
            // Some platforms (notably Windows) cannot open a directory as a channel; the rename is
            // still ordered on those. Nothing actionable — leave the file as-is.
            log.debug("DLQ: directory fsync skipped for {}: {}", dir, e.getMessage());
        }
    }

    // -----------------------------------------------------------------------
    // Internal – serialisation (tab-separated, with escape for special chars)
    // -----------------------------------------------------------------------

    /**
     * Serialises a {@link FailedUploadTask} to a single tab-separated line.
     * Fields: failedAt, attemptCount, dbName, cfName, filePath, remoteKey, lastError,
     *         sourceSstPath, uploadEpoch.
     */
    String serialize(FailedUploadTask task) {
        return task.getFailedAt() + "\t"
               + task.getAttemptCount() + "\t"
               + escape(task.getDbName()) + "\t"
               + escape(task.getCfName()) + "\t"
               + escape(task.getFilePath()) + "\t"
               + escape(task.getRemoteKey()) + "\t"
               + escape(task.getLastError()) + "\t"
               + escape(task.getSourceSstPath()) + "\t"
               + task.getUploadEpoch();
    }

    /** Deserialises a line; returns {@code null} and logs a warning on parse errors. */
    FailedUploadTask deserialize(String line) {
        // Split into at most 9 fields; older versions may have fewer.
        String[] parts = line.split("\t", 9);
        if (parts.length < 7) {
            log.warn("DLQ: skipping malformed line (expected 7+ fields, got {}): {}",
                     parts.length, line);
            return null;
        }
        try {
            long failedAt = Long.parseLong(parts[0].trim());
            int attemptCount = Integer.parseInt(parts[1].trim());
            String filePath = unescape(parts[4]);
            // sourceSstPath field (index 7) was added later; fall back to filePath for old entries.
            String sourceSstPath = parts.length >= 8 ? unescape(parts[7]) : filePath;
            // uploadEpoch field (index 8) was added later; fall back to 0 for old entries.
            long uploadEpoch = parts.length >= 9 ? Long.parseLong(parts[8].trim()) : 0L;
            return new FailedUploadTask(
                    unescape(parts[2]),   // dbName
                    unescape(parts[3]),   // cfName
                    filePath,             // filePath
                    sourceSstPath,        // sourceSstPath
                    unescape(parts[5]),   // remoteKey
                    failedAt,
                    attemptCount,
                    unescape(parts[6]),   // lastError
                    uploadEpoch
            );
        } catch (NumberFormatException e) {
            log.warn("DLQ: failed to parse numeric field in line '{}': {}", line, e.getMessage());
            return null;
        }
    }

    /** Escapes backslash, tab and newline so the serialised form is safe in TSV lines. */
    private static String escape(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        return s.replace("\\", "\\\\")
                .replace("\t", "\\t")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }

    private static String unescape(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        // Process escape sequences left-to-right, handling \\ first to avoid double-decode.
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(i + 1);
                switch (next) {
                    case '\\':
                        sb.append('\\');
                        i++;
                        break;
                    case 't':
                        sb.append('\t');
                        i++;
                        break;
                    case 'n':
                        sb.append('\n');
                        i++;
                        break;
                    case 'r':
                        sb.append('\r');
                        i++;
                        break;
                    default:
                        sb.append(c);
                        break;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}

