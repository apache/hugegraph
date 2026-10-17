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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import org.apache.hugegraph.store.cloud.CloudStorageNonRetryableException;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Asynchronous whole-file retry queue for failed cloud SST uploads.
 *
 * <h3>Retry strategy</h3>
 * <p>The number of whole-file retries is configured by the caller via
 * {@code cloud.storage.upload-retry-max-attempts}, which defaults to {@code 3} (whole-file retries
 * enabled). {@code maxAttempts == 0} is an opt-in mode — not the shipped default — for deployments
 * whose provider already performs sufficient internal retries (e.g. the S3 provider's underlying
 * AWS SDK client, which retries transient per-request/per-part failures on its own).
 * <ul>
 *   <li>When {@code maxAttempts == 0}, the queue gives up on a failure immediately, with no
 *       whole-file retry.</li>
 *   <li>When {@code maxAttempts > 0} (the default), the first retry is scheduled after
 *       {@code initialDelayMs} milliseconds.</li>
 *   <li>Subsequent retries use exponential backoff: {@code delay = initialDelayMs * 2^(attempt-1)},
 *       capped at {@code maxDelayMs}.</li>
 *   <li>After {@code maxAttempts} consecutive failures the queue gives up on the upload.</li>
 *   <li>If the local SST file no longer exists at retry time (compacted away by RocksDB)
 *       the retry is silently dropped — there is nothing to upload.</li>
 * </ul>
 *
 * <h3>Giving up</h3>
 * A given-up upload is logged at ERROR, counted in {@link #getGivenUpCount()}, and its staging
 * hard-link is released; this queue does not retry it again. The file is never marked confirmed
 * in {@link CloudSyncTracker}, so the listener re-uploads it if it is still live:
 * <ul>
 *   <li>the delete guard re-uploads unconfirmed live SSTs before removing any superseded cloud
 *       object, and holds the delete (keeping the old object) until the live set is durable;</li>
 *   <li>metadata sync uploads every SST the captured manifest references before publishing
 *       CURRENT/MANIFEST, so cloud never points at a missing SST;</li>
 *   <li>startup SST backfill uploads every local SST missing from cloud when its DB is opened.</li>
 * </ul>
 * An SST compacted away before then needs no upload; its data lives in the compaction output.
 *
 * <h3>Thread safety</h3>
 * All public methods are thread-safe.
 *
 * <h3>Lifecycle</h3>
 * Call {@link #close()} (e.g. from a Spring {@code @PreDestroy} method) to shut down the
 * background executor gracefully.
 */
@Slf4j
public class CloudUploadRetryQueue implements Closeable {

    // -----------------------------------------------------------------------
    // Configuration
    // -----------------------------------------------------------------------

    private final int maxAttempts;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final CloudStorageProvider provider;

    /**
     * Invoked with {@code (dbName, sourceSstPath, uploadEpoch)} whenever an upload succeeds via
     * retry, so the caller can mark the file confirmed-present in cloud.
     * The {@code uploadEpoch} is the {@link CloudSyncTracker} epoch captured at submission time;
     * the callback should use {@link CloudSyncTracker#markConfirmedIfEpoch} to drop stale
     * callbacks.
     */
    @FunctionalInterface
    public interface UploadConfirmedCallback {
        void onConfirmed(String dbName, String sourceSstPath, long uploadEpoch);
    }

    private final UploadConfirmedCallback onUploadConfirmed;

    /**
     * Invoked with {@code dbName} after an upload becomes durable via retry, so the
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

    /** Number of uploads given up on since startup (monitoring / tests). */
    private final AtomicLong givenUpCount = new AtomicLong();

    /**
     * Retries that are scheduled but have not started running yet. A scheduled task is added here
     * before {@code scheduler.schedule(...)} and removed the moment it starts executing. On
     * {@link #close()}, a forced {@code shutdownNow()} silently discards the executor's queued
     * (delayed) tasks — but those wrappers are opaque, so we cannot recover their payload from the
     * returned runnables. Tracking the payload here lets close() give up on any retry that never
     * ran and release its staging hard-link.
     */
    private final Set<RetryContext> pendingRetries = ConcurrentHashMap.newKeySet();

    /**
     * Retries that have STARTED executing but not yet finished. A task moves from
     * {@link #pendingRetries} to this set when it begins running, and is removed when it completes
     * (success, give-up, or reschedule). On {@link #close()}, a retry still here after the shutdown
     * timeout is one hung in {@code provider.uploadFile(...)}; it is given up on like an unrun
     * retry.
     */
    private final Set<RetryContext> inFlightRetries = ConcurrentHashMap.newKeySet();

    /** Background thread pool used to schedule retries. */
    private final ScheduledExecutorService scheduler;

    /** Counter of in-flight retry tasks (for testing / monitoring). */
    private final AtomicInteger inFlightCount = new AtomicInteger(0);

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    /**
     * Fully-configured constructor.
     *
     * @param provider          initialized cloud storage provider used for retries
     * @param maxAttempts       maximum whole-file upload retries after a first failure.
     *                          {@code 0} means no retries – the queue gives up immediately
     *                          (provider is expected to handle its own retries internally).
     *                          Positive values enable exponential-backoff whole-file retries.
     * @param initialDelayMs    delay before the first retry, in milliseconds; clamped to &ge; 100.
     *                          Ignored when {@code maxAttempts == 0}.
     * @param maxDelayMs        upper bound for exponential backoff delay; clamped to
     *                          &ge; {@code initialDelayMs}. Ignored when {@code maxAttempts == 0}.
     * @param onUploadConfirmed epoch-aware callback invoked on every successful retry upload;
     *                          pass {@code null} for none.
     */
    public CloudUploadRetryQueue(CloudStorageProvider provider, int maxAttempts,
                                 long initialDelayMs, long maxDelayMs,
                                 UploadConfirmedCallback onUploadConfirmed) {
        this.provider = Objects.requireNonNull(provider, "provider");
        this.maxAttempts = Math.max(0, maxAttempts);
        this.initialDelayMs = Math.max(100L, initialDelayMs);
        this.maxDelayMs = Math.max(this.initialDelayMs, maxDelayMs);
        this.onUploadConfirmed = onUploadConfirmed != null
                                 ? onUploadConfirmed
                                 : (db, path, epoch) -> { };

        java.util.concurrent.ScheduledThreadPoolExecutor sched =
                new java.util.concurrent.ScheduledThreadPoolExecutor(2, r -> {
                    Thread t = new Thread(r, "cloud-upload-retry");
                    t.setDaemon(true);
                    return t;
                });
        // On shutdown(), cancel not-yet-due delayed retries instead of holding the executor open
        // until their (possibly minutes-long) delay elapses. close() then gives up on these
        // pending retries via giveUpUnfinishedRetries().
        sched.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.scheduler = sched;
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Submits a failed upload for retry, or gives up on it immediately.
     *
     * <p>If {@code maxAttempts == 0} (opt-in mode, not the shipped default), the queue gives up
     * immediately — the provider is expected to handle its own retries internally.
     *
     * <p>If {@code maxAttempts > 0} (the default is {@code 3}), the first retry is scheduled after
     * {@code initialDelayMs} ms with exponential backoff. After all attempts are exhausted the
     * queue gives up on the upload.
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
     * Epoch-aware variant. The {@code uploadEpoch} is passed through to the
     * {@link UploadConfirmedCallback} so that stale callbacks after DB recreation are discarded.
     */
    public void submitPinned(String dbName, String cfName, String pinnedPath,
                             String sourceSstPath, String remoteKey,
                             long uploadEpoch, Exception cause) {
        if (cause instanceof CloudStorageNonRetryableException || maxAttempts == 0) {
            giveUp(dbName, cfName, pinnedPath, sourceSstPath, remoteKey, uploadEpoch, 0,
                      cause.getMessage() != null ? cause.getMessage() : "non-retryable");
            return;
        }
        // This method is invoked from RocksDB flush/compaction callbacks (via
        // CloudStorageEventListener). It must never throw back across that JNI/event boundary.
        // When the scheduler is shutting down, scheduler.schedule(...) throws
        // RejectedExecutionException — in that race we give up on the upload and return normally.
        try {
            scheduleRetry(dbName, cfName, pinnedPath, sourceSstPath, remoteKey, uploadEpoch,
                          1);
        } catch (RejectedExecutionException e) {
            log.warn("Cloud upload retry scheduling rejected (scheduler shutting down); "
                     + "giving up: db={}, cf={}, path={}",
                     dbName, cfName, pinnedPath);
            giveUp(dbName, cfName, pinnedPath, sourceSstPath, remoteKey, uploadEpoch, 1,
                      "retry scheduling rejected during shutdown: "
                      + (cause.getMessage() != null ? cause.getMessage() : cause.toString()));
        }
    }

    /** Returns the number of uploads given up on since startup. */
    public long getGivenUpCount() {
        return givenUpCount.get();
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
     * Returns the number of retry tasks currently scheduled or executing.
     */
    public int getInFlightCount() {
        return inFlightCount.get();
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
            // A forced shutdownNow() discards scheduled-but-unrun retries AND interrupts (but
            // cannot guarantee prompt exit of) retries hung in a slow provider.uploadFile(...).
            // Give up on both and release their staging pins; startup backfill re-uploads the live
            // SSTs.
            // If the executor drained gracefully both sets are already empty (each task removed
            // itself), so this is a no-op on the happy path.
            giveUpUnfinishedRetries();
        }
    }

    /**
     * Gives up on any scheduled-but-unrun ({@link #pendingRetries}) and started-but-unfinished
     * ({@link #inFlightRetries}) retries (called from {@link #close()}).
     */
    private void giveUpUnfinishedRetries() {
        int rescued = 0;
        rescued += drainRetrySet(pendingRetries,
                                 "retry not executed before shutdown (forced)");
        rescued += drainRetrySet(inFlightRetries,
                                 "retry still in flight at shutdown (forced)");
        if (rescued > 0) {
            log.warn("CloudUploadRetryQueue: gave up on {} unfinished retry(ies) during "
                     + "shutdown; startup backfill will re-upload any that are still live.",
                     rescued);
        }
    }

    /** Gives up on every retry in one retry-context set, claiming each entry exactly once. */
    private int drainRetrySet(Set<RetryContext> set, String reason) {
        int rescued = 0;
        for (RetryContext ctx : set) {
            if (!set.remove(ctx)) {
                continue; // raced with the task itself removing it; it will handle its own outcome
            }
            giveUp(ctx.dbName, ctx.cfName, ctx.filePath, ctx.sourceSstPath, ctx.remoteKey,
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
                                long uploadEpoch, int attempt) {
        long delayMs = computeDelay(attempt);
        log.info("Cloud upload retry scheduled: db={}, cf={}, path={}, attempt={}/{}, delayMs={}",
                 dbName, cfName, filePath, attempt, maxAttempts, delayMs);
        // Increment before schedule; decrement here on rejection (executeRetry's finally
        // handles the decrement on the happy path, but RejectedExecutionException bypasses it).
        inFlightCount.incrementAndGet();
        // Record the payload so a forced shutdown can give up on this retry if it never
        // runs. Removed when the task starts executing (see wrapper below).
        RetryContext ctx = new RetryContext(dbName, cfName, filePath, sourceSstPath, remoteKey,
                                            uploadEpoch, attempt);
        pendingRetries.add(ctx);
        try {
            scheduler.schedule(() -> {
                // Move from "scheduled" to "in-flight" so close() can still rescue this retry's
                // intent if the upload below hangs past the shutdown timeout.
                pendingRetries.remove(ctx);
                inFlightRetries.add(ctx);
                try {
                    executeRetry(dbName, cfName, filePath, sourceSstPath, remoteKey,
                                 uploadEpoch, attempt);
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

    /**
     * Immutable payload of a scheduled-but-not-yet-started retry, tracked in
     * {@link #pendingRetries}.
     */
    private static final class RetryContext {
        final String dbName;
        final String cfName;
        final String filePath;
        final String sourceSstPath;
        final String remoteKey;
        final long uploadEpoch;
        final int attempt;

        RetryContext(String dbName, String cfName, String filePath, String sourceSstPath,
                     String remoteKey, long uploadEpoch, int attempt) {
            this.dbName = dbName;
            this.cfName = cfName;
            this.filePath = filePath;
            this.sourceSstPath = sourceSstPath;
            this.remoteKey = remoteKey;
            this.uploadEpoch = uploadEpoch;
            this.attempt = attempt;
        }
    }

    private void executeRetry(String dbName, String cfName, String filePath,
                               String sourceSstPath, String remoteKey,
                               long uploadEpoch, int attempt) {
        try {
            // If the local SST (or staging pin) is gone, drop silently – nothing to upload.
            if (!Files.exists(Paths.get(filePath))) {
                log.info("Cloud upload retry: local file no longer exists (compacted?), "
                         + "dropping: db={}, cf={}, path={}", dbName, cfName, filePath);
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
                // misclassifying into retry/give-up.
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
                giveUp(dbName, cfName, filePath, sourceSstPath, remoteKey, uploadEpoch,
                          attempt, e.getMessage() != null ? e.getMessage() : "non-retryable");
                return;
            }
            if (attempt >= maxAttempts) {
                giveUp(dbName, cfName, filePath, sourceSstPath, remoteKey, uploadEpoch,
                          attempt, e.getMessage());
            } else {
                try {
                    scheduleRetry(dbName, cfName, filePath, sourceSstPath, remoteKey,
                                  uploadEpoch, attempt + 1);
                } catch (RejectedExecutionException ree) {
                    // Scheduler is shutting down. scheduleRetry rethrows the rejection; if we let
                    // it escape here it would propagate out of this scheduler task and be silently
                    // swallowed by the executor, leaving the file neither retried nor recorded.
                    // Give up instead; startup backfill re-uploads it if still live.
                    log.warn("Cloud upload retry reschedule rejected (scheduler shutting down); "
                             + "giving up: db={}, cf={}, path={}", dbName, cfName, filePath);
                    giveUp(dbName, cfName, filePath, sourceSstPath, remoteKey, uploadEpoch,
                              attempt, "retry reschedule rejected during shutdown");
                }
            }
        } finally {
            inFlightCount.decrementAndGet();
        }
    }

    private void giveUp(String dbName, String cfName, String filePath,
                        String sourceSstPath, String remoteKey,
                        long uploadEpoch, int attemptCount, String lastError) {
        givenUpCount.incrementAndGet();

        // Nothing uploads from a given-up task again, so release its staging hard-link now;
        // otherwise it would pin the (possibly compacted-away) SST's disk space indefinitely.
        if (sourceSstPath != null && !filePath.equals(sourceSstPath)) {
            try {
                Files.deleteIfExists(Paths.get(filePath));
            } catch (IOException e) {
                log.debug("Failed to release staging file for given-up upload: {}", filePath);
            }
        }

        log.error("Cloud upload given up after {} attempt(s) – file stays unconfirmed and will be "
                  + "re-uploaded by the next metadata sync, delete guard, or startup backfill: "
                  + "db={}, cf={}, path={}, remoteKey={}, epoch={}, lastError={}",
                  attemptCount, dbName, cfName, filePath, remoteKey, uploadEpoch, lastError);
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
}
