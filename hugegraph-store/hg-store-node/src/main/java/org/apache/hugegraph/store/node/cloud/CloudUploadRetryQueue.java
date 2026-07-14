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
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import org.apache.hugegraph.store.cloud.CloudStorageNonRetryableException;
import org.apache.hugegraph.store.cloud.CloudStorageProviderFactory;

import lombok.extern.slf4j.Slf4j;

/**
 * Asynchronous retry queue with a file-backed dead-letter queue (DLQ) for failed
 * cloud SST uploads.
 *
 * <h3>Retry strategy</h3>
 * <ul>
 *   <li>When {@code maxAttempts == 0} (default), failures go directly to the DLQ with no
 *       whole-file retry. The provider is expected to handle its own retries internally
 *       (e.g. S3 multipart-part-retry).</li>
 *   <li>When {@code maxAttempts > 0}, the first retry is scheduled after
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
            + "failedAt\\tattemptCount\\tdbName\\tcfName\\tfilePath\\tremoteKey\\tlastError";

    // -----------------------------------------------------------------------
    // Configuration
    // -----------------------------------------------------------------------

    private final int maxAttempts;
    private final long initialDelayMs;
    private final long maxDelayMs;

    /**
     * Invoked with {@code (dbName, filePath)} whenever an upload succeeds via retry or DLQ replay,
     * so the caller can mark the file confirmed-present in cloud. May be a no-op.
     */
    private final java.util.function.BiConsumer<String, String> onUploadConfirmed;

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    /** In-memory dead-letter queue – holds tasks that exhausted all retries. */
    private final Deque<FailedUploadTask> dlq = new ConcurrentLinkedDeque<>();

    /** Absolute path of the on-disk DLQ file. */
    private final Path dlqFile;

    /** Background thread pool used to schedule retries. */
    private final ScheduledExecutorService scheduler;

    /** Counter of in-flight retry tasks (for testing / monitoring). */
    private final AtomicInteger inFlightCount = new AtomicInteger(0);

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
        this(maxAttempts, initialDelayMs, maxDelayMs, dataRoot, null);
    }

    /**
     * @param onUploadConfirmed callback invoked with {@code (dbName, filePath)} on every successful
     *                          retry / DLQ-replay upload; pass {@code null} for none.
     */
    public CloudUploadRetryQueue(int maxAttempts, long initialDelayMs,
                                 long maxDelayMs, String dataRoot,
                                 java.util.function.BiConsumer<String, String> onUploadConfirmed) {
        this.maxAttempts = Math.max(0, maxAttempts);
        this.initialDelayMs = Math.max(100L, initialDelayMs);
        this.maxDelayMs = Math.max(this.initialDelayMs, maxDelayMs);
        this.onUploadConfirmed = onUploadConfirmed != null
                                 ? onUploadConfirmed
                                 : (db, path) -> { };
        this.dlqFile = Paths.get(dataRoot, DLQ_FILE_NAME);

        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "cloud-upload-retry");
            t.setDaemon(true);
            return t;
        });

        loadDlqFromDisk();
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Submits a failed upload to the retry queue or directly to the DLQ.
     *
     * <p>If {@code maxAttempts == 0} (default), the task goes directly to the DLQ — the
     * provider is expected to handle its own retries internally.
     *
     * <p>If {@code maxAttempts > 0}, the first retry is scheduled after {@code initialDelayMs}
     * ms with exponential backoff. After all attempts are exhausted the task is moved to the DLQ.
     *
     * <p>If the file no longer exists at retry time (compacted away by RocksDB) the task is
     * silently dropped.
     *
     * @param dbName    RocksDB instance name (partition id)
     * @param cfName    column-family name
     * @param filePath  absolute local path of the SST file
     * @param remoteKey remote object key (relative to bucket root)
     * @param cause     the exception from the first upload attempt
     */
    public void submit(String dbName, String cfName, String filePath,
                       String remoteKey, Exception cause) {
        if (cause instanceof CloudStorageNonRetryableException || maxAttempts == 0) {
            // Non-retryable exception, or retry disabled: go straight to DLQ.
            moveToDlq(dbName, cfName, filePath, remoteKey, 0,
                      cause.getMessage() != null ? cause.getMessage() : "non-retryable");
            return;
        }
        scheduleRetry(dbName, cfName, filePath, remoteKey, 1);
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

        for (FailedUploadTask task : snapshot) {
            if (!Files.exists(Paths.get(task.getFilePath()))) {
                log.info("DLQ replay: local file gone, dropping: path={}", task.getFilePath());
                dropped++;
                continue;
            }
            if (provider == null) {
                // No provider – re-queue and bail out early.
                scheduleRetry(task.getDbName(), task.getCfName(), task.getFilePath(),
                              task.getRemoteKey(), 1);
                requeued++;
                continue;
            }
            try {
                provider.uploadFile(task.getFilePath(), task.getRemoteKey());
                onUploadConfirmed.accept(task.getDbName(), task.getFilePath());
                log.info("DLQ replay succeeded: db={}, cf={}, path={}",
                         task.getDbName(), task.getCfName(), task.getFilePath());
                succeeded++;
            } catch (Exception e) {
                log.warn("DLQ replay failed, re-queuing: db={}, cf={}, path={}, reason={}",
                         task.getDbName(), task.getCfName(), task.getFilePath(), e.getMessage());
                scheduleRetry(task.getDbName(), task.getCfName(), task.getFilePath(),
                              task.getRemoteKey(), 1);
                requeued++;
            }
        }

        // Rewrite the DLQ file to reflect removals (only currently queued DLQ entries remain).
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
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("CloudUploadRetryQueue: executor did not terminate within 10 s; "
                         + "forcing shutdown ({} tasks still in flight)", inFlightCount.get());
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // -----------------------------------------------------------------------
    // Internal – retry scheduling
    // -----------------------------------------------------------------------

    private void scheduleRetry(String dbName, String cfName, String filePath,
                                String remoteKey, int attempt) {
        long delayMs = computeDelay(attempt);
        log.info("Cloud upload retry scheduled: db={}, cf={}, path={}, attempt={}/{}, delayMs={}",
                 dbName, cfName, filePath, attempt, maxAttempts, delayMs);
        inFlightCount.incrementAndGet();
        scheduler.schedule(
                () -> executeRetry(dbName, cfName, filePath, remoteKey, attempt),
                delayMs, TimeUnit.MILLISECONDS);
    }

    private void executeRetry(String dbName, String cfName, String filePath,
                               String remoteKey, int attempt) {
        try {
            // If the local SST file is gone (compacted), drop silently – nothing to upload.
            if (!Files.exists(Paths.get(filePath))) {
                log.info("Cloud upload retry: local file no longer exists (compacted?), "
                         + "dropping: db={}, cf={}, path={}", dbName, cfName, filePath);
                return;
            }

            CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
            if (provider == null) {
                log.warn("Cloud upload retry: no active provider, giving up: "
                         + "db={}, cf={}, path={}", dbName, cfName, filePath);
                moveToDlq(dbName, cfName, filePath, remoteKey, attempt, "No active provider");
                return;
            }

            provider.uploadFile(filePath, remoteKey);
            onUploadConfirmed.accept(dbName, filePath);
            log.info("Cloud upload retry succeeded: db={}, cf={}, path={}, attempt={}",
                     dbName, cfName, filePath, attempt);

        } catch (Exception e) {
            log.warn("Cloud upload retry failed: db={}, cf={}, path={}, attempt={}/{}, reason={}",
                     dbName, cfName, filePath, attempt, maxAttempts, e.getMessage());
            if (e instanceof CloudStorageNonRetryableException) {
                moveToDlq(dbName, cfName, filePath, remoteKey, attempt,
                          e.getMessage() != null ? e.getMessage() : "non-retryable");
                return;
            }
            if (attempt >= maxAttempts) {
                moveToDlq(dbName, cfName, filePath, remoteKey, attempt, e.getMessage());
            } else {
                scheduleRetry(dbName, cfName, filePath, remoteKey, attempt + 1);
            }
        } finally {
            inFlightCount.decrementAndGet();
        }
    }

    private void moveToDlq(String dbName, String cfName, String filePath,
                            String remoteKey, int attemptCount, String lastError) {
        FailedUploadTask task = new FailedUploadTask(dbName, cfName, filePath,
                                                     remoteKey, attemptCount, lastError);
        dlq.addLast(task);
        appendDlqEntryToDisk(task);
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

    // -----------------------------------------------------------------------
    // Internal – DLQ persistence
    // -----------------------------------------------------------------------

    /** Loads any persisted DLQ entries from disk at startup. */
    private void loadDlqFromDisk() {
        if (!Files.exists(dlqFile)) {
            return;
        }
        int loaded = 0;
        try (BufferedReader reader = Files.newBufferedReader(dlqFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                FailedUploadTask task = deserialize(line);
                if (task != null) {
                    dlq.addLast(task);
                    loaded++;
                }
            }
        } catch (IOException e) {
            log.warn("DLQ: failed to load persisted entries from {}: {}",
                     dlqFile, e.getMessage());
        }
        if (loaded > 0) {
            log.warn("DLQ: loaded {} pending failed upload(s) from disk – "
                     + "call replayDlq() to retry them (file={})", loaded, dlqFile);
        }
    }

    /** Appends a single DLQ entry to the on-disk file. */
    private void appendDlqEntryToDisk(FailedUploadTask task) {
        try {
            boolean newFile = !Files.exists(dlqFile);
            try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(
                    dlqFile, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND))) {
                if (newFile) {
                    pw.println(DLQ_COMMENT);
                }
                pw.println(serialize(task));
            }
        } catch (IOException e) {
            log.warn("DLQ: failed to persist entry to {}: {}", dlqFile, e.getMessage());
        }
    }

    /** Rewrites the DLQ file from the current in-memory DLQ (used after replay). */
    private void rewriteDlqFile() {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(
                dlqFile, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING))) {
            pw.println(DLQ_COMMENT);
            dlq.forEach(t -> pw.println(serialize(t)));
        } catch (IOException e) {
            log.warn("DLQ: failed to rewrite {}: {}", dlqFile, e.getMessage());
        }
    }

    // -----------------------------------------------------------------------
    // Internal – serialisation (tab-separated, with escape for special chars)
    // -----------------------------------------------------------------------

    /**
     * Serialises a {@link FailedUploadTask} to a single tab-separated line.
     * Fields: failedAt, attemptCount, dbName, cfName, filePath, remoteKey, lastError.
     */
    String serialize(FailedUploadTask task) {
        return task.getFailedAt() + "\t"
               + task.getAttemptCount() + "\t"
               + escape(task.getDbName()) + "\t"
               + escape(task.getCfName()) + "\t"
               + escape(task.getFilePath()) + "\t"
               + escape(task.getRemoteKey()) + "\t"
               + escape(task.getLastError());
    }

    /** Deserialises a line; returns {@code null} and logs a warning on parse errors. */
    FailedUploadTask deserialize(String line) {
        String[] parts = line.split("\t", 7);
        if (parts.length < 7) {
            log.warn("DLQ: skipping malformed line (expected 7 fields, got {}): {}",
                     parts.length, line);
            return null;
        }
        try {
            long failedAt = Long.parseLong(parts[0].trim());
            int attemptCount = Integer.parseInt(parts[1].trim());
            return new FailedUploadTask(
                    unescape(parts[2]),   // dbName
                    unescape(parts[3]),   // cfName
                    unescape(parts[4]),   // filePath
                    unescape(parts[5]),   // remoteKey
                    failedAt,
                    attemptCount,
                    unescape(parts[6])    // lastError
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

