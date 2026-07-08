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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
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
 *       (e.g. surviving from a previous run) and triggers an async MemTable flush so that
 *       WAL-recovered or recently-written data is also written to SST files.</li>
 *   <li>{@link #onTableFileCreated} uploads newly created SST files.</li>
 *   <li>{@link #onTableFileDeleted} removes the corresponding object from cloud storage.</li>
 * </ul>
 *
 * <h3>Remote key construction</h3>
 * The remote key is derived by stripping the {@code dataRoot} prefix from the absolute
 * local file path.  This keeps the object layout clean and independent of the container
 * filesystem layout:
 * <pre>
 *   dataRoot  = /hugegraph-store/storage
 *   filePath  = /hugegraph-store/storage/hgstore-metadata/000008.sst
 *   remoteKey = hgstore-metadata/000008.sst
 *   (with path-prefix "hugegraph") → hugegraph/hgstore-metadata/000008.sst
 * </pre>
 *
 * This listener is registered with {@link RocksDBFactory} during application startup
 * (see {@link org.apache.hugegraph.store.node.AppConfig}).
 */
@Slf4j
public class CloudStorageEventListener implements RocksdbChangedListener {

    /** Absolute, normalised path of the store's data root directory. */
    private final String dataRoot;

    private static final long DEFAULT_READ_MISS_GUARD_WINDOW_MS = 3000L;

    private final boolean startupHydrationEnabled;
    private final long readMissGuardWindowMs;
    private final Map<String, Long> readMissAttemptTs;

    /**
     * Optional retry queue; when non-null, upload failures are submitted here instead
     * of just being logged. When null, failures are only logged (no retry).
     */
    private final CloudUploadRetryQueue retryQueue;

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
     * Full constructor.
     *
     * @param retryQueue optional {@link CloudUploadRetryQueue}; when non-null, upload failures
     *                   are retried asynchronously and eventually moved to the dead-letter queue.
     *                   Pass {@code null} to disable retries (failures are only logged).
     */
    public CloudStorageEventListener(String dataRoot,
                                     boolean startupHydrationEnabled,
                                     long readMissGuardWindowMs,
                                     CloudUploadRetryQueue retryQueue) {
        String normalised = Paths.get(dataRoot).toAbsolutePath().normalize().toString();
        // Strip trailing separator so substring arithmetic is consistent.
        this.dataRoot = normalised.endsWith(File.separator)
                        ? normalised.substring(0, normalised.length() - 1)
                        : normalised;
        this.startupHydrationEnabled = startupHydrationEnabled;
        this.readMissGuardWindowMs = Math.max(0L, readMissGuardWindowMs);
        this.readMissAttemptTs = new ConcurrentHashMap<>();
        this.retryQueue = retryQueue;
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
     * Called when a read returns null in RocksDB. We try to hydrate missing SST files from cloud,
     * ingest them into the target CF, then caller retries get().
     */
    @Override
    public boolean onReadMiss(RocksDBSession session, String table, byte[] key) {
        if (!shouldAttemptReadMissHydration(session.getGraphName(), table)) {
            return false;
        }
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            return false;
        }
        List<String> downloaded = downloadMissingSstFiles(provider,
                                                          session.getGraphName(),
                                                          session.getDbPath());
        if (downloaded.isEmpty()) {
            return false;
        }
        try {
            Map<byte[], List<String>> sstByCf = new HashMap<>();
            sstByCf.put(table.getBytes(StandardCharsets.UTF_8), downloaded);
            session.ingestSstFile(sstByCf);
            log.info("Cloud read-miss hydration succeeded: db={}, table={}, files={}",
                     session.getGraphName(), table, downloaded.size());
            return true;
        } catch (Exception e) {
            log.warn("Cloud read-miss hydration failed: db={}, table={}, reason={}",
                     session.getGraphName(), table, e.getMessage());
            return false;
        }
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
        CloudStorageProvider provider = CloudStorageProviderFactory.getActiveProvider();
        if (provider == null) {
            return;
        }
        uploadExistingSstFiles(provider, dbName, dbPath);
        flushDb(dbName);
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
        String remoteKey = toRelativeKey(filePath);
        try {
            provider.uploadFile(filePath, remoteKey);
            log.debug("Cloud upload success: db={}, cf={}, path={}, size={}",
                      dbName, cfName, filePath, fileSize);
        } catch (Exception e) {
            // NOTE: this callback is invoked via RocksDB's JNI event-listener mechanism.
            // Any exception thrown here crosses the JNI boundary and is silently swallowed
            // by the native layer — it will NOT crash the server and the SST file will NOT
            // be retried automatically. Log the failure and submit to the retry queue (if
            // configured) so the upload can be retried asynchronously and, after exhausting
            // all attempts, moved to the dead-letter queue for later inspection / replay.
            log.error("Cloud upload failed (SST file is local-only, may be missing from cloud): "
                      + "db={}, cf={}, path={}", dbName, cfName, filePath, e);
            if (retryQueue != null) {
                retryQueue.submit(dbName, cfName, filePath, remoteKey, e);
            }
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
        String remoteKey = toRelativeKey(filePath);
        try {
            provider.deleteFile(remoteKey);
            log.debug("Cloud delete success: db={}, cf={}, path={}", dbName, cfName, filePath);
        } catch (Exception e) {
            // Non-fatal: log and continue.
            log.error("Cloud delete failed: db={}, cf={}, path={}", dbName, cfName, filePath, e);
        }
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
        if (filePath.startsWith(dataRoot)) {
            String rel = filePath.substring(dataRoot.length());
            // Strip leading separator produced by the substring.
            return rel.startsWith("/") || rel.startsWith(File.separator)
                   ? rel.substring(1)
                   : rel;
        }
        // Fallback: strip any leading slash so the key does not start with '/'.
        return filePath.startsWith("/") ? filePath.substring(1) : filePath;
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

        String prefix = dbPrefix(dbPath);
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
            log.info("Cloud pre-hydration finished: db={}, downloadedFiles={}", dbName, downloaded);
        }
    }

    private List<String> downloadMissingSstFiles(CloudStorageProvider provider,
                                                 String dbName,
                                                 String dbPath) {
        String prefix = dbPrefix(dbPath);
        List<String> remoteFiles = listRemoteKeys(provider, prefix);
        if (remoteFiles.isEmpty()) {
            return List.of();
        }

        List<String> downloaded = new ArrayList<>();
        for (String remoteKey : remoteFiles) {
            if (!remoteKey.endsWith(".sst")) {
                continue;
            }
            Path localPath = resolveLocalPath(remoteKey);
            if (Files.exists(localPath)) {
                continue;
            }
            try {
                Files.createDirectories(localPath.getParent());
                provider.downloadFile(remoteKey, localPath.toString());
                downloaded.add(localPath.toString());
            } catch (IOException e) {
                throw new IllegalStateException(
                        String.format("Cloud read-miss download failed for db=%s key=%s",
                                      dbName, remoteKey), e);
            }
        }
        return downloaded;
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
        Path root = Paths.get(this.dataRoot);
        Path local = root.resolve(remoteKey).normalize();
        if (!local.startsWith(root)) {
            throw new IllegalArgumentException("Invalid remote key outside data root: " + remoteKey);
        }
        return local;
    }

    /**
     * Triggers an asynchronous MemTable flush for the named DB via {@link RocksDBFactory}.
     * This causes any in-memory data (including WAL-recovered entries) to be written to an
     * SST file, which in turn fires {@link #onTableFileCreated} and uploads the file.
     */
    private void flushDb(String dbName) {
        try {
            RocksDBFactory.getInstance().flushSession(dbName, false);
            log.debug("Cloud storage: triggered async flush for db={}", dbName);
        } catch (Exception e) {
            log.warn("Cloud storage: flush failed for db={}: {}", dbName, e.getMessage());
        }
    }

    private boolean shouldAttemptReadMissHydration(String dbName, String table) {
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
