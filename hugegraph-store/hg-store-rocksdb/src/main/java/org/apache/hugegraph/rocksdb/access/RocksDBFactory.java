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

package org.apache.hugegraph.rocksdb.access;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.config.HugeConfig;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.Cache;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.MemoryUsageType;
import org.rocksdb.MemoryUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.Status;
import org.rocksdb.TableFileCreationInfo;
import org.rocksdb.TableFileDeletionInfo;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class RocksDBFactory {

    // CopyOnWriteArrayList: the list is iterated (forEach / for-each) from RocksDB JNI callback
    // threads (onTableFileCreated/Deleted, compaction) while add/removeRocksdbChangedListener can
    // mutate it from the main thread (Spring teardown, test @After). A plain ArrayList would throw
    // ConcurrentModificationException on the callback thread; COW gives each iteration a stable
    // snapshot and makes mutation safe without locking the hot callback path.
    private static final List<RocksdbChangedListener> rocksdbChangedListeners =
            new CopyOnWriteArrayList<>();
    private static RocksDBFactory dbFactory;
    /** Singleton event listener wired into every new RocksDB instance. */
    private final RocksdbEventListener rocksdbEventListener = new RocksdbEventListener();

    static {
        RocksDB.loadLibrary();
    }

    private final Map<String, RocksDBSession> dbSessionMap = new ConcurrentHashMap<>();
    private final List<DBSessionWatcher> destroyGraphDBs = new CopyOnWriteArrayList<>();
    private final ReentrantReadWriteLock operateLock;
    /** Names of DBs currently being created (between Phase 1 and Phase 3). Guards the RocksDB LOCK file. */
    private final Set<String> pendingCreations = new HashSet<>();
    ScheduledExecutorService scheduledExecutor;
    private HugeConfig hugeConfig;
    private AtomicBoolean closing = new AtomicBoolean(false);

    private RocksDBFactory() {
        this.operateLock = new ReentrantReadWriteLock();
        scheduledExecutor = Executors.newScheduledThreadPool(2);
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                dbSessionMap.forEach((k, session) -> {
                    for (var entry : session.getIteratorMap().entrySet()) {
                        String key = entry.getKey();
                        var ts = Long.parseLong(key.split("-")[0]);
                        // output once per 10min
                        var passed = (System.currentTimeMillis() - ts) / 1000 - 600;
                        if (passed > 0 && passed % 10 == 0) {
                            log.info("iterator not close, stack: {}", entry.getValue());
                        }
                    }
                });
            } catch (Exception e) {
                log.error("got error, ", e);
            }

            try {
                Iterator<DBSessionWatcher> itr = destroyGraphDBs.listIterator();
                while (itr.hasNext()) {
                    DBSessionWatcher watcher = itr.next();
                    if (0 == watcher.dbSession.getRefCount()) {
                        // Destroy-then-recreate race guard: a concurrent createGraphDB may have
                        // re-occupied the same DB name/path (createGraphDB checks dbSessionMap and
                        // pendingCreations, not destroyGraphDBs). If so, this stale watcher must NOT
                        // delete the new DB's local files or purge its cloud objects. Check under
                        // the read lock so it serialises with createGraphDB's write-locked slot
                        // reservation and map insertion.
                        String staleName = watcher.dbSession.getGraphName();
                        boolean superseded;
                        operateLock.readLock().lock();
                        try {
                            superseded = dbSessionMap.containsKey(staleName)
                                         || pendingCreations.contains(staleName);
                        } finally {
                            operateLock.readLock().unlock();
                        }
                        if (superseded) {
                            log.warn("DestroyGraphDB: db={} was recreated at path={} before "
                                     + "cleanup ran — discarding stale destroy watcher without "
                                     + "deleting files or purging cloud objects",
                                     staleName, watcher.dbSession.getDbPath());
                            destroyGraphDBs.remove(watcher);
                            continue;
                        }
                        try {
                            watcher.dbSession.shutdown();
                            FileUtils.deleteDirectory(new File(watcher.dbSession.getDbPath()));
                            rocksdbChangedListeners.forEach(listener -> {
                                listener.onDBDeleted(watcher.dbSession.getGraphName(),
                                                     watcher.dbSession.getDbPath());
                            });
                            log.info("removed db {} and delete files",
                                     watcher.dbSession.getDbPath());
                            destroyGraphDBs.remove(watcher);
                        } catch (Exception e) {
                            watcher.deleteAttempts++;
                            if (watcher.deleteAttempts >= 3) {
                                // Give up after 3 consecutive failures: continuing to retry
                                // risks calling onDBDeleted (which purges cloud objects) against
                                // a DB that may have been recreated at the same path.
                                log.error("DestroyGraphDB: giving up after {} failed attempts for "
                                          + "db={}: {}", watcher.deleteAttempts,
                                          watcher.dbSession.getDbPath(), e);
                                destroyGraphDBs.remove(watcher);
                            } else {
                                log.error("DestroyGraphDB exception (attempt {}), will retry: {}",
                                          watcher.deleteAttempts, e);
                            }
                        }
                    } else if (watcher.timestamp < (System.currentTimeMillis() - 1800 * 1000)) {
                        // Force delete after 30-min timeout: session is stuck with live refs.
                        watcher.dbSession.forceResetRefCount();
                    } else {
                        log.warn("DB {}  has not been deleted refCount is {}, time is {} seconds",
                                 watcher.dbSession.getDbPath(),
                                 watcher.dbSession.getRefCount(),
                                 (System.currentTimeMillis() - watcher.timestamp) / 1000);
                    }
                }

            } catch (Exception e) {
                log.error("RocksDBFactory scheduledExecutor exception {}", e);
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    public static RocksDBFactory getInstance() {
        if (dbFactory == null) {
            synchronized (RocksDBFactory.class) {
                if (dbFactory == null) {
                    dbFactory = new RocksDBFactory();
                }
            }
        }
        return dbFactory;
    }

    public int getSessionSize() {
        return dbSessionMap.size();
    }

    public Set<String> getGraphNames() {
        return dbSessionMap.keySet();
    }

    public HugeConfig getHugeConfig() {
        return this.hugeConfig;
    }

    public void setHugeConfig(HugeConfig nodeConfig) {
        this.hugeConfig = nodeConfig;
    }

    public boolean findPathInRemovedList(String path) {
        for (DBSessionWatcher pair : destroyGraphDBs) {
            if (pair.dbSession.getDbPath().equals(path)) {
                return true;
            }
        }
        return false;
    }

    public RocksDBSession queryGraphDB(String dbName) {
        operateLock.readLock().lock();
        try {
            RocksDBSession session = dbSessionMap.get(dbName);
            if (session != null) {
                return session.clone();
            }
        } finally {
            operateLock.readLock().unlock();
        }
        return null;
    }
    //TODO is this necessary?
    class RocksdbEventListener extends AbstractEventListener {
        @Override
        public void onCompactionCompleted(RocksDB db, CompactionJobInfo compactionJobInfo) {
            super.onCompactionCompleted(db, compactionJobInfo);
            rocksdbChangedListeners.forEach(listener -> {
                listener.onCompacted(db.getName());
            });
        }

        @Override
        public void onCompactionBegin(final RocksDB db, final CompactionJobInfo compactionJobInfo) {
            log.info("RocksdbEventListener onCompactionBegin");
        }

        /**
         * Invoked by RocksDB after a new SST (table) file has been successfully created.
         * Propagates the event to all registered {@link RocksdbChangedListener}s so that
         * cloud-storage providers can upload the new file.
         */
        @Override
        public void onTableFileCreated(final TableFileCreationInfo info) {
            if (info.getStatus().getCode() != Status.Code.Ok) {
                log.warn("onTableFileCreated: skipping failed file creation, path={}, status={}",
                         info.getFilePath(), info.getStatus().getCodeString());
                return;
            }
            log.debug("onTableFileCreated: db={}, cf={}, path={}, size={}",
                      info.getDbName(), info.getColumnFamilyName(),
                      info.getFilePath(), info.getFileSize());
            rocksdbChangedListeners.forEach(listener ->
                    listener.onTableFileCreated(info.getDbName(), info.getColumnFamilyName(),
                                                info.getFilePath(), info.getFileSize())
            );
        }

        /**
         * Invoked by RocksDB after an SST (table) file has been deleted.
         * Propagates the event to all registered {@link RocksdbChangedListener}s so that
         * cloud-storage providers can remove the corresponding remote object.
         */
        @Override
        public void onTableFileDeleted(final TableFileDeletionInfo info) {
            if (info.getStatus().getCode() != Status.Code.Ok) {
                log.warn("onTableFileDeleted: skipping failed file deletion, path={}, status={}",
                         info.getFilePath(), info.getStatus().getCodeString());
                return;
            }
            log.info("onTableFileDeleted: db={}, path={}",
                      info.getDbName(), info.getFilePath());
            rocksdbChangedListeners.forEach(listener ->
                    listener.onTableFileDeleted(info.getDbName(), null, info.getFilePath())
            );
        }
    }

    public RocksDBSession createGraphDB(String dbPath, String dbName) {
        return createGraphDB(dbPath, dbName, 0);
    }

    public RocksDBSession createGraphDB(String dbPath, String dbName, long version) {
        if (closing.get()) {
            throw new RuntimeException("db closed");
        }

        // Phase 1: check the map and, if absent, construct the session under the write lock.
        // The session is NOT inserted yet — inserting before hydration would let a concurrent
        // queryGraphDB hand out an un-hydrated session.
        // Also check pendingCreations: if another thread is already in Phase 2 for this DB,
        // wait for it to finish rather than opening a second RocksDBSession for the same path
        // (which would fail with a RocksDB LOCK-file error).
        RocksDBSession dbSession;
        operateLock.writeLock().lock();
        try {
            // Spin-wait while another thread holds the pending-creation slot for this DB.
            while (pendingCreations.contains(dbName)) {
                operateLock.writeLock().unlock();
                try { Thread.sleep(10); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted waiting for DB creation: " + dbName, ie);
                }
                operateLock.writeLock().lock();
            }
            RocksDBSession existing = dbSessionMap.get(dbName);
            if (existing != null) {
                // Already exists — return a clone with no hydration needed.
                return existing.clone();
            }
            log.info("create rocksdb for {}", dbName);
            // Reserve the creation slot; construct (which hydrates + opens) OUTSIDE the lock below.
            pendingCreations.add(dbName);
        } finally {
            operateLock.writeLock().unlock();
        }

        // Phase 2: construct the session OUTSIDE the write lock so other DBs' queryGraphDB calls
        // are not blocked during a potentially long S3 hydration. Construction runs the pre-open
        // hook, which fires onDBOpening to hydrate the DB directory from cloud BEFORE RocksDB opens
        // it — so a node whose local files were lost opens on the recovered data instead of a fresh
        // empty DB. Per-DB serialization is provided by the pendingCreations slot reserved above,
        // so two threads never open the same path concurrently (which would fail on the LOCK file).
        boolean constructed = false;
        try {
            dbSession = new RocksDBSession(
                    this.hugeConfig, dbPath, dbName, version,
                    (name, resolvedPath) ->
                            rocksdbChangedListeners.forEach(l -> l.onDBOpening(name, resolvedPath)));
            constructed = true;
        } finally {
            if (!constructed) {
                // Hydration or open failed with ANY throwable — including java.lang.Error
                // (OutOfMemoryError, StackOverflowError) which a plain `catch (Exception)` would
                // miss. If the slot leaked, every future createGraphDB for this DB would spin-wait
                // forever (the wait loop has no timeout). A finally clears it on all paths; the
                // original throwable still propagates. No native handle exists to close (open did
                // not complete).
                operateLock.writeLock().lock();
                try { pendingCreations.remove(dbName); } finally { operateLock.writeLock().unlock(); }
            }
        }

        // Phase 3: insert the hydrated session under the write lock and clear the pending slot
        // so any thread that was spin-waiting can now proceed (it will find the map entry).
        operateLock.writeLock().lock();
        try {
            pendingCreations.remove(dbName);
            RocksDBSession existing = dbSessionMap.get(dbName);
            if (existing != null) {
                // Race: another thread won Phase 3 first; discard our duplicate.
                try { dbSession.shutdown(); } catch (Exception ignore) {}
                return existing.clone();
            }
            dbSessionMap.put(dbName, dbSession);
        } catch (Exception e) {
            pendingCreations.remove(dbName);
            try { dbSession.shutdown(); } catch (Exception ignore) {}
            throw e;
        } finally {
            operateLock.writeLock().unlock();
        }

        // Notify listeners (upload pre-existing SSTs, flush WAL) outside the lock.
        final String finalDbName = dbSession.getGraphName();
        final String finalDbPath = dbSession.getDbPath();
        rocksdbChangedListeners.forEach(listener -> listener.onDBCreated(finalDbName, finalDbPath));

        return dbSession.clone();
    }

    /**
     * @param :
     * @return long
     * @description the size(KB) of the total rocksdb's data.
     */
    public long getTotalSize() {
        long kbSize = dbSessionMap.entrySet()
                                  .stream()
                                  .map(e -> e.getValue().getApproximateDataSize())
                                  .reduce(0L, Long::sum);
        return kbSize;
    }

    public Map<String, Long> getTotalKey() {
        Map<String, Long> totalKeys = dbSessionMap.entrySet().stream()
                                                  .collect(Collectors.toMap(e -> e.getKey(),
                                                                            e -> e.getValue()
                                                                                  .getEstimateNumKeys()));
        return totalKeys;
    }

    /**
     * Release rocksdb object
     *
     * @param dbName
     * @return
     */
    public boolean releaseGraphDB(String dbName) {
        log.info("close {} 's  rocksdb.", dbName);
        operateLock.writeLock().lock();
        try {
            RocksDBSession dbSession = dbSessionMap.get(dbName);
            if (dbSession != null) {
                dbSessionMap.remove(dbName);
                rocksdbChangedListeners.forEach(listener -> {
                    listener.onDBSessionReleased(dbSession);
                });
                dbSession.close();
            }
        } finally {
            operateLock.writeLock().unlock();
        }

        return false;
    }

    /**
     * Destroy the graph, and delete the data file.
     *
     * @param dbName
     */
    public void destroyGraphDB(String dbName) {
        log.info("destroy {} 's  rocksdb.", dbName);
        RocksDBSession dbSession = dbSessionMap.get(dbName);
        if (dbSession != null) {
            // Fire onDBDeleteBegin BEFORE releasing the session or enqueuing the destroy watcher.
            // A listener may throw here to HOLD the delete — e.g. the cloud listener cannot durably
            // persist its anti-resurrection marker, so proceeding would risk a crash re-hydrating
            // stale objects as live data. If we had already enqueued the watcher (which
            // asynchronously deletes the local dir and purges cloud) the throw could not actually
            // stop the delete. By notifying first and letting the exception propagate before
            // release/enqueue, the DB is left intact and still in dbSessionMap, so the caller can
            // retry the destroy once the underlying problem clears.
            rocksdbChangedListeners.forEach(listener -> {
                listener.onDBDeleteBegin(dbSession.getGraphName(), dbSession.getDbPath());
            });
        }
        releaseGraphDB(dbName);
        // Add delete mark only after the begin-notification succeeded for every listener.
        if (dbSession != null) {
            destroyGraphDBs.add(new DBSessionWatcher(dbSession));
        }
    }

    public void releaseAllGraphDB() {
        closing.set(true);
        log.info("closing all rocksdb....");
        operateLock.writeLock().lock();
        try {
            dbSessionMap.forEach((k, v) -> {
                v.shutdown();
            });
            dbSessionMap.clear();
        } finally {
            operateLock.writeLock().unlock();
        }
    }

    public Map<MemoryUsageType, Long> getApproximateMemoryUsageByType(List<RocksDB> dbs,
                                                                      List<Cache> caches) {
        if (dbs == null) {
            dbs = new ArrayList<>();
        } else {
            dbs = new ArrayList<>(dbs);
        }
        List<RocksDBSession> sessions = new ArrayList<>();
        for (String dbName : getGraphNames()) {
            RocksDBSession session = this.queryGraphDB(dbName);
            if (session != null) {
                dbs.add(session.getDB());
                sessions.add(session);
            }
        }
        try {
            HashSet<Cache> allCaches = new HashSet<>();
            if (caches != null) {
                allCaches.addAll(caches);
            }
            allCaches.add((Cache) hugeConfig.getProperty(RocksDBOptions.WRITE_CACHE));
            allCaches.add((Cache) hugeConfig.getProperty(RocksDBOptions.BLOCK_CACHE));
            return MemoryUtil.getApproximateMemoryUsageByType(dbs, allCaches);
        } finally {
            sessions.forEach(session -> {
                session.close();
            });
        }
    }

    public void addRocksdbChangedListener(RocksdbChangedListener listener) {
        rocksdbChangedListeners.add(listener);
    }

    /**
     * Removes a previously registered {@link RocksdbChangedListener}. Counterpart to
     * {@link #addRocksdbChangedListener}; used to detach a listener during shutdown or test
     * teardown so it no longer receives forwarded RocksDB events.
     */
    public void removeRocksdbChangedListener(RocksdbChangedListener listener) {
        rocksdbChangedListeners.remove(listener);
    }

    /**
     * Notifies all registered listeners that a RocksDB truncate operation is about to start.
     *
     * @param dbName  the graph / partition name
     * @param dbPath  the absolute path of the RocksDB directory
     */
    public void notifyTruncateBegin(String dbName, String dbPath) {
        rocksdbChangedListeners.forEach(listener -> listener.onDBTruncateBegin(dbName, dbPath));
    }

    /**
     * Notifies all registered listeners that a RocksDB has been truncated.
     * This should be called after the database content has been cleared to allow
     * listeners (e.g., cloud storage) to clean up remote state.
     *
     * @param dbName  the graph / partition name
     * @param dbPath  the absolute path of the RocksDB directory
     */
    public void notifyTruncate(String dbName, String dbPath) {
        rocksdbChangedListeners.forEach(listener -> listener.onDBTruncated(dbName, dbPath));
    }

    /**
     * Notifies all registered listeners that a RocksDB truncate operation FAILED partway (after
     * {@link #notifyTruncateBegin} but before completion). Listeners must undo any "truncation in
     * progress" suppression they applied in {@link RocksdbChangedListener#onDBTruncateBegin}
     * <em>without</em> purging remote state — the truncate did not complete, so the data that was
     * meant to be cleared may still be present locally and must remain recoverable.
     *
     * @param dbName  the graph / partition name
     * @param dbPath  the absolute path of the RocksDB directory
     */
    public void notifyTruncateAbort(String dbName, String dbPath) {
        rocksdbChangedListeners.forEach(listener -> listener.onDBTruncateAbort(dbName, dbPath));
    }

    /**
     * Flushes the MemTable of the named RocksDB session to disk, creating an SST file.
     * This triggers {@link RocksdbChangedListener#onTableFileCreated} for every registered
     * listener (including cloud-storage upload).
     *
     * @param dbName  the graph / partition name
     * @param wait    if {@code true} the call blocks until the flush completes
     */
    public void flushSession(String dbName, boolean wait) {
        RocksDBSession session = dbSessionMap.get(dbName);
        if (session != null) {
            try {
                session.flush(wait);
                log.debug("Flushed RocksDB session for db={}", dbName);
            } catch (Exception e) {
                log.warn("Failed to flush RocksDB session for db={}: {}", dbName, e.getMessage());
            }
        }
    }

    /**
     * Returns the live SST files currently referenced by the named RocksDB session, with the
     * owning column-family name for each. Used by cloud storage to (a) confirm the full live set
     * is present in cloud before deleting a superseded object, and (b) route read-miss hydration
     * to the correct column family.
     *
     * <p>The returned metadata reflects RocksDB's manifest, so it lists files that RocksDB believes
     * are live even if the local file has since been evicted from disk.
     *
     * @param dbName the graph / partition name
     * @return live SST files (possibly empty); never {@code null}
     */
    public List<LiveSstFile> getLiveSstFiles(String dbName) {
        RocksDBSession session = dbSessionMap.get(dbName);
        if (session == null) {
            return List.of();
        }
        RocksDB db = session.getDB();
        if (db == null) {
            return List.of();
        }
        List<LiveSstFile> result = new ArrayList<>();
        for (LiveFileMetaData md : db.getLiveFilesMetaData()) {
            String dir = md.path();
            String name = md.fileName();
            if (name == null || !name.endsWith(".sst")) {
                continue;
            }
            String absolutePath;
            if (name.startsWith(File.separator) || name.startsWith("/")) {
                absolutePath = dir.endsWith(File.separator) || dir.endsWith("/")
                               ? dir.substring(0, dir.length() - 1) + name
                               : dir + name;
            } else {
                absolutePath = dir.endsWith(File.separator) || dir.endsWith("/")
                               ? dir + name
                               : dir + File.separator + name;
            }
            String cfName = new String(md.columnFamilyName(), java.nio.charset.StandardCharsets.UTF_8);
            result.add(new LiveSstFile(absolutePath, cfName));
        }
        return result;
    }

    /** A live SST file and the column family it belongs to. */
    public static final class LiveSstFile {

        private final String absolutePath;
        private final String cfName;

        public LiveSstFile(String absolutePath, String cfName) {
            this.absolutePath = absolutePath;
            this.cfName = cfName;
        }

        public String getAbsolutePath() {
            return absolutePath;
        }

        public String getCfName() {
            return cfName;
        }
    }

    /**
     * Captures a point-in-time, internally-consistent copy of the named DB's RocksDB metadata
     * ({@code CURRENT}, {@code MANIFEST-*}, {@code OPTIONS-*}, the WAL {@code *.log} tail, and
     * hard-links to the live {@code *.sst} set) into a temporary sibling directory of the DB, via
     * RocksDB {@link org.rocksdb.Checkpoint} (the same primitive {@code saveSnapshot} uses).
     *
     * <p>This is the capture primitive behind metadata durability: the returned snapshot exposes
     * the exact {@code {manifest, live-SST-set}} pair as of the checkpoint instant, so a caller can
     * mirror a consistent set of objects to cloud storage without racing live compaction. The
     * hard-linked SSTs also pin their content for the lifetime of the snapshot, so an SST cannot be
     * physically removed by compaction between capture and upload.
     *
     * <p>The caller <b>must</b> call {@link MetadataSnapshot#cleanup()} when done to remove the
     * temporary directory (which only contains metadata copies and SST hard-links — deleting it
     * never touches the real SST files).
     *
     * @param dbName the graph / partition name
     * @return the captured snapshot, or {@code null} if the session is not open
     */
    public MetadataSnapshot captureMetadataSnapshot(String dbName) {
        RocksDBSession session = dbSessionMap.get(dbName);
        if (session == null || session.getDB() == null) {
            return null;
        }
        return session.captureMetadataCheckpoint();
    }

    /**
     * A consistent snapshot of a RocksDB instance's metadata plus hard-links to its live SST set,
     * materialised under {@link #getTempDir()} by {@link #captureMetadataSnapshot(String)}.
     *
     * <p>File names are relative to the checkpoint directory. {@link #getDbDir()} is the real DB
     * directory the metadata belongs to — remote keys must be derived from {@code dbDir + name} (not
     * the temp directory) so a restore lands each file back at its original path.
     */
    public static final class MetadataSnapshot {

        private final String dbDir;
        private final String tempDir;
        private final String currentFileName;
        private final String manifestFileName;
        private final List<String> optionsFileNames;
        private final List<String> sstFileNames;
        private final long generation;

        public MetadataSnapshot(String dbDir, String tempDir, String currentFileName,
                                String manifestFileName, List<String> optionsFileNames,
                                List<String> sstFileNames) {
            this(dbDir, tempDir, currentFileName, manifestFileName,
                 optionsFileNames, sstFileNames,
                 parseManifestGeneration(manifestFileName));
        }

        public MetadataSnapshot(String dbDir, String tempDir, String currentFileName,
                                String manifestFileName, List<String> optionsFileNames,
                                List<String> sstFileNames,
                                long generation) {
            this.dbDir = dbDir;
            this.tempDir = tempDir;
            this.currentFileName = currentFileName;
            this.manifestFileName = manifestFileName;
            this.optionsFileNames = optionsFileNames;
            this.sstFileNames = sstFileNames;
            this.generation = generation;
        }

        /** The real DB directory the captured metadata belongs to (for remote-key derivation). */
        public String getDbDir() {
            return dbDir;
        }

        /** The temporary checkpoint directory holding the metadata copies and SST hard-links. */
        public String getTempDir() {
            return tempDir;
        }

        /** The {@code CURRENT} file name (always {@code "CURRENT"}), or {@code null} if absent. */
        public String getCurrentFileName() {
            return currentFileName;
        }

        /** The {@code MANIFEST-<n>} file name referenced by {@code CURRENT}, or {@code null}. */
        public String getManifestFileName() {
            return manifestFileName;
        }

        /** {@code OPTIONS-<n>} file names present in the checkpoint. */
        public List<String> getOptionsFileNames() {
            return optionsFileNames;
        }

        /** {@code *.sst} file names the captured manifest references (as hard-links). */
        public List<String> getSstFileNames() {
            return sstFileNames;
        }

        /** Monotonic capture generation used to reject stale metadata publications. */
        public long getGeneration() {
            return generation;
        }

        private static long parseManifestGeneration(String manifestFileName) {
            if (manifestFileName == null || !manifestFileName.startsWith("MANIFEST-")) {
                return -1L;
            }
            try {
                return Long.parseLong(manifestFileName.substring("MANIFEST-".length()));
            } catch (NumberFormatException ignored) {
                return -1L;
            }
        }

        /** Removes the temporary checkpoint directory. Never touches the real SST files. */
        public void cleanup() {
            if (tempDir == null) {
                return;
            }
            try {
                FileUtils.deleteDirectory(new File(tempDir));
            } catch (Exception e) {
                log.warn("Failed to clean up metadata checkpoint temp dir {}: {}",
                         tempDir, e.getMessage());
            }
        }
    }

    public boolean onReadMiss(RocksDBSession session, String table, byte[] key) {
        if (session == null) {
            return false;
        }
        boolean hydrated = false;
        for (RocksdbChangedListener listener : rocksdbChangedListeners) {
            try {
                if (listener.onReadMiss(session, table, key)) {
                    hydrated = true;
                }
            } catch (Exception e) {
                log.warn("onReadMiss listener failed for db={}, table={}: {}",
                         session.getGraphName(), table, e.getMessage());
            }
        }
        return hydrated;
    }

    /**
     * Returns the singleton {@link RocksdbEventListener} that should be registered
     * with every new {@link org.rocksdb.DBOptions} via
     * {@link org.rocksdb.DBOptions#setListeners}.
     */
    public RocksdbEventListener getEventListener() {
        return rocksdbEventListener;
    }

    public interface RocksdbChangedListener {

        default void onCompacted(String dbName) {
        }

        default void onDBOpening(String dbName, String dbPath) {
        }

        /**
         * Called immediately after a new RocksDB instance has been opened for the first time.
         *
         * <p>Implementations can use this callback to upload any SST files that already exist
         * in {@code dbPath} (e.g. from a previous run) and to trigger a MemTable flush so that
         * any WAL-recovered data is also written to SST files and forwarded to cloud storage.
         *
         * @param dbName logical name of the graph / partition
         * @param dbPath absolute path of the RocksDB directory
         */
        default void onDBCreated(String dbName, String dbPath) {
        }

        default void onDBDeleteBegin(String dbName, String filePath) {
        }

        default void onDBDeleted(String dbName, String filePath) {
        }

        default void onDBTruncateBegin(String dbName, String filePath) {
        }

        default void onDBTruncated(String dbName, String filePath) {
        }

        /**
         * Called when a truncate operation failed after {@link #onDBTruncateBegin} but before
         * completion. Implementations must clear any "truncation in progress" suppression they
         * started, WITHOUT purging remote state (the data intended for clearing may still exist
         * locally and must stay recoverable).
         */
        default void onDBTruncateAbort(String dbName, String filePath) {
        }

        default void onDBSessionReleased(RocksDBSession dbSession) {
        }

        /**
         * Called after a new SST file has been successfully created by RocksDB.
         *
         * @param dbName   RocksDB instance name
         * @param cfName   column-family name
         * @param filePath absolute path of the new SST file
         * @param fileSize size of the file in bytes
         */
        default void onTableFileCreated(String dbName, String cfName,
                                        String filePath, long fileSize) {
        }

        /**
         * Called after an SST file has been deleted by RocksDB.
         *
         * @param dbName   RocksDB instance name
         * @param cfName   column-family name
         * @param filePath absolute path of the deleted SST file
         */
        default void onTableFileDeleted(String dbName, String cfName, String filePath) {
        }

        /**
         * Called when a get() operation returns null so listeners can attempt cloud hydration.
         *
         * @param session RocksDB session where miss happened
         * @param table   target column-family/table
         * @param key     requested key
         * @return true if listener hydrated new local data and caller should retry get()
         */
        default boolean onReadMiss(RocksDBSession session, String table, byte[] key) {
            return false;
        }
    }

    class DBSessionWatcher {
        public RocksDBSession dbSession;
        public Long timestamp;
        public int deleteAttempts = 0;

        public DBSessionWatcher(RocksDBSession dbSession) {
            this.dbSession = dbSession;
            timestamp = System.currentTimeMillis();
        }
    }
}
