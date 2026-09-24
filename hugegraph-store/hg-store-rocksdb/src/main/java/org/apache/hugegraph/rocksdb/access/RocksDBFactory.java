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
import org.rocksdb.MemoryUsageType;
import org.rocksdb.MemoryUtil;
import org.rocksdb.RocksDB;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class RocksDBFactory {

    private static final List<RocksdbChangedListener> rocksdbChangedListeners = new ArrayList<>();
    private static RocksDBFactory dbFactory;

    static {
        RocksDB.loadLibrary();
    }

    private final Map<String, RocksDBSession> dbSessionMap = new ConcurrentHashMap<>();
    private final List<DBSessionWatcher> destroyGraphDBs = new CopyOnWriteArrayList<>();
    private final ReentrantReadWriteLock operateLock;
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
                        try {
                            watcher.dbSession.shutdown();
                            FileUtils.deleteDirectory(new File(watcher.dbSession.getDbPath()));
                            rocksdbChangedListeners.forEach(listener -> {
                                listener.onDBDeleted(watcher.dbSession.getGraphName(),
                                                     watcher.dbSession.getDbPath());
                            });
                            log.info("removed db {} and delete files",
                                     watcher.dbSession.getDbPath());
                        } catch (Exception e) {
                            log.error("DestroyGraphDB exception {}", e);
                        }
                        destroyGraphDBs.remove(watcher);
                    } else if (watcher.timestamp < (System.currentTimeMillis() - 1800 * 1000)) {
                        log.warn("DB {}  has not been deleted refCount is {}, time is {} seconds",
                                 watcher.dbSession.getDbPath(),
                                 watcher.dbSession.getRefCount(),
                                 (System.currentTimeMillis() - watcher.timestamp) / 1000);
                    } else {
                        // Force delete after timeout (30min)
                        watcher.dbSession.forceResetRefCount();
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
    }

    public RocksDBSession createGraphDB(String dbPath, String dbName) {
        return createGraphDB(dbPath, dbName, 0);
    }

    public RocksDBSession createGraphDB(String dbPath, String dbName, long version) {
        if (closing.get()) {
            throw new RuntimeException("db closed");
        }
        operateLock.writeLock().lock();
        try {
            RocksDBSession dbSession = dbSessionMap.get(dbName);
            if (dbSession == null) {
                log.info("create rocksdb for {}", dbName);
                dbSession = new RocksDBSession(this.hugeConfig, dbPath, dbName, version);
                dbSessionMap.put(dbName, dbSession);
            }
            return dbSession.clone();
        } finally {
            operateLock.writeLock().unlock();
        }
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
        releaseGraphDB(dbName);
        // Add delete mark
        if (dbSession != null) {
            destroyGraphDBs.add(new DBSessionWatcher(dbSession));
            rocksdbChangedListeners.forEach(listener -> {
                listener.onDBDeleteBegin(dbSession.getGraphName(), dbSession.getDbPath());
            });
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

    public interface RocksdbChangedListener {

        default void onCompacted(String dbName) {
        }

        default void onDBDeleteBegin(String dbName, String filePath) {
        }

        default void onDBDeleted(String dbName, String filePath) {
        }

        default void onDBSessionReleased(RocksDBSession dbSession) {
        }
    }

    class DBSessionWatcher {
        public RocksDBSession dbSession;
        public Long timestamp;

        public DBSessionWatcher(RocksDBSession dbSession) {
            this.dbSession = dbSession;
            timestamp = System.currentTimeMillis();
        }
    }
}
