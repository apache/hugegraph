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

package org.apache.hugegraph.backend.store.rocksdb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.serializer.MergeIterator;
import org.apache.hugegraph.backend.store.AbstractBackendStore;
import org.apache.hugegraph.backend.store.BackendAction;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendSessionPool;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.BackendTable;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.exception.ConnectionException;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.Consumers;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.ExecutorUtil;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.Log;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;

public abstract class RocksDBStore extends AbstractBackendStore<RocksDBSessions.Session> {

    private static final Logger LOG = Log.logger(RocksDBStore.class);

    private static final BackendFeatures FEATURES = new RocksDBFeatures();

    private final String store;
    private final String database;

    private final BackendStoreProvider provider;
    private final Map<HugeType, RocksDBTable> tables;
    private final Map<String, RocksDBTable> olapTables;

    private String dataPath;
    private RocksDBSessions sessions;
    private final Map<HugeType, String> tableDiskMapping;
    // DataPath:RocksDB mapping
    private final ConcurrentMap<String, RocksDBSessions> dbs;
    private final ReadWriteLock storeLock;

    private static final String TABLE_GENERAL_KEY = "general";
    private static final String DB_OPEN = "db-open-%s";

    private static final long DB_OPEN_TIMEOUT = 600L; // unit s
    private static final long DB_CLOSE_TIMEOUT = 30L; // unit s
    /**
     * This is threads number used to concurrently opening RocksDB dbs,
     * 8 is supposed enough due to configurable data disks and
     * disk number of one machine
     */
    private static final int OPEN_POOL_THREADS = 8;
    private boolean isGraphStore;

    public RocksDBStore(final BackendStoreProvider provider,
                        final String database, final String store) {
        this.tables = new HashMap<>();
        this.olapTables = new HashMap<>();
        this.provider = provider;
        this.database = database;
        this.store = store;
        this.sessions = null;
        this.tableDiskMapping = new HashMap<>();
        this.dbs = new ConcurrentHashMap<>();
        this.storeLock = new ReentrantReadWriteLock();

        this.registerMetaHandlers();
    }

    private void registerMetaHandlers() {
        Supplier<List<RocksDBSessions>> dbsGet = () -> {
            List<RocksDBSessions> dbs = new ArrayList<>();
            dbs.add(this.sessions);
            dbs.addAll(this.tableDBMapping().values());
            return dbs;
        };

        this.registerMetaHandler("metrics", (session, meta, args) -> {
            RocksDBMetrics metrics = new RocksDBMetrics(dbsGet.get(), session);
            return metrics.metrics();
        });

        this.registerMetaHandler("compact", (session, meta, args) -> {
            RocksDBMetrics metrics = new RocksDBMetrics(dbsGet.get(), session);
            return metrics.compact();
        });
    }

    protected void registerTableManager(HugeType type, RocksDBTable table) {
        this.tables.put(type, table);
    }

    protected void registerTableManager(String name, RocksDBTable table) {
        this.olapTables.put(name, table);
    }

    protected void unregisterTableManager(String name) {
        this.olapTables.remove(name);
    }

    @Override
    protected final RocksDBTable table(HugeType type) {
        RocksDBTable table = this.tables.get(convertTaskOrServerToVertex(type));
        if (table == null) {
            throw new BackendException("Unsupported table: '%s'", type);
        }
        return table;
    }

    protected final RocksDBTable table(String name) {
        RocksDBTable table = this.olapTables.get(name);
        if (table == null) {
            throw new BackendException("Unsupported table: '%s'", name);
        }
        return table;
    }

    protected List<String> tableNames() {
        List<String> tables = this.tables.values().stream()
                                         .map(BackendTable::table)
                                         .collect(Collectors.toList());
        tables.addAll(this.olapTables());
        return tables;
    }

    protected List<String> olapTables() {
        return this.olapTables.values().stream().map(RocksDBTable::table)
                              .collect(Collectors.toList());
    }

    protected List<String> tableNames(HugeType type) {
        return type != HugeType.OLAP ? Collections.singletonList(this.table(type).table()) :
               this.olapTables();
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.database;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public synchronized void open(HugeConfig config) {
        LOG.debug("Store open: {}", this.store);

        E.checkNotNull(config, "config");
        String graphStore = config.get(CoreOptions.STORE_GRAPH);
        this.isGraphStore = this.store.equals(graphStore);
        this.dataPath = config.get(RocksDBOptions.DATA_PATH);

        if (this.sessions != null && !this.sessions.closed()) {
            LOG.debug("Store {} has been opened before", this.store);
            this.useSessions();
            return;
        }

        List<Future<?>> futures = new ArrayList<>();
        ExecutorService openPool = ExecutorUtil.newFixedThreadPool(OPEN_POOL_THREADS, DB_OPEN);
        // Open base disk
        futures.add(openPool.submit(() -> {
            this.sessions = this.open(config, this.tableNames());
        }));

        // Open tables with optimized disk
        Map<String, String> disks = config.getMap(RocksDBOptions.DATA_DISKS);
        Set<String> openedDisks = new HashSet<>();
        if (!disks.isEmpty()) {
            this.parseTableDiskMapping(disks, this.dataPath);
            for (Entry<HugeType, String> e : this.tableDiskMapping.entrySet()) {
                String disk = e.getValue();
                if (openedDisks.contains(disk)) {
                    continue;
                }
                openedDisks.add(disk);
                List<String> tables = this.tableNames(e.getKey());
                futures.add(openPool.submit(() -> {
                    this.open(config, disk, disk, tables);
                }));
            }
        }

        try {
            this.waitOpenFinished(futures);
        } finally {
            this.shutdownOpenPool(openPool);
        }
    }

    private void waitOpenFinished(List<Future<?>> futures) {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Throwable e) {
                if (e.getCause() instanceof ConnectionException) {
                    throw new ConnectionException("Failed to open RocksDB store", e);
                }
                throw new BackendException("Failed to open RocksDB store", e);
            }
        }
    }

    private void shutdownOpenPool(ExecutorService openPool) {
        if (openPool.isShutdown()) {
            return;
        }

        /*
         * Transfer the session holder from db-open thread to main thread,
         * otherwise once the db-open thread pool is closed, we can no longer
         * close the session created by it, which will cause the rocksdb
         * instance fail to close
         */
        this.useSessions();
        try {
            Consumers.executeOncePerThread(openPool, OPEN_POOL_THREADS,
                                           this::closeSessions, DB_CLOSE_TIMEOUT);
        } catch (InterruptedException e) {
            throw new BackendException("Failed to close session opened by open-pool");
        }

        boolean terminated;
        openPool.shutdown();
        try {
            terminated = openPool.awaitTermination(DB_OPEN_TIMEOUT,
                                                   TimeUnit.SECONDS);
        } catch (Throwable e) {
            throw new BackendException("Failed to wait db-open thread pool shutdown", e);
        }
        if (!terminated) {
            LOG.warn("Timeout when waiting db-open thread pool shutdown");
        }
        openPool.shutdownNow();
    }

    protected RocksDBSessions open(HugeConfig config, List<String> tableNames) {
        String dataPath = this.wrapPath(config.get(RocksDBOptions.DATA_PATH));
        String walPath = this.wrapPath(config.get(RocksDBOptions.WAL_PATH));
        return this.open(config, dataPath, walPath, tableNames);
    }

    protected RocksDBSessions open(HugeConfig config, String dataPath,
                                   String walPath, List<String> tableNames) {
        LOG.info("Opening RocksDB with data path: {}", dataPath);
        RocksDBSessions sessions = null;
        try {
            sessions = this.openSessionPool(config, dataPath,
                                            walPath, tableNames);
        } catch (RocksDBException e) {
            RocksDBSessions origin = this.dbs.get(dataPath);
            if (origin != null) {
                if (e.getMessage().contains("No locks available")) {
                    /*
                     * Open twice, copy a RocksDBSessions reference, since from
                     * v0.11.2 release we don't support multi graphs share
                     * rocksdb instance (before v0.11.2 graphs with different
                     * CF-prefix share one rocksdb instance and data path),
                     * so each graph has its independent data paths, but multi
                     * CFs may share same optimized disk(or optimized disk path).
                     */
                    sessions = origin.copy(config, this.database, this.store);
                }
            }

            if (e.getMessage().contains("Column family not found")) {
                if (this.isSchemaStore()) {
                    LOG.info("Failed to open RocksDB '{}' with database '{}'," +
                             " try to init CF later", dataPath, this.database);
                }
                List<String> none;
                boolean existsOtherKeyspace = existsOtherKeyspace(dataPath);
                if (existsOtherKeyspace) {
                    // Open a keyspace after other keyspace closed
                    // Set to empty list to open old CFs(of other keyspace)
                    none = ImmutableList.of();
                } else {
                    // Before init the first keyspace
                    none = null;
                }
                try {
                    sessions = this.openSessionPool(config, dataPath, walPath, none);
                } catch (RocksDBException e1) {
                    e = e1;
                }
                if (sessions == null && !existsOtherKeyspace) {
                    LOG.error("Failed to open RocksDB with default CF, " +
                              "is there data for other programs: {}", dataPath);
                }
            }

            if (sessions == null) {
                // Error after trying other ways
                LOG.error("Failed to open RocksDB '{}'", dataPath, e);
                throw new ConnectionException("Failed to open RocksDB '%s'", e, dataPath);
            }
        }

        if (sessions != null) {
            // May override the original session pool
            this.dbs.put(dataPath, sessions);
            sessions.session().open();
            LOG.debug("Store opened: {}", dataPath);
        }

        return sessions;
    }

    protected RocksDBSessions openSessionPool(HugeConfig config,
                                              String dataPath, String walPath,
                                              List<String> tableNames) throws RocksDBException {
        if (tableNames == null) {
            return new RocksDBStdSessions(config, this.database, this.store, dataPath, walPath);
        } else {
            return new RocksDBStdSessions(config, this.database, this.store,
                                          dataPath, walPath, tableNames);
        }
    }

    protected String wrapPath(String path) {
        // Ensure the `path` exists
        try {
            FileUtils.forceMkdir(FileUtils.getFile(path));
        } catch (IOException e) {
            throw new BackendException(e.getMessage(), e);
        }
        // Join with store type
        return Paths.get(path, this.store).toString();
    }

    protected Map<String, RocksDBSessions> tableDBMapping() {
        Map<String, RocksDBSessions> tableDBMap = InsertionOrderUtil.newMap();
        for (Entry<HugeType, String> e : this.tableDiskMapping.entrySet()) {
            HugeType type = e.getKey();
            RocksDBSessions db = this.db(e.getValue());
            String key = type != HugeType.OLAP ? this.table(type).table() : type.string();
            tableDBMap.put(key, db);
        }
        return tableDBMap;
    }

    protected ReadWriteLock storeLock() {
        return this.storeLock;
    }

    @Override
    public void close() {
        LOG.debug("Store close: {}", this.store);
        this.checkOpened();
        this.closeSessions();
    }

    @Override
    public boolean opened() {
        this.checkDbOpened();
        return this.sessions.session().opened();
    }

    @Override
    public void mutate(BackendMutation mutation) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Store {} mutation: {}", this.store, mutation);
            }

            for (HugeType type : mutation.types()) {
                RocksDBSessions.Session session = this.session(type);
                for (Iterator<BackendAction> it = mutation.mutation(type); it.hasNext(); ) {
                    this.mutate(session, it.next());
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    private void mutate(RocksDBSessions.Session session, BackendAction item) {
        BackendEntry entry = item.entry();
        RocksDBTable table;

        if (!entry.olap()) {
            // Oltp table
            table = this.table(entry.type());
        } else {
            if (entry.type().isIndex()) {
                // Olap index
                table = this.table(this.olapTableName(entry.type()));
            } else {
                // Olap vertex
                table = this.table(this.olapTableName(entry.subId()));
            }
            session = this.session(HugeType.OLAP);
        }

        switch (item.action()) {
            case INSERT:
                table.insert(session, entry);
                break;
            case DELETE:
                table.delete(session, entry);
                break;
            case APPEND:
                table.append(session, entry);
                break;
            case ELIMINATE:
                table.eliminate(session, entry);
                break;
            case UPDATE_IF_PRESENT:
                table.updateIfPresent(session, entry);
                break;
            case UPDATE_IF_ABSENT:
                table.updateIfAbsent(session, entry);
                break;
            default:
                throw new AssertionError(String.format("Unsupported mutate action: %s",
                                                       item.action()));
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();

        try {
            HugeType tableType = RocksDBTable.tableType(query);
            RocksDBTable table;
            RocksDBSessions.Session session;
            if (query.olap()) {
                table = this.table(this.olapTableName(tableType));
                session = this.session(HugeType.OLAP);
            } else {
                table = this.table(tableType);
                session = this.session(tableType);
            }

            Iterator<BackendEntry> entries = table.query(session, query);
            // Merge olap results as needed
            Set<Id> olapPks = query.olapPks();
            if (this.isGraphStore && !olapPks.isEmpty()) {
                List<Iterator<BackendEntry>> iterators = new ArrayList<>();
                for (Id pk : olapPks) {
                    Query q = query.copy();
                    table = this.table(this.olapTableName(pk));
                    iterators.add(table.query(this.session(HugeType.OLAP), q));
                }
                entries = new MergeIterator<>(entries, iterators, BackendEntry::mergeable);
            }
            return entries;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Number queryNumber(Query query) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            HugeType tableType = RocksDBTable.tableType(query);
            RocksDBTable table = this.table(tableType);
            return table.queryNumber(this.session(tableType), query);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public synchronized void init() {
        Lock writeLock = this.storeLock.writeLock();
        writeLock.lock();
        try {
            this.checkDbOpened();
            // Create tables with main disk
            this.createTable(this.sessions, this.tableNames().toArray(new String[0]));

            // Create table with optimized disk
            Map<String, RocksDBSessions> tableDBMap = this.tableDBMapping();
            for (Map.Entry<String, RocksDBSessions> e : tableDBMap.entrySet()) {
                if (e.getKey().equals(HugeType.OLAP.string())) {
                    for (String olapTable : this.olapTables()) {
                        this.createTable(e.getValue(), olapTable);
                    }
                } else {
                    this.createTable(e.getValue(), e.getKey());
                }
            }

            LOG.debug("Store initialized: {}", this.store);
        } finally {
            writeLock.unlock();
        }
    }

    protected void createTable(RocksDBSessions db, String... tables) {
        try {
            db.createTable(tables);
        } catch (RocksDBException e) {
            throw new BackendException("Failed to create tables %s for '%s'",
                                       e, Arrays.asList(tables), this.store);
        }
    }

    @Override
    public synchronized void clear(boolean clearSpace) {
        Lock writeLock = this.storeLock.writeLock();
        writeLock.lock();
        try {
            this.checkDbOpened();
            // Drop tables with main disk
            this.dropTable(this.sessions, this.tableNames().toArray(new String[0]));

            // Drop tables with optimized disk
            Map<String, RocksDBSessions> tableDBMap = this.tableDBMapping();
            for (Map.Entry<String, RocksDBSessions> e : tableDBMap.entrySet()) {
                if (e.getKey().equals(HugeType.OLAP.string())) {
                    for (String olapTable : this.olapTables()) {
                        this.dropTable(e.getValue(), olapTable);
                    }
                } else {
                    this.dropTable(e.getValue(), e.getKey());
                }
            }

            LOG.debug("Store cleared: {}", this.store);
        } finally {
            writeLock.unlock();
        }
    }

    protected void dropTable(RocksDBSessions db, String... tables) {
        try {
            db.dropTable(tables);
        } catch (BackendException e) {
            if (e.getMessage().contains("is not opened")) {
                return;
            }
            throw e;
        } catch (RocksDBException e) {
            throw new BackendException("Failed to drop tables %s for '%s'",
                                       e, Arrays.asList(tables), this.store);
        }
    }

    @Override
    public boolean initialized() {
        this.checkDbOpened();
        if (!this.opened()) {
            return false;
        }

        for (String table : this.tableNames()) {
            if (!this.sessions.existsTable(table)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized void truncate() {
        Lock writeLock = this.storeLock.writeLock();
        writeLock.lock();
        try {
            this.checkOpened();

            this.clear(false);
            this.init();
            // Clear write-batch
            this.dbs.values().forEach(BackendSessionPool::forceResetSessions);
            LOG.debug("Store truncated: {}", this.store);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void beginTx() {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            for (RocksDBSessions.Session session : this.session()) {
                assert !session.hasChanges();
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void commitTx() {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            // Unable to guarantee atomicity when committing multi sessions
            for (RocksDBSessions.Session session : this.session()) {
                Object count = session.commit();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Store {} committed {} items", this.store, count);
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void rollbackTx() {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            for (RocksDBSessions.Session session : this.session()) {
                session.rollback();
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Map<String, String> createSnapshot(String snapshotPrefix) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            Map<String, String> uniqueSnapshotDirMaps = new HashMap<>();
            // Every rocksdb instance should create a snapshot
            for (Map.Entry<String, RocksDBSessions> entry : this.dbs.entrySet()) {
                // Like: parent_path/rocksdb-data/*, * maybe g,m,s
                Path originDataPath = Paths.get(entry.getKey()).toAbsolutePath();
                Path parentParentPath = originDataPath.toAbsolutePath().getParent().getParent();
                // Like: rocksdb-data/*
                Path pureDataPath = parentParentPath.relativize(originDataPath);
                // Like: parent_path/snapshot_rocksdb-data/*
                Path snapshotPath = parentParentPath.resolve(snapshotPrefix +
                                                             "_" + pureDataPath);
                LOG.debug("Create snapshot '{}' for origin data path '{}'",
                          snapshotPath, originDataPath);
                RocksDBSessions sessions = entry.getValue();
                sessions.createSnapshot(snapshotPath.toString());

                String snapshotDir = snapshotPath.toAbsolutePath().getParent().toString();
                // Find correspond data HugeType key
                String diskTableKey = this.findDiskTableKeyByPath(entry.getKey());
                uniqueSnapshotDirMaps.put(snapshotDir, diskTableKey);
            }
            LOG.info("The store '{}' create snapshot successfully", this);
            return uniqueSnapshotDirMaps;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void resumeSnapshot(String snapshotPrefix, boolean deleteSnapshot) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            if (!this.opened()) {
                return;
            }
            Map<String, RocksDBSessions> snapshotPaths = new HashMap<>();
            for (Map.Entry<String, RocksDBSessions> entry : this.dbs.entrySet()) {
                RocksDBSessions sessions = entry.getValue();
                String snapshotPath = sessions.buildSnapshotPath(snapshotPrefix);
                LOG.debug("The origin data path: {}", entry.getKey());
                if (!deleteSnapshot) {
                    snapshotPath = sessions.hardLinkSnapshot(snapshotPath);
                }
                LOG.debug("The snapshot data path: {}", snapshotPath);
                snapshotPaths.put(snapshotPath, sessions);
            }

            for (Map.Entry<String, RocksDBSessions> entry :
                    snapshotPaths.entrySet()) {
                String snapshotPath = entry.getKey();
                RocksDBSessions sessions = entry.getValue();
                sessions.resumeSnapshot(snapshotPath);

                if (deleteSnapshot) {
                    // Delete empty snapshot parent directory
                    Path parentPath = Paths.get(snapshotPath).toAbsolutePath().getParent();
                    if (Files.list(parentPath).count() == 0) {
                        FileUtils.deleteDirectory(parentPath.toFile());
                    }
                }
            }
            LOG.info("The store '{}' resume snapshot successfully", this);
        } catch (RocksDBException | IOException e) {
            throw new BackendException("Failed to resume snapshot", e);
        } finally {
            readLock.unlock();
        }
    }

    private void useSessions() {
        for (RocksDBSessions sessions : this.sessions()) {
            sessions.useSession();
        }
    }

    private void closeSessions() {
        Iterator<Map.Entry<String, RocksDBSessions>> iter = this.dbs.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, RocksDBSessions> entry = iter.next();
            RocksDBSessions sessions = entry.getValue();
            boolean closed = sessions.close();
            if (closed) {
                iter.remove();
            }
        }
    }

    private final Collection<RocksDBSessions> sessions() {
        return this.dbs.values();
    }

    private final List<RocksDBSessions.Session> session() {
        this.checkDbOpened();

        // Collect session of standard disk
        RocksDBSessions.Session session = this.sessions.session();
        if (!session.opened()) {
            this.checkOpened();
        }

        if (this.tableDiskMapping.isEmpty()) {
            return ImmutableList.of(session);
        }

        // Collect session of each table with optimized disk
        List<RocksDBSessions.Session> list = new ArrayList<>(this.tableDiskMapping.size() + 1);
        list.add(session);
        for (String disk : this.tableDiskMapping.values()) {
            RocksDBSessions.Session optimizedSession = this.db(disk).session();
            assert optimizedSession.opened();
            list.add(optimizedSession);
        }
        return list;
    }

    @Override
    protected RocksDBSessions.Session session(HugeType tableType) {
        this.checkDbOpened();

        RocksDBSessions.Session session;
        String disk = this.tableDiskMapping.get(tableType);
        if (disk != null) {
            // Optimized disk
            session = this.db(disk).session();
        } else {
            // Standard disk
            session = this.sessions.session();
        }

        if (!session.opened()) {
            this.checkOpened();
        }
        return session;
    }

    private void parseTableDiskMapping(Map<String, String> disks, String dataPath) {
        // reset and parse
        this.tableDiskMapping.clear();
        for (Map.Entry<String, String> disk : disks.entrySet()) {
            // The format of `disk` like: `graph/vertex: /path/to/disk1`
            String name = disk.getKey();
            String path = disk.getValue();
            E.checkArgument(!dataPath.equals(path),
                            "Invalid disk path (can't be the same as data_path): '%s'", path);
            E.checkArgument(!name.isEmpty() && !path.isEmpty(),
                            "Invalid disk format: '%s', expect `NAME:PATH`", disk);
            String[] pair = name.split("/", 2);
            E.checkArgument(pair.length == 2,
                            "Invalid disk key format: '%s', expect `STORE/TABLE`", name);
            String store = pair[0].trim();
            HugeType table = HugeType.valueOf(pair[1].trim().toUpperCase());
            if (this.store.equals(store)) {
                path = this.wrapPath(path);
                this.tableDiskMapping.put(table, path);
            }
        }
    }

    @SuppressWarnings("unused")
    private Map<String, String> reportDiskMapping() {
        Map<String, String> diskMapping = new HashMap<>();
        diskMapping.put(TABLE_GENERAL_KEY, this.dataPath);
        for (Map.Entry<HugeType, String> e : this.tableDiskMapping.entrySet()) {
            String key = this.store + "/" + e.getKey().name();
            String value = Paths.get(e.getValue()).toAbsolutePath().getParent().toString();
            diskMapping.put(key, value);
        }
        return diskMapping;
    }

    private String findDiskTableKeyByPath(String diskPath) {
        String diskTableKey = TABLE_GENERAL_KEY;
        for (Map.Entry<HugeType, String> e : this.tableDiskMapping.entrySet()) {
            if (diskPath.equals(e.getValue())) {
                diskTableKey = this.store + "/" + e.getKey().name();
                break;
            }
        }
        return diskTableKey;
    }

    private void checkDbOpened() {
        E.checkState(this.sessions != null && !this.sessions.closed(),
                     "RocksDB has not been opened");
    }

    protected RocksDBSessions db(HugeType tableType) {
        this.checkDbOpened();

        // Optimized disk
        String disk = this.tableDiskMapping.get(tableType);
        if (disk != null) {
            return this.db(disk);
        }

        return this.sessions;
    }

    private RocksDBSessions db(String disk) {
        RocksDBSessions db = this.dbs.get(disk);
        E.checkState(db != null && !db.closed(),
                     "RocksDB store has not been opened: %s", disk);
        return db;
    }

    private static boolean existsOtherKeyspace(String dataPath) {
        Set<String> cfs;
        try {
            cfs = RocksDBStdSessions.listCFs(dataPath);
        } catch (RocksDBException e) {
            return false;
        }

        int matched = 0;
        for (String cf : cfs) {
            if (cf.endsWith(RocksDBTables.PropertyKey.TABLE) ||
                cf.endsWith(RocksDBTables.VertexLabel.TABLE) ||
                cf.endsWith(RocksDBTables.EdgeLabel.TABLE) ||
                cf.endsWith(RocksDBTables.IndexLabel.TABLE) ||
                cf.endsWith(RocksDBTables.SecondaryIndex.TABLE) ||
                cf.endsWith(RocksDBTables.SearchIndex.TABLE) ||
                cf.endsWith(RocksDBTables.RangeIntIndex.TABLE) ||
                cf.endsWith(RocksDBTables.RangeFloatIndex.TABLE) ||
                cf.endsWith(RocksDBTables.RangeLongIndex.TABLE) ||
                cf.endsWith(RocksDBTables.RangeDoubleIndex.TABLE)) {
                if (++matched >= 3) {
                    return true;
                }
            }
        }
        return false;
    }

    /***************************** Store defines *****************************/

    public static class RocksDBSchemaStore extends RocksDBStore {

        private final RocksDBTables.Counters counters;

        public RocksDBSchemaStore(BackendStoreProvider provider,
                                  String database, String store) {
            super(provider, database, store);

            this.counters = new RocksDBTables.Counters(database);

            registerTableManager(HugeType.VERTEX_LABEL, new RocksDBTables.VertexLabel(database));
            registerTableManager(HugeType.EDGE_LABEL, new RocksDBTables.EdgeLabel(database));
            registerTableManager(HugeType.PROPERTY_KEY, new RocksDBTables.PropertyKey(database));
            registerTableManager(HugeType.INDEX_LABEL, new RocksDBTables.IndexLabel(database));
            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new RocksDBTables.SecondaryIndex(database));
        }

        @Override
        protected List<String> tableNames() {
            List<String> tableNames = super.tableNames();
            tableNames.add(this.counters.table());
            return tableNames;
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            Lock readLock = super.storeLock.readLock();
            readLock.lock();
            try {
                super.checkOpened();
                RocksDBSessions.Session session = super.sessions.session();
                this.counters.increaseCounter(session, type, increment);
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public long getCounter(HugeType type) {
            Lock readLock = super.storeLock.readLock();
            readLock.lock();
            try {
                super.checkOpened();
                RocksDBSessions.Session session = super.sessions.session();
                return this.counters.getCounter(session, type);
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class RocksDBGraphStore extends RocksDBStore {

        public RocksDBGraphStore(BackendStoreProvider provider,
                                 String database, String store) {
            super(provider, database, store);

            registerTableManager(HugeType.VERTEX, new RocksDBTables.Vertex(database));

            registerTableManager(HugeType.EDGE_OUT, RocksDBTables.Edge.out(database));
            registerTableManager(HugeType.EDGE_IN, RocksDBTables.Edge.in(database));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new RocksDBTables.SecondaryIndex(database));
            registerTableManager(HugeType.VERTEX_LABEL_INDEX,
                                 new RocksDBTables.VertexLabelIndex(database));
            registerTableManager(HugeType.EDGE_LABEL_INDEX,
                                 new RocksDBTables.EdgeLabelIndex(database));
            registerTableManager(HugeType.RANGE_INT_INDEX,
                                 new RocksDBTables.RangeIntIndex(database));
            registerTableManager(HugeType.RANGE_FLOAT_INDEX,
                                 new RocksDBTables.RangeFloatIndex(database));
            registerTableManager(HugeType.RANGE_LONG_INDEX,
                                 new RocksDBTables.RangeLongIndex(database));
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX,
                                 new RocksDBTables.RangeDoubleIndex(database));
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new RocksDBTables.SearchIndex(database));
            registerTableManager(HugeType.SHARD_INDEX,
                                 new RocksDBTables.ShardIndex(database));
            registerTableManager(HugeType.UNIQUE_INDEX,
                                 new RocksDBTables.UniqueIndex(database));

            registerTableManager(this.olapTableName(HugeType.SECONDARY_INDEX),
                                 new RocksDBTables.OlapSecondaryIndex(store));
            registerTableManager(this.olapTableName(HugeType.RANGE_INT_INDEX),
                                 new RocksDBTables.OlapRangeIntIndex(store));
            registerTableManager(this.olapTableName(HugeType.RANGE_LONG_INDEX),
                                 new RocksDBTables.OlapRangeLongIndex(store));
            registerTableManager(this.olapTableName(HugeType.RANGE_FLOAT_INDEX),
                                 new RocksDBTables.OlapRangeFloatIndex(store));
            registerTableManager(this.olapTableName(HugeType.RANGE_DOUBLE_INDEX),
                                 new RocksDBTables.OlapRangeDoubleIndex(store));
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException("RocksDBGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException("RocksDBGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException("RocksDBGraphStore.getCounter()");
        }

        /**
         * TODO: can we remove this method since createOlapTable would register?
         */
        @Override
        public void checkAndRegisterOlapTable(Id id) {
            RocksDBTable table = new RocksDBTables.OlapTable(this.store(), id);
            if (!super.sessions.existsTable(table.table())) {
                throw new HugeException("Not exist table '%s''", table.table());
            }
            registerTableManager(this.olapTableName(id), table);
        }

        @Override
        public void createOlapTable(Id id) {
            RocksDBTable table = new RocksDBTables.OlapTable(this.store(), id);
            this.createTable(this.db(HugeType.OLAP), table.table());
            registerTableManager(this.olapTableName(id), table);
        }

        @Override
        public void clearOlapTable(Id id) {
            String name = this.olapTableName(id);
            RocksDBTable table = this.table(name);
            RocksDBSessions db = this.db(HugeType.OLAP);
            if (!db.existsTable(table.table())) {
                throw new HugeException("Not exist table '%s''", name);
            }
            this.dropTable(db, table.table());
            this.createTable(db, table.table());
        }

        @Override
        public void removeOlapTable(Id id) {
            String name = this.olapTableName(id);
            RocksDBTable table = this.table(name);
            RocksDBSessions db = this.db(HugeType.OLAP);
            if (!db.existsTable(table.table())) {
                throw new HugeException("Not exist table '%s''", name);
            }
            this.dropTable(db, table.table());
            this.unregisterTableManager(this.olapTableName(id));
        }
    }

    public static class RocksDBSystemStore extends RocksDBGraphStore {

        private final RocksDBTables.Meta meta;

        public RocksDBSystemStore(BackendStoreProvider provider, String database, String store) {
            super(provider, database, store);
            this.meta = new RocksDBTables.Meta(database);
        }

        @Override
        public synchronized void init() {
            super.init();
            Lock writeLock = this.storeLock().writeLock();
            writeLock.lock();
            try {
                RocksDBSessions.Session session = super.session(HugeType.META);
                String driverVersion = this.provider().driverVersion();
                this.meta.writeVersion(session, driverVersion);
                LOG.info("Write down the backend version: {}", driverVersion);
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public String storedVersion() {
            Lock readLock = this.storeLock().readLock();
            readLock.lock();
            try {
                super.checkOpened();
                RocksDBSessions.Session session = super.session(null);
                return this.meta.readVersion(session);
            } finally {
                readLock.unlock();
            }
        }

        @Override
        protected List<String> tableNames() {
            List<String> tableNames = super.tableNames();
            tableNames.add(this.meta.table());
            return tableNames;
        }
    }
}
