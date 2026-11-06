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

package org.apache.hugegraph.pd.store;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.discovery.RegisterInfo;
import org.apache.hugegraph.pd.grpc.kv.TxnRequest;
import org.apache.hugegraph.pd.grpc.kv.TxnResponse;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Status;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.alipay.sofa.jraft.util.Utils;
import com.google.common.cache.CacheBuilder;
import com.google.common.primitives.Bytes;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HgKVStoreImpl implements HgKVStore {

    private static final ConcurrentHashMap<String,
            ConcurrentMap<String, Object>> CACHE = new ConcurrentHashMap();
    /*
     * A restarted PD can start before the previous JVM has released the RocksDB LOCK.
     * Wait about as long as stop-hugegraph-pd.sh waits for that JVM, then fail startup.
     * Every attempt rotates the RocksDB info LOG, so back off between attempts. Only the
     * metadata store waits: RaftStartup opens it before the raft log store, and a stopping
     * PD releases the raft log LOCK before this one.
     */
    private static final long LOCK_WAIT_TIMEOUT_MS = 30_000L;
    private static final long LOCK_RETRY_MIN_INTERVAL_MS = 500L;
    private static final long LOCK_RETRY_MAX_INTERVAL_MS = 4_000L;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private RocksDB db;
    private String dbPath;
    private Options dbOptions;
    // Tests shorten this
    private long lockWaitTimeoutMs = LOCK_WAIT_TIMEOUT_MS;

    @Override
    public void init(PDConfig config) {
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            this.dbOptions = new Options().setCreateIfMissing(true);
            this.dbPath = config.getDataPath() + "/rocksdb";
            File file = new File(this.dbPath);
            if (!file.exists()) {
                try {
                    FileUtils.forceMkdir(file);
                } catch (IOException e) {
                    log.warn("Failed to create data file,{}", e);
                }
            }
            openRocksDB(this.dbPath, this.lockWaitTimeoutMs);
        } catch (PDException | RuntimeException | Error e) {
            if (this.dbOptions != null) {
                this.dbOptions.close();
                this.dbOptions = null;
            }
            throw new IllegalStateException("Failed to open PD metadata at " + this.dbPath, e);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void put(byte[] key, byte[] value) throws PDException {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_WRITE_ERROR_VALUE, e);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public byte[] get(byte[] key) throws PDException {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE, e);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<KV> scanPrefix(byte[] prefix) {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try (Slice bound = new Slice(prefix);
             ReadOptions options = new ReadOptions().setIterateLowerBound(bound);
             RocksIterator iterator = db.newIterator(options)) {
            List<KV> kvs = new ArrayList<>();
            iterator.seekToFirst();
            while (iterator.isValid() && 0 == Bytes.indexOf(iterator.key(), prefix)) {
                kvs.add(new KV(iterator.key(), iterator.value()));
                iterator.next();
            }
            return kvs;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long remove(byte[] key) throws PDException {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_DEL_ERROR_VALUE, e);
        } finally {
            readLock.unlock();
        }
        return 0;
    }

    @Override
    public long removeByPrefix(byte[] prefix) throws PDException {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try (Slice bound = new Slice(prefix);
             ReadOptions options = new ReadOptions().setIterateLowerBound(bound);
             RocksIterator iterator = db.newIterator(options)) {
            iterator.seekToFirst();

            while (iterator.isValid()) {
                if (0 == Bytes.indexOf(iterator.key(), prefix)) {
                    db.delete(iterator.key());
                } else {
                    break;
                }
                iterator.next();
            }
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_WRITE_ERROR_VALUE, e);
        } finally {
            readLock.unlock();
        }
        return 0;
    }

    @Override
    public void clear() throws PDException {
        CACHE.clear();
    }

    @Override
    public List getListWithTTL(byte[] key) throws PDException {
        String storeKey = new String(key, Charset.defaultCharset());
        LinkedList<byte[]> ts = new LinkedList<>();
        CACHE.keySet().forEach((cacheKey) -> {
            if (cacheKey.startsWith(storeKey)) {
                ConcurrentMap map;
                if ((map = CACHE.get(cacheKey)) == null) {
                    return;
                }
                map.values().forEach((element) -> {
                    ts.add((byte[]) element);
                });
            }
        });
        return ts;
    }

    @Override
    public byte[] getWithTTL(byte[] key) throws PDException {
        ConcurrentMap map;
        String storeKey = new String(key, Charset.defaultCharset());
        if ((map = CACHE.get(storeKey)) == null) {
            return null;
        }
        Object value = map.get(storeKey);
        return value == null ? null : (byte[]) value;
    }

    @Override
    public void removeWithTTL(byte[] key) throws PDException {
        ConcurrentMap map;
        String storeKey = new String(key, Charset.defaultCharset());
        if ((map = CACHE.get(storeKey)) == null) {
            return;
        }
        map.remove(storeKey);
    }

    @Override
    public void putWithTTL(byte[] key, byte[] value, long ttl) throws PDException {
        this.putWithTTL(key, value, ttl, TimeUnit.SECONDS);
    }

    @Override
    public void putWithTTL(byte[] key, byte[] value, long ttl, TimeUnit timeUnit) throws
                                                                                  PDException {
        try {
            ConcurrentMap spaceNode = CacheBuilder.newBuilder().initialCapacity(200)
                                                  .expireAfterWrite(ttl,
                                                                    timeUnit)
                                                  .<String, RegisterInfo>build().asMap();
            String storeKey = new String(key, Charset.defaultCharset());
            ConcurrentMap space = CACHE.putIfAbsent(storeKey, spaceNode);
            if (space == null) {
                space = spaceNode;
            }
            space.put(storeKey, value);
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_WRITE_ERROR_VALUE, e);
        }
    }

    @Override
    public void saveSnapshot(String snapshotPath) throws PDException {
        log.info("begin save snapshot at {}", snapshotPath);
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try (final Checkpoint checkpoint = Checkpoint.create(this.db)) {
            final String tempPath = Paths.get(snapshotPath) + "_temp";
            final File tempFile = new File(tempPath);
            FileUtils.deleteDirectory(tempFile);
            checkpoint.createCheckpoint(tempPath);
            final File snapshotFile = new File(snapshotPath);
            FileUtils.deleteDirectory(snapshotFile);
            if (!Utils.atomicMoveFile(tempFile, snapshotFile, true)) {
                log.error("Fail to rename {} to {}", tempPath, snapshotPath);
                throw new PDException(Pdpb.ErrorType.ROCKSDB_SAVE_SNAPSHOT_ERROR_VALUE,
                                      String.format("Fail to rename %s to %s", tempPath,
                                                    snapshotPath));
            }
        } catch (final PDException e) {
            throw e;
        } catch (final Exception e) {
            log.error("Fail to write snapshot at path: {}", snapshotPath, e);
            throw new PDException(Pdpb.ErrorType.ROCKSDB_SAVE_SNAPSHOT_ERROR_VALUE, e);
        } finally {
            writeLock.unlock();
        }
        log.info("saved snapshot into {}", snapshotPath);
    }

    @Override
    public void loadSnapshot(String snapshotPath) throws PDException {
        log.info("begin load snapshot from {}", snapshotPath);
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            final File snapshotFile = new File(snapshotPath);
            if (!snapshotFile.exists()) {
                log.error("Snapshot file {} not exists.", snapshotPath);
                return;
            }
            // close DB
            closeRocksDB();
            // replace rocksdb data with snapshot data
            final File dbFile = new File(this.dbPath);
            FileUtils.deleteDirectory(dbFile);
            if (!Utils.atomicMoveFile(snapshotFile, dbFile, true)) {
                log.error("Fail to rename {} to {}", snapshotPath, this.dbPath);
                throw new PDException(Pdpb.ErrorType.ROCKSDB_LOAD_SNAPSHOT_ERROR_VALUE,
                                      String.format("Fail to rename %s to %s", snapshotPath,
                                                    this.dbPath));
            }
            // reopen the db
            openRocksDB(this.dbPath);
        } catch (final PDException e) {
            throw e;
        } catch (final Exception e) {
            log.error("failed to load snapshot from {}", snapshotPath);
            throw new PDException(Pdpb.ErrorType.ROCKSDB_LOAD_SNAPSHOT_ERROR_VALUE, e);
        } finally {
            writeLock.unlock();
        }
        log.info("loaded snapshot from {}", snapshotPath);
    }

    @Override
    public List<KV> scanRange(byte[] start, byte[] end) {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try (Slice lower = new Slice(start);
             Slice upper = new Slice(end);
             ReadOptions options = new ReadOptions().setIterateLowerBound(lower).setIterateUpperBound(upper);
             RocksIterator iterator = db.newIterator(options)) {
            List<KV> kvs = new ArrayList<>();
            iterator.seekToFirst();
            while (iterator.isValid()) {
                kvs.add(new KV(iterator.key(), iterator.value()));
                iterator.next();
            }
            return kvs;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void writeBatch(List<KV> kvs) throws PDException {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try (WriteBatch batch = new WriteBatch();
             WriteOptions options = new WriteOptions()) {
            for (KV kv : kvs) {
                if (kv.getValue() == null) {
                    batch.delete(kv.getKey());
                } else {
                    batch.put(kv.getKey(), kv.getValue());
                }
            }
            db.write(options, batch);
        } catch (RocksDBException e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_WRITE_ERROR_VALUE, e);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * The path without raft (raft.enable=false): there is no log index, so a TXN takes the
     * next count under its own key, never the raft index key, so that this store run with
     * raft later does not skip live entries below that count. The write lock keeps other
     * writes out from between the compares and the batch.
     */
    @Override
    public TxnResponse txn(TxnRequest request) throws PDException {
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            byte[] indexKey = KvTxnApplier.LOCAL_INDEX_KEY;
            return KvTxnApplier.apply(this, KvTxnApplier.entry(request),
                                      KvTxnApplier.lastIndex(this, indexKey) + 1, indexKey,
                                      KvTxnApplier.listener());
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() {
        Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            closeRocksDB();
            if (this.dbOptions != null) {
                this.dbOptions.close();
                this.dbOptions = null;
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void closeRocksDB() {
        if (this.db != null) {
            this.db.close();
            this.db = null;
        }
    }

    private void openRocksDB(String dbPath) throws PDException {
        openRocksDB(dbPath, 0L);
    }

    private void openRocksDB(String dbPath, long lockWaitTimeoutMs) throws PDException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(lockWaitTimeoutMs);
        long interval = LOCK_RETRY_MIN_INTERVAL_MS;
        boolean waited = false;
        while (true) {
            try {
                this.db = RocksDB.open(this.dbOptions, dbPath);
                if (waited) {
                    log.info("Opened RocksDB at {} after its LOCK was released", dbPath);
                }
                return;
            } catch (RocksDBException e) {
                if (!isLockConflict(e) || System.nanoTime() - deadline >= 0) {
                    log.error("Failed to open RocksDB from {}", dbPath, e);
                    throw new PDException(Pdpb.ErrorType.ROCKSDB_LOAD_SNAPSHOT_ERROR_VALUE, e);
                }
                if (!waited) {
                    log.warn("RocksDB LOCK at {} is held, probably by a previous PD that is " +
                             "still stopping; retrying for up to {} ms: {}",
                             dbPath, lockWaitTimeoutMs, e.getMessage());
                    waited = true;
                }
                try {
                    long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                    Thread.sleep(Math.max(1L, Math.min(interval, remainingMs)));
                    interval = Math.min(interval * 2, LOCK_RETRY_MAX_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    e.addSuppressed(ie);
                    throw new PDException(Pdpb.ErrorType.ROCKSDB_LOAD_SNAPSHOT_ERROR_VALUE, e);
                }
            }
        }
    }

    /**
     * The LOCK is held by another process ("While lock file") or by another handle in this
     * JVM ("lock hold by current process"). "While lock file" also covers a file system that
     * cannot lock at all; that fails once the wait runs out. Other open failures, including a
     * LOCK file that cannot be created, are not retried.
     */
    private static boolean isLockConflict(RocksDBException e) {
        Status status = e.getStatus();
        String message = e.getMessage();
        return status != null && status.getCode() == Status.Code.IOError && message != null &&
               (message.contains("While lock file") ||
                message.contains("lock hold by current process"));
    }
}
