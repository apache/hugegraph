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
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

import com.alipay.sofa.jraft.util.Utils;
import com.google.common.cache.CacheBuilder;
import com.google.common.primitives.Bytes;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HgKVStoreImpl implements HgKVStore {

    private static final ConcurrentHashMap<String,
            ConcurrentMap<String, Object>> CACHE = new ConcurrentHashMap();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private RocksDB db;
    private String dbPath;
    private Options dbOptions;

    @Override
    public void init(PDConfig config) {
        dbOptions = new Options().setCreateIfMissing(true);

        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            this.dbPath = config.getDataPath() + "/rocksdb";
            File file = new File(this.dbPath);
            if (!file.exists()) {
                try {
                    FileUtils.forceMkdir(file);
                } catch (IOException e) {
                    log.warn("Failed to create data file,{}", e);
                }
            }
            openRocksDB(dbPath);
        } catch (PDException e) {
            log.error("Failed to open data file,{}", e);
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
        try (ReadOptions options = new ReadOptions()
                .setIterateLowerBound(new Slice(prefix))) {
            List<KV> kvs = new ArrayList<>();
            RocksIterator iterator = db.newIterator(options);
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
        try (ReadOptions options = new ReadOptions()
                .setIterateLowerBound(new Slice(prefix))) {
            RocksIterator iterator = db.newIterator(options);
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
        try (ReadOptions options = new ReadOptions()
                .setIterateLowerBound(new Slice(start))
                .setIterateUpperBound(new Slice(end))) {
            List<KV> kvs = new ArrayList<>();
            RocksIterator iterator = db.newIterator(options);
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
    public void close() {
        closeRocksDB();
    }

    private void closeRocksDB() {
        if (this.db != null) {
            this.db.close();
            this.db = null;
        }
    }

    private void openRocksDB(String dbPath) throws PDException {
        try {
            this.db = RocksDB.open(dbOptions, dbPath);
        } catch (RocksDBException e) {
            log.error("Failed to open RocksDB from {}", dbPath, e);
            throw new PDException(Pdpb.ErrorType.ROCKSDB_LOAD_SNAPSHOT_ERROR_VALUE, e);
        }
    }
}
