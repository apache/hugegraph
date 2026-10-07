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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBIteratorPool.ReusedRocksIterator;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.SstFileManager;
import org.slf4j.Logger;

public class OpenedRocksDB implements AutoCloseable {

    private static final Logger LOG = Log.logger(OpenedRocksDB.class);

    private final RocksDB rocksdb;
    private final Map<String, CFHandle> cfHandles;
    private final SstFileManager sstFileManager;

    public OpenedRocksDB(RocksDB rocksdb, Map<String, CFHandle> cfHandles,
                         SstFileManager sstFileManager) {
        this.rocksdb = rocksdb;
        this.cfHandles = cfHandles;
        this.sstFileManager = sstFileManager;
    }

    protected final RocksDB rocksdb() {
        return this.rocksdb;
    }

    public Set<String> cfs() {
        return this.cfHandles.keySet();
    }

    public CFHandle cf(String cfName) {
        return this.cfHandles.get(cfName);
    }

    public void addCf(String cfName, CFHandle cfHandle) {
        this.cfHandles.put(cfName, cfHandle);
    }

    public CFHandle removeCf(String cfName) {
        return this.cfHandles.remove(cfName);
    }

    public boolean existCf(String cfName) {
        return this.cfHandles.containsKey(cfName);
    }

    public boolean isOwningHandle() {
        return this.rocksdb.isOwningHandle();
    }

    @Override
    public void close() {
        if (!this.isOwningHandle()) {
            return;
        }
        for (CFHandle cf : this.cfHandles.values()) {
            cf.close();
        }
        this.cfHandles.clear();

        this.rocksdb.close();
    }

    public long totalSize() {
        return this.sstFileManager.getTotalSize();
    }

    public void createCheckpoint(String targetPath) {
        Path parentName = Paths.get(targetPath).toAbsolutePath().getParent().getFileName();
        assert parentName.toString().startsWith("snapshot") : targetPath;
        // https://github.com/facebook/rocksdb/wiki/Checkpoints
        try (Checkpoint checkpoint = Checkpoint.create(this.rocksdb)) {
            String tempPath = targetPath + "_temp";
            File tempFile = new File(tempPath);
            FileUtils.deleteDirectory(tempFile);
            LOG.debug("Deleted temp directory {}", tempFile);

            FileUtils.forceMkdir(tempFile.getParentFile());
            checkpoint.createCheckpoint(tempPath);
            File snapshotFile = new File(targetPath);
            FileUtils.deleteDirectory(snapshotFile);
            LOG.debug("Deleted stale directory {}", snapshotFile);
            if (!tempFile.renameTo(snapshotFile)) {
                throw new IOException(String.format("Failed to rename %s to %s",
                                                    tempFile, snapshotFile));
            }
        } catch (Exception e) {
            throw new BackendException("Failed to create checkpoint at path %s", e, targetPath);
        }
    }

    protected static final class CFHandle implements AutoCloseable {

        private final ColumnFamilyHandle handle;
        private final AtomicInteger refs;
        private final RocksDBIteratorPool iterPool;

        public CFHandle(RocksDB rocksdb, ColumnFamilyHandle handle) {
            E.checkNotNull(handle, "handle");
            this.handle = handle;
            this.refs = new AtomicInteger(1);
            this.iterPool = new RocksDBIteratorPool(rocksdb, this.handle);
        }

        public synchronized ColumnFamilyHandle get() {
            E.checkState(this.handle.isOwningHandle(), "It seems CF has been closed");
            assert this.refs.get() >= 1;
            return this.handle;
        }

        public synchronized ReusedRocksIterator newIterator() {
            assert this.handle.isOwningHandle();
            assert this.refs.get() >= 1;
            return this.iterPool.newIterator();
        }

        public synchronized void open() {
            this.refs.incrementAndGet();
        }

        @Override
        public void close() {
            if (this.refs.decrementAndGet() <= 0) {
                this.iterPool.close();
                this.handle.close();
            }
        }

        public synchronized ColumnFamilyHandle waitForDrop() {
            assert this.refs.get() >= 1;
            // When entering this method, the refs won't increase anymore
            final long timeout = TimeUnit.MINUTES.toMillis(30L);
            final long unit = 100L;
            for (long i = 1; this.refs.get() > 1; i++) {
                try {
                    Thread.sleep(unit);
                } catch (InterruptedException ignored) {
                    // 30s rest api timeout may cause InterruptedException
                }
                if (i * unit > timeout) {
                    throw new BackendException("Timeout after %sms to drop CF", timeout);
                }
            }
            assert this.refs.get() == 1;
            return this.handle;
        }

        public synchronized void destroy() {
            this.close();
            assert this.refs.get() == 0 && !this.handle.isOwningHandle();
        }
    }
}
