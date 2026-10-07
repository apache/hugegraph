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

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;

public final class RocksDBIteratorPool implements AutoCloseable {

    private static final Logger LOG = Log.logger(RocksDBIteratorPool.class);

    private static final int ITERATOR_POOL_CAPACITY = CoreOptions.CPUS * 2;

    private final Queue<RocksIterator> pool;
    private final RocksDB rocksdb;
    private final ColumnFamilyHandle cfh;
    private final String cfName;

    public RocksDBIteratorPool(RocksDB rocksdb, ColumnFamilyHandle cfh) {
        this.pool = new ArrayBlockingQueue<>(ITERATOR_POOL_CAPACITY);
        this.rocksdb = rocksdb;
        this.cfh = cfh;

        String cfName;
        try {
            cfName = StringEncoding.decode(this.cfh.getName());
        } catch (RocksDBException e) {
            LOG.warn("Can't get column family name", e);
            cfName = "CF-" + cfh.getID();
        }
        this.cfName = cfName;
    }

    public ReusedRocksIterator newIterator() {
        return new ReusedRocksIterator();
    }

    @Override
    public void close() {
        LOG.debug("Close IteratorPool with pool size {} ({})", this.pool.size(), this);
        for (RocksIterator iter; (iter = this.pool.poll()) != null; ) {
            this.closeIterator(iter);
        }
        assert this.pool.isEmpty();
    }

    @Override
    public String toString() {
        return "IteratorPool-" + this.cfName;
    }

    private RocksIterator allocIterator() {
        /*
         * NOTE: Seems there is a bug if share RocksIterator between threads
         * RocksIterator iter = this.pool.poll();
         */
        RocksIterator iter = this.pool.poll();

        if (iter != null) {
            if (this.refreshIterator(iter)) {
                // Must refresh when an iterator is reused
                return iter;
            } else {
                // Close it if can't fresh, and create a new one later
                this.closeIterator(iter);
            }
        }
        /*
         * Create a new iterator if:
         *  - the pool is empty,
         *  - or the iterator obtained from the pool is closed,
         *  - or the iterator can't refresh.
         */
        iter = this.createIterator();
        try {
            iter.status();
            return iter;
        } catch (RocksDBException e) {
            this.closeIterator(iter);
            throw new BackendException(e);
        }
    }

    private void releaseIterator(RocksIterator iter) {
        assert iter.isOwningHandle();
        boolean added = this.pool.offer(iter);
        if (!added) {
            // Really close iterator if the pool is full
            LOG.debug("Really close iterator {} since the pool is full({})",
                      iter, this.pool.size());
            this.closeIterator(iter);
        } else {
            // Not sure whether it needs to refresh
            assert this.refreshIterator(iter);
        }
    }

    private boolean refreshIterator(RocksIterator iter) {
        if (iter.isOwningHandle()) {
            try {
                iter.refresh();
                return true;
            } catch (RocksDBException e) {
                LOG.warn("Can't refresh RocksIterator: {}", e.getMessage(), e);
            }
        }
        return false;
    }

    private RocksIterator createIterator() {
        RocksIterator iter = this.rocksdb.newIterator(this.cfh);
        LOG.debug("Create iterator: {}", iter);
        return iter;
    }

    private void closeIterator(RocksIterator iter) {
        LOG.debug("Really close iterator {}", iter);
        if (iter.isOwningHandle()) {
            iter.close();
        }
    }

    protected final class ReusedRocksIterator {

        private static final boolean REUSING_ENABLED = false;
        private final RocksIterator iterator;
        private boolean closed;

        public ReusedRocksIterator() {
            this.closed = false;
            if (REUSING_ENABLED) {
                this.iterator = allocIterator();
            } else {
                this.iterator = createIterator();
            }
        }

        public RocksIterator iterator() {
            assert !this.closed;
            return this.iterator;
        }

        public void close() {
            if (this.closed) {
                return;
            }
            this.closed = true;

            if (REUSING_ENABLED) {
                releaseIterator(this.iterator);
            } else {
                closeIterator(this.iterator);
            }
        }
    }
}
