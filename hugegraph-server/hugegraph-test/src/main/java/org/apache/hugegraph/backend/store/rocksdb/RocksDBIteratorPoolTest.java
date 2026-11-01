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

import java.nio.charset.StandardCharsets;

import org.apache.hugegraph.backend.store.rocksdb.RocksDBIteratorPool.ReusedRocksIterator;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

public class RocksDBIteratorPoolTest {

    private static final String CF = "iter-pool-cf";

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    private Options options;
    private RocksDB rocksdb;
    private ColumnFamilyHandle cfh;
    private RocksDBIteratorPool pool;

    @Before
    public void setup() throws Exception {
        this.options = new Options().setCreateIfMissing(true);
        this.rocksdb = RocksDB.open(this.options, this.temporary.newFolder().getAbsolutePath());
        this.cfh = this.rocksdb.createColumnFamily(new ColumnFamilyDescriptor(bytes(CF)));
        this.rocksdb.put(this.cfh, bytes("k1"), bytes("v1"));
        this.rocksdb.put(this.cfh, bytes("k2"), bytes("v2"));
        this.pool = new RocksDBIteratorPool(this.rocksdb, this.cfh);
    }

    @After
    public void teardown() {
        // Native objects must be released child-first: pool, CF, DB, options. Any of them may
        // be missing when setup() failed, and closing it must not hide that failure.
        try {
            if (this.pool != null) {
                this.pool.close();
            }
        } finally {
            if (this.cfh != null) {
                this.cfh.close();
            }
            if (this.rocksdb != null) {
                this.rocksdb.close();
            }
            if (this.options != null) {
                this.options.close();
            }
        }
    }

    @Test
    public void testIteratorCloseReleasesNativeHandleAndIsIdempotent() {
        ReusedRocksIterator reused = this.pool.newIterator();
        RocksIterator iter = reused.iterator();
        Assert.assertTrue(iter.isOwningHandle());

        iter.seekToFirst();
        Assert.assertTrue(iter.isValid());
        Assert.assertEquals("k1", string(iter.key()));
        Assert.assertEquals("v1", string(iter.value()));

        reused.close();
        Assert.assertFalse(iter.isOwningHandle());
        // A second close must not double-free the native iterator
        reused.close();
        Assert.assertFalse(iter.isOwningHandle());
    }

    @Test
    public void testIteratorsAreNotSharedBetweenCallers() {
        ReusedRocksIterator first = this.pool.newIterator();
        ReusedRocksIterator second = this.pool.newIterator();
        try {
            Assert.assertNotSame(first.iterator(), second.iterator());

            first.iterator().seekToFirst();
            second.iterator().seekToLast();
            Assert.assertEquals("k1", string(first.iterator().key()));
            Assert.assertEquals("k2", string(second.iterator().key()));

            // Closing one caller's iterator leaves the other usable
            first.close();
            second.iterator().prev();
            Assert.assertTrue(second.iterator().isValid());
            Assert.assertEquals("k1", string(second.iterator().key()));
        } finally {
            first.close();
            second.close();
        }
    }

    @Test
    public void testPoolCloseDoesNotInvalidateOutstandingIterators() throws Exception {
        Assert.assertEquals("IteratorPool-" + CF, this.pool.toString());

        ReusedRocksIterator reused = this.pool.newIterator();
        try {
            // Outstanding iterators are owned by their callers, not the pool
            this.pool.close();
            this.pool.close();
            reused.iterator().seek(bytes("k2"));
            Assert.assertTrue(reused.iterator().isValid());
            Assert.assertEquals("v2", string(reused.iterator().value()));

            // The pool can still hand out new iterators after close()
            ReusedRocksIterator another = this.pool.newIterator();
            another.iterator().seekToFirst();
            Assert.assertEquals("k1", string(another.iterator().key()));
            another.close();
        } finally {
            reused.close();
        }
    }

    private static byte[] bytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    private static String string(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
