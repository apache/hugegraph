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

package org.apache.hugegraph.store.rocksdb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hugegraph.rocksdb.access.DBStoreException;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.apache.hugegraph.rocksdb.access.SessionOperatorImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

public class SessionOperatorTest extends BaseRocksDbTest {

    private static final String TABLE = "tbl";
    private static final String GRAPH = "op-graph";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    // Runs @Before, the test and @After on one thread, so a stuck CF lock fails the test
    @Rule
    public Timeout timeout = Timeout.seconds(60);

    private ExecutorService prober;
    private String dataPath;
    private RocksDBSession session;
    private int probes;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static List<String> keys(ScanIterator it) {
        List<String> keys = new ArrayList<>();
        try {
            while (it.hasNext()) {
                BackendColumn col = it.next();
                keys.add(new String(col.name, StandardCharsets.UTF_8));
            }
        } finally {
            it.close();
        }
        return keys;
    }

    @Before
    public void setUp() throws Exception {
        this.prober = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "cf-lock-prober");
            t.setDaemon(true);
            return t;
        });
        this.dataPath = this.folder.newFolder("data").getAbsolutePath();
        this.session = new RocksDBSession(hConfig, this.dataPath, GRAPH, 0);
        this.session.checkTable(TABLE);
    }

    @After
    public void tearDown() {
        try {
            if (this.session != null) {
                releaseCfReadLockHolds();
                this.session.close();
            }
        } finally {
            this.prober.shutdownNow();
        }
    }

    /**
     * A test that fails between prepare() and commit() still holds the CF read lock on this
     * thread, and close() would then wait forever for the CF write lock.
     */
    private void releaseCfReadLockHolds() {
        ReentrantReadWriteLock.ReadLock readLock = this.session.getCfHandleReadLock();
        while (true) {
            try {
                readLock.unlock();
            } catch (IllegalMonitorStateException e) {
                return;
            }
        }
    }

    private void putAll(String... keys) {
        SessionOperator op = this.session.sessionOp();
        op.prepare();
        for (String key : keys) {
            op.put(TABLE, b(key), b("v-" + key));
        }
        op.commit();
    }

    private List<String> allKeys() {
        return keys(this.session.sessionOp().scan(TABLE));
    }

    /**
     * Creating a column family takes the CF write lock, so another thread can only do it when
     * the caller does not leak the read lock taken by prepare().
     */
    private void assertCfLockReleased() throws Exception {
        String probe = "probe-" + this.probes++;
        Future<?> future = this.prober.submit(() -> this.session.checkTable(probe));
        future.get(10, TimeUnit.SECONDS);
        assertTrue(this.session.tableIsExist(probe));
    }

    @Test
    public void testIncreaseOneCarriesAndRejectsOverflow() {
        assertArrayEquals(new byte[]{1, 3}, SessionOperatorImpl.increaseOne(new byte[]{1, 2}));
        assertArrayEquals(new byte[]{2, 0, 0},
                          SessionOperatorImpl.increaseOne(new byte[]{1, (byte) 0xff, (byte) 0xff}));
        assertThrows(DBStoreException.class,
                     () -> SessionOperatorImpl.increaseOne(new byte[]{(byte) 0xff, (byte) 0xff}));
    }

    @Test
    public void testCommitAppliesTheBatchAndReturnsItsSize() throws Exception {
        SessionOperator op = this.session.sessionOp();
        op.prepare();
        op.put(TABLE, b("k1"), b("v1"));
        op.put(TABLE, b("k2"), b("v2"));
        op.delete(TABLE, b("k1"));
        assertNull("batched writes are invisible before commit", op.get(TABLE, b("k2")));
        assertEquals(Integer.valueOf(3), op.commit());

        assertNull(op.get(TABLE, b("k1")));
        assertArrayEquals(b("v2"), op.get(TABLE, b("k2")));

        op.prepare();
        assertEquals(Integer.valueOf(0), op.commit());
        assertCfLockReleased();
    }

    @Test
    public void testRollbackDiscardsPendingWritesAndReleasesTheLock() throws Exception {
        putAll("keep");
        SessionOperator op = this.session.sessionOp();
        op.prepare();
        op.put(TABLE, b("lost"), b("v"));
        op.delete(TABLE, b("keep"));
        op.rollback();

        assertNull(op.get(TABLE, b("lost")));
        assertArrayEquals(b("v-keep"), op.get(TABLE, b("keep")));
        assertCfLockReleased();

        // The operator stays usable after a rollback
        op.prepare();
        op.put(TABLE, b("after"), b("v"));
        assertEquals(Integer.valueOf(1), op.commit());
        assertArrayEquals(b("v"), op.get(TABLE, b("after")));
    }

    @Test
    public void testDeletePrefixRemovesOnlyPrefixedKeys() throws Exception {
        putAll("a", "ab", "ab0", "ab~", "ac", "b");
        SessionOperator op = this.session.sessionOp();
        op.prepare();
        op.deletePrefix(TABLE, b("ab"));
        op.commit();

        assertEquals(List.of("a", "ac", "b"), allKeys());

        byte[] unbounded = new byte[]{(byte) 0xff, (byte) 0xff};
        op.prepare();
        try {
            assertThrows(DBStoreException.class, () -> op.deletePrefix(TABLE, unbounded));
        } finally {
            op.rollback();
        }
        assertCfLockReleased();
    }

    @Test
    public void testDeleteRangeIsHalfOpenAndValidatesBounds() throws Exception {
        putAll("k1", "k2", "k3", "k4", "k5");
        SessionOperator op = this.session.sessionOp();

        op.deleteRange(TABLE, b("k2"), b("k4"));
        assertEquals(List.of("k1", "k4", "k5"), allKeys());

        DBStoreException inverted = assertThrows(DBStoreException.class,
                                                 () -> op.deleteRange(TABLE, b("k5"), b("k1")));
        assertTrue(inverted.getMessage(), inverted.getMessage().contains("is lower than"));
        assertThrows(IllegalArgumentException.class, () -> op.deleteRange(TABLE, null, b("k5")));
        assertThrows(IllegalArgumentException.class, () -> op.deleteRange(TABLE, b("k1"), null));

        assertEquals("rejected ranges must not delete anything", List.of("k1", "k4", "k5"),
                     allKeys());
        assertCfLockReleased();
    }

    @Test
    public void testScanIteratorHoldsASessionReferenceUntilClosed() {
        putAll("p1", "p2", "q1");
        assertEquals(1, this.session.getRefCount());

        ScanIterator it = this.session.sessionOp().scan(TABLE, b("p"));
        assertEquals(2, this.session.getRefCount());
        assertEquals(1, this.session.getIteratorMap().size());
        assertTrue(it.hasNext());
        it.close();
        it.close();
        assertEquals(1, this.session.getRefCount());
        assertTrue(this.session.getIteratorMap().isEmpty());

        // Exhausting an iterator releases it without an explicit close
        assertEquals(2L, this.session.sessionOp().keyCount(b("p"), b("q"), TABLE));
        assertEquals(1, this.session.getRefCount());
        assertTrue(this.session.getIteratorMap().isEmpty());
    }

    @Test
    public void testPrefixAndRangeScansHonourTheirBounds() {
        putAll("p1", "p2", "pz", "q1", "r1");
        SessionOperator op = this.session.sessionOp();

        assertEquals(List.of("p1", "p2", "pz"), keys(op.scan(TABLE, b("p"))));
        assertEquals(List.of("p2", "pz", "q1"),
                     keys(op.scan(TABLE, b("p2"), b("r1"), ScanIterator.Trait.SCAN_LT_END)));
        assertEquals(List.of("p2", "pz", "q1", "r1"),
                     keys(op.scan(TABLE, b("p2"), b("r1"), ScanIterator.Trait.SCAN_LTE_END)));
        assertTrue(keys(op.scan(TABLE, b("x"))).isEmpty());
        assertEquals(1, this.session.getRefCount());
    }

    @Test
    public void testSnapshotRoundTripAndMissingSnapshot() throws Exception {
        putAll("s1", "s2");
        String snapshot = new File(this.folder.getRoot(), "snapshot").getAbsolutePath();
        this.session.saveSnapshot(snapshot);
        assertTrue(new File(snapshot, "CURRENT").exists());
        assertFalse("temporary checkpoint dir must be renamed away",
                    new File(snapshot + "_temp").exists());

        DBStoreException missing = assertThrows(DBStoreException.class, () ->
                this.session.loadSnapshot(snapshot + "-missing", 7));
        assertTrue(missing.getMessage(), missing.getMessage().contains("not exists"));
        // The CF write lock taken by loadSnapshot must be released on failure
        putAll("s3");
        assertCfLockReleased();

        this.session.loadSnapshot(snapshot, 7);
        assertTrue(new File(this.dataPath, GRAPH + "_7").isDirectory());
        this.session.close();
        this.session = null;

        // Reopening picks the newest versioned path, i.e. the loaded snapshot without "s3"
        this.session = new RocksDBSession(hConfig, this.dataPath, GRAPH, 7);
        assertTrue(this.session.getDbPath().endsWith(GRAPH + "_7"));
        assertEquals(List.of("s1", "s2"), allKeys());
    }
}
