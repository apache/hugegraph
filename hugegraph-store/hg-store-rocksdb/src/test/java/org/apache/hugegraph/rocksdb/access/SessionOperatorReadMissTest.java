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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.RocksdbChangedListener;
import org.apache.hugegraph.rocksdb.access.RocksDBSession.CFHandleLock;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * {@link SessionOperatorImpl#get(String, byte[])} read-miss / cloud-hydration behaviour.
 *
 * <p>The {@code get} path releases the column-family handle lock BEFORE invoking the
 * {@link RocksdbChangedListener#onReadMiss} hook (hydration may perform slow S3 I/O), then
 * re-acquires the lock for a single retry read. These tests pin that contract:
 *
 * <ul>
 *   <li>A hit returns the value without ever consulting a read-miss listener.</li>
 *   <li>A miss with no listener (or a listener that reports {@code false}) returns {@code null}
 *       without a retry read.</li>
 *   <li>A miss whose listener hydrates the key and reports {@code true} triggers the retry read
 *       and returns the hydrated value.</li>
 *   <li>A miss whose listener reports {@code true} but hydrates nothing still returns {@code null}
 *       from the retry read.</li>
 * </ul>
 */
public class SessionOperatorReadMissTest {

    private static final String TABLE = "t";

    @BeforeClass
    public static void init() {
        OptionSpace.register("rocksdb",
                             "org.apache.hugegraph.rocksdb.access.RocksDBOptions");
        RocksDBOptions.instance();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("rocksdb.write_buffer_size", "1048576");
        configMap.put("rocksdb.bloom_filter_bits_per_key", "10");
        RocksDBFactory.getInstance().setHugeConfig(new HugeConfig(new MapConfiguration(configMap)));
    }

    @Test
    public void testGetHitDoesNotConsultReadMissListener() throws Exception {
        AtomicInteger readMissCalls = new AtomicInteger(0);
        RocksdbChangedListener counter = new RocksdbChangedListener() {
            @Override
            public boolean onReadMiss(RocksDBSession session, String table, byte[] key) {
                readMissCalls.incrementAndGet();
                return false;
            }
        };
        withSession("hit", counter, session -> {
            SessionOperator op = session.sessionOp();
            op.prepare();
            op.put(TABLE, bytes("k"), bytes("v"));
            op.commit();

            byte[] value = session.sessionOp().get(TABLE, bytes("k"));

            assertArrayEquals(bytes("v"), value);
            assertEquals("read-miss hook must not fire on a hit", 0, readMissCalls.get());
        });
    }

    @Test
    public void testGetMissWithoutHydrationReturnsNull() throws Exception {
        AtomicInteger readMissCalls = new AtomicInteger(0);
        RocksdbChangedListener noHydration = new RocksdbChangedListener() {
            @Override
            public boolean onReadMiss(RocksDBSession session, String table, byte[] key) {
                readMissCalls.incrementAndGet();
                return false;
            }
        };
        withSession("miss-nohydrate", noHydration, session -> {
            session.checkTable(TABLE);

            byte[] value = session.sessionOp().get(TABLE, bytes("absent"));

            assertNull("a miss that is not hydrated must return null", value);
            assertEquals("read-miss hook must fire exactly once on a miss",
                         1, readMissCalls.get());
        });
    }

    @Test
    public void testGetMissTriggersHydrationThenReturnsHydratedValue() throws Exception {
        RocksdbChangedListener hydrator = new RocksdbChangedListener() {
            @Override
            public boolean onReadMiss(RocksDBSession session, String table, byte[] key) {
                // Simulate cloud hydration: the CF handle lock has already been released, so the
                // listener may safely re-enter the session to write the missing key.
                SessionOperator op = session.sessionOp();
                op.prepare();
                op.put(table, key, bytes("hydrated"));
                op.commit();
                return true;
            }
        };
        withSession("miss-hydrate", hydrator, session -> {
            session.checkTable(TABLE);

            byte[] value = session.sessionOp().get(TABLE, bytes("cold"));

            assertArrayEquals("retry read after hydration must observe the hydrated value",
                              bytes("hydrated"), value);
        });
    }

    @Test
    public void testGetMissHydrationReportedButNoDataReturnsNull() throws Exception {
        RocksdbChangedListener liar = new RocksdbChangedListener() {
            @Override
            public boolean onReadMiss(RocksDBSession session, String table, byte[] key) {
                // Reports success but writes nothing — the retry read must still return null.
                return true;
            }
        };
        withSession("miss-emptyhydrate", liar, session -> {
            session.checkTable(TABLE);

            byte[] value = session.sessionOp().get(TABLE, bytes("cold"));

            assertNull("retry read must return null when hydration produced no data", value);
        });
    }

    // =========================================================================
    // Read-miss hydration on the EXCEPTION path
    //
    // A live SST referenced by the manifest that is physically missing surfaces as a
    // RocksDBException (not a null), so hydration must be attempted on the exception too and the
    // read retried; if nothing is restored, the original error must propagate as DBStoreException.
    // A real embedded DB cannot deterministically raise-then-recover an IO error, so the DB and
    // session are mocked here to drive the branch precisely.
    // =========================================================================

    @Test
    public void firstGetThrows_thenHydrationSucceeds_retryReturnsRestoredValue() throws Exception {
        RocksDB db = mock(RocksDB.class);
        RocksDBSession session = mock(RocksDBSession.class);
        ColumnFamilyHandle handle = mock(ColumnFamilyHandle.class);
        CFHandleLock cfLock = mock(CFHandleLock.class);
        when(cfLock.get()).thenReturn(handle);
        when(session.getDB()).thenReturn(db);
        when(session.getCFHandleLock(TABLE)).thenReturn(cfLock);
        when(session.getGraphName()).thenReturn("db-mock");
        // First read throws (missing live SST → IO error); after hydration the retry returns it.
        when(db.get(any(ColumnFamilyHandle.class), any(byte[].class)))
                .thenThrow(new RocksDBException("simulated missing SST"))
                .thenReturn(bytes("hydrated"));

        AtomicInteger hydrations = new AtomicInteger();
        RocksdbChangedListener hydrator = new RocksdbChangedListener() {
            @Override
            public boolean onReadMiss(RocksDBSession s, String table, byte[] key) {
                hydrations.incrementAndGet();
                return true; // simulate a successful cloud restore of the missing SST
            }
        };
        RocksDBFactory.getInstance().addRocksdbChangedListener(hydrator);
        try {
            SessionOperator op = new SessionOperatorImpl(session);
            byte[] value = op.get(TABLE, bytes("cold"));

            assertArrayEquals("retry after hydration must return the restored value",
                              bytes("hydrated"), value);
            assertEquals("hydration must be attempted exactly once", 1, hydrations.get());
            verify(db, times(2)).get(any(ColumnFamilyHandle.class), any(byte[].class));
        } finally {
            RocksDBFactory.getInstance().removeRocksdbChangedListener(hydrator);
        }
    }

    @Test
    public void firstGetThrows_hydrationSucceeds_butRetryAlsoThrows_propagatesRetryError()
            throws Exception {
        RocksDB db = mock(RocksDB.class);
        RocksDBSession session = mock(RocksDBSession.class);
        ColumnFamilyHandle handle = mock(ColumnFamilyHandle.class);
        CFHandleLock cfLock = mock(CFHandleLock.class);
        when(cfLock.get()).thenReturn(handle);
        when(session.getDB()).thenReturn(db);
        when(session.getCFHandleLock(TABLE)).thenReturn(cfLock);
        when(session.getGraphName()).thenReturn("db-mock");
        // Both the initial read and the post-hydration retry throw (the restore did not repair it).
        when(db.get(any(ColumnFamilyHandle.class), any(byte[].class)))
                .thenThrow(new RocksDBException("initial missing SST"))
                .thenThrow(new RocksDBException("still broken after restore"));

        RocksdbChangedListener hydrator = new RocksdbChangedListener() {
            @Override
            public boolean onReadMiss(RocksDBSession s, String table, byte[] key) {
                return true;
            }
        };
        RocksDBFactory.getInstance().addRocksdbChangedListener(hydrator);
        try {
            SessionOperator op = new SessionOperatorImpl(session);
            try {
                op.get(TABLE, bytes("cold"));
                fail("expected DBStoreException when the post-hydration retry also fails");
            } catch (DBStoreException expected) {
                // expected
            }
            verify(db, times(2)).get(any(ColumnFamilyHandle.class), any(byte[].class));
        } finally {
            RocksDBFactory.getInstance().removeRocksdbChangedListener(hydrator);
        }
    }

    @Test
    public void firstGetThrows_noHydration_propagatesAsDbStoreExceptionWithoutRetry()
            throws Exception {
        RocksDB db = mock(RocksDB.class);
        RocksDBSession session = mock(RocksDBSession.class);
        ColumnFamilyHandle handle = mock(ColumnFamilyHandle.class);
        CFHandleLock cfLock = mock(CFHandleLock.class);
        when(cfLock.get()).thenReturn(handle);
        when(session.getDB()).thenReturn(db);
        when(session.getCFHandleLock(TABLE)).thenReturn(cfLock);
        when(session.getGraphName()).thenReturn("db-mock");
        when(db.get(any(ColumnFamilyHandle.class), any(byte[].class)))
                .thenThrow(new RocksDBException("simulated missing SST"));

        RocksdbChangedListener noHydration = new RocksdbChangedListener() {
            @Override
            public boolean onReadMiss(RocksDBSession s, String table, byte[] key) {
                return false; // nothing could be restored
            }
        };
        RocksDBFactory.getInstance().addRocksdbChangedListener(noHydration);
        try {
            SessionOperator op = new SessionOperatorImpl(session);
            try {
                op.get(TABLE, bytes("cold"));
                fail("expected DBStoreException to propagate when hydration restores nothing");
            } catch (DBStoreException expected) {
                // expected
            }
            // No retry read when hydration reported nothing: only the single failing get.
            verify(db, times(1)).get(any(ColumnFamilyHandle.class), any(byte[].class));
        } finally {
            RocksDBFactory.getInstance().removeRocksdbChangedListener(noHydration);
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private interface SessionTask {
        void run(RocksDBSession session) throws Exception;
    }

    private static void withSession(String dbName, RocksdbChangedListener listener,
                                    SessionTask task) throws Exception {
        Path parent = Files.createTempDirectory("hg-readmiss-");
        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.addRocksdbChangedListener(listener);
        try {
            RocksDBSession session = factory.createGraphDB(parent.toString(), dbName, 0);
            task.run(session);
        } finally {
            factory.removeRocksdbChangedListener(listener);
            try {
                factory.destroyGraphDB(dbName);
            } catch (Exception ignore) {
                // best-effort
            }
            deleteRecursively(parent.toFile());
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void deleteRecursively(java.io.File dir) {
        if (dir == null || !dir.exists()) {
            return;
        }
        java.io.File[] children = dir.listFiles();
        if (children != null) {
            for (java.io.File c : children) {
                if (c.isDirectory()) {
                    deleteRecursively(c);
                } else {
                    c.delete();
                }
            }
        }
        dir.delete();
    }
}
