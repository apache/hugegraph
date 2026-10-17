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

package org.apache.hugegraph.unit.rocksdb;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBMetrics;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBOptions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBStdSessions;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBStore;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBStoreProvider;
import org.apache.hugegraph.backend.store.rocksdbsst.RocksDBSstSessions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.unit.FakeObjects;
import org.junit.Test;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class RocksDBSessionsTest extends BaseRocksDBUnitTest {

    @Test
    public void testResetWaitsForNativeCloseAndSkipsRetainedClosedSession() throws Exception {
        CountDownLatch disposed = new CountDownLatch(1);
        CountDownLatch finishClose = new CountDownLatch(1);
        CountDownLatch resetStarted = new CountDownLatch(1);
        CountDownLatch resetFinished = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<RocksDBSessions.Session> closing = new AtomicReference<>();
        Thread closer = new Thread(() -> {
            try {
                RocksDBSessions.Session session = this.rocks.session();
                closing.set(session);
                WriteBatch previous = Whitebox.getInternalState(session, "batch");
                previous.close();
                WriteBatch guarded = new WriteBatch() {
                    @Override
                    public void close() {
                        super.close();
                        disposed.countDown();
                        try {
                            Assert.assertTrue(finishClose.await(30, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }

                    @Override
                    public void clear() {
                        // Trap the bad interleaving before calling a freed JNI address.
                        Assert.assertTrue("Reset touched a disposed native batch", this.isOwningHandle());
                        super.clear();
                    }
                };
                Whitebox.setInternalState(session, "batch", guarded);
                this.rocks.close();
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
                disposed.countDown();
            }
        }, "session-close");
        Thread resetter = new Thread(() -> {
            resetStarted.countDown();
            try {
                this.rocks.forceResetSessions();
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            } finally {
                resetFinished.countDown();
            }
        }, "session-reset");
        closer.start();
        try {
            Assert.assertTrue(disposed.await(30, TimeUnit.SECONDS));
            Assert.assertNull(failure.get());
            // Close is still running and the pool still exposes this session.
            Assert.assertTrue(closing.get().opened());
            resetter.start();
            Assert.assertTrue(resetStarted.await(30, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (resetter.getState() != Thread.State.BLOCKED && resetFinished.getCount() != 0 &&
                   System.nanoTime() < deadline) {
                Thread.sleep(1L);
            }
            Assert.assertNull(failure.get());
            Assert.assertEquals(Thread.State.BLOCKED, resetter.getState());
        } finally {
            finishClose.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(30));
            resetter.join(TimeUnit.SECONDS.toMillis(30));
        }
        Assert.assertFalse(closer.isAlive());
        Assert.assertFalse(resetter.isAlive());
        Assert.assertNull(failure.get());
        Assert.assertTrue(closing.get().closed());
        // A reference retained before pool removal must remain safe to reset.
        closing.get().reset();
        this.put("after-close", "usable");
        Assert.assertEquals("usable", this.get("after-close"));
    }

    @Test
    public void testResetDiscardsPendingWritesWithoutLeakingNativeBatch() throws Exception {
        RocksDBSessions.Session session = this.rocks.session();
        WriteBatch previous = Whitebox.getInternalState(session, "batch");
        for (int i = 0; i < 32; i++) {
            session.put(TABLE, getBytes("pending"), getBytes("value"));
            this.rocks.forceResetSessions();
            WriteBatch current = Whitebox.getInternalState(session, "batch");
            Assert.assertFalse(session.hasChanges());
            Assert.assertTrue(current.isOwningHandle());
            Assert.assertTrue(current == previous || !previous.isOwningHandle());
            previous = current;
        }
        session.put(TABLE, getBytes("retained"), getBytes("value"));
        session.commit();
        Assert.assertNull(this.get("pending"));
        Assert.assertEquals("value", this.get("retained"));
    }

    @Test
    public void testFinalDetachDisposesNativeOwners() throws Exception {
        RocksDBSessions.Session session = this.rocks.session();
        WriteBatch batch = Whitebox.getInternalState(session, "batch");
        WriteOptions options = Whitebox.getInternalState(session, "writeOptions");
        // Keep the DB alive while this worker releases its final request lease.
        CountDownLatch attached = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread keeper = new Thread(() -> {
            try {
                this.rocks.session();
                attached.countDown();
                release.await();
            } catch (Throwable e) {
                failure.set(e);
            } finally {
                attached.countDown();
                try {
                    this.rocks.close();
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            }
        });
        keeper.start();
        try {
            Assert.assertTrue(attached.await(30, TimeUnit.SECONDS));
            Assert.assertNull(failure.get());
            Assert.assertSame(session, this.rocks.useSession());
            Assert.assertFalse(this.rocks.close());
            Assert.assertTrue(batch.isOwningHandle());
            Assert.assertTrue(options.isOwningHandle());
            session.put(TABLE, getBytes("lease"), getBytes("retained"));
            session.commit();
            Assert.assertFalse(this.rocks.close());
            Assert.assertFalse(batch.isOwningHandle());
            Assert.assertFalse(options.isOwningHandle());
            // Native close is idempotent even after the pool removed this session.
            session.close();
            Assert.assertFalse(batch.isOwningHandle());
            Assert.assertFalse(options.isOwningHandle());
            for (int i = 0; i < 32; i++) {
                RocksDBSessions.Session request = this.rocks.session();
                Assert.assertNotSame(session, request);
                WriteBatch requestBatch = Whitebox.getInternalState(request, "batch");
                WriteOptions requestOptions = Whitebox.getInternalState(request, "writeOptions");
                Assert.assertTrue(requestBatch.isOwningHandle());
                Assert.assertTrue(requestOptions.isOwningHandle());
                Assert.assertArrayEquals(getBytes("retained"), request.get(TABLE, getBytes("lease")));
                request.put(TABLE, getBytes("request"), getBytes("value"));
                request.commit();
                Assert.assertFalse(this.rocks.close());
                Assert.assertFalse(requestBatch.isOwningHandle());
                Assert.assertFalse(requestOptions.isOwningHandle());
            }
        } finally {
            // Leave the fixture's main-worker session open for ordinary teardown.
            this.rocks.session();
            release.countDown();
            keeper.join(TimeUnit.SECONDS.toMillis(30));
        }
        Assert.assertFalse(keeper.isAlive());
        Assert.assertNull(failure.get());
        Assert.assertEquals("retained", this.get("lease"));
    }

    @Test
    public void testAdapterToplingTruncateWithMultipleKeys() throws Exception {
        this.assertAdapterTruncate(true);
    }

    @Test
    public void testAdapterStandardTruncateWithMultipleKeys() throws Exception {
        this.assertAdapterTruncate(false);
    }

    @Test
    public void testAdapterToplingTruncateDiscardsPendingWritesInEmptyTable() throws Exception {
        this.assertAdapterTruncateDiscardsPendingWrites(true, false);
    }

    @Test
    public void testAdapterToplingTruncateDiscardsPendingWritesOutsideRange() throws Exception {
        this.assertAdapterTruncateDiscardsPendingWrites(true, true);
    }

    @Test
    public void testAdapterStandardTruncateDiscardsPendingWritesInEmptyTable() throws Exception {
        this.assertAdapterTruncateDiscardsPendingWrites(false, false);
    }

    @Test
    public void testAdapterStandardTruncateDiscardsPendingWritesOutsideRange() throws Exception {
        this.assertAdapterTruncateDiscardsPendingWrites(false, true);
    }

    @Test
    public void testAdapterToplingTruncatePreservesBackendVersion() throws Exception {
        this.assertAdapterTruncatePreservesBackendVersion(true);
    }

    @Test
    public void testAdapterStandardTruncatePreservesBackendVersion() throws Exception {
        this.assertAdapterTruncatePreservesBackendVersion(false);
    }

    @Test
    public void testToplingTruncateRoutesDynamicOlapTablesToTheirDatabase() throws Exception {
        String dynamic = "graph+ap_123";
        String olapPath = DB_PATH + "/independent-olap";
        RocksDBStdSessions olap = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "olap", olapPath, olapPath);
        try {
            olap.createTable(dynamic);
            olap.session().put(dynamic, getBytes("key"), getBytes("olap value"));
            olap.session().commit();
            this.put("main", "main value");
            RocksDBStore store = new RocksDBStore.RocksDBGraphStore(null, "db", "graph") {
                @Override
                protected List<String> tableNames() {
                    return ImmutableList.of(TABLE, dynamic);
                }

                @Override
                protected List<String> olapTables() {
                    return ImmutableList.of(dynamic);
                }

                @Override
                protected Map<String, RocksDBSessions> tableDBMapping() {
                    return java.util.Collections.singletonMap(HugeType.OLAP.string(), olap);
                }
            };
            Whitebox.setInternalState(store, "sessions", this.rocks);
            Map<String, RocksDBSessions> databases = Whitebox.getInternalState(store, "dbs");
            databases.put(DB_PATH, this.rocks);
            databases.put(olapPath, olap);
            Whitebox.setInternalState(store, "toplingProvider", true);
            // Real standard JNI exercises the Topling Java routing branch.
            Assert.assertFalse(this.rocks.existsTable(dynamic));
            store.truncate();
            Assert.assertNull(this.get("main"));
            Assert.assertTrue(olap.existsTable(dynamic));
            Assert.assertNull(olap.session().get(dynamic, getBytes("key")));
            olap.session().put(dynamic, getBytes("next"), getBytes("after truncate"));
            olap.session().commit();
            Assert.assertArrayEquals(getBytes("after truncate"), olap.session().get(dynamic, getBytes("next")));
        } finally {
            olap.close();
        }
    }

    @Test
    public void testToplingOlapClearKeepsHandleAndOtherTables() throws Exception {
        this.assertOlapClear(true, false);
    }

    @Test
    public void testToplingOlapClearRoutesIndependentDatabase() throws Exception {
        this.assertOlapClear(true, true);
    }

    @Test
    public void testStandardOlapClearRecreatesTable() throws Exception {
        this.assertOlapClear(false, false);
    }

    private void assertOlapClear(boolean topling, boolean separate) throws Exception {
        Id id = IdGenerator.of(123L);
        String path = DB_PATH + "/olap-clear-" + topling + "-" + separate;
        RocksDBSessions olap = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "olap", path, path);
        RocksDBStore.RocksDBGraphStore store = new RocksDBStore.RocksDBGraphStore(
                new RocksDBStoreProvider(), "db", "store") {
            @Override
            protected RocksDBSessions db(HugeType type) {
                return type == HugeType.OLAP ? olap : super.db(type);
            }
        };
        Whitebox.setInternalState(store, "sessions", separate ? this.rocks : olap);
        Whitebox.setInternalState(store, "toplingProvider", topling);
        try {
            olap.session();
            store.createOlapTable(id);
            String table = "store+ap_123";
            olap.createTable(TABLE, "other-olap");
            RocksDBSessions.Session session = olap.session();
            session.put(table, getBytes("m"), getBytes("old"));
            session.put(table, getBytes("n"), getBytes("old"));
            session.put("other-olap", getBytes("other"), getBytes("retained"));
            session.commit();
            session.put(TABLE, getBytes("main"), getBytes("retained main"));
            session.commit();
            AtomicReference<?> owner = Whitebox.getInternalState(olap, "rocksdb");
            Object opened = owner.get();
            Map<String, ?> handles = Whitebox.getInternalState(opened, "cfHandles");
            Object handle = handles.get(table);
            Assert.assertNotNull(handle);
            for (int i = 0; i < 2; i++) {
                // Pending keys lie outside the stored range, or in an empty table.
                if (topling) {
                    session.put(table, getBytes("a"), getBytes("pending first"));
                    session.put(table, getBytes("z"), getBytes("pending last"));
                }
                store.clearOlapTable(id);
                if (topling) {
                    Assert.assertSame(handle, handles.get(table));
                } else {
                    Assert.assertNotSame(handle, handles.get(table));
                }
                Assert.assertTrue(olap.existsTable(table));
                Assert.assertNull(session.keyRange(table));
                session.commit();
                Assert.assertNull(session.keyRange(table));
                Assert.assertArrayEquals(getBytes("retained"), session.get("other-olap", getBytes("other")));
                Assert.assertArrayEquals(getBytes("retained main"), session.get(TABLE, getBytes("main")));
            }
            session.put(table, getBytes("new"), getBytes("after clear"));
            session.commit();
            olap.close();
            RocksDBSessions reopened = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "olap", path, path,
                                                               ImmutableList.of(TABLE, table, "other-olap"));
            try {
                RocksDBSessions.Session reader = reopened.session();
                Assert.assertArrayEquals(getBytes("after clear"), reader.get(table, getBytes("new")));
                Assert.assertArrayEquals(getBytes("retained"), reader.get("other-olap", getBytes("other")));
            } finally {
                reopened.close();
            }
        } finally {
            olap.close();
            FileUtils.deleteDirectory(new File(path));
        }
    }

    private void assertAdapterTruncatePreservesBackendVersion(boolean topling) throws Exception {
        RocksDBStoreProvider provider = new RocksDBStoreProvider();
        RocksDBStore store = new RocksDBStore.RocksDBSystemStore(provider, "db", "store") {
            @Override
            protected List<String> tableNames() {
                return ImmutableList.<String>builder().addAll(super.tableNames()).add(TABLE).build();
            }
        };
        Whitebox.setInternalState(store, "sessions", this.rocks);
        Map<String, RocksDBSessions> databases = Whitebox.getInternalState(store, "dbs");
        databases.put(DB_PATH, this.rocks);
        Whitebox.setInternalState(store, "toplingProvider", topling);
        store.init();
        this.put("old", "before truncate");
        Assert.assertEquals(provider.driverVersion(), store.storedVersion());

        store.truncate();

        Assert.assertNull(this.get("old"));
        Assert.assertEquals(provider.driverVersion(), store.storedVersion());
        this.put("new", "after truncate");
        this.rocks.close();
        this.rocks = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                           DB_PATH, DB_PATH, ImmutableList.of(TABLE));
        this.rocks.session();
        Whitebox.setInternalState(store, "sessions", this.rocks);
        databases.clear();
        databases.put(DB_PATH, this.rocks);
        // Reopening must read the persisted version without init repairing it.
        Assert.assertEquals(provider.driverVersion(), store.storedVersion());
        Assert.assertEquals("after truncate", this.get("new"));
    }

    private void assertAdapterTruncateDiscardsPendingWrites(boolean topling, boolean committedRange) throws Exception {
        if (committedRange) {
            this.put("m", "committed first");
            this.put("n", "committed last");
        }
        RocksDBSessions.Session session = this.rocks.session();
        // Both inserts fall outside the committed range; an empty CF has no range at all.
        session.put(TABLE, getBytes("a"), getBytes("pending first"));
        session.put(TABLE, getBytes("z"), getBytes("pending last"));
        Assert.assertTrue(session.hasChanges());

        RocksDBStore store = this.adapterStore(topling, ImmutableList.of(TABLE));
        store.truncate();

        Assert.assertFalse(session.hasChanges());
        Assert.assertNull(session.keyRange(TABLE));
        Assert.assertNull(session.get(TABLE, getBytes("a")));
        Assert.assertNull(session.get(TABLE, getBytes("z")));
        // The retained session must not replay pending writes after truncate either.
        session.commit();
        Assert.assertNull(session.keyRange(TABLE));
        this.put("new", "after truncate");
        Assert.assertEquals("after truncate", this.get("new"));
    }

    private void assertAdapterTruncate(boolean topling) throws Exception {
        List<String> tables = ImmutableList.of(TABLE, "single", "empty", "multiple");
        this.rocks.createTable("single", "empty", "multiple");
        // Deliberately unordered, including empty and unsigned byte boundaries.
        byte[][] keys = {new byte[]{(byte) 0xff}, new byte[]{0}, new byte[]{},
                         new byte[]{(byte) 0x80}, new byte[]{0, (byte) 0xff}, new byte[]{0x7f}};
        for (String table : ImmutableList.of(TABLE, "multiple")) {
            for (byte[] key : keys) {
                this.rocks.session().put(table, key, new byte[]{42});
            }
        }
        this.rocks.session().put("single", new byte[]{(byte) 0xff}, new byte[]{43});
        this.commit();
        AtomicReference<?> owner = Whitebox.getInternalState(this.rocks, "rocksdb");
        Object opened = owner.get();
        Map<String, ?> handles = Whitebox.getInternalState(opened, "cfHandles");
        Map<String, ?> before = new HashMap<>(handles);
        RocksDBStore store = this.adapterStore(topling, tables);
        store.truncate();
        for (String table : tables) {
            Assert.assertTrue(this.rocks.existsTable(table));
            Assert.assertNull(this.rocks.session().keyRange(table));
            for (byte[] key : keys) {
                Assert.assertNull(this.rocks.session().get(table, key));
            }
            if (topling) {
                Assert.assertSame(before.get(table), handles.get(table));
            } else {
                Assert.assertNotSame(before.get(table), handles.get(table));
            }
        }
        for (String table : tables) {
            this.rocks.session().put(table, new byte[]{(byte) 0xff}, new byte[]{44});
        }
        this.commit();
        for (String table : tables) {
            Assert.assertArrayEquals(new byte[]{44}, this.rocks.session().get(table, new byte[]{(byte) 0xff}));
        }
        // A second truncate covers the single-key path for every formerly empty CF.
        store.truncate();
        for (String table : tables) {
            Assert.assertNull(this.rocks.session().keyRange(table));
        }
    }

    @Test
    public void testTruncatePropagatesFirstKeyReadFailure() throws Exception {
        this.assertTruncateKeyReadFailure(false);
    }

    @Test
    public void testTruncatePropagatesLastKeyReadFailure() throws Exception {
        this.assertTruncateKeyReadFailure(true);
    }

    private void assertTruncateKeyReadFailure(boolean lastKey) throws Exception {
        String brokenTable = "broken";
        this.rocks.createTable(brokenTable);
        this.put("healthy", "retained");
        this.rocks.session().put(brokenTable, getBytes("a"), getBytes("first"));
        this.rocks.session().put(brokenTable, getBytes("z"), getBytes("last"));
        this.commit();
        RocksDBStore store = this.adapterStore(true, ImmutableList.of(TABLE, brokenTable));
        AtomicReference<?> owner = Whitebox.getInternalState(this.rocks, "rocksdb");
        Object opened = owner.get();
        RocksDB realDB = Whitebox.getInternalState(opened, "rocksdb");
        Map<String, ?> handles = Whitebox.getInternalState(opened, "cfHandles");
        ColumnFamilyHandle brokenHandle = Whitebox.getInternalState(handles.get(brokenTable), "handle");
        RocksIterator brokenIterator = Mockito.mock(RocksIterator.class);
        Mockito.when(brokenIterator.isValid()).thenReturn(lastKey, false);
        Mockito.when(brokenIterator.key()).thenReturn(getBytes("a"));
        RocksDBException readFailure = new RocksDBException("injected iterator read failure");
        Mockito.doThrow(readFailure).when(brokenIterator).status();
        // Keep a real DB and healthy iterator; replace only the failing native
        // boundary. The production truncate -> clearTables -> keyRange chain runs.
        RocksDB faultDB = Mockito.mock(RocksDB.class, AdditionalAnswers.delegatesTo(realDB));
        Mockito.doReturn(brokenIterator).when(faultDB).newIterator(brokenHandle);
        Whitebox.setInternalState(opened, "rocksdb", faultDB);
        try {
            Throwable failure = Assert.assertThrows(BackendException.class, store::truncate);
            Assert.assertSame(readFailure, failure.getCause());
            Mockito.verify(brokenIterator).status();
            Mockito.verify(brokenIterator).close();
        } finally {
            Whitebox.setInternalState(opened, "rocksdb", realDB);
        }
        Assert.assertEquals("retained", this.get("healthy"));
        Assert.assertArrayEquals(getBytes("first"), this.rocks.session().get(brokenTable, getBytes("a")));
        Assert.assertArrayEquals(getBytes("last"), this.rocks.session().get(brokenTable, getBytes("z")));
        Assert.assertFalse(this.rocks.session().hasChanges());
        Assert.assertSame(brokenHandle, Whitebox.getInternalState(handles.get(brokenTable), "handle"));
    }

    private RocksDBStore adapterStore(boolean topling, List<String> tables) {
        RocksDBStore store = new RocksDBStore.RocksDBGraphStore(new RocksDBStoreProvider(), "db", "store") {
            @Override
            protected List<String> tableNames() {
                return tables;
            }
        };
        Whitebox.setInternalState(store, "sessions", this.rocks);
        Map<String, RocksDBSessions> databases = Whitebox.getInternalState(store, "dbs");
        databases.put(DB_PATH, this.rocks);
        // Select the actual adapter branch, independently of the loaded JNI.
        // This fixture also runs unchanged with the externally loaded TP JNI on Linux.
        Whitebox.setInternalState(store, "toplingProvider", topling);
        return store;
    }

    @Test
    public void testDatabaseOpenedDoesNotCreateSession() throws RocksDBException {
        HugeConfig config = FakeObjects.newConfig();
        String path = DB_PATH + "/opened";
        RocksDBSessions sessions =
                new RocksDBStdSessions(config, "db", "store", path, path);
        AtomicInteger sessionCount =
                Whitebox.getInternalState(sessions, "sessionCount");

        Assert.assertEquals(0, sessionCount.get());
        Assert.assertTrue(sessions.databaseOpened());
        Assert.assertEquals(0, sessionCount.get());

        sessions.forceCloseRocksDB();
        Assert.assertFalse(sessions.databaseOpened());
    }

    @Test
    public void testTable() throws RocksDBException {
        final String TABLE2 = "test-table2";

        Assert.assertTrue(this.rocks.existsTable(TABLE));
        Assert.assertFalse(this.rocks.existsTable(TABLE2));

        this.rocks.createTable(TABLE2);
        Assert.assertTrue(this.rocks.existsTable(TABLE2));
        Assert.assertEquals(ImmutableSet.of(TABLE, TABLE2),
                            this.rocks.openedTables());

        this.rocks.session().put(TABLE, getBytes("person:1gname"), getBytes("James"));
        this.rocks.session().put(TABLE2, getBytes("person:1gname"), getBytes("James2"));
        this.commit();

        String value = getString(this.rocks.session().get(TABLE, getBytes("person:1gname")));
        Assert.assertEquals("James", value);

        String value2 = getString(this.rocks.session().get(TABLE2, getBytes("person:1gname")));
        Assert.assertEquals("James2", value2);

        this.rocks.dropTable(TABLE2);
        Assert.assertFalse(this.rocks.existsTable(TABLE2));
        Assert.assertEquals(ImmutableSet.of(TABLE),
                            this.rocks.openedTables());
    }

    @Test
    public void testProperty() throws RocksDBException {
        final String TABLE2 = "test-table2";
        this.rocks.createTable(TABLE2);

        this.rocks.session().put(TABLE, getBytes("person:1gname"), getBytes("James"));
        this.rocks.session().put(TABLE, getBytes("person:2gname"), getBytes("James2"));
        this.commit();

        Assert.assertEquals(ImmutableList.of("0"),
                            this.rocks.property(RocksDBMetrics.KEY_DISK_USAGE));
        Assert.assertEquals(ImmutableList.of("2", "0"),
                            this.rocks.property(RocksDBMetrics.KEY_NUM_KEYS));

        this.rocks.session().put(TABLE2, getBytes("person:1gname"), getBytes("James1"));
        this.rocks.session().put(TABLE2, getBytes("person:2gname"), getBytes("James2"));
        this.rocks.session().put(TABLE2, getBytes("person:3gname"), getBytes("James3"));
        this.commit();

        Assert.assertEquals(ImmutableList.of("0"),
                            this.rocks.property(RocksDBMetrics.KEY_DISK_USAGE));
        Assert.assertEquals(ImmutableList.of("2", "3"),
                            this.rocks.property(RocksDBMetrics.KEY_NUM_KEYS));
    }

    @Test
    public void testCompactRange() throws RocksDBException {
        this.rocks.session().put(TABLE, getBytes("person:1gname"), getBytes("James"));
        this.rocks.session().put(TABLE, getBytes("person:2gname"), getBytes("James2"));
        this.commit();

        this.rocks.compactRange();

        String value = getString(this.rocks.session().get(TABLE, getBytes("person:1gname")));
        Assert.assertEquals("James", value);

        value = getString(this.rocks.session().get(TABLE, getBytes("person:2gname")));
        Assert.assertEquals("James2", value);
    }

    @Test
    public void testSnapshot() throws RocksDBException, IOException {
        this.rocks.session().put(TABLE, getBytes("person:1gname"), getBytes("James"));
        this.rocks.session().commit();

        String snapshotPath = SNAPSHOT_PATH + "/rocksdb";
        try {
            this.rocks.createSnapshot(snapshotPath);

            byte[] value = this.rocks.session().get(TABLE, getBytes("person:1gname"));
            Assert.assertEquals("James", getString(value));

            this.rocks.session().put(TABLE, getBytes("person:1gname"), getBytes("James2"));
            this.rocks.session().commit();

            value = this.rocks.session().get(TABLE, getBytes("person:1gname"));
            Assert.assertEquals("James2", getString(value));

            this.rocks.resumeSnapshot(snapshotPath);

            value = this.rocks.session().get(TABLE, getBytes("person:1gname"));
            Assert.assertEquals("James", getString(value));
        } finally {
            File snapshotFile = FileUtils.getFile(SNAPSHOT_PATH);
            if (snapshotFile.exists()) {
                FileUtils.forceDelete(snapshotFile);
            }
        }
    }

    @Test
    public void testSnapshotWithSeparateWalDirectory() throws Exception {
        String dataPath = DB_PATH + "/separate-data";
        String walPath = DB_PATH + "/separate-wal";
        String snapshotPath = SNAPSHOT_PATH + "/separate-rocks";
        FileUtils.deleteDirectory(FileUtils.getFile(dataPath));
        FileUtils.deleteDirectory(FileUtils.getFile(walPath));
        FileUtils.deleteDirectory(FileUtils.getFile(snapshotPath));
        HugeConfig config = FakeObjects.newConfig();
        RocksDBSessions sessions = new RocksDBStdSessions(config, "db", "store",
                                                          dataPath, walPath);
        try {
            sessions.createTable(TABLE);
            sessions.session().put(TABLE, getBytes("person:1gname"),
                                   getBytes("James"));
            sessions.session().put(TABLE, getBytes("person:2gname"),
                                   getBytes("Lisa"));
            sessions.session().commit();

            sessions.createSnapshot(snapshotPath);

            sessions.session().put(TABLE, getBytes("person:1gname"),
                                   getBytes("James2"));
            sessions.session().put(TABLE, getBytes("person:3gname"),
                                   getBytes("After"));
            sessions.session().commit();
            Assert.assertEquals("James2", getString(sessions.session().get(
                    TABLE, getBytes("person:1gname"))));

            sessions.resumeSnapshot(snapshotPath);

            Assert.assertEquals("James", getString(sessions.session().get(
                    TABLE, getBytes("person:1gname"))));
            Assert.assertEquals("Lisa", getString(sessions.session().get(
                    TABLE, getBytes("person:2gname"))));
            Assert.assertNull(sessions.session().get(TABLE,
                                                     getBytes("person:3gname")));
            Assert.assertEquals(0, readWalLogs(dataPath).size());
            assertNoResumeResidue(dataPath);
        } finally {
            sessions.close();
            FileUtils.deleteDirectory(FileUtils.getFile(dataPath));
            FileUtils.deleteDirectory(FileUtils.getFile(walPath));
            File snapshotFile = FileUtils.getFile(SNAPSHOT_PATH);
            if (snapshotFile.exists()) {
                FileUtils.forceDelete(snapshotFile);
            }
        }
    }


    @Test
    public void testResumeWhenDataDirectoryIsInsideWal() throws Exception {
        String walPath = nestedRoot("parent-wal");
        String dataPath = walPath + "/nested-data";
        resumeNestedAndExpectSnapshot(dataPath, walPath);
    }

    @Test
    public void testResumeWhenWalDirectoryIsInsideData() throws Exception {
        String dataPath = nestedRoot("parent-data");
        String walPath = dataPath + "/nested-wal";
        resumeNestedAndExpectSnapshot(dataPath, walPath);
    }


    private static String nestedRoot(String name) {
        return System.getProperty("java.io.tmpdir") + "/nested-wal-closure/" + name;
    }

    private void resumeNestedAndExpectSnapshot(String dataPath, String walPath)
                                                throws Exception {
        String snapshotPath = SNAPSHOT_PATH + "/nested-rocks";
        FileUtils.deleteDirectory(FileUtils.getFile(dataPath));
        FileUtils.deleteDirectory(FileUtils.getFile(walPath));
        FileUtils.forceMkdir(FileUtils.getFile(walPath).getParentFile());
        FileUtils.forceMkdir(FileUtils.getFile(dataPath).getParentFile());
        HugeConfig config = FakeObjects.newConfig();
        RocksDBSessions sessions = new RocksDBStdSessions(config, "db", "store",
                                                          dataPath, walPath);
        try {
            sessions.createTable(TABLE);
            sessions.session().put(TABLE, getBytes("person:1gname"),
                                   getBytes("James"));
            sessions.session().commit();
            sessions.createSnapshot(snapshotPath);
            sessions.session().put(TABLE, getBytes("person:1gname"),
                                   getBytes("James2"));
            sessions.session().commit();
            sessions.resumeSnapshot(snapshotPath);
            Assert.assertEquals("James", getString(sessions.session().get(
                    TABLE, getBytes("person:1gname"))));
        } finally {
            sessions.close();
            FileUtils.deleteDirectory(FileUtils.getFile(dataPath));
            FileUtils.deleteDirectory(FileUtils.getFile(walPath));
            File snapshotFile = FileUtils.getFile(SNAPSHOT_PATH);
            if (snapshotFile.exists()) {
                FileUtils.forceDelete(snapshotFile);
            }
        }
    }

    private static Map<String, byte[]> readWalLogs(String directory) throws IOException {
        Map<String, byte[]> logs = new HashMap<>();
        File dir = FileUtils.getFile(directory);
        File[] children = dir.listFiles();
        if (children == null) {
            return logs;
        }
        for (File child : children) {
            String name = child.getName();
            int dot = name.lastIndexOf('.');
            if (!child.isFile() || dot <= 0 || !".log".equals(name.substring(dot))) {
                continue;
            }
            boolean digits = true;
            for (int i = 0; i < dot; i++) {
                if (!Character.isDigit(name.charAt(i))) {
                    digits = false;
                    break;
                }
            }
            if (digits) {
                logs.put(name, FileUtils.readFileToByteArray(child));
            }
        }
        return logs;
    }

    @Test
    public void testResumeResidueAssertionDetectsPendingMarker() throws IOException {
        String data = DB_PATH + "/residue-data";
        File marker = FileUtils.getFile(data + ".resume-pending");
        try {
            FileUtils.writeByteArrayToFile(marker, new byte[]{1});
            Assert.assertThrows(AssertionError.class, () -> assertNoResumeResidue(data));
        } finally {
            FileUtils.forceDelete(marker);
        }
    }

    private static void assertNoResumeResidue(String dataPath) {
        File data = FileUtils.getFile(dataPath).getAbsoluteFile();
        File[] children = data.getParentFile().listFiles();
        Assert.assertNotNull(children);
        for (File child : children) {
            String name = child.getName();
            Assert.assertFalse(name.startsWith(data.getName() + ".resume-staging-"));
            Assert.assertFalse(name.equals(data.getName() + ".resume-pending"));
        }
    }

    @Test
    public void testCopySessions() throws RocksDBException {
        Assert.assertFalse(this.rocks.closed());

        HugeConfig config = FakeObjects.newConfig();
        RocksDBSessions copy = this.rocks.copy(config, "db2", "store2");
        Assert.assertFalse(this.rocks.closed());

        final String TABLE2 = "test-table2";
        copy.createTable(TABLE2);

        copy.session().put(TABLE2, getBytes("person:1gname"), getBytes("James"));
        copy.session().commit();

        String value = getString(copy.session().get(TABLE2, getBytes("person:1gname")));
        Assert.assertEquals("James", value);

        copy.close();
        Assert.assertTrue(copy.closed());
        Assert.assertFalse(this.rocks.closed());
    }

    @Test
    public void testIngestSst() throws RocksDBException {
        HugeConfig config = FakeObjects.newConfig();
        String sstPath = DB_PATH + "/sst";
        config.addProperty(RocksDBOptions.SST_PATH.name(), sstPath);
        RocksDBSstSessions sstSessions = new RocksDBSstSessions(config, "sst", "store", sstPath);
        final String TABLE1 = "test-table1";
        final String TABLE2 = "test-table2";
        sstSessions.createTable(TABLE1);
        Assert.assertEquals(1, sstSessions.openedTables().size());
        sstSessions.createTable(TABLE2);
        Assert.assertEquals(2, sstSessions.openedTables().size());
        Assert.assertTrue(sstSessions.existsTable(TABLE1));
        Assert.assertTrue(sstSessions.existsTable(TABLE2));

        // Write some data to sst file
        for (int i = 0; i < 1000; i++) {
            String k = String.format("%03d", i);
            sstSessions.session().put(TABLE1, getBytes("person:" + k), getBytes("James" + i));
        }
        for (int i = 0; i < 2000; i++) {
            String k = String.format("%04d", i);
            sstSessions.session().put(TABLE2, getBytes("book:" + k), getBytes("Java" + i));
        }
        sstSessions.session().commit();
        sstSessions.close();

        sstSessions.dropTable(TABLE1);
        sstSessions.dropTable(TABLE2);
        Assert.assertEquals(0, sstSessions.openedTables().size());
        Assert.assertFalse(sstSessions.existsTable(TABLE1));
        Assert.assertFalse(sstSessions.existsTable(TABLE2));

        RocksDBSessions rocks = new RocksDBStdSessions(config, "db", "store", sstPath, sstPath);
        // Will ingest sst file of TABLE1
        rocks.createTable(TABLE1);
        Assert.assertEquals(ImmutableList.of("1000"),
                            rocks.property(RocksDBMetrics.KEY_NUM_KEYS));
        String value = getString(rocks.session().get(TABLE1, getBytes("person:001")));
        Assert.assertEquals("James1", value);
        value = getString(rocks.session().get(TABLE1, getBytes("person:010")));
        Assert.assertEquals("James10", value);
        value = getString(rocks.session().get(TABLE1, getBytes("person:999")));
        Assert.assertEquals("James999", value);

        // Will ingest sst file of TABLE2
        rocks.createTable(TABLE2);
        Assert.assertEquals(ImmutableList.of("1000", "2000"),
                            rocks.property(RocksDBMetrics.KEY_NUM_KEYS));
        value = getString(rocks.session().get(TABLE2, getBytes("book:0001")));
        Assert.assertEquals("Java1", value);
        value = getString(rocks.session().get(TABLE2, getBytes("book:0010")));
        Assert.assertEquals("Java10", value);
        value = getString(rocks.session().get(TABLE2, getBytes("book:0999")));
        Assert.assertEquals("Java999", value);
        value = getString(rocks.session().get(TABLE2, getBytes("book:1999")));
        Assert.assertEquals("Java1999", value);
    }
}
