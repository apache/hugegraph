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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBOptions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions.Session;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBStdSessions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Lifecycle and negative-path tests for {@link RocksDBStdSessions}. Every test
 * uses its own temporary directory so it never shares the fixed path used by
 * {@link BaseRocksDBUnitTest}.
 */
public class RocksDBStdSessionsLifecycleTest extends BaseUnitTest {

    private static final String TABLE = "lifecycle-table";

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    private final List<RocksDBSessions> opened = new ArrayList<>();

    @BeforeClass
    public static void registerRocksDBOptions() {
        // Typed values in HugeConfig require the RocksDB options to be known
        if (!OptionSpace.containKey(RocksDBOptions.NUM_LEVELS.name())) {
            OptionSpace.register("rocksdb", RocksDBOptions.class.getName());
        }
    }

    @After
    public void closeOpenedSessions() {
        for (RocksDBSessions sessions : this.opened) {
            try {
                // Closes this thread's session and, when it is the last one, the DB
                sessions.close();
            } finally {
                if (sessions.databaseOpened()) {
                    sessions.forceCloseRocksDB();
                }
            }
        }
        this.opened.clear();
    }

    @Test
    public void testOperationsOnUnopenedTableAreRejected() throws Exception {
        RocksDBSessions sessions = this.open(this.newPath("unopened"));
        Session session = sessions.session();
        byte[] key = getBytes("k");

        Assert.assertThrows(BackendException.class, () -> {
            session.put("missing", key, getBytes("v"));
        }, e -> {
            Assert.assertContains("Table 'missing' is not opened", e.getMessage());
        });
        Assert.assertFalse(session.hasChanges());

        Assert.assertThrows(BackendException.class, () -> {
            session.get("missing", key);
        }, e -> {
            Assert.assertContains("Table 'missing' is not opened", e.getMessage());
        });
        Assert.assertThrows(BackendException.class, () -> {
            session.scan("missing");
        }, e -> {
            Assert.assertContains("Table 'missing' is not opened", e.getMessage());
        });
        Assert.assertThrows(BackendException.class, () -> {
            session.delete("missing", key);
        }, e -> {
            Assert.assertContains("Table 'missing' is not opened", e.getMessage());
        });
        Assert.assertThrows(BackendException.class, () -> {
            session.keyRange("missing");
        }, e -> {
            Assert.assertContains("Table 'missing' is not opened", e.getMessage());
        });

        // The opened table is still fully usable after the rejected calls
        session.put(TABLE, key, getBytes("v"));
        Assert.assertEquals(1, (int) session.commit());
        Assert.assertEquals("v", getString(session.get(TABLE, key)));
    }

    @Test
    public void testCreateTableTwiceKeepsDataAndDropMissingTableIsNoop() throws Exception {
        RocksDBSessions sessions = this.open(this.newPath("idempotent"));
        Session session = sessions.session();
        session.put(TABLE, getBytes("k"), getBytes("v"));
        session.commit();

        // Re-creating an existing table must not truncate or replace it
        sessions.createTable(TABLE);
        Assert.assertEquals("v", getString(session.get(TABLE, getBytes("k"))));

        // Dropping a table that never existed leaves the opened tables intact
        sessions.dropTable("never-created");
        Assert.assertTrue(sessions.existsTable(TABLE));
        Assert.assertFalse(sessions.existsTable("never-created"));

        sessions.dropTable(TABLE);
        Assert.assertFalse(sessions.existsTable(TABLE));
        // Dropping again after the table is gone is still a no-op
        sessions.dropTable(TABLE);
        Assert.assertFalse(sessions.openedTables().contains(TABLE));
    }

    @Test
    public void testRollbackDiscardsPendingWrites() throws Exception {
        RocksDBSessions sessions = this.open(this.newPath("rollback"));
        Session session = sessions.session();

        Assert.assertEquals(0, (int) session.commit());
        session.put(TABLE, getBytes("k2"), getBytes("v2"));
        Assert.assertEquals(1, (int) session.commit());

        session.put(TABLE, getBytes("k1"), getBytes("v1"));
        session.delete(TABLE, getBytes("k2"));
        Assert.assertTrue(session.hasChanges());

        session.rollback();
        Assert.assertFalse(session.hasChanges());
        Assert.assertEquals(0, (int) session.commit());
        // Neither the pending put nor the pending delete reached the DB
        Assert.assertNull(session.get(TABLE, getBytes("k1")));
        Assert.assertEquals("v2", getString(session.get(TABLE, getBytes("k2"))));
    }

    @Test
    public void testCommittedDataSurvivesReopenButPendingWritesDoNot() throws Exception {
        String path = this.newPath("reopen");
        RocksDBSessions sessions = this.open(path);
        Session session = sessions.session();
        session.put(TABLE, getBytes("committed"), getBytes("v1"));
        session.commit();
        session.put(TABLE, getBytes("pending"), getBytes("v2"));
        Assert.assertTrue(session.hasChanges());

        Assert.assertTrue(sessions.close());
        Assert.assertFalse(sessions.databaseOpened());

        // Reopen without createTable(): existing column families are reloaded
        RocksDBSessions reopened = this.openWithoutTable(path);
        Assert.assertTrue(reopened.existsTable(TABLE));
        Session session2 = reopened.session();
        Assert.assertFalse(session2.hasChanges());
        Assert.assertEquals("v1", getString(session2.get(TABLE, getBytes("committed"))));
        Assert.assertNull(session2.get(TABLE, getBytes("pending")));
    }

    @Test
    public void testLiveDatabaseCannotBeOpenedTwiceUntilClosed() throws Exception {
        String path = this.newPath("exclusive");
        RocksDBSessions sessions = this.open(path);
        sessions.session().put(TABLE, getBytes("k"), getBytes("v"));
        sessions.session().commit();

        Assert.assertThrows(RocksDBException.class, () -> {
            this.openWithoutTable(path);
        }, e -> {
            Assert.assertEquals(Status.Code.IOError, ((RocksDBException) e).getStatus().getCode());
        });
        // The failed open must not disturb the live instance
        Assert.assertTrue(sessions.databaseOpened());
        Assert.assertEquals("v", getString(sessions.session().get(TABLE, getBytes("k"))));

        Assert.assertTrue(sessions.close());
        RocksDBSessions reopened = this.openWithoutTable(path);
        Assert.assertEquals("v", getString(reopened.session().get(TABLE, getBytes("k"))));
    }

    @Test
    public void testClosedDatabaseRejectsTableAndSessionAccess() throws Exception {
        String path = this.newPath("closed");
        RocksDBSessions sessions = this.open(path);
        sessions.forceCloseRocksDB();
        Assert.assertFalse(sessions.databaseOpened());

        Assert.assertThrows(IllegalStateException.class, () -> {
            sessions.createTable("another-table");
        }, e -> {
            Assert.assertContains("RocksDB has been closed", e.getMessage());
        });
        Assert.assertThrows(IllegalStateException.class, () -> {
            sessions.dropTable(TABLE);
        }, e -> {
            Assert.assertContains("RocksDB has been closed", e.getMessage());
        });
        Assert.assertThrows(IllegalStateException.class, sessions::compactRange, e -> {
            Assert.assertContains("RocksDB has been closed", e.getMessage());
        });
        Assert.assertThrows(IllegalStateException.class, sessions::session, e -> {
            Assert.assertContains("session pool is closed", e.getMessage());
        });

        // Force close must release the database lock for a later owner
        RocksDBSessions reopened = this.openWithoutTable(path);
        Assert.assertTrue(reopened.databaseOpened());
        Assert.assertEquals(ImmutableSet.of("default", TABLE), reopened.openedTables());
    }

    @Test
    public void testScanIteratorCloseIsIdempotentAndStopsIteration() throws Exception {
        RocksDBSessions sessions = this.open(this.newPath("scan-close"));
        Session session = sessions.session();
        session.put(TABLE, getBytes("a1"), getBytes("1"));
        session.put(TABLE, getBytes("a2"), getBytes("2"));
        session.put(TABLE, getBytes("b1"), getBytes("3"));
        session.commit();

        BackendColumnIterator iter = session.scan(TABLE, getBytes("a"));
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals("a1", getString(iter.next().name));
        Assert.assertEquals("a1", getString(iter.position()));

        // Closing early releases the native iterator; further reads end cleanly
        iter.close();
        iter.close();
        Assert.assertFalse(iter.hasNext());
        Assert.assertThrows(NoSuchElementException.class, iter::next);

        // A fully consumed iterator clears its paging position
        BackendColumnIterator all = session.scan(TABLE, getBytes("a"));
        Assert.assertEquals("a1", getString(all.next().name));
        Assert.assertEquals("a2", getString(all.next().name));
        Assert.assertFalse(all.hasNext());
        Assert.assertNull(all.position());
        all.close();
    }

    @Test
    public void testMismatchedLevelCompressionsRejectedAndLockReleased() throws Exception {
        String path = this.newPath("bad-options");
        PropertiesConfiguration conf = new PropertiesConfiguration();
        conf.setProperty(RocksDBOptions.NUM_LEVELS.name(), "3");
        conf.setProperty(RocksDBOptions.LEVELS_COMPRESSIONS.name(), "[none, snappy]");
        HugeConfig badConfig = new HugeConfig(conf);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new RocksDBStdSessions(badConfig, "db", "store", path, path);
        }, e -> {
            Assert.assertContains(RocksDBOptions.LEVELS_COMPRESSIONS.name(), e.getMessage());
            Assert.assertContains("but got 2 != 3", e.getMessage());
        });

        // The failed open must not leave the database lock behind
        RocksDBSessions sessions = this.open(path);
        Assert.assertTrue(sessions.databaseOpened());
    }

    @Test
    public void testBuildSnapshotPathRequiresExistingSnapshot() throws Exception {
        File parent = this.temporary.newFolder("graph");
        File dataDir = new File(parent, "rocksdb-data/g");
        Assert.assertTrue(dataDir.getParentFile().mkdirs());
        String dataPath = dataDir.getAbsolutePath();
        RocksDBSessions sessions = this.open(dataPath);

        File expected = new File(parent, "snapshot_rocksdb-data/g");
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            sessions.buildSnapshotPath("snapshot");
        }, e -> {
            Assert.assertContains("doesn't exist", e.getMessage());
            Assert.assertContains(expected.getPath(), e.getMessage());
        });

        Assert.assertTrue(expected.mkdirs());
        Assert.assertEquals(expected.getPath(), sessions.buildSnapshotPath("snapshot"));
    }

    @Test
    public void testNonAsciiDataSurvivesReopen() throws Exception {
        String path = this.newPath("utf8");
        RocksDBSessions sessions = this.open(path);
        sessions.session().put(TABLE, getBytes("图-key"), getBytes("值-\uD83D\uDE00"));
        sessions.session().commit();
        Assert.assertTrue(sessions.close());

        RocksDBSessions reopened = this.openWithoutTable(path);
        Assert.assertEquals("值-\uD83D\uDE00",
                            getString(reopened.session().get(TABLE, getBytes("图-key"))));
        Assert.assertArrayEquals("图-key".getBytes(StandardCharsets.UTF_8), getBytes("图-key"));
    }

    private String newPath(String name) throws Exception {
        return new File(this.temporary.newFolder(name), "db").getAbsolutePath();
    }

    private RocksDBSessions open(String path) throws RocksDBException {
        RocksDBSessions sessions = this.openWithoutTable(path);
        sessions.createTable(TABLE);
        return sessions;
    }

    private RocksDBSessions openWithoutTable(String path) throws RocksDBException {
        // Passing no CF names still reopens every column family found on disk
        RocksDBSessions sessions = new RocksDBStdSessions(FakeObjects.newConfig(), "db",
                                                          "store", path, path,
                                                          ImmutableList.of());
        this.opened.add(sessions);
        return sessions;
    }

    private static byte[] getBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    private static String getString(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }
}
