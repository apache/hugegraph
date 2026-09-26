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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBMetrics;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBOptions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBStdSessions;
import org.apache.hugegraph.backend.store.rocksdbsst.RocksDBSstSessions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.FakeObjects;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class RocksDBSessionsTest extends BaseRocksDBUnitTest {

    @Test
    public void testClearTablesWithoutRecreatingColumnFamilies()
            throws RocksDBException, InterruptedException {
        final String table2 = "test-table2";
        this.rocks.createTable(table2);
        this.rocks.session().put(TABLE, getBytes("person:1"),
                                 getBytes("James"));
        this.rocks.session().put(table2, getBytes("person:2"),
                                 getBytes("Tom"));
        this.commit();

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                this.rocks.clearTables(ImmutableList.of(TABLE, table2));
            } catch (Throwable e) {
                failure.set(e);
            }
        });
        thread.start();
        thread.join();

        Assert.assertNull(failure.get());
        Assert.assertNull(this.rocks.session().get(TABLE,
                                                  getBytes("person:1")));
        Assert.assertNull(this.rocks.session().get(table2,
                                                  getBytes("person:2")));
        Assert.assertTrue(this.rocks.existsTable(TABLE));
        Assert.assertTrue(this.rocks.existsTable(table2));
        AtomicInteger sessionCount =
                Whitebox.getInternalState(this.rocks, "sessionCount");
        Assert.assertEquals(1, sessionCount.get());
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
            assertNoResumeResidue(walPath);
        } finally {
            sessions.close();
            FileUtils.deleteDirectory(FileUtils.getFile(dataPath));
            FileUtils.deleteDirectory(FileUtils.getFile(walPath));
            FileUtils.deleteDirectory(FileUtils.getFile(walPath + ".resume-aside"));
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

    @Test
    public void testSeparateWalInstallsCheckpointTail() throws Exception {
        String dataPath = DB_PATH + "/tail-data";
        String walPath = DB_PATH + "/tail-wal";
        FileUtils.deleteDirectory(FileUtils.getFile(dataPath));
        FileUtils.deleteDirectory(FileUtils.getFile(walPath));
        FileUtils.deleteDirectory(FileUtils.getFile(walPath + ".resume-aside"));
        HugeConfig config = FakeObjects.newConfig();
        RocksDBSessions sessions = new RocksDBStdSessions(config, "db", "store",
                                                          dataPath, walPath);
        boolean open = true;
        try {
            sessions.createTable(TABLE);
            sessions.close();
            open = false;
            byte[] checkpoint = "checkpoint-tail".getBytes("UTF-8");
            byte[] live = "live-longer-tail".getBytes("UTF-8");
            FileUtils.forceMkdir(FileUtils.getFile(dataPath));
            FileUtils.forceMkdir(FileUtils.getFile(walPath));
            FileUtils.writeByteArrayToFile(new File(dataPath, "000123.log"),
                                           checkpoint);
            FileUtils.writeByteArrayToFile(new File(walPath, "000123.log"), live);
            FileUtils.writeByteArrayToFile(new File(walPath, "000124.log"),
                                           "later".getBytes("UTF-8"));

            Whitebox.invoke(RocksDBStdSessions.class,
                            "replaceSeparateWalDirectory", sessions);

            assertSameWalLogs(Collections.singletonMap("000123.log", checkpoint),
                              readWalLogs(walPath));
            Assert.assertFalse(new File(dataPath, "000123.log").exists());
            Assert.assertFalse(new File(walPath, "000124.log").exists());
            assertNoResumeResidue(walPath);
        } finally {
            if (open) {
                sessions.close();
            }
            FileUtils.deleteDirectory(FileUtils.getFile(dataPath));
            FileUtils.deleteDirectory(FileUtils.getFile(walPath));
            FileUtils.deleteDirectory(FileUtils.getFile(walPath + ".resume-aside"));
        }
    }


    @Test
    public void testDataInsideWalKeepsDataAndInstallsTail() throws Exception {
        String walPath = nestedRoot("contain-wal");
        String dataPath = walPath + "/data";
        FileUtils.deleteDirectory(FileUtils.getFile(walPath));
        FileUtils.forceMkdir(FileUtils.getFile(dataPath).getParentFile());
        HugeConfig config = FakeObjects.newConfig();
        RocksDBSessions sessions = new RocksDBStdSessions(config, "db", "store",
                                                          dataPath, walPath);
        boolean open = true;
        try {
            sessions.createTable(TABLE);
            sessions.close();
            open = false;
            byte[] checkpoint = "checkpoint-tail".getBytes("UTF-8");
            FileUtils.forceMkdir(FileUtils.getFile(dataPath));
            FileUtils.forceMkdir(FileUtils.getFile(walPath));
            FileUtils.writeByteArrayToFile(new File(dataPath, "000123.log"),
                                           checkpoint);
            FileUtils.writeByteArrayToFile(new File(walPath, "000123.log"),
                                           "live-longer-tail".getBytes("UTF-8"));
            FileUtils.writeByteArrayToFile(new File(walPath, "000124.log"),
                                           "later".getBytes("UTF-8"));
            File marker = new File(dataPath, "keep-data.txt");
            FileUtils.writeByteArrayToFile(marker, "kept".getBytes("UTF-8"));

            Whitebox.invoke(RocksDBStdSessions.class,
                            "replaceSeparateWalDirectory", sessions);

            assertSameWalLogs(Collections.singletonMap("000123.log", checkpoint),
                              readWalLogs(walPath));
            Assert.assertTrue(marker.isFile());
            Assert.assertFalse(new File(dataPath, "000123.log").exists());
            assertNoResumeResidue(walPath);
        } finally {
            if (open) {
                sessions.close();
            }
            FileUtils.deleteDirectory(FileUtils.getFile(walPath));
        }
    }

    @Test
    public void testWalInsideDataInstallsTail() throws Exception {
        String dataPath = nestedRoot("inside-data");
        String walPath = dataPath + "/wal";
        FileUtils.deleteDirectory(FileUtils.getFile(dataPath));
        FileUtils.forceMkdir(FileUtils.getFile(walPath));
        HugeConfig config = FakeObjects.newConfig();
        RocksDBSessions sessions = new RocksDBStdSessions(config, "db", "store",
                                                          dataPath, walPath);
        boolean open = true;
        try {
            sessions.createTable(TABLE);
            sessions.close();
            open = false;
            byte[] checkpoint = "checkpoint-tail".getBytes("UTF-8");
            FileUtils.writeByteArrayToFile(new File(dataPath, "000123.log"),
                                           checkpoint);
            FileUtils.writeByteArrayToFile(new File(walPath, "000124.log"),
                                           "later".getBytes("UTF-8"));

            Whitebox.invoke(RocksDBStdSessions.class,
                            "replaceSeparateWalDirectory", sessions);

            assertSameWalLogs(Collections.singletonMap("000123.log", checkpoint),
                              readWalLogs(walPath));
            Assert.assertFalse(new File(dataPath, "000123.log").exists());
            Assert.assertFalse(new File(walPath, "000124.log").exists());
            assertNoResumeResidue(walPath);
        } finally {
            if (open) {
                sessions.close();
            }
            FileUtils.deleteDirectory(FileUtils.getFile(dataPath));
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

    private static void assertSameWalLogs(Map<String, byte[]> expected,
                                          Map<String, byte[]> actual) {
        Assert.assertEquals(expected.keySet(), actual.keySet());
        for (Map.Entry<String, byte[]> entry : expected.entrySet()) {
            Assert.assertArrayEquals(entry.getValue(), actual.get(entry.getKey()));
        }
    }

    private static void assertNoResumeResidue(String walPath) {
        File wal = FileUtils.getFile(walPath);
        File[] children = wal.getParentFile().listFiles();
        Assert.assertNotNull(children);
        for (File child : children) {
            String name = child.getName();
            Assert.assertFalse(name.startsWith(wal.getName() + ".resume-aside-"));
            Assert.assertFalse(name.startsWith(wal.getName() + ".resume-staging-"));
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

        RocksDBSessions rocks =
                new RocksDBStdSessions(config, "db", "store", sstPath, sstPath);
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
