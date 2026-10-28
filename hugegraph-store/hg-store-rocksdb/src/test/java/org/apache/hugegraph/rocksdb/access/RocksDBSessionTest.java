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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

public class RocksDBSessionTest {

    private final String graphName = "testDummy";

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @BeforeClass
    public static void init() {
        OptionSpace.register("rocksdb",
                             "org.apache.hugegraph.rocksdb.access.RocksDBOptions");
        RocksDBOptions.instance();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("rocksdb.write_buffer_size", "1048576");
        configMap.put("rocksdb.bloom_filter_bits_per_key", "10");

        HugeConfig hConfig = new HugeConfig(new MapConfiguration(configMap));
        RocksDBFactory rFactory = RocksDBFactory.getInstance();
        rFactory.setHugeConfig(hConfig);
    }

    @Test
    public void resolveExistingPathPreservesVersionsAndConstructorStillPrunes() throws Exception {
        Path root = this.temporary.newFolder("versions").toPath();
        for (String name : new String[]{"db", "db_2", "db_2_3", "db_4_1", "db_99_temp_"}) {
            writeNative(root.resolve(name), bytes(name));
        }
        Map<String, String> before = fileHashes(root);
        assertEquals(root.resolve("db_4_1").toString(),
                     RocksDBSession.resolveExistingDBPath(root.toString(), "db", 5L));
        assertEquals(before, fileHashes(root));
        assertArrayEquals(bytes("db_4_1"), readNative(root.resolve("db_4_1")));
        try (RocksDBSession session = new RocksDBSession(smallConfig(), root.toString(), "db", 5L)) {
            assertEquals(root.resolve("db_4_1").toString(), session.getDbPath());
            assertArrayEquals(bytes("db_4_1"), session.getDB().get(bytes("key")));
            assertFalse(Files.exists(root.resolve("db")));
            assertFalse(Files.exists(root.resolve("db_2")));
            assertFalse(Files.exists(root.resolve("db_2_3")));
            assertTrue(Files.exists(root.resolve("db_99_temp_")));
        }
    }

    @Test
    public void resolveNewPathDoesNotCreateDatabase() throws Exception {
        Path root = this.temporary.newFolder("missing").toPath();
        Path missing = root.resolve("not-created");
        assertNull(RocksDBSession.resolveExistingDBPath(missing.toString(), "db", 0L));
        assertFalse(Files.exists(missing));
        Files.createDirectory(root.resolve("db"));
        assertNull(RocksDBSession.resolveExistingDBPath(root.toString(), "db", 0L));
        assertTrue(Files.isDirectory(root.resolve("db")));
        assertFalse(Files.exists(root.resolve("db/CURRENT")));
    }

    @Test
    public void resolveIncompleteNewerVersionPreservesExistingDatabase() throws Exception {
        Path root = this.temporary.newFolder("incomplete").toPath();
        writeNative(root.resolve("db_1"), bytes("existing"));
        Files.createDirectory(root.resolve("db_2"));
        Map<String, String> before = fileHashes(root);
        assertThrows(DBStoreException.class,
                     () -> RocksDBSession.resolveExistingDBPath(root.toString(), "db", 2L));
        assertTrue(Files.isDirectory(root.resolve("db_2")));
        assertEquals(before, fileHashes(root));
        assertArrayEquals(bytes("existing"), readNative(root.resolve("db_1")));
    }

    @Test
    public void resolveMissingCurrentDoesNotTreatCheckpointAsNewDatabase() throws Exception {
        Path root = this.temporary.newFolder("missing-current-checkpoint").toPath();
        Path source = root.resolve("producer");
        Path database = root.resolve("db");
        writeNative(source, bytes("existing"));
        createNativeSnapshot(source, database);
        assertArrayEquals(bytes("existing"), readNative(database));
        Files.delete(database.resolve("CURRENT"));
        Map<String, String> before = fileHashes(root);

        DBStoreException failure = assertThrows(DBStoreException.class,
                () -> RocksDBSession.resolveExistingDBPath(root.toString(), "db", 0L));

        assertTrue(failure.getMessage(), failure.getMessage().contains("no CURRENT"));
        assertTrue(failure.getMessage(), failure.getMessage().contains(database.toString()));
        assertTrue(failure.getMessage(), failure.getMessage().contains("#incomplete-database-directories"));
        assertEquals(before, fileHashes(root));
        assertArrayEquals(bytes("existing"), readNative(source));
    }

    @Test
    public void resolveRejectsNonemptyRetainedVersionWithoutCurrent() throws Exception {
        Path root = this.temporary.newFolder("retained-missing-current").toPath();
        writeNative(root.resolve("db_1"), bytes("retained"));
        writeNative(root.resolve("db_2"), bytes("latest"));
        Files.delete(root.resolve("db_1/CURRENT"));
        Map<String, String> before = fileHashes(root);

        DBStoreException failure = assertThrows(DBStoreException.class,
                () -> RocksDBSession.resolveExistingDBPath(root.toString(), "db", 2L));

        assertTrue(failure.getMessage(), failure.getMessage().contains(root.resolve("db_1").toString()));
        assertTrue(failure.getMessage(), failure.getMessage().contains("#incomplete-database-directories"));
        assertEquals(before, fileHashes(root));
        assertArrayEquals(bytes("latest"), readNative(root.resolve("db_2")));
    }

    @Test
    public void resolveRejectsUnreadableParent() throws Exception {
        Path root = this.temporary.newFolder("unreadable-parent").toPath();
        assertRestrictedPathRejected(root, root, "-wx------", root);
    }

    @Test
    public void resolveRejectsUnsearchableDatabase() throws Exception {
        Path root = this.temporary.newFolder("unsearchable-db").toPath();
        assertRestrictedPathRejected(root, root.resolve("db_2"), "r--------", root);
    }

    @Test
    public void resolveRejectsUnknownMissingPathUnderDeniedAncestor() throws Exception {
        Path root = this.temporary.newFolder("denied-ancestor").toPath();
        assertRestrictedPathRejected(root, root, "---------", root.resolve("missing"));
    }

    private static void assertRestrictedPathRejected(Path root, Path restricted,
                                                    String mode, Path requested) throws Exception {
        Assume.assumeTrue("POSIX permissions are required for the access regression",
                          Files.getFileStore(root).supportsFileAttributeView("posix"));
        Path database = root.resolve("db_2");
        writeNative(database, bytes("existing"));
        Map<String, String> before = fileHashes(root);
        Set<PosixFilePermission> original = Files.getPosixFilePermissions(restricted);
        try {
            Files.setPosixFilePermissions(restricted, PosixFilePermissions.fromString(mode));
            boolean denied = restricted.equals(database) ? !Files.isExecutable(restricted) :
                             !Files.isReadable(restricted);
            Assume.assumeTrue("Permission denial is required; root DAC override may bypass it", denied);
            DBStoreException failure = assertThrows(DBStoreException.class,
                    () -> RocksDBSession.resolveExistingDBPath(requested.toString(), "db", 2L));
            assertTrue(failure.getCause() instanceof IOException);
        } finally {
            Files.setPosixFilePermissions(restricted, original);
        }
        assertEquals(before, fileHashes(root));
        assertArrayEquals(bytes("existing"), readNative(database));
        assertFalse(Files.exists(root.resolve("db")));
        assertFalse(Files.exists(root.resolve("missing")));
    }

    @Test
    public void rejectedSnapshotPreservesActiveVersionAndStaging() throws Exception {
        Path root = this.temporary.newFolder("rejected").toPath();
        Path snapshot = root.resolve("snapshot");
        writeNative(root.resolve("producer"), bytes("candidate"));
        createNativeSnapshot(root.resolve("producer"), snapshot);
        Path target = root.resolve("db_20");
        Path staging = root.resolve("db_20_temp_");
        IllegalStateException rejection = new IllegalStateException("candidate content rejected");
        AtomicInteger validated = new AtomicInteger();
        AtomicReference<Map<String, String>> stagedBefore = new AtomicReference<>();
        try (RocksDBSession active = new RocksDBSession(smallConfig(), root.toString(), "db", 0L)) {
            active.getDB().put(bytes("key"), bytes("active"));
            writeNative(target, bytes("previous-version"));
            String activePath = active.getDbPath();
            long sequence = active.getLatestSequenceNumber();
            Map<String, String> activeBefore = fileHashes(Path.of(activePath));
            Map<String, String> targetBefore = fileHashes(target);
            Map<String, String> snapshotBefore = fileHashes(snapshot);
            DBStoreException failed = assertThrows(DBStoreException.class, () ->
                    active.loadSnapshot(snapshot.toString(), 20L, path -> {
                        assertEquals(staging.toString(), path);
                        try {
                            assertArrayEquals(bytes("candidate"), readNative(Path.of(path)));
                            stagedBefore.set(fileHashes(Path.of(path)));
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                        validated.incrementAndGet();
                        throw rejection;
                    }));
            assertSame(rejection, failed.getCause());
            assertEquals(1, validated.get());
            assertEquals(activePath, active.getDbPath());
            assertEquals(sequence, active.getLatestSequenceNumber());
            assertArrayEquals(bytes("active"), active.getDB().get(bytes("key")));
            assertEquals(activeBefore, fileHashes(Path.of(activePath)));
            assertEquals(targetBefore, fileHashes(target));
            assertEquals(snapshotBefore, fileHashes(snapshot));
            assertArrayEquals(bytes("previous-version"), readNative(target));
            assertArrayEquals(bytes("candidate"), readNative(staging));
            assertEquals(stagedBefore.get(), fileHashes(staging));
        }
    }

    @Test
    public void acceptedSnapshotAndOriginalOverloadActivateNativeData() throws Exception {
        Path root = this.temporary.newFolder("accepted").toPath();
        Path snapshot = root.resolve("snapshot");
        writeNative(root.resolve("producer"), bytes("candidate"));
        createNativeSnapshot(root.resolve("producer"), snapshot);
        AtomicInteger validated = new AtomicInteger();
        try (RocksDBSession active = new RocksDBSession(smallConfig(), root.toString(), "db", 0L)) {
            active.loadSnapshot(snapshot.toString(), 20L, path -> {
                try {
                    assertArrayEquals(bytes("candidate"), readNative(Path.of(path)));
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                validated.incrementAndGet();
            });
            assertEquals(1, validated.get());
            assertFalse(Files.exists(root.resolve("db_20_temp_")));
            assertArrayEquals(bytes("candidate"), readNative(root.resolve("db_20")));
            active.loadSnapshot(snapshot.toString(), 21L);
            assertArrayEquals(bytes("candidate"), readNative(root.resolve("db_21")));
        }
        try (RocksDBSession reopened = new RocksDBSession(smallConfig(), root.toString(), "db", 21L)) {
            assertEquals(root.resolve("db_21").toString(), reopened.getDbPath());
            assertArrayEquals(bytes("candidate"), reopened.getDB().get(bytes("key")));
        }
    }

    private static HugeConfig smallConfig() {
        Map<String, Object> values = new HashMap<>();
        values.put("rocksdb.optimize_mode", false);
        values.put("rocksdb.write_buffer_size", 65536L);
        values.put("rocksdb.max_background_jobs", 1);
        values.put("rocksdb.max_subcompactions", 1);
        values.put("rocksdb.log_level", "ERROR");
        return new HugeConfig(new MapConfiguration(values));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void writeNative(Path path, byte[] value) throws Exception {
        try (Options options = new Options().setCreateIfMissing(true).setWriteBufferSize(65536L);
             RocksDB db = RocksDB.open(options, path.toString())) {
            db.put(bytes("key"), value);
        }
    }

    private static void createNativeSnapshot(Path source, Path snapshot) throws Exception {
        try (Options options = new Options();
             RocksDB db = RocksDB.open(options, source.toString());
             Checkpoint checkpoint = Checkpoint.create(db)) {
            checkpoint.createCheckpoint(snapshot.toString());
        }
    }

    private static byte[] readNative(Path path) throws Exception {
        try (Options options = new Options();
             ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
             DBOptions dbOptions = new DBOptions();
             Logger logger = new Logger(dbOptions) {
                 @Override
                 protected void log(InfoLogLevel level, String message) {
                     // Native readback must not create or replace the DB's LOG file.
                 }
             }) {
            dbOptions.setLogger(logger);
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            for (byte[] name : RocksDB.listColumnFamilies(options, path.toString())) {
                descriptors.add(new ColumnFamilyDescriptor(name, cfOptions));
            }
            List<ColumnFamilyHandle> handles = new ArrayList<>();
            try (RocksDB db = RocksDB.openReadOnly(dbOptions, path.toString(), descriptors, handles)) {
                try {
                    return db.get(handles.get(0), bytes("key"));
                } finally {
                    for (ColumnFamilyHandle handle : handles) {
                        handle.close();
                    }
                }
            }
        }
    }

    private static Map<String, String> fileHashes(Path root) throws Exception {
        Map<String, String> hashes = new TreeMap<>();
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.filter(Files::isRegularFile).collect(java.util.stream.Collectors.toList())) {
                byte[] digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path));
                StringBuilder encoded = new StringBuilder();
                for (byte value : digest) {
                    encoded.append(String.format("%02x", value));
                }
                hashes.put(root.relativize(path).toString(), encoded.toString());
            }
        }
        return hashes;
    }

    private static byte[] intToBytesForPartId(int v) {
        short s = (short) v;
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN);
        buffer.putShort(s);
        return buffer.array();
    }

    public static byte[] byteCompose(byte[] b1, byte[] b2) {
        byte[] b3 = new byte[b1.length + b2.length];
        System.arraycopy(b1, 0, b3, 0, b1.length);
        System.arraycopy(b2, 0, b3, b1.length, b2.length);
        return b3;
    }

    private static byte[] keyAppendPartId(int partId, byte[] key) {
        byte[] partBytes = intToBytesForPartId(partId);
        byte[] targetKey = byteCompose(partBytes, key);
        return targetKey;
    }

    private static byte[] getPartStartKey(int partId) {
        return intToBytesForPartId(partId);
    }

    private static byte[] getPartEndKey(int partId) {
        return intToBytesForPartId(partId + 1);
    }

    //    @Test
    public void put() {
        System.out.println("RocksDBSessionTest::put test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        String tName = "t1";
        rSession.checkTable(tName);

        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.put(tName, "a1".getBytes(), "f1".getBytes());
        sessionOp.commit();

        String vRet = new String(rSession.sessionOp().get(tName, "a1".getBytes()));
        assertEquals(vRet, "f1");
    }

    // @Test
    public void selectTable() {

        System.out.println("RocksDBSessionTest::selectTable test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        String tName = "t2";
        rSession.checkTable(tName);

        assertTrue(rSession.tableIsExist(tName));

    }

    // @Test
    public void batchPut() {
        System.out.println("RocksDBSessionTest::batchPut test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        String tName = "t3";
        String tName2 = "t4";

        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.put(tName, "a1".getBytes(), "f1".getBytes());
        sessionOp.put(tName, "a2".getBytes(), "f2".getBytes());
        sessionOp.put(tName, "a3".getBytes(), "f3".getBytes());
        sessionOp.put(tName, "a4".getBytes(), "f4".getBytes());
        sessionOp.put(tName, "a5".getBytes(), "f5".getBytes());
        sessionOp.put(tName, "a6".getBytes(), "f6".getBytes());
        sessionOp.commit();

        sessionOp.put(tName2, "m1".getBytes(), "k1".getBytes());
        sessionOp.put(tName2, "m2".getBytes(), "k2".getBytes());
        sessionOp.put(tName2, "m3".getBytes(), "k3".getBytes());
        sessionOp.put(tName, "a7".getBytes(), "f7".getBytes());
        sessionOp.commit();

        String vRet1 = new String(rSession.sessionOp().get(tName, "a1".getBytes()));
        String vRet2 = new String(rSession.sessionOp().get(tName2, "m1".getBytes()));
        String vRet3 = new String(rSession.sessionOp().get(tName, "a7".getBytes()));

        assertEquals(vRet1, "f1");
        assertEquals(vRet2, "k1");
        assertEquals(vRet3, "f7");

    }

    // @Test
    public void get() {
        System.out.println("RocksDBSessionTest::get test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        String tName = "t5";
        rSession.checkTable(tName);
        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.put(tName, "hash".getBytes(), "youareok".getBytes());
        sessionOp.commit();

        String vRet = new String(rSession.sessionOp().get(tName, "hash".getBytes()));
        assertEquals(vRet, "youareok");
    }

    // @Test
    public void createTables() {
        System.out.println("RocksDBSessionTest::createTables test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        rSession.createTables("fly1", "fly2", "fly3");

        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.put("fly1", "cat1".getBytes(), "hit1".getBytes());
        sessionOp.put("fly2", "cat2".getBytes(), "hit2".getBytes());
        sessionOp.put("fly3", "cat3".getBytes(), "hit3".getBytes());
        sessionOp.commit();

        assertTrue(rSession.tableIsExist("fly1"));
        assertTrue(rSession.tableIsExist("fly2"));
        assertTrue(rSession.tableIsExist("fly3"));

    }

    // @Test
    public void dropTables() {
        System.out.println("RocksDBSessionTest::dropTables test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        rSession.createTables("dummy1");

        assertTrue(rSession.tableIsExist("dummy1"));

        rSession.dropTables("dummy1");

        assertFalse(rSession.tableIsExist("dummy1"));

    }

    // @Test
    public void scan() {
        System.out.println("RocksDBSessionTest::scan test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();
        String graph1 = "tBatch";
        rFactory.createGraphDB("./tmp", graphName);
        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graph1);

        String tName = "t6";
        rSession.checkTable(tName);

        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.put(tName, "box1".getBytes(), "gift1".getBytes());
        sessionOp.put(tName, "box2".getBytes(), "gift2".getBytes());
        sessionOp.put(tName, "box3".getBytes(), "gift3".getBytes());
        sessionOp.put(tName, "box4".getBytes(), "gift4".getBytes());
        sessionOp.put(tName, "box5".getBytes(), "gift5".getBytes());
        sessionOp.put(tName, "box6".getBytes(), "gift6".getBytes());
        sessionOp.put(tName, "box7".getBytes(), "gift7".getBytes());
        sessionOp.put(tName, "box8".getBytes(), "gift8".getBytes());
        sessionOp.commit();

        //scan table
        ScanIterator it = sessionOp.scan(tName);
        while (it.hasNext()) {
            RocksDBSession.BackendColumn col = it.next();
            System.out.println(new String(col.name) + " : " + new String(col.value));
        }

        long c = rSession.getApproximateDataSize();
        System.out.println(c);

        long c1 = RocksDBFactory.getInstance().getTotalSize();
        System.out.println(c1);
    }

    // @Test
    public void testScan() {
        System.out.println("RocksDBSessionTest::testScan test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        String tName = "t7";
        rSession.checkTable(tName);

        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.put(tName, "box1".getBytes(), "gift1".getBytes());
        sessionOp.put(tName, "box2".getBytes(), "gift2".getBytes());
        sessionOp.put(tName, "box3".getBytes(), "gift3".getBytes());
        sessionOp.put(tName, "room1".getBytes(), "killer1".getBytes());
        sessionOp.put(tName, "box4".getBytes(), "gift4".getBytes());
        sessionOp.put(tName, "box5".getBytes(), "gift5".getBytes());
        sessionOp.put(tName, "room2".getBytes(), "killer2".getBytes());
        sessionOp.put(tName, "box6".getBytes(), "gift6".getBytes());
        sessionOp.put(tName, "boat1".getBytes(), "girl1".getBytes());
        sessionOp.put(tName, "box7".getBytes(), "gift7".getBytes());
        sessionOp.put(tName, "box8".getBytes(), "gift8".getBytes());
        sessionOp.commit();

        //prefix scan
        ScanIterator it = sessionOp.scan(tName, "bo".getBytes());
        while (it.hasNext()) {
            RocksDBSession.BackendColumn col = it.next();
            System.out.println(new String(col.name) + " : " + new String(col.value));
        }

    }

    // @Test
    public void testScan1() {
        System.out.println("RocksDBSessionTest::testScan1 test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        String tName = "t8";
        rSession.checkTable(tName);

        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.put(tName, "box1".getBytes(), "gift1".getBytes());
        sessionOp.put(tName, "box2".getBytes(), "gift2".getBytes());
        sessionOp.put(tName, "box3".getBytes(), "gift3".getBytes());
        sessionOp.put(tName, "room1".getBytes(), "killer1".getBytes());
        sessionOp.put(tName, "box4".getBytes(), "gift4".getBytes());
        sessionOp.put(tName, "box5".getBytes(), "gift5".getBytes());
        sessionOp.put(tName, "room2".getBytes(), "killer2".getBytes());
        sessionOp.put(tName, "box6".getBytes(), "gift6".getBytes());
        sessionOp.put(tName, "boat1".getBytes(), "girl1".getBytes());
        sessionOp.put(tName, "box7".getBytes(), "gift7".getBytes());
        sessionOp.put(tName, "box8".getBytes(), "gift8".getBytes());
        sessionOp.commit();

        //range scan
        ScanIterator it = sessionOp.scan(tName, "box2".getBytes(), "box5".getBytes(),
                                         ScanIterator.Trait.SCAN_GTE_BEGIN |
                                         ScanIterator.Trait.SCAN_LTE_END);
        while (it.hasNext()) {
            RocksDBSession.BackendColumn col = it.next();
            System.out.println(new String(col.name) + " : " + new String(col.value));
        }

    }

    // @Test
    public void testCount() {
        System.out.println("RocksDBSessionTest::testCount test");
        RocksDBFactory rFactory = RocksDBFactory.getInstance();
        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);
        String tName = "c1";
        rSession.checkTable(tName);

        int part1 = 1;
        int part2 = 2;
        int part3 = 3;
        SessionOperator sessionOp = rSession.sessionOp();
        sessionOp.prepare();
        sessionOp.put(tName, keyAppendPartId(part1, "k1".getBytes()), "v1".getBytes());
        sessionOp.put(tName, keyAppendPartId(part1, "k2".getBytes()), "v2".getBytes());
        sessionOp.put(tName, keyAppendPartId(part1, "k3".getBytes()), "v3".getBytes());
        sessionOp.put(tName, intToBytesForPartId(part2), "v".getBytes());
        sessionOp.put(tName, keyAppendPartId(part2, "k4".getBytes()), "v4".getBytes());
        sessionOp.put(tName, keyAppendPartId(part2, "k5".getBytes()), "v5".getBytes());
        sessionOp.put(tName, keyAppendPartId(part2, "k6".getBytes()), "v6".getBytes());
        sessionOp.put(tName, intToBytesForPartId(part3), "v".getBytes());
        sessionOp.put(tName, keyAppendPartId(part3, "k8".getBytes()), "v8".getBytes());
        sessionOp.put(tName, keyAppendPartId(part3, "k9".getBytes()), "v9".getBytes());
        sessionOp.put(tName, keyAppendPartId(part3, "k10".getBytes()), "v10".getBytes());
        sessionOp.put(tName, keyAppendPartId(part3, "k11".getBytes()), "v11".getBytes());
        sessionOp.commit();

//        assertEquals(3, rSession.sessionOp().keyCount(tName, getPartStartKey(part1),
//        getPartEndKey(part1)));
//        assertEquals(4, rSession.sessionOp().keyCount(tName, getPartStartKey(part2),
//        getPartEndKey(part2)));
//        assertEquals(5, rSession.sessionOp().keyCount(tName, getPartStartKey(part3),
//        getPartEndKey(part3)));

        assertEquals(12, rSession.sessionOp().keyCount("\0".getBytes(StandardCharsets.UTF_8),
                                                       "\255".getBytes(StandardCharsets.UTF_8),
                                                       tName));
        assertEquals(12, rSession.sessionOp().estimatedKeyCount(tName));
    }

    // @Test
    public void merge() {
        System.out.println("RocksDBSessionTest::merge test");

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        RocksDBSession rSession = rFactory.createGraphDB("./tmp", graphName);

        String tName = "room1";
        rSession.checkTable(tName);
        byte[] x2 = longToByteArray(19);
        System.out.println(x2);
        SessionOperator sessionOp = rSession.sessionOp();
        byte[] x1 = sessionOp.get(tName, "p3".getBytes());
        String str1 = new String(x1);
        if (x1 != null) {
            long curValue = longFromByteArray(x1);
            System.out.println("current value:" + curValue);
            sessionOp.merge(tName, "p3".getBytes(), longToByteArray(10));
            sessionOp.commit();
        }

        byte[] value = rSession.sessionOp().get(tName, "p3".getBytes());
        final long longValue = longFromByteArray(value);
        System.out.println("after merge value: " + longValue);

        sessionOp.put(tName, "p3".getBytes(), "19".getBytes());
        sessionOp.commit();

        ScanIterator it = sessionOp.scan(tName, "p".getBytes());
        System.out.println("after put ------>");
        while (it.hasNext()) {
            RocksDBSession.BackendColumn col = it.next();
            System.out.println(new String(col.name) + ":" + new String(col.value));
        }

    }

    // @Test
    public void truncate() {
        System.out.println("RocksDBSessionTest::truncate test");
        RocksDBFactory rFactory = RocksDBFactory.getInstance();
        RocksDBSession rSession = rFactory.createGraphDB("./tmp", "gg1");
        rSession.checkTable("f1");
        SessionOperator op = rSession.sessionOp();
        op.put("f1", "m1".getBytes(), "n1".getBytes());
        op.put("f1", "m2".getBytes(), "n2".getBytes());
        op.put("f1", "m3".getBytes(), "n3".getBytes());
        op.commit();
        rSession.checkTable("f2");

        RocksDBSession rocksDBSession2 = rFactory.createGraphDB("./tmp", "gg2");
        rocksDBSession2.checkTable("txt");

        rSession.truncate();
        op = rSession.sessionOp();
        op.put("f1", "beijing".getBytes(), "renkou".getBytes());
        op.commit();

        rFactory.releaseAllGraphDB();
    }

    // @Test
    public void batchPut2() {
        RocksDBFactory rFactory = RocksDBFactory.getInstance();
        RocksDBSession rSession = rFactory.createGraphDB("./tmp", "gg1");
        rSession.checkTable("f1");
        SessionOperator op = rSession.sessionOp();
        try {
            op.put("f1", "m2".getBytes(), "xx2".getBytes());
            op.put("f1", "m1".getBytes(), "xx1".getBytes());
            op.put("f1", "m3".getBytes(), "xx3".getBytes());
            op.put("f1", "m4".getBytes(), "xx5".getBytes());
            op.commit();
            op.deleteRange("f1", "m5".getBytes(), "m2".getBytes());

            op.put("f2", new byte[]{1, 1}, new byte[]{1, -3});
            op.put("f2", new byte[]{1, -2}, new byte[]{2, 0});
            op.put("f2", new byte[]{1, 10}, new byte[]{1, 5});
            op.put("f2", new byte[]{1, 9}, new byte[]{1, 3});
            op.commit();

            op.deleteRange("f2", new byte[]{1, 1}, new byte[]{1, -1});
            op.commit();

            ScanIterator it = op.scan("f2");
            while (it.hasNext()) {
                RocksDBSession.BackendColumn col = it.next();
                System.out.println(Arrays.toString(col.name) + "=>" + Arrays.toString(col.value));
            }

        } catch (DBStoreException e) {
            System.out.println(e);
        }

    }

    // @Test
    public void doStuff() {
        byte[] stuff = new byte[]{1, 3, 1, -3};
        System.out.println(Arrays.toString(stuff));
    }

    private byte[] longToByteArray(long l) {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(l);
        return buf.array();
    }

    private long longFromByteArray(byte[] a) {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(a);
        buf.flip();
        return buf.getLong();
    }

}
