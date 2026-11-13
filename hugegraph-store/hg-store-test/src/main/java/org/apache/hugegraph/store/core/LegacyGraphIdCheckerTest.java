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

package org.apache.hugegraph.store.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.meta.MetadataKeyHelper;
import org.apache.hugegraph.store.upgrade.LegacyGraphIdChecker;
import org.apache.hugegraph.store.util.HgStoreException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import com.google.protobuf.Int64Value;

public class LegacyGraphIdCheckerTest {

    private static final String GRAPH = "DEFAULT/hugegraph/g";
    private static final byte[] LEGACY_KEY = {(byte) 0xFF, (byte) 0xFE, 1, 0, 0};
    private static final byte[] LEGAL_KEY = {(byte) 0xFF, (byte) 0xFD, 1, 0, 0};
    private static final byte[] VALUE = {1, 2, 3};
    private static HugeConfig config;

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @BeforeClass
    public static void init() {
        RocksDB.loadLibrary();
        config = LegacyGraphIdChecker.configuration(Map.of(
                "rocksdb.optimize_mode", false,
                "rocksdb.write_buffer_size", 1048576L));
    }

    @Test
    public void testLegalHighIdAndMappingLeaveDatabaseUnchanged() throws Exception {
        Path database = this.temporary.newFolder().toPath();
        write(database, "default", MetadataKeyHelper.getGraphIDKey(GRAPH),
              Int64Value.of(65533).toByteArray());
        write(database, "g+v", LEGAL_KEY, VALUE);
        Map<String, String> before = hashes(database);
        LegacyGraphIdChecker.checkDatabase(database, config);
        assertEquals(before, hashes(database));
    }

    @Test
    public void testLegacyPrefixInEveryColumnFamilyLeavesDatabaseUnchanged() throws Exception {
        for (String table : List.of("default", "g+v", "g+oe", "g+ie", "g+index",
                                    "g+olap", "g+task", "g+server", "custom-table")) {
            Path database = this.temporary.newFolder().toPath();
            write(database, table, LEGACY_KEY, VALUE);
            Map<String, String> before = hashes(database);
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> LegacyGraphIdChecker.checkDatabase(database, config));
            assertTrue(failure.getMessage(), failure.getMessage().contains("0xFFFE"));
            assertTrue(failure.getMessage(), failure.getMessage().contains(table));
            assertEquals(before, hashes(database));
        }
    }

    @Test
    public void testInvalidMappingWithoutBusinessRowsIsRejected() throws Exception {
        for (long id : new long[]{-1L, 65534L, 65535L, Long.MAX_VALUE}) {
            Path database = this.temporary.newFolder().toPath();
            write(database, "default", MetadataKeyHelper.getGraphIDKey(GRAPH),
                  Int64Value.of(id).toByteArray());
            Map<String, String> before = hashes(database);
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> LegacyGraphIdChecker.checkDatabase(database, config));
            assertTrue(failure.getMessage(), failure.getMessage().contains("Invalid GraphID metadata"));
            assertEquals(before, hashes(database));
        }
    }

    @Test
    public void testDuplicateMappingsWithoutBusinessRowsAreRejected() throws Exception {
        Path database = this.temporary.newFolder().toPath();
        write(database, "default", MetadataKeyHelper.getGraphIDKey(GRAPH), Int64Value.of(7).toByteArray());
        write(database, "default", MetadataKeyHelper.getGraphIDKey("DEFAULT/other/g"),
              Int64Value.of(7).toByteArray());
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> LegacyGraphIdChecker.checkDatabase(database, config));
        assertTrue(failure.getMessage(), failure.getMessage().contains("multiple graphs"));
    }

    @Test
    public void testMalformedMappingValueIsRejectedWithoutModification() throws Exception {
        Path database = this.temporary.newFolder().toPath();
        write(database, "default", MetadataKeyHelper.getGraphIDKey(GRAPH), new byte[]{8});
        Map<String, String> before = hashes(database);
        assertThrows(IOException.class, () -> LegacyGraphIdChecker.checkDatabase(database, config));
        assertEquals(before, hashes(database));
    }

    @Test
    public void testInvalidUtf8GraphNamesCannotHideInvalidMapping() throws Exception {
        Path database = this.temporary.newFolder().toPath();
        byte[] prefix = MetadataKeyHelper.getGraphIDKey("");
        byte[] first = Arrays.copyOf(prefix, prefix.length + 1);
        byte[] second = Arrays.copyOf(first, first.length);
        first[prefix.length] = (byte) 0x80;
        second[prefix.length] = (byte) 0x81;
        // Both suffixes decode to the replacement character; a String-keyed map loses the first value.
        write(database, "default", first, Int64Value.of(65534).toByteArray());
        write(database, "default", second, Int64Value.of(1).toByteArray());
        Map<String, String> before = hashes(database);
        assertThrows(IllegalStateException.class, () -> LegacyGraphIdChecker.checkDatabase(database, config));
        assertEquals(before, hashes(database));
    }

    @Test
    public void testRetainedDatabaseVersionIsCheckedWithoutPruning() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        Path retained = root.resolve("db/00000");
        write(retained, "g+v", LEGACY_KEY, VALUE);
        write(root.resolve("db/00000_42"), "g+v", LEGAL_KEY, VALUE);
        Map<String, String> before = hashes(root);
        HgStoreException failure = assertThrows(HgStoreException.class,
                () -> LegacyGraphIdChecker.checkStore(root.toString(), config));
        assertTrue(failure.getMessage(), failure.getMessage().contains(retained.toString()));
        assertEquals(before, hashes(root));
    }

    @Test
    public void testIncompleteRetainedDirectoryNeedsExplicitRecoveryWithoutPruning() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        Path retained = root.resolve("db/00000");
        write(retained, "g+v", LEGAL_KEY, VALUE);
        write(root.resolve("db/00000_42"), "g+v", LEGAL_KEY, VALUE);
        Files.delete(retained.resolve("CURRENT"));
        Map<String, String> before = hashes(root);

        HgStoreException failure = assertThrows(HgStoreException.class,
                () -> LegacyGraphIdChecker.checkStore(root.toString(), config));
        assertTrue(failure.getMessage(), failure.getMessage().contains(retained.toString()));
        assertTrue(failure.getMessage(), failure.getMessage().contains("CURRENT is missing"));
        assertTrue(failure.getMessage(), failure.getMessage().contains("#incomplete-database-directories"));
        assertEquals(before, hashes(root));
    }

    @Test
    public void testIncompleteLatestDirectoryIsNeverIgnored() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        write(root.resolve("db/00000"), "g+v", LEGAL_KEY, VALUE);
        Path latest = root.resolve("db/00000_42");
        write(latest, "g+v", LEGAL_KEY, VALUE);
        Files.delete(latest.resolve("CURRENT"));
        Map<String, String> before = hashes(root);

        HgStoreException failure = assertThrows(HgStoreException.class,
                () -> LegacyGraphIdChecker.checkStore(root.toString(), config));
        assertTrue(failure.getMessage(), failure.getMessage().contains(latest.toString()));
        assertEquals(before, hashes(root));
    }

    @Test
    public void testIncompleteMetadataDirectoryIsNeverIgnored() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        Path stored = this.temporary.newFolder().toPath();
        write(stored.resolve("db/00007"), "g+oe", LEGACY_KEY, VALUE);
        storeLocation(root, stored, 7);
        IllegalStateException legacy = assertThrows(IllegalStateException.class,
                () -> LegacyGraphIdChecker.checkOffline(root.toString(), config));
        assertTrue(legacy.getMessage(), legacy.getMessage().contains(stored.toString()));

        Path metadata = root.resolve("hgstore-metadata");
        // Check missing CURRENT alone, then CURRENT and LOCK together: diagnose before locking.
        for (String file : List.of("CURRENT", "LOCK")) {
            Files.delete(metadata.resolve(file));
            Map<String, String> before = hashes(this.temporary.getRoot().toPath());
            IOException offline = assertThrows(IOException.class,
                    () -> LegacyGraphIdChecker.checkOffline(root.toString(), config));
            LegacyGraphIdChecker.StartupPreflightException startup = assertThrows(
                    LegacyGraphIdChecker.StartupPreflightException.class,
                    () -> LegacyGraphIdChecker.checkStore(root.toString(), config));
            for (Exception failure : List.of(offline, startup)) {
                assertTrue(failure.getMessage(), failure.getMessage().contains(metadata.toString()));
                assertTrue(failure.getMessage(), failure.getMessage().contains("CURRENT is missing"));
                assertTrue(failure.getMessage(), failure.getMessage().contains("#incomplete-database-directories"));
            }
            assertEquals(before, hashes(this.temporary.getRoot().toPath()));
        }
    }

    @Test
    public void testFreshDataRootWithoutMetadataIsAccepted() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        LegacyGraphIdChecker.InspectionResult result =
                LegacyGraphIdChecker.checkOffline(root.toString(), config);
        assertEquals(0, result.databases());
        assertEquals(0, result.snapshots());
        assertFalse(result.metadata());
        LegacyGraphIdChecker.checkStore(root + ",", config);
        assertFalse(Files.exists(root.resolve("hgstore-metadata")));
        assertTrue(hashes(root).isEmpty());
    }

    @Test
    public void testStartupChecksCheckpointWithoutLock() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        Path database = root.resolve("db/00000_42");
        createCheckpoint(database, LEGAL_KEY);
        assertFalse(Files.exists(database.resolve("LOCK")));
        Map<String, String> before = hashes(root);

        LegacyGraphIdChecker.checkStore(root.toString(), config);
        assertTrue(Files.isRegularFile(database.resolve("LOCK")));
        assertEquals(0, Files.size(database.resolve("LOCK")));
        Files.delete(database.resolve("LOCK"));
        assertEquals(before, hashes(root));
    }

    @Test
    public void testStartupRejectsLegacyCheckpointWithoutLock() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        Path database = root.resolve("db/00000_42");
        createCheckpoint(database, LEGACY_KEY);
        assertFalse(Files.exists(database.resolve("LOCK")));
        Map<String, String> before = hashes(root);

        assertThrows(LegacyGraphIdChecker.StartupPreflightException.class,
                     () -> LegacyGraphIdChecker.checkStore(root.toString(), config));
        assertTrue(Files.isRegularFile(database.resolve("LOCK")));
        assertEquals(0, Files.size(database.resolve("LOCK")));
        Files.delete(database.resolve("LOCK"));
        assertEquals(before, hashes(root));
    }

    @Test
    public void testDanglingMetadataLinkIsRejected() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        Path metadata = root.resolve("hgstore-metadata");
        Path missing = root.resolve("missing-metadata");
        Files.createSymbolicLink(metadata, missing);
        IOException failure = assertThrows(IOException.class,
                () -> LegacyGraphIdChecker.checkOffline(root.toString(), config));
        assertTrue(failure.getMessage(), failure.getMessage().contains(metadata.toString()));
        assertTrue(failure.getMessage(), failure.getMessage().contains("CURRENT is missing"));
        assertThrows(LegacyGraphIdChecker.StartupPreflightException.class,
                () -> LegacyGraphIdChecker.checkStore(root.toString(), config));
        assertEquals(missing, Files.readSymbolicLink(metadata));
        assertFalse(Files.exists(missing));
    }

    @Test
    public void testLatestSnapshotIsCheckedDespiteShouldNotLoadMarker() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        write(root.resolve("db/00000"), "g+v", LEGAL_KEY, VALUE);
        Path snapshot = root.resolve("raft/00000/snapshot/snapshot_00000000000000000042");
        write(snapshot.resolve("data"), "g+v", LEGACY_KEY, VALUE);
        Files.writeString(snapshot.resolve("should_not_load"), "saved snapshot");
        Map<String, String> before = hashes(root);
        HgStoreException failure = assertThrows(HgStoreException.class,
                () -> LegacyGraphIdChecker.checkStore(root.toString(), config));
        assertTrue(failure.getMessage(), failure.getMessage().contains(snapshot.toString()));
        assertEquals(before, hashes(root));
    }

    @Test
    public void testSupersededSnapshotIsNotSelected() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        write(root.resolve("raft/00000/snapshot/snapshot_2/data"), "g+v", LEGACY_KEY, VALUE);
        write(root.resolve("raft/00000/snapshot/snapshot_10/data"), "g+v", LEGAL_KEY, VALUE);
        Map<String, String> before = hashes(root);
        LegacyGraphIdChecker.checkStore(root.toString(), config);
        assertEquals(before, hashes(root));
    }

    @Test
    public void testWhitespaceInConfiguredDirectoryIsNotTrimmed() throws Exception {
        Path root = this.temporary.newFolder(" store ").toPath();
        write(root.resolve("db/00000"), "g+v", LEGACY_KEY, VALUE);
        Map<String, String> before = hashes(root);
        HgStoreException failure = assertThrows(HgStoreException.class,
                () -> LegacyGraphIdChecker.checkStore(root.toString(), config));
        assertTrue(failure.getMessage(), failure.getMessage().contains(root.toString()));
        assertEquals(before, hashes(root));
    }

    @Test
    public void testAllConfiguredDataRootsAreChecked() throws Exception {
        Path first = this.temporary.newFolder().toPath();
        Path second = this.temporary.newFolder().toPath();
        write(first.resolve("db/00000"), "g+v", LEGAL_KEY, VALUE);
        write(second.resolve("db/00001"), "g+ie", LEGACY_KEY, VALUE);
        Map<String, String> before = hashes(this.temporary.getRoot().toPath());
        HgStoreException failure = assertThrows(HgStoreException.class,
                () -> LegacyGraphIdChecker.checkStore(first + "," + second, config));
        assertTrue(failure.getMessage(), failure.getMessage().contains(second.toString()));
        assertEquals(before, hashes(this.temporary.getRoot().toPath()));
    }

    @Test
    public void testUndiscoveredStoredLocationIsChecked() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        Path stored = this.temporary.newFolder().toPath();
        write(stored.resolve("db/00007"), "g+oe", LEGACY_KEY, VALUE);
        storeLocation(root, stored, 7);
        Map<String, String> before = hashes(this.temporary.getRoot().toPath());
        HgStoreException failure = assertThrows(HgStoreException.class,
                () -> LegacyGraphIdChecker.checkStore(root.toString(), config));
        assertTrue(failure.getMessage(), failure.getMessage().contains(stored.toString()));
        assertEquals(before, hashes(this.temporary.getRoot().toPath()));
    }

    @Test
    public void testOfflineRefusesUnverifiedStoredPartitionWithoutBlockingStartup() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        Path missing = root.resolve("missing");
        write(root.resolve("db/00000"), "g+v", LEGAL_KEY, VALUE);
        storeLocation(root, missing, 7);
        Map<String, String> before = hashes(root);

        IOException failure = assertThrows(IOException.class,
                () -> LegacyGraphIdChecker.checkOffline(root.toString(), config));
        assertTrue(failure.getMessage(), failure.getMessage().contains("partition 7"));
        assertTrue(failure.getMessage(), failure.getMessage().contains(missing.toString()));
        assertTrue(failure.getMessage(), failure.getMessage().contains("may be stale"));
        LegacyGraphIdChecker.checkStore(root.toString(), config);
        assertEquals(before, hashes(root));
    }

    @Test
    public void testOfflineRefusesStoredRootWithoutMatchingDatabase() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        Path stored = this.temporary.newFolder().toPath();
        write(root.resolve("db/00000"), "g+v", LEGAL_KEY, VALUE);
        write(stored.resolve("db/00008"), "g+v", LEGAL_KEY, VALUE);
        storeLocation(root, stored, 7);

        IOException failure = assertThrows(IOException.class,
                () -> LegacyGraphIdChecker.checkOffline(root.toString(), config));
        assertTrue(failure.getMessage(), failure.getMessage().contains("partition 7"));
    }

    @Test
    public void testOfflineRefusesEmptyStoredLocation() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        write(root.resolve("db/00000"), "g+v", LEGAL_KEY, VALUE);
        storeLocation(root, Path.of(""), 7);

        IOException failure = assertThrows(IOException.class,
                () -> LegacyGraphIdChecker.checkOffline(root.toString(), config));
        assertTrue(failure.getMessage(), failure.getMessage().contains("partition 7"));
        assertTrue(failure.getMessage(), failure.getMessage().contains("stored location is empty"));
    }

    @Test
    public void testMovedConfiguredPartitionSupersedesStoredLocation() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        Path stored = this.temporary.newFolder().toPath();
        write(root.resolve("db/00007"), "g+v", LEGAL_KEY, VALUE);
        write(stored.resolve("db/00007"), "g+v", LEGACY_KEY, VALUE);
        storeLocation(root, stored, 7);
        Map<String, String> before = hashes(this.temporary.getRoot().toPath());
        LegacyGraphIdChecker.checkStore(root.toString(), config);
        assertEquals(before, hashes(this.temporary.getRoot().toPath()));
    }

    @Test
    public void testOfflineCheckRejectsAnotherProcessesNativeLock() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        Path metadata = root.resolve("hgstore-metadata");
        write(metadata, "default", new byte[]{1}, VALUE);
        Path signals = this.temporary.newFolder().toPath();
        Path ready = signals.resolve("ready");
        Path output = signals.resolve("writer.log");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process writer = new ProcessBuilder(java, "-cp", classpath, NativeLockHolder.class.getName(),
                                            metadata.toString(), ready.toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!Files.exists(ready) && writer.isAlive() && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertTrue(Files.readString(output), Files.exists(ready));
            Map<String, String> before = hashes(root);
            IOException failure = assertThrows(IOException.class,
                    () -> LegacyGraphIdChecker.checkOffline(root.toString(), config));
            assertTrue(failure.getMessage(), failure.getMessage().contains("locked"));
            LegacyGraphIdChecker.StartupPreflightException startup = assertThrows(
                    LegacyGraphIdChecker.StartupPreflightException.class,
                    () -> LegacyGraphIdChecker.checkStore(root.toString(), config));
            assertTrue(startup.getMessage(), startup.getMessage().contains("locked"));
            assertEquals(before, hashes(root));
        } finally {
            writer.getOutputStream().close();
            if (!writer.waitFor(10, TimeUnit.SECONDS)) {
                writer.destroyForcibly();
                writer.waitFor(10, TimeUnit.SECONDS);
            }
        }
        assertEquals(Files.readString(output), 0, writer.exitValue());
        LegacyGraphIdChecker.checkOffline(root.toString(), config);
    }

    @Test
    public void testOfflineCheckRequiresExistingRootsAndLocks() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        Path missing = root.resolve("missing");
        assertThrows(IOException.class, () -> LegacyGraphIdChecker.checkOffline(missing.toString(), config));
        assertFalse(Files.exists(missing));
        Path database = root.resolve("db/00000");
        write(database, "g+v", LEGAL_KEY, VALUE);
        Files.delete(database.resolve("LOCK"));
        Map<String, String> before = hashes(root);
        assertThrows(IOException.class, () -> LegacyGraphIdChecker.checkOffline(root.toString(), config));
        assertEquals(before, hashes(root));
    }

    private void createCheckpoint(Path target, byte[] key) throws Exception {
        Path source = this.temporary.newFolder().toPath().resolve("source");
        write(source, "default", key, VALUE);
        Files.createDirectories(target.getParent());
        try (Options options = new Options();
             RocksDB database = RocksDB.open(options, source.toString());
             Checkpoint checkpoint = Checkpoint.create(database)) {
            checkpoint.createCheckpoint(target.toString());
        }
    }

    private static void storeLocation(Path root, Path location, int partition) throws Exception {
        write(root.resolve("hgstore-metadata"), "default", MetadataKeyHelper.getPartitionStoreKey(partition),
              Metapb.PartitionStore.newBuilder().setPartitionId(partition)
                    .setStoreLocation(location.toString()).build().toByteArray());
    }

    private static void write(Path path, String table, byte[] key, byte[] value) throws Exception {
        Files.createDirectories(path);
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB database = RocksDB.open(options, path.toString())) {
            if ("default".equals(table)) {
                database.put(key, value);
            } else {
                try (ColumnFamilyOptions familyOptions = new ColumnFamilyOptions();
                     ColumnFamilyHandle handle = database.createColumnFamily(new ColumnFamilyDescriptor(
                             table.getBytes(StandardCharsets.UTF_8), familyOptions))) {
                    database.put(handle, key, value);
                }
            }
        }
    }

    private static Map<String, String> hashes(Path root) throws Exception {
        Map<String, String> result = new TreeMap<>();
        try (var paths = Files.walk(root)) {
            for (Path path : paths.filter(Files::isRegularFile).toList()) {
                result.put(root.relativize(path).toString(), Files.getLastModifiedTime(path) + ":" +
                        HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                                                             .digest(Files.readAllBytes(path))));
            }
        }
        return result;
    }

    public static final class NativeLockHolder {

        public static void main(String[] args) throws Exception {
            RocksDB.loadLibrary();
            try (Options options = new Options();
                 RocksDB ignored = RocksDB.open(options, args[0])) {
                Files.writeString(Path.of(args[1]), "locked");
                System.in.read();
            }
        }
    }
}
