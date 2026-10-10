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
import java.io.FilterOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.exception.ConnectionException;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Rule;
import org.junit.Test;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RocksDBSnapshotRestoreTest {

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void testParentBindCheckpointSameDataRejectsBeforeDeletion() throws Exception {
        this.checkParentBindCheckpointOverlap("same");
    }

    @Test
    public void testParentBindCheckpointInsideDataRejectsBeforeDeletion() throws Exception {
        this.checkParentBindCheckpointOverlap("inside");
    }

    @Test
    public void testParentBindCheckpointContainingDataRejectsBeforeDeletion() throws Exception {
        this.checkParentBindCheckpointOverlap("parent");
    }

    @Test
    public void testParentBindCheckpointContainingWalRejectsBeforeDeletion() throws Exception {
        this.checkParentBindCheckpointOverlap("wal");
    }

    private void checkParentBindCheckpointOverlap(String layout) throws Exception {
        String original = System.getProperty("hugegraph.test.recovery.parent.original");
        String alias = System.getProperty("hugegraph.test.recovery.parent.alias");
        org.junit.Assume.assumeTrue("requires the explicit parent-bind kernel fixture",
                                   original != null && alias != null);
        assertTrue(Files.isSameFile(Path.of(original), Path.of(alias)));
        Path root = Files.createTempDirectory(Path.of(original), "checkpoint-overlap-" + layout + "-");
        Path aliased = Path.of(alias).resolve(root.getFileName());
        File data = root.resolve("live").toFile();
        File wal = root.resolve("wal").toFile();
        File snapshot = aliased.resolve("live").toFile();
        if ("parent".equals(layout) || "wal".equals(layout)) {
            File physicalCheckpoint = root.resolve("checkpoint").toFile();
            RocksDB.loadLibrary();
            try (Options options = new Options().setCreateIfMissing(true);
                 RocksDB db = RocksDB.open(options, physicalCheckpoint.toString())) {
                db.put(new byte[]{8}, new byte[]{9});
            }
            snapshot = aliased.resolve("checkpoint").toFile();
            if ("parent".equals(layout)) {
                data = new File(physicalCheckpoint, "live");
            } else {
                wal = new File(physicalCheckpoint, "wal");
            }
        }
        RocksDBStdSessions owner = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                         data.toString(), wal.toString());
        try {
            owner.createTable("test");
            owner.session().put("test", new byte[]{1}, new byte[]{2});
            owner.session().commit();
            if ("inside".equals(layout)) {
                AtomicReference<OpenedRocksDB> shared = Whitebox.getInternalState(owner, "rocksdb");
                try (Checkpoint checkpoint = Checkpoint.create(shared.get().rocksdb())) {
                    checkpoint.createCheckpoint(new File(data, "checkpoint").toString());
                }
                snapshot = aliased.resolve("live/checkpoint").toFile();
            }
            byte[] liveCurrent = Files.readAllBytes(new File(data, "CURRENT").toPath());
            byte[] sourceCurrent = Files.readAllBytes(new File(snapshot, "CURRENT").toPath());
            boolean rejected = false;
            try {
                owner.resumeSnapshot(snapshot.toString());
            } catch (BackendException expected) {
                rejected = true;
            }
            Path marker = Path.of(data + ".resume-pending");
            // Keep the actual fixture and observations, including destructive negative controls.
            String observed = "layout=" + layout + "\nlive_exists=" + data.exists() +
                              "\nsource_exists=" + snapshot.exists() +
                              "\nmarker_exists=" + Files.exists(marker) + "\n";
            Files.writeString(root.resolve("observed.txt"), observed);
            System.out.print(observed);
            assertTrue("Physical overlap must be rejected: " + observed, rejected);
            assertFalse("Refusal must precede marker publication: " + observed, Files.exists(marker));
            assertArrayEquals(liveCurrent, Files.readAllBytes(new File(data, "CURRENT").toPath()));
            assertArrayEquals(sourceCurrent, Files.readAllBytes(new File(snapshot, "CURRENT").toPath()));
        } finally {
            owner.close();
        }
        RocksDBStdSessions fresh = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                         data.toString(), wal.toString(),
                                                         Collections.singletonList("test"));
        try {
            assertArrayEquals(new byte[]{2}, fresh.session().get("test", new byte[]{1}));
        } finally {
            fresh.close();
        }
    }

    @Test
    public void testParentBindUnsafePendingRejectsBeforeDeletingNativeData() throws Exception {
        String original = System.getProperty("hugegraph.test.recovery.parent.original");
        String alias = System.getProperty("hugegraph.test.recovery.parent.alias");
        org.junit.Assume.assumeTrue("requires the explicit parent-bind kernel fixture",
                                   original != null && alias != null);
        Path root = Files.createTempDirectory(Path.of(original), "unsafe-pending-");
        File data = root.resolve("live").toFile();
        File wal = root.resolve("wal").toFile();
        File snapshot = Path.of(alias).resolve(root.getFileName()).resolve("live").toFile();
        RocksDBStdSessions owner = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                         data.toString(), wal.toString());
        try {
            owner.createTable("test");
            owner.session().put("test", new byte[]{1}, new byte[]{2});
            owner.session().commit();
        } finally {
            owner.close();
        }
        // Represent a checksum-valid pending record accepted by the previous lexical guard.
        Properties state = new Properties();
        state.setProperty("operation", java.util.UUID.randomUUID().toString());
        state.setProperty("data", data.getCanonicalPath());
        state.setProperty("snapshot", snapshot.getCanonicalPath());
        state.setProperty("wal", wal.getCanonicalPath());
        state.setProperty("configured-wal", wal.getCanonicalPath());
        state.setProperty("wal-links", "0");
        java.lang.reflect.Method generation = RocksDBSnapshotRestore.class.getDeclaredMethod("generation", File.class);
        generation.setAccessible(true);
        state.setProperty("generation", (String) generation.invoke(null, snapshot));
        java.lang.reflect.Method checksum = RocksDBSnapshotRestore.class.getDeclaredMethod("metadataDigest",
                                                                                          Properties.class);
        checksum.setAccessible(true);
        state.setProperty("metadata-sha256", (String) checksum.invoke(null, state));
        Path marker = Path.of(data + ".resume-pending");
        writeRecoveryMetadata(marker, state);
        byte[] pending = Files.readAllBytes(marker);
        byte[] current = Files.readAllBytes(new File(data, "CURRENT").toPath());
        try {
            RocksDBStdSessions fresh = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                             data.toString(), wal.toString(),
                                                             Collections.singletonList("test"));
            fresh.close();
            fail("Previously accepted physical-overlap pending record must fail closed");
        } catch (BackendException expected) {
            assertArrayEquals(pending, Files.readAllBytes(marker));
            assertArrayEquals(current, Files.readAllBytes(new File(data, "CURRENT").toPath()));
            assertTrue(snapshot.exists());
        }
    }

    @Test
    public void testParentBindDataWalAliasPreservesNativeWalTail() throws Exception {
        String original = System.getProperty("hugegraph.test.recovery.parent.original");
        String alias = System.getProperty("hugegraph.test.recovery.parent.alias");
        org.junit.Assume.assumeTrue("requires the explicit parent-bind kernel fixture",
                                   original != null && alias != null);
        assertTrue(Files.isSameFile(Path.of(original), Path.of(alias)));
        for (boolean aliased : new boolean[]{false, true}) {
            Path root = Files.createTempDirectory(Path.of(original), "wal-tail-");
            File data = root.resolve("data").toFile();
            File wal = aliased ? Path.of(alias).resolve(root.getFileName()).resolve("data").toFile() : data;
            File checkpoint = root.resolve("checkpoint").toFile();
            Files.createDirectories(data.toPath());
            assertTrue(Files.isSameFile(data.toPath(), wal.toPath()));
            try {
                nativeCheckpointWithWalTail(data, wal, checkpoint);
                try (RecoveryLock held = RocksDBSnapshotRestore.lock(data.toString())) {
                    RocksDBSnapshotRestore.start(data.toString(), wal.toString(), checkpoint.toString());
                    RocksDBSnapshotRestore restore =
                            RocksDBSnapshotRestore.prepareOpen(data.toString(), wal.toString());
                    assertNotNull(restore);
                    try (Options options = new Options().setWalDir(wal.toString());
                         RocksDB reopened = RocksDB.open(options, data.toString())) {
                        assertArrayEquals(new byte[]{2}, reopened.get(new byte[]{1}));
                        assertArrayEquals("Restored WAL-only data must survive physical directory aliases",
                                          new byte[]{4}, reopened.get(new byte[]{3}));
                        restore.complete();
                    }
                }
                assertFalse(new File(data + ".resume-pending").exists());
                assertFalse(checkpoint.exists());
            } finally {
                FileUtils.deleteDirectory(root.toFile());
            }
        }
    }

    private static void nativeCheckpointWithWalTail(File data, File wal, File checkpoint) throws Exception {
        RocksDB.loadLibrary();
        try (Options options = new Options().setCreateIfMissing(true).setWalDir(wal.toString());
             RocksDB db = RocksDB.open(options, data.toString())) {
            db.put(new byte[]{1}, new byte[]{2});
            try (Checkpoint snapshot = Checkpoint.create(db)) {
                snapshot.createCheckpoint(checkpoint.toString());
            }
            db.put(new byte[]{3}, new byte[]{4});
            db.flushWal(true);
            File[] logs = wal.listFiles(file -> file.getName().matches("[0-9]+\\.log"));
            assertNotNull(logs);
            assertTrue(logs.length > 0);
            for (File log : logs) {
                FileUtils.copyFile(log, new File(checkpoint, log.getName()));
            }
            // Verify the actual checkpoint can replay its tail before testing restore.
            try (Options readOptions = new Options();
                 RocksDB source = RocksDB.openReadOnly(readOptions, checkpoint.toString())) {
                assertArrayEquals(new byte[]{2}, source.get(new byte[]{1}));
                assertArrayEquals(new byte[]{4}, source.get(new byte[]{3}));
            }
        }
    }

    @Test
    public void testDirectDataSymlinkIsRejectedBeforeNativeOpen() throws Exception {
        File root = this.temporary.newFolder("direct-data-link");
        File data = new File(root, "physical-data");
        Path alias = new File(root, "configured-data").toPath();
        RocksDB.loadLibrary();
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, data.toString())) {
            db.put(new byte[]{1}, new byte[]{2});
        }
        byte[] current = Files.readAllBytes(new File(data, "CURRENT").toPath());
        Files.createSymbolicLink(alias, data.toPath());
        for (String configured : Arrays.asList(alias.toString(), alias.resolve(".").toString())) {
            for (boolean withTables : new boolean[]{false, true}) {
                RocksDBStdSessions opened = null;
                try {
                    opened = withTables ? new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                                 configured, data.toString(), Collections.emptyList()) :
                             new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                    configured, data.toString());
                    fail("Direct data symlink must be rejected before opening a native owner");
                } catch (RocksDBException expected) {
                    assertTrue(expected.getCause().getCause().getMessage().contains("symbolic link"));
                } finally {
                    if (opened != null) {
                        opened.forceCloseRocksDB();
                    }
                }
                assertArrayEquals(current, Files.readAllBytes(new File(data, "CURRENT").toPath()));
                assertFalse(new File(data + ".resume-pending").exists());
            }
        }
        FileUtils.deleteDirectory(data);
        try {
            RocksDBSnapshotRestore.lock(alias.toString());
            fail("A dangling direct DB link must not change recovery guard identity");
        } catch (BackendException expected) {
            assertTrue(expected.getCause().getMessage().contains("symbolic link"));
        }
        assertFalse(new File(alias + ".resume-lock").exists());
    }

    @Test
    public void testParentDataSymlinkStillSupportsNativeRestore() throws Exception {
        File root = this.temporary.newFolder("snapshot-parent-link");
        Path physical = new File(root, "physical").toPath();
        Files.createDirectories(physical);
        Path parent = new File(root, "parent").toPath();
        Files.createSymbolicLink(parent, physical);
        File data = parent.resolve("db").toFile();
        File snapshot = new File(root, "checkpoint");
        RocksDBStdSessions owner = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                          data.toString(), data.toString());
        try {
            owner.createTable("test");
            owner.session().put("test", new byte[]{1}, new byte[]{2});
            owner.session().commit();
            owner.createSnapshot(snapshot.toString());
            owner.session().put("test", new byte[]{1}, new byte[]{3});
            owner.session().commit();
            owner.resumeSnapshot(snapshot.toString());
            assertArrayEquals(new byte[]{2}, owner.session().get("test", new byte[]{1}));
            assertTrue(Files.isSymbolicLink(parent));
        } finally {
            owner.close();
        }
    }

    @Test
    public void testUnknownMarkerExistenceRejectsRecovery() throws Exception {
        Path root = this.temporary.newFolder("inaccessible-marker").toPath();
        org.junit.Assume.assumeTrue(Files.getFileStore(root).supportsFileAttributeView("posix"));
        Path data = root.resolve("data");
        Files.createDirectories(data);
        Path marker = root.resolve("data.resume-pending");
        byte[] record = new byte[]{4, 2};
        Files.write(marker, record);
        Set<java.nio.file.attribute.PosixFilePermission> permissions = Files.getPosixFilePermissions(root);
        try {
            Files.setPosixFilePermissions(root, Collections.emptySet());
            org.junit.Assume.assumeTrue("runner bypasses directory permissions; marker remains observable",
                                       !Files.exists(marker, java.nio.file.LinkOption.NOFOLLOW_LINKS) &&
                                       !Files.notExists(marker, java.nio.file.LinkOption.NOFOLLOW_LINKS));
            assertFalse(Files.exists(marker, java.nio.file.LinkOption.NOFOLLOW_LINKS));
            assertFalse(Files.notExists(marker, java.nio.file.LinkOption.NOFOLLOW_LINKS));
            try {
                RocksDBSnapshotRestore.prepareOpen(data.toString(), data.toString());
                fail("Unknown marker existence must not be treated as no pending restore");
            } catch (BackendException expected) {
                assertTrue(expected.getMessage().contains("preserve"));
            }
        } finally {
            Files.setPosixFilePermissions(root, permissions);
        }
        assertArrayEquals(record, Files.readAllBytes(marker));
        assertEquals(0, data.toFile().list().length);
    }

    @Test
    public void testUnrecordedWalLinksRejectBeforeDeletingNativeData() throws Exception {
        for (boolean targetChain : new boolean[]{false, true}) {
            File root = this.temporary.newFolder("unsafe-wal-" + targetChain);
            File data = new File(root, "data");
            Files.createDirectories(data.toPath());
            Path physical = new File(root, "physical/child").toPath();
            Files.createDirectories(physical);
            Path inner = new File(data, "link").toPath();
            Files.createSymbolicLink(inner, physical);
            Path wal;
            if (targetChain) {
                Path external = new File(root, "external-link").toPath();
                Files.createSymbolicLink(external, inner);
                wal = external.resolve("logs");
            } else {
                wal = inner.resolve("../logs");
            }
            FileUtils.forceMkdir(wal.toFile());
            File snapshot = new File(root, "checkpoint");
            nativeCheckpointWithWalTail(data, wal.toFile(), snapshot);
            byte[] current = Files.readAllBytes(new File(data, "CURRENT").toPath());
            try {
                RocksDBSnapshotRestore.start(data.toString(), wal.toString(), snapshot.toString());
                fail("WAL links lost when data is replaced must be rejected before mutation");
            } catch (IOException expected) {
                assertTrue(expected.getMessage().contains("WAL"));
            }
            assertTrue(Files.isSymbolicLink(inner));
            assertArrayEquals(current, Files.readAllBytes(new File(data, "CURRENT").toPath()));
            assertTrue(new File(snapshot, "CURRENT").isFile());
            assertFalse(new File(data + ".resume-pending").exists());
            try (Options options = new Options().setWalDir(wal.toString());
                 RocksDB reopened = RocksDB.open(options, data.toString())) {
                assertArrayEquals(new byte[]{2}, reopened.get(new byte[]{1}));
                assertArrayEquals(new byte[]{4}, reopened.get(new byte[]{3}));
            }
        }
    }

    @Test
    public void testFailedStoreRestoreCanCloseAndReopenSameInstance() throws Exception {
        File root = this.temporary.newFolder("store-reopen");
        File data = new File(root, "snapshot-data/store");
        File snapshot = new File(root, "snapshot_snapshot-data/store");
        HugeConfig config = FakeObjects.newConfig();
        config.setProperty(RocksDBOptions.DATA_PATH.name(), data.getParent());
        config.setProperty(RocksDBOptions.WAL_PATH.name(), data.getParent());
        RocksDBStdSessions failed = new RocksDBStdSessions(config, "db", "store",
                                                           data.toString(), data.toString()) {
            @Override
            public synchronized void resumeSnapshot(String path) {
                AtomicReference<OpenedRocksDB> reference = Whitebox.getInternalState(this, "rocksdb");
                RecoveryLock lease = reference.get().closeForRestore();
                try {
                    RocksDBSnapshotRestore restore = new RocksDBSnapshotRestore(
                            data.toString(), data.toString(), path, new FaultyFiles("data-copy"));
                    restore.begin();
                    restore.install();
                    fail("Expected checkpoint copy failure");
                } catch (IOException e) {
                    throw new BackendException("Injected copy failure", e);
                } finally {
                    RocksDBSnapshotRestore.unlock(lease);
                }
            }
        };
        RocksDBStore.RocksDBGraphStore store = (RocksDBStore.RocksDBGraphStore) cleanupStore(failed);
        List<String> tables = store.tableNames();
        String table = tables.get(0);
        try {
            // Match the real Store CF layout so reopen exercises recovery rather
            // than an unrelated missing-column-family fallback.
            failed.createTable(tables.toArray(new String[0]));
            failed.session().open();
            failed.session().put(table, new byte[]{1}, new byte[]{2});
            failed.session().commit();
            failed.createSnapshot(snapshot.toString());
            try {
                store.resumeSnapshot("snapshot", true);
                fail("Expected native owner to close before injected copy failure");
            } catch (BackendException expected) {
                assertTrue(expected.getMessage().contains("Injected copy failure"));
            }
            assertFalse(store.opened());
            assertTrue(new File(data + ".resume-pending").exists());
            store.close();
            assertTrue(failed.closed());
            store.open(config);
            assertTrue(store.opened());
            RocksDBSessions reopened = Whitebox.getInternalState(store, "sessions");
            assertArrayEquals(new byte[]{2}, reopened.session().get(table, new byte[]{1}));
            assertFalse(new File(data + ".resume-pending").exists());
            assertFalse(snapshot.exists());
            store.close();
        } finally {
            failed.forceCloseRocksDB();
            RocksDBSessions remaining = Whitebox.getInternalState(store, "sessions");
            remaining.forceCloseRocksDB();
        }
    }

    @Test
    public void testPendingRestoreRejectsRepeatedCopiesBeforeOpeningSource() throws Exception {
        for (boolean malformed : new boolean[]{false, true}) {
            File root = this.temporary.newFolder("snapshot-pending-copy-" + malformed);
            File data = new File(root, "data");
            File source = new File(root, "source");
            AtomicInteger sourceOpens = new AtomicInteger();
            RocksDBStdSessions owner = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                             data.toString(), data.toString()) {
                @Override
                OpenedRocksDB openSnapshot(String path) throws RocksDBException {
                    sourceOpens.incrementAndGet();
                    return super.openSnapshot(path);
                }
            };
            try {
                owner.createSnapshot(source.toString());
                String pendingCopy = owner.hardLinkSnapshot(source.toString());
                RocksDBSnapshotRestore.start(data.toString(), data.toString(), pendingCopy);
                Path marker = new File(data + ".resume-pending").toPath();
                if (malformed) {
                    Files.writeString(marker, "incomplete pending record");
                    // Exercise the acquired-lock path as well as a live owner's lease.
                    owner.forceCloseRocksDB();
                }
                byte[] pendingBytes = Files.readAllBytes(marker);
                byte[] current = Files.readAllBytes(new File(pendingCopy, "CURRENT").toPath());
                sourceOpens.set(0);
                for (int attempt = 0; attempt < 3; attempt++) {
                    try {
                        owner.hardLinkSnapshot(source.toString());
                        fail("Existing recovery must reject a new checkpoint copy");
                    } catch (BackendException expected) {
                        assertTrue(expected.getMessage().contains("reopen the database"));
                    }
                    assertEquals(0, sourceOpens.get());
                    assertArrayEquals(new String[]{new File(pendingCopy).getName()},
                                      root.list((parent, name) -> name.startsWith("data_temp-")));
                    assertArrayEquals(pendingBytes, Files.readAllBytes(marker));
                    assertArrayEquals(current, Files.readAllBytes(new File(pendingCopy, "CURRENT").toPath()));
                    assertTrue(new File(source, "CURRENT").isFile());
                }
            } finally {
                owner.forceCloseRocksDB();
            }
        }
    }

    @Test
    public void testLaterPendingDatabaseReclaimsEarlierNewCopy() throws Exception {
        File root = this.temporary.newFolder("snapshot-later-pending");
        File firstSource = new File(root, "first-source");
        File secondSource = new File(root, "second-source");
        File secondData = new File(root, "second");
        AtomicInteger sourceOpens = new AtomicInteger();
        RocksDBStdSessions first = cleanupOwner(new File(root, "first"), firstSource);
        RocksDBStdSessions second = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                          secondData.toString(), secondData.toString()) {
            @Override
            public String buildSnapshotPath(String prefix) {
                return secondSource.toString();
            }

            @Override
            OpenedRocksDB openSnapshot(String path) throws RocksDBException {
                sourceOpens.incrementAndGet();
                return super.openSnapshot(path);
            }
        };
        try {
            first.createSnapshot(firstSource.toString());
            second.createSnapshot(secondSource.toString());
            RocksDBSnapshotRestore.start(secondData.toString(), secondData.toString(), secondSource.toString());
            Path marker = new File(secondData + ".resume-pending").toPath();
            byte[] pendingBytes = Files.readAllBytes(marker);
            RocksDBStore store = cleanupStore(first, second);
            for (int attempt = 0; attempt < 3; attempt++) {
                try {
                    store.resumeSnapshot("ignored", false);
                    fail("Later database is already recovering");
                } catch (BackendException expected) {
                    assertTrue(expected.getMessage().contains("reopen the database"));
                }
                assertEquals(0, sourceOpens.get());
                assertEquals(0, root.list((parent, name) -> name.startsWith("first_temp-") ||
                                                          name.startsWith("second_temp-")).length);
                assertArrayEquals(pendingBytes, Files.readAllBytes(marker));
                assertTrue(new File(firstSource, "CURRENT").isFile());
                assertTrue(new File(secondSource, "CURRENT").isFile());
            }
        } finally {
            first.forceCloseRocksDB();
            second.forceCloseRocksDB();
        }
    }

    @Test
    public void testUnusedCopiesDoNotAccumulateOnFirstPassFailure() throws Exception {
        File root = this.temporary.newFolder("snapshot-copy-failure");
        File source = new File(root, "source");
        RuntimeException failure = new IllegalStateException("later database has no snapshot");
        RocksDBStdSessions first = cleanupOwner(new File(root, "first"), source);
        RocksDBStdSessions second = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                          new File(root, "second").toString(),
                                                          new File(root, "second").toString()) {
            @Override
            public String buildSnapshotPath(String prefix) {
                throw failure;
            }
        };
        try {
            first.createSnapshot(source.toString());
            RocksDBStore store = cleanupStore(first, second);
            File foreign = new File(root, "first_temp-foreign");
            assertTrue(foreign.mkdir());
            for (int attempt = 0; attempt < 3; attempt++) {
                try {
                    store.resumeSnapshot("ignored", false);
                    fail("Expected first-pass failure");
                } catch (IllegalStateException expected) {
                    assertSame(failure, expected);
                }
                assertArrayEquals(new String[]{"first_temp-foreign"},
                                  root.list((parent, name) -> name.startsWith("first_temp-")));
                assertTrue(source.isDirectory());
                assertTrue(first.databaseOpened());
            }
        } finally {
            first.forceCloseRocksDB();
            second.forceCloseRocksDB();
        }
    }

    @Test
    public void testRestoreFailurePreservesPendingButCleansUnstartedCopy() throws Exception {
        File root = this.temporary.newFolder("snapshot-restore-failure");
        List<String> copies = new ArrayList<>();
        List<String> started = new ArrayList<>();
        RocksDBStdSessions[] owners = new RocksDBStdSessions[2];
        try {
            for (int i = 0; i < owners.length; i++) {
                File data = new File(root, "data" + i);
                File source = new File(root, "source" + i);
                owners[i] = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                  data.toString(), data.toString()) {
                    @Override
                    public String buildSnapshotPath(String prefix) {
                        return source.toString();
                    }

                    @Override
                    public String hardLinkSnapshot(String path) throws RocksDBException {
                        String copy = super.hardLinkSnapshot(path);
                        copies.add(copy);
                        return copy;
                    }

                    @Override
                    public synchronized void resumeSnapshot(String path) {
                        started.add(path);
                        this.forceCloseRocksDB();
                        try (RecoveryLock lock = RocksDBSnapshotRestore.lock(data.toString())) {
                            RocksDBSnapshotRestore.start(data.toString(), data.toString(), path);
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                        throw new IllegalStateException("stop after pending marker");
                    }
                };
                owners[i].createSnapshot(source.toString());
            }
            try {
                cleanupStore(owners).resumeSnapshot("ignored", false);
                fail("Expected second-pass failure");
            } catch (IllegalStateException expected) {
                assertEquals("stop after pending marker", expected.getMessage());
            }
            assertEquals(2, copies.size());
            assertEquals(1, started.size());
            for (String copy : copies) {
                assertEquals(started.contains(copy), new File(copy).exists());
            }
            assertTrue(new File(root, "source0").exists());
            assertTrue(new File(root, "source1").exists());
        } finally {
            for (RocksDBStdSessions owner : owners) {
                if (owner != null) {
                    owner.forceCloseRocksDB();
                }
            }
        }
    }

    @Test
    public void testCleanupPreservesCopiesWithMalformedOrNonFileMarker() throws Exception {
        File root = this.temporary.newFolder("snapshot-uncertain-marker");
        File data = new File(root, "data");
        File source = new File(root, "source");
        RocksDBStdSessions owner = cleanupOwner(data, source);
        try {
            owner.createSnapshot(source.toString());
            String copy = owner.hardLinkSnapshot(source.toString());
            Path marker = new File(data + ".resume-pending").toPath();
            Files.write(marker, "snapshot=broken\n".getBytes(StandardCharsets.UTF_8));
            owner.cleanupSnapshot(copy);
            assertTrue(new File(copy).exists());
            Files.delete(marker);
            Files.createDirectory(marker);
            owner.cleanupSnapshot(copy);
            assertTrue(new File(copy).exists());
            Files.delete(marker);
            owner.cleanupSnapshot(copy);
            assertFalse(new File(copy).exists());
            assertTrue(source.exists());
        } finally {
            owner.forceCloseRocksDB();
        }
    }

    @Test
    public void testCleanupRetainsCopyWhenAnotherOwnerHoldsLock() throws Exception {
        File root = this.temporary.newFolder("snapshot-cleanup-contention");
        File data = new File(root, "data");
        File source = new File(root, "source");
        RocksDBStdSessions owner = cleanupOwner(data, source);
        try {
            owner.createSnapshot(source.toString());
            String copy = owner.hardLinkSnapshot(source.toString());
            owner.forceCloseRocksDB();
            try (RecoveryLock competing = RocksDBSnapshotRestore.lock(data.toString())) {
                try {
                    owner.cleanupSnapshot(copy);
                    fail("Cleanup must acquire recovery ownership");
                } catch (BackendException expected) {
                    assertTrue(new File(copy).exists());
                    assertTrue(competing.isOpen());
                }
            }
            owner.cleanupSnapshot(copy);
            assertFalse(new File(copy).exists());
            assertTrue(source.exists());
        } finally {
            owner.forceCloseRocksDB();
        }
    }

    @Test
    public void testConsumingRestoreFailureNeverCleansUserCheckpoint() throws Exception {
        File root = this.temporary.newFolder("snapshot-consuming-failure");
        File source = new File(root, "source");
        RocksDBStdSessions owner = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                         new File(root, "data").toString(),
                                                         new File(root, "data").toString()) {
            @Override
            public String buildSnapshotPath(String prefix) {
                return source.toString();
            }

            @Override
            public void resumeSnapshot(String path) {
                throw new IllegalStateException("before consuming source");
            }
        };
        try {
            owner.createSnapshot(source.toString());
            try {
                cleanupStore(owner).resumeSnapshot("ignored", true);
                fail("Expected restore failure");
            } catch (IllegalStateException expected) {
                assertEquals("before consuming source", expected.getMessage());
            }
            assertTrue(new File(source, "CURRENT").isFile());
        } finally {
            owner.forceCloseRocksDB();
        }
    }

    @Test
    public void testCopyFailureBeforeReturnCleansOutputAndInternalStaging() throws Exception {
        for (boolean closeFailure : new boolean[]{false, true}) {
            File root = this.temporary.newFolder("snapshot-before-return-" + closeFailure);
            File source = new File(root, "source");
            RuntimeException failure = new IllegalStateException("checkpoint creation or close failed");
            RocksDBStdSessions owner = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                             new File(root, "data").toString(),
                                                         new File(root, "data").toString()) {
                @Override
                OpenedRocksDB openSnapshot(String path) throws RocksDBException {
                    OpenedRocksDB delegate = super.openSnapshot(path);
                    return new OpenedRocksDB(delegate.rocksdb(), new HashMap<>(), null) {
                        @Override
                        public void createCheckpoint(String target) {
                            if (closeFailure) {
                                delegate.createCheckpoint(target);
                            } else {
                                try {
                                    Files.createDirectory(new File(target + "_temp").toPath());
                                    Files.write(new File(target + "_temp/partial").toPath(), new byte[]{1});
                                } catch (IOException e) {
                                    throw new AssertionError(e);
                                }
                                throw failure;
                            }
                        }

                        @Override
                        public void close() {
                            delegate.close();
                            if (closeFailure) {
                                throw failure;
                            }
                        }
                    };
                }
            };
            try {
                owner.createSnapshot(source.toString());
                try {
                    owner.hardLinkSnapshot(source.toString());
                    fail("Expected checkpoint failure");
                } catch (IllegalStateException expected) {
                    assertSame(failure, expected);
                }
                assertEquals(0, root.list((parent, name) -> name.startsWith("data_temp-")).length);
                assertTrue(new File(source, "CURRENT").isFile());
            } finally {
                owner.forceCloseRocksDB();
            }
        }
    }

    @Test
    public void testCleanupFailureDoesNotReplaceRestoreFailure() throws Exception {
        File root = this.temporary.newFolder("snapshot-cleanup-error");
        File data = new File(root, "data");
        File source = new File(root, "source");
        RuntimeException original = new IllegalStateException("restore failed");
        RuntimeException cleanup = new IllegalStateException("cleanup failed");
        RocksDBStdSessions owner = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                         data.toString(), data.toString()) {
            @Override
            public String buildSnapshotPath(String prefix) {
                return source.toString();
            }

            @Override
            public void resumeSnapshot(String path) {
                throw original;
            }

            @Override
            void cleanupSnapshot(String path) {
                throw cleanup;
            }
        };
        try {
            owner.createSnapshot(source.toString());
            try {
                cleanupStore(owner).resumeSnapshot("ignored", false);
                fail("Expected restore failure");
            } catch (IllegalStateException expected) {
                assertSame(original, expected);
                assertArrayEquals(new Throwable[]{cleanup}, expected.getSuppressed());
            }
        } finally {
            owner.forceCloseRocksDB();
        }
    }

    private static RocksDBStdSessions cleanupOwner(File data, File source) throws RocksDBException {
        return new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                      data.toString(), data.toString()) {
            @Override
            public String buildSnapshotPath(String prefix) {
                return source.toString();
            }
        };
    }

    private static RocksDBStore cleanupStore(RocksDBStdSessions... owners) {
        RocksDBStore store = new RocksDBStore.RocksDBGraphStore(new RocksDBStoreProvider(), "db", "store");
        Whitebox.setInternalState(store, "sessions", owners[0]);
        Map<String, RocksDBSessions> databases = new ConcurrentSkipListMap<>();
        for (int i = 0; i < owners.length; i++) {
            // Match registerOpenedSessions: native ownership alone leaves the
            // BackendSessionPool closed until its initial session is registered.
            owners[i].session().open();
            databases.put(Integer.toString(i), owners[i]);
        }
        Whitebox.setInternalState(store, "dbs", databases);
        assertTrue(store.opened());
        return store;
    }

    @Test
    public void testDataCopyFailureRetainsCheckpointAndRetries() throws Exception {
        this.failureAndRetry("data-copy", "separate");
    }

    @Test
    public void testWalCopyFailureRetainsCheckpointAndRetries() throws Exception {
        this.failureAndRetry("copy", "separate");
    }

    @Test
    public void testShortCopyIsNotPublished() throws Exception {
        this.failureAndRetry("short-copy", "separate");
    }

    @Test
    public void testSameLengthCorruptionIsNotPublished() throws Exception {
        this.failureAndRetry("corrupt-copy", "separate");
    }

    @Test
    public void testRetirementFailureRetainsMarkerAndRetries() throws Exception {
        this.failureAndRetry("retire", "separate");
    }

    @Test
    public void testPublishFailureRetainsMarkerAndRetries() throws Exception {
        this.failureAndRetry("publish", "separate");
    }

    @Test
    public void testParentWalRetirementFailurePreservesData() throws Exception {
        this.failureAndRetry("retire", "parent");
    }

    @Test
    public void testNestedWalPublishFailureRetries() throws Exception {
        this.failureAndRetry("publish", "nested");
    }

    @Test
    public void testSymlinkWalPublishFailurePreservesLink() throws Exception {
        this.failureAndRetry("publish", "symlink");
    }

    @Test
    public void testSymlinkInsideDataSurvivesInterruptedRestore() throws Exception {
        this.failureAndRetry("data-copy", "nested-symlink");
    }

    @Test
    public void testIndirectSymlinkInsideDataSurvivesInterruptedRestore() throws Exception {
        this.failureAndRetry("data-copy", "ancestor-symlink");
    }

    @Test
    public void testCorruptedDataCopyIsNotOpened() throws Exception {
        this.failureAndRetry("corrupt-data-copy", "separate");
    }

    private static void fakeCheckpoint(File snapshot) throws IOException {
        Files.write(new File(snapshot, "CURRENT").toPath(), "MANIFEST-000001\n".getBytes(StandardCharsets.UTF_8));
        Files.write(new File(snapshot, "MANIFEST-000001").toPath(), new byte[]{1, 2, 3});
    }

    private void failureAndRetry(String fault, String layout) throws Exception {
        File root = this.temporary.newFolder();
        File data = new File(root, "data");
        File wal = new File(root, "wal");
        File snapshot = new File(root, "snapshot");
        if ("parent".equals(layout)) {
            data = new File(wal, "data");
            snapshot = new File(wal, "checkpoints/snapshot");
        } else if ("nested".equals(layout)) {
            wal = new File(data, "wal");
        }
        FileUtils.forceMkdir(data);
        if (layout.contains("symlink")) {
            File target = new File(root, "target");
            FileUtils.forceMkdir(target);
            File linkParent = "nested-symlink".equals(layout) || "ancestor-symlink".equals(layout) ? data : root;
            wal = new File(linkParent, "wal-link");
            Files.createSymbolicLink(wal.toPath(), target.toPath());
            if ("ancestor-symlink".equals(layout)) {
                wal = new File(wal, "child-wal");
                FileUtils.forceMkdir(wal);
            }
        } else {
            FileUtils.forceMkdir(wal);
        }
        FileUtils.forceMkdir(snapshot);
        fakeCheckpoint(snapshot);
        byte[] tail = "checkpoint".getBytes(StandardCharsets.UTF_8);
        FileUtils.writeByteArrayToFile(new File(snapshot, "000123.log"), tail);
        FileUtils.writeByteArrayToFile(new File(snapshot, "keep.txt"), tail);
        // Same-name, same-length old WAL must never be accepted as the checkpoint.
        FileUtils.writeByteArrayToFile(new File(wal, "000123.log"),
                                      "old-oldwal".getBytes(StandardCharsets.UTF_8));
        FileUtils.writeByteArrayToFile(new File(wal, "000124.log"), tail);
        RocksDBSnapshotRestore restore = new RocksDBSnapshotRestore(
                data.toString(), wal.toString(), snapshot.toString(), new FaultyFiles(fault));
        restore.begin();
        try {
            restore.install();
            fail("Expected injected " + fault);
        } catch (IOException expected) {
            assertTrue(new File(data + ".resume-pending").isFile());
            assertArrayEquals(tail, Files.readAllBytes(new File(snapshot, "000123.log").toPath()));
        }
        // The public resume path can retry the SAME source even if its WAL alias
        // disappeared between deleting data and copying the checkpoint.
        RocksDBSnapshotRestore.start(data.toString(), wal.toString(), snapshot.toString());
        RocksDBSnapshotRestore retry = RocksDBSnapshotRestore.prepareOpen(data.toString(), wal.toString());
        assertNotNull(retry);
        assertArrayEquals(tail, Files.readAllBytes(new File(wal, "000123.log").toPath()));
        assertFalse(new File(wal, "000124.log").exists());
        assertFalse(new File(data, "000123.log").exists());
        assertTrue(new File(data, "keep.txt").isFile());
        assertTrue(new File(snapshot, "000123.log").isFile());
        if (layout.contains("symlink")) {
            assertTrue(Files.isSymbolicLink("ancestor-symlink".equals(layout) ?
                                           wal.getParentFile().toPath() : wal.toPath()));
        }
        retry.complete();
        assertFalse(new File(data + ".resume-pending").exists());
    }

    @Test
    public void testMarkerDirectorySyncPrecedesInstallAndCheckpointCleanup() throws Exception {
        this.checkMarkerDirectorySync(0);
    }

    @Test
    public void testPublicationDirectorySyncFailurePreservesLiveData() throws Exception {
        this.checkMarkerDirectorySync(1);
    }

    @Test
    public void testRetryDirectorySyncFailurePreservesLiveData() throws Exception {
        this.checkMarkerDirectorySync(2);
    }

    @Test
    public void testCompletionDirectorySyncFailurePreservesCheckpoint() throws Exception {
        this.checkMarkerDirectorySync(3);
    }

    private void checkMarkerDirectorySync(int failAt) throws Exception {
        File root = this.temporary.newFolder("directory-sync-" + failAt);
        File data = new File(root, "data");
        File snapshot = new File(root, "snapshot");
        FileUtils.forceMkdir(data);
        FileUtils.forceMkdir(snapshot);
        fakeCheckpoint(snapshot);
        Path live = new File(data, "live-sentinel").toPath();
        byte[] original = new byte[]{42};
        Files.write(live, original);
        Path marker = Path.of(data + ".resume-pending");
        AtomicInteger syncs = new AtomicInteger();
        RocksDBSnapshotRestore.FileOperations operations = new RocksDBSnapshotRestore.FileOperations() {
            @Override
            void forceDirectory(Path directory) throws IOException {
                int call = syncs.incrementAndGet();
                assertEquals(marker.getParent(), directory);
                assertEquals(call < 3, Files.exists(marker));
                // The retry source survives both sides of the marker removal boundary.
                assertTrue(snapshot.isDirectory());
                if (call == failAt) {
                    throw new IOException("injected directory sync failure " + call);
                }
                // Execute the real Linux directory fsync; this does not simulate a power loss.
                super.forceDirectory(directory);
            }

            @Override
            void deleteDirectory(File directory) throws IOException {
                assertEquals(2, syncs.get());
                super.deleteDirectory(directory);
            }
        };
        RocksDBSnapshotRestore restore = new RocksDBSnapshotRestore(
                data.toString(), data.toString(), snapshot.toString(), operations);
        try {
            restore.begin();
            restore.install();
            restore.complete();
            assertEquals(0, failAt);
            assertEquals(3, syncs.get());
            assertFalse(Files.exists(marker));
            assertFalse(snapshot.exists());
        } catch (IOException | BackendException expected) {
            assertTrue(failAt > 0);
            assertEquals(failAt, syncs.get());
            assertTrue(snapshot.isDirectory());
            if (failAt < 3) {
                assertArrayEquals(original, Files.readAllBytes(live));
                assertTrue(Files.isRegularFile(marker));
                // A normal subsequent open retries the preserved checkpoint.
                RocksDBSnapshotRestore retry = RocksDBSnapshotRestore.prepareOpen(data.toString(), data.toString());
                assertNotNull(retry);
                retry.complete();
            } else {
                // Installation already succeeded, but failed unlink fsync must not consume its source.
                assertFalse(Files.exists(marker));
                assertTrue(new File(data, "CURRENT").isFile());
            }
        }
    }

    @Test
    public void testWalNameCollisionFailsClosed() throws Exception {
        File data = this.temporary.newFolder("data");
        File wal = this.temporary.newFolder("wal");
        File snapshot = this.temporary.newFolder("snapshot");
        fakeCheckpoint(snapshot);
        FileUtils.writeByteArrayToFile(new File(snapshot, "000123.log"), new byte[]{1});
        FileUtils.forceMkdir(new File(wal, "000123.log"));
        RocksDBSnapshotRestore restore = new RocksDBSnapshotRestore(
                data.toString(), wal.toString(), snapshot.toString(),
                new RocksDBSnapshotRestore.FileOperations());
        restore.begin();
        try {
            RocksDBSnapshotRestore.prepareOpen(data.toString(), wal.toString());
            fail("WAL directory collision must fail closed");
        } catch (BackendException expected) {
            assertTrue(new File(data + ".resume-pending").isFile());
            assertTrue(new File(snapshot, "000123.log").isFile());
        }
    }

    @Test
    public void testFreshSessionRecoversPendingSnapshotBeforeNativeOpen() throws Exception {
        File data = this.temporary.newFolder("data");
        File wal = this.temporary.newFolder("wal");
        File snapshot = new File(this.temporary.newFolder("snapshot-root"), "rocks");
        HugeConfig config = FakeObjects.newConfig();
        RocksDBSessions original = new RocksDBStdSessions(config, "db", "store", data.toString(), wal.toString());
        byte[] key = new byte[]{1};
        try {
            original.createTable("test");
            original.session().put("test", key, new byte[]{2});
            original.session().commit();
            original.createSnapshot(snapshot.toString());
            original.session().put("test", key, new byte[]{3});
            original.session().commit();
        } finally {
            original.close();
        }
        RocksDBSnapshotRestore restore = new RocksDBSnapshotRestore(
                data.toString(), wal.toString(), snapshot.toString(), new FaultyFiles("data-copy"));
        restore.begin();
        try {
            restore.install();
            fail("Expected data copy failure");
        } catch (IOException expected) {
            assertTrue(new File(data + ".resume-pending").exists());
        }
        RocksDBSessions fresh = new RocksDBStdSessions(config, "db", "store", data.toString(), wal.toString(),
                                                       Collections.singletonList("test"));
        try {
            assertArrayEquals(new byte[]{2}, fresh.session().get("test", key));
            assertFalse(new File(data + ".resume-pending").exists());
            assertFalse(snapshot.exists());
        } finally {
            fresh.close();
        }
    }

    @Test
    public void testBothOpenOverloadsRejectIncompleteMarker() throws Exception {
        File data = this.temporary.newFolder("data");
        File marker = new File(data + ".resume-pending");
        Files.write(marker.toPath(), new byte[0]);
        HugeConfig config = FakeObjects.newConfig();
        for (boolean withTables : new boolean[]{false, true}) {
            try {
                if (withTables) {
                    new RocksDBStdSessions(config, "db", "store", data.toString(), data.toString(),
                                           Collections.emptyList());
                } else {
                    new RocksDBStdSessions(config, "db", "store", data.toString(), data.toString());
                }
                fail("Native open must not bypass incomplete marker");
            } catch (BackendException expected) {
                assertEquals(0, data.list().length);
                assertTrue(marker.isFile());
            }
        }
    }

    @Test
    public void testCompetingRecoveryCannotEnterWhileOwnerHoldsLock() throws Exception {
        File data = this.temporary.newFolder("data");
        try (RecoveryLock owner = RocksDBSnapshotRestore.lock(data.toString())) {
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread contender = new Thread(() -> {
                try {
                    for (boolean withTables : new boolean[]{false, true}) {
                        try {
                            if (withTables) {
                                new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                       data.toString(), data.toString(),
                                                       Collections.emptyList());
                            } else {
                                new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                       data.toString(), data.toString());
                            }
                            fail("Competing open acquired recovery lock");
                        } catch (RocksDBException expected) {
                            assertTrue(expected.getMessage().contains("No locks available"));
                            assertEquals(Status.Code.IOError, expected.getStatus().getCode());
                            assertTrue(expected.getCause() instanceof BackendException);
                            assertNotNull(expected.getCause().getCause());
                        }
                    }
                } catch (Throwable e) {
                    failure.set(e);
                } finally {
                    done.countDown();
                }
            });
            contender.start();
            assertTrue(done.await(10, TimeUnit.SECONDS));
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            assertEquals(0, data.list().length);
        }
        try (RecoveryLock retry = RocksDBSnapshotRestore.lock(data.toString())) {
            assertTrue(retry.isOpen());
        }
    }

    @Test
    public void testGuardErrorTextCannotReuseCachedNativeOwner() throws Exception {
        File data = this.temporary.newFolder("No locks available Column family not found");
        HugeConfig config = FakeObjects.newConfig();
        RocksDBSessions owner = new RocksDBStdSessions(config, "db", "store",
                                                       data.toString(), data.toString());
        RocksDBStore store = new RocksDBStore.RocksDBGraphStore(new RocksDBStoreProvider(), "db", "store");
        Map<String, RocksDBSessions> databases = Whitebox.getInternalState(store, "dbs");
        databases.put(data.toString(), owner);
        Path guard = new File(data + ".resume-lock").toPath();
        Path held = new File(data + ".owned-lock").toPath();
        try {
            owner.createTable("test");
            owner.session().put("test", new byte[]{1}, new byte[]{2});
            owner.session().commit();
            // Genuine contention can share the live owner's CFs.
            RocksDBSessions copy = store.open(config, data.toString(), data.toString(),
                                               Collections.singletonList("test"));
            assertArrayEquals(new byte[]{2}, copy.session().get("test", new byte[]{1}));
            copy.close();
            databases.put(data.toString(), owner);

            // Inject an IO failure, retaining the locked inode and live native owner.
            Files.move(guard, held);
            Files.createDirectory(guard);
            try {
                store.open(config, data.toString(), data.toString(), Collections.singletonList("test"));
                fail("Path text must not classify a guard IO failure as contention or missing CF");
            } catch (ConnectionException expected) {
                assertTrue(expected.getCause() instanceof RocksDBStdSessions.RecoveryLockException);
                assertFalse(((RocksDBStdSessions.RecoveryLockException) expected.getCause()).isContention());
                assertTrue(expected.getCause().getCause() instanceof BackendException);
                assertTrue(expected.getCause().getCause().getCause() instanceof IOException);
                assertTrue(expected.getCause().getMessage().contains("No locks available"));
                assertTrue(expected.getCause().getMessage().contains("Column family not found"));
                assertTrue(owner.databaseOpened());
                assertArrayEquals(new byte[]{2}, owner.session().get("test", new byte[]{1}));
                assertFalse(new File(data + ".resume-pending").exists());
            }
        } finally {
            if (Files.exists(held)) {
                Files.delete(guard);
                Files.move(held, guard);
            }
            owner.forceCloseRocksDB();
        }
    }

    @Test
    public void testLiveDatabasePreventsPendingRecovery() throws Exception {
        File data = this.temporary.newFolder("data");
        File snapshot = new File(this.temporary.newFolder("snapshot-root"), "rocks");
        RocksDBSessions owner = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                       data.toString(), data.toString());
        try {
            owner.createSnapshot(snapshot.toString());
            RocksDBSnapshotRestore restore = new RocksDBSnapshotRestore(
                    data.toString(), data.toString(), snapshot.toString(),
                    new RocksDBSnapshotRestore.FileOperations());
            restore.begin();
            byte[] manifest = Files.readAllBytes(new File(data, "CURRENT").toPath());
            try {
                new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                       data.toString(), data.toString());
                fail("Must not restore under an open native owner");
            } catch (RocksDBException expected) {
                assertTrue(expected.getMessage().contains("No locks available"));
                assertEquals(Status.Code.IOError, expected.getStatus().getCode());
                assertArrayEquals(manifest, Files.readAllBytes(new File(data, "CURRENT").toPath()));
                assertTrue(owner.databaseOpened());
                assertTrue(snapshot.isDirectory());
            }
        } finally {
            owner.forceCloseRocksDB();
        }
        RocksDBSessions recovered = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                           data.toString(), data.toString());
        recovered.forceCloseRocksDB();
        assertFalse(new File(data + ".resume-pending").exists());
    }

    @Test
    public void testRepeatedAdapterSnapshotResumePreservesConsumeSemantics() throws Exception {
        File root = this.temporary.newFolder("adapter");
        // createCheckpoint's existing assertion requires a snapshot-prefixed parent.
        File data = new File(root, "snapshot-data/store");
        File wal = new File(root, "wal");
        RocksDBSessions sessions = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                          data.toString(), wal.toString());
        RocksDBStore store = new RocksDBStore.RocksDBGraphStore(new RocksDBStoreProvider(), "db", "store");
        Whitebox.setInternalState(store, "sessions", sessions);
        Map<String, RocksDBSessions> databases = Whitebox.getInternalState(store, "dbs");
        databases.put(data.toString(), sessions);
        byte[] key = new byte[]{1};
        try {
            sessions.createTable("test");
            sessions.session().put("test", key, new byte[]{2});
            sessions.session().commit();
            store.createSnapshot("snapshot");
            String snapshot = sessions.buildSnapshotPath("snapshot");
            for (boolean consume : new boolean[]{false, false, true}) {
                sessions.session().put("test", key, new byte[]{3});
                sessions.session().commit();
                store.resumeSnapshot("snapshot", consume);
                assertArrayEquals(new byte[]{2}, sessions.session().get("test", key));
                assertEquals(!consume, new File(snapshot).exists());
                assertFalse(new File(snapshot + ".resume-lock").exists());
                assertEquals(!consume, new File(snapshot).getParentFile().exists());
                assertEquals(0, data.getParentFile().listFiles(file -> file.isDirectory() &&
                             file.getName().startsWith("store_temp-")).length);
                assertFalse(new File(data + ".resume-pending").exists());
            }
        } finally {
            sessions.close();
        }
    }

    @Test
    public void testMissingManifestRejectedBeforeDataMutation() throws Exception {
        File data = this.temporary.newFolder("data");
        File snapshot = this.temporary.newFolder("snapshot");
        File sentinel = new File(data, "live");
        Files.write(sentinel.toPath(), new byte[]{42});
        try {
            RocksDBSnapshotRestore.start(data.toString(), data.toString(), snapshot.toString());
            fail("Incomplete snapshot must not be installed");
        } catch (IOException expected) {
            assertArrayEquals(new byte[]{42}, Files.readAllBytes(sentinel.toPath()));
            assertFalse(new File(data + ".resume-pending").exists());
        }
    }

    @Test
    public void testNativeOpenFailureKeepsRecoverySourceAndMarker() throws Exception {
        File data = this.temporary.newFolder("data");
        File snapshot = this.temporary.newFolder("snapshot");
        fakeCheckpoint(snapshot);
        RocksDBSnapshotRestore.start(data.toString(), data.toString(), snapshot.toString());
        try {
            new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store", data.toString(), data.toString());
            fail("Invalid native manifest must not report success");
        } catch (org.rocksdb.RocksDBException expected) {
            assertTrue(new File(data + ".resume-pending").isFile());
            assertArrayEquals(new byte[]{1, 2, 3},
                              Files.readAllBytes(new File(snapshot, "MANIFEST-000001").toPath()));
        }
    }

    @Test
    public void testMetadataChangesRejectBeforeRestoringMissingWalAlias() throws Exception {
        for (String field : Arrays.asList("wal", "wal-link-target-0", "generation", "unexpected", "missing-wal")) {
            File root = this.temporary.newFolder("metadata-" + field);
            File data = new File(root, "data");
            File snapshot = new File(root, "snapshot");
            File target = new File(root, "wal-target");
            Files.createDirectories(data.toPath());
            Files.createDirectories(snapshot.toPath());
            Files.createDirectories(target.toPath());
            fakeCheckpoint(snapshot);
            Path alias = new File(root, "wal-link").toPath();
            Files.createSymbolicLink(alias, target.toPath());
            Path sentinel = new File(data, "untouched").toPath();
            Files.write(sentinel, new byte[]{9});
            RocksDBSnapshotRestore.start(data.toString(), alias.toString(), snapshot.toString());
            Path marker = new File(data + ".resume-pending").toPath();
            Properties state = readRecoveryMetadata(marker);
            if ("missing-wal".equals(field)) {
                state.remove("wal");
            } else {
                state.setProperty(field, "generation".equals(field) ? String.join("", Collections.nCopies(64, "a")) :
                                         new File(root, "different-target").getCanonicalPath());
            }
            writeRecoveryMetadata(marker, state);
            byte[] damaged = Files.readAllBytes(marker);
            Files.delete(alias);
            try {
                RocksDBSnapshotRestore.prepareOpen(data.toString(), alias.toString());
                fail("Changed metadata must fail before alias reconstruction: " + field);
            } catch (BackendException expected) {
                assertTrue(expected.getCause().getMessage().contains("metadata checksum"));
            }
            assertFalse(Files.exists(alias, java.nio.file.LinkOption.NOFOLLOW_LINKS));
            assertArrayEquals(new byte[]{9}, Files.readAllBytes(sentinel));
            assertArrayEquals(damaged, Files.readAllBytes(marker));
            assertTrue(snapshot.exists());
        }
    }

    @Test
    public void testValidMetadataCanRestoreMissingWalAlias() throws Exception {
        File root = this.temporary.newFolder("metadata-valid-alias");
        File data = new File(root, "data");
        File snapshot = new File(root, "snapshot");
        File target = new File(root, "wal-target");
        Files.createDirectories(data.toPath());
        Files.createDirectories(snapshot.toPath());
        Files.createDirectories(target.toPath());
        fakeCheckpoint(snapshot);
        Files.write(new File(snapshot, "000001.log").toPath(), new byte[]{5});
        Path alias = new File(root, "wal-link").toPath();
        Files.createSymbolicLink(alias, target.toPath());
        RocksDBSnapshotRestore.start(data.toString(), alias.toString(), snapshot.toString());
        Path marker = new File(data + ".resume-pending").toPath();
        // Re-serialization changes comments/order/escaping, not the property values.
        writeRecoveryMetadata(marker, readRecoveryMetadata(marker));
        Files.delete(alias);
        RocksDBSnapshotRestore.start(data.toString(), alias.toString(), snapshot.toString());
        RocksDBSnapshotRestore recovered = RocksDBSnapshotRestore.prepareOpen(data.toString(), alias.toString());
        assertNotNull(recovered);
        assertTrue(Files.isSymbolicLink(alias));
        assertEquals(target.toPath().toRealPath(), alias.toRealPath());
        assertArrayEquals(new byte[]{5}, Files.readAllBytes(new File(target, "000001.log").toPath()));
        assertTrue(snapshot.exists());
        assertTrue(Files.exists(marker));
    }

    @Test
    public void testRecoveryKeepsFilesystemMeaningOfSymlinkThenParent() throws Exception {
        File root = this.temporary.newFolder("metadata-symlink-parent");
        File data = new File(root, "data");
        File snapshot = new File(root, "snapshot");
        Path target = new File(root, "physical/child").toPath();
        Path realWal = target.getParent().resolve("logs");
        Path lexicalWal = new File(root, "logs").toPath();
        Files.createDirectories(data.toPath());
        Files.createDirectories(snapshot.toPath());
        Files.createDirectories(target);
        Files.createDirectories(realWal);
        Files.createDirectories(lexicalWal);
        Path alias = new File(root, "link").toPath();
        Files.createSymbolicLink(alias, target);
        Path configuredWal = alias.resolve("../logs");
        assertEquals(realWal.toRealPath(), configuredWal.toRealPath());
        assertFalse(configuredWal.toRealPath().equals(configuredWal.normalize().toRealPath()));
        fakeCheckpoint(snapshot);
        Files.write(new File(snapshot, "000001.log").toPath(), new byte[]{5});
        Files.write(realWal.resolve("000002.log"), new byte[]{6});
        Files.write(lexicalWal.resolve("000002.log"), new byte[]{7});
        RocksDBSnapshotRestore.start(data.toString(), configuredWal.toString(), snapshot.toString());
        RocksDBSnapshotRestore recovered =
                RocksDBSnapshotRestore.prepareOpen(data.toString(), configuredWal.toString());
        assertNotNull(recovered);
        assertArrayEquals(new byte[]{5}, Files.readAllBytes(realWal.resolve("000001.log")));
        assertFalse(Files.exists(realWal.resolve("000002.log")));
        assertArrayEquals(new byte[]{7}, Files.readAllBytes(lexicalWal.resolve("000002.log")));
        assertFalse(Files.exists(lexicalWal.resolve("000001.log")));
    }

    @Test
    public void testExistingMarkerRequiresChecksumAndRetainsOperation() throws Exception {
        File data = this.temporary.newFolder("metadata-existing-data");
        File snapshot = this.temporary.newFolder("metadata-existing-snapshot");
        fakeCheckpoint(snapshot);
        RocksDBSnapshotRestore original = new RocksDBSnapshotRestore(
                data.toString(), data.toString(), snapshot.toString(), new RocksDBSnapshotRestore.FileOperations());
        original.begin();
        Path marker = new File(data + ".resume-pending").toPath();
        byte[] valid = Files.readAllBytes(marker);
        new RocksDBSnapshotRestore(data.toString(), data.toString(), snapshot.toString(),
                                   new RocksDBSnapshotRestore.FileOperations()).begin();
        assertArrayEquals(valid, Files.readAllBytes(marker));
        Properties state = readRecoveryMetadata(marker);
        state.remove("metadata-sha256");
        writeRecoveryMetadata(marker, state);
        byte[] legacy = Files.readAllBytes(marker);
        try {
            new RocksDBSnapshotRestore(data.toString(), data.toString(), snapshot.toString(),
                                       new RocksDBSnapshotRestore.FileOperations()).begin();
            fail("Existing legacy marker must not receive an invented checksum");
        } catch (IOException expected) {
            assertTrue(expected.getMessage().contains("metadata checksum"));
        }
        try {
            RocksDBSnapshotRestore.start(data.toString(), data.toString(), snapshot.toString());
            fail("Public retry must reject legacy marker");
        } catch (IOException expected) {
            assertTrue(expected.getMessage().contains("metadata checksum"));
        }
        try {
            RocksDBSnapshotRestore.prepareOpen(data.toString(), data.toString());
            fail("Opening must reject legacy marker");
        } catch (BackendException expected) {
            assertTrue(expected.getCause().getMessage().contains("metadata checksum"));
        }
        assertArrayEquals(legacy, Files.readAllBytes(marker));
        assertTrue(snapshot.exists());
    }

    private static Properties readRecoveryMetadata(Path marker) throws IOException {
        Properties state = new Properties();
        try (java.io.InputStream input = Files.newInputStream(marker)) {
            state.load(input);
        }
        return state;
    }

    private static void writeRecoveryMetadata(Path marker, Properties state) throws IOException {
        try (java.io.OutputStream output = Files.newOutputStream(marker)) {
            state.store(output, "re-serialized test metadata");
        }
    }

    @Test
    public void testInvalidOperationDoesNotReconstructWalAlias() throws Exception {
        File data = this.temporary.newFolder("data");
        File snapshot = this.temporary.newFolder("snapshot");
        fakeCheckpoint(snapshot);
        Path wal = new File(this.temporary.getRoot(), "wal-link").toPath();
        Files.createSymbolicLink(wal, this.temporary.newFolder("wal-target").toPath());
        RocksDBSnapshotRestore.start(data.toString(), wal.toString(), snapshot.toString());
        Path marker = new File(data + ".resume-pending").toPath();
        String metadata = Files.readString(marker);
        Files.writeString(marker, metadata.replaceAll("operation=[^\n]+", "operation=invalid"));
        Files.delete(wal);
        try {
            RocksDBSnapshotRestore.prepareOpen(data.toString(), wal.toString());
            fail("Invalid operation must fail before restoring any path");
        } catch (BackendException expected) {
            assertFalse(Files.exists(wal, java.nio.file.LinkOption.NOFOLLOW_LINKS));
            assertTrue(Files.exists(marker));
        }
    }

    @Test
    public void testReplacedCheckpointGenerationIsRejectedBeforeTargetChanges() throws Exception {
        File data = this.temporary.newFolder("data");
        File snapshot = this.temporary.newFolder("snapshot");
        fakeCheckpoint(snapshot);
        Files.write(new File(data, "untouched").toPath(), new byte[]{9});
        RocksDBSnapshotRestore.start(data.toString(), data.toString(), snapshot.toString());
        File saved = new File(this.temporary.getRoot(), "saved-generation");
        Files.move(snapshot.toPath(), saved.toPath());
        FileUtils.copyDirectory(saved, snapshot);
        Files.write(new File(snapshot, "MANIFEST-000001").toPath(), new byte[]{4, 5, 6});
        try {
            RocksDBSnapshotRestore.prepareOpen(data.toString(), data.toString());
            fail("A replacement at the same path must not become the pending source");
        } catch (BackendException expected) {
            assertArrayEquals(new byte[]{9}, Files.readAllBytes(new File(data, "untouched").toPath()));
            assertTrue(new File(data + ".resume-pending").exists());
            assertTrue(saved.exists());
        }
    }

    @Test
    public void testPartialMarkerWriteLeavesNativeDataAndCheckpointRetryable() throws Exception {
        File root = this.temporary.newFolder("partial-marker");
        File data = new File(root, "data");
        File wal = new File(root, "wal");
        File snapshot = new File(root, "snapshot");
        RocksDBStdSessions original = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                            data.toString(), wal.toString());
        try {
            original.createTable("test");
            original.session().put("test", new byte[]{1}, new byte[]{2});
            original.session().put("test", new byte[]{6}, new byte[]{7});
            original.session().commit();
            AtomicReference<OpenedRocksDB> shared = Whitebox.getInternalState(original, "rocksdb");
            try (Checkpoint checkpoint = Checkpoint.create(shared.get().rocksdb())) {
                checkpoint.createCheckpoint(snapshot.toString());
            }
            original.session().put("test", new byte[]{1}, new byte[]{3});
            original.session().commit();
        } finally {
            original.close();
        }
        Map<Path, byte[]> before = new HashMap<>();
        for (File directory : Arrays.asList(data, wal, snapshot)) {
            try (java.util.stream.Stream<Path> paths = Files.walk(directory.toPath())) {
                for (Path path : paths.filter(Files::isRegularFile).collect(java.util.stream.Collectors.toList())) {
                    before.put(path, Files.readAllBytes(path));
                }
            }
        }
        runRecoveryProcess(data, wal, snapshot, "marker-partial", 0);
        assertFalse(new File(data + ".resume-pending").exists());
        assertEquals(0, root.listFiles(file -> file.getName().startsWith("data.resume-pending.staging-")).length);
        for (Map.Entry<Path, byte[]> entry : before.entrySet()) {
            assertArrayEquals(entry.getKey().toString(), entry.getValue(), Files.readAllBytes(entry.getKey()));
        }
        // With the IO fault gone, a fresh JVM opens the live value and retries the same checkpoint.
        runRecoveryProcess(data, wal, snapshot, "marker-retry", 0);
        runRecoveryProcess(data, wal, snapshot, "finish", 0);
    }

    @Test
    public void testKilledRestoreRetriesSameGenerationAndReclaimsOnlyOwnedStaging() throws Exception {
        for (String phase : Arrays.asList("stage", "retire", "publish", "reopen")) {
            File root = this.temporary.newFolder(phase);
            File data = new File(root, "data");
            File wal = new File(root, "wal");
            File snapshot = new File(root, "snapshot");
            HugeConfig config = FakeObjects.newConfig();
            RocksDBStdSessions original = new RocksDBStdSessions(config, "db", "store",
                                                                data.toString(), wal.toString());
            try {
                original.createTable("test");
                original.session().put("test", new byte[]{1}, new byte[]{2});
                original.session().commit();
                AtomicReference<OpenedRocksDB> shared = Whitebox.getInternalState(original, "rocksdb");
                try (Checkpoint checkpoint = Checkpoint.create(shared.get().rocksdb())) {
                    // A native checkpoint flushes SSTs; append a captured real WAL tail below.
                    checkpoint.createCheckpoint(snapshot.toString());
                }
                original.session().put("test", new byte[]{6}, new byte[]{7});
                original.session().commit();
                shared.get().rocksdb().flushWal(true);
                File[] tails = wal.listFiles(file -> file.getName().matches("[0-9]+\\.log"));
                assertNotNull(tails);
                assertTrue("The native database must produce a real WAL tail", tails.length > 0);
                for (File tail : tails) {
                    FileUtils.copyFile(tail, new File(snapshot, tail.getName()));
                }
                original.session().put("test", new byte[]{1}, new byte[]{3});
                original.session().commit();
            } finally {
                original.close();
            }
            File foreign = new File(data + ".resume-staging-foreign");
            FileUtils.forceMkdir(foreign);
            Files.write(new File(foreign, "keep").toPath(), new byte[]{7});
            runRecoveryProcess(data, wal, snapshot, phase, 31);
            assertTrue(new File(data + ".resume-pending").exists());
            // Interrupt the SAME operation again; no new staging namespace is allocated.
            runRecoveryProcess(data, wal, snapshot, "reopen", 31);
            runRecoveryProcess(data, wal, snapshot, "finish", 0);
            assertFalse(new File(data + ".resume-pending").exists());
            assertFalse(snapshot.exists());
            assertArrayEquals(new byte[]{7}, Files.readAllBytes(new File(foreign, "keep").toPath()));
            assertEquals(1, root.listFiles(file -> file.getName().startsWith("data.resume-staging-")).length);
        }
    }

    private static void runRecoveryProcess(File data, File wal, File snapshot, String phase,
                                           int expectedExit) throws Exception {
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        File output = new File(data.getParentFile(), "child-" + phase + "-" + System.nanoTime() + ".log");
        Process child = new ProcessBuilder(new File(System.getProperty("java.home"), "bin/java").toString(),
                                           "-cp", classpath, RocksDBSnapshotRestoreTest.class.getName(),
                                           data.toString(), wal.toString(), snapshot.toString(), phase)
                        .redirectErrorStream(true).redirectOutput(output).start();
        try {
            assertTrue("Recovery child timed out: " + output, child.waitFor(30, TimeUnit.SECONDS));
            assertEquals(Files.readString(output.toPath()), expectedExit, child.exitValue());
        } finally {
            child.destroyForcibly();
        }
    }

    private static void recoverChild(String[] args) throws Exception {
        String phase = args[3];
        if ("marker-partial".equals(phase)) {
            try (RecoveryLock held = RocksDBSnapshotRestore.lock(args[0])) {
                RocksDBSnapshotRestore restore = new RocksDBSnapshotRestore(args[0], args[1], args[2],
                        new RocksDBSnapshotRestore.FileOperations() {
                            @Override
                            OutputStream markerOutput(Path staged) throws IOException {
                                return new FilterOutputStream(super.markerOutput(staged)) {
                                    @Override
                                    public void write(byte[] bytes, int offset, int length) throws IOException {
                                        // Commit real bytes, then fail while Properties.store is still flushing.
                                        this.out.write(bytes, offset, Math.min(32, length));
                                        this.out.flush();
                                        assertTrue(Files.size(staged) > 0);
                                        throw new IOException("injected partial marker write");
                                    }
                                };
                            }
                        });
                try {
                    restore.begin();
                    fail("Partial marker IO must fail before installing data");
                } catch (IOException expected) {
                    assertEquals("injected partial marker write", expected.getMessage());
                }
            }
            return;
        }
        if ("marker-retry".equals(phase)) {
            RocksDBStdSessions fresh = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                              args[0], args[1], Collections.singletonList("test"));
            try {
                assertArrayEquals(new byte[]{3}, fresh.session().get("test", new byte[]{1}));
                fresh.resumeSnapshot(args[2]);
                assertArrayEquals(new byte[]{2}, fresh.session().get("test", new byte[]{1}));
                assertArrayEquals(new byte[]{7}, fresh.session().get("test", new byte[]{6}));
            } finally {
                fresh.close();
            }
            return;
        }
        if ("finish".equals(phase)) {
            RocksDBStdSessions fresh = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                              args[0], args[1], Collections.singletonList("test"));
            try {
                assertArrayEquals(new byte[]{2}, fresh.session().get("test", new byte[]{1}));
                assertArrayEquals(new byte[]{7}, fresh.session().get("test", new byte[]{6}));
                fresh.session().put("test", new byte[]{4}, new byte[]{5});
                fresh.session().commit();
                assertArrayEquals(new byte[]{5}, fresh.session().get("test", new byte[]{4}));
            } finally {
                fresh.close();
            }
            return;
        }
        try (RecoveryLock held = RocksDBSnapshotRestore.lock(args[0])) {
            RocksDBSnapshotRestore.start(args[0], args[1], args[2]);
            if ("reopen".equals(phase)) {
                RocksDBSnapshotRestore.prepareOpen(args[0], args[1]);
            } else {
                RocksDBSnapshotRestore restore = new RocksDBSnapshotRestore(args[0], args[1], args[2],
                        new RocksDBSnapshotRestore.FileOperations() {
                            @Override
                            void copyFile(File source, File target) throws IOException {
                                super.copyFile(source, target);
                                if ("stage".equals(phase)) {
                                    Runtime.getRuntime().halt(31);
                                }
                            }

                            @Override
                            void move(Path source, Path target) throws IOException {
                                super.move(source, target);
                                if (("retire".equals(phase) && target.toString().endsWith(".aside")) ||
                                    ("publish".equals(phase) && target.toString().endsWith(".log"))) {
                                    Runtime.getRuntime().halt(31);
                                }
                            }
                        });
                restore.begin();
                restore.install();
                throw new AssertionError("Did not reach requested kill boundary: " + phase);
            }
            // Halt bypasses finally and native reopening: this is a process interruption.
            Runtime.getRuntime().halt(31);
        }
    }

    @Test
    public void testRecoveryLockExcludesAnotherProcess() throws Exception {
        File data = this.temporary.newFolder("data");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        try (RecoveryLock owner = RocksDBSnapshotRestore.lock(data.toString())) {
            Process child = new ProcessBuilder(new File(System.getProperty("java.home"), "bin/java").toString(),
                                               "-cp", classpath, getClass().getName(), data.toString())
                            .redirectErrorStream(true).start();
            try {
                assertTrue("Competing process did not finish", child.waitFor(10, TimeUnit.SECONDS));
                String output = new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                assertEquals(output, 23, child.exitValue());
                assertEquals(0, data.list().length);
            } finally {
                child.destroyForcibly();
            }
        }
    }

    @Test
    public void testExternalOwnerRejectsBothPublicOpenPaths() throws Exception {
        File data = this.temporary.newFolder("external-owner");
        File ready = new File(this.temporary.getRoot(), "owner-ready");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process child = new ProcessBuilder(new File(System.getProperty("java.home"), "bin/java").toString(),
                                           "-cp", classpath, getClass().getName(), data.toString(),
                                           ready.toString()).redirectErrorStream(true).start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!ready.exists() && child.isAlive() && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertTrue("External owner did not acquire lock", ready.exists());
            for (boolean withTables : new boolean[]{false, true}) {
                try {
                    if (withTables) {
                        new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                               data.toString(), data.toString(), Collections.emptyList());
                    } else {
                        new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                               data.toString(), data.toString());
                    }
                    fail("Public open must reject a competing process before native recovery");
                } catch (RocksDBException expected) {
                    assertTrue(expected instanceof RocksDBStdSessions.RecoveryLockException);
                    assertTrue(((RocksDBStdSessions.RecoveryLockException) expected).isContention());
                    assertEquals(Status.Code.IOError, expected.getStatus().getCode());
                    assertTrue(expected.getMessage().contains("No locks available"));
                    assertEquals(0, data.list().length);
                }
            }
            child.getOutputStream().write('\n');
            child.getOutputStream().flush();
            assertTrue("External owner did not close", child.waitFor(10, TimeUnit.SECONDS));
            assertEquals(0, child.exitValue());
        } finally {
            child.destroyForcibly();
        }
        try (RecoveryLock retry = RocksDBSnapshotRestore.lock(data.toString())) {
            assertTrue(retry.isOpen());
        }
    }

    public static void main(String[] args) {
        if (args.length == 4) {
            try {
                recoverChild(args);
                return;
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
        try (RecoveryLock channel = RocksDBSnapshotRestore.lock(args[0])) {
            if (args.length == 2) {
                Files.write(new File(args[1]).toPath(), new byte[]{1});
                System.in.read();
            } else {
                System.out.println("acquired");
            }
        } catch (BackendException expected) {
            System.exit(23);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testCachedOwnerCopyCannotOverlapNativeReload() throws Exception {
        File data = this.temporary.newFolder("copy-reload");
        CountDownLatch checked = new CountDownLatch(1);
        CountDownLatch allowCopy = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<RocksDBSessions> copied = new AtomicReference<>();
        RocksDBStdSessions owner = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                         data.toString(), data.toString()) {
            @Override
            public boolean databaseOpened() {
                boolean opened = super.databaseOpened();
                checked.countDown();
                try {
                    assertTrue("Copy check was not released", allowCopy.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return opened;
            }

            @Override
            public RocksDBSessions copy(HugeConfig config, String database, String store) {
                assertTrue("Native owner closed between its open check and copy", super.databaseOpened());
                return super.copy(config, database, store);
            }
        };
        RocksDBStore store = new RocksDBStore.RocksDBGraphStore(new RocksDBStoreProvider(), "db", "store");
        Map<String, RocksDBSessions> databases = Whitebox.getInternalState(store, "dbs");
        databases.put(data.toString(), owner);
        Thread open = new Thread(() -> {
            try {
                copied.set(store.open(FakeObjects.newConfig(), data.toString(), data.toString(),
                                      Collections.singletonList("test")));
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        }, "cached-owner-copy");
        Thread reload = new Thread(() -> {
            try {
                owner.reloadRocksDB();
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        }, "cached-owner-reload");
        try {
            owner.createTable("test");
            owner.session().put("test", new byte[]{1}, new byte[]{2});
            owner.session().commit();
            open.start();
            assertTrue("Open did not reach the cached owner check", checked.await(10, TimeUnit.SECONDS));
            reload.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (reload.getState() != Thread.State.BLOCKED && reload.isAlive() &&
                   System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals("Native reload must wait for the owner check and copy", Thread.State.BLOCKED,
                         reload.getState());
            allowCopy.countDown();
            open.join(10000);
            reload.join(10000);
            assertFalse(open.isAlive());
            assertFalse(reload.isAlive());
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            assertNotNull(copied.get());
            assertArrayEquals(new byte[]{2}, owner.session().get("test", new byte[]{1}));
        } finally {
            allowCopy.countDown();
            open.join(10000);
            reload.join(10000);
            owner.forceCloseRocksDB();
        }
    }

    @Test
    public void testNativeCloseToRestoreTransitionRetainsSameLock() throws Exception {
        File data = this.temporary.newFolder("data");
        File snapshot = new File(this.temporary.newFolder("snapshot-root"), "rocks");
        RocksDBSessions sessions = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                          data.toString(), data.toString());
        RecoveryLock transferred = null;
        try {
            sessions.createTable("test");
            sessions.session().put("test", new byte[]{1}, new byte[]{2});
            sessions.session().commit();
            sessions.createSnapshot(snapshot.toString());
            AtomicReference<OpenedRocksDB> shared = Whitebox.getInternalState(sessions, "rocksdb");
            OpenedRocksDB old = shared.get();
            transferred = old.closeForRestore();
            RecoveryLock held = transferred;
            assertFalse(old.isOwningHandle());
            assertTrue(held.isOpen());
            RocksDBStore store = new RocksDBStore.RocksDBGraphStore(new RocksDBStoreProvider(), "db", "store");
            Map<String, RocksDBSessions> databases = Whitebox.getInternalState(store, "dbs");
            databases.put(data.toString(), sessions);
            try {
                store.open(FakeObjects.newConfig(), data.toString(), data.toString(),
                           Collections.singletonList("test"));
                fail("Cached owner must not be copied while native close/restore is in progress");
            } catch (ConnectionException expected) {
                assertFalse(sessions.databaseOpened());
                assertTrue(held.isOpen());
            }
            // Deterministic observation of the exact native-closed/marker-not-yet-created window.
            try {
                new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                       data.toString(), data.toString());
                fail("Native close must not release recovery ownership");
            } catch (RocksDBException expected) {
                assertTrue(expected.getMessage().contains("No locks available"));
                assertEquals(Status.Code.IOError, expected.getStatus().getCode());
                assertFalse(new File(data + ".resume-pending").exists());
            }
            // Native LOCK is gone here: the recovery lease alone must still exclude another JVM,
            // including after the two same-JVM contender paths above have failed.
            assertExternalLock(data.toString(), 23);
            RocksDBSnapshotRestore.start(data.toString(), data.toString(), snapshot.toString());
            old.close();
            assertTrue("Closing the detached old handle must retain the transferred lock", held.isOpen());
            OpenedRocksDB replacement = Whitebox.invokeStatic(
                    RocksDBStdSessions.class,
                    new Class<?>[]{HugeConfig.class, List.class, String.class, String.class, RecoveryLock.class},
                    "openRocksDB", FakeObjects.newConfig(), Collections.emptyList(),
                    data.toString(), data.toString(), held);
            shared.set(replacement);
            assertSame(held, Whitebox.getInternalState(replacement, "recoveryLock"));
            assertArrayEquals(new byte[]{2}, sessions.session().get("test", new byte[]{1}));
        } finally {
            try {
                sessions.close();
            } finally {
                RocksDBSnapshotRestore.unlock(transferred);
            }
        }
        try (RecoveryLock available = RocksDBSnapshotRestore.lock(data.toString())) {
            assertTrue(available.isOpen());
        }
    }

    @Test
    public void testSameJvmAliasesDoNotReleaseIndependentProcessLock() throws Exception {
        this.checkSameJvmAliases(false);
    }

    @Test
    public void testMissingFileKeyUsesPhysicalIdentityWithoutOpeningAnotherDescriptor() throws Exception {
        this.checkSameJvmAliases(true);
    }

    private void checkSameJvmAliases(boolean withoutFileKey) throws Exception {
        File parent = this.temporary.newFolder();
        File data = new File(parent, "data");
        Files.createDirectory(data.toPath());
        Path symlinkParent = new File(this.temporary.getRoot(), "symlink-" + parent.getName()).toPath();
        Files.createSymbolicLink(symlinkParent, parent.toPath());
        File hardlinkData = this.temporary.newFolder();
        List<String> aliases = Arrays.asList(new File(parent, "./data").toString(),
                                             symlinkParent.resolve("data").toString(),
                                             hardlinkData.toString());
        try (RecoveryLock owner = RocksDBSnapshotRestore.lock(data.toString())) {
            if (withoutFileKey) {
                // The JDK permits null keys; keep the actual file/channel and exercise isSameFile.
                Whitebox.setInternalState(owner, "identity", null);
            }
            Files.createLink(new File(hardlinkData + ".resume-lock").toPath(),
                             new File(data + ".resume-lock").toPath());
            for (String alias : aliases) {
                assertSameJvmContended(alias);
                assertTrue(owner.isOpen());
                assertExternalLock(alias, 23);
            }
            assertEquals(0, data.list().length);
            assertEquals(0, hardlinkData.list().length);
        }
        for (String alias : aliases) {
            assertExternalLock(alias, 0);
        }
    }

    /** Explicit external Docker/kernel fixture: bind the same parent at both paths. */
    @Test
    public void testParentBindAliasRetainsIndependentProcessExclusion() throws Exception {
        String original = System.getProperty("hugegraph.test.recovery.parent.original");
        String alias = System.getProperty("hugegraph.test.recovery.parent.alias");
        org.junit.Assume.assumeTrue("requires the explicit parent-bind kernel fixture",
                                   original != null && alias != null);
        Path data = new File(original, "data").toPath();
        Files.createDirectories(data);
        String aliasData = new File(alias, "data").toString();
        try (RecoveryLock owner = RocksDBSnapshotRestore.lock(data.toString())) {
            assertSameJvmContended(aliasData);
            assertTrue(owner.isOpen());
            assertExternalLock(aliasData, 23);
        }
        assertExternalLock(aliasData, 0);
    }

    private static void assertSameJvmContended(String data) {
        try (RecoveryLock contender = RocksDBSnapshotRestore.lock(data)) {
            fail("The physical file already has a same-JVM owner: " + data);
        } catch (BackendException expected) {
            assertTrue(expected.getCause() instanceof OverlappingFileLockException);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static void assertExternalLock(String data, int expectedExit) throws Exception {
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process child = new ProcessBuilder(new File(System.getProperty("java.home"), "bin/java").toString(),
                                           "-cp", classpath, RocksDBSnapshotRestoreTest.class.getName(), data)
                        .redirectErrorStream(true).start();
        try {
            assertTrue("Independent JVM did not finish", child.waitFor(10, TimeUnit.SECONDS));
            String output = new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(output, expectedExit, child.exitValue());
        } finally {
            child.destroyForcibly();
            assertTrue("Independent JVM did not stop", child.waitFor(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testCopiesShareReloadedNativeOwnerAndLock() throws Exception {
        this.checkSharedReplacement(false);
    }

    @Test
    public void testCopiesShareRestoredNativeOwnerAndLock() throws Exception {
        this.checkSharedReplacement(true);
    }

    private void checkSharedReplacement(boolean restore) throws Exception {
        for (boolean replaceThroughCopy : new boolean[]{false, true}) {
            for (boolean closeReplacementFirst : new boolean[]{false, true}) {
                File data = this.temporary.newFolder();
                File snapshot = new File(this.temporary.newFolder("snapshot-" + data.getName()), "rocks");
                HugeConfig config = FakeObjects.newConfig();
                RocksDBSessions owner = new RocksDBStdSessions(config, "db", "store",
                                                               data.toString(), data.toString());
                RocksDBSessions copy = null;
                try {
                    owner.createTable("test");
                    owner.session().put("test", new byte[]{1}, new byte[]{2});
                    owner.session().commit();
                    copy = owner.copy(config, "db", "copy");
                    assertArrayEquals(new byte[]{2}, copy.session().get("test", new byte[]{1}));
                    AtomicReference<OpenedRocksDB> shared = Whitebox.getInternalState(owner, "rocksdb");
                    assertSame(shared, Whitebox.getInternalState(copy, "rocksdb"));
                    OpenedRocksDB old = shared.get();
                    RecoveryLock held = Whitebox.getInternalState(old, "recoveryLock");
                    RocksDBSessions replacing = replaceThroughCopy ? copy : owner;
                    RocksDBSessions other = replaceThroughCopy ? owner : copy;
                    if (restore) {
                        owner.createSnapshot(snapshot.toString());
                        owner.session().put("test", new byte[]{1}, new byte[]{3});
                        owner.session().commit();
                        replacing.resumeSnapshot(snapshot.toString());
                    } else {
                        replacing.reloadRocksDB();
                    }
                    OpenedRocksDB replacement = shared.get();
                    assertFalse(old.isOwningHandle());
                    assertSame(held, Whitebox.getInternalState(replacement, "recoveryLock"));
                    assertArrayEquals(new byte[]{2}, owner.session().get("test", new byte[]{1}));
                    assertArrayEquals(new byte[]{2}, copy.session().get("test", new byte[]{1}));
                    old.close();
                    assertTrue("The old native wrapper must relinquish its lock", held.isOpen());

                    RocksDBSessions first = closeReplacementFirst ? replacing : other;
                    RocksDBSessions last = closeReplacementFirst ? other : replacing;
                    first.close();
                    assertTrue(last.databaseOpened());
                    assertTrue(replacement.isOwningHandle());
                    assertTrue(held.isOpen());
                    assertArrayEquals(new byte[]{2}, last.session().get("test", new byte[]{1}));
                    // A pending opener must fail before replacing files under the live last copy.
                    last.createSnapshot(snapshot.toString());
                    RocksDBSnapshotRestore.start(data.toString(), data.toString(), snapshot.toString());
                    byte[] current = Files.readAllBytes(new File(data, "CURRENT").toPath());
                    try {
                        new RocksDBStdSessions(config, "db", "contender", data.toString(), data.toString());
                        fail("The last shared native owner must retain recovery exclusion");
                    } catch (RocksDBStdSessions.RecoveryLockException expected) {
                        assertTrue(expected.isContention());
                        assertArrayEquals(current, Files.readAllBytes(new File(data, "CURRENT").toPath()));
                        assertTrue(new File(data + ".resume-pending").exists());
                        assertTrue(snapshot.isDirectory());
                    }
                    last.close();
                    assertFalse(replacement.isOwningHandle());
                    assertFalse(held.isOpen());
                    RocksDBSessions fresh = new RocksDBStdSessions(config, "db", "fresh",
                                                                   data.toString(), data.toString(),
                                                                   Collections.emptyList());
                    try {
                        assertArrayEquals(new byte[]{2}, fresh.session().get("test", new byte[]{1}));
                        assertFalse(new File(data + ".resume-pending").exists());
                        assertFalse(snapshot.exists());
                    } finally {
                        fresh.forceCloseRocksDB();
                    }
                } finally {
                    owner.forceCloseRocksDB();
                    if (copy != null) {
                        copy.forceCloseRocksDB();
                    }
                }
            }
        }
    }

    @Test
    public void testForceCloseReleasesNativeAndRecoveryOwnership() throws Exception {
        File data = this.temporary.newFolder("data");
        RocksDBSessions sessions = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                          data.toString(), data.toString());
        sessions.forceCloseRocksDB();
        assertFalse(sessions.databaseOpened());
        RocksDBSessions fresh = new RocksDBStdSessions(FakeObjects.newConfig(), "db", "store",
                                                       data.toString(), data.toString());
        fresh.forceCloseRocksDB();
    }

    @Test
    public void testRootAndLinuxMountPointRejectedBeforeCreatingRecoveryLock() {
        try {
            RocksDBSnapshotRestore.lock(File.listRoots()[0].toString());
            fail("Filesystem root must not be a store directory");
        } catch (BackendException expected) {
            assertTrue(expected.getCause().getMessage().contains("filesystem root"));
        }
        if (System.getProperty("os.name").startsWith("Linux")) {
            Path lock = new File("/proc.resume-lock").toPath();
            assertFalse(Files.exists(lock));
            try {
                RocksDBSnapshotRestore.lock("/proc");
                fail("Linux mount point must not be a store directory");
            } catch (BackendException expected) {
                assertTrue(expected.getCause().getMessage().contains("mount point"));
            }
            assertFalse(Files.exists(lock));
        }
    }

    @Test
    public void testDescriptorCloseRetainsAliasesWithoutBlockingOtherDatabases() throws Exception {
        File data = this.temporary.newFolder("closing-data");
        File unrelated = this.temporary.newFolder("unrelated-data");
        RecoveryLock owner = RocksDBSnapshotRestore.lock(data.toString());
        Path alias = this.temporary.newFolder("alias-data").toPath();
        Files.createLink(new File(alias + ".resume-lock").toPath(), new File(data + ".resume-lock").toPath());
        ClosingChannel channel = new ClosingChannel(Whitebox.getInternalState(owner, "channel"), null, true);
        Whitebox.setInternalState(owner, "channel", channel);
        FutureTask<Void> close = new FutureTask<>(() -> { owner.close(); return null; });
        Thread closer = new Thread(close, "descriptor-close-test");
        FutureTask<Void> probe = new FutureTask<>(() -> {
            assertSameJvmContended(alias.toString());
            try (RecoveryLock other = RocksDBSnapshotRestore.lock(unrelated.toString())) {
                assertTrue(other.isOpen());
            }
            return null;
        });
        Thread prober = new Thread(probe, "unrelated-open-test");
        try {
            closer.start();
            assertTrue(channel.entered.await(10, TimeUnit.SECONDS));
            assertFalse("JDK clears isOpen before the descriptor is closed", channel.isOpen());
            prober.start();
            probe.get(10, TimeUnit.SECONDS);
            assertExternalLock(alias.toString(), 23);
        } finally {
            channel.release.countDown();
            closer.join(10000L);
            prober.join(10000L);
            owner.close();
        }
        close.get(10, TimeUnit.SECONDS);
        owner.close();
        assertEquals(1, channel.closes);
        assertExternalLock(alias.toString(), 0);
    }

    @Test
    public void testDescriptorIOExceptionCannotReleaseReservationOnRepeatedClose() throws Exception {
        this.checkDescriptorCloseFailure(new IOException("injected descriptor close"));
    }

    @Test
    public void testDescriptorRuntimeExceptionCannotReleaseReservationOnRepeatedClose() throws Exception {
        this.checkDescriptorCloseFailure(new IllegalStateException("injected descriptor close"));
    }

    private void checkDescriptorCloseFailure(Exception failure) throws Exception {
        File data = this.temporary.newFolder();
        RecoveryLock owner = RocksDBSnapshotRestore.lock(data.toString());
        ClosingChannel channel = new ClosingChannel(Whitebox.getInternalState(owner, "channel"), failure, false);
        Whitebox.setInternalState(owner, "channel", channel);
        Path alias = this.temporary.newFolder().toPath();
        Files.createLink(new File(alias + ".resume-lock").toPath(), new File(data + ".resume-lock").toPath());
        try {
            for (int i = 0; i < 2; i++) {
                try {
                    owner.close();
                    fail("Failed close must remain failed");
                } catch (IOException | RuntimeException expected) {
                    assertSame(failure, expected);
                }
                assertSameJvmContended(alias.toString());
                assertExternalLock(alias.toString(), 23);
            }
            assertEquals(1, channel.closes);
            RuntimeException primary = new IllegalStateException("open failed");
            RocksDBSnapshotRestore.unlock(owner, primary);
            assertEquals(1, primary.getSuppressed().length);
            Throwable suppressed = primary.getSuppressed()[0];
            assertSame(failure, failure instanceof IOException ? suppressed.getCause() : suppressed);
        } finally {
            // The injected failure intentionally left the real descriptor open.
            // Only the fixture, after real close, may remove its failed reservation.
            channel.delegate.close();
            Set<RecoveryLock> owners = Whitebox.getInternalState(RecoveryLock.class, "OWNERS");
            synchronized (owners) {
                owners.remove(owner);
            }
        }
        assertExternalLock(alias.toString(), 0);
    }

    @Test
    public void testNativeDisposalBlocksCloseAndRetainsLeaseThroughTransfer() throws Exception {
        File data = this.temporary.newFolder();
        RecoveryLock owner = RocksDBSnapshotRestore.lock(data.toString());
        ClosingRocksDB rocksdb = new ClosingRocksDB(data.toString(), null, true);
        OpenedRocksDB opened = new OpenedRocksDB(rocksdb, new HashMap<>(), null, owner);
        FutureTask<RecoveryLock> transfer = new FutureTask<>(opened::closeForRestore);
        FutureTask<Void> close = new FutureTask<>(() -> { opened.close(); return null; });
        Thread transferring = new Thread(transfer, "native-transfer-test");
        Thread closing = new Thread(close, "native-close-test");
        try {
            transferring.start();
            assertTrue(rocksdb.entered.await(10, TimeUnit.SECONDS));
            assertFalse("JNI ownership flips before native disposal finishes", opened.isOwningHandle());
            assertSameJvmContended(data.toString());
            assertExternalLock(data.toString(), 23);
            closing.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (closing.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals("Concurrent close must reach the owner monitor", Thread.State.BLOCKED, closing.getState());
            try {
                close.get(100, TimeUnit.MILLISECONDS);
                fail("Concurrent close must wait for native disposal and lease transfer");
            } catch (TimeoutException expected) {
                assertTrue(owner.isOpen());
            }
            rocksdb.release.countDown();
            assertSame(owner, transfer.get(10, TimeUnit.SECONDS));
            close.get(10, TimeUnit.SECONDS);
            assertTrue(owner.isOpen());
            assertExternalLock(data.toString(), 23);
        } finally {
            rocksdb.release.countDown();
            transferring.join(10000L);
            closing.join(10000L);
            opened.close();
            owner.close();
        }
    }

    @Test
    public void testNativeDisposalFailureCannotReleaseOrTransferLease() throws Exception {
        File data = this.temporary.newFolder();
        RuntimeException failure = new IllegalStateException("injected native disposal failure");
        RecoveryLock owner = RocksDBSnapshotRestore.lock(data.toString());
        ClosingRocksDB rocksdb = new ClosingRocksDB(data.toString(), failure, false);
        OpenedRocksDB opened = new OpenedRocksDB(rocksdb, new HashMap<>(), null, owner);
        try {
            for (Runnable operation : Arrays.<Runnable>asList(opened::close, opened::close, opened::closeForRestore)) {
                try {
                    operation.run();
                    fail("Native close failure must remain failed");
                } catch (RuntimeException expected) {
                    assertSame(failure, expected);
                }
                assertFalse(opened.isOwningHandle());
                assertTrue(owner.isOpen());
                assertSameJvmContended(data.toString());
                assertExternalLock(data.toString(), 23);
            }
            assertEquals(1, rocksdb.disposals);
            RuntimeException primary = new IllegalStateException("marker completion failed");
            Whitebox.invokeStatic(RocksDBStdSessions.class,
                                 new Class<?>[]{OpenedRocksDB.class, RecoveryLock.class, Throwable.class},
                                 "closeFailedOpen", opened, owner, primary);
            assertSame(failure, primary.getSuppressed()[0]);
            assertTrue(owner.isOpen());
        } finally {
            rocksdb.finishDisposal();
            owner.close();
        }
    }

    private static void awaitClose(CountDownLatch release) throws IOException {
        try {
            if (!release.await(10, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for injected close");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private static class ClosingRocksDB extends RocksDB {

        private final RuntimeException failure;
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release;
        private int disposals;

        ClosingRocksDB(String data, RuntimeException failure, boolean block) throws RocksDBException {
            super(openHandle(data));
            this.failure = failure;
            this.release = new CountDownLatch(block ? 1 : 0);
        }

        private static long openHandle(String data) throws RocksDBException {
            RocksDB.loadLibrary();
            try (Options options = new Options().setCreateIfMissing(true)) {
                RocksDB original = RocksDB.open(options, data);
                Whitebox.invoke(RocksDB.class, "disOwnNativeHandle", original);
                return original.getNativeHandle();
            }
        }

        @Override
        protected void disposeInternal(long handle) {
            this.disposals++;
            this.entered.countDown();
            try {
                awaitClose(this.release);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            if (this.failure != null) {
                throw this.failure;
            }
            super.disposeInternal(handle);
        }

        void finishDisposal() {
            super.disposeInternal(this.nativeHandle_);
        }
    }

    private static class ClosingChannel extends FileChannel {

        private final FileChannel delegate;
        private final Exception failure;
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release;
        private int closes;

        ClosingChannel(FileChannel delegate, Exception failure, boolean block) {
            this.delegate = delegate;
            this.failure = failure;
            this.release = new CountDownLatch(block ? 1 : 0);
        }

        @Override
        protected void implCloseChannel() throws IOException {
            this.closes++;
            this.entered.countDown();
            awaitClose(this.release);
            if (this.failure instanceof IOException) {
                throw (IOException) this.failure;
            }
            if (this.failure instanceof RuntimeException) {
                throw (RuntimeException) this.failure;
            }
            this.delegate.close();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return this.delegate.read(dst);
        }

        @Override
        public long read(ByteBuffer[] dst, int offset, int length) throws IOException {
            return this.delegate.read(dst, offset, length);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            return this.delegate.write(src);
        }

        @Override
        public long write(ByteBuffer[] src, int offset, int length) throws IOException {
            return this.delegate.write(src, offset, length);
        }

        @Override
        public long position() throws IOException {
            return this.delegate.position();
        }

        @Override
        public FileChannel position(long position) throws IOException {
            return this.delegate.position(position);
        }

        @Override
        public long size() throws IOException {
            return this.delegate.size();
        }

        @Override
        public FileChannel truncate(long size) throws IOException {
            return this.delegate.truncate(size);
        }

        @Override
        public void force(boolean metadata) throws IOException {
            this.delegate.force(metadata);
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
            return this.delegate.transferTo(position, count, target);
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
            return this.delegate.transferFrom(src, position, count);
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            return this.delegate.read(dst, position);
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            return this.delegate.write(src, position);
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
            return this.delegate.map(mode, position, size);
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) throws IOException {
            return this.delegate.lock(position, size, shared);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return this.delegate.tryLock(position, size, shared);
        }
    }

    private static class FaultyFiles extends RocksDBSnapshotRestore.FileOperations {

        private final String fault;

        FaultyFiles(String fault) {
            this.fault = fault;
        }

        @Override
        void copyDirectory(File source, File target) throws IOException {
            if ("data-copy".equals(this.fault)) {
                throw new IOException("injected data copy failure");
            }
            super.copyDirectory(source, target);
            if ("corrupt-data-copy".equals(this.fault)) {
                Files.write(new File(target, "MANIFEST-000001").toPath(), new byte[]{3, 2, 1});
            }
        }

        @Override
        void copyFile(File source, File target) throws IOException {
            if ("copy".equals(this.fault)) {
                throw new IOException("injected copy failure");
            }
            super.copyFile(source, target);
            if ("short-copy".equals(this.fault)) {
                Files.write(target.toPath(), new byte[]{0});
            } else if ("corrupt-copy".equals(this.fault)) {
                Files.write(target.toPath(), new byte[(int) target.length()]);
            }
        }

        @Override
        void move(Path source, Path target) throws IOException {
            if (("retire".equals(this.fault) && target.toString().endsWith(".aside")) ||
                ("publish".equals(this.fault) && target.toString().endsWith(".log"))) {
                throw new IOException("injected " + this.fault + " failure");
            }
            super.move(source, target);
        }
    }
}
