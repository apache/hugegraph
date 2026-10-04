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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.RocksdbChangedListener;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * {@link RocksDBFactory} / {@link RocksDBSession} metadata-checkpoint and cloud-hydration behaviour.
 *
 * <ul>
 *   <li>{@link #testMetadataCheckpointFlushBehavior()} — the flushing checkpoint
 *       ({@code captureMetadataSnapshot} / RocksJava {@code Checkpoint.createCheckpoint}) DOES flush
 *       the memtable, creating a new live SST and firing {@code onTableFileCreated}.</li>
 *   <li>{@link #testOnDbOpeningRunsBeforeRocksDbOpensTheDirectory()} and
 *       {@link #testCreateGraphDbHydrationFailurePropagatesAndRemainsRetryable()} — the ordering
 *       contract between cloud pre-hydration ({@code onDBOpening}) and {@code RocksDB.open}, plus
 *       its failure/retry behaviour.</li>
 * </ul>
 */
public class RocksDBFactoryTest {

    @BeforeClass
    public static void init() {
        OptionSpace.register("rocksdb",
                             "org.apache.hugegraph.rocksdb.access.RocksDBOptions");
        RocksDBOptions.instance();
        Map<String, Object> configMap = new HashMap<>();
        // Large write buffer so the small writes below stay in the memtable (no natural flush) —
        // any new SST must therefore come from the capture itself. The hydration-ordering tests
        // write no data, so this size is harmless for them.
        configMap.put("rocksdb.write_buffer_size", String.valueOf(256L * 1024 * 1024));
        configMap.put("rocksdb.bloom_filter_bits_per_key", "10");
        RocksDBFactory.getInstance().setHugeConfig(new HugeConfig(new MapConfiguration(configMap)));
    }

    // =========================================================================
    // Flushing checkpoint: DOES flush the memtable (creates a new live SST)
    // =========================================================================

    /**
     * Empirical probe: {@code captureMetadataSnapshot} (RocksJava {@code Checkpoint.createCheckpoint})
     * flushes the memtable, creating a new live SST that fires {@code onTableFileCreated}. If so,
     * metadata sync on every SST would drive flush amplification — which is exactly why {@code wal}
     * mode uses the flush-free path exercised by the next test.
     */
    @Test
    public void testMetadataCheckpointFlushBehavior() throws Exception {
        Path parent = Files.createTempDirectory("hg-ckflush-");
        String dbName = "flushcheck";

        AtomicInteger created = new AtomicInteger(0);
        RocksdbChangedListener probe = new RocksdbChangedListener() {
            @Override
            public void onTableFileCreated(String db, String cf, String path, long size) {
                created.incrementAndGet();
            }
        };

        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.addRocksdbChangedListener(probe);
        RocksDBSession session;
        try {
            session = factory.createGraphDB(parent.toString(), dbName, 0);
            session.checkTable("t");
            SessionOperator op = session.sessionOp();
            op.prepare();
            for (int i = 0; i < 200; i++) {
                op.put("t", ("k" + i).getBytes(StandardCharsets.UTF_8),
                       ("v" + i).getBytes(StandardCharsets.UTF_8));
            }
            op.commit();

            int liveSstBefore = countSst(session.getDbPath());
            int eventsBefore = created.get();

            // Trigger a metadata checkpoint WITHOUT an explicit flush.
            RocksDBFactory.MetadataSnapshot snap = factory.captureMetadataSnapshot(dbName);
            // Give any async table-created event a moment to be delivered.
            long deadline = System.currentTimeMillis() + 1000L;
            while (created.get() == eventsBefore && System.currentTimeMillis() < deadline) {
                LockSupport.parkNanos(10_000_000L);
            }
            int liveSstAfter = countSst(session.getDbPath());
            int eventsAfter = created.get();
            if (snap != null) {
                snap.cleanup();
            }

            System.out.printf(
                    "%n[CHECKPOINT-FLUSH-PROBE] onTableFileCreated events: before=%d after=%d "
                    + "(delta=%d); live *.sst in db dir: before=%d after=%d (delta=%d)%n",
                    eventsBefore, eventsAfter, eventsAfter - eventsBefore,
                    liveSstBefore, liveSstAfter, liveSstAfter - liveSstBefore);

            // Document the observed behavior: the checkpoint flushes the memtable, so a new live
            // SST appears and onTableFileCreated fires. (If this fails with delta=0, the RocksDB
            // build does NOT flush on checkpoint and the amplification concern does not apply.)
            assertTrue(
                    "checkpoint is expected to flush the memtable, creating a new live SST "
                    + "(delta events=" + (eventsAfter - eventsBefore)
                    + ", delta sst=" + (liveSstAfter - liveSstBefore) + ")",
                    eventsAfter > eventsBefore || liveSstAfter > liveSstBefore);
        } finally {
            factory.removeRocksdbChangedListener(probe);
            try {
                factory.destroyGraphDB(dbName);
            } catch (Exception ignore) {
                // best-effort
            }
            deleteRecursively(parent.toFile());
        }
    }

    // =========================================================================
    // Hydration ordering: onDBOpening (cloud pre-hydration) runs BEFORE RocksDB.open
    // =========================================================================

    /**
     * Cloud disk-loss recovery downloads {@code CURRENT}/{@code MANIFEST}/SST into the DB directory
     * during {@code onDBOpening}. For RocksDB to open on the recovered data, hydration must run
     * <b>before</b> {@code RocksDB.open}; otherwise open creates a fresh empty DB (and a local
     * {@code CURRENT}) and the downloaded files are never loaded. This asserts that when
     * {@code onDBOpening} fires, RocksDB has NOT yet created its {@code CURRENT}.
     */
    @Test
    public void testOnDbOpeningRunsBeforeRocksDbOpensTheDirectory() throws Exception {
        Path parent = Files.createTempDirectory("hg-hydration-order-");
        String dbName = "recdb";

        AtomicBoolean hydrationCalled = new AtomicBoolean(false);
        AtomicBoolean currentExistedAtHydration = new AtomicBoolean(false);

        RocksdbChangedListener probe = new RocksdbChangedListener() {
            @Override
            public void onDBOpening(String db, String dbPath) {
                hydrationCalled.set(true);
                // If RocksDB already opened this dir, it will have written CURRENT. Its presence
                // here means hydration is running too late to influence the open.
                currentExistedAtHydration.set(new File(dbPath, "CURRENT").exists());
            }
        };

        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.addRocksdbChangedListener(probe);
        try {
            factory.createGraphDB(parent.toString(), dbName, 0);

            assertTrue("onDBOpening (cloud hydration) must be invoked during DB creation",
                       hydrationCalled.get());
            assertFalse("onDBOpening must run BEFORE RocksDB.open creates CURRENT — otherwise "
                        + "cloud-hydrated files are never loaded into the already-open DB and "
                        + "disk-loss recovery silently yields an empty database",
                        currentExistedAtHydration.get());
        } finally {
            factory.removeRocksdbChangedListener(probe);
            try {
                factory.destroyGraphDB(dbName);
            } catch (Exception ignore) {
                // best-effort
            }
            deleteRecursively(parent.toFile());
        }
    }

    @Test
    public void testCreateGraphDbHydrationFailurePropagatesAndRemainsRetryable() throws Exception {
        Path parent = Files.createTempDirectory("hg-hydration-fail-");
        String dbName = "failrec";

        RocksdbChangedListener boom = new RocksdbChangedListener() {
            @Override
            public void onDBOpening(String db, String dbPath) {
                throw new RuntimeException("simulated hydration failure");
            }
        };

        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.addRocksdbChangedListener(boom);
        try {
            factory.createGraphDB(parent.toString(), dbName, 0);
            throw new AssertionError("createGraphDB must propagate the hydration failure");
        } catch (RuntimeException expected) {
            // expected — hydration threw before open
        } finally {
            factory.removeRocksdbChangedListener(boom);
        }

        // The failed attempt must have cleared its pending-creation slot and left no half-open
        // session, so a subsequent open (no failing hook) succeeds instead of wedging on the slot.
        RocksDBSession recovered = null;
        try {
            recovered = factory.createGraphDB(parent.toString(), dbName, 0);
            assertNotNull("retry after hydration failure must succeed", recovered);
        } finally {
            if (recovered != null) {
                recovered.close();
            }
            try {
                factory.releaseGraphDB(dbName);
            } catch (Exception ignore) {
                // best-effort
            }
            deleteRecursively(parent.toFile());
        }
    }

    // =========================================================================
    // Destroy: a listener throwing from onDBDeleteBegin must HOLD the delete
    // =========================================================================

    /**
     * A listener may throw from {@code onDBDeleteBegin} to hold the delete — e.g. the cloud
     * listener cannot durably persist its anti-resurrection marker, so proceeding could let a crash
     * re-hydrate stale objects as live data. {@code destroyGraphDB} must fire that notification
     * BEFORE releasing the session or enqueuing the async destroy watcher, so the throw genuinely
     * holds the delete: the session stays registered (files not deleted, cloud not purged) and the
     * caller can retry once the underlying problem clears.
     */
    @Test
    public void testDestroyGraphDbHeldWhenDeleteBeginListenerThrows() throws Exception {
        Path parent = Files.createTempDirectory("hg-destroy-hold-");
        String dbName = "holddb";

        // A benign listener registered FIRST records that onDBDeleteBegin fired (and returns
        // normally); the throwing listener registered AFTER it then holds the delete. Ordering
        // matters: it proves the begin-notification runs, and that a later listener throwing still
        // holds the delete.
        AtomicBoolean beginNotified = new AtomicBoolean(false);
        RocksdbChangedListener recorder = new RocksdbChangedListener() {
            @Override
            public void onDBDeleteBegin(String db, String dbPath) {
                beginNotified.set(true);
            }
        };
        RocksdbChangedListener holdDelete = new RocksdbChangedListener() {
            @Override
            public void onDBDeleteBegin(String db, String dbPath) {
                throw new IllegalStateException(
                        "simulated: cannot durably persist anti-resurrection marker");
            }
        };

        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.createGraphDB(parent.toString(), dbName, 0);
        factory.addRocksdbChangedListener(recorder);
        factory.addRocksdbChangedListener(holdDelete);
        try {
            try {
                factory.destroyGraphDB(dbName);
                throw new AssertionError(
                        "destroyGraphDB must propagate the hold-the-delete exception");
            } catch (IllegalStateException expected) {
                // expected — the listener held the delete
            }

            assertTrue("onDBDeleteBegin must be fired before the delete is held", beginNotified.get());
            // The delete was held: the session was NOT released, so it is still registered and the
            // destroy can be retried. (Had the watcher been enqueued before the throw, the async
            // cleanup would delete the dir / purge cloud regardless — the bug this guards against.)
            assertTrue("a held delete must leave the session registered for retry",
                       factory.getGraphNames().contains(dbName));
        } finally {
            factory.removeRocksdbChangedListener(holdDelete);
            factory.removeRocksdbChangedListener(recorder);
            // With the throwing listener gone, the retry destroy proceeds normally.
            try {
                factory.destroyGraphDB(dbName);
            } catch (Exception ignore) {
                // best-effort
            }
            deleteRecursively(parent.toFile());
        }
    }

    // =========================================================================
    // getLiveSstFiles / captureMetadataSnapshot / flushSession / onReadMiss
    // =========================================================================

    @Test
    public void testGetLiveSstFilesReflectsFlushedSstsAndCfName() throws Exception {
        Path parent = Files.createTempDirectory("hg-livesst-");
        String dbName = "livesst";
        RocksDBFactory factory = RocksDBFactory.getInstance();
        try {
            RocksDBSession session = factory.createGraphDB(parent.toString(), dbName, 0);
            session.checkTable("t");
            SessionOperator op = session.sessionOp();
            op.prepare();
            for (int i = 0; i < 50; i++) {
                op.put("t", ("k" + i).getBytes(StandardCharsets.UTF_8),
                       ("v" + i).getBytes(StandardCharsets.UTF_8));
            }
            op.commit();
            // Explicit flush turns the memtable into at least one live SST.
            factory.flushSession(dbName, true);

            java.util.List<RocksDBFactory.LiveSstFile> live = factory.getLiveSstFiles(dbName);
            assertFalse("a flushed DB must report at least one live SST", live.isEmpty());
            for (RocksDBFactory.LiveSstFile f : live) {
                assertTrue("live file must be an absolute *.sst path: " + f.getAbsolutePath(),
                           f.getAbsolutePath().endsWith(".sst"));
                assertNotNull("column-family name must be populated", f.getCfName());
            }
            // Unknown DB → empty (never null); no session means nothing is live.
            assertTrue(factory.getLiveSstFiles("no-such-db").isEmpty());
        } finally {
            try {
                factory.destroyGraphDB(dbName);
            } catch (Exception ignore) {
                // best-effort
            }
            deleteRecursively(parent.toFile());
        }
    }

    @Test
    public void testCaptureMetadataSnapshotConsistentThenCleanup() throws Exception {
        Path parent = Files.createTempDirectory("hg-metasnap-");
        String dbName = "metasnap";
        RocksDBFactory factory = RocksDBFactory.getInstance();
        try {
            RocksDBSession session = factory.createGraphDB(parent.toString(), dbName, 0);
            session.checkTable("t");
            SessionOperator op = session.sessionOp();
            op.prepare();
            op.put("t", "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));
            op.commit();

            RocksDBFactory.MetadataSnapshot snap = factory.captureMetadataSnapshot(dbName);
            assertNotNull("a live DB must yield a metadata snapshot", snap);
            try {
                assertEquals("CURRENT", snap.getCurrentFileName());
                assertNotNull("snapshot must reference a MANIFEST", snap.getManifestFileName());
                assertTrue(snap.getManifestFileName().startsWith("MANIFEST-"));
                assertEquals(session.getDbPath(), snap.getDbDir());
                assertNotNull(snap.getTempDir());
                assertTrue("checkpoint temp dir must exist before cleanup",
                           new File(snap.getTempDir()).isDirectory());
                assertTrue("generation must be non-negative", snap.getGeneration() >= 0L);
                assertNotNull(snap.getOptionsFileNames());
                assertNotNull(snap.getSstFileNames());
            } finally {
                String tempDir = snap.getTempDir();
                snap.cleanup();
                assertFalse("cleanup() must remove the checkpoint temp dir",
                            new File(tempDir).exists());
            }

            // Unknown DB → null snapshot.
            assertNull(factory.captureMetadataSnapshot("no-such-db"));
        } finally {
            try {
                factory.destroyGraphDB(dbName);
            } catch (Exception ignore) {
                // best-effort
            }
            deleteRecursively(parent.toFile());
        }
    }

    @Test
    public void testFlushSessionUnknownDbIsNoopAndOnReadMissNullSessionIsFalse() {
        RocksDBFactory factory = RocksDBFactory.getInstance();
        // No session registered under this name: must be a quiet no-op, not an exception.
        factory.flushSession("no-such-db", true);
        factory.flushSession("no-such-db", false);
        // A null session short-circuits to "not hydrated".
        assertFalse(factory.onReadMiss(null, "t", "k".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testMetadataSnapshotConstructorsGettersAndGenerationParsing() {
        RocksDBFactory.MetadataSnapshot parsed = new RocksDBFactory.MetadataSnapshot(
                "/db", "/db_tmp", "CURRENT", "MANIFEST-000123",
                java.util.List.of("OPTIONS-000100"), java.util.List.of("000042.sst"));
        assertEquals("/db", parsed.getDbDir());
        assertEquals("/db_tmp", parsed.getTempDir());
        assertEquals("CURRENT", parsed.getCurrentFileName());
        assertEquals("MANIFEST-000123", parsed.getManifestFileName());
        assertEquals(java.util.List.of("OPTIONS-000100"), parsed.getOptionsFileNames());
        assertEquals(java.util.List.of("000042.sst"), parsed.getSstFileNames());
        assertEquals("generation is parsed from the MANIFEST-<n> name", 123L,
                     parsed.getGeneration());

        // Unparseable / absent manifest names yield -1.
        assertEquals(-1L, new RocksDBFactory.MetadataSnapshot(
                "/db", "/t", "CURRENT", "MANIFEST-xyz",
                java.util.List.of(), java.util.List.of()).getGeneration());
        assertEquals(-1L, new RocksDBFactory.MetadataSnapshot(
                "/db", "/t", null, null,
                java.util.List.of(), java.util.List.of()).getGeneration());

        // Explicit-generation constructor wins over name parsing.
        assertEquals(999L, new RocksDBFactory.MetadataSnapshot(
                "/db", "/t", "CURRENT", "MANIFEST-000123",
                java.util.List.of(), java.util.List.of(), 999L).getGeneration());

        // cleanup() with a null temp dir is a safe no-op.
        new RocksDBFactory.MetadataSnapshot("/db", null, "CURRENT", "MANIFEST-1",
                                            java.util.List.of(), java.util.List.of()).cleanup();
    }

    @Test
    public void testLiveSstFileGetters() {
        RocksDBFactory.LiveSstFile f =
                new RocksDBFactory.LiveSstFile("/data/db/000042.sst", "default");
        assertEquals("/data/db/000042.sst", f.getAbsolutePath());
        assertEquals("default", f.getCfName());
    }

    // =========================================================================
    // findLatestDBPath: DB-discovery filtering
    // =========================================================================

    /**
     * Non-directory entries (loose files) in the DB parent dir must be ignored by
     * {@code findLatestDBPath}. Before the fix, {@code File.listFiles()} returned plain files
     * alongside dirs; iterating over them without an {@code isDirectory()} guard would blow up on
     * {@code sFile.getName()} matching the prefix and then treating the file as a DB candidate.
     * Verified by asserting the resolved path is the default (unversioned) name, not a file entry.
     */
    @Test
    public void testFindLatestDbPath_ignoresNonDirectoryEntries() throws Exception {
        Path parent = Files.createTempDirectory("hg-find-latest-nondirs-");
        String dbName = "testdb";
        Path dbDir = parent.resolve(dbName);
        Files.createDirectories(dbDir);
        // Plant a plain file whose name starts with the DB prefix — must be ignored.
        Files.write(parent.resolve(dbName + "_stray.sst"), "noise".getBytes(StandardCharsets.UTF_8));

        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBSession session = factory.createGraphDB(parent.toString(), dbName);
        try {
            assertTrue("resolved path must live under the parent dir",
                       session.getDbPath().startsWith(parent.toString()));
            // The stray file must not have been treated as a versioned DB candidate.
            assertTrue("resolved path must be the default (unversioned) DB dir",
                       session.getDbPath().endsWith(dbName));
        } finally {
            factory.destroyGraphDB(dbName);
            deleteRecursively(parent.toFile());
        }
    }

    /**
     * Checkpoint temp dirs created by {@code captureMetadataSnapshot} carry the {@code _cloudmeta_}
     * marker in their name (pattern: {@code <dbName>_cloudmeta_<nanoTime>}). They must be skipped
     * by {@code findLatestDBPath} so an in-progress or crashed-leftover checkpoint dir is never
     * mistaken for a versioned DB candidate.
     */
    @Test
    public void testFindLatestDbPath_ignoresCloudmetaTempDirs() throws Exception {
        Path parent = Files.createTempDirectory("hg-find-latest-cloudmeta-");
        String dbName = "testdb";
        Path dbDir = parent.resolve(dbName);
        Files.createDirectories(dbDir);
        // Simulate a leftover checkpoint temp dir.
        Files.createDirectories(parent.resolve(dbName + "_cloudmeta_123456789"));

        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBSession session = factory.createGraphDB(parent.toString(), dbName);
        try {
            assertTrue("resolved path must be the default (unversioned) DB dir, not the checkpoint",
                       session.getDbPath().endsWith(dbName));
            assertFalse("checkpoint temp dir must not be chosen as the DB path",
                        session.getDbPath().contains("_cloudmeta_"));
        } finally {
            factory.destroyGraphDB(dbName);
            deleteRecursively(parent.toFile());
        }
    }

    /**
     * Dirs whose version suffix cannot be parsed as a long (e.g. {@code testdb_abc} or
     * {@code testdb_1_xyz}) must be silently skipped rather than throwing
     * {@link NumberFormatException} and aborting the DB open.
     */
    @Test
    public void testFindLatestDbPath_ignoresDirsWithNonNumericVersionSuffix() throws Exception {
        Path parent = Files.createTempDirectory("hg-find-latest-badver-");
        String dbName = "testdb";
        Path dbDir = parent.resolve(dbName);
        Files.createDirectories(dbDir);
        // Dirs with unparseable version suffixes — must be skipped, not throw.
        Files.createDirectories(parent.resolve(dbName + "_abc"));
        Files.createDirectories(parent.resolve(dbName + "_1_xyz"));

        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBSession session = factory.createGraphDB(parent.toString(), dbName);
        try {
            assertTrue("resolved path must be the default (unversioned) DB dir",
                       session.getDbPath().endsWith(dbName));
        } finally {
            factory.destroyGraphDB(dbName);
            deleteRecursively(parent.toFile());
        }
    }

    /**
     * When multiple valid versioned DB dirs exist alongside the default, {@code findLatestDBPath}
     * must select the one with the highest version number and delete the older ones.
     */
    @Test
    public void testFindLatestDbPath_picksHighestVersionAndDeletesOlder() throws Exception {
        Path parent = Files.createTempDirectory("hg-find-latest-versions-");
        String dbName = "testdb";
        // Create versioned dirs (no default, as happens after the first rename cycle).
        Path v1 = parent.resolve(dbName + "_1");
        Path v2 = parent.resolve(dbName + "_2");
        Path v3 = parent.resolve(dbName + "_3");
        Files.createDirectories(v1);
        Files.createDirectories(v2);
        Files.createDirectories(v3);

        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBSession session = factory.createGraphDB(parent.toString(), dbName);
        try {
            assertTrue("findLatestDBPath must resolve to the highest-versioned dir",
                       session.getDbPath().endsWith(dbName + "_3"));
            assertFalse("older versioned dirs must be deleted by findLatestDBPath",
                        v1.toFile().exists());
            assertFalse("older versioned dirs must be deleted by findLatestDBPath",
                        v2.toFile().exists());
        } finally {
            factory.destroyGraphDB(dbName);
            deleteRecursively(parent.toFile());
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static int countSst(String dir) {
        File[] files = new File(dir).listFiles((d, name) -> name.endsWith(".sst"));
        return files == null ? 0 : files.length;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void deleteRecursively(File dir) {
        if (dir == null || !dir.exists()) {
            return;
        }
        File[] children = dir.listFiles();
        if (children != null) {
            for (File c : children) {
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
