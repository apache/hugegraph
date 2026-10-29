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
import static org.mockito.Mockito.doAnswer;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.RocksdbChangedListener;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.rocksdb.AbstractEventListener.EnabledEventCallback;
import org.rocksdb.DBOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;
import org.rocksdb.Status;
import org.rocksdb.TableFileCreationInfo;
import org.rocksdb.TableFileDeletionInfo;

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

    /**
     * A second {@code createGraphDB} for a DB whose creation is still in flight (between Phase 1
     * and Phase 3) must spin-wait on the pending-creation slot rather than open the same path a
     * second time, then return a clone of the session the first caller registered. A waiter
     * interrupted while spinning must fail fast instead of waiting forever.
     */
    @Test
    public void testCreateGraphDbWaitsForInFlightCreationOfSameDb() throws Exception {
        Path parent = Files.createTempDirectory("hg-create-wait-");
        String dbName = "createwait";
        CountDownLatch hydrating = new CountDownLatch(1);
        CountDownLatch releaseHydration = new CountDownLatch(1);
        AtomicInteger openings = new AtomicInteger(0);
        RocksdbChangedListener gate = new RocksdbChangedListener() {
            @Override
            public void onDBOpening(String db, String dbPath) {
                if (!dbName.equals(db)) {
                    return;
                }
                openings.incrementAndGet();
                hydrating.countDown();
                try {
                    releaseHydration.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.addRocksdbChangedListener(gate);
        AtomicReference<RocksDBSession> created = new AtomicReference<>();
        AtomicReference<RocksDBSession> waited = new AtomicReference<>();
        try {
            Thread creator = new Thread(
                    () -> created.set(factory.createGraphDB(parent.toString(), dbName, 0)));
            creator.start();
            assertTrue("the first creation must reach hydration",
                       hydrating.await(30, TimeUnit.SECONDS));

            // An interrupted waiter gives up with a RuntimeException instead of spinning forever.
            AtomicReference<Throwable> interruptedError = new AtomicReference<>();
            Thread interrupted = new Thread(() -> {
                try {
                    factory.createGraphDB(parent.toString(), dbName, 0);
                } catch (Throwable t) {
                    interruptedError.set(t);
                }
            });
            interrupted.start();
            interrupted.interrupt();
            interrupted.join(30_000);
            assertNotNull("an interrupted waiter must fail", interruptedError.get());
            assertTrue(interruptedError.get().getMessage()
                                       .contains("Interrupted waiting for DB creation"));

            // A second waiter spins on the slot until the first creation registers the session.
            Thread waiter = new Thread(
                    () -> waited.set(factory.createGraphDB(parent.toString(), dbName, 0)));
            waiter.start();
            long deadline = System.currentTimeMillis() + 30_000;
            while (waiter.getState() != Thread.State.TIMED_WAITING &&
                   System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertEquals("the waiter must be spinning on the pending-creation slot",
                         Thread.State.TIMED_WAITING, waiter.getState());

            releaseHydration.countDown();
            creator.join(30_000);
            waiter.join(30_000);

            assertNotNull(created.get());
            assertNotNull("the waiter must receive the session the first caller created",
                          waited.get());
            assertEquals("the DB must be hydrated and opened only once", 1, openings.get());
        } finally {
            releaseHydration.countDown();
            factory.removeRocksdbChangedListener(gate);
            if (created.get() != null) {
                created.get().close();
            }
            if (waited.get() != null) {
                waited.get().close();
            }
            factory.destroyGraphDB(dbName);
            factory.processPendingDestroys();
            deleteRecursively(parent.toFile());
        }
    }

    /**
     * Phase 3 race guard: if a session for the same name was registered while this caller was
     * constructing its own (outside the lock), the duplicate must be shut down and the
     * already-registered session returned.
     */
    @Test
    public void testCreateGraphDbDiscardsDuplicateWhenSessionRegisteredDuringConstruction()
            throws Exception {
        Path parent = Files.createTempDirectory("hg-create-race-");
        String dbName = "createrace";
        RocksDBSession winner = mock(RocksDBSession.class);
        when(winner.clone()).thenReturn(winner);

        RocksDBFactory factory = RocksDBFactory.getInstance();
        Map<String, RocksDBSession> sessions = sessionMap(factory);
        RocksdbChangedListener racer = new RocksdbChangedListener() {
            @Override
            public void onDBOpening(String db, String dbPath) {
                if (dbName.equals(db)) {
                    sessions.put(dbName, winner);
                }
            }
        };
        factory.addRocksdbChangedListener(racer);
        try {
            RocksDBSession result = factory.createGraphDB(parent.toString(), dbName, 0);
            assertTrue("the already-registered session must win the race", result == winner);
            assertTrue(sessions.get(dbName) == winner);
        } finally {
            factory.removeRocksdbChangedListener(racer);
            sessions.remove(dbName, winner);
            deleteRecursively(parent.toFile());
        }
    }

    /**
     * Phase 3 failure: if registering the constructed session fails, the pending-creation slot
     * must be cleared and the half-registered session shut down, so a retry can open the DB
     * instead of spin-waiting on a leaked slot.
     */
    @Test
    public void testCreateGraphDbRegistrationFailureClearsSlotAndRemainsRetryable()
            throws Exception {
        Path parent = Files.createTempDirectory("hg-create-regfail-");
        String dbName = "createregfail";
        RocksDBFactory factory = RocksDBFactory.getInstance();
        Map<String, RocksDBSession> original = sessionMap(factory);
        Map<String, RocksDBSession> failing = new ConcurrentHashMap<String, RocksDBSession>(
                original) {
            @Override
            public RocksDBSession put(String key, RocksDBSession value) {
                if (dbName.equals(key)) {
                    throw new IllegalStateException("simulated registration failure");
                }
                return super.put(key, value);
            }
        };

        setSessionMap(factory, failing);
        try {
            factory.createGraphDB(parent.toString(), dbName, 0);
            throw new AssertionError("createGraphDB must propagate the registration failure");
        } catch (IllegalStateException expected) {
            // expected — Phase 3 could not register the session
        } finally {
            setSessionMap(factory, original);
        }

        RocksDBSession recovered = null;
        try {
            // The duplicate was shut down (releasing the RocksDB LOCK) and the slot cleared, so
            // the retry opens the same path without wedging or a LOCK-file error.
            recovered = factory.createGraphDB(parent.toString(), dbName, 0);
            assertNotNull("retry after a registration failure must succeed", recovered);
        } finally {
            if (recovered != null) {
                recovered.close();
            }
            factory.destroyGraphDB(dbName);
            factory.processPendingDestroys();
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
    // processPendingDestroys / logUnclosedIterators (the 60s scheduled cleanup pass)
    // =========================================================================

    @Test
    public void testProcessPendingDestroysDeletesFilesAndNotifiesListeners() throws Exception {
        Path parent = Files.createTempDirectory("hg-destroy-pass-");
        String dbName = "destroypass";
        List<String> deleted = new CopyOnWriteArrayList<>();
        RocksdbChangedListener recorder = new RocksdbChangedListener() {
            @Override
            public void onDBDeleted(String db, String dbPath) {
                deleted.add(db);
            }
        };

        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.addRocksdbChangedListener(recorder);
        try {
            RocksDBSession session = factory.createGraphDB(parent.toString(), dbName, 0);
            String dbPath = session.getDbPath();
            session.close();
            factory.destroyGraphDB(dbName);
            assertTrue("destroyGraphDB must enqueue a destroy watcher",
                       factory.findPathInRemovedList(dbPath));
            assertTrue(new File(dbPath).exists());

            factory.processPendingDestroys();

            assertFalse("the pass must delete the DB directory", new File(dbPath).exists());
            assertTrue("onDBDeleted must fire for the destroyed DB", deleted.contains(dbName));
            assertFalse("the watcher must be removed after a successful delete",
                        factory.findPathInRemovedList(dbPath));
        } finally {
            factory.removeRocksdbChangedListener(recorder);
            deleteRecursively(parent.toFile());
        }
    }

    /** A live DB at the SAME path as the stale watcher must be protected from its delete. */
    @Test
    public void testProcessPendingDestroysDiscardsWatcherWhenDbRecreatedAtSamePath()
            throws Exception {
        Path dir = Files.createTempDirectory("hg-destroy-samepath-");
        String dbName = "destroysamepath-" + System.nanoTime();
        RocksDBSession stale = mock(RocksDBSession.class);
        when(stale.getRefCount()).thenReturn(0);
        when(stale.getGraphName()).thenReturn(dbName);
        when(stale.getDbPath()).thenReturn(dir.toString());
        RocksDBSession live = mock(RocksDBSession.class);
        when(live.getDbPath()).thenReturn(dir.toString());
        List<String> deleted = new CopyOnWriteArrayList<>();
        RocksdbChangedListener recorder = new RocksdbChangedListener() {
            @Override
            public void onDBDeleted(String db, String dbPath) {
                deleted.add(db);
            }
        };

        RocksDBFactory factory = RocksDBFactory.getInstance();
        Map<String, RocksDBSession> sessions = sessionMap(factory);
        factory.addRocksdbChangedListener(recorder);
        sessions.put(dbName, live);
        RocksDBFactory.DBSessionWatcher watcher = factory.addDestroyWatcher(stale);
        try {
            factory.processPendingDestroys();

            assertFalse("the stale watcher must be discarded", factory.isDestroyPending(watcher));
            verify(stale, never()).shutdown();
            assertTrue("the live DB's files must not be deleted", dir.toFile().exists());
            assertFalse("onDBDeleted must not fire for a superseded watcher",
                        deleted.contains(dbName));
        } finally {
            sessions.remove(dbName, live);
            factory.removeRocksdbChangedListener(recorder);
            deleteRecursively(dir.toFile());
        }
    }

    /**
     * A DB recreated under the same name at a DIFFERENT path (a version-suffixed sibling) must not
     * stop the stale directory from being deleted and reported through {@code onDBDeleted}.
     */
    @Test
    public void testProcessPendingDestroysDeletesStaleDirWhenDbRecreatedAtDifferentPath()
            throws Exception {
        Path parent = Files.createTempDirectory("hg-destroy-recreate-");
        String dbName = "destroyrecreate";
        List<String> deleted = new CopyOnWriteArrayList<>();
        RocksdbChangedListener recorder = new RocksdbChangedListener() {
            @Override
            public void onDBDeleted(String db, String dbPath) {
                deleted.add(dbPath);
            }
        };

        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.addRocksdbChangedListener(recorder);
        try {
            RocksDBSession first = factory.createGraphDB(parent.toString(), dbName, 0);
            String stalePath = first.getDbPath();
            first.close();
            factory.destroyGraphDB(dbName);
            assertTrue(factory.findPathInRemovedList(stalePath));

            // Recreate under the same name before the cleanup pass runs; the removed path is
            // skipped so the new DB lives at a version-suffixed sibling.
            RocksDBSession second = factory.createGraphDB(parent.toString(), dbName, 0);
            String livePath = second.getDbPath();
            second.close();
            assertFalse(stalePath.equals(livePath));

            factory.processPendingDestroys();

            assertFalse("the stale watcher must be completed",
                        factory.findPathInRemovedList(stalePath));
            assertFalse("the stale directory must be deleted", new File(stalePath).exists());
            assertTrue("the recreated DB's files must not be deleted", new File(livePath).exists());
            assertTrue("onDBDeleted must fire with the stale path", deleted.contains(stalePath));
            assertFalse("onDBDeleted must not fire for the live path", deleted.contains(livePath));
            assertTrue(factory.getGraphNames().contains(dbName));
        } finally {
            factory.removeRocksdbChangedListener(recorder);
            factory.destroyGraphDB(dbName);
            factory.processPendingDestroys();
            deleteRecursively(parent.toFile());
        }
    }

    @Test
    public void testProcessPendingDestroysGivesUpAfterThreeFailedAttempts() throws Exception {
        Path dir = Files.createTempDirectory("hg-destroy-fail-");
        String dbName = "destroyfail-" + System.nanoTime();
        RocksDBSession session = mock(RocksDBSession.class);
        when(session.getRefCount()).thenReturn(0);
        when(session.getGraphName()).thenReturn(dbName);
        when(session.getDbPath()).thenReturn(dir.toString());
        doThrow(new IllegalStateException("simulated shutdown failure")).when(session).shutdown();
        List<String> deleted = new CopyOnWriteArrayList<>();
        RocksdbChangedListener recorder = new RocksdbChangedListener() {
            @Override
            public void onDBDeleted(String db, String dbPath) {
                deleted.add(db);
            }
        };

        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.addRocksdbChangedListener(recorder);
        try {
            RocksDBFactory.DBSessionWatcher watcher = factory.addDestroyWatcher(session);

            factory.processPendingDestroys();
            factory.processPendingDestroys();
            assertEquals(2, watcher.deleteAttempts);
            assertTrue("the watcher must be retried before the 3rd failure",
                       factory.isDestroyPending(watcher));

            factory.processPendingDestroys();
            assertEquals(3, watcher.deleteAttempts);
            assertFalse("the watcher must be dropped after the 3rd failure",
                        factory.isDestroyPending(watcher));
            assertTrue("a failed delete must leave the directory alone", dir.toFile().exists());
            assertFalse("onDBDeleted must not fire when the delete failed",
                        deleted.contains(dbName));
        } finally {
            factory.removeRocksdbChangedListener(recorder);
            deleteRecursively(dir.toFile());
        }
    }

    @Test
    public void testProcessPendingDestroysWaitsOnLiveRefsThenForceResetsAfterTimeout()
            throws Exception {
        Path dir = Files.createTempDirectory("hg-destroy-refs-");
        AtomicInteger refCount = new AtomicInteger(2);
        RocksDBSession session = mock(RocksDBSession.class);
        when(session.getRefCount()).thenAnswer(inv -> refCount.get());
        when(session.getGraphName()).thenReturn("destroyrefs-" + System.nanoTime());
        when(session.getDbPath()).thenReturn(dir.toString());
        doAnswer(inv -> {
            refCount.set(0);
            return null;
        }).when(session).forceResetRefCount();

        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBFactory.DBSessionWatcher watcher = factory.addDestroyWatcher(session);
        try {
            // Live refs inside the 30-min window: only waits (logs), no reset, no delete.
            factory.processPendingDestroys();
            verify(session, never()).forceResetRefCount();
            verify(session, never()).shutdown();
            assertTrue(factory.isDestroyPending(watcher));

            // Past the 30-min window: the stuck ref count is force-reset, still no delete yet.
            watcher.timestamp = System.currentTimeMillis() - 31 * 60 * 1000L;
            factory.processPendingDestroys();
            verify(session, times(1)).forceResetRefCount();
            verify(session, never()).shutdown();
            assertTrue(factory.isDestroyPending(watcher));

            // The next pass sees refCount 0 and completes the delete.
            factory.processPendingDestroys();
            verify(session, times(1)).shutdown();
            assertFalse(factory.isDestroyPending(watcher));
            assertFalse(dir.toFile().exists());
        } finally {
            refCount.set(0);
            factory.processPendingDestroys();
            deleteRecursively(dir.toFile());
        }
    }

    @Test
    public void testProcessPendingDestroysSwallowsUnexpectedErrorAndRetriesNextPass()
            throws Exception {
        Path dir = Files.createTempDirectory("hg-destroy-err-");
        RocksDBSession session = mock(RocksDBSession.class);
        // The first pass blows up outside the per-watcher try; the next pass proceeds normally.
        when(session.getRefCount()).thenThrow(new IllegalStateException("simulated"))
                                   .thenReturn(0);
        when(session.getGraphName()).thenReturn("destroyerr-" + System.nanoTime());
        when(session.getDbPath()).thenReturn(dir.toString());

        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBFactory.DBSessionWatcher watcher = factory.addDestroyWatcher(session);
        try {
            factory.processPendingDestroys();
            assertTrue("an unexpected error must not drop the watcher",
                       factory.isDestroyPending(watcher));

            factory.processPendingDestroys();
            assertFalse(factory.isDestroyPending(watcher));
            assertFalse(dir.toFile().exists());
        } finally {
            deleteRecursively(dir.toFile());
        }
    }

    @Test
    public void testLogUnclosedIteratorsToleratesStaleAndMalformedKeys() throws Exception {
        Path parent = Files.createTempDirectory("hg-iter-leak-");
        String dbName = "iterleak";
        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBSession session = factory.createGraphDB(parent.toString(), dbName, 0);
        try {
            // An iterator opened ~10 minutes 10 seconds ago falls in the "log it" window.
            long now = System.currentTimeMillis();
            String staleKey = (now - 610_000L) + "-stale";
            // Younger than 10 minutes, and past 10 minutes but off the 10s log beat: not logged.
            String freshKey = now + "-fresh";
            String offBeatKey = (now - 605_000L) + "-offbeat";
            session.addIterator(staleKey, null);
            session.addIterator(freshKey, null);
            session.addIterator(offBeatKey, null);
            factory.logUnclosedIterators();
            session.removeIterator(freshKey);
            session.removeIterator(offBeatKey);

            // A key without a numeric timestamp prefix must be swallowed, not propagate.
            session.addIterator("not-a-timestamp", null);
            factory.logUnclosedIterators();

            session.removeIterator(staleKey);
            session.removeIterator("not-a-timestamp");
        } finally {
            session.close();
            factory.destroyGraphDB(dbName);
            factory.processPendingDestroys();
            deleteRecursively(parent.toFile());
        }
    }

    // =========================================================================
    // Event-listener / reporting edge cases
    // =========================================================================

    @Test
    public void testOnTableFileCreatedSkipsFailedFileCreation() {
        TableFileCreationInfo info = mock(TableFileCreationInfo.class);
        when(info.getStatus()).thenReturn(new Status(Status.Code.IOError, Status.SubCode.None,
                                                     "simulated flush failure"));
        when(info.getFilePath()).thenReturn("/tmp/failed.sst");
        AtomicInteger created = new AtomicInteger(0);
        RocksdbChangedListener probe = new RocksdbChangedListener() {
            @Override
            public void onTableFileCreated(String db, String cf, String path, long size) {
                created.incrementAndGet();
            }
        };

        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.addRocksdbChangedListener(probe);
        try {
            factory.getEventListener().onTableFileCreated(info);
            assertEquals("a failed SST creation must not be propagated", 0, created.get());
        } finally {
            factory.removeRocksdbChangedListener(probe);
        }
    }

    @Test
    public void testOnTableFileDeletedPropagatesSuccessfulDeletion() {
        TableFileDeletionInfo info = mock(TableFileDeletionInfo.class);
        when(info.getStatus()).thenReturn(new Status(Status.Code.Ok, Status.SubCode.None, null));
        when(info.getDbName()).thenReturn("/data/deldb");
        when(info.getFilePath()).thenReturn("/data/deldb/000042.sst");
        List<String> deleted = new CopyOnWriteArrayList<>();
        RocksdbChangedListener probe = new RocksdbChangedListener() {
            @Override
            public void onTableFileDeleted(String db, String cf, String path) {
                assertNull("the column family is unknown for deletions", cf);
                deleted.add(db + "|" + path);
            }
        };

        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.addRocksdbChangedListener(probe);
        try {
            factory.getEventListener().onTableFileDeleted(info);
            assertEquals(List.of("/data/deldb|/data/deldb/000042.sst"), deleted);
        } finally {
            factory.removeRocksdbChangedListener(probe);
        }
    }

    @Test
    public void testOnTableFileDeletedSkipsFailedDeletion() {
        TableFileDeletionInfo info = mock(TableFileDeletionInfo.class);
        when(info.getStatus()).thenReturn(new Status(Status.Code.IOError, Status.SubCode.None,
                                                     "simulated delete failure"));
        when(info.getFilePath()).thenReturn("/tmp/failed.sst");
        AtomicInteger deleted = new AtomicInteger(0);
        RocksdbChangedListener probe = new RocksdbChangedListener() {
            @Override
            public void onTableFileDeleted(String db, String cf, String path) {
                deleted.incrementAndGet();
            }
        };

        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.addRocksdbChangedListener(probe);
        try {
            factory.getEventListener().onTableFileDeleted(info);
            assertEquals("a failed SST deletion must not be propagated", 0, deleted.get());
        } finally {
            factory.removeRocksdbChangedListener(probe);
        }
    }

    @Test
    public void testGetTotalKeyReportsOpenDb() throws Exception {
        Path parent = Files.createTempDirectory("hg-totalkey-");
        String dbName = "totalkey";
        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBSession session = factory.createGraphDB(parent.toString(), dbName, 0);
        try {
            Map<String, Long> totalKeys = factory.getTotalKey();
            assertTrue(totalKeys.containsKey(dbName));
            assertNotNull(totalKeys.get(dbName));
        } finally {
            session.close();
            factory.destroyGraphDB(dbName);
            factory.processPendingDestroys();
            deleteRecursively(parent.toFile());
        }
    }

    @Test
    public void testDefaultTruncateHooksAreNoOps() {
        RocksdbChangedListener listener = new RocksdbChangedListener() {
        };
        listener.onDBTruncateBegin("db", "/path");
        listener.onDBTruncated("db", "/path");
        listener.onDBTruncateAbort("db", "/path");
    }

    // =========================================================================
    // RocksDBSession: truncate notifications / metadata checkpoint failure
    // =========================================================================

    @Test
    public void testTruncateNotifiesBeginThenTruncated() throws Exception {
        Path parent = Files.createTempDirectory("hg-truncate-ok-");
        String dbName = "truncateok";
        List<String> events = new CopyOnWriteArrayList<>();
        RocksdbChangedListener recorder = truncateRecorder(dbName, events);

        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBSession session = factory.createGraphDB(parent.toString(), dbName, 0);
        factory.addRocksdbChangedListener(recorder);
        try {
            session.createTables("t1", "t2");
            session.truncate();

            assertEquals(List.of("begin", "truncated"), events);
            assertTrue("truncate must recreate the dropped tables",
                       session.tableIsExist("t1") && session.tableIsExist("t2"));
        } finally {
            factory.removeRocksdbChangedListener(recorder);
            session.close();
            factory.destroyGraphDB(dbName);
            factory.processPendingDestroys();
            deleteRecursively(parent.toFile());
        }
    }

    /**
     * A truncate that fails partway must still deliver a terminal notification — an abort, not a
     * "truncated" — so listeners leave their "truncating" state without purging remote data.
     */
    @Test
    public void testTruncateFailureNotifiesAbortInsteadOfTruncated() throws Exception {
        Path parent = Files.createTempDirectory("hg-truncate-fail-");
        String dbName = "truncatefail";
        List<String> events = new CopyOnWriteArrayList<>();
        RocksdbChangedListener recorder = truncateRecorder(dbName, events);

        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBSession session = factory.createGraphDB(parent.toString(), dbName, 0);
        factory.addRocksdbChangedListener(recorder);
        try {
            session.createTables("t1");
            RocksDBSession failing = spy(session);
            doThrow(new DBStoreException("simulated drop failure"))
                    .when(failing).dropTables(ArgumentMatchers.<String>any());

            try {
                failing.truncate();
                throw new AssertionError("truncate must propagate the drop failure");
            } catch (DBStoreException expected) {
                // expected
            }

            assertEquals(List.of("begin", "abort"), events);
        } finally {
            factory.removeRocksdbChangedListener(recorder);
            session.close();
            factory.destroyGraphDB(dbName);
            factory.processPendingDestroys();
            deleteRecursively(parent.toFile());
        }
    }

    /**
     * If the checkpoint cannot be created, the partial temp dir must be removed and the failure
     * surfaced as a {@link DBStoreException}. The failure is forced by making the directory that
     * holds the {@code _cloudmeta_} temp dir read-only (skipped where permissions are not
     * enforced, e.g. when running as root).
     */
    @Test
    public void testCaptureMetadataCheckpointFailureCleansUpAndThrows() throws Exception {
        Path parent = Files.createTempDirectory("hg-ckfail-");
        String dbName = "ckfail";
        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBSession session = factory.createGraphDB(parent.toString(), dbName, 0);
        File checkpointParent = new File(session.getDbPath()).getParentFile();
        try {
            assumeTrue("needs enforceable directory permissions",
                       checkpointParent.setWritable(false) && !checkpointParent.canWrite());
            try {
                session.captureMetadataCheckpoint();
                throw new AssertionError("captureMetadataCheckpoint must fail on a read-only dir");
            } catch (DBStoreException expected) {
                // expected
            }
            String[] leftovers = checkpointParent.list((d, name) -> name.contains("_cloudmeta_"));
            assertEquals("no partial checkpoint dir may be left behind",
                         0, leftovers == null ? 0 : leftovers.length);
        } finally {
            checkpointParent.setWritable(true);
            session.close();
            factory.destroyGraphDB(dbName);
            factory.processPendingDestroys();
            deleteRecursively(parent.toFile());
        }
    }

    // =========================================================================
    // Scheduled maintenance / destroy edge cases
    // =========================================================================

    @Test
    public void testRunMaintenancePassDrainsDestroyQueue() throws Exception {
        Path dir = Files.createTempDirectory("hg-maint-pass-");
        RocksDBSession session = mock(RocksDBSession.class);
        when(session.getRefCount()).thenReturn(0);
        when(session.getGraphName()).thenReturn("maintpass-" + System.nanoTime());
        when(session.getDbPath()).thenReturn(dir.toString());

        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBFactory.DBSessionWatcher watcher = factory.addDestroyWatcher(session);
        try {
            factory.runMaintenancePass();
            assertFalse(factory.isDestroyPending(watcher));
            assertFalse(dir.toFile().exists());
        } finally {
            deleteRecursively(dir.toFile());
        }
    }

    /**
     * While a creation of the same DB name is in flight (slot reserved in {@code pendingCreations}
     * but its path not yet known) the stale watcher must be kept and retried later: it must
     * neither delete the directory the creation may be about to open nor be dropped.
     */
    @Test
    public void testProcessPendingDestroysDefersWatcherWhileSameDbCreationInFlight()
            throws Exception {
        Path dir = Files.createTempDirectory("hg-destroy-inflight-");
        String dbName = "destroyinflight-" + System.nanoTime();
        RocksDBSession session = mock(RocksDBSession.class);
        when(session.getRefCount()).thenReturn(0);
        when(session.getGraphName()).thenReturn(dbName);
        when(session.getDbPath()).thenReturn(dir.toString());

        RocksDBFactory factory = RocksDBFactory.getInstance();
        Map<String, CompletableFuture<Void>> pending = pendingCreations(factory);
        RocksDBFactory.DBSessionWatcher watcher = factory.addDestroyWatcher(session);
        pending.put(dbName, new CompletableFuture<>());
        try {
            factory.processPendingDestroys();

            assertTrue("the watcher must be kept for a later round",
                       factory.isDestroyPending(watcher));
            verify(session, never()).shutdown();
            assertTrue("the in-flight DB's directory must not be deleted", dir.toFile().exists());

            // Once the creation slot is gone the deferred cleanup proceeds.
            pending.remove(dbName);
            factory.processPendingDestroys();
            assertFalse(factory.isDestroyPending(watcher));
            assertFalse(dir.toFile().exists());
        } finally {
            pending.remove(dbName);
            deleteRecursively(dir.toFile());
        }
    }

    @Test
    public void testShutdownQuietlySwallowsShutdownFailure() {
        RocksDBSession session = mock(RocksDBSession.class);
        when(session.getGraphName()).thenReturn("shutdownfail");
        doThrow(new IllegalStateException("simulated shutdown failure")).when(session).shutdown();

        RocksDBFactory.shutdownQuietly(session);

        verify(session, times(1)).shutdown();
    }

    @Test
    public void testDestroyGraphDbUnknownNameIsNoop() {
        RocksDBFactory factory = RocksDBFactory.getInstance();
        String dbName = "no-such-db-" + System.nanoTime();
        AtomicInteger deleteBegins = new AtomicInteger(0);
        RocksdbChangedListener probe = new RocksdbChangedListener() {
            @Override
            public void onDBDeleteBegin(String db, String dbPath) {
                deleteBegins.incrementAndGet();
            }
        };
        factory.addRocksdbChangedListener(probe);
        try {
            factory.destroyGraphDB(dbName);
            assertEquals("no listener may be notified for an unknown DB", 0, deleteBegins.get());
            assertFalse(factory.getGraphNames().contains(dbName));
        } finally {
            factory.removeRocksdbChangedListener(probe);
        }
    }

    // =========================================================================
    // Reporting helpers against stubbed sessions
    // =========================================================================

    @Test
    public void testFlushSessionSwallowsFlushFailure() throws Exception {
        String dbName = "flushfail-" + System.nanoTime();
        RocksDBSession session = mock(RocksDBSession.class);
        doThrow(new IllegalStateException("simulated flush failure")).when(session).flush(true);

        RocksDBFactory factory = RocksDBFactory.getInstance();
        Map<String, RocksDBSession> sessions = sessionMap(factory);
        sessions.put(dbName, session);
        try {
            factory.flushSession(dbName, true);
            verify(session, times(1)).flush(true);
        } finally {
            sessions.remove(dbName);
        }
    }

    @Test
    public void testLiveSstFilesAndMetadataSnapshotForSessionWithoutDb() throws Exception {
        String dbName = "nodb-" + System.nanoTime();
        RocksDBSession session = mock(RocksDBSession.class);
        when(session.getDB()).thenReturn(null);

        RocksDBFactory factory = RocksDBFactory.getInstance();
        Map<String, RocksDBSession> sessions = sessionMap(factory);
        sessions.put(dbName, session);
        try {
            assertTrue(factory.getLiveSstFiles(dbName).isEmpty());
            assertNull(factory.captureMetadataSnapshot(dbName));
        } finally {
            sessions.remove(dbName);
        }
    }

    /**
     * Only {@code *.sst} entries are reported, and the dir / file name are joined with exactly
     * one separator whether or not the dir has a trailing one or the name a leading one.
     */
    @Test
    public void testGetLiveSstFilesJoinsPathsAndSkipsNonSstEntries() throws Exception {
        String dbName = "livesst-" + System.nanoTime();
        RocksDB db = mock(RocksDB.class);
        List<LiveFileMetaData> metadata = List.of(
                liveFile("/data/db", "/000001.sst", "default"),
                liveFile("/data/db/", "/000002.sst", "default"),
                liveFile("/data/db", "000003.sst", "g+v"),
                liveFile("/data/db/", "000004.sst", "g+v"),
                liveFile("/data/db", "/MANIFEST-000005", "default"),
                liveFile("/data/db", null, "default"));
        when(db.getLiveFilesMetaData()).thenReturn(metadata);
        RocksDBSession session = mock(RocksDBSession.class);
        when(session.getDB()).thenReturn(db);

        RocksDBFactory factory = RocksDBFactory.getInstance();
        Map<String, RocksDBSession> sessions = sessionMap(factory);
        sessions.put(dbName, session);
        try {
            List<RocksDBFactory.LiveSstFile> files = factory.getLiveSstFiles(dbName);

            assertEquals(4, files.size());
            for (int i = 0; i < 4; i++) {
                assertEquals(new File("/data/db", String.format("00000%d.sst", i + 1)).getPath(),
                             files.get(i).getAbsolutePath());
            }
            assertEquals("default", files.get(0).getCfName());
            assertEquals("g+v", files.get(2).getCfName());
        } finally {
            sessions.remove(dbName);
        }
    }

    @Test
    public void testMetadataSnapshotNonManifestNameAndCleanupFailureAreTolerated()
            throws Exception {
        assertEquals(-1L, new RocksDBFactory.MetadataSnapshot(
                "/db", "/t", "CURRENT", "OPTIONS-000007",
                java.util.List.of(), java.util.List.of()).getGeneration());

        // A temp "dir" that is actually a regular file makes deleteDirectory throw; cleanup()
        // must log and carry on rather than propagate.
        Path notADir = Files.createTempFile("hg-ckpt-cleanup-", ".tmp");
        try {
            new RocksDBFactory.MetadataSnapshot("/db", notADir.toString(), "CURRENT",
                                                "MANIFEST-1", java.util.List.of(),
                                                java.util.List.of()).cleanup();
        } finally {
            Files.deleteIfExists(notADir);
        }
    }

    @Test
    public void testOnReadMissSwallowsFailingListenerAndKeepsOtherHydration() {
        RocksDBSession session = mock(RocksDBSession.class);
        when(session.getGraphName()).thenReturn("readmiss");
        RocksdbChangedListener failing = new RocksdbChangedListener() {
            @Override
            public boolean onReadMiss(RocksDBSession s, String table, byte[] key) {
                throw new IllegalStateException("simulated hydration failure");
            }
        };
        RocksdbChangedListener hydrating = new RocksdbChangedListener() {
            @Override
            public boolean onReadMiss(RocksDBSession s, String table, byte[] key) {
                return true;
            }
        };

        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.addRocksdbChangedListener(failing);
        factory.addRocksdbChangedListener(hydrating);
        try {
            assertTrue(factory.onReadMiss(session, "t", "k".getBytes(StandardCharsets.UTF_8)));
        } finally {
            factory.removeRocksdbChangedListener(failing);
            factory.removeRocksdbChangedListener(hydrating);
        }
    }

    @Test
    public void testDefaultFileAndReadMissHooksAreNoOps() {
        RocksdbChangedListener listener = new RocksdbChangedListener() {
        };
        listener.onTableFileCreated("db", "default", "/path/000001.sst", 1L);
        listener.onTableFileDeleted("db", "default", "/path/000001.sst");
        assertFalse(listener.onReadMiss(null, "t", "k".getBytes(StandardCharsets.UTF_8)));
    }

    // =========================================================================
    // RocksDBSession: construction / db-path discovery / checkpoint classification
    // =========================================================================

    @Test
    public void testSessionConstructorWithoutPreOpenHookOpensDb() throws Exception {
        Path parent = Files.createTempDirectory("hg-session-nohook-");
        String dbName = "nohook";
        RocksDBSession session = new RocksDBSession(RocksDBFactory.getInstance().getHugeConfig(),
                                                    parent.toString(), dbName, 0);
        try {
            assertNotNull(session.getDB());
            assertTrue(new File(session.getDbPath(), "CURRENT").exists());
        } finally {
            session.shutdown();
            deleteRecursively(parent.toFile());
        }
    }

    @Test
    public void testCloseQuietlySwallowsCloseFailure() {
        AtomicBoolean closed = new AtomicBoolean(false);
        RocksDBSession.closeQuietly(null);
        RocksDBSession.closeQuietly(() -> {
            closed.set(true);
            throw new IllegalStateException("simulated close failure");
        });
        assertTrue(closed.get());
    }

    /**
     * Snapshot temp dirs ({@code *_temp_}) and dirs with more than two version parts are not DB
     * candidates; among {@code <name>_<v1>_<v2>} dirs the highest {@code (v1, v2)} wins and the
     * older ones are deleted.
     */
    @Test
    public void testFindLatestDbPath_skipsTempAndMalformedDirsAndComparesTwoPartVersions()
            throws Exception {
        Path parent = Files.createTempDirectory("hg-find-latest-twopart-");
        String dbName = "testdb";
        Path older = Files.createDirectories(parent.resolve(dbName + "_1_2"));
        Path newer = Files.createDirectories(parent.resolve(dbName + "_1_5"));
        Path temp = Files.createDirectories(parent.resolve(dbName + "_9_temp_"));
        Path threePart = Files.createDirectories(parent.resolve(dbName + "_9_9_9"));

        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBSession session = factory.createGraphDB(parent.toString(), dbName);
        try {
            assertEquals(newer.toString(), session.getDbPath());
            assertFalse("the older two-part version must be deleted", older.toFile().exists());
            assertTrue("snapshot temp dirs must be left alone", temp.toFile().exists());
            assertTrue("malformed version dirs must be left alone", threePart.toFile().exists());
        } finally {
            session.close();
            factory.destroyGraphDB(dbName);
            factory.processPendingDestroys();
            deleteRecursively(parent.toFile());
        }
    }

    @Test
    public void testCategoriseCheckpointClassifiesFilesAndToleratesMissingDir() throws Exception {
        Path parent = Files.createTempDirectory("hg-categorise-");
        String dbName = "categorise";
        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBSession session = factory.createGraphDB(parent.toString(), dbName, 0);
        try {
            Path ckpt = Files.createDirectories(parent.resolve("ckpt"));
            for (String name : List.of("CURRENT", "MANIFEST-000007", "OPTIONS-000009",
                                       "000011.sst", "000012.log")) {
                Files.write(ckpt.resolve(name), new byte[0]);
            }

            RocksDBFactory.MetadataSnapshot snap = session.categoriseCheckpoint(ckpt.toString());
            assertEquals(session.getDbPath(), snap.getDbDir());
            assertEquals("CURRENT", snap.getCurrentFileName());
            assertEquals("MANIFEST-000007", snap.getManifestFileName());
            assertEquals(List.of("OPTIONS-000009"), snap.getOptionsFileNames());
            assertEquals("the WAL file must not be classified as an SST",
                         List.of("000011.sst"), snap.getSstFileNames());

            RocksDBFactory.MetadataSnapshot empty = session.categoriseCheckpoint(
                    parent.resolve("missing").toString());
            assertNull(empty.getCurrentFileName());
            assertNull(empty.getManifestFileName());
            assertTrue(empty.getOptionsFileNames().isEmpty());
            assertTrue(empty.getSstFileNames().isEmpty());
        } finally {
            session.close();
            factory.destroyGraphDB(dbName);
            factory.processPendingDestroys();
            deleteRecursively(parent.toFile());
        }
    }

    // =========================================================================
    // Remote-state gating: native listener and event mask
    // =========================================================================

    @Test
    public void testNativeListenerAttachedOnlyWhenRemoteStateListenerRegistered()
            throws Exception {
        RocksDBFactory factory = RocksDBFactory.getInstance();
        assumeTrue("another test leaked a remote-state listener",
                   !factory.hasRemoteStateListeners());
        Path parent = Files.createTempDirectory("hg-native-listener-");
        RocksdbChangedListener plain = new RocksdbChangedListener() {
        };
        RocksdbChangedListener remote = remoteStateListener();
        RocksDBSession withoutRemote = null;
        RocksDBSession withRemote = null;
        factory.addRocksdbChangedListener(plain);
        try {
            withoutRemote = factory.createGraphDB(parent.toString(), "nolistener", 0);
            assertFalse(factory.hasRemoteStateListeners());
            assertTrue("no native listener may be attached without a remote-state listener",
                       nativeListeners(withoutRemote).isEmpty());

            factory.addRocksdbChangedListener(remote);
            withRemote = factory.createGraphDB(parent.toString(), "withlistener", 0);
            assertTrue(factory.hasRemoteStateListeners());
            assertEquals("the native listener must be attached with a remote-state listener",
                         1, nativeListeners(withRemote).size());
        } finally {
            factory.removeRocksdbChangedListener(remote);
            factory.removeRocksdbChangedListener(plain);
            closeQuietly(withoutRemote);
            closeQuietly(withRemote);
            destroyAndCleanup(factory, parent, "nolistener", "withlistener");
        }
    }

    /** The native event mask must enable exactly the callbacks RocksdbEventListener overrides. */
    @Test
    public void testEventListenerEnablesExactlyTheOverriddenCallbacks() {
        Set<String> overridden = new HashSet<>();
        for (Method method : RocksDBFactory.RocksdbEventListener.class.getDeclaredMethods()) {
            if (method.getName().startsWith("on") && !method.isSynthetic()) {
                overridden.add(method.getName());
            }
        }
        Set<String> enabled = new HashSet<>();
        for (EnabledEventCallback callback : RocksDBFactory.ENABLED_EVENT_CALLBACKS) {
            StringBuilder name = new StringBuilder("on");
            for (String part : callback.name().substring("ON_".length()).split("_")) {
                name.append(part.charAt(0)).append(part.substring(1).toLowerCase());
            }
            enabled.add(name.toString());
        }
        assertFalse(overridden.isEmpty());
        assertEquals("enabled native callbacks must match the overridden on* methods",
                     overridden, enabled);
    }

    // =========================================================================
    // Stale checkpoint sweep, metadata capture, createGraphDB lifecycle
    // =========================================================================

    @Test
    public void testOpenSweepsStaleCloudMetaDirsOfSameGraphOnly() throws Exception {
        Path parent = Files.createTempDirectory("hg-sweep-");
        String dbName = "sweepdb";
        File stale = Files.createDirectories(parent.resolve(dbName + "_cloudmeta_123")).toFile();
        File staleVersioned = Files.createDirectories(parent.resolve(dbName + "_5_cloudmeta_9"))
                                   .toFile();
        File otherGraph = Files.createDirectories(parent.resolve(dbName + "x_cloudmeta_1")).toFile();

        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBSession session = null;
        try {
            session = factory.createGraphDB(parent.toString(), dbName, 0);
            assertFalse("stale checkpoint dir must be swept", stale.exists());
            assertFalse("stale versioned checkpoint dir must be swept", staleVersioned.exists());
            assertTrue("another graph's checkpoint dir must be kept", otherGraph.exists());
        } finally {
            closeQuietly(session);
            destroyAndCleanup(factory, parent, dbName);
        }
    }

    @Test
    public void testCaptureMetadataCheckpointOnClosedSessionFailsCleanly() throws Exception {
        Path parent = Files.createTempDirectory("hg-capture-closed-");
        try {
            RocksDBSession session = new RocksDBSession(
                    RocksDBFactory.getInstance().getHugeConfig(), parent.toString(),
                    "captureclosed", 0);
            session.close();
            try {
                session.captureMetadataCheckpoint();
                throw new AssertionError("capture on a closed session must fail");
            } catch (DBStoreException expected) {
                assertTrue(expected.getMessage().contains("closed"));
            }
        } finally {
            deleteRecursively(parent.toFile());
        }
    }

    @Test
    public void testCaptureMetadataSnapshotReleasesSessionReference() throws Exception {
        Path parent = Files.createTempDirectory("hg-capture-ref-");
        String dbName = "captureref";
        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksDBSession session = null;
        try {
            session = factory.createGraphDB(parent.toString(), dbName, 0);
            int before = session.getRefCount();
            RocksDBFactory.MetadataSnapshot snap = factory.captureMetadataSnapshot(dbName);
            assertNotNull(snap);
            snap.cleanup();
            assertEquals("the capture must release its session reference",
                         before, session.getRefCount());
        } finally {
            closeQuietly(session);
            destroyAndCleanup(factory, parent, dbName);
        }
    }

    @Test
    public void testResolveOpenPathRedirectsOnlyToSiblingsWithoutLocalDb() throws Exception {
        Path parent = Files.createTempDirectory("hg-open-redirect-");
        Path other = Files.createTempDirectory("hg-open-redirect-other-");
        String moved = "redirected";
        String rejected = "rejectedredirect";
        RocksDBFactory factory = RocksDBFactory.getInstance();
        Map<String, String> openedAt = new ConcurrentHashMap<>();
        RocksdbChangedListener redirector = new RocksdbChangedListener() {
            @Override
            public String resolveOpenPath(String db, String dbPath) {
                if (moved.equals(db)) {
                    return parent.resolve(moved + "_4").toString();
                }
                if (rejected.equals(db)) {
                    return other.resolve(rejected + "_4").toString(); // not a sibling
                }
                return dbPath;
            }

            @Override
            public void onDBOpening(String db, String dbPath) {
                openedAt.put(db, dbPath);
            }
        };
        factory.addRocksdbChangedListener(redirector);
        try {
            RocksDBSession session = factory.createGraphDB(parent.toString(), moved, 0);
            assertEquals(parent.resolve(moved + "_4").toString(), session.getDbPath());
            assertEquals("onDBOpening must see the redirected path",
                         session.getDbPath(), openedAt.get(moved));
            session.close();
            factory.releaseGraphDB(moved);

            // The directory now holds a DB (CURRENT), so the next open is not redirected again.
            openedAt.clear();
            RocksDBSession reopened = factory.createGraphDB(parent.toString(), moved, 0);
            assertEquals(parent.resolve(moved + "_4").toString(), reopened.getDbPath());
            reopened.close();
            factory.releaseGraphDB(moved);

            RocksDBSession kept = factory.createGraphDB(parent.toString(), rejected, 0);
            assertEquals(parent.resolve(rejected).toString(), kept.getDbPath());
            kept.close();
            factory.releaseGraphDB(rejected);
        } finally {
            factory.removeRocksdbChangedListener(redirector);
            deleteRecursively(parent.toFile());
            deleteRecursively(other.toFile());
        }
    }

    @Test
    public void testOnDbClosingFiresWhileSessionIsStillOpen() throws Exception {
        Path parent = Files.createTempDirectory("hg-db-closing-");
        String released = "closingrelease";
        String releasedAll = "closingreleaseall";
        RocksDBFactory factory = RocksDBFactory.getInstance();
        Map<String, Boolean> openAtClosing = new ConcurrentHashMap<>();
        RocksdbChangedListener probe = new RocksdbChangedListener() {
            @Override
            public void onDBClosing(String db) {
                // A final publish needs a live session: captureMetadataSnapshot must still work.
                RocksDBFactory.MetadataSnapshot snap = factory.captureMetadataSnapshot(db);
                openAtClosing.put(db, snap != null);
                if (snap != null) {
                    snap.cleanup();
                }
            }
        };
        factory.addRocksdbChangedListener(probe);
        try {
            factory.createGraphDB(parent.toString(), released, 0).close();
            factory.createGraphDB(parent.toString(), releasedAll, 0).close();

            factory.releaseGraphDB(released);
            assertEquals(Boolean.TRUE, openAtClosing.get(released));
            assertFalse(openAtClosing.containsKey(releasedAll));

            factory.releaseAllGraphDB();
            assertEquals("releaseAllGraphDB must notify before shutting sessions down",
                         Boolean.TRUE, openAtClosing.get(releasedAll));
        } finally {
            factory.removeRocksdbChangedListener(probe);
            setClosing(factory, false);
            deleteRecursively(parent.toFile());
        }
    }

    @Test
    public void testCreateGraphDbDiscardsSessionWhenFactoryClosesDuringCreation()
            throws Exception {
        Path parent = Files.createTempDirectory("hg-create-closing-");
        String dbName = "createclosing";
        RocksDBFactory factory = RocksDBFactory.getInstance();
        RocksdbChangedListener closer = new RocksdbChangedListener() {
            @Override
            public void onDBOpening(String db, String dbPath) {
                if (dbName.equals(db)) {
                    try {
                        setClosing(factory, true);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
            }
        };
        factory.addRocksdbChangedListener(closer);
        try {
            try {
                factory.createGraphDB(parent.toString(), dbName, 0);
                throw new AssertionError("createGraphDB must fail once the factory is closing");
            } catch (DBStoreException expected) {
                assertTrue(expected.getMessage().contains("closed"));
            }
            assertFalse("the discarded session must not be registered",
                        factory.getGraphNames().contains(dbName));
            assertFalse("the creation slot must be cleared",
                        pendingCreations(factory).containsKey(dbName));
        } finally {
            factory.removeRocksdbChangedListener(closer);
            setClosing(factory, false);
            deleteRecursively(parent.toFile());
        }
    }

    @Test
    public void testCreateGraphDbWaiterTimesOutOnStuckCreation() throws Exception {
        String dbName = "createstuck-" + System.nanoTime();
        RocksDBFactory factory = RocksDBFactory.getInstance();
        Map<String, CompletableFuture<Void>> pending = pendingCreations(factory);
        long originalTimeout = factory.creationWaitTimeoutMs;
        pending.put(dbName, new CompletableFuture<>());
        factory.creationWaitTimeoutMs = 200;
        try {
            try {
                factory.createGraphDB("unused", dbName, 0);
                throw new AssertionError("a waiter must time out on a stuck creation");
            } catch (DBStoreException expected) {
                assertTrue(expected.getMessage().contains("Timed out"));
            }
            // The global write lock must have been released: other DBs remain usable.
            assertNull(factory.queryGraphDB("no-such-db"));
        } finally {
            factory.creationWaitTimeoutMs = originalTimeout;
            pending.remove(dbName);
        }
    }

    @Test
    public void testCreateGraphDbWaiterWakesWhenCreatorFailsAndRecreates() throws Exception {
        Path parent = Files.createTempDirectory("hg-create-waiter-retry-");
        String dbName = "createwaiterretry";
        CountDownLatch hydrating = new CountDownLatch(1);
        CountDownLatch failNow = new CountDownLatch(1);
        AtomicInteger openings = new AtomicInteger();
        RocksdbChangedListener gate = new RocksdbChangedListener() {
            @Override
            public void onDBOpening(String db, String dbPath) {
                if (!dbName.equals(db) || openings.incrementAndGet() > 1) {
                    return;
                }
                hydrating.countDown();
                try {
                    failNow.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new RuntimeException("simulated hydration failure");
            }
        };
        RocksDBFactory factory = RocksDBFactory.getInstance();
        factory.addRocksdbChangedListener(gate);
        AtomicReference<RocksDBSession> waited = new AtomicReference<>();
        try {
            Thread creator = new Thread(() -> {
                try {
                    factory.createGraphDB(parent.toString(), dbName, 0);
                } catch (RuntimeException expected) {
                    // simulated failure
                }
            });
            creator.start();
            assertTrue(hydrating.await(30, TimeUnit.SECONDS));
            Thread waiter = new Thread(
                    () -> waited.set(factory.createGraphDB(parent.toString(), dbName, 0)));
            waiter.start();
            while (waiter.getState() != Thread.State.TIMED_WAITING && waiter.isAlive()) {
                Thread.sleep(5);
            }
            failNow.countDown();
            creator.join(30_000);
            waiter.join(30_000);
            assertNotNull("the waiter must re-check and create the DB itself", waited.get());
        } finally {
            failNow.countDown();
            factory.removeRocksdbChangedListener(gate);
            closeQuietly(waited.get());
            destroyAndCleanup(factory, parent, dbName);
        }
    }

    @Test
    public void testAwaitPendingCreationsDrainedWaitsForInFlightCreation() throws Exception {
        RocksDBFactory factory = RocksDBFactory.getInstance();
        Map<String, CompletableFuture<Void>> pending = pendingCreations(factory);
        String dbName = "drain-" + System.nanoTime();
        CompletableFuture<Void> creation = new CompletableFuture<>();
        pending.put(dbName, creation);
        try {
            AtomicBoolean returned = new AtomicBoolean();
            Thread drainer = new Thread(() -> {
                factory.awaitPendingCreationsDrained();
                returned.set(true);
            });
            drainer.start();
            Thread.sleep(200);
            assertFalse("must wait while a creation is in flight", returned.get());
            creation.complete(null);
            drainer.join(10_000);
            assertTrue("must return once the creation completes", returned.get());
        } finally {
            pending.remove(dbName);
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static RocksdbChangedListener truncateRecorder(String dbName, List<String> events) {
        return new RocksdbChangedListener() {
            @Override
            public void onDBTruncateBegin(String db, String dbPath) {
                if (dbName.equals(db)) {
                    events.add("begin");
                }
            }

            @Override
            public void onDBTruncated(String db, String dbPath) {
                if (dbName.equals(db)) {
                    events.add("truncated");
                }
            }

            @Override
            public void onDBTruncateAbort(String db, String dbPath) {
                if (dbName.equals(db)) {
                    events.add("abort");
                }
            }
        };
    }

    private static LiveFileMetaData liveFile(String dir, String fileName, String cfName) {
        LiveFileMetaData md = mock(LiveFileMetaData.class);
        when(md.path()).thenReturn(dir);
        when(md.fileName()).thenReturn(fileName);
        when(md.columnFamilyName()).thenReturn(cfName.getBytes(StandardCharsets.UTF_8));
        return md;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, CompletableFuture<Void>> pendingCreations(RocksDBFactory factory)
            throws Exception {
        Field field = RocksDBFactory.class.getDeclaredField("pendingCreations");
        field.setAccessible(true);
        return (Map<String, CompletableFuture<Void>>) field.get(factory);
    }

    private static List<?> nativeListeners(RocksDBSession session) throws Exception {
        Field field = RocksDBSession.class.getDeclaredField("dbOptions");
        field.setAccessible(true);
        return ((DBOptions) field.get(session)).listeners();
    }

    private static RocksdbChangedListener remoteStateListener() {
        return new RocksdbChangedListener() {
            @Override
            public boolean managesRemoteState() {
                return true;
            }
        };
    }

    private static void closeQuietly(RocksDBSession session) {
        if (session != null) {
            session.close();
        }
    }

    private static void destroyAndCleanup(RocksDBFactory factory, Path parent, String... dbNames) {
        for (String dbName : dbNames) {
            factory.destroyGraphDB(dbName);
        }
        factory.processPendingDestroys();
        deleteRecursively(parent.toFile());
    }

    private static void setClosing(RocksDBFactory factory, boolean value) throws Exception {
        Field field = RocksDBFactory.class.getDeclaredField("closing");
        field.setAccessible(true);
        ((AtomicBoolean) field.get(factory)).set(value);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, RocksDBSession> sessionMap(RocksDBFactory factory)
            throws Exception {
        return (Map<String, RocksDBSession>) sessionMapField().get(factory);
    }

    private static void setSessionMap(RocksDBFactory factory, Map<String, RocksDBSession> map)
            throws Exception {
        sessionMapField().set(factory, map);
    }

    private static Field sessionMapField() throws Exception {
        Field field = RocksDBFactory.class.getDeclaredField("dbSessionMap");
        field.setAccessible(true);
        return field;
    }

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
