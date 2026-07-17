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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
