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

package org.apache.hugegraph.store.node.cloud;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.RocksDBOptions;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end cloud recovery/mirroring against a <em>real</em> {@link RocksDBSession} plus an
 * in-memory cloud provider. Consolidates the former per-scenario integration classes:
 *
 * <ul>
 *   <li>{@link #testReopenAfterLocalDiskLossRecoversDataFromCloud} — flushed-SST disk-loss recovery
 *       (was {@code CloudDiskLossRecoveryIntegrationTest}).</li>
 *   <li>{@link #testInjectedProviderFailureAcrossRealRocksDbCallbackKeepsSessionStableAndGivesUp}
 *       — real RocksDB JNI callback failure is given up on by the retry queue and keeps the
 *       session stable (was {@code RocksDBCallbackJniBoundaryIntegrationTest}).</li>
 * </ul>
 *
 * <p>Mock-driven callback-path edge cases live in the unit suites, while this class keeps the
 * real RocksDB/JNI boundary integration coverage.
 */
public class CloudRecoveryIntegrationTest {

    private static RocksDBFactory factory;

    private Path baseDir;
    private String dbName;
    private CloudStorageEventListener listener;
    private CloudUploadRetryQueue retryQueue;

    @BeforeAll
    public static void initFactory() {
        OptionSpace.register("rocksdb",
                             "org.apache.hugegraph.rocksdb.access.RocksDBOptions");
        RocksDBOptions.instance();
        Map<String, Object> cfg = new HashMap<>();
        // Large write buffer to avoid incidental natural flushes; the disk-loss and JNI tests flush
        // explicitly via session.flush(true), so this is harmless there.
        cfg.put("rocksdb.write_buffer_size", String.valueOf(256L * 1024 * 1024));
        cfg.put("rocksdb.bloom_filter_bits_per_key", "10");
        factory = RocksDBFactory.getInstance();
        factory.setHugeConfig(new HugeConfig(new MapConfiguration(cfg)));
    }

    @BeforeEach
    public void setUp() throws IOException {
        this.baseDir = Files.createTempDirectory("hg-cloudrec-");
    }

    @AfterEach
    public void tearDown() {
        if (this.listener != null && factory != null) {
            factory.removeRocksdbChangedListener(this.listener);
        }
        if (factory != null && this.dbName != null) {
            try {
                factory.releaseGraphDB(this.dbName);
            } catch (Exception ignore) {
                // best-effort
            }
        }
        if (this.retryQueue != null) {
            this.retryQueue.close();
            this.retryQueue = null;
        }
        if (this.baseDir != null) {
            deleteRecursively(this.baseDir.toFile());
        }
        this.listener = null;
        this.dbName = null;
    }

    // =========================================================================
    // Flushed-SST disk-loss recovery
    // =========================================================================

    @Test
    public void testReopenAfterLocalDiskLossRecoversDataFromCloud() {
        this.dbName = "recdb";
        InMemoryCloudStore cloud = new InMemoryCloudStore();

        CloudSyncTracker tracker = new CloudSyncTracker();
        this.listener = CloudStorageTestFactory.newListener(cloud,
                Collections.singletonList(this.baseDir.toString()),
                true, 0L, null, tracker);
        factory.addRocksdbChangedListener(this.listener);

        // --- Phase 1: create DB, write data, mirror a consistent snapshot to cloud ---
        RocksDBSession session = factory.createGraphDB(this.baseDir.toString(), this.dbName, 0);
        String resolvedPath = session.getDbPath();
        session.checkTable("t");

        Map<String, String> expected = new HashMap<>();
        SessionOperator op = session.sessionOp();
        op.prepare();
        for (int i = 0; i < 500; i++) {
            String k = "key-" + i;
            String v = "value-" + i;
            op.put("t", k.getBytes(StandardCharsets.UTF_8), v.getBytes(StandardCharsets.UTF_8));
            expected.put(k, v);
        }
        op.commit();
        session.flush(true); // create SST files

        // Mirror a consistent set (referenced SSTs + OPTIONS + MANIFEST + CURRENT) to cloud.
        assertTrue(this.listener.syncMetadataSnapshotInline(cloud, this.dbName),
                   "metadata mirror must succeed");
        assertTrue(cloud.fileExists(this.dbName + "/CURRENT"),
                   "cloud must contain the CURRENT pointer after mirroring");

        // --- Phase 2: simulate local disk loss (no cloud purge) ---
        session.close();                       // release our clone's ref
        factory.releaseGraphDB(this.dbName);   // close the DB handle, remove from map (no purge)
        deleteRecursively(new File(resolvedPath));
        assertFalse(new File(resolvedPath, "CURRENT").exists(),
                    "local DB directory must be gone (simulated disk loss)");

        // --- Phase 3: reopen at the same path — must hydrate from cloud BEFORE open ---
        RocksDBSession recovered = factory.createGraphDB(this.baseDir.toString(), this.dbName, 0);
        assertNotNull(recovered);
        assertEquals(resolvedPath,
                     recovered.getDbPath(),
                     "recovery must resolve to the same DB path so cloud keys match");

        // --- Phase 4: every key must be readable from the recovered DB ---
        recovered.checkTable("t");
        SessionOperator rop = recovered.sessionOp();
        int recoveredCount = 0;
        for (Map.Entry<String, String> e : expected.entrySet()) {
            byte[] got = rop.get("t", e.getKey().getBytes(StandardCharsets.UTF_8));
            assertArrayEquals(e.getValue().getBytes(StandardCharsets.UTF_8),
                              got,
                              "recovered value mismatch for " + e.getKey());
            recoveredCount++;
        }
        assertEquals(expected.size(), recoveredCount, "all keys must be recovered from cloud");
    }

    // =========================================================================
    // Real RocksDB JNI callback: failure is given up on, session stays stable
    // =========================================================================

    @Test
    public void testInjectedProviderFailureAcrossRealRocksDbCallbackKeepsSessionStableAndGivesUp()
            throws Exception {
        this.dbName = "jni-boundary-db";
        Path dbPath = this.baseDir.resolve(this.dbName);

        // maxAttempts=0 → an async upload failure is given up on immediately, giving a
        // deterministic postcondition instead of a timing-dependent retry cycle.
        // The provider starts healthy and is only switched to failing after the DB is open, so
        // that ONLY the real onTableFileCreated callback (fired by the flush below) exercises the
        // failure path.
        AtomicInteger uploadAttempts = new AtomicInteger(0);
        ToggleableFailingUploadProvider provider =
                new ToggleableFailingUploadProvider(uploadAttempts);
        this.retryQueue = CloudStorageTestFactory.newRetryQueue(provider, 0, 50L, 50L);
        // Data root = baseDir so the SST files RocksDB writes under dbPath (a child of
        // baseDir) can be hard-link staged for async upload.
        this.listener = CloudStorageTestFactory.newListener(provider,
                List.of(this.baseDir.toString()), false, 0L, this.retryQueue);

        // The listener must be registered BEFORE the DB is opened: RocksDBSession#openRocksDB
        // only wires the native RocksdbChangedListener into dbOptions when
        // RocksDBFactory#hasRocksdbChangedListeners() is true AT OPEN TIME (a one-time decision,
        // to avoid ever exercising the native callback — and its classloading — when no listener
        // feature is enabled). The provider is still healthy at this point, so onDBOpening and
        // onDBCreated succeed; only the flush below exercises the injected failure.
        factory.addRocksdbChangedListener(this.listener);

        RocksDBSession session = factory.createGraphDB(dbPath.toString(), this.dbName);

        try (session) {
            assertNotNull(session, "expected a real RocksDB session");

            // Switch the provider to failing now, so ONLY the real onTableFileCreated callback
            // (fired by the flush below) exercises the failure path.
            provider.startFailing();

            // Write a batch and flush: RocksDB creates a real SST and invokes onTableFileCreated
            // across the JNI boundary on its own background thread.
            session.checkTable("t");
            SessionOperator op = session.sessionOp();
            op.prepare();
            for (int i = 0; i < 500; i++) {
                op.put("t", ("key-" + i).getBytes(), ("value-" + i).getBytes());
            }
            op.commit();
            session.flush(true);

            // The real callback must have driven an upload attempt that failed and was captured.
            waitForCondition(() -> this.retryQueue.getGivenUpCount() > 0
            );
            assertTrue(uploadAttempts.get() > 0,
                       "provider.uploadFile must have been invoked by the real callback");
            assertTrue(this.retryQueue.getGivenUpCount() > 0,
                       "upload failure must be captured on the durability path (given up)");

            // Process/session stability: the DB must remain fully usable after the callback-path
            // failure — a JNI-boundary regression would typically crash the JVM or corrupt state.
            SessionOperator after = session.sessionOp();
            after.prepare();
            after.put("t", "post-failure".getBytes(), "still-alive".getBytes());
            after.commit();

            byte[] readBack = session.sessionOp().get("t", "post-failure".getBytes());
            assertArrayEquals("still-alive".getBytes(),
                              readBack,
                              "DB must remain readable/writable after callback-path upload "
                              + "failure");
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static void waitForCondition(BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            LockSupport.parkNanos(10_000_000L);
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Interrupted while waiting for async cloud callback");
            }
        }
        fail("injected provider failure from the real RocksDB callback should be given up on "
             + "by the retry queue");
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

    /**
     * Thread-safe in-memory cloud store sufficient for the recovery round-trips.
     */
    private static final class InMemoryCloudStore implements CloudStorageProvider {

        private final Map<String, byte[]> objects = new ConcurrentHashMap<>();

        @Override
        public String providerName() {
            return "in-memory-cloudrec-test";
        }

        @Override
        public void init(CloudStorageConfig config) {
            // no-op
        }

        @Override
        public void uploadFile(String localPath, String remoteKey) throws IOException {
            objects.put(remoteKey, Files.readAllBytes(Paths.get(localPath)));
        }

        @Override
        public void deleteFile(String remoteKey) {
            objects.remove(remoteKey);
        }

        @Override
        public boolean fileExists(String remoteKey) {
            return objects.containsKey(remoteKey);
        }

        @Override
        public List<String> listFiles(String remoteDirPrefix) {
            String prefix = remoteDirPrefix.endsWith("/") ? remoteDirPrefix : remoteDirPrefix + "/";
            return objects.keySet().stream()
                          .filter(k -> k.startsWith(prefix))
                          .collect(Collectors.toList());
        }

        @Override
        public void downloadFile(String remoteKey, String localPath) throws IOException {
            byte[] content = objects.get(remoteKey);
            if (content == null) {
                throw new IOException("Key not found: " + remoteKey);
            }
            Path dest = Paths.get(localPath);
            if (dest.getParent() != null) {
                Files.createDirectories(dest.getParent());
            }
            Files.write(dest, content);
        }

        @Override
        public int deletePrefix(String remoteDirPrefix) {
            String prefix = remoteDirPrefix.endsWith("/") ? remoteDirPrefix : remoteDirPrefix + "/";
            List<String> keys = new ArrayList<>(objects.keySet());
            int deleted = 0;
            for (String k : keys) {
                if (k.startsWith(prefix)) {
                    objects.remove(k);
                    deleted++;
                }
            }
            return deleted;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /**
     * Provider whose uploads succeed until {@link #startFailing()} is called, after which every
     * upload fails, simulating a persistent cloud outage that begins mid-test.
     */
    private static final class ToggleableFailingUploadProvider implements CloudStorageProvider {

        private final AtomicInteger uploadAttempts;
        private volatile boolean failing;

        ToggleableFailingUploadProvider(AtomicInteger uploadAttempts) {
            this.uploadAttempts = uploadAttempts;
        }

        void startFailing() {
            this.failing = true;
        }

        @Override
        public String providerName() {
            return "always-failing-test-provider";
        }

        @Override
        public void init(CloudStorageConfig config) {
            // no-op
        }

        @Override
        public void uploadFile(String localPath, String remoteKey) throws IOException {
            if (!this.failing) {
                return;
            }
            this.uploadAttempts.incrementAndGet();
            throw new IOException("simulated persistent cloud outage");
        }

        @Override
        public void deleteFile(String remoteKey) {
            // no-op
        }

        @Override
        public boolean fileExists(String remoteKey) {
            return false;
        }

        @Override
        public void downloadFile(String remoteKey, String localPath) throws IOException {
            throw new IOException("download not supported in this test provider");
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
