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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.apache.hugegraph.store.cloud.CloudStorageProviderFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * End-to-end cloud recovery/mirroring against a <em>real</em> {@link RocksDBSession} plus an
 * in-memory cloud provider. Consolidates the former per-scenario integration classes:
 *
 * <ul>
 *   <li>{@link #testReopenAfterLocalDiskLossRecoversDataFromCloud} — flushed-SST disk-loss recovery
 *       (was {@code CloudDiskLossRecoveryIntegrationTest}).</li>
 *   <li>{@link #testInjectedProviderFailureAcrossRealRocksDbCallbackKeepsSessionStableAndRoutesToDlq}
 *       — real RocksDB JNI callback failure routes to the DLQ and keeps the session stable (was
 *       {@code RocksDBCallbackJniBoundaryIntegrationTest}).</li>
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

    @BeforeClass
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

    @Before
    public void setUp() throws IOException {
        this.baseDir = Files.createTempDirectory("hg-cloudrec-");
    }

    @After
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
        CloudStorageProviderFactory.reset();
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
        CloudStorageProviderFactory.setActiveProviderForTest(cloud);

        CloudSyncTracker tracker = new CloudSyncTracker();
        this.listener = new CloudStorageEventListener(
                Collections.singletonList(this.baseDir.toString()),
                true, 0L, null, tracker, 0);
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
        assertTrue("metadata mirror must succeed",
                   this.listener.syncMetadataSnapshotInline(cloud, this.dbName));
        assertTrue("cloud must contain the CURRENT pointer after mirroring",
                   cloud.fileExists(this.dbName + "/CURRENT"));

        // --- Phase 2: simulate local disk loss (no cloud purge) ---
        session.close();                       // release our clone's ref
        factory.releaseGraphDB(this.dbName);   // close the DB handle, remove from map (no purge)
        deleteRecursively(new File(resolvedPath));
        assertFalse("local DB directory must be gone (simulated disk loss)",
                    new File(resolvedPath, "CURRENT").exists());

        // --- Phase 3: reopen at the same path — must hydrate from cloud BEFORE open ---
        RocksDBSession recovered = factory.createGraphDB(this.baseDir.toString(), this.dbName, 0);
        assertNotNull(recovered);
        assertEquals("recovery must resolve to the same DB path so cloud keys match",
                     resolvedPath, recovered.getDbPath());

        // --- Phase 4: every key must be readable from the recovered DB ---
        recovered.checkTable("t");
        SessionOperator rop = recovered.sessionOp();
        int recoveredCount = 0;
        for (Map.Entry<String, String> e : expected.entrySet()) {
            byte[] got = rop.get("t", e.getKey().getBytes(StandardCharsets.UTF_8));
            assertArrayEquals("recovered value mismatch for " + e.getKey(),
                              e.getValue().getBytes(StandardCharsets.UTF_8), got);
            recoveredCount++;
        }
        assertEquals("all keys must be recovered from cloud", expected.size(), recoveredCount);
    }

    // =========================================================================
    // Real RocksDB JNI callback: failure routes to DLQ, session stays stable
    // =========================================================================

    @Test
    public void testInjectedProviderFailureAcrossRealRocksDbCallbackKeepsSessionStableAndRoutesToDlq()
            throws Exception {
        this.dbName = "jni-boundary-db";
        Path dbPath = this.baseDir.resolve(this.dbName);

        // Open a real RocksDB instance. No listener/provider is wired yet, so the open path
        // (onDBOpening / onDBCreated) is unaffected by the injected failure below.
        RocksDBSession session = factory.createGraphDB(dbPath.toString(), this.dbName);

        try (session) {
            assertNotNull("expected a real RocksDB session", session);
            // maxAttempts=0 → an async upload failure is routed straight to the DLQ, giving a
            // deterministic postcondition instead of a timing-dependent retry cycle.
            this.retryQueue = new CloudUploadRetryQueue(0, 50L, 50L, this.baseDir.toString());
            // Data root = baseDir so the SST files RocksDB writes under dbPath (a child of
            // baseDir) can be hard-link staged for async upload.
            this.listener = new CloudStorageEventListener(
                    List.of(this.baseDir.toString()), false, 0L, this.retryQueue);

            // Register AFTER open, then inject an always-failing provider so ONLY the real
            // onTableFileCreated callback (fired by the flush below) exercises the failure path.
            factory.addRocksdbChangedListener(this.listener);
            AtomicInteger uploadAttempts = new AtomicInteger(0);
            CloudStorageProviderFactory.setActiveProviderForTest(
                    new AlwaysFailingUploadProvider(uploadAttempts));

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
            waitForCondition(() -> this.retryQueue.getDlqSize() > 0
            );
            assertTrue("provider.uploadFile must have been invoked by the real callback",
                       uploadAttempts.get() > 0);
            assertTrue("upload failure must be captured on the durability path (DLQ)",
                       this.retryQueue.getDlqSize() > 0);

            // Process/session stability: the DB must remain fully usable after the callback-path
            // failure — a JNI-boundary regression would typically crash the JVM or corrupt state.
            SessionOperator after = session.sessionOp();
            after.prepare();
            after.put("t", "post-failure".getBytes(), "still-alive".getBytes());
            after.commit();

            byte[] readBack = session.sessionOp().get("t", "post-failure".getBytes());
            assertArrayEquals("DB must remain readable/writable after callback-path upload failure",
                              "still-alive".getBytes(), readBack);
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
        fail("injected provider failure from the real RocksDB callback should land in the DLQ");
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

    /** Provider whose upload always fails, simulating a persistent cloud outage. */
    private static final class AlwaysFailingUploadProvider implements CloudStorageProvider {

        private final AtomicInteger uploadAttempts;

        AlwaysFailingUploadProvider(AtomicInteger uploadAttempts) {
            this.uploadAttempts = uploadAttempts;
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
